package partiql

// Update grammar AST (DynamoDbUpdateExpressionSyntax.java).
type Update struct {
	Sets    []setAction
	Removes []Path
	Adds    []applyAction
	Deletes []applyAction
}

type setAction struct {
	Target Path
	Value  valNode
}

type applyAction struct {
	Target Path
	Ref    string
}

// Value grammar of the SET action: ":ref", paths, list_append(...),
// if_not_exists(path, ...) and +/- arithmetic (no parentheses, exactly like
// the Java value grammar).
type valNode interface{}
type vRef struct{ Ref string }
type vPath struct{ Path Path }
type vListAppend struct{ L, R valNode }
type vIfNotExists struct {
	Path Path
	V    valNode
}
type vArith struct {
	L, R valNode
	Op   string // "+" or "-"
}

var actionOrder = map[string]int{"set": 0, "remove": 1, "add": 2, "delete": 3}

const (
	updSet    = "set"
	updRemove = "remove"
	updAdd    = "add"
	updDelete = "delete"
)

func addOrDelete(k string) bool { return k == updAdd || k == updDelete }

// parseUpdate enforces the Java grammar: the four action keywords occur at
// most once and in the order SET, REMOVE, ADD, DELETE; assignments and
// operands inside an action are separated by commas.
func (p *parser) parseUpdate() (*Update, error) {
	if t := p.cur(); t.Kind == tkEOF {
		return nil, p.failTok(t)
	}
	u := &Update{}
	seenKw := map[string]bool{}
	last := -1
	for {
		t := p.cur()
		if t.Kind != tkWord {
			return nil, p.failTok(t)
		}
		kw := foldWord(t)
		idx, ok := actionOrder[kw]
		if !ok || seenKw[kw] || idx < last {
			return nil, p.failTok(t)
		}
		last = idx
		seenKw[kw] = true
		p.eat()
		switch kw {
		case "set":
			for {
				pt := p.cur()
				pth, err := p.parsePathFrom(pt)
				if err != nil {
					return nil, err
				}
				if err := p.expectSym("="); err != nil {
					return nil, err
				}
				v, err := p.parseValueExpr()
				if err != nil {
					return nil, err
				}
				u.Sets = append(u.Sets, setAction{Target: pth, Value: v})
				if p.atSym(",") {
					p.eat()
					continue
				}
				break
			}
		case "remove":
			for {
				pt := p.cur()
				pth, err := p.parsePathFrom(pt)
				if err != nil {
					return nil, err
				}
				u.Removes = append(u.Removes, pth)
				if p.atSym(",") {
					p.eat()
					continue
				}
				break
			}
		default: // add / delete
			for {
				pt := p.cur()
				pth, err := p.parsePathFrom(pt)
				if err != nil {
					return nil, err
				}
				rt := p.cur()
				if rt.Kind != tkValueRef {
					return nil, p.failTok(rt)
				}
				p.eat()
				p.usedV[rt.Text] = struct{}{}
				act := applyAction{Target: pth, Ref: rt.Text}
				if kw == "add" {
					u.Adds = append(u.Adds, act)
				} else {
					u.Deletes = append(u.Deletes, act)
				}
				if p.atSym(",") {
					p.eat()
					continue
				}
				break
			}
		}
		if t2 := p.cur(); t2.Kind == tkEOF {
			break
		}
	}
	// overlap / duplicate path validation, as in the Java syntax pass
	if err := u.checkOverlaps(); err != nil {
		return nil, err
	}
	return u, nil
}

func (p *parser) parseValueExpr() (valNode, error) { return p.parseArith() }

func (p *parser) parseArith() (valNode, error) {
	l, err := p.parseValueTerm()
	if err != nil {
		return nil, err
	}
	for {
		t := p.cur()
		if t.Kind == tkSym && (t.Text == "+" || t.Text == "-") {
			p.eat()
			r, err := p.parseValueTerm()
			if err != nil {
				return nil, err
			}
			l = vArith{L: l, R: r, Op: t.Text}
			continue
		}
		break
	}
	return l, nil
}

func (p *parser) parseValueTerm() (valNode, error) {
	t := p.cur()
	switch t.Kind {
	case tkValueRef:
		p.eat()
		p.usedV[t.Text] = struct{}{}
		return vRef{Ref: t.Text}, nil
	case tkWord:
		k := foldWord(t)
		if (k == "list_append" || k == "if_not_exists") && p.peekAt(1).Kind == tkSym && p.peekAt(1).Text == "(" {
			return p.parseValueFunc(k)
		}
		pth, err := p.parsePathFrom(t)
		if err != nil {
			return nil, err
		}
		return vPath{Path: pth}, nil
	case tkNameRef:
		pth, err := p.parsePathFrom(t)
		if err != nil {
			return nil, err
		}
		return vPath{Path: pth}, nil
	default:
		return nil, p.failTok(t)
	}
}

func (p *parser) parseValueFunc(k string) (valNode, error) {
	p.eat() // function word
	p.eat() // '('
	if k == "if_not_exists" {
		ft := p.cur()
		if ft.Kind != tkWord && ft.Kind != tkNameRef {
			return nil, p.failTok(ft)
		}
		pth, err := p.parsePathFrom(ft)
		if err != nil {
			return nil, err
		}
		if err := p.expectSym(","); err != nil {
			return nil, err
		}
		v, err := p.parseArith()
		if err != nil {
			return nil, err
		}
		if err := p.expectSym(")"); err != nil {
			return nil, err
		}
		return vIfNotExists{Path: pth, V: v}, nil
	}
	l, err := p.parseArith()
	if err != nil {
		return nil, err
	}
	if err := p.expectSym(","); err != nil {
		return nil, err
	}
	r, err := p.parseArith()
	if err != nil {
		return nil, err
	}
	if err := p.expectSym(")"); err != nil {
		return nil, err
	}
	return vListAppend{L: l, R: r}, nil
}

func (u *Update) checkOverlaps() error {
	root := newOverlapNode()
	for _, a := range u.Sets {
		if err := insertPath(root, a.Target.Elems, updSet); err != nil {
			return err
		}
	}
	for _, pth := range u.Removes {
		if err := insertPath(root, pth.Elems, updRemove); err != nil {
			return err
		}
	}
	for _, a := range u.Adds {
		if err := insertPath(root, a.Target.Elems, updAdd); err != nil {
			return err
		}
	}
	for _, a := range u.Deletes {
		if err := insertPath(root, a.Target.Elems, updDelete); err != nil {
			return err
		}
	}
	return nil
}

func insertPath(root *overlapNode, elems []PathElem, kind string) error {
	kinds, dup, ov := root.insert(elems, kind)
	if ov {
		return overlapErr(KindUpdate)
	}
	if dup {
		if !addOrDelete(kind) {
			return overlapErr(KindUpdate)
		}
		for k := range kinds {
			if !addOrDelete(k) {
				return overlapErr(KindUpdate)
			}
		}
		return addDeleteDupErr()
	}
	return nil
}

// Apply executes the update actions against item in declaration order,
// mutating it in place; evaluation errors abort at the first failing action.
func (u *Update) Apply(item AV, values map[string]AV, kind ExprKind) error {
	if item == nil {
		return documentPathErr()
	}
	ctx := &updCtx{item: item, values: values, kind: kind}
	for _, a := range u.Sets {
		v, err := evalValue(a.Value, ctx)
		if err != nil {
			return err
		}
		if err := setLeaf(item, a.Target, v); err != nil {
			return err
		}
	}
	for _, r := range u.Removes {
		if err := removeLeaf(item, r); err != nil {
			return err
		}
	}
	for _, a := range u.Adds {
		v, err := ctx.lookup(a.Ref)
		if err != nil {
			return err
		}
		if err := addLeaf(item, a.Target, v, ctx); err != nil {
			return err
		}
	}
	for _, a := range u.Deletes {
		v, err := ctx.lookup(a.Ref)
		if err != nil {
			return err
		}
		if err := deleteLeaf(item, a.Target, v, ctx); err != nil {
			return err
		}
	}
	return nil
}

type updCtx struct {
	item   AV
	values map[string]AV
	kind   ExprKind
}

// lookup resolves an ExpressionAttributeValue reference, rejecting empty
// sets exactly like the Java value validation.
func (c *updCtx) lookup(ref string) (AV, error) {
	v, ok := c.values[ref]
	if !ok {
		return nil, valueNotDefinedErr(ref)
	}
	if err := checkSetValue(v); err != nil {
		return nil, err
	}
	if KindOf(v) == "" {
		return nil, avBad()
	}
	return v, nil
}

// evalValue evaluates a SET value expression to an attribute value.
func evalValue(n valNode, ctx *updCtx) (AV, error) {
	switch x := n.(type) {
	case vRef:
		v, err := ctx.lookup(x.Ref)
		if err != nil {
			return nil, err
		}
		return DeepCopyAV(v), nil
	case vPath:
		v, ok := avGet(ctx.item, x.Path)
		if !ok {
			return nil, missingAttrErr()
		}
		m, mok := mapAny(v)
		if !mok {
			return nil, documentPathErr()
		}
		return DeepCopyAV(m), nil
	case vIfNotExists:
		if v, ok := avGet(ctx.item, x.Path); ok {
			m, mok := mapAny(v)
			if !mok {
				return nil, documentPathErr()
			}
			return DeepCopyAV(m), nil
		}
		return evalValue(x.V, ctx)
	case vListAppend:
		l, err := evalValue(x.L, ctx)
		if err != nil {
			return nil, err
		}
		r, err := evalValue(x.R, ctx)
		if err != nil {
			return nil, err
		}
		if lk := KindOf(l); lk != TypeL {
			return nil, operandTypeErr(ctx.kind, "list_append", lk)
		}
		if rk := KindOf(r); rk != TypeL {
			return nil, operandTypeErr(ctx.kind, "list_append", rk)
		}
		le, _ := listElems(l[TypeL])
		re, _ := listElems(r[TypeL])
		ne := make([]any, 0, len(le)+len(re))
		for _, e := range le {
			ne = append(ne, CopyVal(e))
		}
		for _, e := range re {
			ne = append(ne, CopyVal(e))
		}
		return AV{TypeL: ne}, nil
	case vArith:
		l, err := evalValue(x.L, ctx)
		if err != nil {
			return nil, err
		}
		r, err := evalValue(x.R, ctx)
		if err != nil {
			return nil, err
		}
		if lk := KindOf(l); lk != TypeN {
			return nil, operandTypeErr(ctx.kind, x.Op, lk)
		}
		if rk := KindOf(r); rk != TypeN {
			return nil, operandTypeErr(ctx.kind, x.Op, rk)
		}
		lv, lok := decVal(l[TypeN])
		rv, rok := decVal(r[TypeN])
		if !lok || !rok {
			return nil, badValueErr("Invalid decimal value")
		}
		var d Dec
		var derr error
		if x.Op == "+" {
			d, derr = decAdd(lv, rv)
		} else {
			d, derr = decSub(lv, rv)
		}
		if derr != nil {
			return nil, derr
		}
		return AV{TypeN: d.String()}, nil
	}
	return nil, verr(CodeValidation, "internal: unknown value node")
}

// container is the parent document node a write-action leaf lives in.
type container struct {
	kind  string // "M" map, "L" list, "S" set
	m     AV
	l     []any
	write func(ns []any) // persists a replacement list back into the document
}

func asContainer(v any, put func(ns []any)) (*container, error) {
	switch x := v.(type) {
	case AV:
		return &container{kind: "M", m: x, write: func(ns []any) {}}, nil
	case []any:
		k := "L"
		if len(x) > 0 {
			if _, ok := x[0].(AV); !ok {
				k = "S" // sets are navigable like the Java document walker says: not at all
			}
		}
		return &container{kind: k, l: x, write: put}, nil
	default:
		return nil, documentPathErr()
	}
}

// walkParent navigates to the container owning the leaf of elems; every
// intermediate segment must exist and be typed correctly.
func walkParent(item AV, elems []PathElem) (*container, error) {
	cur := &container{kind: "M", m: item, write: func(ns []any) {}}
	for _, e := range elems[:len(elems)-1] {
		var child any
		var put func(ns []any)
		switch cur.kind {
		case "M":
			if e.IsIdx {
				return nil, documentPathErr()
			}
			v, ok := cur.m[e.Name]
			if !ok {
				return nil, documentPathErr()
			}
			child = v
			name, m := e.Name, cur.m
			put = func(ns []any) { m[name] = ns }
		case "L":
			if !e.IsIdx || e.Index >= len(cur.l) {
				return nil, documentPathErr()
			}
			child = cur.l[e.Index]
			idx, l := e.Index, cur.l
			put = func(ns []any) { l[idx] = ns }
		default:
			return nil, documentPathErr()
		}
		nc, err := asContainer(child, put)
		if err != nil {
			return nil, err
		}
		cur = nc
	}
	return cur, nil
}

func setLeaf(item AV, p Path, v AV) error {
	parent, err := walkParent(item, p.Elems)
	if err != nil {
		return err
	}
	last := p.Elems[len(p.Elems)-1]
	switch parent.kind {
	case "L":
		if !last.IsIdx || last.Index >= len(parent.l) {
			return documentPathErr()
		}
		parent.l[last.Index] = v
	case "S":
		return documentPathErr()
	default:
		if last.IsIdx {
			return documentPathErr()
		}
		parent.m[last.Name] = v
	}
	return nil
}

func removeLeaf(item AV, p Path) error {
	parent, err := walkParent(item, p.Elems)
	if err != nil {
		return err
	}
	last := p.Elems[len(p.Elems)-1]
	switch parent.kind {
	case "L":
		if !last.IsIdx || last.Index >= len(parent.l) {
			return documentPathErr()
		}
		ns := make([]any, 0, len(parent.l))
		ns = append(ns, parent.l[:last.Index]...)
		ns = append(ns, parent.l[last.Index+1:]...)
		parent.write(ns)
	case "S":
		return documentPathErr()
	default:
		delete(parent.m, last.Name)
	}
	return nil
}

func addLeaf(item AV, p Path, v AV, ctx *updCtx) error {
	parent, err := walkParent(item, p.Elems)
	if err != nil {
		return err
	}
	last := p.Elems[len(p.Elems)-1]
	var cur AV
	have := false
	store := func(dest AV) {
		if parent.kind == "L" {
			parent.l[last.Index] = dest
		} else {
			parent.m[last.Name] = dest
		}
	}
	switch parent.kind {
	case "L":
		if !last.IsIdx || last.Index >= len(parent.l) {
			return documentPathErr()
		}
		if raw := parent.l[last.Index]; raw != nil {
			m, ok := mapAny(raw)
			if !ok {
				return documentPathErr()
			}
			cur, have = m, true
		}
	case "S":
		return documentPathErr()
	default:
		if last.IsIdx {
			return documentPathErr()
		}
		if raw, ok := parent.m[last.Name]; ok && raw != nil {
			m, mok := mapAny(raw)
			if !mok {
				return documentPathErr()
			}
			cur, have = m, true
		}
	}
	vk := KindOf(v)
	if !have {
		if vk != TypeN && vk != TypeSS && vk != TypeNS && vk != TypeBS {
			return operandTypeErr(ctx.kind, "ADD", vk)
		}
		store(DeepCopyAV(v))
		return nil
	}
	ck := KindOf(cur)
	switch ck {
	case TypeN:
		if vk != TypeN {
			return operandTypeErr(ctx.kind, "ADD", vk)
		}
		cn, ok1 := decVal(cur[TypeN])
		vn, ok2 := decVal(v[TypeN])
		if !ok1 || !ok2 {
			return badValueErr("Invalid decimal value")
		}
		res, err := decAdd(cn, vn)
		if err != nil {
			return err
		}
		store(AV{TypeN: res.String()})
	case TypeSS, TypeNS, TypeBS:
		if vk != ck {
			return operandTypeErr(ctx.kind, "ADD", vk)
		}
		merged, err := mergeSet(ck, cur, v)
		if err != nil {
			return err
		}
		store(merged)
	default:
		return operandTypeErr(ctx.kind, "ADD", ck)
	}
	return nil
}

func deleteLeaf(item AV, p Path, v AV, ctx *updCtx) error {
	parent, err := walkParent(item, p.Elems)
	if err != nil {
		return err
	}
	last := p.Elems[len(p.Elems)-1]
	var cur AV
	have := false
	store := func(dest AV) {
		if parent.kind == "L" {
			parent.l[last.Index] = dest
		} else {
			parent.m[last.Name] = dest
		}
	}
	switch parent.kind {
	case "L":
		if !last.IsIdx || last.Index >= len(parent.l) {
			return documentPathErr()
		}
		if raw := parent.l[last.Index]; raw != nil {
			m, ok := mapAny(raw)
			if !ok {
				return documentPathErr()
			}
			cur, have = m, true
		}
	case "S":
		return documentPathErr()
	default:
		if last.IsIdx {
			return documentPathErr()
		}
		if raw, ok := parent.m[last.Name]; ok && raw != nil {
			m, mok := mapAny(raw)
			if !mok {
				return documentPathErr()
			}
			cur, have = m, true
		}
	}
	vk := KindOf(v)
	if !have {
		if vk != TypeSS && vk != TypeNS && vk != TypeBS {
			return operandTypeErr(ctx.kind, "DELETE", vk)
		}
		return nil // removing from a set that does not exist is a no-op
	}
	ck := KindOf(cur)
	switch ck {
	case TypeSS, TypeNS, TypeBS:
		if vk != ck {
			return operandTypeErr(ctx.kind, "DELETE", vk)
		}
		merged, err := removeFromSet(ck, cur, v)
		if err != nil {
			return err
		}
		if merged == nil {
			return emptySetErr(ck)
		}
		store(merged)
	default:
		return operandTypeErr(ctx.kind, "DELETE", ck)
	}
	return nil
}

// mergeSet unions value into set, keeping the existing element order.
func mergeSet(kind string, set AV, v AV) (AV, error) {
	se, _ := listElems(set[kind])
	ve, _ := listElems(v[kind])
	out := make([]any, 0, len(se)+len(ve))
	out = append(out, se...)
	for _, e := range ve {
		if !setContains(kind, set, e) {
			out = append(out, CopyVal(e))
		}
	}
	return AV{kind: out}, nil
}

// removeFromSet removes the value's elements from set; nil signals that the
// set would become empty (which DynamoDB rejects).
func removeFromSet(kind string, set AV, v AV) (AV, error) {
	se, _ := listElems(set[kind])
	out := make([]any, 0, len(se))
	for _, e := range se {
		if !setContains(kind, v, e) {
			out = append(out, e)
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return AV{kind: out}, nil
}
