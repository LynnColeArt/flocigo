package partiql

import (
	"bytes"
	"strconv"
	"strings"
)

type operand interface{}

type valRef struct{ Ref string }   // :value reference
type pathRef struct{ Path Path }   // document path
type sizeRef struct{ Path Path }   // size(path)

// Condition grammar AST (ExpressionEvaluator.java).
type condNode interface{}
type cndOr struct {
	Xs []condNode
}
type cndAnd struct {
	Xs []condNode
}
type cndNot struct {
	X condNode
}
type cndCmp struct {
	Op   string // =, <>, <, <=, >, >=
	L, R operand
}
type cndBetween struct {
	X, Lo, Hi operand
}
type cndIn struct {
	X    operand
	List []operand
}
type cndExists struct {
	Path Path
	Not  bool
}
type cndBegins struct {
	L, R operand
}
type cndContains struct {
	L, R operand
}
type cndAttrType struct {
	L, R operand
}

// Condition holds a parsed condition / filter expression.
type Condition struct {
	Root condNode
}

var condFuncNames = map[string]bool{
	"attribute_exists":     true,
	"" + "attribute_not_exists": true,
	"begins_with":          true,
	"contains":             true,
	"attribute_type":       true,
}

var comparatorSyms = map[string]bool{
"=": true, "<>": true, "<": true, "<=": true, ">": true, ">=": true,
}

const inLimit = 100

// parseCondition parses the body of a condition (or filter) expression.
func (p *parser) parseCondition() (condNode, error) {
	x, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if t := p.cur(); t.Kind != tkEOF {
		return nil, p.failTok(t)
	}
	return x, nil
}

// parseOr implements OR with the lowest precedence.
func (p *parser) parseOr() (condNode, error) {
	first, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	xs := []condNode{first}
	for p.isKw("or") {
		p.eat()
		y, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		xs = append(xs, y)
	}
	if len(xs) == 1 {
		return xs[0], nil
	}
	return cndOr{Xs: xs}, nil
}

func (p *parser) parseAnd() (condNode, error) {
	first, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	xs := []condNode{first}
	for p.isKw("and") {
		p.eat()
		y, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		xs = append(xs, y)
	}
	if len(xs) == 1 {
		return xs[0], nil
	}
	return cndAnd{Xs: xs}, nil
}

// parseNot implements NOT with the highest precedence of the boolean ops.
func (p *parser) parseNot() (condNode, error) {
	if p.isKw("not") {
		p.eat()
		x, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return cndNot{X: x}, nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (condNode, error) {
	t := p.cur()
	if t.Kind == tkSym && t.Text == "(" {
		p.eat()
		x, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if err := p.expectSym(")"); err != nil {
			return nil, err
		}
		return x, nil
	}
	if t.Kind == tkWord {
		name := foldWord(t)
		if condFuncNames[name] && p.peekAt(1).Kind == tkSym && p.peekAt(1).Text == "(" {
			return p.parseCondFunc(name)
		}
	}
	l, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	nt := p.cur()
	switch {
	case nt.Kind == tkSym && comparatorSyms[nt.Text]:
		p.eat()
		r, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return cndCmp{Op: nt.Text, L: l, R: r}, nil
	case p.isKw("between"):
		p.eat()
		lo, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		if !p.isKw("and") {
			return nil, p.failTok(p.cur())
		}
		p.eat()
		hi, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return cndBetween{X: l, Lo: lo, Hi: hi}, nil
	case p.isKw("in"):
		p.eat()
		if err := p.expectSym("("); err != nil {
			return nil, err
		}
		elems, err := p.parseOperandList()
		if err != nil {
			return nil, err
		}
		if len(elems) > inLimit {
			return nil, inLimitErr(p.kind)
		}
		return cndIn{X: l, List: elems}, nil
	default:
		return nil, p.failTok(nt)
	}
}

func (p *parser) parseOperandList() ([]operand, error) {
	var xs []operand
	for {
		x, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		xs = append(xs, x)
		if p.atSym(",") {
			p.eat()
			continue
		}
		break
	}
	if err := p.expectSym(")"); err != nil {
		return nil, err
	}
	return xs, nil
}

func (p *parser) parseCondFunc(name string) (condNode, error) {
	p.eat() // function word
	p.eat() // '('
	if name == "attribute_exists" || name == "attribute_not_exists" {
		t := p.cur()
		if t.Kind != tkWord && t.Kind != tkNameRef {
			return nil, p.failTok(t)
		}
		pth, err := p.parsePathFrom(t)
		if err != nil {
			return nil, err
		}
		if err := p.expectSym(")"); err != nil {
			return nil, err
		}
		return cndExists{Path: pth, Not: name == "attribute_not_exists"}, nil
	}
	l, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	if err := p.expectSym(","); err != nil {
		return nil, err
	}
	r, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	if err := p.expectSym(")"); err != nil {
		return nil, err
	}
	switch name {
	case "begins_with":
		return cndBegins{L: l, R: r}, nil
	case "contains":
		return cndContains{L: l, R: r}, nil
	default:
		return cndAttrType{L: l, R: r}, nil
	}
}

// parseOperand parses a comparison operand: ":ref" | path | "size(path)".
func (p *parser) parseOperand() (operand, error) {
	t := p.cur()
	switch t.Kind {
	case tkValueRef:
		p.eat()
		p.usedV[t.Text] = struct{}{}
		return valRef{Ref: t.Text}, nil
	case tkWord:
		if foldWord(t) == "size" && p.peekAt(1).Kind == tkSym && p.peekAt(1).Text == "(" {
			p.eat()
			p.eat()
			it := p.cur()
			if it.Kind != tkWord && it.Kind != tkNameRef {
				return nil, p.failTok(it)
			}
			pth, err := p.parsePathFrom(it)
			if err != nil {
				return nil, err
			}
			if err := p.expectSym(")"); err != nil {
				return nil, err
			}
			return sizeRef{Path: pth}, nil
		}
		pth, err := p.parsePathFrom(t)
		if err != nil {
			return nil, err
		}
		return pathRef{Path: pth}, nil
	case tkNameRef:
		pth, err := p.parsePathFrom(t)
		if err != nil {
			return nil, err
		}
		return pathRef{Path: pth}, nil
	default:
		return nil, p.failTok(t)
	}
}

type evalCtx struct {
	values map[string]AV
	item   AV
	kind   ExprKind
}

// evalCond evaluates the boolean structure. Operand errors propagate;
// missing paths simply yield false, exactly like the Java evaluator.
func evalCond(n condNode, ctx *evalCtx) (bool, error) {
	switch x := n.(type) {
	case cndOr:
		for _, c := range x.Xs {
			v, err := evalCond(c, ctx)
			if err != nil {
				return false, err
			}
			if v {
				return true, nil
			}
		}
		return false, nil
	case cndAnd:
		for _, c := range x.Xs {
			v, err := evalCond(c, ctx)
			if err != nil {
				return false, err
			}
			if !v {
				return false, nil
			}
		}
		return true, nil
	case cndNot:
		v, err := evalCond(x.X, ctx)
		if err != nil {
			return false, err
		}
		return !v, nil
	case cndCmp:
		return evalCmp(x.Op, x.L, x.R, ctx)
	case cndBetween:
		return evalBetween(x, ctx)
	case cndIn:
		return evalIn(x, ctx)
	case cndExists:
		_, ok := avGet(ctx.item, x.Path)
		if x.Not {
			return !ok, nil
		}
		return ok, nil
	case cndBegins:
		return evalBegins(x, ctx)
	case cndContains:
		return evalContains(x, ctx)
	case cndAttrType:
		return evalAttrType(x, ctx)
	}
	return false, nil
}

// resolveOperand returns the operand's type, value and whether it exists.
// Values are always found; document paths silently miss.
func resolveOperand(o operand, ctx *evalCtx) (string, AV, bool, error) {
	switch x := o.(type) {
	case valRef:
		v, ok := ctx.values[x.Ref]
		if !ok {
			return "", nil, false, valueNotDefinedErr(x.Ref)
		}
		if err := checkSetValue(v); err != nil {
			return "", nil, false, err
		}
		k := KindOf(v)
		if k == "" {
			return "", nil, false, avBad()
		}
		return k, v, true, nil
	case pathRef:
		v, ok := avGet(ctx.item, x.Path)
		if !ok {
			return "", nil, false, nil
		}
		m, mok := mapAny(v)
		if !mok {
			return "", nil, false, nil
		}
		k := KindOf(m)
		if k == "" {
			return "", nil, false, avBad()
		}
		return k, m, true, nil
	case sizeRef:
		v, ok := avGet(ctx.item, x.Path)
		if !ok {
			return "", nil, false, nil
		}
		m, mok := mapAny(v)
		if !mok {
			return "", nil, false, nil
		}
		n, err := SizeOf(m)
		if err != nil {
			return "", nil, false, operandTypeErr(ctx.kind, "size", KindOf(m))
		}
		return TypeN, AV{TypeN: strconv.Itoa(n)}, true, nil
	}
	return "", nil, false, nil
}

// checkSetValue mirrors the Java rejection of empty sets anywhere a value
// is used.
func checkSetValue(v AV) error {
	switch k := KindOf(v); k {
	case TypeSS, TypeNS, TypeBS:
		e, _ := listElems(v[k])
		if len(e) == 0 {
			return emptySetErr(k)
		}
	}
	return nil
}

func evalCmp(op string, l, r operand, ctx *evalCtx) (bool, error) {
	lk, lv, lok, err := resolveOperand(l, ctx)
	if err != nil {
		return false, err
	}
	rk, rv, rok, err := resolveOperand(r, ctx)
	if err != nil {
		return false, err
	}
	if !lok || !rok {
		return false, nil
	}
	if !scalarComparable(lk) {
		return false, operandTypeErr(ctx.kind, op, lk)
	}
	if !scalarComparable(rk) {
		return false, operandTypeErr(ctx.kind, op, rk)
	}
	if lk != rk {
		return false, nil // scalar type mismatch compares false, no error
	}
	c, err := compareScalars(lk, lv[lk], rv[rk])
	if err != nil {
		return false, err
	}
	switch op {
	case "=":
		return c == 0, nil
	case "<>":
		return c != 0, nil
	case "<":
		return c < 0, nil
	case "<=":
		return c <= 0, nil
	case ">":
		return c > 0, nil
	case ">=":
		return c >= 0, nil
	}
	return false, nil
}

func evalBetween(x cndBetween, ctx *evalCtx) (bool, error) {
	xk, xv, xok, err := resolveOperand(x.X, ctx)
	if err != nil {
		return false, err
	}
	lok2, lov, look, err := resolveOperand(x.Lo, ctx)
	if err != nil {
		return false, err
	}
	hik, hiv, hiok, err := resolveOperand(x.Hi, ctx)
	if err != nil {
		return false, err
	}
	if !xok || !look || !hiok {
		return false, nil
	}
	for _, k := range []string{xk, lok2, hik} {
		if !scalarComparable(k) {
			return false, operandTypeErr(ctx.kind, "BETWEEN", k)
		}
	}
	if xk != lok2 || xk != hik {
		return false, nil
	}
	c1, err := compareScalars(xk, xv[xk], lov[lok2])
	if err != nil {
		return false, err
	}
	c2, err := compareScalars(xk, xv[xk], hiv[hik])
	if err != nil {
		return false, err
	}
	return c1 >= 0 && c2 <= 0, nil
}

func evalIn(x cndIn, ctx *evalCtx) (bool, error) {
	xk, xv, xok, err := resolveOperand(x.X, ctx)
	if err != nil {
		return false, err
	}
	if !xok {
		return false, nil
	}
	if !scalarComparable(xk) {
		return false, operandTypeErr(ctx.kind, "IN", xk)
	}
	for _, e := range x.List {
		ek, ev, eok, err := resolveOperand(e, ctx)
		if err != nil {
			return false, err
		}
		if !eok {
			continue
		}
		if !scalarComparable(ek) {
			return false, operandTypeErr(ctx.kind, "IN", ek)
		}
		if ek != xk {
			continue
		}
		c, err := compareScalars(xk, xv[xk], ev[ek])
		if err != nil {
			return false, err
		}
		if c == 0 {
			return true, nil
		}
	}
	return false, nil
}

func evalBegins(x cndBegins, ctx *evalCtx) (bool, error) {
	lk, lv, lok, err := resolveOperand(x.L, ctx)
	if err != nil {
		return false, err
	}
	rk, rv, rok, err := resolveOperand(x.R, ctx)
	if err != nil {
		return false, err
	}
	if !lok || !rok {
		return false, nil
	}
	if lk == TypeS && rk == TypeS {
		ls, _ := stringVal(lv[TypeS])
		rs, _ := stringVal(rv[TypeS])
		return strings.HasPrefix(ls, rs), nil
	}
	if lk == TypeB && rk == TypeB {
		lb, err := bytesVal(lv[TypeB])
		if err != nil {
			return false, err
		}
		rb, err := bytesVal(rv[TypeB])
		if err != nil {
			return false, err
		}
		return bytes.HasPrefix(lb, rb), nil
	}
	if !scalarComparable(lk) {
		return false, operandTypeErr(ctx.kind, "begins_with", lk)
	}
	return false, operandTypeErr(ctx.kind, "begins_with", rk)
}

func evalContains(x cndContains, ctx *evalCtx) (bool, error) {
	lk, lv, lok, err := resolveOperand(x.L, ctx)
	if err != nil {
		return false, err
	}
	rk, rv, rok, err := resolveOperand(x.R, ctx)
	if err != nil {
		return false, err
	}
	if !lok || !rok {
		return false, nil
	}
	switch lk {
	case TypeS:
		if rk != TypeS {
			return false, operandTypeErr(ctx.kind, "contains", rk)
		}
		ls, _ := stringVal(lv[TypeS])
		rs, _ := stringVal(rv[TypeS])
		return strings.Contains(ls, rs), nil
	case TypeB:
		if rk != TypeB {
			return false, operandTypeErr(ctx.kind, "contains", rk)
		}
		lb, err := bytesVal(lv[TypeB])
		if err != nil {
			return false, err
		}
		rb, err := bytesVal(rv[TypeB])
		if err != nil {
			return false, err
		}
		return bytes.Contains(lb, rb), nil
	case TypeSS, TypeNS, TypeBS:
		if rk != setElemType(lk) {
			return false, operandTypeErr(ctx.kind, "contains", rk)
		}
		return setContains(lk, lv, rv[setElemType(lk)]), nil
	case TypeL:
		le, _ := listElems(lv[TypeL])
		for _, e := range le {
			em, ok := mapAny(e)
			if ok && EqualDeep(em, rv) {
				return true, nil
			}
		}
		return false, nil
	}
	return false, operandTypeErr(ctx.kind, "contains", lk)
}

func setElemType(setKind string) string {
	switch setKind {
	case TypeSS:
		return TypeS
	case TypeNS:
		return TypeN
	case TypeBS:
		return TypeB
	}
	return ""
}

func evalAttrType(x cndAttrType, ctx *evalCtx) (bool, error) {
	lk, _, lok, err := resolveOperand(x.L, ctx)
	if err != nil {
		return false, err
	}
	rk, rv, rok, err := resolveOperand(x.R, ctx)
	if err != nil {
		return false, err
	}
	if !lok || !rok {
		return false, nil
	}
	if rk != TypeS {
		return false, operandTypeErr(ctx.kind, "attribute_type", rk)
	}
	want, _ := stringVal(rv[TypeS])
	return want != "" && lk == want, nil
}
