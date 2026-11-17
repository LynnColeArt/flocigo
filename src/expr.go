package ddb

import (
	"bytes"
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

// Tokenizer ------------------------------------------------------------------

type tokKind int

const (
	tEnd tokKind = iota
	tIdent
	tRef
	tStr
	tNum
	tSym
)

type token struct {
	kind tokKind
	text string
	pos  int
}

var (
	reNum   = regexp.MustCompile(`^[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?`)
	reIdent = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*`)
	reRef   = regexp.MustCompile(`^[:#][A-Za-z0-9_]+`)
)

func syntaxErrToken(kindName string, t token, expr string) *APIError {
	pos := t.pos
	if pos > len(expr) {
		pos = len(expr)
	}
	return validationErr("Invalid %s: Syntax error; token: %q, near: %q", kindName, t.text, strings.TrimSpace(expr[pos:]))
}

func tokenize(kindName, expr string) ([]token, *APIError) {
	var toks []token
	i := 0
	for i < len(expr) {
		c := expr[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '\'' || c == '"':
			q := c
			j := i + 1
			var sb strings.Builder
			closed := false
			for j < len(expr) {
				if expr[j] == q {
					if j+1 < len(expr) && expr[j+1] == q {
						sb.WriteByte(q)
						j += 2
						continue
					}
					closed = true
					j++
					break
				}
				sb.WriteByte(expr[j])
				j++
			}
			if !closed {
				return nil, syntaxErrToken(kindName, token{kind: tStr, text: expr[i:], pos: i}, expr)
			}
			toks = append(toks, token{kind: tStr, text: sb.String(), pos: i})
			i = j
		case c == '(' || c == ')' || c == ',' || c == '.' || c == '[' || c == ']' ||
			c == '+' || c == '-' || c == '=' || c == '<' || c == '>':
			if i+1 < len(expr) {
				two := expr[i : i+2]
				if two == "<>" || two == "<=" || two == ">=" {
					toks = append(toks, token{kind: tSym, text: two, pos: i})
					i += 2
					continue
				}
			}
			toks = append(toks, token{kind: tSym, text: string(c), pos: i})
			i++
		case c >= '0' && c <= '9':
			m := reNum.FindString(expr[i:])
			if m == "" {
				return nil, syntaxErrToken(kindName, token{kind: tSym, text: string(c), pos: i}, expr)
			}
			toks = append(toks, token{kind: tNum, text: m, pos: i})
			i += len(m)
		case c == ':' || c == '#':
			m := reRef.FindString(expr[i:])
			if m == "" {
				return nil, syntaxErrToken(kindName, token{kind: tSym, text: string(c), pos: i}, expr)
			}
			toks = append(toks, token{kind: tRef, text: m, pos: i})
			i += len(m)
		default:
			m := reIdent.FindString(expr[i:])
			if m == "" {
				return nil, syntaxErrToken(kindName, token{kind: tSym, text: string(c), pos: i}, expr)
			}
			toks = append(toks, token{kind: tIdent, text: m, pos: i})
			i += len(m)
		}
	}
	return toks, nil
}

// AST ------------------------------------------------------------------------

type condNode any
type valueNode any

type logicalNode struct {
	op   string // "AND" | "OR"
	kids []condNode
}
type notNode struct{ kid condNode }
type cmpNode struct {
	op       string // = <> < <= > >=
	lhs, rhs valueNode
}
type betweenNode struct{ x, lo, hi valueNode }
type inNode struct {
	x    valueNode
	list []valueNode
}
type predNode struct {
	fn   string
	args []valueNode
}

type litNode struct{ val *AttributeValue }
type refNode struct{ ref string } // ":name"

type pathSeg struct {
	name    string
	ref     bool
	isIndex bool
	index   int
}
type pathNode struct {
	root    string
	rootRef bool
	segs    []pathSeg
}

// pathString renders the path with resolved attribute names.
func (p *pathNode) pathString(names map[string]*string) string {
	var sb strings.Builder
	root := p.root
	if p.rootRef {
		if s, err := lookupName(names, p.root); err == nil {
			root = s
		}
	}
	sb.WriteString(root)
	for _, sg := range p.segs {
		if sg.isIndex {
			sb.WriteString("[")
			sb.WriteString(strconv.Itoa(sg.index))
			sb.WriteString("]")
			continue
		}
		sb.WriteString(".")
		nm := sg.name
		if sg.ref {
			if s, err := lookupName(names, sg.name); err == nil {
				nm = s
			}
		}
		sb.WriteString(nm)
	}
	return sb.String()
}

type sizeNode struct{ arg valueNode }
type arithNode struct {
	op   string // "+" | "-"
	l, r valueNode
}
type ifNotExistsNode struct{ path, def valueNode }
type listAppendNode struct{ l, r valueNode }

// Errors ---------------------------------------------------------------------

func undefinedNameErr(ref string) *APIError {
	return validationErr("An expression attribute name used in document path is not defined; attribute: %s", ref)
}

func undefinedValueErr(ref string) *APIError {
	return validationErr("An expression attribute value used in expression is not defined; attribute: %s", ref)
}

func lookupName(names map[string]*string, ref string) (string, *APIError) {
	if names != nil {
		if s, ok := names[ref]; ok && s != nil {
			return *s, nil
		}
	}
	return "", undefinedNameErr(ref)
}

// Parser ---------------------------------------------------------------------

type condParser struct {
	toks     []token
	pos      int
	expr     string
	kindName string
	names    map[string]*string
	values   map[string]*AttributeValue
}

func (p *condParser) peek() token {
	if p.pos < len(p.toks) {
		return p.toks[p.pos]
	}
	return token{kind: tEnd, pos: len(p.expr)}
}

func (p *condParser) next() token {
	t := p.peek()
	if t.kind != tEnd {
		p.pos++
	}
	return t
}

func (p *condParser) peekKw(kw string) bool {
	t := p.peek()
	return t.kind == tIdent && strings.EqualFold(t.text, kw)
}

func (p *condParser) nextIfSym(sym string) bool {
	t := p.peek()
	if t.kind == tSym && t.text == sym {
		p.next()
		return true
	}
	return false
}

func (p *condParser) expectSym(sym string) *APIError {
	if !p.nextIfSym(sym) {
		return p.syntaxErrAt(p.peek())
	}
	return nil
}

func (p *condParser) eofErr() *APIError {
	return validationErr("Invalid %s: Syntax error; unexpected end of expression", p.kindName)
}

func (p *condParser) syntaxErrAt(t token) *APIError {
	if t.kind == tEnd {
		return p.eofErr()
	}
	pos := t.pos
	if pos > len(p.expr) {
		pos = len(p.expr)
	}
	return validationErr("Invalid %s: Syntax error; token: %q, near: %q", p.kindName, t.text, strings.TrimSpace(p.expr[pos:]))
}

func (p *condParser) parseTop() (condNode, *APIError) {
	if p.peek().kind == tEnd {
		return nil, p.eofErr()
	}
	n, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if t := p.peek(); t.kind != tEnd {
		return nil, p.syntaxErrAt(t)
	}
	return n, nil
}

func (p *condParser) parseOr() (condNode, *APIError) {
	n, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.peekKw("OR") {
		p.next()
		r, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		if lg, ok := n.(*logicalNode); ok && lg.op == "OR" {
			lg.kids = append(lg.kids, r)
		} else {
			n = &logicalNode{op: "OR", kids: []condNode{n, r}}
		}
	}
	return n, nil
}

func (p *condParser) parseAnd() (condNode, *APIError) {
	n, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.peekKw("AND") {
		p.next()
		r, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		if lg, ok := n.(*logicalNode); ok && lg.op == "AND" {
			lg.kids = append(lg.kids, r)
		} else {
			n = &logicalNode{op: "AND", kids: []condNode{n, r}}
		}
	}
	return n, nil
}

// groupIsBoolean looks ahead to the matching closing paren and reports
// whether the parenthesised group holds a boolean expression (as opposed to
// an arithmetic value used as a comparison operand).
func (p *condParser) groupIsBoolean() bool {
	depth := 0
	for i := p.pos; i < len(p.toks); i++ {
		t := p.toks[i]
		if t.kind == tSym {
			switch t.text {
			case "(", "[":
				depth++
			case ")", "]":
				depth--
				if depth == 0 {
					return false
				}
			case "=", "<>", "<", "<=", ">", ">=":
				return true
			}
		} else if t.kind == tIdent && depth >= 1 {
			switch strings.ToLower(t.text) {
			case "and", "or", "not", "between", "in",
				"attribute_exists", "attribute_not_exists",
				"attribute_type", "begins_with", "contains":
				return true
			}
		}
	}
	return false
}

func (p *condParser) parseNot() (condNode, *APIError) {
	t := p.peek()
	if t.kind == tSym && t.text == "(" && p.groupIsBoolean() {
		p.next()
		n, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if !p.nextIfSym(")") {
			return nil, p.syntaxErrAt(p.peek())
		}
		return n, nil
	}
	if p.peekKw("NOT") {
		p.next()
		kid, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &notNode{kid: kid}, nil
	}
	return p.parseComparison()
}

func (p *condParser) parseComparison() (condNode, *APIError) {
	lhs, err := p.parseArith()
	if err != nil {
		return nil, err
	}
	t := p.peek()
	if t.kind == tSym {
		switch t.text {
		case "=", "<>", "<", "<=", ">", ">=":
			p.next()
			rhs, err := p.parseArith()
			if err != nil {
				return nil, err
			}
			return &cmpNode{op: t.text, lhs: lhs, rhs: rhs}, nil
		}
	}
	if p.peekKw("BETWEEN") {
		p.next()
		lo, err := p.parseArith()
		if err != nil {
			return nil, err
		}
		if !p.peekKw("AND") {
			return nil, p.syntaxErrAt(p.peek())
		}
		p.next()
		hi, err := p.parseArith()
		if err != nil {
			return nil, err
		}
		return &betweenNode{x: lhs, lo: lo, hi: hi}, nil
	}
	if p.peekKw("IN") {
		p.next()
		if !p.nextIfSym("(") {
			return nil, p.syntaxErrAt(p.peek())
		}
		var list []valueNode
		for {
			e, err := p.parseArith()
			if err != nil {
				return nil, err
			}
			list = append(list, e)
			if !p.nextIfSym(",") {
				break
			}
		}
		if !p.nextIfSym(")") {
			return nil, p.syntaxErrAt(p.peek())
		}
		return &inNode{x: lhs, list: list}, nil
	}
	return lhs, nil
}

func (p *condParser) parseArith() (valueNode, *APIError) {
	l, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind == tSym && (t.text == "+" || t.text == "-") {
			p.next()
			r, err := p.parseOperand()
			if err != nil {
				return nil, err
			}
			l = &arithNode{op: t.text, l: l, r: r}
			continue
		}
		return l, nil
	}
}

func (p *condParser) parseOperand() (valueNode, *APIError) {
	t := p.peek()
	switch t.kind {
	case tEnd:
		return nil, p.eofErr()
	case tStr:
		p.next()
		return &litNode{val: &AttributeValue{S: t.text}}, nil
	case tNum:
		p.next()
		return &litNode{val: aN(t.text)}, nil
	case tSym:
		if t.text == "(" {
			p.next()
			n, err := p.parseArith()
			if err != nil {
				return nil, err
			}
			if !p.nextIfSym(")") {
				return nil, p.syntaxErrAt(p.peek())
			}
			return n, nil
		}
		return nil, p.syntaxErrAt(t)
	case tRef:
		if strings.HasPrefix(t.text, ":") {
			if _, ok := p.values[t.text]; !ok {
				return nil, undefinedValueErr(t.text)
			}
			p.next()
			return &refNode{ref: t.text}, nil
		}
		return p.parsePath()
	case tIdent:
		if p.kindName == "ConditionExpression" {
			lw := strings.ToLower(t.text)
			switch lw {
			case "size", "attribute_exists", "attribute_not_exists", "attribute_type", "begins_with", "contains":
				return p.parseCondFunc(lw)
			}
		}
		return p.parsePath()
	}
	return nil, p.syntaxErrAt(t)
}

func isPathLike(n valueNode) bool {
	switch t := n.(type) {
	case *pathNode:
		return true
	case *refNode:
		return strings.HasPrefix(t.ref, "#")
	}
	return false
}

func (p *condParser) parseCondFunc(fn string) (valueNode, *APIError) {
	p.next() // the function name
	if !p.nextIfSym("(") {
		return nil, p.syntaxErrAt(p.peek())
	}
	switch fn {
	case "size":
		arg, err := p.parseArith()
		if err != nil {
			return nil, err
		}
		if !p.nextIfSym(")") {
			return nil, p.syntaxErrAt(p.peek())
		}
		return &sizeNode{arg: arg}, nil
	case "attribute_exists", "attribute_not_exists":
		arg, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		if !p.nextIfSym(")") {
			return nil, p.syntaxErrAt(p.peek())
		}
		if !isPathLike(arg) {
			return nil, validationErr("Invalid ConditionExpression: %s requires a document path", fn)
		}
		return &predNode{fn: fn, args: []valueNode{arg}}, nil
	default:
		a1, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		if !isPathLike(a1) {
			return nil, validationErr("Invalid ConditionExpression: %s requires a document path as its first argument", fn)
		}
		if !p.nextIfSym(",") {
			return nil, p.syntaxErrAt(p.peek())
		}
		a2, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		if !p.nextIfSym(")") {
			return nil, p.syntaxErrAt(p.peek())
		}
		return &predNode{fn: fn, args: []valueNode{a1, a2}}, nil
	}
}

func (p *condParser) parsePath() (valueNode, *APIError) {
	t := p.next()
	var pn pathNode
	switch t.kind {
	case tIdent:
		pn.root = t.text
	case tRef:
		if !strings.HasPrefix(t.text, "#") {
			return nil, p.syntaxErrAt(t)
		}
		if _, err := lookupName(p.names, t.text); err != nil {
			return nil, err
		}
		pn.root = t.text
		pn.rootRef = true
	default:
		return nil, p.syntaxErrAt(t)
	}
	for {
		t2 := p.peek()
		if t2.kind == tSym && t2.text == "." {
			p.next()
			t3 := p.next()
			switch t3.kind {
			case tIdent:
				pn.segs = append(pn.segs, pathSeg{name: t3.text})
			case tRef:
				if !strings.HasPrefix(t3.text, "#") {
					return nil, p.syntaxErrAt(t3)
				}
				if _, err := lookupName(p.names, t3.text); err != nil {
					return nil, err
				}
				pn.segs = append(pn.segs, pathSeg{name: t3.text, ref: true})
			default:
				return nil, p.syntaxErrAt(t3)
			}
		} else if t2.kind == tSym && t2.text == "[" {
			p.next()
			t3 := p.next()
			if t3.kind != tNum || strings.ContainsAny(t3.text, ".eE") {
				return nil, p.syntaxErrAt(t3)
			}
			idx, aerr := strconv.Atoi(t3.text)
			if aerr != nil {
				return nil, p.syntaxErrAt(t3)
			}
			if !p.nextIfSym("]") {
				return nil, p.syntaxErrAt(p.peek())
			}
			pn.segs = append(pn.segs, pathSeg{isIndex: true, index: idx})
		} else {
			break
		}
	}
	return &pn, nil
}

// Condition compilation and evaluation ----------------------------------------

// compiledCond is a parsed ConditionExpression bound to its operand maps.
type compiledCond struct {
	names  map[string]*string
	values map[string]*AttributeValue
	ast    condNode
}

func compileCondition(kindName, expr string, names map[string]*string, values map[string]*AttributeValue) (*compiledCond, *APIError) {
	if strings.TrimSpace(expr) == "" {
		return nil, nil
	}
	toks, err := tokenize(kindName, expr)
	if err != nil {
		return nil, err
	}
	p := &condParser{toks: toks, expr: expr, kindName: kindName, names: names, values: values}
	ast, err := p.parseTop()
	if err != nil {
		return nil, err
	}
	return &compiledCond{names: names, values: values, ast: ast}, nil
}

// eval evaluates the condition against an item snapshot (which may be nil
// when the item does not exist).
func (c *compiledCond) eval(item map[string]*AttributeValue) (bool, *APIError) {
	e := &evaluator{item: item, names: c.names, values: c.values, kind: "ConditionExpression"}
	return e.eval(c.ast)
}

type evaluator struct {
	item   map[string]*AttributeValue
	names  map[string]*string
	values map[string]*AttributeValue
	kind   string
}

func (e *evaluator) eval(n condNode) (bool, *APIError) {
	switch t := n.(type) {
	case *logicalNode:
		for _, kid := range t.kids {
			b, err := e.eval(kid)
			if err != nil {
				return false, err
			}
			if t.op == "AND" && !b {
				return false, nil
			}
			if t.op == "OR" && b {
				return true, nil
			}
		}
		return t.op == "AND", nil
	case *notNode:
		b, err := e.eval(t.kid)
		if err != nil {
			return false, err
		}
		return !b, nil
	case *cmpNode:
		lv, lok, err := e.value(t.lhs)
		if err != nil {
			return false, err
		}
		rv, rok, err := e.value(t.rhs)
		if err != nil {
			return false, err
		}
		if !lok || !rok {
			return false, nil
		}
		switch t.op {
		case "=":
			return valuesEqual(lv, rv), nil
		case "<>":
			return !valuesEqual(lv, rv), nil
		}
		c, ok := compareValues(lv, rv)
		if !ok {
			return false, nil
		}
		switch t.op {
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
	case *betweenNode:
		x, xok, err := e.value(t.x)
		if err != nil {
			return false, err
		}
		lo, lok, err := e.value(t.lo)
		if err != nil {
			return false, err
		}
		hi, hok, err := e.value(t.hi)
		if err != nil {
			return false, err
		}
		if !xok || !lok || !hok {
			return false, nil
		}
		c1, ok1 := compareValues(x, lo)
		c2, ok2 := compareValues(x, hi)
		if !ok1 || !ok2 {
			return false, nil
		}
		return c1 >= 0 && c2 <= 0, nil
	case *inNode:
		x, xok, err := e.value(t.x)
		if err != nil {
			return false, err
		}
		if !xok {
			return false, nil
		}
		for _, it := range t.list {
			v, ok, err := e.value(it)
			if err != nil {
				return false, err
			}
			if ok && valuesEqual(x, v) {
				return true, nil
			}
		}
		return false, nil
	case *predNode:
		return e.evalPred(t)
	case *litNode, *refNode, *pathNode, *sizeNode, *arithNode, *ifNotExistsNode, *listAppendNode:
		return false, validationErr("Invalid %s: expected a condition expression, got a value", e.kind)
	}
	return false, validationErr("Invalid %s: unable to evaluate expression", e.kind)
}

func (e *evaluator) evalPred(t *predNode) (bool, *APIError) {
	switch t.fn {
	case "attribute_exists", "attribute_not_exists":
		_, ok, err := e.pathValue(t.args[0])
		if err != nil {
			return false, err
		}
		if t.fn == "attribute_exists" {
			return ok, nil
		}
		return !ok, nil
	case "attribute_type":
		v, ok, err := e.value(t.args[0])
		if err != nil {
			return false, err
		}
		tv, tok, err := e.value(t.args[1])
		if err != nil {
			return false, err
		}
		if !ok || !tok || tv.TypeName() != "S" {
			return false, nil
		}
		return v.TypeName() == tv.S, nil
	case "begins_with":
		v, ok, err := e.value(t.args[0])
		if err != nil {
			return false, err
		}
		p, pok, err := e.value(t.args[1])
		if err != nil {
			return false, err
		}
		if !ok || !pok || v.TypeName() != "S" || p.TypeName() != "S" {
			return false, nil
		}
		return strings.HasPrefix(v.S, p.S), nil
	case "contains":
		v, ok, err := e.value(t.args[0])
		if err != nil {
			return false, err
		}
		arg, aok, err := e.value(t.args[1])
		if err != nil {
			return false, err
		}
		if !ok || !aok {
			return false, nil
		}
		return containsValue(v, arg), nil
	}
	return false, validationErr("Invalid %s: unsupported function %s", e.kind, t.fn)
}

func containsValue(v, arg *AttributeValue) bool {
	vt, at := v.TypeName(), arg.TypeName()
	switch {
	case vt == "S" && at == "S":
		return strings.Contains(v.S, arg.S)
	case vt == "SS" && at == "S":
		for _, s := range v.SS {
			if s == arg.S {
				return true
			}
		}
	case vt == "NS" && at == "N":
		for _, n := range v.NS {
			if c, ok := compareNumbers(n, arg.N); ok && c == 0 {
				return true
			}
		}
	case vt == "BS" && at == "B":
		for _, b := range v.BS {
			if bytes.Equal(b, arg.B) {
				return true
			}
		}
	case vt == "L":
		for _, x := range v.L {
			if valuesEqual(x, arg) {
				return true
			}
		}
	}
	return false
}

func (e *evaluator) pathValue(n valueNode) (*AttributeValue, bool, *APIError) {
	if pn, ok := n.(*pathNode); ok {
		return e.path(pn)
	}
	return nil, false, validationErr("Invalid %s: expected a document path", e.kind)
}

// path resolves a document path; a missing attribute is reported as
// (nil, false, nil) while a structurally invalid path is an error.
func (e *evaluator) path(p *pathNode) (*AttributeValue, bool, *APIError) {
	var cur *AttributeValue
	if p.rootRef {
		nm, err := lookupName(e.names, p.root)
		if err != nil {
			return nil, false, err
		}
		cur, _ = e.item[nm]
	} else {
		cur, _ = e.item[p.root]
	}
	for _, sg := range p.segs {
		if cur == nil {
			return nil, false, nil
		}
		if sg.isIndex {
			if cur.TypeName() != "L" {
				return nil, false, validationErr("The document path provided in the expression is invalid")
			}
			if sg.index < 0 || sg.index >= len(cur.L) {
				return nil, false, nil
			}
			cur = cur.L[sg.index]
			continue
		}
		nm := sg.name
		if sg.ref {
			dn, err := lookupName(e.names, sg.name)
			if err != nil {
				return nil, false, err
			}
			nm = dn
		}
		if cur.TypeName() != "M" {
			return nil, false, validationErr("The document path provided in the expression is invalid")
		}
		next, ok := cur.M[nm]
		if !ok {
			return nil, false, nil
		}
		cur = next
	}
	if cur == nil {
		return nil, false, nil
	}
	return cur, true, nil
}

// value resolves one value operand. The boolean reports whether the value
// exists (paths may be missing).
func (e *evaluator) value(n valueNode) (*AttributeValue, bool, *APIError) {
	switch t := n.(type) {
	case *litNode:
		return t.val, true, nil
	case *refNode:
		v := e.values[t.ref]
		if v == nil {
			return nil, false, undefinedValueErr(t.ref)
		}
		return cloneValue(v), true, nil
	case *pathNode:
		return e.path(t)
	case *sizeNode:
		v, ok, err := e.value(t.arg)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, validationErr(msgMissingAttr)
		}
		return aN(strconv.Itoa(sizeOf(v))), true, nil
	case *arithNode:
		l, lok, err := e.value(t.l)
		if err != nil {
			return nil, false, err
		}
		r, rok, err := e.value(t.r)
		if err != nil {
			return nil, false, err
		}
		if !lok || !rok {
			return nil, false, validationErr(msgMissingAttr)
		}
		if l.TypeName() != "N" || r.TypeName() != "N" {
			return nil, false, validationErr("Operand for arithmetic is not a number")
		}
		lr, ok1 := parseRat(l.N)
		rr, ok2 := parseRat(r.N)
		if !ok1 || !ok2 {
			return nil, false, validationErr("Operand for arithmetic is not a number")
		}
		sum := new(big.Rat)
		if t.op == "+" {
			sum.Add(lr, rr)
		} else {
			sum.Sub(lr, rr)
		}
		return aN(FormatRat(sum)), true, nil
	case *ifNotExistsNode:
		v, ok, err := e.value(t.path)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return v, true, nil
		}
		return e.value(t.def)
	case *listAppendNode:
		l, lok, err := e.value(t.l)
		if err != nil {
			return nil, false, err
		}
		r, rok, err := e.value(t.r)
		if err != nil {
			return nil, false, err
		}
		if !lok || !rok {
			return nil, false, validationErr(msgMissingAttr)
		}
		if l.TypeName() != "L" || r.TypeName() != "L" {
			return nil, false, validationErr("Invalid ListAppend usage: operand is not a list")
		}
		nl := &AttributeValue{L: make([]*AttributeValue, 0, len(l.L)+len(r.L))}
		for _, x := range l.L {
			nl.L = append(nl.L, cloneValue(x))
		}
		for _, x := range r.L {
			nl.L = append(nl.L, cloneValue(x))
		}
		return nl, true, nil
	}
	return nil, false, validationErr("Invalid %s: expected a value operand", e.kind)
}

func sizeOf(v *AttributeValue) int {
	switch v.TypeName() {
	case "S":
		return len(v.S)
	case "N":
		return len(canonicalNumber(v.N))
	case "B":
		return len(v.B)
	case "SS":
		return len(v.SS)
	case "NS":
		return len(v.NS)
	case "BS":
		return len(v.BS)
	case "L":
		return len(v.L)
	case "M":
		return len(v.M)
	}
	return 0
}
