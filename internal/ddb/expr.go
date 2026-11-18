package ddb

import (
	"bytes"
	"strconv"
	"strings"
)

// ---------- tokens ----------

type tokKind int

const (
	tkEOF tokKind = iota
	tkName
	tkValue
	tkIdent
	tkOp
	tkLParen
	tkRParen
	tkComma
	tkDot
	tkLBracket
	tkRBracket
	tkPlus
	tkMinus
)

type token struct {
	kind tokKind
	text string
}

func isIdentChar(c byte) bool {
	return c == '_' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func syntaxErrorf(label, tokText string) *apiErr {
	return ddbErrorf("ValidationException", "Invalid %s: Syntax error; token: %q", label, tokText)
}

func tokenizeExpr(expr string, label string) ([]token, *apiErr) {
	var toks []token
	i := 0
	for i < len(expr) {
		c := expr[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '#', c == ':':
			i++
			start := i
			for i < len(expr) && isIdentChar(expr[i]) {
				i++
			}
			if i == start {
				return nil, syntaxErrorf(label, string(c))
			}
			k := tkName
			if c == ':' {
				k = tkValue
			}
			toks = append(toks, token{k, expr[start:i]})
		case isIdentChar(c):
			start := i
			for i < len(expr) && isIdentChar(expr[i]) {
				i++
			}
			toks = append(toks, token{tkIdent, expr[start:i]})
		case c == '(':
			toks = append(toks, token{tkLParen, "("})
			i++
		case c == ')':
			toks = append(toks, token{tkRParen, ")"})
			i++
		case c == ',':
			toks = append(toks, token{tkComma, ","})
			i++
		case c == '.':
			toks = append(toks, token{tkDot, "."})
			i++
		case c == '[':
			toks = append(toks, token{tkLBracket, "["})
			i++
		case c == ']':
			toks = append(toks, token{tkRBracket, "]"})
			i++
		case c == '+':
			toks = append(toks, token{tkPlus, "+"})
			i++
		case c == '-':
			toks = append(toks, token{tkMinus, "-"})
			i++
		case c == '=':
			toks = append(toks, token{tkOp, "="})
			i++
		case c == '<':
			if i+1 < len(expr) && expr[i+1] == '=' {
				toks = append(toks, token{tkOp, "<="})
				i += 2
			} else if i+1 < len(expr) && expr[i+1] == '>' {
				toks = append(toks, token{tkOp, "<>"})
				i += 2
			} else {
				toks = append(toks, token{tkOp, "<"})
				i++
			}
		case c == '>':
			if i+1 < len(expr) && expr[i+1] == '=' {
				toks = append(toks, token{tkOp, ">="})
				i += 2
			} else {
				toks = append(toks, token{tkOp, ">"})
				i++
			}
		default:
			return nil, syntaxErrorf(label, string(c))
		}
	}
	toks = append(toks, token{tkEOF, ""})
	return toks, nil
}

// ---------- expression context ----------

type exprCtx struct {
	names  map[string]string
	values map[string]map[string]any
	label  string
}

func newExprCtx(namesAny, valuesAny any, label string) (*exprCtx, *apiErr) {
	c := &exprCtx{
		names:  map[string]string{},
		values: map[string]map[string]any{},
		label:  label,
	}
	if nm, ok := namesAny.(map[string]any); ok && nm != nil {
		for k, v := range nm {
			s, ok2 := v.(string)
			if !ok2 {
				return nil, ddbErrorf("ValidationException", "One or more parameter values were invalid: Invalid expression attribute name: %s", k)
			}
			c.names[k] = s
		}
	}
	if vm, ok := valuesAny.(map[string]any); ok && vm != nil {
		for k, v := range vm {
			a, ok2 := toAttr(v)
			if !ok2 {
				return nil, ddbErrorf("ValidationException", "One or more parameter values were invalid: Invalid expression attribute value: %s", k)
			}
			c.values[k] = a
		}
	}
	return c, nil
}

func (c *exprCtx) undefinedName(t token) *apiErr {
	return ddbErrorf("ValidationException", "Invalid %s: An expression attribute name used in the %s is not defined; attribute name: %s", c.label, c.label, t.text)
}

func (c *exprCtx) undefinedValue(t token) *apiErr {
	return ddbErrorf("ValidationException", "Invalid %s: An expression attribute value used in the %s is not defined; attribute value: %s", c.label, c.label, t.text)
}

// ---------- paths ----------

type pathPart struct {
	name    string
	index   int
	isIndex bool
}

func (c *exprCtx) resolveNameToken(t token) (string, *apiErr) {
	if t.kind == tkName {
		s, ok := c.names[t.text]
		if !ok {
			return "", c.undefinedName(t)
		}
		return s, nil
	}
	return t.text, nil
}

// parsePathFromText parses a substituted attribute path string.
func (c *exprCtx) parsePathFromText(s string) ([]pathPart, *apiErr) {
	if s == "" {
		return nil, syntaxErrorf(c.label, s)
	}
	var parts []pathPart
	i := 0
	for {
		start := i
		for i < len(s) && isIdentChar(s[i]) {
			i++
		}
		if i == start {
			return nil, syntaxErrorf(c.label, string(s[i]))
		}
		name := s[start:i]
		parts = append(parts, pathPart{name: name})
		for i < len(s) && s[i] == '[' {
			j := i + 1
			dstart := j
			for j < len(s) && s[j] >= '0' && s[j] <= '9' {
				j++
			}
			if j == dstart || j >= len(s) || s[j] != ']' {
				return nil, syntaxErrorf(c.label, "[")
			}
			idx, err := strconv.Atoi(s[dstart:j])
			if err != nil {
				return nil, syntaxErrorf(c.label, s[dstart:j])
			}
			parts = append(parts, pathPart{index: idx, isIndex: true})
			i = j + 1
		}
		if i >= len(s) {
			break
		}
		if s[i] == '.' {
			i++
			if i >= len(s) {
				return nil, syntaxErrorf(c.label, ".")
			}
			continue
		}
		return nil, syntaxErrorf(c.label, string(s[i]))
	}
	return parts, nil
}

// parsePathTok parses a path starting at toks[*i] (ident or name token).
func (c *exprCtx) parsePathTok(toks []token, i *int) ([]pathPart, *apiErr) {
	t := toks[*i]
	resolved, aerr := c.resolveNameToken(t)
	if aerr != nil {
		return nil, aerr
	}
	parts, aerr := c.parsePathFromText(resolved)
	if aerr != nil {
		return nil, aerr
	}
	*i++
	for *i < len(toks) {
		switch toks[*i].kind {
		case tkDot:
			dot := toks[*i]
			*i++
			if *i >= len(toks) || (toks[*i].kind != tkIdent && toks[*i].kind != tkName) {
				return nil, syntaxErrorf(c.label, dot.text)
			}
			sub, aerr := c.resolveNameToken(toks[*i])
			if aerr != nil {
				return nil, aerr
			}
			more, aerr := c.parsePathFromText(sub)
			if aerr != nil {
				return nil, aerr
			}
			parts = append(parts, more...)
			*i++
		case tkLBracket:
			*i++
			if *i >= len(toks) || toks[*i].kind != tkIdent {
				return nil, syntaxErrorf(c.label, "[")
			}
			it := toks[*i]
			idx, err := strconv.Atoi(it.text)
			if err != nil || idx < 0 {
				return nil, syntaxErrorf(c.label, it.text)
			}
			*i++
			if *i >= len(toks) || toks[*i].kind != tkRBracket {
				return nil, syntaxErrorf(c.label, "]")
			}
			*i++
			parts = append(parts, pathPart{index: idx, isIndex: true})
		default:
			return parts, nil
		}
	}
	return parts, nil
}

// ---------- item path helpers ----------

func missingExprErr() *apiErr {
	return ddbError("ValidationException", "The provided expression refers to an attribute that does not exist in the item")
}

func getPathValue(item map[string]any, parts []pathPart) (any, bool) {
	var cur any = item
	for _, p := range parts {
		if p.isIndex {
			lst, ok := cur.([]any)
			if !ok || p.index < 0 || p.index >= len(lst) {
				return nil, false
			}
			cur = lst[p.index]
		} else {
			m, ok := cur.(map[string]any)
			if !ok {
				return nil, false
			}
			v, ok := m[p.name]
			if !ok {
				return nil, false
			}
			cur = v
		}
	}
	return cur, true
}

func setPathValue(item map[string]any, parts []pathPart, val any, create bool) *apiErr {
	if len(parts) == 0 {
		return missingExprErr()
	}
	var cur any = item
	for k := 0; k < len(parts)-1; k++ {
		p := parts[k]
		if p.isIndex {
			lst, ok := cur.([]any)
			if !ok || p.index < 0 || p.index >= len(lst) {
				return missingExprErr()
			}
			cur = lst[p.index]
		} else {
			m, ok := cur.(map[string]any)
			if !ok {
				return missingExprErr()
			}
			child, ok := m[p.name]
			if !ok {
				if create {
					child = map[string]any{}
					m[p.name] = child
				} else {
					return missingExprErr()
				}
			}
			cur = child
		}
	}
	last := parts[len(parts)-1]
	if last.isIndex {
		lst, ok := cur.([]any)
		if !ok || last.index < 0 || last.index >= len(lst) {
			return missingExprErr()
		}
		lst[last.index] = val
	} else {
		m, ok := cur.(map[string]any)
		if !ok {
			return missingExprErr()
		}
		m[last.name] = val
	}
	return nil
}

func removePathValue(item map[string]any, parts []pathPart) {
	if len(parts) == 0 {
		return
	}
	if len(parts) == 1 {
		if parts[0].isIndex {
			return
		}
		delete(item, parts[0].name)
		return
	}
	parent, ok := getPathValue(item, parts[:len(parts)-1])
	if !ok {
		return
	}
	last := parts[len(parts)-1]
	switch pv := parent.(type) {
	case map[string]any:
		if last.isIndex {
			return
		}
		delete(pv, last.name)
	case []any:
		if !last.isIndex || last.index < 0 || last.index >= len(pv) {
			return
		}
		out := make([]any, 0, len(pv)-1)
		out = append(out, pv[:last.index]...)
		out = append(out, pv[last.index+1:]...)
		_ = setPathValue(item, parts[:len(parts)-1], out, false)
	default:
	}
}

// pathsOverlap reports whether two paths share a common prefix.
func pathsOverlap(a, b []pathPart) bool {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i].isIndex != b[i].isIndex {
			return false
		}
		if a[i].isIndex {
			if a[i].index != b[i].index {
				return false
			}
		} else if a[i].name != b[i].name {
			return false
		}
	}
	return true
}

// nestAny wraps a value in the containers described by parts.
func nestAny(parts []pathPart, v any) any {
	if len(parts) == 0 {
		return v
	}
	p := parts[0]
	inner := nestAny(parts[1:], v)
	if p.isIndex {
		lst := make([]any, p.index+1)
		lst[p.index] = inner
		return lst
	}
	return map[string]any{p.name: inner}
}

// mergeNested deep-merges freshly built nested values into dst.
func mergeNested(dst, src any) any {
	if sm, ok := src.(map[string]any); ok {
		dm, ok2 := dst.(map[string]any)
		if !ok2 {
			return sm
		}
		for k, v := range sm {
			dm[k] = mergeNested(dm[k], v)
		}
		return dm
	}
	if sl, ok := src.([]any); ok {
		dl, ok2 := dst.([]any)
		if !ok2 {
			return sl
		}
		n := max(len(dl), len(sl))
		out := make([]any, n)
		for i := 0; i < n; i++ {
			switch {
			case i >= len(dl):
				out[i] = sl[i]
			case i >= len(sl):
				out[i] = dl[i]
			default:
				out[i] = mergeNested(dl[i], sl[i])
			}
		}
		return out
	}
	return src
}

// projectItem extracts the requested paths from an item.
func projectItem(item map[string]any, paths [][]pathPart) map[string]any {
	out := map[string]any{}
	for _, p := range paths {
		v, ok := getPathValue(item, p)
		if !ok {
			continue
		}
		m, ok2 := v.(map[string]any)
		if !ok2 {
			continue
		}
		mergeNested(out, nestAny(p, m))
	}
	return out
}

// ---------- operands & conditions ----------

type operand interface {
	get(item map[string]any) (map[string]any, bool, *apiErr)
}

type valOperand struct{ av map[string]any }

func (o valOperand) get(item map[string]any) (map[string]any, bool, *apiErr) {
	return o.av, true, nil
}

type pathOperand struct{ parts []pathPart }

func (o pathOperand) get(item map[string]any) (map[string]any, bool, *apiErr) {
	v, ok := getPathValue(item, o.parts)
	if !ok {
		return nil, false, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, false, nil
	}
	return m, true, nil
}

type sizeOperand struct {
	parts []pathPart
	label string
}

func (o *sizeOperand) get(item map[string]any) (map[string]any, bool, *apiErr) {
	v, ok := getPathValue(item, o.parts)
	if !ok {
		return nil, false, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, false, nil
	}
	var n int
	switch t := attrTypeOf(m); t {
	case "S":
		n = len(attrString(m))
	case "B":
		n = len(attrBytes(m))
	case "L":
		lst, _ := m["L"].([]any)
		n = len(lst)
	case "SS", "NS", "BS":
		lst, _ := m[t].([]any)
		n = len(lst)
	case "M":
		mm, _ := m["M"].(map[string]any)
		n = len(mm)
	default:
		return nil, false, ddbErrorf("ValidationException", "Invalid %s: Incorrect operand type for operator or function; operator or function: size, operand type: %s", o.label, t)
	}
	return map[string]any{"N": strconv.Itoa(n)}, true, nil
}

type condNode interface {
	eval(item map[string]any) (bool, *apiErr)
}

type condAnd struct{ subs []condNode }

func (n *condAnd) eval(item map[string]any) (bool, *apiErr) {
	for _, s := range n.subs {
		v, aerr := s.eval(item)
		if aerr != nil {
			return false, aerr
		}
		if !v {
			return false, nil
		}
	}
	return true, nil
}

type condOr struct{ subs []condNode }

func (n *condOr) eval(item map[string]any) (bool, *apiErr) {
	for _, s := range n.subs {
		v, aerr := s.eval(item)
		if aerr != nil {
			return false, aerr
		}
		if v {
			return true, nil
		}
	}
	return false, nil
}

type condNot struct{ sub condNode }

func (n *condNot) eval(item map[string]any) (bool, *apiErr) {
	v, aerr := n.sub.eval(item)
	if aerr != nil {
		return false, aerr
	}
	return !v, nil
}

type condCmp struct {
	lhs operand
	op  string
	rhs operand
}

func (n *condCmp) eval(item map[string]any) (bool, *apiErr) {
	la, lok, aerr := n.lhs.get(item)
	if aerr != nil {
		return false, aerr
	}
	ra, rok, aerr := n.rhs.get(item)
	if aerr != nil {
		return false, aerr
	}
	if !lok || !rok {
		return false, nil
	}
	switch n.op {
	case "=":
		return attrEq(la, ra), nil
	case "<>":
		return !attrEq(la, ra), nil
	}
	switch attrTypeOf(la) {
	case "S", "N", "B":
		if attrTypeOf(ra) != attrTypeOf(la) {
			return false, nil
		}
		switch compareAttr(la, ra) {
		case -1:
			return n.op == "<" || n.op == "<=", nil
		case 0:
			return n.op == "<=" || n.op == ">=", nil
		case 1:
			return n.op == ">" || n.op == ">=", nil
		}
	}
	return false, nil
}

type condBetween struct {
	val operand
	lo  operand
	hi  operand
}

func (n *condBetween) eval(item map[string]any) (bool, *apiErr) {
	v, ok, aerr := n.val.get(item)
	if aerr != nil {
		return false, aerr
	}
	lo, ok2, aerr := n.lo.get(item)
	if aerr != nil {
		return false, aerr
	}
	hi, ok3, aerr := n.hi.get(item)
	if aerr != nil {
		return false, aerr
	}
	if !ok || !ok2 || !ok3 {
		return false, nil
	}
	t := attrTypeOf(v)
	if (t != "S" && t != "N" && t != "B") || attrTypeOf(lo) != t || attrTypeOf(hi) != t {
		return false, nil
	}
	return compareAttr(v, lo) >= 0 && compareAttr(v, hi) <= 0, nil
}

type condIn struct {
	lhs  operand
	opts []operand
}

func (n *condIn) eval(item map[string]any) (bool, *apiErr) {
	la, lok, aerr := n.lhs.get(item)
	if aerr != nil {
		return false, aerr
	}
	if !lok {
		return false, nil
	}
	for _, o := range n.opts {
		oa, ok, aerr := o.get(item)
		if aerr != nil {
			return false, aerr
		}
		if ok && attrEq(la, oa) {
			return true, nil
		}
	}
	return false, nil
}

type condExists struct {
	p   operand
	neg bool
}

func (n *condExists) eval(item map[string]any) (bool, *apiErr) {
	_, ok, aerr := n.p.get(item)
	if aerr != nil {
		return false, aerr
	}
	return ok != n.neg, nil
}

type condBeginsWith struct {
	p   operand
	val operand
}

func (n *condBeginsWith) eval(item map[string]any) (bool, *apiErr) {
	v, ok1, aerr := n.p.get(item)
	if aerr != nil {
		return false, aerr
	}
	o, ok2, aerr := n.val.get(item)
	if aerr != nil {
		return false, aerr
	}
	if !ok1 || !ok2 {
		return false, nil
	}
	if attrTypeOf(v) == "S" && attrTypeOf(o) == "S" {
		return strings.HasPrefix(attrString(v), attrString(o)), nil
	}
	if attrTypeOf(v) == "B" && attrTypeOf(o) == "B" {
		return bytes.HasPrefix(attrBytes(v), attrBytes(o)), nil
	}
	return false, nil
}

type condContains struct {
	p   operand
	val operand
}

func (n *condContains) eval(item map[string]any) (bool, *apiErr) {
	v, ok1, aerr := n.p.get(item)
	if aerr != nil {
		return false, aerr
	}
	o, ok2, aerr := n.val.get(item)
	if aerr != nil {
		return false, aerr
	}
	if !ok1 || !ok2 {
		return false, nil
	}
	switch t := attrTypeOf(v); t {
	case "S":
		if attrTypeOf(o) != "S" {
			return false, nil
		}
		return strings.Contains(attrString(v), attrString(o)), nil
	case "B":
		if attrTypeOf(o) != "B" {
			return false, nil
		}
		return bytes.Contains(attrBytes(v), attrBytes(o)), nil
	case "SS", "BS":
		lst, _ := v[t].([]any)
		for _, el := range lst {
			if s, ok := el.(string); ok && s == attrString(o) {
				return true, nil
			}
		}
		return false, nil
	case "NS":
		ro, ok := parseNumber(attrString(o))
		if !ok {
			return false, nil
		}
		lst, _ := v["NS"].([]any)
		for _, el := range lst {
			if s, ok := el.(string); ok {
				if rl, ok2 := parseNumber(s); ok2 && rl.Cmp(ro) == 0 {
					return true, nil
				}
			}
		}
		return false, nil
	case "L":
		lst, _ := v["L"].([]any)
		for _, el := range lst {
			if m, ok := el.(map[string]any); ok && attrEq(m, o) {
				return true, nil
			}
		}
		return false, nil
	}
	return false, nil
}

type condAttrType struct {
	p   operand
	val operand
}

func (n *condAttrType) eval(item map[string]any) (bool, *apiErr) {
	v, ok1, aerr := n.p.get(item)
	if aerr != nil {
		return false, aerr
	}
	o, ok2, aerr := n.val.get(item)
	if aerr != nil {
		return false, aerr
	}
	if !ok1 || !ok2 {
		return false, nil
	}
	if attrTypeOf(o) != "S" {
		return false, nil
	}
	return attrTypeOf(v) == attrString(o), nil
}

type condParser struct {
	toks []token
	i    int
	ctx  *exprCtx
}

func (p *condParser) cur() token { return p.toks[p.i] }

func (p *condParser) parseOr() (condNode, *apiErr) {
	first, aerr := p.parseAnd()
	if aerr != nil {
		return nil, aerr
	}
	subs := []condNode{first}
	for p.cur().kind == tkIdent && strings.EqualFold(p.cur().text, "OR") {
		p.i++
		sub, aerr := p.parseAnd()
		if aerr != nil {
			return nil, aerr
		}
		subs = append(subs, sub)
	}
	if len(subs) == 1 {
		return first, nil
	}
	return &condOr{subs}, nil
}

func (p *condParser) parseAnd() (condNode, *apiErr) {
	first, aerr := p.parseUnary()
	if aerr != nil {
		return nil, aerr
	}
	subs := []condNode{first}
	for p.cur().kind == tkIdent && strings.EqualFold(p.cur().text, "AND") {
		p.i++
		sub, aerr := p.parseUnary()
		if aerr != nil {
			return nil, aerr
		}
		subs = append(subs, sub)
	}
	if len(subs) == 1 {
		return first, nil
	}
	return &condAnd{subs}, nil
}

func (p *condParser) parseUnary() (condNode, *apiErr) {
	if p.cur().kind == tkIdent && strings.EqualFold(p.cur().text, "NOT") {
		p.i++
		sub, aerr := p.parseUnary()
		if aerr != nil {
			return nil, aerr
		}
		return &condNot{sub}, nil
	}
	return p.parsePrimary()
}

func (p *condParser) parsePrimary() (condNode, *apiErr) {
	t := p.cur()
	if t.kind == tkLParen {
		p.i++
		node, aerr := p.parseOr()
		if aerr != nil {
			return nil, aerr
		}
		if p.cur().kind != tkRParen {
			return nil, syntaxErrorf(p.ctx.label, p.cur().text)
		}
		p.i++
		return node, nil
	}
	if t.kind == tkEOF {
		return nil, syntaxErrorf(p.ctx.label, "")
	}
	if t.kind == tkIdent && p.i+1 < len(p.toks) && p.toks[p.i+1].kind == tkLParen {
		switch strings.ToLower(t.text) {
		case "attribute_exists", "attribute_not_exists":
			p.i += 2
			if p.cur().kind != tkIdent && p.cur().kind != tkName {
				return nil, syntaxErrorf(p.ctx.label, p.cur().text)
			}
			parts, aerr := p.ctx.parsePathTok(p.toks, &p.i)
			if aerr != nil {
				return nil, aerr
			}
			if p.cur().kind != tkRParen {
				return nil, syntaxErrorf(p.ctx.label, p.cur().text)
			}
			p.i++
			return &condExists{pathOperand{parts}, strings.ToLower(t.text) == "attribute_not_exists"}, nil
		case "begins_with", "contains", "attribute_type":
			p.i += 2
			if p.cur().kind != tkIdent && p.cur().kind != tkName {
				return nil, syntaxErrorf(p.ctx.label, p.cur().text)
			}
			parts, aerr := p.ctx.parsePathTok(p.toks, &p.i)
			if aerr != nil {
				return nil, aerr
			}
			if p.cur().kind != tkComma {
				return nil, syntaxErrorf(p.ctx.label, p.cur().text)
			}
			p.i++
			arg, aerr := p.parseOperand()
			if aerr != nil {
				return nil, aerr
			}
			if p.cur().kind != tkRParen {
				return nil, syntaxErrorf(p.ctx.label, p.cur().text)
			}
			p.i++
			switch strings.ToLower(t.text) {
			case "begins_with":
				return &condBeginsWith{pathOperand{parts}, arg}, nil
			case "contains":
				return &condContains{pathOperand{parts}, arg}, nil
			default:
				return &condAttrType{pathOperand{parts}, arg}, nil
			}
		}
	}
	if t.kind != tkIdent && t.kind != tkName {
		return nil, syntaxErrorf(p.ctx.label, t.text)
	}
	parts, aerr := p.ctx.parsePathTok(p.toks, &p.i)
	if aerr != nil {
		return nil, aerr
	}
	lhs := operand(pathOperand{parts})
	nt := p.cur()
	switch {
	case nt.kind == tkOp:
		p.i++
		rhs, aerr := p.parseOperand()
		if aerr != nil {
			return nil, aerr
		}
		return &condCmp{lhs, nt.text, rhs}, nil
	case nt.kind == tkIdent && strings.EqualFold(nt.text, "BETWEEN"):
		p.i++
		lo, aerr := p.parseOperand()
		if aerr != nil {
			return nil, aerr
		}
		if !(p.cur().kind == tkIdent && strings.EqualFold(p.cur().text, "AND")) {
			return nil, syntaxErrorf(p.ctx.label, p.cur().text)
		}
		p.i++
		hi, aerr := p.parseOperand()
		if aerr != nil {
			return nil, aerr
		}
		return &condBetween{lhs, lo, hi}, nil
	case nt.kind == tkIdent && strings.EqualFold(nt.text, "IN"):
		p.i++
		if p.cur().kind != tkLParen {
			return nil, syntaxErrorf(p.ctx.label, p.cur().text)
		}
		p.i++
		var opts []operand
		for {
			op, aerr := p.parseOperand()
			if aerr != nil {
				return nil, aerr
			}
			opts = append(opts, op)
			if p.cur().kind == tkComma {
				p.i++
				continue
			}
			if p.cur().kind == tkRParen {
				p.i++
				break
			}
			return nil, syntaxErrorf(p.ctx.label, p.cur().text)
		}
		return &condIn{lhs, opts}, nil
	default:
		return nil, syntaxErrorf(p.ctx.label, nt.text)
	}
}

func (p *condParser) parseOperand() (operand, *apiErr) {
	t := p.cur()
	switch t.kind {
	case tkValue:
		av, ok := p.ctx.values[t.text]
		if !ok {
			return nil, p.ctx.undefinedValue(t)
		}
		p.i++
		return valOperand{av}, nil
	case tkIdent:
		if p.i+1 < len(p.toks) && p.toks[p.i+1].kind == tkLParen && strings.EqualFold(t.text, "size") {
			p.i += 2
			if p.cur().kind != tkIdent && p.cur().kind != tkName {
				return nil, syntaxErrorf(p.ctx.label, p.cur().text)
			}
			parts, aerr := p.ctx.parsePathTok(p.toks, &p.i)
			if aerr != nil {
				return nil, aerr
			}
			if p.cur().kind != tkRParen {
				return nil, syntaxErrorf(p.ctx.label, p.cur().text)
			}
			p.i++
			return &sizeOperand{parts, p.ctx.label}, nil
		}
		parts, aerr := p.ctx.parsePathTok(p.toks, &p.i)
		if aerr != nil {
			return nil, aerr
		}
		return pathOperand{parts}, nil
	case tkName:
		parts, aerr := p.ctx.parsePathTok(p.toks, &p.i)
		if aerr != nil {
			return nil, aerr
		}
		return pathOperand{parts}, nil
	default:
		return nil, syntaxErrorf(p.ctx.label, t.text)
	}
}

func parseConditionExpression(s string, namesAny, valuesAny any, label string) (condNode, *apiErr) {
	if strings.TrimSpace(s) == "" {
		return nil, syntaxErrorf(label, "")
	}
	ctx, aerr := newExprCtx(namesAny, valuesAny, label)
	if aerr != nil {
		return nil, aerr
	}
	toks, aerr := tokenizeExpr(s, label)
	if aerr != nil {
		return nil, aerr
	}
	p := &condParser{toks: toks, ctx: ctx}
	node, aerr := p.parseOr()
	if aerr != nil {
		return nil, aerr
	}
	if p.cur().kind != tkEOF {
		return nil, syntaxErrorf(label, p.cur().text)
	}
	return node, nil
}

// ---------- legacy condition lists ----------

func parseLegacyConditions(m map[string]any, label string) (condNode, *apiErr) {
	var subs []condNode
	for name, specAny := range m {
		spec, ok := specAny.(map[string]any)
		if !ok {
			return nil, ddbErrorf("ValidationException", "One or more parameter values were invalid: Invalid condition for attribute %s in %s", name, label)
		}
		op, _ := spec["ComparisonOperator"].(string)
		var vals []map[string]any
		if lst, ok := reqListValue(spec, "AttributeValueList"); ok {
			for _, el := range lst {
				a, ok2 := toAttr(el)
				if !ok2 {
					return nil, ddbErrorf("ValidationException", "One or more parameter values were invalid: Invalid AttributeValueList entry for attribute %s", name)
				}
				vals = append(vals, a)
			}
		}
		node, aerr := buildLegacyNode(name, op, vals, label)
		if aerr != nil {
			return nil, aerr
		}
		subs = append(subs, node)
	}
	if len(subs) == 0 {
		return nil, nil
	}
	return &condAnd{subs}, nil
}

func buildLegacyNode(name, op string, vals []map[string]any, label string) (condNode, *apiErr) {
	ctx := &exprCtx{names: map[string]string{}, values: map[string]map[string]any{}, label: label}
	parts, aerr := ctx.parsePathFromText(name)
	if aerr != nil {
		return nil, aerr
	}
	lhs := operand(pathOperand{parts})
	valAt := func(i int) operand { return valOperand{vals[i]} }
	needVals := func(n int) *apiErr {
		if len(vals) < n {
			return ddbErrorf("ValidationException", "One or more parameter values were invalid: Invalid AttributeValueList for attribute %s", name)
		}
		return nil
	}
	switch strings.ToUpper(op) {
	case "EQ":
		if aerr := needVals(1); aerr != nil {
			return nil, aerr
		}
		return &condCmp{lhs, "=", valAt(0)}, nil
	case "NE":
		if aerr := needVals(1); aerr != nil {
			return nil, aerr
		}
		return &condCmp{lhs, "<>", valAt(0)}, nil
	case "LE":
		if aerr := needVals(1); aerr != nil {
			return nil, aerr
		}
		return &condCmp{lhs, "<=", valAt(0)}, nil
	case "LT":
		if aerr := needVals(1); aerr != nil {
			return nil, aerr
		}
		return &condCmp{lhs, "<", valAt(0)}, nil
	case "GE":
		if aerr := needVals(1); aerr != nil {
			return nil, aerr
		}
		return &condCmp{lhs, ">=", valAt(0)}, nil
	case "GT":
		if aerr := needVals(1); aerr != nil {
			return nil, aerr
		}
		return &condCmp{lhs, ">", valAt(0)}, nil
	case "BETWEEN":
		if aerr := needVals(2); aerr != nil {
			return nil, aerr
		}
		return &condBetween{lhs, valAt(0), valAt(1)}, nil
	case "IN":
		if aerr := needVals(1); aerr != nil {
			return nil, aerr
		}
		var opts []operand
		for i := range vals {
			opts = append(opts, valAt(i))
		}
		return &condIn{lhs, opts}, nil
	case "CONTAINS":
		if aerr := needVals(1); aerr != nil {
			return nil, aerr
		}
		return &condContains{lhs, valAt(0)}, nil
	case "NOT_CONTAINS":
		if aerr := needVals(1); aerr != nil {
			return nil, aerr
		}
		return &condNot{&condContains{lhs, valAt(0)}}, nil
	case "NULL":
		return &condExists{lhs, true}, nil
	case "NOT_NULL":
		return &condExists{lhs, false}, nil
	default:
		return nil, ddbErrorf("ValidationException", "One or more parameter values were invalid: Unknown ComparisonOperator: %s", op)
	}
}

func parseExpected(m map[string]any, label string) (condNode, *apiErr) {
	var subs []condNode
	for name, specAny := range m {
		spec, ok := specAny.(map[string]any)
		if !ok {
			return nil, ddbErrorf("ValidationException", "One or more parameter values were invalid: Invalid expected condition for attribute %s", name)
		}
		ctx := &exprCtx{names: map[string]string{}, values: map[string]map[string]any{}, label: label}
		parts, aerr := ctx.parsePathFromText(name)
		if aerr != nil {
			return nil, aerr
		}
		lhs := operand(pathOperand{parts})
		hasVal := false
		var val map[string]any
		if v, ok := spec["Value"]; ok {
			a, ok2 := toAttr(v)
			if !ok2 {
				return nil, ddbErrorf("ValidationException", "One or more parameter values were invalid: Invalid expected value for attribute %s", name)
			}
			val = a
			hasVal = true
		}
		exists, hasExists := true, false
		if v, ok := spec["Exists"]; ok {
			b, ok2 := v.(bool)
			if !ok2 {
				return nil, ddbErrorf("ValidationException", "One or more parameter values were invalid: Invalid Exists value for attribute %s", name)
			}
			exists = b
			hasExists = true
		}
		switch {
		case hasExists && !exists:
			subs = append(subs, &condExists{lhs, true})
		case hasVal:
			subs = append(subs, &condCmp{lhs, "=", valOperand{val}})
		case hasExists && exists:
			subs = append(subs, &condExists{lhs, false})
		default:
			return nil, ddbErrorf("ValidationException", "One or more parameter values were invalid: Invalid expected condition for attribute %s", name)
		}
	}
	if len(subs) == 0 {
		return nil, nil
	}
	return &condAnd{subs}, nil
}

func buildCondition(req map[string]any) (condNode, *apiErr) {
	v, hasCE := req["ConditionExpression"]
	if hasCE {
		s, isStr := v.(string)
		if !isStr {
			return nil, ddbError("ValidationException", "One or more parameter values were invalid: ConditionExpression must be a string")
		}
		if _, hasExpected := req["Expected"]; hasExpected {
			return nil, ddbError("ValidationException", "One or more parameter values were invalid: You cannot specify both ConditionExpression and Expected")
		}
		return parseConditionExpression(s, req["ExpressionAttributeNames"], req["ExpressionAttributeValues"], "ConditionExpression")
	}
	if em, ok := req["Expected"].(map[string]any); ok {
		return parseExpected(em, "Expected")
	}
	return nil, nil
}

// ---------- key conditions (Query) ----------

// keyRef describes the key schema being queried.
type keyRef struct {
	pk           string
	sk           string
	pkType       string
	skType       string
	isIndex, lsi bool
}

type kcond struct {
	attr string
	op   string
	vals []map[string]any
}

type keyMatcher struct {
	pk    map[string]any
	skOp  string
	sk    []map[string]any
	hasSk bool
}

func (m *keyMatcher) match(e entry) bool {
	if compareAttr(e.pk, m.pk) != 0 {
		return false
	}
	if !m.hasSk {
		return true
	}
	sk := e.sk
	if sk == nil {
		return false
	}
	switch m.skOp {
	case "=":
		return compareAttr(sk, m.sk[0]) == 0
	case "<":
		return compareAttr(sk, m.sk[0]) < 0
	case "<=":
		return compareAttr(sk, m.sk[0]) <= 0
	case ">":
		return compareAttr(sk, m.sk[0]) > 0
	case ">=":
		return compareAttr(sk, m.sk[0]) >= 0
	case "BETWEEN":
		return compareAttr(sk, m.sk[0]) >= 0 && compareAttr(sk, m.sk[1]) <= 0
	case "BEGINS_WITH":
		switch attrTypeOf(sk) {
		case "S":
			p2, ok1 := m.sk[0]["S"].(string)
			q, ok2 := sk["S"].(string)
			return ok1 && ok2 && strings.HasPrefix(q, p2)
		case "B":
			return bytes.HasPrefix(attrBytes(sk), attrBytes(m.sk[0]))
		}
		return false
	}
	return false
}

func buildKeyCondition(req map[string]any, sch keyRef) (*keyMatcher, *apiErr) {
	var hasKCE, hasKC bool
	var kce string
	if v, ok := req["KeyConditionExpression"]; ok {
		hasKCE = true
		s, isStr := v.(string)
		if !isStr {
			return nil, ddbError("ValidationException", "One or more parameter values were invalid: KeyConditionExpression must be a string")
		}
		kce = s
	}
	if _, ok := req["KeyConditions"]; ok {
		hasKC = true
	}
	if hasKCE && hasKC {
		return nil, ddbError("ValidationException", "One or more parameter values were invalid: You cannot specify both KeyConditionExpression and KeyConditions")
	}
	if !hasKCE && !hasKC {
		return nil, ddbError("ValidationException", "Either the KeyConditions or KeyConditionExpression parameter must be specified in the request.")
	}
	if hasKCE {
		return parseKeyConditionExpression(kce, req["ExpressionAttributeNames"], req["ExpressionAttributeValues"], sch)
	}
	kcMap, _ := req["KeyConditions"].(map[string]any)
	return parseKeyConditions(kcMap, sch)
}

func parseKeyConditionExpression(s string, namesAny, valuesAny any, sch keyRef) (*keyMatcher, *apiErr) {
	if strings.TrimSpace(s) == "" {
		return nil, syntaxErrorf("KeyConditionExpression", "")
	}
	ctx, aerr := newExprCtx(namesAny, valuesAny, "KeyConditionExpression")
	if aerr != nil {
		return nil, aerr
	}
	toks, aerr := tokenizeExpr(s, "KeyConditionExpression")
	if aerr != nil {
		return nil, aerr
	}
	i := 0
	var conds []kcond
	for {
		c, aerr := parseKCond(ctx, toks, &i)
		if aerr != nil {
			return nil, aerr
		}
		conds = append(conds, *c)
		if toks[i].kind == tkEOF {
			break
		}
		if toks[i].kind == tkIdent && strings.EqualFold(toks[i].text, "AND") {
			i++
			continue
		}
		return nil, syntaxErrorf("KeyConditionExpression", toks[i].text)
	}
	return keyMatcherFromConds(conds, sch)
}

func requireKCEValue(ctx *exprCtx, toks []token, i *int) (map[string]any, *apiErr) {
	t := toks[*i]
	if t.kind != tkValue {
		return nil, syntaxErrorf(ctx.label, t.text)
	}
	v, ok := ctx.values[t.text]
	if !ok {
		return nil, ctx.undefinedValue(t)
	}
	*i++
	return v, nil
}

func parseKCond(ctx *exprCtx, toks []token, i *int) (*kcond, *apiErr) {
	t := toks[*i]
	if t.kind != tkIdent && t.kind != tkName {
		return nil, syntaxErrorf(ctx.label, t.text)
	}
	nameText, aerr := ctx.resolveNameToken(t)
	if aerr != nil {
		return nil, aerr
	}
	if strings.ContainsAny(nameText, ".[") {
		return nil, syntaxErrorf(ctx.label, t.text)
	}
	*i++
	opTok := toks[*i]
	switch {
	case opTok.kind == tkOp:
		*i++
		v, aerr := requireKCEValue(ctx, toks, i)
		if aerr != nil {
			return nil, aerr
		}
		return &kcond{nameText, opTok.text, []map[string]any{v}}, nil
	case opTok.kind == tkIdent && strings.EqualFold(opTok.text, "BETWEEN"):
		*i++
		v1, aerr := requireKCEValue(ctx, toks, i)
		if aerr != nil {
			return nil, aerr
		}
		if toks[*i].kind != tkIdent || !strings.EqualFold(toks[*i].text, "AND") {
			return nil, syntaxErrorf(ctx.label, toks[*i].text)
		}
		*i++
		v2, aerr := requireKCEValue(ctx, toks, i)
		if aerr != nil {
			return nil, aerr
		}
		return &kcond{nameText, "BETWEEN", []map[string]any{v1, v2}}, nil
	case opTok.kind == tkIdent:
		return nil, ddbError("ValidationException", "Query key condition not supported")
	default:
		return nil, syntaxErrorf(ctx.label, opTok.text)
	}
}

func checkCondType(v map[string]any, schemaType string) *apiErr {
	t := attrTypeOf(v)
	if t != "S" && t != "N" && t != "B" {
		return ddbError("ValidationException", "One or more parameter values were invalid: Condition parameter type does not match schema type")
	}
	if t != schemaType {
		return ddbError("ValidationException", "One or more parameter values were invalid: Condition parameter type does not match schema type")
	}
	return nil
}

func keyMatcherFromConds(conds []kcond, sch keyRef) (*keyMatcher, *apiErr) {
	var pkC, skC *kcond
	unsupported := false
	for i := range conds {
		c := conds[i]
		if c.attr == sch.pk {
			if pkC != nil {
				return nil, ddbError("ValidationException", "Query key condition not supported")
			}
			pkC = &conds[i]
		} else if sch.sk != "" && c.attr == sch.sk {
			if skC != nil {
				return nil, ddbError("ValidationException", "Query key condition not supported")
			}
			skC = &conds[i]
		} else {
			unsupported = true
		}
	}
	if pkC == nil {
		return nil, ddbErrorf("ValidationException", "Query condition missed key schema element: %s", sch.pk)
	}
	if pkC.op != "=" {
		return nil, ddbError("ValidationException", "Query key condition not supported")
	}
	if unsupported {
		return nil, ddbError("ValidationException", "Query key condition not supported")
	}
	if aerr := checkCondType(pkC.vals[0], sch.pkType); aerr != nil {
		return nil, aerr
	}
	m := &keyMatcher{pk: pkC.vals[0]}
	if skC != nil {
		switch skC.op {
		case "=", "<", "<=", ">", ">=", "BETWEEN", "BEGINS_WITH":
		default:
			return nil, ddbError("ValidationException", "Query key condition not supported")
		}
		if skC.op == "BETWEEN" && len(skC.vals) != 2 {
			return nil, syntaxErrorf("KeyConditionExpression", "AND")
		}
		if skC.op != "BETWEEN" && len(skC.vals) != 1 {
			return nil, syntaxErrorf("KeyConditionExpression", "AND")
		}
		for _, v := range skC.vals {
			if aerr := checkCondType(v, sch.skType); aerr != nil {
				return nil, aerr
			}
		}
		if skC.op == "BEGINS_WITH" {
			if sch.skType != "S" && sch.skType != "B" {
				return nil, ddbError("ValidationException", "One or more parameter values were invalid: Condition parameter type does not match schema type")
			}
			if attrTypeOf(skC.vals[0]) != sch.skType {
				return nil, ddbError("ValidationException", "One or more parameter values were invalid: Condition parameter type does not match schema type")
			}
		}
		m.skOp = skC.op
		m.sk = skC.vals
		m.hasSk = true
	}
	return m, nil
}

func parseKeyConditions(m map[string]any, sch keyRef) (*keyMatcher, *apiErr) {
	if m == nil {
		return nil, ddbError("ValidationException", "Either the KeyConditions or KeyConditionExpression parameter must be specified in the request.")
	}
	var conds []kcond
	for attr, specAny := range m {
		spec, ok := specAny.(map[string]any)
		if !ok {
			return nil, ddbErrorf("ValidationException", "One or more parameter values were invalid: Invalid KeyConditions entry for attribute %s", attr)
		}
		op, _ := spec["ComparisonOperator"].(string)
		var vals []map[string]any
		if lst, ok := reqListValue(spec, "AttributeValueList"); ok {
			for _, el := range lst {
				a, ok2 := toAttr(el)
				if !ok2 {
					return nil, ddbErrorf("ValidationException", "One or more parameter values were invalid: Invalid AttributeValueList entry for attribute %s", attr)
				}
				vals = append(vals, a)
			}
		}
		var o string
		switch strings.ToUpper(op) {
		case "EQ":
			o = "="
		case "LE":
			o = "<="
		case "LT":
			o = "<"
		case "GE":
			o = ">="
		case "GT":
			o = ">"
		case "BETWEEN", "BEGINS_WITH":
			o = strings.ToUpper(op)
		default:
			return nil, ddbError("ValidationException", "Query key condition not supported")
		}
		if o == "BETWEEN" && len(vals) != 2 {
			return nil, ddbError("ValidationException", "One or more parameter values were invalid: Operator used in KeyConditions needs two operands")
		}
		if o != "BETWEEN" && len(vals) != 1 {
			return nil, ddbError("ValidationException", "One or more parameter values were invalid: Operator used in KeyConditions needs one operand")
		}
		conds = append(conds, kcond{attr, o, vals})
	}
	return keyMatcherFromConds(conds, sch)
}

// ---------- filter root collection ----------

func operandRoot(o operand) string {
	switch oo := o.(type) {
	case pathOperand:
		if len(oo.parts) > 0 && !oo.parts[0].isIndex {
			return oo.parts[0].name
		}
	case *sizeOperand:
		if len(oo.parts) > 0 && !oo.parts[0].isIndex {
			return oo.parts[0].name
		}
	}
	return ""
}

func collectFilterRoots(n condNode, roots map[string]bool) {
	switch c := n.(type) {
	case *condAnd:
		for _, s := range c.subs {
			collectFilterRoots(s, roots)
		}
	case *condOr:
		for _, s := range c.subs {
			collectFilterRoots(s, roots)
		}
	case *condNot:
		collectFilterRoots(c.sub, roots)
	case *condCmp:
		if r := operandRoot(c.lhs); r != "" {
			roots[r] = true
		}
		if r := operandRoot(c.rhs); r != "" {
			roots[r] = true
		}
	case *condBetween:
		for _, o := range []operand{c.val, c.lo, c.hi} {
			if r := operandRoot(o); r != "" {
				roots[r] = true
			}
		}
	case *condIn:
		if r := operandRoot(c.lhs); r != "" {
			roots[r] = true
		}
		for _, o := range c.opts {
			if r := operandRoot(o); r != "" {
				roots[r] = true
			}
		}
	case *condExists:
		if r := operandRoot(c.p); r != "" {
			roots[r] = true
		}
	case *condBeginsWith:
		for _, o := range []operand{c.p, c.val} {
			if r := operandRoot(o); r != "" {
				roots[r] = true
			}
		}
	case *condContains:
		for _, o := range []operand{c.p, c.val} {
			if r := operandRoot(o); r != "" {
				roots[r] = true
			}
		}
	case *condAttrType:
		for _, o := range []operand{c.p, c.val} {
			if r := operandRoot(o); r != "" {
				roots[r] = true
			}
		}
	}
}

// ---------- update expressions ----------

type updValue interface {
	resolve(item map[string]any) (map[string]any, *apiErr)
}

type constUpdValue struct{ av map[string]any }

func (v constUpdValue) resolve(item map[string]any) (map[string]any, *apiErr) {
	return cloneAttr(v.av), nil
}

type pathUpdValue struct {
	parts []pathPart
}

func (v pathUpdValue) resolve(item map[string]any) (map[string]any, *apiErr) {
	av, ok, aerr := pathOperand{v.parts}.get(item)
	if aerr != nil {
		return nil, aerr
	}
	if !ok {
		return nil, missingExprErr()
	}
	return cloneAttr(av), nil
}

type listAppendUpdValue struct {
	a, b updValue
}

func (v listAppendUpdValue) resolve(item map[string]any) (map[string]any, *apiErr) {
	av, aerr := v.a.resolve(item)
	if aerr != nil {
		return nil, aerr
	}
	bv, aerr := v.b.resolve(item)
	if aerr != nil {
		return nil, aerr
	}
	if attrTypeOf(av) != "L" || attrTypeOf(bv) != "L" {
		return nil, ddbError("ValidationException", "Incompatible types for list_append")
	}
	la, _ := av["L"].([]any)
	lb, _ := bv["L"].([]any)
	out := make([]any, 0, len(la)+len(lb))
	out = append(out, la...)
	out = append(out, lb...)
	return map[string]any{"L": out}, nil
}

type ifNotExistsUpdValue struct {
	parts []pathPart
	alt   updValue
}

func (v ifNotExistsUpdValue) resolve(item map[string]any) (map[string]any, *apiErr) {
	if raw, ok := getPathValue(item, v.parts); ok {
		if m, ok2 := raw.(map[string]any); ok2 {
			return cloneAttr(m), nil
		}
	}
	return v.alt.resolve(item)
}

type arithUpdValue struct {
	op  string
	lhs updValue
	rhs updValue
}

func (v arithUpdValue) resolve(item map[string]any) (map[string]any, *apiErr) {
	la, aerr := v.lhs.resolve(item)
	if aerr != nil {
		return nil, aerr
	}
	ra, aerr := v.rhs.resolve(item)
	if aerr != nil {
		return nil, aerr
	}
	if attrTypeOf(la) != "N" || attrTypeOf(ra) != "N" {
		return nil, ddbError("ValidationException", "An operand in the update expression has an incorrect type")
	}
	l, ok1 := parseNumber(attrString(la))
	r, ok2 := parseNumber(attrString(ra))
	if !ok1 || !ok2 {
		return nil, ddbError("ValidationException", "One or more parameter values were invalid: The parameter cannot be converted to a numeric value")
	}
	if v.op == "+" {
		l.Add(l, r)
	} else {
		l.Sub(l, r)
	}
	return map[string]any{"N": bigRatDecimal(l)}, nil
}

type updOp struct {
	verb  string
	parts []pathPart
	root  string
	val   updValue
}

func parseUpdateExpression(s string, namesAny, valuesAny any) ([]updOp, *apiErr) {
	if strings.TrimSpace(s) == "" {
		return nil, syntaxErrorf("UpdateExpression", "")
	}
	ctx, aerr := newExprCtx(namesAny, valuesAny, "UpdateExpression")
	if aerr != nil {
		return nil, aerr
	}
	toks, aerr := tokenizeExpr(s, "UpdateExpression")
	if aerr != nil {
		return nil, aerr
	}
	i := 0
	var ops []updOp
	for i < len(toks) {
		t := toks[i]
		if t.kind != tkIdent {
			return nil, syntaxErrorf("UpdateExpression", t.text)
		}
		verb := strings.ToUpper(t.text)
		if verb != "SET" && verb != "ADD" && verb != "REMOVE" && verb != "DELETE" {
			return nil, syntaxErrorf("UpdateExpression", t.text)
		}
		i++
		for {
			if toks[i].kind != tkIdent && toks[i].kind != tkName {
				return nil, syntaxErrorf("UpdateExpression", toks[i].text)
			}
			parts, aerr := ctx.parsePathTok(toks, &i)
			if aerr != nil {
				return nil, aerr
			}
			var val updValue
			switch verb {
			case "SET":
				if toks[i].kind != tkOp || toks[i].text != "=" {
					return nil, syntaxErrorf("UpdateExpression", toks[i].text)
				}
				i++
				val, aerr = parseUpdValue(ctx, toks, &i, true)
				if aerr != nil {
					return nil, aerr
				}
			case "ADD", "DELETE":
				val, aerr = parseUpdValue(ctx, toks, &i, false)
				if aerr != nil {
					return nil, aerr
				}
			case "REMOVE":
				val = nil
			}
			ops = append(ops, updOp{verb, parts, parts[0].name, val})
			if toks[i].kind == tkComma {
				i++
				continue
			}
			break
		}
	}
	return ops, nil
}

func parseUpdValue(ctx *exprCtx, toks []token, i *int, allowExpr bool) (updValue, *apiErr) {
	v, aerr := parseUpdAtom(ctx, toks, i, allowExpr)
	if aerr != nil {
		return nil, aerr
	}
	for allowExpr && (toks[*i].kind == tkPlus || toks[*i].kind == tkMinus) {
		opTok := toks[*i]
		*i++
		rhs, aerr := parseUpdAtom(ctx, toks, i, allowExpr)
		if aerr != nil {
			return nil, aerr
		}
		v = arithUpdValue{opTok.text, v, rhs}
	}
	return v, nil
}

func parseUpdAtom(ctx *exprCtx, toks []token, i *int, allowExpr bool) (updValue, *apiErr) {
	t := toks[*i]
	switch t.kind {
	case tkValue:
		av, ok := ctx.values[t.text]
		if !ok {
			return nil, ctx.undefinedValue(t)
		}
		*i++
		return constUpdValue{av}, nil
	case tkIdent:
		if allowExpr && *i+1 < len(toks) && toks[*i+1].kind == tkLParen {
			switch strings.ToUpper(t.text) {
			case "LIST_APPEND":
				*i += 2
				a, aerr := parseUpdValue(ctx, toks, i, true)
				if aerr != nil {
					return nil, aerr
				}
				if toks[*i].kind != tkComma {
					return nil, syntaxErrorf(ctx.label, toks[*i].text)
				}
				*i++
				b, aerr := parseUpdValue(ctx, toks, i, true)
				if aerr != nil {
					return nil, aerr
				}
				if toks[*i].kind != tkRParen {
					return nil, syntaxErrorf(ctx.label, toks[*i].text)
				}
				*i++
				return listAppendUpdValue{a, b}, nil
			case "IF_NOT_EXISTS":
				*i += 2
				if toks[*i].kind != tkIdent && toks[*i].kind != tkName {
					return nil, syntaxErrorf(ctx.label, toks[*i].text)
				}
				parts, aerr := ctx.parsePathTok(toks, i)
				if aerr != nil {
					return nil, aerr
				}
				if toks[*i].kind != tkComma {
					return nil, syntaxErrorf(ctx.label, toks[*i].text)
				}
				*i++
				alt, aerr := parseUpdValue(ctx, toks, i, true)
				if aerr != nil {
					return nil, aerr
				}
				if toks[*i].kind != tkRParen {
					return nil, syntaxErrorf(ctx.label, toks[*i].text)
				}
				*i++
				return ifNotExistsUpdValue{parts, alt}, nil
			}
		}
		parts, aerr := ctx.parsePathTok(toks, i)
		if aerr != nil {
			return nil, aerr
		}
		return pathUpdValue{parts}, nil
	case tkName:
		parts, aerr := ctx.parsePathTok(toks, i)
		if aerr != nil {
			return nil, aerr
		}
		return pathUpdValue{parts}, nil
	default:
		return nil, syntaxErrorf(ctx.label, t.text)
	}
}

// applyUpdateOps applies parsed operations in DynamoDB's fixed verb order.
func applyUpdateOps(item map[string]any, ops []updOp, t *Table) ([]updOp, *apiErr) {
	protected := func(root string) *apiErr {
		if root == t.PK || (t.SK != "" && root == t.SK) {
			return ddbErrorf("ValidationException", "One or more parameter values were invalid: Cannot update attribute %s: primary key", root)
		}
		return nil
	}
	var touched []updOp
	for _, verb := range []string{"SET", "REMOVE", "ADD", "DELETE"} {
		for _, op := range ops {
			if op.verb != verb {
				continue
			}
			if aerr := protected(op.root); aerr != nil {
				return nil, aerr
			}
			switch verb {
			case "SET":
				val, aerr := op.val.resolve(item)
				if aerr != nil {
					return nil, aerr
				}
				if aerr := setPathValue(item, op.parts, val, true); aerr != nil {
					return nil, aerr
				}
				touched = append(touched, op)
			case "REMOVE":
				removePathValue(item, op.parts)
				touched = append(touched, op)
			case "ADD":
				if len(op.parts) != 1 {
					return nil, ddbError("ValidationException", "Invalid UpdateExpression: Only one path element is allowed for the ADD action")
				}
				val, aerr := op.val.resolve(item)
				if aerr != nil {
					return nil, aerr
				}
				existing, has := item[op.root]
				newVal, aerr := addUpdateValues(existing, has, val)
				if aerr != nil {
					return nil, aerr
				}
				if newVal == nil {
					delete(item, op.root)
				} else {
					item[op.root] = newVal
				}
				touched = append(touched, op)
			case "DELETE":
				if len(op.parts) != 1 {
					return nil, ddbError("ValidationException", "Invalid UpdateExpression: Only one path element is allowed for the DELETE action")
				}
				val, aerr := op.val.resolve(item)
				if aerr != nil {
					return nil, aerr
				}
				existing, has := item[op.root]
				if !has {
					touched = append(touched, op)
					continue
				}
				newVal, aerr := deleteUpdateValues(existing, val)
				if aerr != nil {
					return nil, aerr
				}
				if newVal == nil {
					delete(item, op.root)
				} else {
					item[op.root] = newVal
				}
				touched = append(touched, op)
			}
		}
	}
	return touched, nil
}

func elStr(el any) string {
	s, _ := el.(string)
	return s
}

func addUpdateValues(existing map[string]any, has bool, val map[string]any) (map[string]any, *apiErr) {
	vt := attrTypeOf(val)
	if vt == "" {
		return nil, ddbError("ValidationException", "An operand in the update expression has an incorrect type")
	}
	if !has {
		return val, nil
	}
	et := attrTypeOf(existing)
	if et == "N" && vt == "N" {
		l, ok1 := parseNumber(attrString(existing))
		r, ok2 := parseNumber(attrString(val))
		if !ok1 || !ok2 {
			return nil, ddbError("ValidationException", "One or more parameter values were invalid: The parameter cannot be converted to a numeric value")
		}
		l.Add(l, r)
		return map[string]any{"N": bigRatDecimal(l)}, nil
	}
	if et == vt && (et == "SS" || et == "NS" || et == "BS") {
		return setUnion(existing, val, et), nil
	}
	return nil, ddbError("ValidationException", "An operand in the update expression has an incorrect type")
}

func deleteUpdateValues(existing map[string]any, val map[string]any) (map[string]any, *apiErr) {
	et := attrTypeOf(existing)
	vt := attrTypeOf(val)
	if et == "" || et != vt || (et != "SS" && et != "NS" && et != "BS") {
		return nil, ddbError("ValidationException", "An operand in the update expression has an incorrect type")
	}
	lst, _ := existing[et].([]any)
	rem, _ := val[vt].([]any)
	var out []any
	for _, ex := range lst {
		drop := false
		for _, el := range rem {
			if et == "NS" {
				a, ok1 := parseNumber(elStr(ex))
				b, ok2 := parseNumber(elStr(el))
				if ok1 && ok2 && a.Cmp(b) == 0 {
					drop = true
					break
				}
			} else if el == ex {
				drop = true
				break
			}
		}
		if !drop {
			out = append(out, ex)
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return map[string]any{et: out}, nil
}

func setUnion(existing, val map[string]any, t string) map[string]any {
	lst, _ := existing[t].([]any)
	add, _ := val[t].([]any)
	out := make([]any, 0, len(lst)+len(add))
	out = append(out, lst...)
	for _, el := range add {
		dup := false
		for _, ex := range out {
			if t == "NS" {
				a, ok1 := parseNumber(elStr(el))
				b, ok2 := parseNumber(elStr(ex))
				if ok1 && ok2 && a.Cmp(b) == 0 {
					dup = true
					break
				}
			} else if el == ex {
				dup = true
				break
			}
		}
		if !dup {
			out = append(out, el)
		}
	}
	return map[string]any{t: out}
}

// ---------- projection expressions ----------

func parseProjectionExpression(pe string, namesAny any) ([][]pathPart, *apiErr) {
	if strings.TrimSpace(pe) == "" {
		return nil, syntaxErrorf("ProjectionExpression", "")
	}
	ctx, aerr := newExprCtx(namesAny, nil, "ProjectionExpression")
	if aerr != nil {
		return nil, aerr
	}
	toks, aerr := tokenizeExpr(pe, "ProjectionExpression")
	if aerr != nil {
		return nil, aerr
	}
	i := 0
	var paths [][]pathPart
	for {
		t := toks[i]
		if t.kind != tkIdent && t.kind != tkName {
			return nil, syntaxErrorf("ProjectionExpression", t.text)
		}
		parts, aerr := ctx.parsePathTok(toks, &i)
		if aerr != nil {
			return nil, aerr
		}
		for _, ex := range paths {
			if pathsOverlap(ex, parts) {
				return nil, ddbError("ValidationException", "Invalid ProjectionExpression: Two document paths overlap with each other")
			}
		}
		paths = append(paths, parts)
		if toks[i].kind == tkComma {
			i++
			continue
		}
		if toks[i].kind == tkEOF {
			break
		}
		return nil, syntaxErrorf("ProjectionExpression", toks[i].text)
	}
	return paths, nil
}

func parseAttributesToGet(v any) ([][]pathPart, *apiErr) {
	list, ok := v.([]any)
	if !ok {
		return nil, ddbError("ValidationException", "One or more parameter values were invalid: AttributesToGet must be a list")
	}
	ctx := &exprCtx{names: map[string]string{}, values: map[string]map[string]any{}, label: "ProjectionExpression"}
	var paths [][]pathPart
	for _, el := range list {
		s, ok := el.(string)
		if !ok {
			return nil, ddbError("ValidationException", "One or more parameter values were invalid: AttributesToGet must contain strings")
		}
		if s == "" {
			return nil, ddbError("ValidationException", "One or more parameter values were invalid: AttributesToGet must not contain empty attribute names")
		}
		parts, aerr := ctx.parsePathFromText(s)
		if aerr != nil {
			return nil, aerr
		}
		for _, ex := range paths {
			if pathsOverlap(ex, parts) {
				return nil, ddbError("ValidationException", "Invalid ProjectionExpression: Two document paths overlap with each other")
			}
		}
		paths = append(paths, parts)
	}
	return paths, nil
}

func parseProjectionParams(req map[string]any) ([][]pathPart, *apiErr) {
	pe, hasPE := req["ProjectionExpression"]
	atg, hasATG := req["AttributesToGet"]
	if hasPE && hasATG {
		return nil, ddbError("ValidationException", "Cannot specify both ProjectionExpression and AttributesToGet")
	}
	if hasPE {
		s, isStr := pe.(string)
		if !isStr {
			return nil, syntaxErrorf("ProjectionExpression", "")
		}
		return parseProjectionExpression(s, req["ExpressionAttributeNames"])
	}
	if hasATG {
		return parseAttributesToGet(atg)
	}
	return nil, nil
}
