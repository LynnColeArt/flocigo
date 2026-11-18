package ddb

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// ---------------------------------------------------------------------------
// Expression engine kernel for DynamoDB update / condition expressions.
//
// The engine operates over the raw on-wire attribute value representation
// ({"S": "x"}, {"N": "1"}, {"L": [...]}, ...). All symbols carry the "ux"
// prefix so the kernel stays contained inside this package.
// ---------------------------------------------------------------------------

// uxAV is a raw DynamoDB attribute value (single-key typed map).
type uxAV = map[string]any

// uxErr carries an API level error (type + message).
type uxErr struct {
	Code string
	Msg  string
}

func (e *uxErr) Error() string { return e.Code + ": " + e.Msg }

func uxValid(format string, args ...any) *uxErr {
	return &uxErr{Code: "ValidationException", Msg: fmt.Sprintf(format, args...)}
}

func uxInvalidPath() *uxErr {
	return uxValid("The document path provided in the update expression is invalid for update")
}

// ------------------------------------------------------------------ lexer --

type uxTokKind int

const (
	uxTokEOF uxTokKind = iota
	uxTokWord
	uxTokNum
	uxTokVal
	uxTokName
	uxTokLBrack
	uxTokRBrack
	uxTokLParen
	uxTokRParen
	uxTokComma
	uxTokDot
	uxTokEq
	uxTokNe
	uxTokLt
	uxTokLe
	uxTokGt
	uxTokGe
	uxTokPlus
	uxTokMinus
)

type uxTok struct {
	kind uxTokKind
	text string
}

func uxLex(expr string) ([]uxTok, *uxErr) {
	var toks []uxTok
	i, n := 0, len(expr)
	isStart := func(c byte) bool {
		return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
	}
	isName := func(c byte) bool { return isStart(c) || (c >= '0' && c <= '9') }
	isDigit := func(c byte) bool { return c >= '0' && c <= '9' }
	push := func(k uxTokKind, text string) { toks = append(toks, uxTok{kind: k, text: text}) }
	for i < n {
		c := expr[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == ':':
			st := i
			i++
			for i < n && isName(expr[i]) {
				i++
			}
			if i == st+1 {
				return nil, uxValid("Invalid token \":\" in expression")
			}
			push(uxTokVal, expr[st:i])
		case c == '#':
			st := i
			i++
			for i < n && isName(expr[i]) {
				i++
			}
			if i == st+1 {
				return nil, uxValid("Invalid token \"#\" in expression")
			}
			push(uxTokName, expr[st:i])
		case c == '[':
			push(uxTokLBrack, "[")
			i++
		case c == ']':
			push(uxTokRBrack, "]")
			i++
		case c == '(':
			push(uxTokLParen, "(")
			i++
		case c == ')':
			push(uxTokRParen, ")")
			i++
		case c == ',':
			push(uxTokComma, ",")
			i++
		case c == '.':
			push(uxTokDot, ".")
			i++
		case isDigit(c):
			st := i
			for i < n && isDigit(expr[i]) {
				i++
			}
			push(uxTokNum, expr[st:i])
		case isStart(c):
			st := i
			for i < n && isName(expr[i]) {
				i++
			}
			push(uxTokWord, expr[st:i])
		case c == '=':
			push(uxTokEq, "=")
			i++
		case c == '<':
			if i+1 < n && expr[i+1] == '>' {
				push(uxTokNe, "<>")
				i += 2
			} else if i+1 < n && expr[i+1] == '=' {
				push(uxTokLe, "<=")
				i += 2
			} else {
				push(uxTokLt, "<")
				i++
			}
		case c == '>':
			if i+1 < n && expr[i+1] == '=' {
				push(uxTokGe, ">=")
				i += 2
			} else {
				push(uxTokGt, ">")
				i++
			}
		case c == '+':
			push(uxTokPlus, "+")
			i++
		case c == '-':
			push(uxTokMinus, "-")
			i++
		default:
			return nil, uxValid("Invalid token %q in expression", string(c))
		}
	}
	push(uxTokEOF, "<eof>")
	return toks, nil
}

// ----------------------------------------------------------------- parser --

type uxParser struct {
	toks  []uxTok
	pos   int
	exprN string             // "UpdateExpression" or "ConditionExpression"
	names map[string]string  // ExpressionAttributeNames (keys include '#')
	usedN map[string]bool
	usedV map[string]bool
}

func (p *uxParser) peek() uxTok { return p.toks[p.pos] }

func (p *uxParser) peekAt(k int) uxTok {
	if p.pos+k < len(p.toks) {
		return p.toks[p.pos+k]
	}
	return p.toks[len(p.toks)-1]
}

func (p *uxParser) next() uxTok {
	t := p.toks[p.pos]
	if p.pos < len(p.toks)-1 {
		p.pos++
	}
	return t
}

func (p *uxParser) isKw(t uxTok, kw string) bool {
	return t.kind == uxTokWord && strings.EqualFold(t.text, kw)
}

func (p *uxParser) syntax(tok string) *uxErr {
	return uxValid("Invalid %s: Syntax error; token: %q", p.exprN, tok)
}

func (p *uxParser) fail(msg string) *uxErr {
	return uxValid("Invalid %s: %s", p.exprN, msg)
}

var uxReserved = map[string]bool{
	"and": true, "or": true, "not": true, "between": true, "in": true,
	"set": true, "remove": true, "add": true, "delete": true,
	"size": true, "contains": true, "begins_with": true,
	"attribute_type": true, "attribute_exists": true, "attribute_not_exists": true,
	"if_not_exists": true, "list_append": true,
}

type uxComp struct {
	attr  string
	idx   int
	isIdx bool
}

type uxPath struct{ comps []uxComp }

func (p *uxParser) resolveName(tok uxTok) (string, *uxErr) {
	name, ok := p.names[tok.text]
	if !ok {
		return "", p.fail("An expression attribute name used in the expression is not defined")
	}
	p.usedN[tok.text] = true
	return name, nil
}

func (p *uxParser) parsePath() (uxPath, *uxErr) {
	var path uxPath
	t := p.next()
	switch t.kind {
	case uxTokWord:
		if uxReserved[strings.ToLower(t.text)] {
			return path, p.syntax(t.text)
		}
		path.comps = append(path.comps, uxComp{attr: t.text})
	case uxTokName:
		name, e := p.resolveName(t)
		if e != nil {
			return path, e
		}
		path.comps = append(path.comps, uxComp{attr: name})
	default:
		return path, p.syntax(t.text)
	}
	for {
		t := p.peek()
		switch t.kind {
		case uxTokDot:
			p.next()
			nt := p.next()
			switch nt.kind {
			case uxTokWord:
				if uxReserved[strings.ToLower(nt.text)] {
					return path, p.syntax(nt.text)
				}
				path.comps = append(path.comps, uxComp{attr: nt.text})
			case uxTokName:
				name, e := p.resolveName(nt)
				if e != nil {
					return path, e
				}
				path.comps = append(path.comps, uxComp{attr: name})
			default:
				return path, p.syntax(nt.text)
			}
		case uxTokLBrack:
			p.next()
			nt := p.next()
			if nt.kind != uxTokNum {
				return path, p.syntax(nt.text)
			}
			idx, aerr := strconv.Atoi(nt.text)
			if aerr != nil {
				return path, p.syntax(nt.text)
			}
			if p.next().kind != uxTokRBrack {
				return path, p.syntax("]")
			}
			path.comps = append(path.comps, uxComp{idx: idx, isIdx: true})
		default:
			return path, nil
		}
	}
}

// --------------------------------------------------------------- update AST

type uxOperand struct {
	isVal bool
	path  uxPath
	val   string // includes ':' sigil
	size  *uxOperand
}

type uxSetVal struct {
	kind string // "op", "arith", "ifne", "append"
	opnd uxOperand
	op   string
	a, b *uxSetVal
}

type uxAction struct {
	op   string // SET, REMOVE, ADD, DELETE
	path uxPath
	set  *uxSetVal
	val  string // ADD / DELETE value ref (includes ':')
}

func uxParseUpdate(expr string, names map[string]string) ([]uxAction, map[string]bool, map[string]bool, *uxErr) {
	toks, e := uxLex(expr)
	if e != nil {
		return nil, nil, nil, e
	}
	p := &uxParser{
		toks: toks, exprN: "UpdateExpression", names: names,
		usedN: map[string]bool{}, usedV: map[string]bool{},
	}
	var acts []uxAction
	stage := 0
	ranks := map[string]int{"SET": 1, "REMOVE": 2, "ADD": 3, "DELETE": 4}
	for {
		t := p.peek()
		if t.kind == uxTokEOF {
			break
		}
		if t.kind != uxTokWord {
			return nil, nil, nil, p.syntax(t.text)
		}
		rank, ok := ranks[strings.ToUpper(t.text)]
		if !ok || rank <= stage {
			return nil, nil, nil, p.syntax(t.text)
		}
		stage = rank
		opname := strings.ToUpper(t.text)
		p.next()
		for {
			act, e2 := p.parseAction(opname, rank)
			if e2 != nil {
				return nil, nil, nil, e2
			}
			acts = append(acts, act)
			if p.peek().kind == uxTokComma {
				p.next()
				continue
			}
			break
		}
	}
	return acts, p.usedN, p.usedV, nil
}

func (p *uxParser) parseAction(op string, rank int) (uxAction, *uxErr) {
	path, e := p.parsePath()
	if e != nil {
		return uxAction{}, e
	}
	a := uxAction{op: op, path: path}
	switch rank {
	case 1: // SET path = value
		if p.peek().kind != uxTokEq {
			return a, p.syntax(p.peek().text)
		}
		p.next()
		sv, e2 := p.parseSetVal()
		if e2 != nil {
			return a, e2
		}
		a.set = sv
	case 2: // REMOVE path
	default: // ADD / DELETE path :value
		t := p.peek()
		if t.kind != uxTokVal {
			return a, p.syntax(t.text)
		}
		p.next()
		p.usedV[t.text] = true
		a.val = t.text
	}
	return a, nil
}

func (p *uxParser) parseSetVal() (*uxSetVal, *uxErr) {
	a, e := p.parseSetOperand()
	if e != nil {
		return nil, e
	}
	t := p.peek()
	if t.kind == uxTokPlus || t.kind == uxTokMinus {
		op := "+"
		if t.kind == uxTokMinus {
			op = "-"
		}
		p.next()
		b, e2 := p.parseSetOperand()
		if e2 != nil {
			return nil, e2
		}
		return &uxSetVal{kind: "arith", op: op, a: a, b: b}, nil
	}
	return a, nil
}

func (p *uxParser) parseSetOperand() (*uxSetVal, *uxErr) {
	t := p.peek()
	if t.kind == uxTokVal {
		p.next()
		p.usedV[t.text] = true
		return &uxSetVal{kind: "op", opnd: uxOperand{isVal: true, val: t.text}}, nil
	}
	if t.kind == uxTokWord && p.peekAt(1).kind == uxTokLParen {
		switch strings.ToLower(t.text) {
		case "if_not_exists":
			p.next()
			p.next()
			path, e := p.parsePath()
			if e != nil {
				return nil, e
			}
			if p.peek().kind != uxTokComma {
				return nil, p.syntax(p.peek().text)
			}
			p.next()
			fb, e2 := p.parseSetOperand()
			if e2 != nil {
				return nil, e2
			}
			if p.next().kind != uxTokRParen {
				return nil, p.syntax(p.peek().text)
			}
			return &uxSetVal{kind: "ifne", opnd: uxOperand{path: path}, a: fb}, nil
		case "list_append":
			p.next()
			p.next()
			x, e := p.parseSetOperand()
			if e != nil {
				return nil, e
			}
			if p.peek().kind != uxTokComma {
				return nil, p.syntax(p.peek().text)
			}
			p.next()
			y, e2 := p.parseSetOperand()
			if e2 != nil {
				return nil, e2
			}
			if p.next().kind != uxTokRParen {
				return nil, p.syntax(p.peek().text)
			}
			return &uxSetVal{kind: "append", a: x, b: y}, nil
		default:
			return nil, p.syntax(t.text)
		}
	}
	path, e := p.parsePath()
	if e != nil {
		return nil, e
	}
	return &uxSetVal{kind: "op", opnd: uxOperand{path: path}}, nil
}

// ----------------------------------------------------------- condition AST

type uxCond struct {
	kind string // or, and, not, cmp, between, in, exists, notexists, contains, begins_with, attribute_type
	op   string
	kids []*uxCond
	ops  []uxOperand
}

func uxParseCondition(expr string, names map[string]string) (*uxCond, map[string]bool, map[string]bool, *uxErr) {
	toks, e := uxLex(expr)
	if e != nil {
		return nil, nil, nil, e
	}
	p := &uxParser{
		toks: toks, exprN: "ConditionExpression", names: names,
		usedN: map[string]bool{}, usedV: map[string]bool{},
	}
	n, e2 := p.parseOr()
	if e2 != nil {
		return nil, nil, nil, e2
	}
	if p.peek().kind != uxTokEOF {
		return nil, nil, nil, p.syntax(p.peek().text)
	}
	return n, p.usedN, p.usedV, nil
}

func (p *uxParser) parseOr() (*uxCond, *uxErr) {
	left, e := p.parseAnd()
	if e != nil {
		return nil, e
	}
	for p.isKw(p.peek(), "OR") {
		p.next()
		right, e2 := p.parseAnd()
		if e2 != nil {
			return nil, e2
		}
		left = &uxCond{kind: "or", kids: []*uxCond{left, right}}
	}
	return left, nil
}

func (p *uxParser) parseAnd() (*uxCond, *uxErr) {
	left, e := p.parseNot()
	if e != nil {
		return nil, e
	}
	for p.isKw(p.peek(), "AND") {
		p.next()
		right, e2 := p.parseNot()
		if e2 != nil {
			return nil, e2
		}
		left = &uxCond{kind: "and", kids: []*uxCond{left, right}}
	}
	return left, nil
}

func (p *uxParser) parseNot() (*uxCond, *uxErr) {
	if p.isKw(p.peek(), "NOT") {
		p.next()
		kid, e := p.parseNot()
		if e != nil {
			return nil, e
		}
		return &uxCond{kind: "not", kids: []*uxCond{kid}}, nil
	}
	return p.parsePrimary()
}

func (p *uxParser) parsePrimary() (*uxCond, *uxErr) {
	t := p.peek()
	if t.kind == uxTokLParen {
		p.next()
		n, e := p.parseOr()
		if e != nil {
			return nil, e
		}
		if p.next().kind != uxTokRParen {
			return nil, p.syntax(p.peek().text)
		}
		return n, nil
	}
	if t.kind == uxTokWord && p.peekAt(1).kind == uxTokLParen {
		lname := strings.ToLower(t.text)
		switch lname {
		case "attribute_exists", "attribute_not_exists":
			p.next()
			p.next()
			path, e := p.parsePath()
			if e != nil {
				return nil, e
			}
			if p.next().kind != uxTokRParen {
				return nil, p.syntax(p.peek().text)
			}
			kind := "exists"
			if lname == "attribute_not_exists" {
				kind = "notexists"
			}
			return &uxCond{kind: kind, ops: []uxOperand{{path: path}}}, nil
		case "contains", "begins_with", "attribute_type":
			p.next()
			p.next()
			x, e := p.parseOperand()
			if e != nil {
				return nil, e
			}
			if p.peek().kind != uxTokComma {
				return nil, p.syntax(p.peek().text)
			}
			p.next()
			y, e2 := p.parseOperand()
			if e2 != nil {
				return nil, e2
			}
			if p.next().kind != uxTokRParen {
				return nil, p.syntax(p.peek().text)
			}
			return &uxCond{kind: lname, op: lname, ops: []uxOperand{x, y}}, nil
		}
	}
	return p.parseCmp()
}

func (p *uxParser) parseCmp() (*uxCond, *uxErr) {
	x, e := p.parseOperand()
	if e != nil {
		return nil, e
	}
	t := p.peek()
	switch t.kind {
	case uxTokEq, uxTokNe, uxTokLt, uxTokLe, uxTokGt, uxTokGe:
		p.next()
		y, e2 := p.parseOperand()
		if e2 != nil {
			return nil, e2
		}
		return &uxCond{kind: "cmp", op: t.text, ops: []uxOperand{x, y}}, nil
	}
	if p.isKw(t, "BETWEEN") {
		p.next()
		lo, e2 := p.parseOperand()
		if e2 != nil {
			return nil, e2
		}
		if !p.isKw(p.peek(), "AND") {
			return nil, p.syntax(p.peek().text)
		}
		p.next()
		hi, e3 := p.parseOperand()
		if e3 != nil {
			return nil, e3
		}
		return &uxCond{kind: "between", ops: []uxOperand{x, lo, hi}}, nil
	}
	if p.isKw(t, "IN") {
		p.next()
		if p.next().kind != uxTokLParen {
			return nil, p.syntax("(")
		}
		var ops []uxOperand
		for {
			o, e2 := p.parseOperand()
			if e2 != nil {
				return nil, e2
			}
			ops = append(ops, o)
			if p.peek().kind == uxTokComma {
				p.next()
				continue
			}
			break
		}
		if p.next().kind != uxTokRParen {
			return nil, p.syntax(")")
		}
		if len(ops) > 100 {
			return nil, p.fail("The IN list must have between 1 and 100 items")
		}
		return &uxCond{kind: "in", ops: ops}, nil
	}
	return nil, p.syntax(t.text)
}

func (p *uxParser) parseOperand() (uxOperand, *uxErr) {
	t := p.peek()
	if t.kind == uxTokVal {
		p.next()
		p.usedV[t.text] = true
		return uxOperand{isVal: true, val: t.text}, nil
	}
	if t.kind == uxTokWord && strings.EqualFold(t.text, "size") && p.peekAt(1).kind == uxTokLParen {
		p.next()
		p.next()
		inner, e := p.parseOperand()
		if e != nil {
			return uxOperand{}, e
		}
		if p.next().kind != uxTokRParen {
			return uxOperand{}, p.syntax(p.peek().text)
		}
		return uxOperand{size: &inner}, nil
	}
	path, e := p.parsePath()
	if e != nil {
		return uxOperand{}, e
	}
	return uxOperand{path: path}, nil
}

// ------------------------------------------------------------- traversal

func uxGetPath(root map[string]uxAV, path uxPath) (uxAV, bool) {
	if root == nil || len(path.comps) == 0 {
		return nil, false
	}
	first := path.comps[0]
	if first.isIdx {
		return nil, false
	}
	v, ok := root[first.attr]
	if !ok || v == nil {
		return nil, false
	}
	cur := any(v)
	for _, c := range path.comps[1:] {
		if c.isIdx {
			m, ok := cur.(map[string]any)
			if !ok {
				return nil, false
			}
			lv, ok := m["L"]
			if !ok {
				return nil, false
			}
			arr, ok := lv.([]any)
			if !ok {
				return nil, false
			}
			if c.idx < 0 || c.idx >= len(arr) {
				return nil, false
			}
			cur = arr[c.idx]
		} else {
			m, ok := cur.(map[string]any)
			if !ok {
				return nil, false
			}
			mv, ok := m["M"]
			if !ok {
				return nil, false
			}
			sub, ok := mv.(map[string]any)
			if !ok {
				return nil, false
			}
			nv, ok := sub[c.attr]
			if !ok || nv == nil {
				return nil, false
			}
			cur = nv
		}
	}
	av, ok := cur.(map[string]any)
	if !ok {
		return nil, false
	}
	return av, true
}

func uxOverlaps(a, b uxPath) bool {
	k := len(a.comps)
	if len(b.comps) < k {
		k = len(b.comps)
	}
	for i := 0; i < k; i++ {
		x, y := a.comps[i], b.comps[i]
		if x.isIdx != y.isIdx {
			return false
		}
		if x.isIdx {
			if x.idx != y.idx {
				return false
			}
		} else if x.attr != y.attr {
			return false
		}
	}
	return true
}

func uxKeyName(path uxPath) string {
	for i := len(path.comps) - 1; i >= 0; i-- {
		if !path.comps[i].isIdx {
			return path.comps[i].attr
		}
	}
	return ""
}

// ---------------------------------------------------------------- values

func uxTypeOf(v map[string]any) string {
	if v == nil {
		return ""
	}
	for _, k := range []string{"S", "N", "B", "BOOL", "NULL", "L", "M", "SS", "NS", "BS"} {
		if _, ok := v[k]; ok {
			return k
		}
	}
	return ""
}

func uxNumOf(v map[string]any) (*big.Rat, *uxErr) {
	s, _ := v["N"].(string)
	r, ok := new(big.Rat).SetString(strings.TrimSpace(s))
	if !ok {
		return nil, uxValid("The provided parameter is not a valid number: %s", s)
	}
	return r, nil
}

func uxRatString(r *big.Rat) string {
	if r.Sign() == 0 {
		return "0"
	}
	if r.IsInt() {
		return r.Num().String()
	}
	t := new(big.Int).Set(r.Denom())
	two := big.NewInt(2)
	five := big.NewInt(5)
	d2, d5 := 0, 0
	for {
		q, rem := new(big.Int).QuoRem(t, two, new(big.Int))
		if rem.Sign() != 0 {
			break
		}
		t = q
		d2++
	}
	for {
		q, rem := new(big.Int).QuoRem(t, five, new(big.Int))
		if rem.Sign() != 0 {
			break
		}
		t = q
		d5++
	}
	d := d2
	if d5 > d {
		d = d5
	}
	if t.Cmp(big.NewInt(1)) != 0 {
		d = 18
	}
	out := r.FloatString(d)
	out = strings.TrimRight(out, "0")
	out = strings.TrimSuffix(out, ".")
	if out == "" || out == "-" {
		return "0"
	}
	return out
}

func uxNumEval(a, b, op string) (string, *uxErr) {
	ra, ok := new(big.Rat).SetString(strings.TrimSpace(a))
	if !ok {
		return "", uxValid("The provided parameter is not a valid number: %s", a)
	}
	rb, ok := new(big.Rat).SetString(strings.TrimSpace(b))
	if !ok {
		return "", uxValid("The provided parameter is not a valid number: %s", b)
	}
	if op == "-" {
		return uxRatString(new(big.Rat).Sub(ra, rb)), nil
	}
	return uxRatString(new(big.Rat).Add(ra, rb)), nil
}

func uxDecodeB(s string) string {
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return s
	}
	return string(raw)
}

func uxBytesOf(v map[string]any) string {
	s, _ := v["B"].(string)
	return uxDecodeB(s)
}

func uxSetKey(x any, setType string) (string, *uxErr) {
	bad := uxValid("An operand in the update expression has an incorrect data type")
	switch setType {
	case "SS":
		s, ok := x.(string)
		if !ok {
			return "", bad
		}
		return s, nil
	case "NS":
		s, ok := x.(string)
		if !ok {
			return "", bad
		}
		r, ok2 := new(big.Rat).SetString(strings.TrimSpace(s))
		if !ok2 {
			return "", bad
		}
		return uxRatString(r), nil
	case "BS":
		s, ok := x.(string)
		if !ok {
			return "", bad
		}
		raw, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return s, nil
		}
		return string(raw), nil
	}
	return "", bad
}

func uxAddVals(old, opv map[string]any) (map[string]any, *uxErr) {
	bad := uxValid("An operand in the update expression has an incorrect data type")
	told, top := uxTypeOf(old), uxTypeOf(opv)
	if told == "" {
		switch top {
		case "N":
			s, _ := opv["N"].(string)
			return map[string]any{"N": s}, nil
		case "SS":
			arr, _ := opv["SS"].([]any)
			return map[string]any{"SS": append([]any{}, arr...)}, nil
		case "NS":
			arr, _ := opv["NS"].([]any)
			return map[string]any{"NS": append([]any{}, arr...)}, nil
		case "BS":
			arr, _ := opv["BS"].([]any)
			return map[string]any{"BS": append([]any{}, arr...)}, nil
		}
		return nil, bad
	}
	if told == "N" {
		if top != "N" {
			return nil, bad
		}
		on, e := uxNumOf(old)
		if e != nil {
			return nil, e
		}
		ov, e := uxNumOf(opv)
		if e != nil {
			return nil, e
		}
		return map[string]any{"N": uxRatString(new(big.Rat).Add(on, ov))}, nil
	}
	switch told {
	case "SS", "BS", "NS":
		if top != told {
			return nil, bad
		}
		oldArr, _ := old[told].([]any)
		opArr, _ := opv[top].([]any)
		seen := map[string]bool{}
		out := make([]any, 0, len(oldArr)+len(opArr))
		for _, x := range oldArr {
			k, e := uxSetKey(x, told)
			if e != nil {
				return nil, e
			}
			if !seen[k] {
				seen[k] = true
				out = append(out, x)
			}
		}
		for _, x := range opArr {
			k, e := uxSetKey(x, top)
			if e != nil {
				return nil, e
			}
			if !seen[k] {
				seen[k] = true
				out = append(out, x)
			}
		}
		return map[string]any{told: out}, nil
	}
	return nil, bad
}

func uxDeleteVals(old, opv map[string]any) (map[string]any, *uxErr) {
	bad := uxValid("An operand in the update expression has an incorrect data type")
	told, top := uxTypeOf(old), uxTypeOf(opv)
	if told != top || (told != "SS" && told != "NS" && told != "BS") {
		return nil, bad
	}
	remove := map[string]bool{}
	opArr, _ := opv[top].([]any)
	for _, x := range opArr {
		k, e := uxSetKey(x, top)
		if e != nil {
			return nil, e
		}
		remove[k] = true
	}
	oldArr, _ := old[told].([]any)
	out := make([]any, 0, len(oldArr))
	for _, x := range oldArr {
		k, e := uxSetKey(x, told)
		if e != nil {
			return nil, e
		}
		if !remove[k] {
			out = append(out, x)
		}
	}
	return map[string]any{told: out}, nil
}

func uxSizeOf(v map[string]any) (int, *uxErr) {
	t := uxTypeOf(v)
	switch t {
	case "S":
		s, _ := v["S"].(string)
		return len(s), nil
	case "N":
		s, _ := v["N"].(string)
		return len(s), nil
	case "B":
		raw, err := base64.StdEncoding.DecodeString(uxBytesOf(v))
		if err != nil {
			s, _ := v["B"].(string)
			return len(s), nil
		}
		return len(raw), nil
	case "L":
		arr, _ := v["L"].([]any)
		return len(arr), nil
	case "M":
		m, _ := v["M"].(map[string]any)
		return len(m), nil
	case "SS", "NS", "BS":
		arr, _ := v[t].([]any)
		return len(arr), nil
	}
	return 0, uxValid("Incorrect operand type for operator or function; operator or function: size, operand type: %s", t)
}

func uxCloneVal(v map[string]any) map[string]any {
	if v == nil {
		return nil
	}
	out := make(map[string]any, len(v))
	for k, val := range v {
		switch x := val.(type) {
		case map[string]any:
			out[k] = uxCloneVal(x)
		case []any:
			out[k] = uxCloneSlice(x)
		default:
			out[k] = val
		}
	}
	return out
}

func uxCloneSlice(s []any) []any {
	out := make([]any, len(s))
	for i, x := range s {
		switch e := x.(type) {
		case map[string]any:
			out[i] = uxCloneVal(e)
		case []any:
			out[i] = uxCloneSlice(e)
		default:
			out[i] = x
		}
	}
	return out
}

// --------------------------------------------------------- update apply

type uxApply struct {
	root   map[string]uxAV
	values map[string]uxAV
	oldOut map[string]uxAV
	newOut map[string]uxAV
}

func (a *uxApply) recOld(key string, v map[string]any) {
	if v == nil {
		return
	}
	a.oldOut[key] = uxCloneVal(v)
}

func (a *uxApply) recNew(key string, v map[string]any) {
	if v == nil {
		return
	}
	a.newOut[key] = uxCloneVal(v)
}

func (a *uxApply) evalSet(sv *uxSetVal) (map[string]any, *uxErr) {
	switch sv.kind {
	case "op":
		o := sv.opnd
		if o.isVal {
			v, ok := a.values[o.val]
			if !ok || v == nil {
				return nil, uxValid("An expression attribute value used in the expression is not defined")
			}
			return v, nil
		}
		v, ok := uxGetPath(a.root, o.path)
		if !ok {
			return nil, uxValid("The provided expression refers to an attribute that does not exist in the item")
		}
		return v, nil
	case "arith":
		av, e := a.evalSet(sv.a)
		if e != nil {
			return nil, e
		}
		bv, e := a.evalSet(sv.b)
		if e != nil {
			return nil, e
		}
		an, ok1 := av["N"].(string)
		bn, ok2 := bv["N"].(string)
		if !ok1 || !ok2 {
			t := uxTypeOf(av)
			if ok1 {
				t = uxTypeOf(bv)
			}
			return nil, uxValid("Incorrect operand type for operator or function; operator or function: %s, operand type: %s", sv.op, t)
		}
		s, e := uxNumEval(an, bn, sv.op)
		if e != nil {
			return nil, e
		}
		return map[string]any{"N": s}, nil
	case "ifne":
		if v, ok := uxGetPath(a.root, sv.opnd.path); ok && v != nil {
			return v, nil
		}
		return a.evalSet(sv.a)
	case "append":
		av, e := a.evalSet(sv.a)
		if e != nil {
			return nil, e
		}
		bv, e := a.evalSet(sv.b)
		if e != nil {
			return nil, e
		}
		if uxTypeOf(av) != "L" || uxTypeOf(bv) != "L" {
			t := uxTypeOf(av)
			if t == "L" {
				t = uxTypeOf(bv)
			}
			return nil, uxValid("Incorrect operand type for operator or function; operator or function: list_append, operand type: %s", t)
		}
		la, _ := av["L"].([]any)
		lb, _ := bv["L"].([]any)
		out := make([]any, 0, len(la)+len(lb))
		out = append(out, la...)
		out = append(out, lb...)
		return map[string]any{"L": out}, nil
	}
	return nil, uxValid("Invalid UpdateExpression: invalid value")
}

func (a *uxApply) run(act uxAction) *uxErr {
	comps := act.path.comps
	if len(comps) == 0 {
		return uxInvalidPath()
	}
	var cur map[string]any // current AV along the path; nil = at item root
	for i := 0; i < len(comps)-1; i++ {
		c := comps[i]
		if c.isIdx {
			if cur == nil {
				return uxInvalidPath()
			}
			lv, ok := cur["L"]
			if !ok {
				return uxInvalidPath()
			}
			arr, ok := lv.([]any)
			if !ok {
				return uxInvalidPath()
			}
			if c.idx < 0 || c.idx >= len(arr) {
				return uxInvalidPath()
			}
			v := arr[c.idx]
			if v == nil {
				return uxInvalidPath()
			}
			cur = v
			continue
		}
		var m map[string]any
		if cur == nil {
			m = a.root
		} else {
			mv, ok := cur["M"]
			if !ok {
				return uxInvalidPath()
			}
			m, ok = mv.(map[string]any)
			if !ok {
				return uxInvalidPath()
			}
		}
		v, ok := m[c.attr]
		if !ok || v == nil {
			if act.op == "SET" || act.op == "ADD" {
				nv := map[string]any{"M": map[string]any{}}
				m[c.attr] = nv
				cur = nv
			} else {
				return nil // REMOVE / DELETE on a missing document path is a no-op
			}
			continue
		}
		cur = v
	}
	lc := comps[len(comps)-1]
	if lc.isIdx {
		if cur == nil {
			return uxInvalidPath()
		}
		lv, ok := cur["L"]
		if !ok {
			return uxInvalidPath()
		}
		arr, ok := lv.([]any)
		if !ok {
			return uxInvalidPath()
		}
		if lc.idx < 0 || lc.idx >= len(arr) {
			return uxInvalidPath()
		}
		key := uxKeyName(act.path)
		switch act.op {
		case "SET":
			v, e := a.evalSet(act.set)
			if e != nil {
				return e
			}
			if om, ok2 := arr[lc.idx].(map[string]any); ok2 {
				a.recOld(key, om)
			}
			arr[lc.idx] = uxCloneVal(v)
			a.recNew(key, v)
		case "REMOVE":
			if om, ok2 := arr[lc.idx].(map[string]any); ok2 {
				a.recOld(key, om)
			}
			nArr := make([]any, 0, len(arr)-1)
			nArr = append(nArr, arr[:lc.idx]...)
			nArr = append(nArr, arr[lc.idx+1:]...)
			cur["L"] = nArr
		default:
			return uxInvalidPath()
		}
		return nil
	}
	var m map[string]any
	if cur == nil {
		m = a.root
	} else {
		mv, ok := cur["M"]
		if !ok {
			return uxInvalidPath()
		}
		m, ok = mv.(map[string]any)
		if !ok {
			return uxInvalidPath()
		}
	}
	key := uxKeyName(act.path)
	switch act.op {
	case "SET":
		v, e := a.evalSet(act.set)
		if e != nil {
			return e
		}
		if old, ok := m[lc.attr]; ok && old != nil {
			a.recOld(key, old)
		}
		cl := uxCloneVal(v)
		m[lc.attr] = cl
		a.recNew(key, cl)
	case "REMOVE":
		if old, ok := m[lc.attr]; ok && old != nil {
			a.recOld(key, old)
			delete(m, lc.attr)
		}
	case "ADD":
		opv, ok := a.values[act.val]
		if !ok || opv == nil {
			return uxValid("An expression attribute value used in the expression is not defined")
		}
		old, hasOld := m[lc.attr]
		var merged map[string]any
		var e *uxErr
		if hasOld && old != nil {
			merged, e = uxAddVals(old, opv)
		} else {
			merged, e = uxAddVals(nil, opv)
		}
		if e != nil {
			return e
		}
		if hasOld && old != nil {
			a.recOld(key, old)
		}
		m[lc.attr] = merged
		a.recNew(key, merged)
	case "DELETE":
		opv, ok := a.values[act.val]
		if !ok || opv == nil {
			return uxValid("An expression attribute value used in the expression is not defined")
		}
		ot := uxTypeOf(opv)
		if ot != "SS" && ot != "NS" && ot != "BS" {
			return uxValid("An operand in the update expression has an incorrect data type")
		}
		old, hasOld := m[lc.attr]
		if !hasOld || old == nil {
			return nil
		}
		diff, e := uxDeleteVals(old, opv)
		if e != nil {
			return e
		}
		dt := uxTypeOf(diff)
		darr, _ := diff[dt].([]any)
		if len(darr) == 0 {
			delete(m, lc.attr)
		} else {
			m[lc.attr] = diff
			a.recNew(key, diff)
		}
	}
	return nil
}

// ------------------------------------------------------ condition evaluation

func uxResolveOperand(o uxOperand, root map[string]uxAV, values map[string]uxAV) (map[string]any, bool, *uxErr) {
	if o.isVal {
		v, ok := values[o.val]
		if !ok || v == nil {
			return nil, false, uxValid("An expression attribute value used in the expression is not defined")
		}
		return v, true, nil
	}
	if o.size != nil {
		v, ok, e := uxResolveOperand(*o.size, root, values)
		if e != nil {
			return nil, false, e
		}
		if !ok {
			return nil, false, nil
		}
		n, e2 := uxSizeOf(v)
		if e2 != nil {
			return nil, false, e2
		}
		return map[string]any{"N": strconv.Itoa(n)}, true, nil
	}
	v, ok := uxGetPath(root, o.path)
	return v, ok, nil
}

func uxOrder(x, y map[string]any, op string) (int, *uxErr) {
	tx, ty := uxTypeOf(x), uxTypeOf(y)
	if tx == "" || ty == "" {
		return 0, nil
	}
	if tx != ty {
		return 0, uxValid("Incorrect operand type for operator or function; operator or function: %s, operand type: %s", op, ty)
	}
	switch tx {
	case "S":
		xs, _ := x["S"].(string)
		ys, _ := y["S"].(string)
		return strings.Compare(xs, ys), nil
	case "N":
		xn, e := uxNumOf(x)
		if e != nil {
			return 0, e
		}
		yn, e := uxNumOf(y)
		if e != nil {
			return 0, e
		}
		return xn.Cmp(yn), nil
	case "B":
		return strings.Compare(uxBytesOf(x), uxBytesOf(y)), nil
	}
	return 0, uxValid("Incorrect operand type for operator or function; operator or function: %s, operand type: %s", op, tx)
}

func uxAEquals(x, y map[string]any) (bool, *uxErr) {
	tx, ty := uxTypeOf(x), uxTypeOf(y)
	if tx == "" || ty == "" || tx != ty {
		return false, nil
	}
	switch tx {
	case "S":
		xs, _ := x["S"].(string)
		ys, _ := y["S"].(string)
		return xs == ys, nil
	case "N":
		xn, e := uxNumOf(x)
		if e != nil {
			return false, e
		}
		yn, e := uxNumOf(y)
		if e != nil {
			return false, e
		}
		return xn.Cmp(yn) == 0, nil
	case "B":
		return uxBytesOf(x) == uxBytesOf(y), nil
	case "BOOL":
		xb, _ := x["BOOL"].(bool)
		yb, _ := y["BOOL"].(bool)
		return xb == yb, nil
	case "NULL":
		return true, nil
	case "L":
		la, _ := x["L"].([]any)
		lb, _ := y["L"].([]any)
		if len(la) != len(lb) {
			return false, nil
		}
		for i := range la {
			xav, ok1 := la[i].(map[string]any)
			yav, ok2 := lb[i].(map[string]any)
			if !ok1 || !ok2 {
				return false, nil
			}
			eq, e := uxAEquals(xav, yav)
			if e != nil {
				return false, e
			}
			if !eq {
				return false, nil
			}
		}
		return true, nil
	case "M":
		ma, _ := x["M"].(map[string]any)
		mb, _ := y["M"].(map[string]any)
		if len(ma) != len(mb) {
			return false, nil
		}
		for k, xv := range ma {
			xav, ok1 := xv.(map[string]any)
			yav, ok2 := mb[k].(map[string]any)
			if !ok1 || !ok2 {
				return false, nil
			}
			eq, e := uxAEquals(xav, yav)
			if e != nil {
				return false, e
			}
			if !eq {
				return false, nil
			}
		}
		return true, nil
	case "SS":
		as, _ := x["SS"].([]any)
		bs, _ := y["SS"].([]any)
		return uxMultiset(as, bs, "SS"), nil
	case "NS":
		an, _ := x["NS"].([]any)
		bn, _ := y["NS"].([]any)
		return uxMultiset(an, bn, "NS"), nil
	case "BS":
		ab, _ := x["BS"].([]any)
		bb, _ := y["BS"].([]any)
		return uxMultiset(ab, bb, "BS"), nil
	}
	return false, nil
}

func uxMultiset(a, b []any, t string) bool {
	if len(a) != len(b) {
		return false
	}
	used := make([]bool, len(b))
	for _, xv := range a {
		found := false
		for j, yv := range b {
			if used[j] {
				continue
			}
			if uxSetKeyEq(xv, yv, t) {
				used[j] = true
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func uxSetKeyEq(x, y any, t string) bool {
	xs, ok1 := x.(string)
	ys, ok2 := y.(string)
	if !ok1 || !ok2 {
		return false
	}
	if t == "SS" {
		return xs == ys
	}
	if t == "BS" {
		return uxDecodeB(xs) == uxDecodeB(ys)
	}
	rx, ok := new(big.Rat).SetString(strings.TrimSpace(xs))
	if !ok {
		return xs == ys
	}
	ry, ok := new(big.Rat).SetString(strings.TrimSpace(ys))
	if !ok {
		return xs == ys
	}
	return rx.Cmp(ry) == 0
}

func uxContainsMatch(x, y map[string]any) (bool, *uxErr) {
	tx, ty := uxTypeOf(x), uxTypeOf(y)
	if tx == "" || ty == "" {
		return false, nil
	}
	switch tx {
	case "S":
		if ty != "S" {
			return false, nil
		}
		xs, _ := x["S"].(string)
		ys, _ := y["S"].(string)
		return strings.Contains(xs, ys), nil
	case "B":
		if ty != "B" {
			return false, nil
		}
		return strings.Contains(uxBytesOf(x), uxBytesOf(y)), nil
	case "SS":
		if ty != "S" {
			return false, nil
		}
		needle, _ := y["S"].(string)
		arr, _ := x["SS"].([]any)
		for _, e := range arr {
			if s, ok := e.(string); ok && s == needle {
				return true, nil
			}
		}
		return false, nil
	case "BS":
		if ty != "B" {
			return false, nil
		}
		needle := uxBytesOf(y)
		arr, _ := x["BS"].([]any)
		for _, e := range arr {
			if s, ok := e.(string); ok && uxDecodeB(s) == needle {
				return true, nil
			}
		}
		return false, nil
	case "NS":
		if ty != "N" {
			return false, nil
		}
		yn, e := uxNumOf(y)
		if e != nil {
			return false, e
		}
		arr, _ := x["NS"].([]any)
		for _, e := range arr {
			s, ok := e.(string)
			if !ok {
				continue
			}
			r, ok2 := new(big.Rat).SetString(strings.TrimSpace(s))
			if !ok2 {
				continue
			}
			if r.Cmp(yn) == 0 {
				return true, nil
			}
		}
		return false, nil
	case "L":
		arr, _ := x["L"].([]any)
		for _, e := range arr {
			m, ok := e.(map[string]any)
			if !ok {
				continue
			}
			eq, err := uxAEquals(m, y)
			if err != nil {
				return false, err
			}
			if eq {
				return true, nil
			}
		}
		return false, nil
	}
	return false, nil
}

func uxBeginsWith(x, y map[string]any) bool {
	tx, ty := uxTypeOf(x), uxTypeOf(y)
	if tx != ty {
		return false
	}
	switch tx {
	case "S":
		xs, _ := x["S"].(string)
		ys, _ := y["S"].(string)
		return strings.HasPrefix(xs, ys)
	case "B":
		return strings.HasPrefix(uxBytesOf(x), uxBytesOf(y))
	}
	return false
}

func uxEvalCond(n *uxCond, root map[string]uxAV, values map[string]uxAV) (bool, *uxErr) {
	switch n.kind {
	case "or":
		l, e := uxEvalCond(n.kids[0], root, values)
		if e != nil {
			return false, e
		}
		if l {
			return true, nil
		}
		return uxEvalCond(n.kids[1], root, values)
	case "and":
		l, e := uxEvalCond(n.kids[0], root, values)
		if e != nil {
			return false, e
		}
		if !l {
			return false, nil
		}
		return uxEvalCond(n.kids[1], root, values)
	case "not":
		k, e := uxEvalCond(n.kids[0], root, values)
		return !k, e
	case "exists":
		_, ok := uxGetPath(root, n.ops[0].path)
		return ok, nil
	case "notexists":
		_, ok := uxGetPath(root, n.ops[0].path)
		return !ok, nil
	case "cmp":
		x, fx, e := uxResolveOperand(n.ops[0], root, values)
		if e != nil {
			return false, e
		}
		y, fy, e := uxResolveOperand(n.ops[1], root, values)
		if e != nil {
			return false, e
		}
		if !fx || !fy {
			return false, nil
		}
		if n.op == "=" || n.op == "<>" {
			eq, e := uxAEquals(x, y)
			if e != nil {
				return false, e
			}
			if n.op == "=" {
				return eq, nil
			}
			return !eq, nil
		}
		c, e := uxOrder(x, y, n.op)
		if e != nil {
			return false, e
		}
		switch n.op {
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
	case "between":
		x, fx, e := uxResolveOperand(n.ops[0], root, values)
		if e != nil {
			return false, e
		}
		lo, flo, e := uxResolveOperand(n.ops[1], root, values)
		if e != nil {
			return false, e
		}
		hi, fhi, e := uxResolveOperand(n.ops[2], root, values)
		if e != nil {
			return false, e
		}
		if !fx || !flo || !fhi {
			return false, nil
		}
		tx := uxTypeOf(x)
		if tx != "S" && tx != "N" && tx != "B" {
			return false, uxValid("Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: %s", tx)
		}
		if uxTypeOf(lo) != tx || uxTypeOf(hi) != tx {
			return false, nil
		}
		cl, e := uxOrder(x, lo, "BETWEEN")
		if e != nil {
			return false, e
		}
		ch, e := uxOrder(x, hi, "BETWEEN")
		if e != nil {
			return false, e
		}
		return cl >= 0 && ch <= 0, nil
	case "in":
		x, fx, e := uxResolveOperand(n.ops[0], root, values)
		if e != nil {
			return false, e
		}
		if !fx {
			return false, nil
		}
		for _, o := range n.ops[1:] {
			y, fy, e := uxResolveOperand(o, root, values)
			if e != nil {
				return false, e
			}
			if !fy {
				continue
			}
			eq, e := uxAEquals(x, y)
			if e != nil {
				return false, e
			}
			if eq {
				return true, nil
			}
		}
		return false, nil
	case "contains":
		x, fx, e := uxResolveOperand(n.ops[0], root, values)
		if e != nil {
			return false, e
		}
		y, fy, e := uxResolveOperand(n.ops[1], root, values)
		if e != nil {
			return false, e
		}
		if !fx || !fy {
			return false, nil
		}
		return uxContainsMatch(x, y)
	case "begins_with":
		x, fx, e := uxResolveOperand(n.ops[0], root, values)
		if e != nil {
			return false, e
		}
		y, fy, e := uxResolveOperand(n.ops[1], root, values)
		if e != nil {
			return false, e
		}
		if !fx || !fy {
			return false, nil
		}
		return uxBeginsWith(x, y), nil
	case "attribute_type":
		x, fx, e := uxResolveOperand(n.ops[0], root, values)
		if e != nil {
			return false, e
		}
		y, fy, e := uxResolveOperand(n.ops[1], root, values)
		if e != nil {
			return false, e
		}
		if !fx || !fy {
			return false, nil
		}
		ys, ok := y["S"].(string)
		if !ok {
			return false, nil
		}
		return uxTypeOf(x) == ys, nil
	}
	return false, uxValid("Invalid ConditionExpression: unsupported condition")
}