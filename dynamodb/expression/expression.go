package expression

import (
	"bytes"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
)

// ------------------------------------------------------------------ errors

// ExpressionError carries the exact DynamoDB ValidationException message.
type ExpressionError struct{ Message string }

func (e *ExpressionError) Error() string { return e.Message }

// Code returns the wire error class this maps to.
func (e *ExpressionError) Code() string { return "ValidationException" }

func exprErr(format string, args ...interface{}) *ExpressionError {
	return &ExpressionError{Message: fmt.Sprintf(format, args...)}
}

// invalidExpression builds "Invalid <Type>Expression: <text>".
func invalidExpression(exprType, text string) *ExpressionError {
	return exprErr("Invalid %sExpression: %s", exprType, text)
}

func errInvalidDocumentPath() *ExpressionError {
	return &ExpressionError{Message: "The document path provided in the expression is invalid for update"}
}

func errAttrNotExists() *ExpressionError {
	return &ExpressionError{Message: "The provided expression refers to an attribute that does not exist in the item"}
}

func syntaxError(exprType, tokenText, near string) *ExpressionError {
	return invalidExpression(exprType, fmt.Sprintf("Syntax error; token: %q, near: %q", tokenText, near))
}

func undefinedValueError(ref string) *ExpressionError {
	return exprErr("An expression attribute value used in expression is not defined; attribute value: %s", ref)
}

func undefinedNameError(ref string) *ExpressionError {
	return exprErr("An expression attribute name used in the document path is not defined; attribute name: %s", ref)
}

func operandIncorrectType(operandText string) *ExpressionError {
	return exprErr("An operand in the update expression has an incorrect type; operand: %q", operandText)
}

func incorrectOperandTypeError(operator, operandType string) *ExpressionError {
	return exprErr("Incorrect operand type for operator or function; operator or function: %s, operand type: %s", operator, operandType)
}

func reservedKeywordError(exprType, word string) *ExpressionError {
	return invalidExpression(exprType, fmt.Sprintf("Attribute name is a reserved keyword; reserved keyword: %s", word))
}

func twoPathsOverlap(path1, path2 string) *ExpressionError {
	return invalidExpression("Update", fmt.Sprintf("Two document paths overlap with each other; path1: %s, path2: %s", path1, path2))
}

func missedKeySchemaElement(attr string) *ExpressionError {
	return exprErr("Query condition missed key schema element: %s", attr)
}

func queryKeyNotSupported() *ExpressionError {
	return &ExpressionError{Message: "Query key condition not supported"}
}

func keyConditionsRequiredError() *ExpressionError {
	return &ExpressionError{Message: "Either the KeyConditions or KeyConditionExpression parameter must be specified in the request."}
}

func onlyOneConditionPerKeyError() *ExpressionError {
	return &ExpressionError{Message: "One or more parameter values were invalid: Only one condition per key is supported"}
}

func typeMismatchIndexKey(attr, expected, actual, indexName string) *ExpressionError {
	name := indexName
	if name == "" {
		name = "undefined"
	}
	return exprErr("One or more parameter values were invalid: Type mismatch for Index Key %s Expected: %s Actual: %s IndexName: %s", attr, expected, actual, name)
}

// ------------------------------------------------------------------ lexer

type tokenKind int

const (
	tokenIdent tokenKind = iota
	tokenNameRef
	tokenValueRef
	tokenLParen
	tokenRParen
	tokenComma
	tokenDot
	tokenLBracket
	tokenRBracket
	tokenEq
	tokenNe
	tokenLt
	tokenLe
	tokenGt
	tokenGe
	tokenPlus
	tokenMinus
	tokenEOF
)

type token struct {
	kind  tokenKind
	text  string
	start int
	end   int
}

func (t token) isKeyword(words ...string) bool {
	if t.kind != tokenIdent {
		return false
	}
	u := strings.ToUpper(t.text)
	for _, w := range words {
		if u == w {
			return true
		}
	}
	return false
}

func isIdentStart(c byte) bool {
	return c == '_' || c == '$' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func isIdentChar(c byte) bool { return isIdentStart(c) }

// lex tokenizes an expression. On error it returns the successfully
// tokenized prefix plus the offending token text.
func lex(src string) (tokens []token, offending string, ok bool) {
	i, n := 0, len(src)
	appendTok := func(kind tokenKind, start, end int) {
		tokens = append(tokens, token{kind: kind, text: src[start:end], start: start, end: end})
	}
	for i < n {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '#' || c == ':':
			kind := tokenNameRef
			if c == ':' {
				kind = tokenValueRef
			}
			j := i + 1
			for j < n && isIdentChar(src[j]) {
				j++
			}
			if j == i+1 {
				return tokens, string(c), false
			}
			appendTok(kind, i, j)
			i = j
		case isIdentStart(c):
			j := i
			for j < n && isIdentChar(src[j]) {
				j++
			}
			appendTok(tokenIdent, i, j)
			i = j
		default:
			if i+1 < n {
				switch src[i : i+2] {
				case "<>":
					appendTok(tokenNe, i, i+2)
					i += 2
					continue
				case "<=":
					appendTok(tokenLe, i, i+2)
					i += 2
					continue
				case ">=":
					appendTok(tokenGe, i, i+2)
					i += 2
					continue
				}
			}
			switch c {
			case '(':
				appendTok(tokenLParen, i, i+1)
			case ')':
				appendTok(tokenRParen, i, i+1)
			case ',':
				appendTok(tokenComma, i, i+1)
			case '.':
				appendTok(tokenDot, i, i+1)
			case '[':
				appendTok(tokenLBracket, i, i+1)
			case ']':
				appendTok(tokenRBracket, i, i+1)
			case '=':
				appendTok(tokenEq, i, i+1)
			case '<':
				appendTok(tokenLt, i, i+1)
			case '>':
				appendTok(tokenGt, i, i+1)
			case '+':
				appendTok(tokenPlus, i, i+1)
			case '-':
				appendTok(tokenMinus, i, i+1)
			default:
				return tokens, string(c), false
			}
			i++
		}
	}
	tokens = append(tokens, token{kind: tokenEOF, text: "", start: n, end: n})
	return tokens, "", true
}

func joinTokens(ts []token) string {
	parts := make([]string, 0, len(ts))
	for _, t := range ts {
		if t.kind == tokenEOF {
			continue
		}
		parts = append(parts, t.text)
	}
	return strings.Join(parts, " ")
}

// ------------------------------------------------------------ parser core

// ExpressionAttributes carries ExpressionAttributeNames/Values maps.
type ExpressionAttributes struct {
	Names  map[string]string
	Values map[string]*Value
}

type parser struct {
	toks     []token
	pos      int
	exprType string // "Condition", "Filter", "Update", "KeyCondition"
	attrs    *ExpressionAttributes
	item     map[string]*Value
}

func newParser(exprType string, src string, attrs *ExpressionAttributes) (*parser, *ExpressionError) {
	tokens, offending, ok := lex(src)
	p := &parser{toks: tokens, exprType: exprType, attrs: attrs}
	if !ok {
		near := joinTokens(tokens)
		if near != "" {
			near += " "
		}
		near += offending
		return p, syntaxError(exprType, offending, near)
	}
	return p, nil
}

func (p *parser) cur() token { return p.toks[p.pos] }

func (p *parser) tkAt(off int) token {
	i := p.pos + off
	if i >= len(p.toks) {
		return p.toks[len(p.toks)-1]
	}
	return p.toks[i]
}

func (p *parser) advance() token {
	t := p.cur()
	if p.pos < len(p.toks)-1 {
		p.pos++
	}
	return t
}

func (p *parser) atEnd() bool { return p.cur().kind == tokenEOF }

// errorHere reports a syntax error at the current token. The near text is the
// remainder of the expression starting at the offending token.
func (p *parser) errorHere() *ExpressionError {
	return syntaxError(p.exprType, p.cur().text, joinTokens(p.toks[p.pos:]))
}

func (p *parser) valueOf(ref string) (*Value, bool) {
	if p.attrs == nil || p.attrs.Values == nil {
		return nil, false
	}
	v, ok := p.attrs.Values[ref]
	if !ok || v == nil {
		return nil, false
	}
	return v, true
}

func (p *parser) nameOf(ref string) (string, bool) {
	if p.attrs == nil || p.attrs.Names == nil {
		return "", false
	}
	n, ok := p.attrs.Names[ref]
	if !ok || n == "" {
		return "", false
	}
	return n, true
}

func (p *parser) incorrectOperandFunc(fn, t string) *ExpressionError {
	return invalidExpression(p.exprType, fmt.Sprintf("Incorrect operand type for operator or function; operator or function: %s, operand type: %s", fn, t))
}

var grammarKeywords = map[string]bool{
	"AND": true, "OR": true, "NOT": true, "IN": true, "BETWEEN": true,
	"SET": true, "REMOVE": true, "ADD": true, "DELETE": true,
}

// ------------------------------------------------------------ document paths

type PathSegment struct {
	Name    string // resolved attribute name
	Index   int
	IsIndex bool
	raw     string // original token text (#ref kept verbatim)
	isBare  bool   // segment was an unescaped identifier
}

type DocumentPath struct{ Segments []PathSegment }

// String renders the resolved path (document path text used in errors).
func (dp *DocumentPath) String() string {
	var sb strings.Builder
	for i, seg := range dp.Segments {
		if i > 0 {
			if seg.IsIndex {
				fmt.Fprintf(&sb, "[%d]", seg.Index)
				continue
			}
			sb.WriteString(".")
		}
		name := seg.Name
		if name == "" {
			name = seg.raw
		}
		sb.WriteString(name)
	}
	return sb.String()
}

func segmentsKey(segs []PathSegment) string {
	var sb strings.Builder
	for _, seg := range segs {
		if seg.IsIndex {
			fmt.Fprintf(&sb, "[%d]", seg.Index)
			continue
		}
		sb.WriteString(".")
		sb.WriteString(seg.Name)
	}
	return sb.String()
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// parseDocumentPath parses a.b[0].#c style document paths.
func (p *parser) parseDocumentPath() (*DocumentPath, *ExpressionError) {
	t := p.cur()
	if t.kind != tokenIdent && t.kind != tokenNameRef {
		return nil, p.errorHere()
	}
	if t.kind == tokenIdent && grammarKeywords[strings.ToUpper(t.text)] {
		return nil, p.errorHere()
	}
	p.advance()
	segs := []PathSegment{{raw: t.text, isBare: t.kind == tokenIdent}}
	for {
		switch p.cur().kind {
		case tokenDot:
			p.advance()
			t2 := p.cur()
			if t2.kind != tokenIdent && t2.kind != tokenNameRef {
				return nil, p.errorHere()
			}
			if t2.kind == tokenIdent && grammarKeywords[strings.ToUpper(t2.text)] {
				return nil, p.errorHere()
			}
			p.advance()
			segs = append(segs, PathSegment{raw: t2.text, isBare: t2.kind == tokenIdent})
		case tokenLBracket:
			p.advance()
			t2 := p.cur()
			if t2.kind != tokenIdent || !isDigits(t2.text) {
				return nil, p.errorHere()
			}
			idx, convErr := strconv.Atoi(t2.text)
			if convErr != nil {
				return nil, p.errorHere()
			}
			p.advance()
			if p.cur().kind != tokenRBracket {
				return nil, p.errorHere()
			}
			p.advance()
			segs = append(segs, PathSegment{Index: idx, IsIndex: true, raw: t2.text})
		default:
			return &DocumentPath{Segments: segs}, nil
		}
	}
}

// resolvePath expands #placeholders and rejects bare reserved keywords.
func (p *parser) resolvePath(dp *DocumentPath) (*DocumentPath, *ExpressionError) {
	out := &DocumentPath{Segments: make([]PathSegment, len(dp.Segments))}
	copy(out.Segments, dp.Segments)
	for i, seg := range dp.Segments {
		if seg.IsIndex {
			continue
		}
		if seg.isBare {
			if isReservedWord(seg.raw) {
				return nil, reservedKeywordError(p.exprType, seg.raw)
			}
			out.Segments[i].Name = seg.raw
		} else {
			n, ok := p.nameOf(seg.raw)
			if !ok {
				return nil, undefinedNameError(seg.raw)
			}
			out.Segments[i].Name = n
		}
	}
	return out, nil
}

// readPath resolves a path tolerantly (condition semantics): missing or
// invalid steps simply mean "absent".
func readPath(item map[string]*Value, dp *DocumentPath) (*Value, bool) {
	cur, ok := item[dp.Segments[0].Name]
	if !ok {
		return nil, false
	}
	for i := 1; i < len(dp.Segments); i++ {
		if cur == nil {
			return nil, false
		}
		seg := dp.Segments[i]
		if seg.IsIndex {
			if cur.Kind != KindList || seg.Index >= len(cur.L) {
				return nil, false
			}
			cur = cur.L[seg.Index]
		} else {
			if cur.Kind != KindMap {
				return nil, false
			}
			v, ok2 := cur.M[seg.Name]
			if !ok2 {
				return nil, false
			}
			cur = v
		}
		if cur == nil {
			return nil, false
		}
	}
	return cur, true
}

// readPathStrict resolves a path with update-time errors.
func readPathStrict(item map[string]*Value, dp *DocumentPath) (*Value, *ExpressionError) {
	cur, ok := item[dp.Segments[0].Name]
	if !ok {
		return nil, errAttrNotExists()
	}
	for i := 1; i < len(dp.Segments); i++ {
		if cur == nil {
			return nil, errAttrNotExists()
		}
		seg := dp.Segments[i]
		if seg.IsIndex {
			if cur.Kind != KindList {
				return nil, errInvalidDocumentPath()
			}
			if seg.Index >= len(cur.L) {
				return nil, errAttrNotExists()
			}
			cur = cur.L[seg.Index]
		} else {
			if cur.Kind != KindMap {
				return nil, errInvalidDocumentPath()
			}
			v, ok2 := cur.M[seg.Name]
			if !ok2 {
				return nil, errAttrNotExists()
			}
			cur = v
		}
	}
	return cur, nil
}

func cloneItem(item map[string]*Value) map[string]*Value {
	out := make(map[string]*Value, len(item))
	for k, v := range item {
		out[k] = v.Clone()
	}
	return out
}

// ------------------------------------------------------- write operations

type slot struct {
	m     map[string]*Value // containing map (root or nested)
	name  string
	list  *Value // containing list (Kind == KindList)
	index int
}

func (s *slot) get() *Value {
	if s.list != nil {
		return s.list.L[s.index]
	}
	return s.m[s.name]
}

func (s *slot) set(v *Value) {
	if s.list != nil {
		s.list.L[s.index] = v
	} else {
		s.m[s.name] = v
	}
}

func (s *slot) remove() {
	if s.list != nil {
		s.list.L = append(s.list.L[:s.index], s.list.L[s.index+1:]...)
		return
	}
	delete(s.m, s.name)
}

// createListOrMap creates a fresh container appropriate for the next path
// segment. Fresh lists get a nil placeholder element at index 0, which is
// materialized when descended into.
func createListOrMap(next PathSegment) *Value {
	if next.IsIndex {
		return &Value{Kind: KindList, L: []*Value{nil}}
	}
	return &Value{Kind: KindMap, M: map[string]*Value{}}
}

// setVal assigns v at segs in m following DynamoDB creation rules for SET:
// missing maps are created; missing lists are created only when the very
// first element (index 0) is targeted.
func setVal(m map[string]*Value, segs []PathSegment, v *Value) *ExpressionError {
	head := segs[0]
	if len(segs) == 1 {
		m[head.Name] = v.Clone()
		return nil
	}
	child, ok := m[head.Name]
	if !ok {
		child = createListOrMap(segs[1])
		m[head.Name] = child
	}
	return setDescend(child, segs[1:], v)
}

func setDescend(child *Value, segs []PathSegment, v *Value) *ExpressionError {
	head := segs[0]
	if len(segs) == 1 {
		switch child.Kind {
		case KindMap:
			if head.IsIndex {
				return errInvalidDocumentPath()
			}
			child.M[head.Name] = v.Clone()
			return nil
		case KindList:
			if !head.IsIndex {
				return errInvalidDocumentPath()
			}
			if head.Index >= len(child.L) {
				return errAttrNotExists()
			}
			child.L[head.Index] = v.Clone()
			return nil
		default:
			return errInvalidDocumentPath()
		}
	}
	var el *Value
	switch child.Kind {
	case KindMap:
		if head.IsIndex {
			return errInvalidDocumentPath()
		}
		el = child.M[head.Name]
		if el == nil {
			el = createListOrMap(segs[1])
			child.M[head.Name] = el
		}
	case KindList:
		if !head.IsIndex {
			return errInvalidDocumentPath()
		}
		if head.Index >= len(child.L) {
			return errAttrNotExists()
		}
		el = child.L[head.Index]
		if el == nil {
			el = createListOrMap(segs[1])
			child.L[head.Index] = el
		}
	default:
		return errInvalidDocumentPath()
	}
	return setDescend(el, segs[1:], v)
}

// removeVal deletes the subtree at segs; missing paths are no-ops.
func removeVal(m map[string]*Value, segs []PathSegment) *ExpressionError {
	head := segs[0]
	if !head.IsIndex {
		if len(segs) == 1 {
			delete(m, head.Name)
			return nil
		}
		child, ok := m[head.Name]
		if !ok {
			return nil
		}
		return removeDescend(child, segs[1:])
	}
	return errInvalidDocumentPath()
}

func removeDescend(child *Value, segs []PathSegment) *ExpressionError {
	if len(segs) == 1 {
		seg := segs[0]
		switch child.Kind {
		case KindMap:
			if seg.IsIndex {
				return errInvalidDocumentPath()
			}
			delete(child.M, seg.Name)
			return nil
		case KindList:
			if !seg.IsIndex {
				return errInvalidDocumentPath()
			}
			if seg.Index >= len(child.L) {
				return nil
			}
			child.L = append(child.L[:seg.Index], child.L[seg.Index+1:]...)
			return nil
		default:
			return errInvalidDocumentPath()
		}
	}
	switch child.Kind {
	case KindMap:
		if segs[0].IsIndex {
			return errInvalidDocumentPath()
		}
		el, ok := child.M[segs[0].Name]
		if !ok {
			return nil
		}
		return removeDescend(el, segs[1:])
	case KindList:
		if !segs[0].IsIndex {
			return errInvalidDocumentPath()
		}
		if segs[0].Index >= len(child.L) {
			return nil
		}
		el := child.L[segs[0].Index]
		if el == nil {
			return nil
		}
		return removeDescend(el, segs[1:])
	default:
		return errInvalidDocumentPath()
	}
}

// addSlotPath locates (creating missing containers like SET does) the target
// slot of an ADD action.
func addSlotPath(m map[string]*Value, segs []PathSegment) (*slot, *ExpressionError) {
	head := segs[0]
	if len(segs) == 1 {
		return &slot{m: m, name: head.Name}, nil
	}
	child, ok := m[head.Name]
	if !ok {
		child = createListOrMap(segs[1])
		m[head.Name] = child
	}
	return addSlotDescend(child, segs[1:])
}

func addSlotDescend(child *Value, segs []PathSegment) (*slot, *ExpressionError) {
	head := segs[0]
	if len(segs) == 1 {
		switch child.Kind {
		case KindMap:
			if head.IsIndex {
				return nil, errInvalidDocumentPath()
			}
			return &slot{m: child.M, name: head.Name}, nil
		case KindList:
			if !head.IsIndex {
				return nil, errInvalidDocumentPath()
			}
			if head.Index >= len(child.L) {
				return nil, errAttrNotExists()
			}
			return &slot{list: child, index: head.Index}, nil
		default:
			return nil, errInvalidDocumentPath()
		}
	}
	var el *Value
	switch child.Kind {
	case KindMap:
		if head.IsIndex {
			return nil, errInvalidDocumentPath()
		}
		el = child.M[head.Name]
		if el == nil {
			el = createListOrMap(segs[1])
			child.M[head.Name] = el
		}
	case KindList:
		if !head.IsIndex {
			return nil, errInvalidDocumentPath()
		}
		if head.Index >= len(child.L) {
			return nil, errAttrNotExists()
		}
		el = child.L[head.Index]
		if el == nil {
			el = createListOrMap(segs[1])
			child.L[head.Index] = el
		}
	default:
		return nil, errInvalidDocumentPath()
	}
	return addSlotDescend(el, segs[1:])
}

// deleteSlotPath locates the target of a DELETE action without creating
// anything; missing intermediates yield a nil slot (no-op).
func deleteSlotPath(m map[string]*Value, segs []PathSegment) (*slot, *ExpressionError) {
	head := segs[0]
	if len(segs) == 1 {
		return &slot{m: m, name: head.Name}, nil
	}
	child, ok := m[head.Name]
	if !ok {
		return nil, nil
	}
	return deleteSlotDescend(child, segs[1:])
}

func deleteSlotDescend(child *Value, segs []PathSegment) (*slot, *ExpressionError) {
	head := segs[0]
	if len(segs) == 1 {
		switch child.Kind {
		case KindMap:
			if head.IsIndex {
				return nil, errInvalidDocumentPath()
			}
			return &slot{m: child.M, name: head.Name}, nil
		case KindList:
			if !head.IsIndex {
				return nil, errInvalidDocumentPath()
			}
			if head.Index >= len(child.L) {
				return nil, nil
			}
			return &slot{list: child, index: head.Index}, nil
		default:
			return nil, errInvalidDocumentPath()
		}
	}
	var el *Value
	switch child.Kind {
	case KindMap:
		if head.IsIndex {
			return nil, errInvalidDocumentPath()
		}
		el = child.M[head.Name]
		if el == nil {
			return nil, nil
		}
	case KindList:
		if !head.IsIndex {
			return nil, errInvalidDocumentPath()
		}
		if head.Index >= len(child.L) {
			return nil, nil
		}
		el = child.L[head.Index]
		if el == nil {
			return nil, nil
		}
	default:
		return nil, errInvalidDocumentPath()
	}
	return deleteSlotDescend(el, segs[1:])
}

func applyAdd(sl *slot, v *Value, pathText string) *ExpressionError {
	cur := sl.get()
	if v.Kind == KindNumber {
		if cur == nil {
			sl.set(NewNumber(v.Str))
			return nil
		}
		if cur.Kind != KindNumber {
			return incorrectOperandTypeError("ADD", cur.Kind.String())
		}
		r1, ok1 := parseDecimal(cur.Str)
		r2, ok2 := parseDecimal(v.Str)
		if !ok1 || !ok2 {
			return operandIncorrectType(pathText)
		}
		sl.set(NewNumber(numberString(new(big.Rat).Add(r1, r2))))
		return nil
	}
	if v.IsSet() {
		if cur == nil {
			if setLength(v) == 0 {
				return nil
			}
			sl.set(v.Clone())
			return nil
		}
		if cur.Kind != v.Kind {
			return incorrectOperandTypeError("ADD", cur.Kind.String())
		}
		sl.set(setUnion(cur, v))
		return nil
	}
	return incorrectOperandTypeError("ADD", v.Kind.String())
}

func applyDelete(sl *slot, v *Value) *ExpressionError {
	if !v.IsSet() {
		return incorrectOperandTypeError("DELETE", v.Kind.String())
	}
	cur := sl.get()
	if cur == nil {
		return nil
	}
	if cur.Kind != v.Kind {
		return incorrectOperandTypeError("DELETE", cur.Kind.String())
	}
	rest := setSubtract(cur, v)
	if setLength(rest) == 0 {
		sl.remove()
		return nil
	}
	sl.set(rest)
	return nil
}

// ---------------------------------------------------- condition evaluation

type condOperand struct {
	isValue  bool
	value    *Value
	path     *DocumentPath
	isSize   bool
	sizePath *DocumentPath
	raw      string
}

func (p *parser) evalCondOperand(o *condOperand) (*Value, bool, *ExpressionError) {
	if o.isValue {
		return o.value.Clone(), true, nil
	}
	if o.isSize {
		v, ok := readPath(p.item, o.sizePath)
		if !ok {
			return nil, false, nil
		}
		switch v.Kind {
		case KindString:
			return NewNumber(strconv.Itoa(len(v.Str))), true, nil
		case KindBinary:
			return NewNumber(strconv.Itoa(len(v.Bin))), true, nil
		case KindStringSet:
			return NewNumber(strconv.Itoa(len(v.SS))), true, nil
		case KindNumberSet:
			return NewNumber(strconv.Itoa(len(v.NS))), true, nil
		case KindBinarySet:
			return NewNumber(strconv.Itoa(len(v.BS))), true, nil
		case KindList:
			return NewNumber(strconv.Itoa(len(v.L))), true, nil
		case KindMap:
			return NewNumber(strconv.Itoa(len(v.M))), true, nil
		default:
			return nil, false, p.incorrectOperandFunc("size", v.Kind.String())
		}
	}
	v, ok := readPath(p.item, o.path)
	if !ok {
		return nil, false, nil
	}
	return v.Clone(), true, nil
}

func (p *parser) evalPlainOperand(o *condOperand) (*Value, bool) {
	if o.isValue {
		return o.value.Clone(), true
	}
	v, ok := readPath(p.item, o.path)
	if !ok {
		return nil, false
	}
	return v.Clone(), true
}

func (p *parser) parseCondOperand() (*condOperand, *ExpressionError) {
	t := p.cur()
	if t.kind == tokenValueRef {
		p.advance()
		v, ok := p.valueOf(t.text)
		if !ok {
			return nil, undefinedValueError(t.text)
		}
		return &condOperand{isValue: true, value: v, raw: t.text}, nil
	}
	if t.kind == tokenIdent && p.tkAt(1).kind == tokenLParen && strings.ToLower(t.text) == "size" {
		return p.parseSizeOperand()
	}
	if t.kind == tokenIdent || t.kind == tokenNameRef {
		raw, err := p.parseDocumentPath()
		if err != nil {
			return nil, err
		}
		dp, err := p.resolvePath(raw)
		if err != nil {
			return nil, err
		}
		return &condOperand{path: dp, raw: dp.String()}, nil
	}
	return nil, p.errorHere()
}

func (p *parser) parseSizeOperand() (*condOperand, *ExpressionError) {
	p.advance() // size
	p.advance() // (
	t := p.cur()
	if t.kind == tokenValueRef {
		p.advance()
		v, ok := p.valueOf(t.text)
		if !ok {
			return nil, undefinedValueError(t.text)
		}
		return nil, p.incorrectOperandFunc("size", v.Kind.String())
	}
	raw, err := p.parseDocumentPath()
	if err != nil {
		return nil, err
	}
	rdp, err := p.resolvePath(raw)
	if err != nil {
		return nil, err
	}
	if p.cur().kind != tokenRParen {
		return nil, p.errorHere()
	}
	p.advance()
	return &condOperand{isSize: true, sizePath: rdp, raw: "size(" + rdp.String() + ")"}, nil
}

func (p *parser) parseOr() (bool, *ExpressionError) {
	b, err := p.parseAnd()
	if err != nil {
		return false, err
	}
	for p.cur().isKeyword("OR") {
		p.advance()
		r, err2 := p.parseAnd()
		if err2 != nil {
			return false, err2
		}
		b = b || r
	}
	return b, nil
}

func (p *parser) parseAnd() (bool, *ExpressionError) {
	b, err := p.parsePrimary()
	if err != nil {
		return false, err
	}
	for p.cur().isKeyword("AND") {
		p.advance()
		r, err2 := p.parsePrimary()
		if err2 != nil {
			return false, err2
		}
		b = b && r
	}
	return b, nil
}

func (p *parser) parsePrimary() (bool, *ExpressionError) {
	t := p.cur()
	if t.isKeyword("NOT") {
		p.advance()
		b, err := p.parsePrimary()
		if err != nil {
			return false, err
		}
		return !b, nil
	}
	if t.kind == tokenLParen {
		p.advance()
		b, err := p.parseOr()
		if err != nil {
			return false, err
		}
		if p.cur().kind != tokenRParen {
			return false, p.errorHere()
		}
		p.advance()
		return b, nil
	}
	return p.parseComparison()
}

func (p *parser) tryComparisonOp() (string, bool) {
	t := p.cur()
	switch t.kind {
	case tokenEq:
		return "=", true
	case tokenNe:
		return "!=", true
	case tokenLt:
		return "<", true
	case tokenLe:
		return "<=", true
	case tokenGt:
		return ">", true
	case tokenGe:
		return ">=", true
	}
	return "", false
}

func (p *parser) parseComparison() (bool, *ExpressionError) {
	t := p.cur()
	if t.kind == tokenIdent && p.tkAt(1).kind == tokenLParen {
		switch strings.ToLower(t.text) {
		case "attribute_exists", "attribute_not_exists", "begins_with", "contains", "attribute_type":
			return p.evalConditionFunction(strings.ToLower(t.text))
		}
	}
	left, err := p.parseCondOperand()
	if err != nil {
		return false, err
	}
	if p.cur().isKeyword("BETWEEN") {
		p.advance()
		lo, err := p.parseCondOperand()
		if err != nil {
			return false, err
		}
		if !p.cur().isKeyword("AND") {
			return false, p.errorHere()
		}
		p.advance()
		hi, err := p.parseCondOperand()
		if err != nil {
			return false, err
		}
		lv, lok, err2 := p.evalCondOperand(left)
		if err2 != nil {
			return false, err2
		}
		av, aok, err3 := p.evalCondOperand(lo)
		if err3 != nil {
			return false, err3
		}
		bv, bok, err4 := p.evalCondOperand(hi)
		if err4 != nil {
			return false, err4
		}
		return betweenValue(lv, lok, av, aok, bv, bok), nil
	}
	if p.cur().isKeyword("IN") {
		p.advance()
		if p.cur().kind != tokenLParen {
			return false, p.errorHere()
		}
		p.advance()
		lv, lok, lerr := p.evalCondOperand(left)
		if lerr != nil {
			return false, lerr
		}
		any := false
		for {
			o, oerr := p.parseCondOperand()
			if oerr != nil {
				return false, oerr
			}
			rv, rok, rerr := p.evalCondOperand(o)
			if rerr != nil {
				return false, rerr
			}
			if lok && rok && valuesEqual(lv, rv) {
				any = true
			}
			switch p.cur().kind {
			case tokenComma:
				p.advance()
			case tokenRParen:
				p.advance()
				return any, nil
			default:
				return false, p.errorHere()
			}
		}
	}
	op, ok := p.tryComparisonOp()
	if !ok {
		return false, p.errorHere()
	}
	p.advance()
	right, rerr := p.parseCondOperand()
	if rerr != nil {
		return false, rerr
	}
	lv2, lok2, lerr2 := p.evalCondOperand(left)
	if lerr2 != nil {
		return false, lerr2
	}
	rv2, rok2, rerr2 := p.evalCondOperand(right)
	if rerr2 != nil {
		return false, rerr2
	}
	return compareCond(lv2, lok2, rv2, rok2, op), nil
}

func (p *parser) evalConditionFunction(name string) (bool, *ExpressionError) {
	p.advance() // function name
	p.advance() // (
	a1, err := p.parseCondOperand()
	if err != nil {
		return false, err
	}
	if a1.isValue || a1.isSize {
		t := "N"
		if a1.isValue {
			t = a1.value.Kind.String()
		}
		return false, p.incorrectOperandFunc(name, t)
	}
	var a2 *condOperand
	if name != "attribute_exists" && name != "attribute_not_exists" {
		if p.cur().kind != tokenComma {
			return false, p.errorHere()
		}
		p.advance()
		a2, err = p.parseCondOperand()
		if err != nil {
			return false, err
		}
	}
	if p.cur().kind != tokenRParen {
		return false, p.errorHere()
	}
	p.advance()
	v1, ok1 := readPath(p.item, a1.path)
	switch name {
	case "attribute_exists":
		return ok1, nil
	case "attribute_not_exists":
		return !ok1, nil
	}
	if !ok1 {
		return false, nil
	}
	v2, ok2 := p.evalPlainOperand(a2)
	if !ok2 {
		return false, nil
	}
	switch name {
	case "begins_with":
		if v1.Kind != v2.Kind {
			return false, nil
		}
		switch v1.Kind {
		case KindString:
			return strings.HasPrefix(v1.Str, v2.Str), nil
		case KindBinary:
			return bytes.HasPrefix(v1.Bin, v2.Bin), nil
		}
		return false, nil
	case "contains":
		switch v1.Kind {
		case KindString:
			if v2.Kind != KindString {
				return false, nil
			}
			return strings.Contains(v1.Str, v2.Str), nil
		case KindBinary:
			if v2.Kind != KindBinary {
				return false, nil
			}
			return bytes.Contains(v1.Bin, v2.Bin), nil
		case KindStringSet:
			if v2.Kind != KindString {
				return false, nil
			}
			return containsString(v1.SS, v2.Str), nil
		case KindNumberSet:
			if v2.Kind != KindNumber {
				return false, nil
			}
			rs, ok := parseDecimal(v2.Str)
			if !ok {
				return false, nil
			}
			for _, s := range v1.NS {
				rn, ok2 := parseDecimal(s)
				if ok2 && rn.Cmp(rs) == 0 {
					return true, nil
				}
			}
			return false, nil
		case KindBinarySet:
			if v2.Kind != KindBinary {
				return false, nil
			}
			return containsBinary(v1.BS, v2.Bin), nil
		case KindList:
			for _, el := range v1.L {
				if valuesEqual(el, v2) {
					return true, nil
				}
			}
			return false, nil
		case KindMap:
			for _, mv := range v1.M {
				if valuesEqual(mv, v2) {
					return true, nil
				}
			}
			return false, nil
		}
		return false, nil
	case "attribute_type":
		if v2.Kind != KindString {
			return false, nil
		}
		return v1.Kind.String() == v2.Str, nil
	}
	return false, nil
}

func betweenValue(v *Value, vok bool, lo *Value, lok bool, hi *Value, hok bool) bool {
	if !vok || !lok || !hok {
		return false
	}
	if v == nil || lo == nil || hi == nil {
		return false
	}
	if v.Kind != lo.Kind || v.Kind != hi.Kind {
		return false
	}
	switch v.Kind {
	case KindString:
		return strings.Compare(lo.Str, v.Str) <= 0 && strings.Compare(v.Str, hi.Str) <= 0
	case KindNumber:
		rv, ok1 := parseDecimal(v.Str)
		rl, ok2 := parseDecimal(lo.Str)
		rh, ok3 := parseDecimal(hi.Str)
		if !ok1 || !ok2 || !ok3 {
			return false
		}
		return rl.Cmp(rv) <= 0 && rv.Cmp(rh) <= 0
	case KindBinary:
		return bytes.Compare(lo.Bin, v.Bin) <= 0 && bytes.Compare(v.Bin, hi.Bin) <= 0
	}
	return false
}

func compareCond(a *Value, aok bool, b *Value, bok bool, op string) bool {
	if !aok || !bok {
		return false
	}
	if a == nil || b == nil {
		return false
	}
	if a.Kind != b.Kind {
		return op == "!="
	}
	var cmp int
	isOrdered := false
	switch a.Kind {
	case KindString:
		cmp = strings.Compare(a.Str, b.Str)
		isOrdered = true
	case KindNumber:
		ra, ok1 := parseDecimal(a.Str)
		rb, ok2 := parseDecimal(b.Str)
		if !ok1 || !ok2 {
			cmp = strings.Compare(a.Str, b.Str)
		} else {
			cmp = ra.Cmp(rb)
		}
		isOrdered = true
	case KindBinary:
		cmp = bytes.Compare(a.Bin, b.Bin)
		isOrdered = true
	}
	if isOrdered {
		switch op {
		case "=":
			return cmp == 0
		case "!=":
			return cmp != 0
		case "<":
			return cmp < 0
		case "<=":
			return cmp <= 0
		case ">":
			return cmp > 0
		case ">=":
			return cmp >= 0
		}
		return false
	}
	eq := valuesEqual(a, b)
	switch op {
	case "=":
		return eq
	case "!=":
		return !eq
	}
	return false
}

// ------------------------------------------------- update expressions

type UpdateKind int

const (
	UpdateSet UpdateKind = iota
	UpdateRemove
	UpdateAdd
	UpdateDelete
)

// UpdateOperand is one operand of an update expression value.
type UpdateOperand struct {
	IsValue bool
	Value   *Value
	IsPath  bool
	Path    *DocumentPath
	Fn      string // "list_append" or "if_not_exists"
	Args    []*UpdateOperand
	Raw     string // resolved text, used in error messages
}

// SetValueExpr is the right-hand side of a SET action (1 operand, or 2 for +/-).
type SetValueExpr struct {
	Binary   bool
	Plus     bool
	Operands []*UpdateOperand
}

// UpdateAction is a single action of an update expression.
type UpdateAction struct {
	Kind    UpdateKind
	Path    *DocumentPath
	Value   *SetValueExpr   // SET
	Operand *UpdateOperand  // ADD / DELETE
}

// UpdatePlan is a parsed UpdateExpression.
type UpdatePlan struct {
	Actions []UpdateAction
}

func (p *parser) parseUpdate() (*UpdatePlan, *ExpressionError) {
	if p.atEnd() {
		return nil, p.errorHere()
	}
	plan := &UpdatePlan{}
	seen := make(map[UpdateKind]bool, 4)
	for !p.atEnd() {
		t := p.cur()
		if t.kind != tokenIdent {
			return nil, p.errorHere()
		}
		var kind UpdateKind
		switch strings.ToUpper(t.text) {
		case "SET":
			kind = UpdateSet
		case "REMOVE":
			kind = UpdateRemove
		case "ADD":
			kind = UpdateAdd
		case "DELETE":
			kind = UpdateDelete
		default:
			return nil, p.errorHere()
		}
		if seen[kind] {
			return nil, p.errorHere()
		}
		seen[kind] = true
		p.advance()
		for {
			act, err := p.parseUpdateAction(kind)
			if err != nil {
				return nil, err
			}
			plan.Actions = append(plan.Actions, *act)
			if p.cur().kind == tokenComma {
				p.advance()
				continue
			}
			break
		}
	}
	if err := plan.validateOverlaps(); err != nil {
		return nil, err
	}
	return plan, nil
}

func (p *parser) parseUpdateAction(kind UpdateKind) (*UpdateAction, *ExpressionError) {
	raw, err := p.parseDocumentPath()
	if err != nil {
		return nil, err
	}
	dp, err := p.resolvePath(raw)
	if err != nil {
		return nil, err
	}
	act := &UpdateAction{Kind: kind, Path: dp}
	switch kind {
	case UpdateSet:
		if p.cur().kind != tokenEq {
			return nil, p.errorHere()
		}
		p.advance()
		act.Value, err = p.parseSetValue()
		if err != nil {
			return nil, err
		}
	case UpdateAdd, UpdateDelete:
		act.Operand, err = p.parseUpdateOperand()
		if err != nil {
			return nil, err
		}
	}
	return act, nil
}

func (p *parser) parseSetValue() (*SetValueExpr, *ExpressionError) {
	first, err := p.parseUpdateOperand()
	if err != nil {
		return nil, err
	}
	if p.cur().kind == tokenPlus || p.cur().kind == tokenMinus {
		plus := p.cur().kind == tokenPlus
		p.advance()
		second, err2 := p.parseUpdateOperand()
		if err2 != nil {
			return nil, err2
		}
		return &SetValueExpr{Binary: true, Plus: plus, Operands: []*UpdateOperand{first, second}}, nil
	}
	return &SetValueExpr{Operands: []*UpdateOperand{first}}, nil
}

func operandKindName(o *UpdateOperand) string {
	if o.IsValue {
		return o.Value.Kind.String()
	}
	return "path"
}

func (p *parser) parseUpdateOperand() (*UpdateOperand, *ExpressionError) {
	t := p.cur()
	if t.kind == tokenValueRef {
		p.advance()
		v, ok := p.valueOf(t.text)
		if !ok {
			return nil, undefinedValueError(t.text)
		}
		return &UpdateOperand{IsValue: true, Value: v, Raw: t.text}, nil
	}
	if t.kind == tokenIdent && p.tkAt(1).kind == tokenLParen {
		switch strings.ToLower(t.text) {
		case "list_append":
			p.advance()
			p.advance()
			a1, err := p.parseUpdateOperand()
			if err != nil {
				return nil, err
			}
			if p.cur().kind != tokenComma {
				return nil, p.errorHere()
			}
			p.advance()
			a2, err := p.parseUpdateOperand()
			if err != nil {
				return nil, err
			}
			if p.cur().kind != tokenRParen {
				return nil, p.errorHere()
			}
			p.advance()
			return &UpdateOperand{Fn: "list_append", Args: []*UpdateOperand{a1, a2}, Raw: "list_append(" + a1.Raw + ", " + a2.Raw + ")"}, nil
		case "if_not_exists":
			p.advance()
			p.advance()
			a1, err := p.parseUpdateOperand()
			if err != nil {
				return nil, err
			}
			if !a1.IsPath {
				return nil, p.incorrectOperandFunc("if_not_exists", operandKindName(a1))
			}
			if p.cur().kind != tokenComma {
				return nil, p.errorHere()
			}
			p.advance()
			a2, err := p.parseUpdateOperand()
			if err != nil {
				return nil, err
			}
			if p.cur().kind != tokenRParen {
				return nil, p.errorHere()
			}
			p.advance()
			return &UpdateOperand{Fn: "if_not_exists", Args: []*UpdateOperand{a1, a2}, Raw: "if_not_exists(" + a1.Raw + ", " + a2.Raw + ")"}, nil
		case "size":
			return nil, p.errorHere()
		}
	}
	if t.kind == tokenIdent || t.kind == tokenNameRef {
		raw, err := p.parseDocumentPath()
		if err != nil {
			return nil, err
		}
		dp, err := p.resolvePath(raw)
		if err != nil {
			return nil, err
		}
		return &UpdateOperand{IsPath: true, Path: dp, Raw: dp.String()}, nil
	}
	return nil, p.errorHere()
}

func pathsOverlap(a, b *DocumentPath) bool {
	n := len(a.Segments)
	if len(b.Segments) < n {
		n = len(b.Segments)
	}
	for i := 0; i < n; i++ {
		sa, sb := a.Segments[i], b.Segments[i]
		if sa.IsIndex != sb.IsIndex {
			return false
		}
		if sa.IsIndex {
			if sa.Index != sb.Index {
				return false
			}
		} else if sa.Name != sb.Name {
			return false
		}
	}
	return true
}

func (plan *UpdatePlan) validateOverlaps() *ExpressionError {
	for i := range plan.Actions {
		for j := i + 1; j < len(plan.Actions); j++ {
			if pathsOverlap(plan.Actions[i].Path, plan.Actions[j].Path) {
				return twoPathsOverlap(plan.Actions[i].Path.String(), plan.Actions[j].Path.String())
			}
		}
	}
	return nil
}

func evalUpdateOperand(o *UpdateOperand, snapshot map[string]*Value) (*Value, *ExpressionError) {
	if o.IsValue {
		return o.Value.Clone(), nil
	}
	if o.IsPath {
		v, err := readPathStrict(snapshot, o.Path)
		if err != nil {
			return nil, err
		}
		return v.Clone(), nil
	}
	if o.Fn == "list_append" {
		v1, err := evalUpdateOperand(o.Args[0], snapshot)
		if err != nil {
			return nil, err
		}
		if v1.Kind != KindList {
			return nil, operandIncorrectType(o.Args[0].Raw)
		}
		v2, err := evalUpdateOperand(o.Args[1], snapshot)
		if err != nil {
			return nil, err
		}
		if v2.Kind != KindList {
			return nil, operandIncorrectType(o.Args[1].Raw)
		}
		merged := make([]*Value, 0, len(v1.L)+len(v2.L))
		merged = append(merged, v1.L...)
		merged = append(merged, v2.L...)
		return &Value{Kind: KindList, L: merged}, nil
	}
	if o.Fn == "if_not_exists" {
		if v, ok := readPath(snapshot, o.Args[0].Path); ok {
			return v.Clone(), nil
		}
		return evalUpdateOperand(o.Args[1], snapshot)
	}
	return nil, operandIncorrectType(o.Raw)
}

func evalSetValue(u *SetValueExpr, snapshot map[string]*Value) (*Value, *ExpressionError) {
	if !u.Binary {
		return evalUpdateOperand(u.Operands[0], snapshot)
	}
	v1, err := evalUpdateOperand(u.Operands[0], snapshot)
	if err != nil {
		return nil, err
	}
	r1, ok1 := v1.Number()
	if !ok1 {
		return nil, operandIncorrectType(u.Operands[0].Raw)
	}
	v2, err := evalUpdateOperand(u.Operands[1], snapshot)
	if err != nil {
		return nil, err
	}
	r2, ok2 := v2.Number()
	if !ok2 {
		return nil, operandIncorrectType(u.Operands[1].Raw)
	}
	if u.Plus {
		return NewNumber(numberString(new(big.Rat).Add(r1, r2))), nil
	}
	return NewNumber(numberString(new(big.Rat).Sub(r1, r2))), nil
}

type indexedRemoval struct {
	parent string
	index  int
	segs   []PathSegment
}

// applyRemovals applies REMOVE actions. List-element removals are grouped by
// parent and applied by descending original index, so indices refer to the
// item as it was at the start of the update, matching the concurrent-action
// semantics of the real engine.
func (plan *UpdatePlan) applyRemovals(working map[string]*Value) *ExpressionError {
	var plain []*DocumentPath
	idxs := []indexedRemoval{}
	for i := range plan.Actions {
		act := &plan.Actions[i]
		if act.Kind != UpdateRemove {
			continue
		}
		segs := act.Path.Segments
		last := segs[len(segs)-1]
		if last.IsIndex {
			idxs = append(idxs, indexedRemoval{parent: segmentsKey(segs[:len(segs)-1]), index: last.Index, segs: segs})
		} else {
			plain = append(plain, act.Path)
		}
	}
	sort.SliceStable(idxs, func(i, j int) bool {
		if idxs[i].parent != idxs[j].parent {
			return idxs[i].parent < idxs[j].parent
		}
		return idxs[i].index > idxs[j].index
	})
	for i := range idxs {
		if err := removeVal(working, idxs[i].segs); err != nil {
			return err
		}
	}
	for _, dp := range plain {
		if err := removeVal(working, dp.Segments); err != nil {
			return err
		}
	}
	return nil
}

// Apply applies the plan to a deep copy of item. All right-hand-side operands
// are evaluated against the original item (two-phase, atomic semantics).
func (plan *UpdatePlan) Apply(item map[string]*Value) (map[string]*Value, *ExpressionError) {
	working := cloneItem(item)
	for i := range plan.Actions {
		act := &plan.Actions[i]
		if act.Kind != UpdateSet {
			continue
		}
		v, err := evalSetValue(act.Value, item)
		if err != nil {
			return nil, err
		}
		if err := setVal(working, act.Path.Segments, v); err != nil {
			return nil, err
		}
	}
	if err := plan.applyRemovals(working); err != nil {
		return nil, err
	}
	for i := range plan.Actions {
		act := &plan.Actions[i]
		if act.Kind != UpdateAdd {
			continue
		}
		v, err := evalUpdateOperand(act.Operand, item)
		if err != nil {
			return nil, err
		}
		sl, err := addSlotPath(working, act.Path.Segments)
		if err != nil {
			return nil, err
		}
		if err := applyAdd(sl, v, act.Path.String()); err != nil {
			return nil, err
		}
	}
	for i := range plan.Actions {
		act := &plan.Actions[i]
		if act.Kind != UpdateDelete {
			continue
		}
		v, err := evalUpdateOperand(act.Operand, item)
		if err != nil {
			return nil, err
		}
		sl, err := deleteSlotPath(working, act.Path.Segments)
		if err != nil {
			return nil, err
		}
		if sl == nil {
			continue
		}
		if err := applyDelete(sl, v); err != nil {
			return nil, err
		}
	}
	return working, nil
}

// ------------------------------------------------------ key conditions

// KeySchemaElement describes one attribute of a table or index key schema.
type KeySchemaElement struct {
	AttributeName string
	KeyType       string // "HASH" or "RANGE"
}

// KeyConditionPart is one validated condition of a KeyConditionExpression.
type KeyConditionPart struct {
	AttributeName string
	Comparison    string // "=", "<", "<=", ">", ">=", "BETWEEN", "BEGINS_WITH"
	Values        []*Value
}

type keyCondPart struct {
	attr string
	op   string
	vals []*Value
}

// KeyCondition is one legacy Query.KeyConditions entry.
type KeyCondition struct {
	AttributeName string
	Operator      string
	Values        []*Value
}

func keyNames(schema []KeySchemaElement) (pk, sk string) {
	for _, el := range schema {
		switch strings.ToUpper(el.KeyType) {
		case "HASH":
			if pk == "" {
				pk = el.AttributeName
			}
		case "RANGE":
			if sk == "" {
				sk = el.AttributeName
			}
		}
	}
	return pk, sk
}

func expectedKeyKind(attr string, keyTypes map[string]string) (Kind, bool) {
	var t string
	if keyTypes != nil {
		t = keyTypes[attr]
	}
	switch strings.ToUpper(t) {
	case "S":
		return KindString, true
	case "N":
		return KindNumber, true
	case "B":
		return KindBinary, true
	}
	return 0, false
}

func checkPartTypes(c keyCondPart, keyTypes map[string]string, indexName string) *ExpressionError {
	expected, ok := expectedKeyKind(c.attr, keyTypes)
	if !ok {
		return nil
	}
	for _, v := range c.vals {
		if v.Kind != expected {
			return typeMismatchIndexKey(c.attr, expected.String(), v.Kind.String(), indexName)
		}
	}
	return nil
}

func checkLegacyTypes(c KeyCondition, keyTypes map[string]string, indexName string) *ExpressionError {
	expected, ok := expectedKeyKind(c.AttributeName, keyTypes)
	if !ok {
		return nil
	}
	op := strings.ToUpper(c.Operator)
		if op == "NOT_NULL" || op == "NULL" {
		return nil
	}
	if op == "BEGINS_WITH" && expected == KindNumber {
		return invalidExpression("KeyCondition", "Incorrect operand type for operator or function; operator or function: begins_with, operand type: N")
	}
	for _, v := range c.Values {
		if v.Kind != expected {
			return typeMismatchIndexKey(c.AttributeName, expected.String(), v.Kind.String(), indexName)
		}
	}
	return nil
}

func (p *parser) parseKeyOperand() (*Value, *ExpressionError) {
	t := p.cur()
	if t.kind != tokenValueRef {
		return nil, p.errorHere()
	}
	p.advance()
	v, ok := p.valueOf(t.text)
	if !ok {
		return nil, undefinedValueError(t.text)
	}
	return v.Clone(), nil
}

func stringCompOp(t token) string {
	switch t.kind {
	case tokenLt:
		return "<"
	case tokenLe:
		return "<="
	case tokenGt:
		return ">"
	case tokenGe:
		return ">="
	}
	return "?"
}

func (p *parser) parseKeyConditionPart() (*keyCondPart, *ExpressionError) {
	t := p.cur()
	if t.kind == tokenIdent && p.tkAt(1).kind == tokenLParen {
		if strings.ToLower(t.text) != "begins_with" {
			return nil, queryKeyNotSupported()
		}
		p.advance()
		p.advance()
		raw, err := p.parseDocumentPath()
		if err != nil {
			return nil, err
		}
		dp, err := p.resolvePath(raw)
		if err != nil {
			return nil, err
		}
		if len(dp.Segments) != 1 || dp.Segments[0].IsIndex {
			return nil, queryKeyNotSupported()
		}
		if p.cur().kind != tokenComma {
			return nil, p.errorHere()
		}
		p.advance()
		v, err := p.parseKeyOperand()
		if err != nil {
			return nil, err
		}
		if p.cur().kind != tokenRParen {
			return nil, p.errorHere()
		}
		p.advance()
		return &keyCondPart{attr: dp.Segments[0].Name, op: "BEGINS_WITH", vals: []*Value{v}}, nil
	}
	raw, err := p.parseDocumentPath()
	if err != nil {
		return nil, err
	}
	dp, err := p.resolvePath(raw)
	if err != nil {
		return nil, err
	}
	if len(dp.Segments) != 1 || dp.Segments[0].IsIndex {
		return nil, queryKeyNotSupported()
	}
	part := &keyCondPart{attr: dp.Segments[0].Name}
	t = p.cur()
	switch t.kind {
	case tokenEq:
		part.op = "="
		p.advance()
	case tokenNe:
		return nil, queryKeyNotSupported()
	case tokenLt, tokenLe, tokenGt, tokenGe:
		part.op = stringCompOp(t)
		p.advance()
	default:
		if t.isKeyword("BETWEEN") {
			p.advance()
			v1, err := p.parseKeyOperand()
			if err != nil {
				return nil, err
			}
			if !p.cur().isKeyword("AND") {
				return nil, p.errorHere()
			}
			p.advance()
			v2, err := p.parseKeyOperand()
			if err != nil {
				return nil, err
			}
			part.op = "BETWEEN"
			part.vals = []*Value{v1, v2}
			return part, nil
		}
		if t.isKeyword("IN") {
			return nil, queryKeyNotSupported()
		}
		return nil, p.errorHere()
	}
	v, err := p.parseKeyOperand()
	if err != nil {
		return nil, err
	}
	part.vals = []*Value{v}
	return part, nil
}

func (p *parser) parseKeyConditionParts() ([]keyCondPart, *ExpressionError) {
	parts := []keyCondPart{}
	for {
		part, err := p.parseKeyConditionPart()
		if err != nil {
			return nil, err
		}
		parts = append(parts, *part)
		if p.atEnd() {
			return parts, nil
		}
		if p.cur().isKeyword("OR") {
			return nil, queryKeyNotSupported()
		}
		if !p.cur().isKeyword("AND") {
			return nil, p.errorHere()
		}
		p.advance()
	}
}

func toKeyConditionPart(c keyCondPart) *KeyConditionPart {
	return &KeyConditionPart{AttributeName: c.attr, Comparison: c.op, Values: c.vals}
}

// ParseAndValidateKeyCondition parses a KeyConditionExpression and validates
// it against the key schema, returning the partition-key and (optional)
// sort-key conditions.
func ParseAndValidateKeyCondition(expr string, attrs *ExpressionAttributes, schema []KeySchemaElement, keyTypes map[string]string, indexName string) (*KeyConditionPart, *KeyConditionPart, *ExpressionError) {
	p, perr := newParser("KeyCondition", expr, attrs)
	if perr != nil {
		return nil, nil, perr
	}
	parts, perr := p.parseKeyConditionParts()
	if perr != nil {
		return nil, nil, perr
	}
	pkName, skName := keyNames(schema)
	if len(parts) == 0 || len(parts) > 2 {
		return nil, nil, queryKeyNotSupported()
	}
	first := parts[0]
	if first.attr != pkName {
		return nil, nil, missedKeySchemaElement(pkName)
	}
	if first.op != "=" {
		return nil, nil, queryKeyNotSupported()
	}
	if err := checkPartTypes(first, keyTypes, indexName); err != nil {
		return nil, nil, err
	}
	if len(parts) == 1 {
		return toKeyConditionPart(first), nil, nil
	}
	second := parts[1]
	if second.attr == pkName {
		return nil, nil, queryKeyNotSupported()
	}
	if skName == "" {
		return nil, nil, queryKeyNotSupported()
	}
	if second.attr != skName {
		return nil, nil, missedKeySchemaElement(skName)
	}
	if second.op == "BEGINS_WITH" {
		if expected, ok := expectedKeyKind(skName, keyTypes); ok && expected == KindNumber {
			return nil, nil, invalidExpression("KeyCondition", "Incorrect operand type for operator or function; operator or function: begins_with, operand type: N")
		}
	} else {
		switch second.op {
		case "=", "<", "<=", ">", ">=", "BETWEEN":
		default:
			return nil, nil, queryKeyNotSupported()
		}
	}
	if err := checkPartTypes(second, keyTypes, indexName); err != nil {
		return nil, nil, err
	}
	return toKeyConditionPart(first), toKeyConditionPart(second), nil
}

// ValidateKeyConditions validates legacy Query.KeyConditions entries.
func ValidateKeyConditions(conds []KeyCondition, schema []KeySchemaElement, keyTypes map[string]string, indexName string) *ExpressionError {
	if len(conds) == 0 {
		return keyConditionsRequiredError()
	}
	pkName, _ := keyNames(schema)
	seen := make(map[string]bool, len(conds))
	pkPresent := false
	for _, c := range conds {
		if seen[c.AttributeName] {
			return onlyOneConditionPerKeyError()
		}
		seen[c.AttributeName] = true
		op := strings.ToUpper(c.Operator)
		switch op {
		case "EQ", "NE", "LE", "LT", "GE", "GT", "BETWEEN", "NOT_NULL", "NULL", "CONTAINS", "NOT_CONTAINS", "BEGINS_WITH", "IN":
		default:
			return exprErr("One or more parameter values were invalid: Unknown operator: %s", c.Operator)
		}
		if c.AttributeName == pkName {
			if op != "EQ" {
				return queryKeyNotSupported()
			}
			pkPresent = true
		}
		if err := checkLegacyTypes(c, keyTypes, indexName); err != nil {
			return err
		}
	}
	if !pkPresent {
		return missedKeySchemaElement(pkName)
	}
	return nil
}

// --------------------------------------------------------- public API

func evalConditionExpression(exprType string, expr string, attrs *ExpressionAttributes, item map[string]*Value) (bool, *ExpressionError) {
	p, err := newParser(exprType, expr, attrs)
	if err != nil {
		return false, err
	}
	p.item = item
	b, perr := p.parseOr()
	if perr != nil {
		return false, perr
	}
	if !p.atEnd() {
		return false, p.errorHere()
	}
	return b, nil
}

// EvalConditionExpression evaluates a ConditionExpression against item.
func EvalConditionExpression(expr string, attrs *ExpressionAttributes, item map[string]*Value) (bool, *ExpressionError) {
	return evalConditionExpression("Condition", expr, attrs, item)
}

// EvalFilterExpression evaluates a FilterExpression / ScanFilter against item.
func EvalFilterExpression(expr string, attrs *ExpressionAttributes, item map[string]*Value) (bool, *ExpressionError) {
	return evalConditionExpression("Filter", expr, attrs, item)
}

// ParseUpdateExpression parses and validates an UpdateExpression, including
// the document-path overlap check.
func ParseUpdateExpression(expr string, attrs *ExpressionAttributes) (*UpdatePlan, *ExpressionError) {
	p, perr := newParser("Update", expr, attrs)
	if perr != nil {
		return nil, perr
	}
	return p.parseUpdate()
}

// ApplyUpdateExpression parses, validates and applies an UpdateExpression,
// returning the new item.
func ApplyUpdateExpression(item map[string]*Value, expr string, attrs *ExpressionAttributes) (map[string]*Value, *ExpressionError) {
	plan, err := ParseUpdateExpression(expr, attrs)
	if err != nil {
		return nil, err
	}
	return plan.Apply(item)
}
