package ddb

// Self-contained port of the condition expression engine (the floci-go
// equivalent of the DynamoDBLocal ExpressionEvaluator): a tokenizer, a
// recursive descent parser and an evaluator used by BatchWriteItem
// ConditionCheck validation.

import (
    "bytes"
    "fmt"
    "sort"
    "strconv"
    "strings"
)

// ---------------------------------------------------------------------------
// Document paths
// ---------------------------------------------------------------------------

type exprPart struct {
    name  string
    idx   int
    isIdx bool
}

type exprPath struct {
    parts []exprPart
}

func pathString(p exprPath) string {
    var sb strings.Builder
    for i, part := range p.parts {
        switch {
        case i == 0:
            sb.WriteString(part.name)
        case part.isIdx:
            fmt.Fprintf(&sb, "[%d]", part.idx)
        default:
            sb.WriteByte('.')
            sb.WriteString(part.name)
        }
    }
    return sb.String()
}

func invalidDocPathError() *APIError {
    return validation("The document path provided in the expression is invalid for the item")
}

// resolveDocPath navigates p inside item. found=false when a referenced
// element does not exist; an error is returned when the path traverses an
// attribute of an incompatible type.
func resolveDocPath(item map[string]AttrVal, p exprPath) (AttrVal, bool, *APIError) {
    v, ok := item[p.parts[0].name]
    if !ok {
        return AttrVal{}, false, nil
    }
    for i := 1; i < len(p.parts); i++ {
        part := p.parts[i]
        if part.isIdx {
            if v.Type != "L" {
                return AttrVal{}, false, invalidDocPathError()
            }
            if part.idx < 0 || part.idx >= len(v.L) {
                return AttrVal{}, false, nil
            }
            v = v.L[part.idx]
        } else {
            if v.Type != "M" {
                return AttrVal{}, false, invalidDocPathError()
            }
            nv, ok := v.M[part.name]
            if !ok {
                return AttrVal{}, false, nil
            }
            v = nv
        }
    }
    return v, true, nil
}

// ---------------------------------------------------------------------------
// Tokens
// ---------------------------------------------------------------------------

type tokKind int

const (
    tokEOF tokKind = iota
    tokIdent
    tokName  // #name
    tokValue // :value
    tokIndex // [n]
    tokLParen
    tokRParen
    tokComma
    tokDot
    tokEq
    tokNe
    tokLt
    tokLe
    tokGt
    tokGe
)

type token struct {
    kind tokKind
    text string
}

func isIdentStart(c byte) bool {
    return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentPart(c byte) bool {
    return isIdentStart(c) || (c >= '0' && c <= '9')
}

func scanIdent(expr string, i int) int {
    for i < len(expr) && isIdentPart(expr[i]) {
        i++
    }
    return i
}

func invalidExprSyntax(exprKind, tok string) *APIError {
    return validation(fmt.Sprintf("Invalid %s: Syntax error; token: %q", exprKind, tok))
}

func tokenize(expr, exprKind string) ([]token, *APIError) {
    var toks []token
    i, n := 0, len(expr)
    for i < n {
        c := expr[i]
        switch {
        case c == ' ' || c == '\t' || c == '\r' || c == '\n':
            i++
        case c == '(':
            toks = append(toks, token{tokLParen, "("})
            i++
        case c == ')':
            toks = append(toks, token{tokRParen, ")"})
            i++
        case c == ',':
            toks = append(toks, token{tokComma, ","})
            i++
        case c == '.':
            toks = append(toks, token{tokDot, "."})
            i++
        case c == '#':
            j := scanIdent(expr, i+1)
            if j == i+1 {
                return nil, invalidExprSyntax(exprKind, "#")
            }
            toks = append(toks, token{tokName, "#" + expr[i+1 : j]})
            i = j
        case c == ':':
            j := scanIdent(expr, i+1)
            if j == i+1 {
                return nil, invalidExprSyntax(exprKind, ":")
            }
            toks = append(toks, token{tokValue, ":" + expr[i+1 : j]})
            i = j
        case c == '[':
            j := i + 1
            for j < n && expr[j] >= '0' && expr[j] <= '9' {
                j++
            }
            if j == i+1 || j >= n || expr[j] != ']' {
                return nil, invalidExprSyntax(exprKind, "[")
            }
            toks = append(toks, token{tokIndex, expr[i+1 : j]})
            i = j + 1
        case c == '<':
            if i+1 < n && expr[i+1] == '>' {
                toks = append(toks, token{tokNe, "<>"})
                i += 2
            } else if i+1 < n && expr[i+1] == '=' {
                toks = append(toks, token{tokLe, "<="})
                i += 2
            } else {
                toks = append(toks, token{tokLt, "<"})
                i++
            }
        case c == '>':
            if i+1 < n && expr[i+1] == '=' {
                toks = append(toks, token{tokGe, ">="})
                i += 2
            } else {
                toks = append(toks, token{tokGt, ">"})
                i++
            }
        case c == '=':
            toks = append(toks, token{tokEq, "="})
            i++
        case isIdentStart(c):
            j := scanIdent(expr, i)
            toks = append(toks, token{tokIdent, expr[i:j]})
            i = j
        default:
            return nil, invalidExprSyntax(exprKind, string(c))
        }
    }
    return toks, nil
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

type opndKind byte

const (
    opndPath opndKind = iota
    opndValue
    opndSize
)

type opnd struct {
    kind opndKind
    path exprPath
    raw  string // value reference text, e.g. ":v"
}

type condNode interface {
    eval(ctx *condEvalCtx) (bool, *APIError)
}

type condEvalCtx struct {
    item   map[string]AttrVal
    values map[string]AttrVal
}

type condCmp struct {
    op   string
    l, r opnd
}

type condBetween struct{ x, lo, hi opnd }

type condIn struct {
    x    opnd
    list []opnd
}

type condAnd struct{ kids []condNode }

type condOr struct{ kids []condNode }

type condNot struct{ kid condNode }

type condExists struct {
    neg  bool
    path exprPath
}

type condArgFunc struct {
    fn   string
    path exprPath
    arg  opnd
}

var comparatorOps = map[tokKind]string{
    tokEq: "=", tokNe: "<>", tokLt: "<", tokLe: "<=", tokGt: ">", tokGe: ">=",
}

type exprParser struct {
    toks       []token
    pos        int
    exprKind   string
    names      map[string]string
    usedNames  map[string]bool
    usedValues map[string]bool
}

func (p *exprParser) peek() token {
    if p.pos >= len(p.toks) {
        return token{tokEOF, ""}
    }
    return p.toks[p.pos]
}

func (p *exprParser) peekAt(off int) token {
    if p.pos+off >= len(p.toks) {
        return token{tokEOF, ""}
    }
    return p.toks[p.pos+off]
}

func (p *exprParser) next() token {
    t := p.peek()
    if t.kind != tokEOF {
        p.pos++
    }
    return t
}

func (p *exprParser) fail(t token) *APIError {
    if t.kind == tokEOF {
        return invalidExprSyntax(p.exprKind, "end of input")
    }
    return invalidExprSyntax(p.exprKind, t.text)
}

func isConditionFunction(name string) bool {
    switch name {
    case "attribute_exists", "attribute_not_exists", "begins_with", "contains", "attribute_type":
        return true
    }
    return false
}

func (p *exprParser) parseRoot() (condNode, *APIError) {
    node, err := p.parseOr()
    if err != nil {
        return nil, err
    }
    if t := p.peek(); t.kind != tokEOF {
        return nil, p.fail(t)
    }
    return node, nil
}

func (p *exprParser) parseOr() (condNode, *APIError) {
    kids := []condNode{}
    for {
        kid, err := p.parseAnd()
        if err != nil {
            return nil, err
        }
        kids = append(kids, kid)
        t := p.peek()
        if t.kind == tokIdent && strings.EqualFold(t.text, "OR") {
            p.next()
            continue
        }
        break
    }
    if len(kids) == 1 {
        return kids[0], nil
    }
    return condOr{kids: kids}, nil
}

func (p *exprParser) parseAnd() (condNode, *APIError) {
    kids := []condNode{}
    for {
        kid, err := p.parseNot()
        if err != nil {
            return nil, err
        }
        kids = append(kids, kid)
        t := p.peek()
        if t.kind == tokIdent && strings.EqualFold(t.text, "AND") {
            p.next()
            continue
        }
        break
    }
    if len(kids) == 1 {
        return kids[0], nil
    }
    return condAnd{kids: kids}, nil
}

func (p *exprParser) parseNot() (condNode, *APIError) {
    t := p.peek()
    if t.kind == tokIdent && strings.EqualFold(t.text, "NOT") {
        p.next()
        kid, err := p.parseNot()
        if err != nil {
            return nil, err
        }
        return condNot{kid: kid}, nil
    }
    return p.parsePrimaryCond()
}

func (p *exprParser) parsePrimaryCond() (condNode, *APIError) {
    t := p.peek()
    switch {
    case t.kind == tokLParen:
        p.next()
        node, err := p.parseOr()
        if err != nil {
            return nil, err
        }
        if c := p.next(); c.kind != tokRParen {
            return nil, p.fail(c)
        }
        return node, nil
    case t.kind == tokIdent && isConditionFunction(t.text) && p.peekAt(1).kind == tokLParen:
        return p.parseConditionFunction()
    case t.kind == tokName || t.kind == tokValue || t.kind == tokIdent:
        return p.parseComparison()
    default:
        return nil, p.fail(t)
    }
}

func (p *exprParser) parseConditionFunction() (condNode, *APIError) {
    name := p.next().text
    if lp := p.next(); lp.kind != tokLParen {
        return nil, p.fail(lp)
    }
    seg := p.next()
    var first string
    switch seg.kind {
    case tokName:
        p.usedNames[seg.text] = true
        if resolved, ok := p.names[seg.text]; ok {
            first = resolved
        } else {
            first = seg.text
        }
    case tokIdent:
        first = seg.text
    default:
        return nil, p.fail(seg)
    }
    path, err := p.parsePathTail(first)
    if err != nil {
        return nil, err
    }
    switch name {
    case "attribute_exists", "attribute_not_exists":
        if c := p.next(); c.kind != tokRParen {
            return nil, p.fail(c)
        }
        return condExists{neg: name == "attribute_not_exists", path: path}, nil
    case "begins_with", "contains", "attribute_type":
        if c := p.next(); c.kind != tokComma {
            return nil, p.fail(c)
        }
        arg, err := p.parseOperand()
        if err != nil {
            return nil, err
        }
        if c := p.next(); c.kind != tokRParen {
            return nil, p.fail(c)
        }
        return condArgFunc{fn: name, path: path, arg: arg}, nil
    }
    return nil, invalidExprSyntax(p.exprKind, name)
}

func (p *exprParser) parseOperand() (opnd, *APIError) {
    t := p.peek()
    switch t.kind {
    case tokValue:
        p.next()
        p.usedValues[t.text] = true
        return opnd{kind: opndValue, raw: t.text}, nil
    case tokIdent:
        if t.text == "size" && p.peekAt(1).kind == tokLParen {
            p.next() // size
            p.next() // (
            seg := p.next()
            var first string
            switch seg.kind {
            case tokName:
                p.usedNames[seg.text] = true
                if resolved, ok := p.names[seg.text]; ok {
                    first = resolved
                } else {
                    first = seg.text
                }
            case tokIdent:
                first = seg.text
            default:
                return opnd{}, p.fail(seg)
            }
            path, err := p.parsePathTail(first)
            if err != nil {
                return opnd{}, err
            }
            if c := p.next(); c.kind != tokRParen {
                return opnd{}, p.fail(c)
            }
            return opnd{kind: opndSize, path: path}, nil
        }
        p.next()
        path, err := p.parsePathTail(t.text)
        return opnd{kind: opndPath, path: path}, err
    case tokName:
        p.next()
        p.usedNames[t.text] = true
        first, ok := p.names[t.text]
        if !ok {
            first = t.text
        }
        path, err := p.parsePathTail(first)
        return opnd{kind: opndPath, path: path}, err
    default:
        return opnd{}, p.fail(t)
    }
}

// parsePathTail consumes the remainder of a document path (".name", "[i]")
// after the first segment has been read.
func (p *exprParser) parsePathTail(first string) (exprPath, *APIError) {
    parts := []exprPart{{name: first}}
    for {
        t := p.peek()
        switch t.kind {
        case tokDot:
            p.next()
            seg := p.next()
            switch seg.kind {
            case tokName:
                p.usedNames[seg.text] = true
                if resolved, ok := p.names[seg.text]; ok {
                    parts = append(parts, exprPart{name: resolved})
                } else {
                    parts = append(parts, exprPart{name: seg.text})
                }
            case tokIdent:
                parts = append(parts, exprPart{name: seg.text})
            default:
                return exprPath{}, p.fail(seg)
            }
        case tokIndex:
            p.next()
            idx, err := strconv.Atoi(t.text)
            if err != nil {
                return exprPath{}, invalidExprSyntax(p.exprKind, t.text)
            }
            parts = append(parts, exprPart{idx: idx, isIdx: true})
        default:
            return exprPath{parts: parts}, nil
        }
    }
}

func (p *exprParser) parseComparison() (condNode, *APIError) {
    lhs, err := p.parseOperand()
    if err != nil {
        return nil, err
    }
    t := p.next()
    if op, ok := comparatorOps[t.kind]; ok {
        rhs, err := p.parseOperand()
        if err != nil {
            return nil, err
        }
        return condCmp{op: op, l: lhs, r: rhs}, nil
    }
    if t.kind == tokIdent && strings.EqualFold(t.text, "BETWEEN") {
        lo, err := p.parseOperand()
        if err != nil {
            return nil, err
        }
        sep := p.next()
        if !(sep.kind == tokIdent && strings.EqualFold(sep.text, "AND")) {
            return nil, p.fail(sep)
        }
        hi, err := p.parseOperand()
        if err != nil {
            return nil, err
        }
        return condBetween{x: lhs, lo: lo, hi: hi}, nil
    }
    if t.kind == tokIdent && strings.EqualFold(t.text, "IN") {
        if c := p.next(); c.kind != tokLParen {
            return nil, p.fail(c)
        }
        list := []opnd{}
        for {
            item, err := p.parseOperand()
            if err != nil {
                return nil, err
            }
            list = append(list, item)
            c := p.next()
            if c.kind == tokComma {
                continue
            }
            if c.kind == tokRParen {
                break
            }
            return nil, p.fail(c)
        }
        return condIn{x: lhs, list: list}, nil
    }
    return nil, p.fail(t)
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

func containsAttr(set, v AttrVal) bool {
    switch set.Type {
    case "S":
        return v.Type == "S" && strings.Contains(set.S, v.S)
    case "SS":
        return v.Type == "S" && containsString(set.SS, v.S)
    case "NS":
        if v.Type != "N" {
            return false
        }
        for _, e := range set.NS {
            if c, ok := compareNumbers(e, v.N); ok && c == 0 {
                return true
            }
        }
        return false
    case "BS":
        if v.Type != "B" {
            return false
        }
        for _, e := range set.BS {
            if bytes.Equal(e, v.B) {
                return true
            }
        }
        return false
    case "L":
        for _, el := range set.L {
            if attrEqual(el, v) {
                return true
            }
        }
        return false
    }
    return false
}

func resolveOpnd(o opnd, ctx *condEvalCtx) (AttrVal, bool, *APIError) {
    switch o.kind {
    case opndValue:
        v, ok := ctx.values[o.raw]
        if !ok {
            return AttrVal{}, false, nil
        }
        return v, true, nil
    case opndPath:
        return resolveDocPath(ctx.item, o.path)
    case opndSize:
        v, found, err := resolveDocPath(ctx.item, o.path)
        if err != nil || !found {
            return AttrVal{}, false, err
        }
        var n int
        switch v.Type {
        case "S", "B", "SS", "NS", "BS", "M", "L":
            if v.Type == "S" {
                n = len(v.S)
            } else if v.Type == "B" {
                n = len(v.B)
            } else if v.Type == "SS" {
                n = len(v.SS)
            } else if v.Type == "NS" {
                n = len(v.NS)
            } else if v.Type == "BS" {
                n = len(v.BS)
            } else if v.Type == "M" {
                n = len(v.M)
            } else {
                n = len(v.L)
            }
        default:
            return AttrVal{}, false, nil
        }
        return AttrVal{Type: "N", N: strconv.Itoa(n)}, true, nil
    }
    return AttrVal{}, false, nil
}

func (n condCmp) eval(ctx *condEvalCtx) (bool, *APIError) {
    lv, lok, err := resolveOpnd(n.l, ctx)
    if err != nil {
        return false, err
    }
    rv, rok, err := resolveOpnd(n.r, ctx)
    if err != nil {
        return false, err
    }
    if !lok || !rok {
        return false, nil
    }
    switch n.op {
    case "=":
        return attrEqual(lv, rv), nil
    case "<>":
        return !attrEqual(lv, rv), nil
    }
    c, ok := compareAttr(lv, rv)
    if !ok {
        return false, nil
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
}

func (n condBetween) eval(ctx *condEvalCtx) (bool, *APIError) {
    xv, ok1, err := resolveOpnd(n.x, ctx)
    if err != nil {
        return false, err
    }
    lv, ok2, err := resolveOpnd(n.lo, ctx)
    if err != nil {
        return false, err
    }
    hv, ok3, err := resolveOpnd(n.hi, ctx)
    if err != nil {
        return false, err
    }
    if !ok1 || !ok2 || !ok3 {
        return false, nil
    }
    c, ok := compareAttr(xv, lv)
    if !ok {
        return false, nil
    }
    d, ok := compareAttr(xv, hv)
    if !ok {
        return false, nil
    }
    return c >= 0 && d <= 0, nil
}

func (n condIn) eval(ctx *condEvalCtx) (bool, *APIError) {
    xv, ok, err := resolveOpnd(n.x, ctx)
    if err != nil {
        return false, err
    }
    if !ok {
        return false, nil
    }
    for _, o := range n.list {
        ov, oOk, err := resolveOpnd(o, ctx)
        if err != nil {
            return false, err
        }
        if oOk && attrEqual(xv, ov) {
            return true, nil
        }
    }
    return false, nil
}

func (n condAnd) eval(ctx *condEvalCtx) (bool, *APIError) {
    for _, k := range n.kids {
        ok, err := k.eval(ctx)
        if err != nil {
            return false, err
        }
        if !ok {
            return false, nil
        }
    }
    return true, nil
}

func (n condOr) eval(ctx *condEvalCtx) (bool, *APIError) {
    for _, k := range n.kids {
        ok, err := k.eval(ctx)
        if err != nil {
            return false, err
        }
        if ok {
            return true, nil
        }
    }
    return false, nil
}

func (n condNot) eval(ctx *condEvalCtx) (bool, *APIError) {
    ok, err := n.kid.eval(ctx)
    return !ok, err
}

func (n condExists) eval(ctx *condEvalCtx) (bool, *APIError) {
    _, found, err := resolveDocPath(ctx.item, n.path)
    if err != nil {
        return false, err
    }
    if n.neg {
        return !found, nil
    }
    return found, nil
}

func (n condArgFunc) eval(ctx *condEvalCtx) (bool, *APIError) {
    pv, found, err := resolveDocPath(ctx.item, n.path)
    if err != nil {
        return false, err
    }
    if !found {
        return false, nil
    }
    av, ok, err := resolveOpnd(n.arg, ctx)
    if err != nil {
        return false, err
    }
    if !ok {
        return false, nil
    }
    switch n.fn {
    case "begins_with":
        switch pv.Type {
        case "S":
            return av.Type == "S" && strings.HasPrefix(pv.S, av.S), nil
        case "B":
            return av.Type == "B" && bytes.HasPrefix(pv.B, av.B), nil
        }
        return false, nil
    case "contains":
        return containsAttr(pv, av), nil
    case "attribute_type":
        return av.Type == "S" && pv.Type == av.S, nil
    }
    return false, nil
}

// ---------------------------------------------------------------------------
// Validation entry points
// ---------------------------------------------------------------------------

func anyUnusedNames(names map[string]string, used map[string]bool) bool {
    for k := range names {
        if !used[k] {
            return true
        }
    }
    return false
}

func anyUnusedValues(values map[string]AttrVal, used map[string]bool) bool {
    for k := range values {
        if !used[k] {
            return true
        }
    }
    return false
}

func unusedNamesError(names map[string]string) *APIError {
    keys := make([]string, 0, len(names))
    for k := range names {
        keys = append(keys, k)
    }
    sort.Strings(keys)
    return validation(fmt.Sprintf("Value provided in ExpressionAttributeNames unused in expressions: keys: {%s}", strings.Join(keys, ", ")))
}

func unusedValuesError(values map[string]AttrVal) *APIError {
    keys := make([]string, 0, len(values))
    for k := range values {
        keys = append(keys, k)
    }
    sort.Strings(keys)
    return validation(fmt.Sprintf("Value provided in ExpressionAttributeValues unused in expressions: keys: {%s}", strings.Join(keys, ", ")))
}

// compileCondition parses and validates a condition expression and returns
// its executable AST.
func compileCondition(expr string, names map[string]string, values map[string]AttrVal) (condNode, *APIError) {
    return compileExpressionKind("ConditionExpression", expr, names, values)
}

func compileExpressionKind(exprKind, expr string, names map[string]string, values map[string]AttrVal) (condNode, *APIError) {
    expr = strings.TrimSpace(expr)
    if expr == "" {
        return nil, validation(fmt.Sprintf("Invalid %s: The expression must not be empty", exprKind))
    }
    toks, err := tokenize(expr, exprKind)
    if err != nil {
        return nil, err
    }
    if names == nil {
        names = map[string]string{}
    }
    if values == nil {
        values = map[string]AttrVal{}
    }
    p := &exprParser{
        toks:       toks,
        exprKind:   exprKind,
        names:      names,
        usedNames:  map[string]bool{},
        usedValues: map[string]bool{},
    }
    node, err := p.parseRoot()
    if err != nil {
        return nil, err
    }
    for raw := range p.usedNames {
        if _, ok := names[raw]; !ok {
            return nil, validation("The provided expression refers to an attribute that is not defined in the expression")
        }
    }
    for raw := range p.usedValues {
        if _, ok := values[raw]; !ok {
            return nil, validation("The provided expression refers to an attribute that is not defined in the expression")
        }
    }
    if anyUnusedNames(names, p.usedNames) {
        return nil, unusedNamesError(names)
    }
    if anyUnusedValues(values, p.usedValues) {
        return nil, unusedValuesError(values)
    }
    return node, nil
}

func pathOverlap(a, b exprPath) bool {
    n := len(a.parts)
    if len(b.parts) < n {
        n = len(b.parts)
    }
    for i := 0; i < n; i++ {
        pa, pb := a.parts[i], b.parts[i]
        if pa.isIdx != pb.isIdx {
            return false
        }
        equal := (pa.isIdx && pa.idx == pb.idx) || (!pa.isIdx && pa.name == pb.name)
        if !equal {
            return false
        }
    }
    return true
}

// parseProjection parses a ProjectionExpression into document paths.
func parseProjection(expr string, names map[string]string) ([]exprPath, *APIError) {
    expr = strings.TrimSpace(expr)
    if expr == "" {
        return nil, validation("Invalid ProjectionExpression: The expression must not be empty")
    }
    toks, err := tokenize(expr, "ProjectionExpression")
    if err != nil {
        return nil, err
    }
    if names == nil {
        names = map[string]string{}
    }
    p := &exprParser{
        toks:      toks,
        exprKind:  "ProjectionExpression",
        names:     names,
        usedNames: map[string]bool{},
    }
    var paths []exprPath
    for {
        t := p.next()
        var first string
        switch t.kind {
        case tokName:
            p.usedNames[t.text] = true
            if resolved, ok := names[t.text]; ok {
                first = resolved
            } else {
                first = t.text
            }
        case tokIdent:
            first = t.text
        default:
            return nil, p.fail(t)
        }
        path, perr := p.parsePathTail(first)
        if perr != nil {
            return nil, perr
        }
        paths = append(paths, path)
        sep := p.next()
        if sep.kind == tokEOF {
            break
        }
        if sep.kind != tokComma {
            return nil, p.fail(sep)
        }
    }
    for raw := range p.usedNames {
        if _, ok := names[raw]; !ok {
            return nil, validation("The provided expression refers to an attribute that is not defined in the expression")
        }
    }
    if anyUnusedNames(names, p.usedNames) {
        return nil, unusedNamesError(names)
    }
    for i := 0; i < len(paths); i++ {
        for j := i + 1; j < len(paths); j++ {
            if pathOverlap(paths[i], paths[j]) {
                return nil, validation(fmt.Sprintf("Invalid ProjectionExpression: Two document paths overlap with each other; path1: %s, path2: %s", pathString(paths[i]), pathString(paths[j])))
            }
        }
    }
    return paths, nil
}
