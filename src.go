// Package main implements floci-go ticket-017: the DynamoDB JSON wire
// protocol Query operation over a single table partition.
//
// Query supports a KeyConditionExpression built from partition key equality
// combined (with AND) with sort key comparison conditions (=, <, <=, >, >=,
// BETWEEN, begins_with), plus ConsistentRead, Limit, ScanIndexForward and
// ExclusiveStartKey -> LastEvaluatedKey pagination. CreateTable, PutItem,
// GetItem, DeleteItem, BatchWriteItem, Scan, DescribeTable, ListTables and
// DeleteTable are included so that partitions can be seeded and inspected
// through the same wire protocol.
package main

import (
    "bytes"
    "encoding/json"
    "errors"
    "flag"
    "fmt"
    "io"
    "math/big"
    "net"
    "net/http"
    "os"
    "sort"
    "strconv"
    "strings"
    "sync"
    "time"
)

// ---------------------------------------------------------------------------
// Typed attribute values
// ---------------------------------------------------------------------------

// AttrValue is a DynamoDB typed attribute value.
type AttrValue struct {
    Type string
    S    string
    N    string
    B    []byte
    BOOL bool
    NULL bool
    M    map[string]*AttrValue
    L    []*AttrValue
    SS   []string
    NS   []string
    BS   [][]byte
}

func StringValue(s string) *AttrValue { return &AttrValue{Type: "S", S: s} }

func NumberValue(n string) *AttrValue { return &AttrValue{Type: "N", N: n} }

func BinaryValue(b []byte) *AttrValue {
    return &AttrValue{Type: "B", B: append([]byte{}, b...)}
}

var errBadAttrValue = errors.New("Supplied AttributeValue is empty, must contain exactly one of the supported datatypes")

func (v *AttrValue) UnmarshalJSON(data []byte) error {
    var raw map[string]json.RawMessage
    if err := json.Unmarshal(data, &raw); err != nil {
        return errBadAttrValue
    }
    if len(raw) != 1 {
        return errBadAttrValue
    }
    for typ, payload := range raw {
        switch typ {
        case "S":
            if err := json.Unmarshal(payload, &v.S); err != nil {
                return errBadAttrValue
            }
            v.Type = "S"
        case "N":
            if err := json.Unmarshal(payload, &v.N); err != nil {
                return errBadAttrValue
            }
            v.Type = "N"
        case "B":
            if err := json.Unmarshal(payload, &v.B); err != nil {
                return errBadAttrValue
            }
            v.Type = "B"
        case "BOOL":
            if err := json.Unmarshal(payload, &v.BOOL); err != nil {
                return errBadAttrValue
            }
            v.Type = "BOOL"
        case "NULL":
            if err := json.Unmarshal(payload, &v.NULL); err != nil {
                return errBadAttrValue
            }
            v.Type = "NULL"
        case "M":
            if err := json.Unmarshal(payload, &v.M); err != nil {
                return errBadAttrValue
            }
            v.Type = "M"
        case "L":
            if err := json.Unmarshal(payload, &v.L); err != nil {
                return errBadAttrValue
            }
            v.Type = "L"
        case "SS":
            if err := json.Unmarshal(payload, &v.SS); err != nil {
                return errBadAttrValue
            }
            v.Type = "SS"
        case "NS":
            if err := json.Unmarshal(payload, &v.NS); err != nil {
                return errBadAttrValue
            }
            v.Type = "NS"
        case "BS":
            if err := json.Unmarshal(payload, &v.BS); err != nil {
                return errBadAttrValue
            }
            v.Type = "BS"
        default:
            return errBadAttrValue
        }
    }
    return nil
}

// MarshalAttrValue renders an attribute value in DynamoDB JSON wire form.
func MarshalAttrValue(v *AttrValue) ([]byte, error) {
    var payload map[string]any
    switch v.Type {
    case "S":
        payload = map[string]any{"S": v.S}
    case "N":
        payload = map[string]any{"N": v.N}
    case "B":
        payload = map[string]any{"B": v.B}
    case "BOOL":
        payload = map[string]any{"BOOL": v.BOOL}
    case "NULL":
        payload = map[string]any{"NULL": true}
    case "M":
        m := v.M
        if m == nil {
            m = map[string]*AttrValue{}
        }
        payload = map[string]any{"M": m}
    case "L":
        l := v.L
        if l == nil {
            l = []*AttrValue{}
        }
        payload = map[string]any{"L": l}
    case "SS":
        s := v.SS
        if s == nil {
            s = []string{}
        }
        payload = map[string]any{"SS": s}
    case "NS":
        s := v.NS
        if s == nil {
            s = []string{}
        }
        payload = map[string]any{"NS": s}
    case "BS":
        s := v.BS
        if s == nil {
            s = [][]byte{}
        }
        payload = map[string]any{"BS": s}
    default:
        payload = map[string]any{"NULL": true}
    }
    return json.Marshal(payload)
}

func (v *AttrValue) MarshalJSON() ([]byte, error) {
    return MarshalAttrValue(v)
}

// ---------------------------------------------------------------------------
// Sort-key ordering
// ---------------------------------------------------------------------------

func parseDecimal(s string) (*big.Float, bool) {
    trimmed := strings.TrimSpace(s)
    if trimmed == "" {
        return nil, false
    }
    f, ok := new(big.Float).SetPrec(384).SetString(trimmed)
    return f, ok
}

func compareNumber(a, b string) int {
    fa, aok := parseDecimal(a)
    fb, bok := parseDecimal(b)
    if !aok || !bok {
        return strings.Compare(strings.TrimSpace(a), strings.TrimSpace(b))
    }
    return fa.Cmp(fb)
}

func scalarValueType(v *AttrValue) int {
    switch v.Type {
    case "N":
        return 0
    case "S":
        return 1
    case "B":
        return 2
    }
    return 3
}

// CompareValues returns -1/0/1 using the DynamoDB scalar ordering: numbers
// before strings before binaries; S compares as UTF-8 bytes, N numerically,
// B by unsigned bytes.
func CompareValues(a, b *AttrValue) int {
    if a == nil || b == nil {
        if a == nil && b == nil {
            return 0
        }
        if a == nil {
            return -1
        }
        return 1
    }
    ra, rb := scalarValueType(a), scalarValueType(b)
    if ra != rb {
        if ra < rb {
            return -1
        }
        return 1
    }
    switch ra {
    case 0:
        return compareNumber(a.N, b.N)
    case 1:
        return strings.Compare(a.S, b.S)
    case 2:
        return bytes.Compare(a.B, b.B)
    }
    return 0
}

func isIndexable(v *AttrValue) bool {
    if v == nil {
        return false
    }
    switch v.Type {
    case "S":
        return v.S != ""
    case "N":
        return v.N != ""
    case "B":
        return len(v.B) > 0
    }
    return false
}

// ---------------------------------------------------------------------------
// Storage model
// ---------------------------------------------------------------------------

type Item struct {
    Attrs map[string]*AttrValue
    Seq   int
}

type IndexDef struct {
    Name             string
    HashKey          string
    RangeKey         string
    IsLSI            bool
    ProjectionType   string
    NonKeyAttributes []string
}

type Table struct {
    Name         string
    HashKey      string
    RangeKey     string
    AttrDefs     map[string]string
    AttrDefOrder []string
    IndexList    []*IndexDef
    IndexMap     map[string]*IndexDef
    Items        []*Item
    NextSeq      int
    BillingMode  string
    ReadCap      int
    WriteCap     int
    CreatedAt    float64
}

func (t *Table) keyNames() []string {
    if t.RangeKey != "" {
        return []string{t.HashKey, t.RangeKey}
    }
    return []string{t.HashKey}
}

func (t *Table) findItem(key map[string]*AttrValue) (int, *Item) {
    hash, ok := key[t.HashKey]
    if !ok {
        return -1, nil
    }
    for i, item := range t.Items {
        if CompareValues(item.Attrs[t.HashKey], hash) != 0 {
            continue
        }
        if t.RangeKey != "" {
            rng, ok2 := key[t.RangeKey]
            if !ok2 || CompareValues(item.Attrs[t.RangeKey], rng) != 0 {
                continue
            }
        }
        return i, item
    }
    return -1, nil
}

// Store holds every table.
type Store struct {
    mu     sync.RWMutex
    tables map[string]*Table
}

func NewStore() *Store {
    return &Store{tables: make(map[string]*Table)}
}

// DefaultStore backs the HTTP server started by main.
var DefaultStore = NewStore()

func (s *Store) GetTable(name string) *Table {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.tables[name]
}

// ---------------------------------------------------------------------------
// Wire errors
// ---------------------------------------------------------------------------

const coralService = "com.amazon.coral.service#"

type wireError struct {
    Status int
    Type   string
    Msg    string
}

func validationErrf(format string, args ...any) *wireError {
    return &wireError{Status: http.StatusBadRequest, Type: coralService + "ValidationException", Msg: fmt.Sprintf(format, args...)}
}

func resourceNotFoundErrf(format string, args ...any) *wireError {
    return &wireError{Status: http.StatusBadRequest, Type: coralService + "ResourceNotFoundException", Msg: fmt.Sprintf(format, args...)}
}

func serializationErr() *wireError {
    return &wireError{Status: http.StatusBadRequest, Type: coralService + "SerializationException", Msg: "Failed to parse the request body as JSON."}
}

func errBody(e *wireError) []byte {
    out, err := json.Marshal(map[string]string{"__type": e.Type, "message": e.Msg})
    if err != nil {
        return []byte(`{"__type":"` + coralService + `ValidationException"}`)
    }
    return out
}

func decodeJSON(body []byte, v any) *wireError {
    if err := json.Unmarshal(body, v); err != nil {
        return serializationErr()
    }
    return nil
}

func lookupTable(store *Store, name string) (*Table, *wireError) {
    table := store.tables[name]
    if table == nil {
        return nil, resourceNotFoundErrf("Requested resource not found: Table: %s not found", name)
    }
    return table, nil
}

// ---------------------------------------------------------------------------
// KeyConditionExpression parsing
// ---------------------------------------------------------------------------

const (
    opEQ         = "EQ"
    opLT         = "LT"
    opLE         = "LE"
    opGT         = "GT"
    opGE         = "GE"
    opBetween    = "BETWEEN"
    opBeginsWith = "BEGINS_WITH"
)

// KeyCondition is one parsed comparison from a KeyConditionExpression.
type KeyCondition struct {
    Attr  string
    Op    string
    Value *AttrValue // EQ/LT/LE/GT/GE/BEGINS_WITH operand
    Lo    *AttrValue // BETWEEN lower bound
    Hi    *AttrValue // BETWEEN upper bound
}

type kceTokenKind int

const (
    kceIdent kceTokenKind = iota
    kceName
    kceValue
    kceOp
    kceLParen
    kceRParen
    kceComma
)

type kceToken struct {
    kind kceTokenKind
    text string
}

func isSpaceByte(c byte) bool {
    return c == ' ' || (c >= 9 && c <= 13)
}

func isKceIdentChar(c byte) bool {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

func kceTokens(expr string) ([]kceToken, *wireError) {
    toks := make([]kceToken, 0, 8)
    i := 0
    for i < len(expr) {
        c := expr[i]
        switch {
        case isSpaceByte(c):
            i++
        case c == '#':
            end := i + 1
            for end < len(expr) && isKceIdentChar(expr[end]) {
                end++
            }
            if end == i+1 {
                return nil, kceSyntaxError(expr[i:])
            }
            toks = append(toks, kceToken{kceName, expr[i:end]})
            i = end
        case c == ':':
            end := i + 1
            for end < len(expr) && isKceIdentChar(expr[end]) {
                end++
            }
            if end == i+1 {
                return nil, kceSyntaxError(expr[i:])
            }
            toks = append(toks, kceToken{kceValue, expr[i:end]})
            i = end
        case c == '(':
            toks = append(toks, kceToken{kceLParen, "("})
            i++
        case c == ')':
            toks = append(toks, kceToken{kceRParen, ")"})
            i++
        case c == ',':
            toks = append(toks, kceToken{kceComma, ","})
            i++
        case c == '=':
            toks = append(toks, kceToken{kceOp, "="})
            i++
        case c == '<' || c == '>':
            if i+1 < len(expr) && expr[i+1] == '=' {
                toks = append(toks, kceToken{kceOp, expr[i : i+2]})
                i += 2
            } else {
                toks = append(toks, kceToken{kceOp, expr[i : i+1]})
                i++
            }
        case isKceIdentChar(c):
            end := i
            for end < len(expr) && isKceIdentChar(expr[end]) {
                end++
            }
            toks = append(toks, kceToken{kceIdent, expr[i:end]})
            i = end
        default:
            return nil, kceSyntaxError(expr[i:])
        }
    }
    return toks, nil
}

func kceSyntaxError(rest string) *wireError {
    rest = strings.TrimSpace(rest)
    if len(rest) > 16 {
        rest = rest[:16]
    }
    return validationErrf("Invalid KeyConditionExpression: Syntax error; token: %q", rest)
}

// ParseKeyConditionExpression parses a KeyConditionExpression into its AND
// separated conditions, resolving #names and :values from the request maps.
func ParseKeyConditionExpression(expr string, names map[string]string, values map[string]*AttrValue) ([]KeyCondition, *wireError) {
    toks, werr := kceTokens(expr)
    if werr != nil {
        return nil, werr
    }
    if len(toks) == 0 {
        return nil, validationErrf("Query key condition not supported")
    }
    conds := make([]KeyCondition, 0, 2)
    pos := 0
    for {
        for pos < len(toks) && toks[pos].kind == kceLParen {
            pos++
        }
        cond, next, werr := kceParseCondition(toks, pos, names, values)
        if werr != nil {
            return nil, werr
        }
        conds = append(conds, *cond)
        pos = next
        for pos < len(toks) && toks[pos].kind == kceRParen {
            pos++
        }
        if pos < len(toks) && toks[pos].kind == kceIdent && strings.EqualFold(toks[pos].text, "and") {
            pos++
            continue
        }
        break
    }
    for pos < len(toks) && toks[pos].kind == kceRParen {
        pos++
    }
    if pos != len(toks) {
        return nil, kceSyntaxError(toks[pos].text)
    }
    return conds, nil
}

func kceParseCondition(toks []kceToken, pos int, names map[string]string, values map[string]*AttrValue) (*KeyCondition, int, *wireError) {
    if pos >= len(toks) {
        return nil, 0, validationErrf("Query key condition not supported")
    }
    tok := toks[pos]
    if tok.kind == kceIdent {
        switch strings.ToLower(tok.text) {
        case "begins_with":
            return kceParseBeginsWith(toks, pos, names, values)
        case "and", "or", "not", "in", "between", "contains", "not_contains", "size", "list_append", "if_not_exists", "attribute_exists", "attribute_not_exists", "attribute_type":
            return nil, 0, validationErrf("Query key condition not supported")
        }
    }
    var attr string
    switch tok.kind {
    case kceIdent, kceName:
        resolved, werr := kceResolveName(tok, names)
        if werr != nil {
            return nil, 0, werr
        }
        attr = resolved
    default:
        return nil, 0, validationErrf("Query key condition not supported")
    }
    pos++
    if pos >= len(toks) {
        return nil, 0, validationErrf("Query key condition not supported")
    }
    opTok := toks[pos]
    if opTok.kind == kceOp {
        value, next, werr := kceParseOperandValue(toks, pos+1, values)
        if werr != nil {
            return nil, 0, werr
        }
        return &KeyCondition{Attr: attr, Op: kceOpCode(opTok.text), Value: value}, next, nil
    }
    if opTok.kind == kceIdent && strings.EqualFold(opTok.text, "between") {
        lo, next, werr := kceParseOperandValue(toks, pos+1, values)
        if werr != nil {
            return nil, 0, werr
        }
        if next >= len(toks) || toks[next].kind != kceIdent || !strings.EqualFold(toks[next].text, "and") {
            return nil, 0, validationErrf("Query key condition not supported")
        }
        hi, next2, werr := kceParseOperandValue(toks, next+1, values)
        if werr != nil {
            return nil, 0, werr
        }
        return &KeyCondition{Attr: attr, Op: opBetween, Lo: lo, Hi: hi}, next2, nil
    }
    return nil, 0, validationErrf("Query key condition not supported")
}

func kceParseBeginsWith(toks []kceToken, pos int, names map[string]string, values map[string]*AttrValue) (*KeyCondition, int, *wireError) {
    fail := func() (*KeyCondition, int, *wireError) {
        return nil, 0, validationErrf("Query key condition not supported")
    }
    pos++
    if pos >= len(toks) || toks[pos].kind != kceLParen {
        return fail()
    }
    pos++
    if pos >= len(toks) || (toks[pos].kind != kceIdent && toks[pos].kind != kceName) {
        return fail()
    }
    attr, werr := kceResolveName(toks[pos], names)
    if werr != nil {
        return nil, 0, werr
    }
    pos++
    if pos >= len(toks) || toks[pos].kind != kceComma {
        return fail()
    }
    pos++
    value, next, werr := kceParseOperandValue(toks, pos, values)
    if werr != nil {
        return nil, 0, werr
    }
    if next >= len(toks) || toks[next].kind != kceRParen {
        return fail()
    }
    return &KeyCondition{Attr: attr, Op: opBeginsWith, Value: value}, next + 1, nil
}

func kceResolveName(tok kceToken, names map[string]string) (string, *wireError) {
    if tok.kind == kceName {
        name, ok := names[tok.text]
        if !ok || name == "" {
            return "", validationErrf("The provided expression refers to an attribute that does not exist in the table")
        }
        return name, nil
    }
    return tok.text, nil
}

func kceParseOperandValue(toks []kceToken, pos int, values map[string]*AttrValue) (*AttrValue, int, *wireError) {
    if pos >= len(toks) {
        return nil, 0, validationErrf("Query key condition not supported")
    }
    tok := toks[pos]
    if tok.kind == kceValue {
        value, ok := values[tok.text]
        if !ok || value == nil {
            return nil, 0, validationErrf("The provided expression refers to an attribute that does not exist in the table")
        }
        return value, pos + 1, nil
    }
    return nil, 0, validationErrf("Query key condition not supported")
}

func kceOpCode(op string) string {
    switch op {
    case "=":
        return opEQ
    case "<":
        return opLT
    case "<=":
        return opLE
    case ">":
        return opGT
    case ">=":
        return opGE
    }
    return opEQ
}

func rangeConditionMatches(cond *KeyCondition, value *AttrValue) bool {
    switch cond.Op {
    case opEQ:
        return CompareValues(value, cond.Value) == 0
    case opLT:
        return CompareValues(value, cond.Value) < 0
    case opLE:
        return CompareValues(value, cond.Value) <= 0
    case opGT:
        return CompareValues(value, cond.Value) > 0
    case opGE:
        return CompareValues(value, cond.Value) >= 0
    case opBetween:
        return CompareValues(value, cond.Lo) >= 0 && CompareValues(value, cond.Hi) <= 0
    case opBeginsWith:
        if value.Type == "S" && cond.Value.Type == "S" {
            return strings.HasPrefix(value.S, cond.Value.S)
        }
        if value.Type == "B" && cond.Value.Type == "B" {
            return bytes.HasPrefix(value.B, cond.Value.B)
        }
        return false
    }
    return false
}

func startKeyWithinBoundaries(cond *KeyCondition, value *AttrValue) bool {
    switch cond.Op {
    case opEQ:
        return CompareValues(value, cond.Value) == 0
    case opLT, opLE:
        return CompareValues(value, cond.Value) <= 0
    case opGT, opGE:
        return CompareValues(value, cond.Value) >= 0
    case opBetween:
        return CompareValues(value, cond.Lo) >= 0 && CompareValues(value, cond.Hi) <= 0
    case opBeginsWith:
        if value.Type == "S" && cond.Value.Type == "S" {
            return strings.HasPrefix(value.S, cond.Value.S)
        }
        return true
    }
    return true
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

// QueryRequest mirrors the DynamoDB Query request document.
type QueryRequest struct {
    TableName                 string
    IndexName                 string
    KeyConditionExpression    *string
    ExpressionAttributeNames  map[string]string
    ExpressionAttributeValues map[string]*AttrValue
    ConsistentRead            *bool
    Limit                     *json.Number
    ScanIndexForward          *bool
    ExclusiveStartKey         map[string]*AttrValue
    Select                    string
    ReturnConsumedCapacity    string
}

type capacity struct {
    TableName     string  `json:"TableName"`
    CapacityUnits float64 `json:"CapacityUnits"`
}

type queryResponse struct {
    Items            *[]map[string]*AttrValue `json:",omitempty"`
    Count            int                      `json:"Count"`
    ScannedCount     int                      `json:"ScannedCount"`
    ConsumedCapacity *capacity                `json:",omitempty"`
    LastEvaluatedKey map[string]*AttrValue    `json:",omitempty"`
}

func resolveIndex(table *Table, name string) (*IndexDef, *wireError) {
    if name == "" {
        return nil, nil
    }
    index, ok := table.IndexMap[name]
    if !ok {
        return nil, validationErrf("The table does not have the specified index: %s", name)
    }
    return index, nil
}

func keyAttrTypeOK(table *Table, name string, value *AttrValue) bool {
    want := table.AttrDefs[name]
    if want == "" {
        return value.Type == "S" || value.Type == "N" || value.Type == "B"
    }
    return value.Type == want
}

func checkKeyValueType(table *Table, name string, value *AttrValue) *wireError {
    want := table.AttrDefs[name]
    if want == "" {
        want = "S"
    }
    if value.Type != want {
        return validationErrf("One or more parameter values were invalid: Condition parameter type does not match schema type")
    }
    return nil
}

func validateKeyConditions(conds []KeyCondition, hashName, rangeName string) (*KeyCondition, *KeyCondition, *wireError) {
    var hashCond, rangeCond *KeyCondition
    unknowns := 0
    for i := range conds {
        cond := &conds[i]
        switch {
        case cond.Attr == hashName:
            if hashCond != nil {
                return nil, nil, validationErrf("Query key condition not supported")
            }
            if cond.Op != opEQ {
                return nil, nil, validationErrf("Query key condition not supported")
            }
            hashCond = cond
        case rangeName != "" && cond.Attr == rangeName:
            if rangeCond != nil {
                return nil, nil, validationErrf("Query key condition not supported")
            }
            rangeCond = cond
        default:
            unknowns++
        }
    }
    if hashCond == nil {
        return nil, nil, validationErrf("Query condition missed key schema element: `%s`", hashName)
    }
    if unknowns > 0 {
        return nil, nil, validationErrf("Query key condition not supported")
    }
    return hashCond, rangeCond, nil
}

func collectPartitionItems(table *Table, index *IndexDef, hashName, rangeName string, hashCond, rangeCond *KeyCondition) []*Item {
    var matched []*Item
    for _, item := range table.Items {
        hash, ok := item.Attrs[hashName]
        if !ok {
            continue
        }
        if index != nil && !isIndexable(hash) {
            continue
        }
        if CompareValues(hash, hashCond.Value) != 0 {
            continue
        }
        if index != nil && index.IsLSI && index.RangeKey != "" {
            if _, ok := item.Attrs[index.RangeKey]; !ok {
                continue
            }
        }
        if rangeCond != nil {
            rng, ok := item.Attrs[rangeName]
            if !ok {
                continue
            }
            if index != nil && !isIndexable(rng) {
                continue
            }
            if !rangeConditionMatches(rangeCond, rng) {
                continue
            }
        }
        matched = append(matched, item)
    }
    return matched
}

func sortPartitionItems(items []*Item, rangeName string) {
    if rangeName == "" || len(items) <= 1 {
        return
    }
    sort.SliceStable(items, func(i, j int) bool {
        a, aok := items[i].Attrs[rangeName]
        b, bok := items[j].Attrs[rangeName]
        ra, rb := -2, -2
        if aok {
            ra = scalarValueType(a)
        }
        if bok {
            rb = scalarValueType(b)
        }
        if ra != rb {
            return ra < rb
        }
        if !aok || !bok {
            return false
        }
        return CompareValues(a, b) < 0
    })
}

func reverseItems(items []*Item) {
    for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
        items[i], items[j] = items[j], items[i]
    }
}

func applyExclusiveStartKey(table *Table, esk map[string]*AttrValue, matched *[]*Item, hashName, rangeName string, hashCond, rangeCond *KeyCondition, forward bool) *wireError {
    hashValue, ok := esk[hashName]
    if !ok || hashValue == nil || !keyAttrTypeOK(table, hashName, hashValue) {
        return validationErrf("The provided starting key is invalid")
    }
    if CompareValues(hashValue, hashCond.Value) != 0 {
        return validationErrf("The provided starting key is outside query boundaries based on provided conditions")
    }
    if rangeName == "" {
        return nil
    }
    rangeValue, ok := esk[rangeName]
    if !ok || rangeValue == nil || !keyAttrTypeOK(table, rangeName, rangeValue) {
        return validationErrf("The provided starting key is invalid")
    }
    if rangeCond != nil && !startKeyWithinBoundaries(rangeCond, rangeValue) {
        return validationErrf("The provided starting key is outside query boundaries based on provided conditions")
    }
    kept := (*matched)[:0]
    for _, item := range *matched {
        itemRange, ok := item.Attrs[rangeName]
        if !ok {
            continue
        }
        comparison := CompareValues(itemRange, rangeValue)
        if forward && comparison > 0 {
            kept = append(kept, item)
        } else if !forward && comparison < 0 {
            kept = append(kept, item)
        }
    }
    *matched = kept
    return nil
}

func queryLimit(raw *json.Number) (int, *wireError) {
    if raw == nil {
        return -1, nil
    }
    n, err := raw.Int64()
    if err != nil || n < 1 {
        return -1, validationErrf("1 validation error detected: Value '%s' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1", raw.String())
    }
    return int(n), nil
}

func primaryKeyOf(item *Item, baseHash, baseRange, queryHash, queryRange string) map[string]*AttrValue {
    lek := map[string]*AttrValue{}
    for _, name := range []string{queryHash, queryRange, baseHash, baseRange} {
        if name == "" {
            continue
        }
        if value, ok := item.Attrs[name]; ok {
            lek[name] = value
        }
    }
    return lek
}

// HandleQuery implements DynamoDB_20120810.Query over the tables in store.
func HandleQuery(store *Store, body []byte) (int, []byte) {
    var req QueryRequest
    if e := decodeJSON(body, &req); e != nil {
        return e.Status, errBody(e)
    }

    store.mu.RLock()
    defer store.mu.RUnlock()

    table, werr := lookupTable(store, req.TableName)
    if werr != nil {
        return werr.Status, errBody(werr)
    }

    index, werr := resolveIndex(table, req.IndexName)
    if werr != nil {
        return werr.Status, errBody(werr)
    }
    consistent := req.ConsistentRead != nil && *req.ConsistentRead
    if index != nil && !index.IsLSI && consistent {
        e := validationErrf("ConsistentRead is not supported on global secondary indexes")
        return e.Status, errBody(e)
    }

    hashName, rangeName := table.HashKey, table.RangeKey
    if index != nil {
        hashName, rangeName = index.HashKey, index.RangeKey
    }

    if req.Select == "ALL_PROJECTED_ATTRIBUTES" && index == nil {
        e := validationErrf("Select ALL_PROJECTED_ATTRIBUTES is only valid for use with an index")
        return e.Status, errBody(e)
    }

    if req.KeyConditionExpression == nil {
        e := validationErrf("Either the KeyConditions or KeyConditionExpression parameter must be specified in the request.")
        return e.Status, errBody(e)
    }

    conds, werr := ParseKeyConditionExpression(*req.KeyConditionExpression, req.ExpressionAttributeNames, req.ExpressionAttributeValues)
    if werr != nil {
        return werr.Status, errBody(werr)
    }

    hashCond, rangeCond, werr := validateKeyConditions(conds, hashName, rangeName)
    if werr != nil {
        return werr.Status, errBody(werr)
    }
    if werr := checkKeyValueType(table, hashName, hashCond.Value); werr != nil {
        return werr.Status, errBody(werr)
    }

    matched := collectPartitionItems(table, index, hashName, rangeName, hashCond, rangeCond)

    sortPartitionItems(matched, rangeName)
    forward := req.ScanIndexForward == nil || *req.ScanIndexForward
    if !forward {
        reverseItems(matched)
    }

    if len(req.ExclusiveStartKey) > 0 {
        if rangeName == "" {
            // Tables without a sort key have no wire-expressible resume
            // position; the remainder of the partition is consumed.
            matched = nil
        } else if werr := applyExclusiveStartKey(table, req.ExclusiveStartKey, &matched, hashName, rangeName, hashCond, rangeCond, forward); werr != nil {
            return werr.Status, errBody(werr)
        }
    }

    limit, werr := queryLimit(req.Limit)
    if werr != nil {
        return werr.Status, errBody(werr)
    }

    result := matched
    var lastEvaluatedKey map[string]*AttrValue
    if req.Limit != nil && len(matched) > limit {
        result = matched[:limit]
        lastEvaluatedKey = primaryKeyOf(matched[limit-1], table.HashKey, table.RangeKey, hashName, rangeName)
    }

    count := len(result)
    var itemsPtr *[]map[string]*AttrValue
    if req.Select != "COUNT" {
        items := make([]map[string]*AttrValue, 0, count)
        for _, item := range result {
            items = append(items, item.Attrs)
        }
        itemsPtr = &items
    }

    resp := queryResponse{
        Items:            itemsPtr,
        Count:            count,
        ScannedCount:     count,
        LastEvaluatedKey: lastEvaluatedKey,
    }
    if req.ReturnConsumedCapacity == "TOTAL" || req.ReturnConsumedCapacity == "INDEXES" {
        units := 0.5
        if consistent && index == nil {
            units = 1
        }
        resp.ConsumedCapacity = &capacity{TableName: req.TableName, CapacityUnits: units}
    }

    out, merr := json.Marshal(resp)
    if merr != nil {
        e := serializationErr()
        return e.Status, errBody(e)
    }
    return http.StatusOK, out
}

// ---------------------------------------------------------------------------
// Item operations used to seed and inspect partitions
// ---------------------------------------------------------------------------

func validateKeyTypes(table *Table, key map[string]*AttrValue) *wireError {
    for _, name := range table.keyNames() {
        value, ok := key[name]
        if !ok {
            continue
        }
        want := table.AttrDefs[name]
        if want == "" {
            want = "S"
        }
        if value.Type != want {
            return validationErrf("One or more parameter values were invalid: Type mismatch for key %s expected: %s actual: %s", name, want, value.Type)
        }
        if value.Type == "S" && value.S == "" {
            return validationErrf("One or more parameter values were invalid: An attribute value may not contain an empty string")
        }
        if value.Type == "B" && len(value.B) == 0 {
            return validationErrf("One or more parameter values were invalid: An attribute value may not contain an empty binary value")
        }
    }
    return nil
}

func validateKeyMap(table *Table, key map[string]*AttrValue) *wireError {
    for _, name := range table.keyNames() {
        if _, ok := key[name]; !ok {
            return validationErrf("One or more parameter values were invalid: Missing the key %s in the item", name)
        }
    }
    return validateKeyTypes(table, key)
}

func validateRequestKey(table *Table, key map[string]*AttrValue) *wireError {
    for _, name := range table.keyNames() {
        if _, ok := key[name]; !ok {
            return validationErrf("One or more parameter values were invalid: The number of conditions on the key is invalid")
        }
    }
    return validateKeyTypes(table, key)
}

func capacityIfRequested(param string, table *Table, units float64) *capacity {
    if param == "TOTAL" || param == "INDEXES" {
        return &capacity{TableName: table.Name, CapacityUnits: units}
    }
    return nil
}

func HandlePutItem(store *Store, body []byte) (int, []byte) {
    var req struct {
        TableName              string
        Item                   map[string]*AttrValue
        ReturnValues           string
        ReturnConsumedCapacity string
    }
    if e := decodeJSON(body, &req); e != nil {
        return e.Status, errBody(e)
    }
    store.mu.Lock()
    defer store.mu.Unlock()
    table, werr := lookupTable(store, req.TableName)
    if werr != nil {
        return werr.Status, errBody(werr)
    }
    if werr := validateKeyMap(table, req.Item); werr != nil {
        return werr.Status, errBody(werr)
    }
    var old map[string]*AttrValue
    _, prev := table.findItem(req.Item)
    if prev != nil {
        old = prev.Attrs
        prev.Attrs = req.Item
    } else {
        table.NextSeq++
        table.Items = append(table.Items, &Item{Attrs: req.Item, Seq: table.NextSeq})
    }
    resp := map[string]any{}
    if req.ReturnValues == "ALL_OLD" && old != nil {
        resp["Attributes"] = old
    }
    if cap := capacityIfRequested(req.ReturnConsumedCapacity, table, 1); cap != nil {
        resp["ConsumedCapacity"] = cap
    }
    out, merr := json.Marshal(resp)
    if merr != nil {
        e := serializationErr()
        return e.Status, errBody(e)
    }
    return http.StatusOK, out
}

func HandleGetItem(store *Store, body []byte) (int, []byte) {
    var req struct {
        TableName              string
        Key                    map[string]*AttrValue
        ConsistentRead         *bool
        ReturnConsumedCapacity string
    }
    if e := decodeJSON(body, &req); e != nil {
        return e.Status, errBody(e)
    }
    store.mu.RLock()
    defer store.mu.RUnlock()
    table, werr := lookupTable(store, req.TableName)
    if werr != nil {
        return werr.Status, errBody(werr)
    }
    if werr := validateRequestKey(table, req.Key); werr != nil {
        return werr.Status, errBody(werr)
    }
    _, prev := table.findItem(req.Key)
    units := 0.5
    if req.ConsistentRead != nil && *req.ConsistentRead {
        units = 1
    }
    resp := map[string]any{}
    if prev != nil {
        resp["Item"] = prev.Attrs
    }
    if cap := capacityIfRequested(req.ReturnConsumedCapacity, table, units); cap != nil {
        resp["ConsumedCapacity"] = cap
    }
    out, merr := json.Marshal(resp)
    if merr != nil {
        e := serializationErr()
        return e.Status, errBody(e)
    }
    return http.StatusOK, out
}

func HandleDeleteItem(store *Store, body []byte) (int, []byte) {
    var req struct {
        TableName              string
        Key                    map[string]*AttrValue
        ReturnValues           string
        ReturnConsumedCapacity string
    }
    if e := decodeJSON(body, &req); e != nil {
        return e.Status, errBody(e)
    }
    store.mu.Lock()
    defer store.mu.Unlock()
    table, werr := lookupTable(store, req.TableName)
    if werr != nil {
        return werr.Status, errBody(werr)
    }
    if werr := validateRequestKey(table, req.Key); werr != nil {
        return werr.Status, errBody(werr)
    }
    var old map[string]*AttrValue
    pos, prev := table.findItem(req.Key)
    if prev != nil {
        old = prev.Attrs
        table.Items = append(table.Items[:pos], table.Items[pos+1:]...)
    }
    resp := map[string]any{}
    if req.ReturnValues == "ALL_OLD" && old != nil {
        resp["Attributes"] = old
    }
    if cap := capacityIfRequested(req.ReturnConsumedCapacity, table, 1); cap != nil {
        resp["ConsumedCapacity"] = cap
    }
    out, merr := json.Marshal(resp)
    if merr != nil {
        e := serializationErr()
        return e.Status, errBody(e)
    }
    return http.StatusOK, out
}

type scanResponse struct {
    Items            []map[string]*AttrValue
    Count            int
    ScannedCount     int
    ConsumedCapacity *capacity `json:",omitempty"`
}

func HandleScan(store *Store, body []byte) (int, []byte) {
    var req struct {
        TableName              string
        IndexName              string
        Select                 string
        ReturnConsumedCapacity string
    }
    if e := decodeJSON(body, &req); e != nil {
        return e.Status, errBody(e)
    }
    store.mu.RLock()
    defer store.mu.RUnlock()
    table, werr := lookupTable(store, req.TableName)
    if werr != nil {
        return werr.Status, errBody(werr)
    }
    list := table.Items
    if req.IndexName != "" {
        index, ok := table.IndexMap[req.IndexName]
        if !ok {
            werr := validationErrf("The table does not have the specified index: %s", req.IndexName)
            return werr.Status, errBody(werr)
        }
        list = nil
        for _, item := range table.Items {
            hash, ok := item.Attrs[index.HashKey]
            if !ok || !isIndexable(hash) {
                continue
            }
            if index.IsLSI && index.RangeKey != "" {
                if _, ok := item.Attrs[index.RangeKey]; !ok {
                    continue
                }
            }
            list = append(list, item)
        }
    }
    items := make([]map[string]*AttrValue, 0, len(list))
    for _, item := range list {
        items = append(items, item.Attrs)
    }
    resp := scanResponse{Items: items, Count: len(items), ScannedCount: len(items)}
    if cap := capacityIfRequested(req.ReturnConsumedCapacity, table, 0.5); cap != nil {
        resp.ConsumedCapacity = cap
    }
    out, merr := json.Marshal(resp)
    if merr != nil {
        e := serializationErr()
        return e.Status, errBody(e)
    }
    return http.StatusOK, out
}

type batchWriteRequestEntry struct {
    PutRequest *struct {
        Item map[string]*AttrValue
    }
    DeleteRequest *struct {
        Key map[string]*AttrValue
    }
}

func HandleBatchWriteItem(store *Store, body []byte) (int, []byte) {
    var req struct {
        RequestItems map[string][]batchWriteRequestEntry
    }
    if e := decodeJSON(body, &req); e != nil {
        return e.Status, errBody(e)
    }
    if len(req.RequestItems) == 0 {
        werr := validationErrf("One or more parameter values were invalid: RequestItems must not be empty")
        return werr.Status, errBody(werr)
    }
    store.mu.Lock()
    defer store.mu.Unlock()
    names := make([]string, 0, len(req.RequestItems))
    for name := range req.RequestItems {
        names = append(names, name)
    }
    sort.Strings(names)
    for _, name := range names {
        if _, ok := store.tables[name]; !ok {
            werr := resourceNotFoundErrf("Requested resource not found: Table: %s not found", name)
            return werr.Status, errBody(werr)
        }
    }
    for _, name := range names {
        table := store.tables[name]
        for _, entry := range req.RequestItems[name] {
            switch {
            case entry.PutRequest != nil:
                if werr := validateKeyMap(table, entry.PutRequest.Item); werr != nil {
                    return werr.Status, errBody(werr)
                }
                if _, prev := table.findItem(entry.PutRequest.Item); prev != nil {
                    prev.Attrs = entry.PutRequest.Item
                } else {
                    table.NextSeq++
                    table.Items = append(table.Items, &Item{Attrs: entry.PutRequest.Item, Seq: table.NextSeq})
                }
            case entry.DeleteRequest != nil:
                if werr := validateRequestKey(table, entry.DeleteRequest.Key); werr != nil {
                    return werr.Status, errBody(werr)
                }
                if pos, prev := table.findItem(entry.DeleteRequest.Key); prev != nil {
                    table.Items = append(table.Items[:pos], table.Items[pos+1:]...)
                }
            default:
                werr := validationErrf("One or more parameter values were invalid: Write request must specify Put or Delete")
                return werr.Status, errBody(werr)
            }
        }
    }
    out, merr := json.Marshal(map[string]any{"UnprocessedItems": map[string]any{}})
    if merr != nil {
        e := serializationErr()
        return e.Status, errBody(e)
    }
    return http.StatusOK, out
}

// ---------------------------------------------------------------------------
// Table operations
// ---------------------------------------------------------------------------

type KeySchemaElement struct {
    AttributeName string
    KeyType       string
}

type AttributeDefinition struct {
    AttributeName string
    AttributeType string
}

type IndexProjection struct {
    ProjectionType   string
    NonKeyAttributes []string
}

type provisionedThroughput struct {
    ReadCapacityUnits  int
    WriteCapacityUnits int
}

type IndexRequest struct {
    IndexName             string
    KeySchema             []KeySchemaElement
    Projection            IndexProjection
    ProvisionedThroughput *provisionedThroughput
}

type CreateTableRequest struct {
    TableName              string
    KeySchema              []KeySchemaElement
    AttributeDefinitions   []AttributeDefinition
    GlobalSecondaryIndexes []IndexRequest
    LocalSecondaryIndexes  []IndexRequest
    BillingMode            string
    ProvisionedThroughput  *provisionedThroughput
}

func keySchemaKeys(schema []KeySchemaElement) (string, string, *wireError) {
    hashKey, rangeKey := "", ""
    for _, element := range schema {
        switch strings.ToUpper(element.KeyType) {
        case "HASH":
            if hashKey != "" && hashKey != element.AttributeName {
                return "", "", validationErrf("Both the Key Schema and Index Schemas should be specified when you create a table")
            }
            hashKey = element.AttributeName
        case "RANGE":
            if rangeKey != "" && rangeKey != element.AttributeName {
                return "", "", validationErrf("Both the Key Schema and Index Schemas should be specified when you create a table")
            }
            rangeKey = element.AttributeName
        }
    }
    if hashKey == "" {
        return "", "", validationErrf("Both the Key Schema and Index Schemas should be specified when you create a table")
    }
    return hashKey, rangeKey, nil
}

func buildIndexDef(spec *IndexRequest, lsi bool) (*IndexDef, *wireError) {
    if spec.IndexName == "" {
        return nil, validationErrf("One or more parameter values were invalid: Index Name is required but was not present")
    }
    hashKey, rangeKey, werr := keySchemaKeys(spec.KeySchema)
    if werr != nil {
        return nil, werr
    }
    projectionType := spec.Projection.ProjectionType
    if projectionType == "" {
        projectionType = "ALL"
    }
    return &IndexDef{
        Name:             spec.IndexName,
        HashKey:          hashKey,
        RangeKey:         rangeKey,
        IsLSI:            lsi,
        ProjectionType:   projectionType,
        NonKeyAttributes: spec.Projection.NonKeyAttributes,
    }, nil
}

func buildTable(req CreateTableRequest) (*Table, *wireError) {
    if req.TableName == "" {
        return nil, validationErrf("TableName must be at least 3 characters long and at most 255 characters long")
    }
    hashKey, rangeKey, werr := keySchemaKeys(req.KeySchema)
    if werr != nil {
        return nil, werr
    }
    table := &Table{
        Name:        req.TableName,
        HashKey:     hashKey,
        RangeKey:    rangeKey,
        AttrDefs:    make(map[string]string),
        IndexMap:    make(map[string]*IndexDef),
        BillingMode: req.BillingMode,
        ReadCap:     5,
        WriteCap:    5,
        CreatedAt:   float64(time.Now().UnixNano()) / 1e9,
    }
    for _, def := range req.AttributeDefinitions {
        if _, seen := table.AttrDefs[def.AttributeName]; !seen {
            table.AttrDefOrder = append(table.AttrDefOrder, def.AttributeName)
        }
        table.AttrDefs[def.AttributeName] = def.AttributeType
    }
    for _, name := range table.keyNames() {
        if _, ok := table.AttrDefs[name]; !ok {
            table.AttrDefs[name] = "S"
            table.AttrDefOrder = append(table.AttrDefOrder, name)
        }
    }
    for i := range req.GlobalSecondaryIndexes {
        index, werr := buildIndexDef(&req.GlobalSecondaryIndexes[i], false)
        if werr != nil {
            return nil, werr
        }
        if _, dup := table.IndexMap[index.Name]; dup {
            return nil, validationErrf("One or more parameter values were invalid: Duplicate index name: %s", index.Name)
        }
        table.IndexList = append(table.IndexList, index)
        table.IndexMap[index.Name] = index
    }
    for i := range req.LocalSecondaryIndexes {
        index, werr := buildIndexDef(&req.LocalSecondaryIndexes[i], true)
        if werr != nil {
            return nil, werr
        }
        if _, dup := table.IndexMap[index.Name]; dup {
            return nil, validationErrf("One or more parameter values were invalid: Duplicate index name: %s", index.Name)
        }
        table.IndexList = append(table.IndexList, index)
        table.IndexMap[index.Name] = index
    }
    for _, index := range table.IndexList {
        for _, name := range []string{index.HashKey, index.RangeKey} {
            if name == "" {
                continue
            }
            if _, ok := table.AttrDefs[name]; !ok {
                table.AttrDefs[name] = "S"
                table.AttrDefOrder = append(table.AttrDefOrder, name)
            }
        }
    }
    switch req.BillingMode {
    case "PAY_PER_REQUEST":
        table.ReadCap, table.WriteCap = 0, 0
    default:
        if req.ProvisionedThroughput != nil {
            table.ReadCap = req.ProvisionedThroughput.ReadCapacityUnits
            table.WriteCap = req.ProvisionedThroughput.WriteCapacityUnits
        }
    }
    return table, nil
}

func tableDescription(table *Table) map[string]any {
    desc := map[string]any{
        "AttributeDefinitions": attrDefList(table),
        "CreationDateTime":     table.CreatedAt,
        "ItemCount":            len(table.Items),
        "KeySchema":            keySchemaList(table.HashKey, table.RangeKey),
        "TableArn":             "arn:aws:dynamodb:ddblocal:000000000000:table/" + table.Name,
        "TableName":            table.Name,
        "TableStatus":          "ACTIVE",
    }
    if table.BillingMode == "PAY_PER_REQUEST" {
        desc["BillingModeSummary"] = map[string]any{"BillingMode": "PAY_PER_REQUEST"}
        desc["ProvisionedThroughput"] = map[string]any{"NumberOfDecreasesToday": 0, "ReadCapacityUnits": 0, "WriteCapacityUnits": 0}
    } else {
        desc["ProvisionedThroughput"] = map[string]any{"NumberOfDecreasesToday": 0, "ReadCapacityUnits": table.ReadCap, "WriteCapacityUnits": table.WriteCap}
    }
    var gsis, lsis []map[string]any
    for _, index := range table.IndexList {
        entry := map[string]any{
            "IndexArn":        "arn:aws:dynamodb:ddblocal:000000000000:table/" + table.Name + "/index/" + index.Name,
            "IndexName":       index.Name,
            "IndexSizeBytes":  0,
            "ItemCount":       0,
            "KeySchema":       keySchemaList(index.HashKey, index.RangeKey),
            "Projection":      indexProjectionOut(index),
        }
        if !index.IsLSI {
            entry["ProvisionedThroughput"] = map[string]any{"NumberOfDecreasesToday": 0, "ReadCapacityUnits": table.ReadCap, "WriteCapacityUnits": table.WriteCap}
            gsis = append(gsis, entry)
        } else {
            lsis = append(lsis, entry)
        }
    }
    if len(gsis) > 0 {
        desc["GlobalSecondaryIndexes"] = gsis
    }
    if len(lsis) > 0 {
        desc["LocalSecondaryIndexes"] = lsis
    }
    return desc
}

func attrDefList(table *Table) []map[string]string {
    list := make([]map[string]string, 0, len(table.AttrDefOrder))
    for _, name := range table.AttrDefOrder {
        list = append(list, map[string]string{"AttributeName": name, "AttributeType": table.AttrDefs[name]})
    }
    return list
}

func keySchemaList(hashKey, rangeKey string) []map[string]string {
    list := []map[string]string{{"AttributeName": hashKey, "KeyType": "HASH"}}
    if rangeKey != "" {
        list = append(list, map[string]string{"AttributeName": rangeKey, "KeyType": "RANGE"})
    }
    return list
}

func indexProjectionOut(index *IndexDef) map[string]any {
    proj := map[string]any{"ProjectionType": index.ProjectionType}
    if len(index.NonKeyAttributes) > 0 {
        proj["NonKeyAttributes"] = index.NonKeyAttributes
    }
    return proj
}

func HandleCreateTable(store *Store, body []byte) (int, []byte) {
    var req CreateTableRequest
    if e := decodeJSON(body, &req); e != nil {
        return e.Status, errBody(e)
    }
    store.mu.Lock()
    defer store.mu.Unlock()
    if _, exists := store.tables[req.TableName]; exists {
        e := &wireError{Status: http.StatusBadRequest, Type: coralService + "ResourceInUseException", Msg: "Table already exists: " + req.TableName}
        return e.Status, errBody(e)
    }
    table, werr := buildTable(req)
    if werr != nil {
        return werr.Status, errBody(werr)
    }
    store.tables[table.Name] = table
    out, merr := json.Marshal(map[string]any{"TableDescription": tableDescription(table)})
    if merr != nil {
        e := serializationErr()
        return e.Status, errBody(e)
    }
    return http.StatusOK, out
}

func HandleDescribeTable(store *Store, body []byte) (int, []byte) {
    var req struct {
        TableName string
    }
    if e := decodeJSON(body, &req); e != nil {
        return e.Status, errBody(e)
    }
    store.mu.RLock()
    defer store.mu.RUnlock()
    table, werr := lookupTable(store, req.TableName)
    if werr != nil {
        return werr.Status, errBody(werr)
    }
    out, merr := json.Marshal(map[string]any{"Table": tableDescription(table)})
    if merr != nil {
        e := serializationErr()
        return e.Status, errBody(e)
    }
    return http.StatusOK, out
}

func HandleListTables(store *Store, body []byte) (int, []byte) {
    store.mu.RLock()
    defer store.mu.RUnlock()
    names := make([]string, 0, len(store.tables))
    for name := range store.tables {
        names = append(names, name)
    }
    sort.Strings(names)
    out, merr := json.Marshal(map[string]any{"TableNames": names})
    if merr != nil {
        e := serializationErr()
        return e.Status, errBody(e)
    }
    return http.StatusOK, out
}

func HandleDeleteTable(store *Store, body []byte) (int, []byte) {
    var req struct {
        TableName string
    }
    if e := decodeJSON(body, &req); e != nil {
        return e.Status, errBody(e)
    }
    store.mu.Lock()
    defer store.mu.Unlock()
    table, werr := lookupTable(store, req.TableName)
    if werr != nil {
        return werr.Status, errBody(werr)
    }
    delete(store.tables, table.Name)
    out, merr := json.Marshal(map[string]any{"TableDescription": tableDescription(table)})
    if merr != nil {
        e := serializationErr()
        return e.Status, errBody(e)
    }
    return http.StatusOK, out
}

// ---------------------------------------------------------------------------
// Dispatch, HTTP surface and entry point
// ---------------------------------------------------------------------------

// HandleRequest dispatches one DynamoDB API call (target without or with the
// "DynamoDB_20120810." prefix) against the store.
func HandleRequest(store *Store, target string, body []byte) (int, []byte) {
    op := target
    if i := strings.LastIndex(target, "."); i >= 0 && i+1 < len(target) {
        op = target[i+1:]
    }
    switch op {
    case "Query":
        return HandleQuery(store, body)
    case "Scan":
        return HandleScan(store, body)
    case "PutItem":
        return HandlePutItem(store, body)
    case "GetItem":
        return HandleGetItem(store, body)
    case "DeleteItem":
        return HandleDeleteItem(store, body)
    case "BatchWriteItem":
        return HandleBatchWriteItem(store, body)
    case "CreateTable":
        return HandleCreateTable(store, body)
    case "DescribeTable":
        return HandleDescribeTable(store, body)
    case "ListTables":
        return HandleListTables(store, body)
    case "DeleteTable":
        return HandleDeleteTable(store, body)
    default:
        return http.StatusBadRequest, []byte(`{"__type":"` + coralService + `UnknownOperationException"}`)
    }
}

// APIHandler returns the HTTP handler speaking the DynamoDB JSON protocol
// (application/x-amz-json-1.0 with the X-Amz-Target header).
func APIHandler(store *Store) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if r.Method == http.MethodPost {
            target := r.Header.Get("X-Amz-Target")
            payload, err := io.ReadAll(r.Body)
            if err != nil {
                e := serializationErr()
                w.Header().Set("Content-Type", "application/x-amz-json-1.0")
                w.WriteHeader(e.Status)
                _, _ = w.Write(errBody(e))
                return
            }
            status, out := HandleRequest(store, target, payload)
            w.Header().Set("Content-Type", "application/x-amz-json-1.0")
            w.WriteHeader(status)
            _, _ = w.Write(out)
            return
        }
        if r.Method == http.MethodOptions {
            w.Header().Set("Access-Control-Allow-Origin", "*")
            w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
            w.Header().Set("Access-Control-Allow-Headers", "X-Amz-Target, Content-Type")
            w.WriteHeader(http.StatusOK)
            return
        }
        w.Header().Set("Content-Type", "text/plain; charset=utf-8")
        w.WriteHeader(http.StatusOK)
        _, _ = w.Write([]byte("ok"))
    })
}

func main() {
    port := flag.Int("port", 8000, "TCP port for the DynamoDB-compatible endpoint")
    address := flag.String("address", "", "Optional host:port address to listen on")
    flag.Parse()

    listenAddr := ""
    if *address != "" {
        listenAddr = *address
    } else {
        listenPort := *port
        portSet := false
        flag.Visit(func(f *flag.Flag) {
            if f.Name == "port" {
                portSet = true
            }
        })
        if !portSet {
            for _, envKey := range []string{"PORT", "FLOCI_PORT", "DDB_PORT"} {
                if value := os.Getenv(envKey); value != "" {
                    if parsed, err := strconv.Atoi(value); err == nil {
                        listenPort = parsed
                    }
                    break
                }
            }
        }
        listenAddr = ":" + strconv.Itoa(listenPort)
    }

    listener, err := net.Listen("tcp", listenAddr)
    if err != nil {
        fmt.Fprintf(os.Stderr, "floci-go: unable to listen on %s: %v\n", listenAddr, err)
        os.Exit(1)
    }
    fmt.Printf("floci-go listening on %s\n", listener.Addr().String())
    server := &http.Server{Handler: APIHandler(DefaultStore)}
    if err := server.Serve(listener); err != nil {
        fmt.Fprintf(os.Stderr, "floci-go: server stopped: %v\n", err)
        os.Exit(1)
    }
}