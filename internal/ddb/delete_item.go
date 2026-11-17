// Package ddb implements the DynamoDB-compatible wire API for floci-go.
//
// Ticket floci-go/ticket-016 — DeleteItem:
//
//   - removes the single item matching the request Key from the named table,
//   - with ReturnValues "ALL_OLD" the previously stored item is returned in
//     Attributes (omitted when nothing was deleted, so the response is {}),
//   - the Key is validated against the table key schema; an unknown table
//     yields ResourceNotFoundException, a bad key ValidationException,
//   - ConditionExpression (and legacy Expected/ConditionalOperator) are
//     honoured; failures yield ConditionalCheckFailedException, including
//     the previous item when ReturnValuesOnConditionCheckFailure==ALL_OLD,
//   - ConsumedCapacity is reported for ReturnConsumedCapacity TOTAL/INDEXES.
//
// Route table wiring (handlers.go):
//
//	case DynamoDB_20120810.DeleteItem:
//		return HandleDeleteItem(store, body)
package ddb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// ---------------------------------------------------------------------------
// Attribute values, items, tables and the store
// ---------------------------------------------------------------------------

// AttributeValue mirrors the DynamoDB AttributeValue wire shape (PascalCase).
type AttributeValue struct {
	S    *string                    `json:"S,omitempty"`
	N    *string                    `json:"N,omitempty"`
	B    []byte                     `json:"B,omitempty"`
	BOOL *bool                      `json:"BOOL,omitempty"`
	NULL *bool                      `json:"NULL,omitempty"`
	SS   []string                   `json:"SS,omitempty"`
	NS   []string                   `json:"NS,omitempty"`
	BS   [][]byte                   `json:"BS,omitempty"`
	M    map[string]*AttributeValue `json:"M,omitempty"`
	L    []*AttributeValue          `json:"L,omitempty"`
}

// Item is a stored item keyed by attribute name.
type Item = map[string]*AttributeValue

// KeySchemaElement identifies one key attribute of a table.
type KeySchemaElement struct {
	AttributeName string `json:"AttributeName"`
	KeyType       string `json:"KeyType"` // "HASH" or "RANGE"
}

// Table is an in-memory table.
type Table struct {
	Name      string
	KeySchema []KeySchemaElement
	KeyTypes  map[string]string // key attribute -> S | N | B
	Items     map[string]Item

	mu sync.RWMutex
}

// Store holds every table served by the emulator.
type Store struct {
	mu     sync.RWMutex
	tables map[string]*Table
}

// NewStore creates an empty store.
func NewStore() *Store { return &Store{tables: map[string]*Table{}} }

// AddTable registers t under its name.
func (s *Store) AddTable(t *Table) {
	if t.Items == nil {
		t.Items = map[string]Item{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tables[t.Name] = t
}

func (s *Store) lookupTable(name string) (*Table, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tables[name]
	return t, ok
}

// NewTable creates an empty table; pass "" as rangeKey/rangeType for a
// hash-only schema. hashType/rangeType are S, N or B.
func NewTable(name, hashKey, hashType, rangeKey, rangeType string) *Table {
	t := &Table{
		Name:      name,
		KeySchema: []KeySchemaElement{{AttributeName: hashKey, KeyType: "HASH"}},
		KeyTypes:  map[string]string{hashKey: hashType},
		Items:     map[string]Item{},
	}
	if rangeKey != "" {
		t.KeySchema = append(t.KeySchema, KeySchemaElement{AttributeName: rangeKey, KeyType: "RANGE"})
		t.KeyTypes[rangeKey] = rangeType
	}
	return t
}

// ---------------------------------------------------------------------------
// Errors (DynamoDB __type encoding, HTTP 400 for client errors)
// ---------------------------------------------------------------------------

// APIError maps an operation failure to its wire representation.
type APIError struct {
	HTTP    int
	Code    string
	Message string
	item    json.RawMessage // Item payload for conditional check failures
}

// Error implements the error interface.
func (e *APIError) Error() string { return e.Code + ": " + e.Message }

type errorBody struct {
	Type    string          `json:"__type"`
	Message string          `json:"message"`
	Item    json.RawMessage `json:"Item,omitempty"`
}

const ddbTypePrefix = "com.amazonaws.dynamodb.v20120810#"

// wireBody encodes the AWS JSON 1.0 error document.
func (e *APIError) wireBody() []byte {
	b, err := json.Marshal(errorBody{Type: ddbTypePrefix + e.Code, Message: e.Message, Item: e.item})
	if err != nil {
		return []byte("{\"__type\":\"com.amazonaws.dynamodb.v20120810#InternalFailure\",\"message\":\"internal error\"}")
	}
	return b
}

func validationErrf(format string, args ...any) *APIError {
	return &APIError{HTTP: http.StatusBadRequest, Code: "ValidationException", Message: fmt.Sprintf(format, args...)}
}

func resourceNotFoundErr() *APIError {
	return &APIError{
		HTTP:    http.StatusBadRequest,
		Code:    "ResourceNotFoundException",
		Message: "Cannot do operations on a non-existent table",
	}
}

func conditionalCheckFailedErr() *APIError {
	return &APIError{
		HTTP:    http.StatusBadRequest,
		Code:    "ConditionalCheckFailedException",
		Message: "The conditional request failed",
	}
}

// ---------------------------------------------------------------------------
// DeleteItem (ticket-016)
// ---------------------------------------------------------------------------

// DeleteItemTarget is the x-amz-target value for DeleteItem.
const DeleteItemTarget = "DynamoDB_20120810.DeleteItem"

// ExpectedCondition is one entry of the legacy Expected parameter.
type ExpectedCondition struct {
	Exists             *bool             `json:"Exists,omitempty"`
	Value              *AttributeValue   `json:"Value,omitempty"`
	ComparisonOperator string            `json:"ComparisonOperator,omitempty"`
	AttributeValueList []*AttributeValue `json:"AttributeValueList,omitempty"`
}

// DeleteItemInput is the DeleteItem request body.
type DeleteItemInput struct {
	TableName                           string                     `json:"TableName"`
	Key                                 Item                       `json:"Key"`
	ReturnValues                        string                     `json:"ReturnValues,omitempty"`
	ReturnConsumedCapacity              string                     `json:"ReturnConsumedCapacity,omitempty"`
	ReturnItemCollectionMetrics         string                     `json:"ReturnItemCollectionMetrics,omitempty"`
	ConditionExpression                 string                     `json:"ConditionExpression,omitempty"`
	ExpressionAttributeNames            map[string]string          `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues           map[string]*AttributeValue `json:"ExpressionAttributeValues,omitempty"`
	Expected                            map[string]*ExpectedCondition `json:"Expected,omitempty"`
	ConditionalOperator                 string                     `json:"ConditionalOperator,omitempty"`
	ReturnValuesOnConditionCheckFailure string                     `json:"ReturnValuesOnConditionCheckFailure,omitempty"`
}

// Capacity is a per-entity capacity breakdown.
type Capacity struct {
	CapacityUnits float64 `json:"CapacityUnits"`
}

// ConsumedCapacity is the consumed write capacity report.
type ConsumedCapacity struct {
	TableName     string    `json:"TableName,omitempty"`
	CapacityUnits float64   `json:"CapacityUnits"`
	Table         *Capacity `json:"Table,omitempty"`
}

// DeleteItemOutput is the DeleteItem response body.
type DeleteItemOutput struct {
	Attributes       Item              `json:"Attributes,omitempty"`
	ConsumedCapacity *ConsumedCapacity `json:"ConsumedCapacity,omitempty"`
}

// HandleDeleteItem decodes the request body, runs the operation and encodes
// the response; it returns the HTTP status code and the JSON body to send.
func HandleDeleteItem(s *Store, body []byte) (int, []byte) {
	var in DeleteItemInput
	if err := json.Unmarshal(body, &in); err != nil {
		return http.StatusBadRequest, validationErrf("Malformed request body: %v", err).wireBody()
	}
	out, apiErr := DeleteItem(s, &in)
	if apiErr != nil {
		return apiErr.HTTP, apiErr.wireBody()
	}
	resp, err := json.Marshal(out)
	if err != nil {
		return http.StatusInternalServerError, (&APIError{HTTP: http.StatusInternalServerError, Code: "InternalFailure", Message: "internal error"}).wireBody()
	}
	return http.StatusOK, resp
}

// handleDeleteItem is the lowercase alias used by the handlers.go route table.
func handleDeleteItem(s *Store, body []byte) (int, []byte) { return HandleDeleteItem(s, body) }

// DeleteItem removes the item identified by in.Key. When
// in.ReturnValues == "ALL_OLD" the previously stored item is returned.
func DeleteItem(s *Store, in *DeleteItemInput) (*DeleteItemOutput, *APIError) {
	if in.TableName == "" {
		return nil, validationErrf("Table name must not be empty")
	}
	if apiErr := validateReturnValues(in.ReturnValues); apiErr != nil {
		return nil, apiErr
	}
	if apiErr := validateReturnValuesOnConditionCheckFailure(in.ReturnValuesOnConditionCheckFailure); apiErr != nil {
		return nil, apiErr
	}
	t, ok := s.lookupTable(in.TableName)
	if !ok {
		return nil, resourceNotFoundErr()
	}
	parts, apiErr := keyParts(t, in.Key)
	if apiErr != nil {
		return nil, apiErr
	}
	storageKey := strings.Join(parts, "\x00")

	t.mu.Lock()
	old, existed := t.Items[storageKey]
	cond, apiErr := evalDeleteConditions(in, old)
	if apiErr != nil {
		t.mu.Unlock()
		return nil, apiErr
	}
	if !cond {
		apiErr = conditionalCheckFailedErr()
		if in.ReturnValuesOnConditionCheckFailure == "ALL_OLD" && existed && old != nil {
			apiErr.item = mustJSON(cloneItem(old))
		}
		t.mu.Unlock()
		return nil, apiErr
	}
	if existed {
		delete(t.Items, storageKey)
	}
	var oldCopy Item
	if existed && old != nil {
		oldCopy = cloneItem(old)
	}
	t.mu.Unlock()

	out := &DeleteItemOutput{}
	if oldCopy != nil && in.ReturnValues == "ALL_OLD" {
		out.Attributes = oldCopy
	}
	if in.ReturnConsumedCapacity == "TOTAL" || in.ReturnConsumedCapacity == "INDEXES" {
		cc := &ConsumedCapacity{TableName: t.Name, CapacityUnits: writeCapacityUnits(oldCopy)}
		if in.ReturnConsumedCapacity == "INDEXES" {
			cc.Table = &Capacity{CapacityUnits: cc.CapacityUnits}
		}
		out.ConsumedCapacity = cc
	}
	return out, nil
}

func validateReturnValues(rv string) *APIError {
	switch rv {
	case "", "NONE", "ALL_OLD":
		return nil
	default:
		return validationErrf("Unsupported ReturnValues: %s", rv)
	}
}

func validateReturnValuesOnConditionCheckFailure(rv string) *APIError {
	switch rv {
	case "", "NONE", "ALL_OLD":
		return nil
	default:
		return validationErrf("Unsupported ReturnValuesOnConditionCheckFailure: %s", rv)
	}
}

// ---------------------------------------------------------------------------
// Keys and attributes
// ---------------------------------------------------------------------------

// keyParts validates the request key against the table key schema and returns
// its canonical segments in schema order (HASH first, then RANGE).
func keyParts(t *Table, key Item) ([]string, *APIError) {
	schema := keySchemaInOrder(t)
	if len(key) != len(schema) {
		return nil, keyMismatchErr()
	}
	parts := make([]string, 0, len(schema))
	for _, ks := range schema {
		want := t.KeyTypes[ks.AttributeName]
		if want == "" {
			want = "S"
		}
		av, ok := key[ks.AttributeName]
		if !ok || av == nil || attrKind(av) != want {
			return nil, keyMismatchErr()
		}
		part, apiErr := canonicalKeySegment(av, want)
		if apiErr != nil {
			return nil, apiErr
		}
		parts = append(parts, part)
	}
	return parts, nil
}

func keySchemaInOrder(t *Table) []KeySchemaElement {
	out := append([]KeySchemaElement(nil), t.KeySchema...)
	sort.SliceStable(out, func(i, j int) bool {
		return keyTypeRank(out[i].KeyType) < keyTypeRank(out[j].KeyType)
	})
	return out
}

func keyTypeRank(kt string) int {
	if strings.EqualFold(kt, "RANGE") {
		return 1
	}
	return 0
}

func keyMismatchErr() *APIError {
	return validationErrf("The provided key element does not match the schema")
}

func canonicalKeySegment(av *AttributeValue, typ string) (string, *APIError) {
	switch typ {
	case "S":
		if av.S == nil || *av.S == "" {
			return "", keyMismatchErr()
		}
		return *av.S, nil
	case "N":
		if av.N == nil {
			return "", keyMismatchErr()
		}
		c, ok := canonicalNumber(*av.N)
		if !ok {
			return "", keyMismatchErr()
		}
		return c, nil
	case "B":
		if len(av.B) == 0 {
			return "", keyMismatchErr()
		}
		return string(av.B), nil
	}
	return "", keyMismatchErr()
}

// attrKind returns the DynamoDB type letter(s) of v, "" for empty/unknown.
func attrKind(v *AttributeValue) string {
	if v == nil {
		return ""
	}
	switch {
	case v.S != nil:
		return "S"
	case v.N != nil:
		return "N"
	case v.B != nil:
		return "B"
	case v.BOOL != nil:
		return "BOOL"
	case v.NULL != nil:
		return "NULL"
	case v.SS != nil:
		return "SS"
	case v.NS != nil:
		return "NS"
	case v.BS != nil:
		return "BS"
	case v.M != nil:
		return "M"
	case v.L != nil:
		return "L"
	}
	return ""
}

// numOf parses a DynamoDB number exactly.
func numOf(s string) (*big.Float, bool) {
	f, _, err := big.ParseFloat(s, 10, 512, big.ToNearestEven)
	if err != nil {
		return nil, false
	}
	return f, true
}

// canonicalNumber normalizes DynamoDB numbers so that "1", "1.0" and "1e0"
// produce the same string (used for keying and numeric comparison).
func canonicalNumber(s string) (string, bool) {
	f, ok := numOf(s)
	if !ok {
		return "", false
	}
	return f.Text('f', -1), true
}

func identityCanon(s string) (string, bool) { return s, true }

// cloneItem deep-copies an item so returned values cannot alias live storage.
func cloneItem(it Item) Item {
	if it == nil {
		return nil
	}
	out := make(Item, len(it))
	for k, v := range it {
		out[k] = cloneValue(v)
	}
	return out
}

func cloneValue(v *AttributeValue) *AttributeValue {
	if v == nil {
		return nil
	}
	nv := &AttributeValue{}
	if v.S != nil {
		s := *v.S
		nv.S = &s
	}
	if v.N != nil {
		s := *v.N
		nv.N = &s
	}
	if v.B != nil {
		nv.B = append([]byte(nil), v.B...)
	}
	if v.BOOL != nil {
		b := *v.BOOL
		nv.BOOL = &b
	}
	if v.NULL != nil {
		b := *v.NULL
		nv.NULL = &b
	}
	if v.SS != nil {
		nv.SS = append([]string(nil), v.SS...)
	}
	if v.NS != nil {
		nv.NS = append([]string(nil), v.NS...)
	}
	if v.BS != nil {
		nv.BS = make([][]byte, len(v.BS))
		for i, b := range v.BS {
			nv.BS[i] = append([]byte(nil), b...)
		}
	}
	if v.M != nil {
		nv.M = make(map[string]*AttributeValue, len(v.M))
		for k, mv := range v.M {
			nv.M[k] = cloneValue(mv)
		}
	}
	if v.L != nil {
		nv.L = make([]*AttributeValue, len(v.L))
		for i, lv := range v.L {
			nv.L[i] = cloneValue(lv)
		}
	}
	return nv
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage("null")
	}
	return b
}

func itemWireSize(it Item) int {
	b, err := json.Marshal(it)
	if err != nil {
		return 1
	}
	return len(b)
}

// writeCapacityUnits: one WCU per 1 KB of the (deleted) item, minimum 1.
func writeCapacityUnits(it Item) float64 {
	if it == nil {
		return 1
	}
	units := math.Ceil(float64(itemWireSize(it)) / 1024)
	if units < 1 {
		units = 1
	}
	return units
}

// ---------------------------------------------------------------------------
// Conditions (ConditionExpression + legacy Expected)
// ---------------------------------------------------------------------------

// evalDeleteConditions evaluates the request conditions against the
// pre-delete item state (old == nil when the key did not exist).
func evalDeleteConditions(in *DeleteItemInput, old Item) (bool, *APIError) {
	if in.ConditionExpression != "" && len(in.Expected) > 0 {
		return false, validationErrf("The ConditionExpression parameter and the Expected parameter cannot be used at the same time")
	}
	if in.ConditionExpression != "" {
		fn, apiErr := compileCondition(in.ConditionExpression, in.ExpressionAttributeNames, in.ExpressionAttributeValues)
		if apiErr != nil {
			return false, apiErr
		}
		return fn(old), nil
	}
	if len(in.Expected) > 0 {
		return evalExpectedConditions(in.Expected, in.ConditionalOperator, old)
	}
	return true, nil
}

func evalExpectedConditions(expected map[string]*ExpectedCondition, op string, old Item) (bool, *APIError) {
	mode := strings.ToUpper(strings.TrimSpace(op))
	if mode == "" {
		mode = "AND"
	}
	if mode != "AND" && mode != "OR" {
		return false, validationErrf("ConditionalOperator must be one of AND, OR")
	}
	result := mode == "AND"
	for attr, exp := range expected {
		if exp == nil {
			continue
		}
		ok, apiErr := evalOneExpected(old, attr, exp)
		if apiErr != nil {
			return false, apiErr
		}
		if mode == "AND" && !ok {
			return false, nil
		}
		if mode == "OR" && ok {
			return true, nil
		}
	}
	return result, nil
}

func evalOneExpected(item Item, attr string, exp *ExpectedCondition) (bool, *APIError) {
	v, ok := resolveAttrPath(item, []pathSeg{{name: attr}})
	if exp.ComparisonOperator != "" {
		return evalComparisonOperator(exp.ComparisonOperator, v, ok, exp.AttributeValueList), nil
	}
	cond := ok
	if exp.Exists != nil {
		cond = ok == *exp.Exists
	}
	if exp.Value != nil {
		cond = cond && ok && attrEqual(v, exp.Value)
	}
	return cond, nil
}

func evalComparisonOperator(op string, v *AttributeValue, ok bool, list []*AttributeValue) bool {
	at := func(i int) *AttributeValue {
		if i < len(list) {
			return list[i]
		}
		return nil
	}
	switch strings.ToUpper(strings.TrimSpace(op)) {
	case "EQ":
		return ok && len(list) > 0 && attrEqual(v, at(0))
	case "NE":
		return ok && len(list) > 0 && !attrEqual(v, at(0))
	case "LE":
		if !ok {
			return false
		}
		c, cmpOK := cmpOrder(v, at(0))
		return cmpOK && c <= 0
	case "LT":
		if !ok {
			return false
		}
		c, cmpOK := cmpOrder(v, at(0))
		return cmpOK && c < 0
	case "GE":
		if !ok {
			return false
		}
		c, cmpOK := cmpOrder(v, at(0))
		return cmpOK && c >= 0
	case "GT":
		if !ok {
			return false
		}
		c, cmpOK := cmpOrder(v, at(0))
		return cmpOK && c > 0
	case "BETWEEN":
		if !ok {
			return false
		}
		cLo, okLo := cmpOrder(v, at(0))
		if !okLo || cLo < 0 {
			return false
		}
		cHi, okHi := cmpOrder(v, at(1))
		return okHi && cHi >= 0
	case "IN":
		if !ok {
			return false
		}
		for _, o := range list {
			if o != nil && attrEqual(v, o) {
				return true
			}
		}
		return false
	case "CONTAINS":
		return ok && len(list) > 0 && containsValue(v, at(0))
	case "NOT_CONTAINS":
		return ok && !(len(list) > 0 && containsValue(v, at(0)))
	case "BEGINS_WITH":
		if !ok || len(list) == 0 {
			return false
		}
		return attrKind(v) == "S" && attrKind(at(0)) == "S" && strings.HasPrefix(*v.S, *at(0).S)
	case "NULL":
		return !ok
	case "NOT_NULL":
		return ok
	}
	return false
}

// ---------------------------------------------------------------------------
// ConditionExpression parser / evaluator
// ---------------------------------------------------------------------------

type condOperand func(item Item) (*AttributeValue, bool)
type condPredicate func(item Item) bool

type pathSeg struct {
	name    string
	index   int
	isIndex bool
}

type condToken struct {
	kind string // id, num, str, name, ref, op, lparen, rparen, comma, dot, lbrack, rbrack
	text string
}

var condFunctions = map[string]bool{
	"attribute_exists":     true,
	"attribute_not_exists": true,
	"begins_with":          true,
	"contains":             true,
}

type condParser struct {
	toks   []condToken
	pos    int
	names  map[string]string
	values map[string]*AttributeValue
}

// compileCondition parses a condition expression into a predicate over items.
func compileCondition(expr string, names map[string]string, values map[string]*AttributeValue) (condPredicate, *APIError) {
	toks, apiErr := lexCondition(expr)
	if apiErr != nil {
		return nil, apiErr
	}
	if len(toks) == 0 {
		return nil, validationErrf("Invalid ConditionExpression: empty expression")
	}
	p := &condParser{toks: toks, names: names, values: values}
	fn, apiErr := p.parseOr()
	if apiErr != nil {
		return nil, apiErr
	}
	if p.pos != len(p.toks) {
		return nil, validationErrf("Invalid ConditionExpression: unexpected token %q", p.toks[p.pos].text)
	}
	return fn, nil
}

func lexCondition(expr string) ([]condToken, *APIError) {
	var toks []condToken
	i, n := 0, len(expr)
	isIdent := func(c byte) bool {
		return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
	}
	for i < n {
		c := expr[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '(':
			toks = append(toks, condToken{"lparen", "("})
			i++
		case c == ')':
			toks = append(toks, condToken{"rparen", ")"})
			i++
		case c == ',':
			toks = append(toks, condToken{"comma", ","})
			i++
		case c == '.':
			toks = append(toks, condToken{"dot", "."})
			i++
		case c == '[':
			toks = append(toks, condToken{"lbrack", "["})
			i++
		case c == ']':
			toks = append(toks, condToken{"rbrack", "]"})
			i++
		case c == '#':
			j := i + 1
			for j < n && isIdent(expr[j]) {
				j++
			}
			if j == i+1 {
				return nil, validationErrf("Invalid ConditionExpression: empty attribute name placeholder")
			}
			toks = append(toks, condToken{"name", expr[i:j]})
			i = j
		case c == ':':
			j := i + 1
			for j < n && isIdent(expr[j]) {
				j++
			}
			if j == i+1 {
				return nil, validationErrf("Invalid ConditionExpression: empty value placeholder")
			}
			toks = append(toks, condToken{"ref", expr[i:j]})
			i = j
		case c == '\'' || c == '"':
			s, ni, err := lexQuoted(expr, i, c)
			if err != nil {
				return nil, validationErrf("Invalid ConditionExpression: %s", err.Error())
			}
			toks = append(toks, condToken{"str", s})
			i = ni
		case c == '=':
			toks = append(toks, condToken{"op", "="})
			i++
		case c == '<':
			if i+1 < n && expr[i+1] == '>' {
				toks = append(toks, condToken{"op", "<>"})
				i += 2
			} else if i+1 < n && expr[i+1] == '=' {
				toks = append(toks, condToken{"op", "<="})
				i += 2
			} else {
				toks = append(toks, condToken{"op", "<"})
				i++
			}
		case c == '>':
			if i+1 < n && expr[i+1] == '=' {
				toks = append(toks, condToken{"op", ">="})
				i += 2
			} else {
				toks = append(toks, condToken{"op", ">"})
				i++
			}
		case c >= '0' && c <= '9':
			j := i
			for j < n && expr[j] >= '0' && expr[j] <= '9' {
				j++
			}
			toks = append(toks, condToken{"num", expr[i:j]})
			i = j
		case isIdent(c):
			j := i
			for j < n && isIdent(expr[j]) {
				j++
			}
			toks = append(toks, condToken{"id", expr[i:j]})
			i = j
		default:
			return nil, validationErrf("Invalid ConditionExpression: unexpected character %q", string(c))
		}
	}
	return toks, nil
}

func lexQuoted(s string, i int, q byte) (string, int, error) {
	i++ // opening quote
	var sb strings.Builder
	for i < len(s) {
		if s[i] == q {
			if i+1 < len(s) && s[i+1] == q {
				sb.WriteByte(q)
				i += 2
				continue
			}
			i++
			return sb.String(), i, nil
		}
		sb.WriteByte(s[i])
		i++
	}
	return "", 0, fmt.Errorf("unterminated string literal")
}

func (p *condParser) peek() *condToken {
	if p.pos < len(p.toks) {
		return &p.toks[p.pos]
	}
	return nil
}

func (p *condParser) peekAt(off int) *condToken {
	if p.pos+off < len(p.toks) {
		return &p.toks[p.pos+off]
	}
	return nil
}

func (p *condParser) next() *condToken {
	t := p.peek()
	if t != nil {
		p.pos++
	}
	return t
}

func (p *condParser) isKeyword(t *condToken, kw string) bool {
	return t != nil && t.kind == "id" && strings.EqualFold(t.text, kw)
}

func (p *condParser) parseOr() (condPredicate, *APIError) {
	left, apiErr := p.parseAnd()
	if apiErr != nil {
		return nil, apiErr
	}
	for p.isKeyword(p.peek(), "OR") {
		p.pos++
		right, apiErr := p.parseAnd()
		if apiErr != nil {
			return nil, apiErr
		}
		left = func(l, r condPredicate) condPredicate {
			return func(it Item) bool { return l(it) || r(it) }
		}(left, right)
	}
	return left, nil
}

func (p *condParser) parseAnd() (condPredicate, *APIError) {
	left, apiErr := p.parseNot()
	if apiErr != nil {
		return nil, apiErr
	}
	for p.isKeyword(p.peek(), "AND") {
		p.pos++
		right, apiErr := p.parseNot()
		if apiErr != nil {
			return nil, apiErr
		}
		left = func(l, r condPredicate) condPredicate {
			return func(it Item) bool { return l(it) && r(it) }
		}(left, right)
	}
	return left, nil
}

func (p *condParser) parseNot() (condPredicate, *APIError) {
	if p.isKeyword(p.peek(), "NOT") {
		p.pos++
		inner, apiErr := p.parseNot()
		if apiErr != nil {
			return nil, apiErr
		}
		return func(f condPredicate) condPredicate {
			return func(it Item) bool { return !f(it) }
		}(inner), nil
	}
	return p.parsePrimary()
}

func (p *condParser) parsePrimary() (condPredicate, *APIError) {
	t := p.peek()
	if t == nil {
		return nil, validationErrf("Invalid ConditionExpression: unexpected end of expression")
	}
	if t.kind == "lparen" {
		p.pos++
		inner, apiErr := p.parseOr()
		if apiErr != nil {
			return nil, apiErr
		}
		if p.peek() == nil || p.peek().kind != "rparen" {
			return nil, validationErrf("Invalid ConditionExpression: expected )")
		}
		p.pos++
		return inner, nil
	}
	if t.kind == "id" {
		if _, isFn := condFunctions[t.text]; isFn {
			if nt := p.peekAt(1); nt != nil && nt.kind == "lparen" {
				return p.parseCall()
			}
		}
	}

	left, apiErr := p.parseOperand()
	if apiErr != nil {
		return nil, apiErr
	}
	opTok := p.peek()
	if opTok != nil && opTok.kind == "op" {
		p.pos++
		right, apiErr := p.parseOperand()
		if apiErr != nil {
			return nil, apiErr
		}
		op := opTok.text
		return func(it Item) bool {
			lv, lok := left(it)
			rv, rok := right(it)
			if !lok || !rok {
				return false
			}
			return compareValues(op, lv, rv)
		}, nil
	}
	if p.isKeyword(opTok, "BETWEEN") {
		p.pos++
		lowOp, apiErr := p.parseOperand()
		if apiErr != nil {
			return nil, apiErr
		}
		if !p.isKeyword(p.peek(), "AND") {
			return nil, validationErrf("Invalid ConditionExpression: expected AND in BETWEEN")
		}
		p.pos++
		highOp, apiErr := p.parseOperand()
		if apiErr != nil {
			return nil, apiErr
		}
		return func(it Item) bool {
			lv, lok := left(it)
			if !lok {
				return false
			}
			lov, _ := lowOp(it)
			hiv, _ := highOp(it)
			cLo, okLo := cmpOrder(lv, lov)
			if !okLo || cLo < 0 {
				return false
			}
			cHi, okHi := cmpOrder(lv, hiv)
			return okHi && cHi >= 0
		}, nil
	}
	if p.isKeyword(opTok, "IN") {
		p.pos++
		if p.peek() == nil || p.peek().kind != "lparen" {
			return nil, validationErrf("Invalid ConditionExpression: expected ( after IN")
		}
		p.pos++
		var rhs []condOperand
		for {
			o, apiErr := p.parseOperand()
			if apiErr != nil {
				return nil, apiErr
			}
			rhs = append(rhs, o)
			if pt := p.peek(); pt != nil && pt.kind == "comma" {
				p.pos++
				continue
			}
			break
		}
		if p.peek() == nil || p.peek().kind != "rparen" {
			return nil, validationErrf("Invalid ConditionExpression: expected ) after IN list")
		}
		p.pos++
		return func(it Item) bool {
			lv, lok := left(it)
			if !lok {
				return false
			}
			for _, o := range rhs {
				ov, ok := o(it)
				if ok && attrEqual(lv, ov) {
					return true
				}
			}
			return false
		}, nil
	}
	return nil, validationErrf("Invalid ConditionExpression: expected a comparison, a function, BETWEEN or IN")
}

func (p *condParser) parseCall() (condPredicate, *APIError) {
	nameTok := p.next()
	p.next() // consume (
	var args []condOperand
	for {
		arg, apiErr := p.parseOperand()
		if apiErr != nil {
			return nil, apiErr
		}
		args = append(args, arg)
		if pt := p.peek(); pt != nil && pt.kind == "comma" {
			p.pos++
			continue
		}
		break
	}
	if p.peek() == nil || p.peek().kind != "rparen" {
		return nil, validationErrf("Invalid ConditionExpression: expected ) after function arguments")
	}
	p.pos++

	switch nameTok.text {
	case "attribute_exists":
		if len(args) != 1 {
			return nil, validationErrf("Invalid ConditionExpression: attribute_exists requires one argument")
		}
		arg := args[0]
		return func(it Item) bool {
			_, ok := arg(it)
			return ok
		}, nil
	case "attribute_not_exists":
		if len(args) != 1 {
			return nil, validationErrf("Invalid ConditionExpression: attribute_not_exists requires one argument")
		}
		arg := args[0]
		return func(it Item) bool {
			_, ok := arg(it)
			return !ok
		}, nil
	case "begins_with":
		if len(args) != 2 {
			return nil, validationErrf("Invalid ConditionExpression: begins_with requires two arguments")
		}
		valOp, prefixOp := args[0], args[1]
		return func(it Item) bool {
			v, ok := valOp(it)
			if !ok || attrKind(v) != "S" {
				return false
			}
			pre, ok2 := prefixOp(it)
			if !ok2 || attrKind(pre) != "S" {
				return false
			}
			return strings.HasPrefix(*v.S, *pre.S)
		}, nil
	case "contains":
		if len(args) != 2 {
			return nil, validationErrf("Invalid ConditionExpression: contains requires two arguments")
		}
		valOp, subOp := args[0], args[1]
		return func(it Item) bool {
			v, ok := valOp(it)
			if !ok {
				return false
			}
			sub, _ := subOp(it)
			return containsValue(v, sub)
		}, nil
	}
	return nil, validationErrf("Invalid ConditionExpression: unknown function %s", nameTok.text)
}

func (p *condParser) parseOperand() (condOperand, *APIError) {
	t := p.next()
	if t == nil {
		return nil, validationErrf("Invalid ConditionExpression: unexpected end of expression")
	}
	switch t.kind {
	case "str":
		sv := t.text
		return func(Item) (*AttributeValue, bool) { return &AttributeValue{S: &sv}, true }, nil
	case "ref":
		v, ok := p.values[t.text]
		if !ok || v == nil {
			return nil, validationErrf("Invalid ConditionExpression: An expression attribute value used in expression is not defined: %s", t.text)
		}
		return func(Item) (*AttributeValue, bool) { return v, true }, nil
	case "id":
		if t.text == "size" && p.peek() != nil && p.peek().kind == "lparen" {
			p.pos++
			nt := p.next()
			if nt == nil || (nt.kind != "id" && nt.kind != "name") {
				return nil, validationErrf("Invalid ConditionExpression: expected attribute path in size()")
			}
			segs, apiErr := p.parsePathSegments(nt)
			if apiErr != nil {
				return nil, apiErr
			}
			if p.peek() == nil || p.peek().kind != "rparen" {
				return nil, validationErrf("Invalid ConditionExpression: expected ) after size() path")
			}
			p.pos++
			return func(it Item) (*AttributeValue, bool) {
				v, ok := resolveAttrPath(it, segs)
				if !ok {
					return nil, false
				}
				s, ok := sizeOfAttr(v)
				if !ok {
					return nil, false
				}
				ns := strconv.Itoa(s)
				return &AttributeValue{N: &ns}, true
			}, nil
		}
		segs, apiErr := p.parsePathSegments(t)
		if apiErr != nil {
			return nil, apiErr
		}
		return func(it Item) (*AttributeValue, bool) { return resolveAttrPath(it, segs) }, nil
	case "name":
		segs, apiErr := p.parsePathSegments(t)
		if apiErr != nil {
			return nil, apiErr
		}
		return func(it Item) (*AttributeValue, bool) { return resolveAttrPath(it, segs) }, nil
	}
	return nil, validationErrf("Invalid ConditionExpression: unexpected token %q", t.text)
}

// parsePathSegments parses an attribute path whose head token is head.
func (p *condParser) parsePathSegments(head *condToken) ([]pathSeg, *APIError) {
	var segs []pathSeg
	switch head.kind {
	case "name":
		attr, ok := p.names[head.text]
		if !ok {
			return nil, validationErrf("Invalid ConditionExpression: An expression attribute name used in expression is not defined: %s", head.text)
		}
		segs = append(segs, pathSeg{name: attr})
	case "id":
		segs = append(segs, pathSeg{name: head.text})
	default:
		return nil, validationErrf("Invalid ConditionExpression: expected attribute path")
	}
	for {
		t := p.peek()
		if t == nil {
			return segs, nil
		}
		switch t.kind {
		case "dot":
			p.pos++
			nt := p.next()
			if nt == nil || (nt.kind != "id" && nt.kind != "name") {
				return nil, validationErrf("Invalid ConditionExpression: expected attribute name after '.'")
			}
			name := nt.text
			if nt.kind == "name" {
				resolved, ok := p.names[name]
				if !ok {
					return nil, validationErrf("Invalid ConditionExpression: An expression attribute name used in expression is not defined: %s", name)
				}
				name = resolved
			}
			segs = append(segs, pathSeg{name: name})
		case "lbrack":
			p.pos++
			nt := p.next()
			if nt == nil || nt.kind != "num" {
				return nil, validationErrf("Invalid ConditionExpression: expected list index after '['")
			}
			idx, err := strconv.Atoi(nt.text)
			if err != nil {
				return nil, validationErrf("Invalid ConditionExpression: bad list index %s", nt.text)
			}
			if p.peek() == nil || p.peek().kind != "rbrack" {
				return nil, validationErrf("Invalid ConditionExpression: expected ']'")
			}
			p.pos++
			segs = append(segs, pathSeg{index: idx, isIndex: true})
		default:
			return segs, nil
		}
	}
}

// resolveAttrPath walks an item along the parsed path.
func resolveAttrPath(item Item, segs []pathSeg) (*AttributeValue, bool) {
	var cur *AttributeValue
	for i, seg := range segs {
		if i == 0 {
			if seg.isIndex {
				return nil, false
			}
			v, ok := item[seg.name]
			if !ok {
				return nil, false
			}
			cur = v
		} else {
			if cur == nil {
				return nil, false
			}
			if seg.isIndex {
				if cur.L == nil || seg.index < 0 || seg.index >= len(cur.L) {
					return nil, false
				}
				cur = cur.L[seg.index]
			} else {
				if cur.M == nil {
					return nil, false
				}
				v, ok := cur.M[seg.name]
				if !ok {
					return nil, false
				}
				cur = v
			}
		}
		if cur == nil {
			return nil, false
		}
	}
	return cur, true
}

func sizeOfAttr(v *AttributeValue) (int, bool) {
	switch attrKind(v) {
	case "S":
		return len(*v.S), true
	case "B":
		return len(v.B), true
	case "SS":
		return len(v.SS), true
	case "NS":
		return len(v.NS), true
	case "BS":
		return len(v.BS), true
	case "M":
		return len(v.M), true
	case "L":
		return len(v.L), true
	}
	return 0, false
}

func compareValues(op string, a, b *AttributeValue) bool {
	switch op {
	case "=":
		return attrEqual(a, b)
	case "<>":
		return !attrEqual(a, b)
	}
	c, ok := cmpOrder(a, b)
	if !ok {
		return false
	}
	switch op {
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	case ">":
		return c > 0
	case ">=":
		return c >= 0
	}
	return false
}

// cmpOrder implements ordering comparison for S and N values.
func cmpOrder(a, b *AttributeValue) (int, bool) {
	ka, kb := attrKind(a), attrKind(b)
	if ka == "S" && kb == "S" {
		return strings.Compare(*a.S, *b.S), true
	}
	if ka == "N" && kb == "N" {
		fa, oka := numOf(*a.N)
		fb, okb := numOf(*b.N)
		if !oka || !okb {
			return 0, false
		}
		return fa.Cmp(fb), true
	}
	return 0, false
}

func attrEqual(a, b *AttributeValue) bool {
	ka, kb := attrKind(a), attrKind(b)
	if ka == "" || kb == "" || ka != kb {
		return false
	}
	switch ka {
	case "S":
		return *a.S == *b.S
	case "N":
		fa, oka := numOf(*a.N)
		fb, okb := numOf(*b.N)
		return oka && okb && fa.Cmp(fb) == 0
	case "B":
		return bytes.Equal(a.B, b.B)
	case "BOOL":
		return *a.BOOL == *b.BOOL
	case "NULL":
		return *a.NULL == *b.NULL
	case "SS":
		return sameStringSet(a.SS, b.SS, identityCanon)
	case "NS":
		return sameStringSet(a.NS, b.NS, canonicalNumber)
	case "BS":
		return sameBytesSet(a.BS, b.BS)
	case "M":
		if len(a.M) != len(b.M) {
			return false
		}
		for k, av := range a.M {
			bv, ok := b.M[k]
			if !ok || !attrEqual(av, bv) {
				return false
			}
		}
		return true
	case "L":
		if len(a.L) != len(b.L) {
			return false
		}
		for i := range a.L {
			if !attrEqual(a.L[i], b.L[i]) {
				return false
			}
		}
		return true
	}
	return false
}

func containsValue(hay, needle *AttributeValue) bool {
	if hay == nil {
		return false
	}
	switch attrKind(hay) {
	case "S":
		return attrKind(needle) == "S" && strings.Contains(*hay.S, *needle.S)
	case "B":
		return attrKind(needle) == "B" && bytes.Contains(hay.B, needle.B)
	case "SS":
		return attrKind(needle) == "S" && containsString(hay.SS, *needle.S, identityCanon)
	case "NS":
		return attrKind(needle) == "N" && containsString(hay.NS, *needle.N, canonicalNumber)
	case "BS":
		if attrKind(needle) != "B" {
			return false
		}
		for _, b := range hay.BS {
			if bytes.Equal(b, needle.B) {
				return true
			}
		}
		return false
	case "L":
		for _, e := range hay.L {
			if attrEqual(e, needle) {
				return true
			}
		}
		return false
	}
	return false
}

func containsString(list []string, val string, canon func(string) (string, bool)) bool {
	cv, ok := canon(val)
	if !ok {
		return false
	}
	for _, s := range list {
		if cs, ok2 := canon(s); ok2 && cs == cv {
			return true
		}
	}
	return false
}

func canonicalSet(list []string, canon func(string) (string, bool)) ([]string, bool) {
	out := make([]string, 0, len(list))
	for _, s := range list {
		c, ok := canon(s)
		if !ok {
			return nil, false
		}
		out = append(out, c)
	}
	sort.Strings(out)
	return out, true
}

func sameStringSet(a, b []string, canon func(string) (string, bool)) bool {
	if len(a) != len(b) {
		return false
	}
	ca, ok1 := canonicalSet(a, canon)
	cb, ok2 := canonicalSet(b, canon)
	if !ok1 || !ok2 {
		return false
	}
	for i := range ca {
		if ca[i] != cb[i] {
			return false
		}
	}
	return true
}

func sameBytesSet(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	sa := make([]string, 0, len(a))
	for _, x := range a {
		sa = append(sa, string(x))
	}
	sb := make([]string, 0, len(b))
	for _, x := range b {
		sb = append(sb, string(x))
	}
	sort.Strings(sa)
	sort.Strings(sb)
	for i := range sa {
		if sa[i] != sb[i] {
			return false
		}
	}
	return true
}
