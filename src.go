// src.go - floci-go ticket-019: TransactGetItems.
//
// Self-contained local DynamoDB subset speaking the standard wire protocol
// (POST + X-Amz-Target: DynamoDB_20120810.<Op>). Implements the table and
// item operations needed to drive TransactGetItems end to end, plus the
// ticket itself:
//
//   - CreateTable / DescribeTable / DeleteTable / ListTables
//   - PutItem / GetItem / DeleteItem / BatchGetItem
//   - TransactGetItems
//
// TransactGetItems behavior (ticket-019):
//   - 1..100 Get actions; missing, empty or oversized input is rejected with
//     ValidationException
//   - every Get targets TableName + Key; keys are validated against the table
//     key schema (missing attribute, extra attributes, HASH/RANGE type
//     mismatch) and canonicalized so numeric keys compare numerically
//   - duplicate table+key pairs are rejected with 'Provided list of item keys
//     contains duplicates.'
//   - unknown table -> ResourceNotFoundException 'Requested resource not found'
//   - ProjectionExpression with ExpressionAttributeNames supports nested
//     document paths (a.b[0].c) and #aliases; unresolved placeholders, unused
//     names and empty expressions are rejected like real DynamoDB
//   - reads are strongly consistent; Responses keep request order and keys
//     that do not exist are silently omitted
//   - ReturnConsumedCapacity NONE/TOTAL/INDEXES: one ConsumedCapacity entry
//     per requested Get (transactional reads cost 2 units per 4 KB block)
//
// Run: go run src.go  (listens on :8000, override with PORT).

package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Errors

const ddbErrPrefix = "com.amazonaws.dynamodb.v20120810#"

type apiError struct {
	typ    string
	msg    string
	status int
}

func (e *apiError) Error() string { return e.typ + ": " + e.msg }

func validationErr(format string, args ...any) *apiError {
	return &apiError{typ: ddbErrPrefix + "ValidationException", status: 400, msg: fmt.Sprintf(format, args...)}
}

func resourceNotFoundErr() *apiError {
	return &apiError{typ: ddbErrPrefix + "ResourceNotFoundException", status: 400, msg: "Requested resource not found"}
}

func serializationErr() *apiError {
	return &apiError{typ: "com.amazon.coral.service#SerializationException", status: 400}
}

func unknownOpErr() *apiError {
	return &apiError{typ: "com.amazon.coral.service#UnknownOperationException", status: 400}
}

// ---------------------------------------------------------------------------
// Helpers

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func strField(m map[string]any, key string) (string, bool) {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s, true
		}
	}
	return "", false
}

func numFrom(m map[string]any, key string) float64 {
	if v, ok := m[key].(float64); ok {
		return v
	}
	return 0
}

func deepCopy(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = deepCopy(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = deepCopy(val)
		}
		return out
	default:
		return v
	}
}

func approxSize(v any) int {
	buf, err := json.Marshal(v)
	if err != nil {
		return 0
	}
	return len(buf)
}

// blockCount maps a byte size to 4 KB capacity blocks (minimum 1).
func blockCount(size int) int {
	b := (size + 4095) / 4096
	if b < 1 {
		b = 1
	}
	return b
}

// validNumber validates a DynamoDB N token (decimal with optional exponent).
func validNumber(s string) bool {
	i, n := 0, len(s)
	if i < n && (s[i] == '+' || s[i] == '-') {
		i++
	}
	digits := 0
	for i < n && s[i] >= '0' && s[i] <= '9' {
		i++
		digits++
	}
	if i < n && s[i] == '.' {
		i++
		for i < n && s[i] >= '0' && s[i] <= '9' {
			i++
			digits++
		}
	}
	if digits == 0 {
		return false
	}
	if i < n && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < n && (s[i] == '+' || s[i] == '-') {
			i++
		}
		expDigits := 0
		for i < n && s[i] >= '0' && s[i] <= '9' {
			i++
			expDigits++
		}
		if expDigits == 0 {
			return false
		}
	}
	return i == n
}

// ---------------------------------------------------------------------------
// Attribute values

func attrTag(v any) string {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return ""
	}
	for k := range m {
		return k
	}
	return ""
}

func checkAttrValue(v any) *apiError {
	m, ok := v.(map[string]any)
	if !ok || len(m) == 0 {
		return validationErr("Supplied AttributeValue is empty, must contain exactly one of the supported datatypes")
	}
	if len(m) > 1 {
		return validationErr("Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes")
	}
	for tag, raw := range m {
		switch tag {
		case "S", "B":
			if _, ok := raw.(string); !ok {
				return validationErr("One or more parameter values were invalid: A value provided cannot be converted into an attribute value of type %s", tag)
			}
		case "N":
			s, ok := raw.(string)
			if !ok || !validNumber(s) {
				return validationErr("One or more parameter values were invalid: A value provided cannot be converted into a number")
			}
		case "SS", "NS", "BS":
			arr, ok := raw.([]any)
			if !ok || len(arr) == 0 {
				switch tag {
				case "SS":
					return validationErr("One or more parameter values were invalid: An string set may not be empty")
				case "NS":
					return validationErr("One or more parameter values were invalid: A number set may not be empty")
				default:
					return validationErr("One or more parameter values were invalid: A binary set may not be empty")
				}
			}
			for _, el := range arr {
				if _, ok := el.(string); !ok {
					return validationErr("One or more parameter values were invalid: A value provided cannot be converted into an attribute value of type %s", tag)
				}
			}
		case "BOOL", "NULL":
			if _, ok := raw.(bool); !ok {
				return validationErr("One or more parameter values were invalid: A value provided cannot be converted into an attribute value of type %s", tag)
			}
		case "M":
			nested, ok := raw.(map[string]any)
			if !ok {
				return validationErr("One or more parameter values were invalid: A value provided cannot be converted into an attribute value of type M")
			}
			for _, el := range nested {
				if aerr := checkAttrValue(el); aerr != nil {
					return aerr
				}
			}
		case "L":
			arr, ok := raw.([]any)
			if !ok {
				return validationErr("One or more parameter values were invalid: A value provided cannot be converted into an attribute value of type L")
			}
			for _, el := range arr {
				if aerr := checkAttrValue(el); aerr != nil {
					return aerr
				}
			}
		default:
			return validationErr("Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes")
		}
	}
	return nil
}

// canonicalAttr renders a scalar attribute value in canonical form so that
// key values can be compared structurally and numerically.
func canonicalAttr(v any) string {
	tag := attrTag(v)
	m := v.(map[string]any)
	switch tag {
	case "S":
		return "S:" + m["S"].(string)
	case "N":
		s, _ := m["N"].(string)
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return "N:" + s
		}
		return "N:" + strconv.FormatFloat(f, 'g', -1, 64)
	case "B":
		s, _ := m["B"].(string)
		if b, err := base64.StdEncoding.DecodeString(s); err == nil {
			return "B:" + base64.StdEncoding.EncodeToString(b)
		}
		return "B:" + s
	}
	return ""
}

// ---------------------------------------------------------------------------
// Store

type keyAttr struct {
	name string
	typ  string // S | N | B
}

type table struct {
	name     string
	key      []keyAttr
	keySch   []map[string]any
	attrDefs []map[string]any
	gsi      []map[string]any
	lsi      []map[string]any
	through  map[string]any
	billing  string
	created  float64
	items    map[string]map[string]any
}

type store struct {
	mu     sync.Mutex
	tables map[string]*table
}

func newStore() *store {
	return &store{tables: map[string]*table{}}
}

func checkTableName(name string) *apiError {
	if len(name) < 3 || len(name) > 255 {
		return validationErr("TableName must be at least 3 characters long and at most 255 characters long")
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.') {
			return validationErr("TableName must match pattern: [a-zA-Z0-9_.-]+")
		}
	}
	return nil
}

func tableNameFrom(body map[string]any) (string, *apiError) {
	v, present := body["TableName"]
	if !present || v == nil {
		return "", validationErr("1 validation error detected: Value 'null' at 'tableName' failed to satisfy constraint: Member must not be null")
	}
	name, ok := v.(string)
	if !ok {
		return "", validationErr("1 validation error detected: Value '%v' at 'tableName' failed to satisfy constraint: Member must not be null", v)
	}
	if aerr := checkTableName(name); aerr != nil {
		return "", aerr
	}
	return name, nil
}

func (s *store) getTable(name string) (*table, *apiError) {
	if aerr := checkTableName(name); aerr != nil {
		return nil, aerr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tables[name]
	if !ok {
		return nil, resourceNotFoundErr()
	}
	return t, nil
}

// keyCanon validates a Key map against the table key schema and returns the
// canonical key string. Numeric tokens compare numerically (1 == 1.0),
// binary values compare by decoded bytes.
func (t *table) keyCanon(raw map[string]any) (string, *apiError) {
	if len(raw) > len(t.key) {
		return "", validationErr("The provided key element does not match the schema")
	}
	var parts []string
	for _, ka := range t.key {
		rv, ok := raw[ka.name]
		if !ok {
			return "", validationErr("One or more parameter values were invalid: Missing the key %s in the item", ka.name)
		}
		if aerr := checkAttrValue(rv); aerr != nil {
			return "", aerr
		}
		tag := attrTag(rv)
		if tag != ka.typ {
			return "", validationErr("One or more parameter values were invalid: Type mismatch for key %s expected: %s actual: %s", ka.name, ka.typ, tag)
		}
		parts = append(parts, canonicalAttr(rv))
	}
	return strings.Join(parts, "\x00"), nil
}

// itemKeyCanon validates the key attributes embedded in a full item.
func (t *table) itemKeyCanon(item map[string]any) (string, *apiError) {
	var parts []string
	for _, ka := range t.key {
		rv, ok := item[ka.name]
		if !ok {
			return "", validationErr("One or more parameter values were invalid: Missing the key %s in the item", ka.name)
		}
		if aerr := checkAttrValue(rv); aerr != nil {
			return "", aerr
		}
		tag := attrTag(rv)
		if tag != ka.typ {
			return "", validationErr("One or more parameter values were invalid: Type mismatch for key %s expected: %s actual: %s", ka.name, ka.typ, tag)
		}
		parts = append(parts, canonicalAttr(rv))
	}
	return strings.Join(parts, "\x00"), nil
}

func (t *table) sizeBytes() int {
	total := 0
	for _, item := range t.items {
		total += approxSize(item)
	}
	return total
}

func (t *table) description() map[string]any {
	attrDefs := t.attrDefs
	if attrDefs == nil {
		attrDefs = []map[string]any{}
	}
	keySch := t.keySch
	if keySch == nil {
		keySch = []map[string]any{}
	}
	d := map[string]any{
		"TableName":             t.name,
		"TableStatus":           "ACTIVE",
		"CreationDateTime":      t.created,
		"ItemCount":             float64(len(t.items)),
		"TableSizeBytes":        float64(t.sizeBytes()),
		"TableArn":              fmt.Sprintf("arn:aws:dynamodb:ddblocal:000000000000:table/%s", t.name),
		"AttributeDefinitions":  attrDefs,
		"KeySchema":             keySch,
		"ProvisionedThroughput": t.through,
	}
	if t.billing == "PAY_PER_REQUEST" {
		d["BillingModeSummary"] = map[string]any{"BillingMode": "PAY_PER_REQUEST"}
	}
	if len(t.gsi) > 0 {
		d["GlobalSecondaryIndexes"] = t.gsi
	}
	if len(t.lsi) > 0 {
		d["LocalSecondaryIndexes"] = t.lsi
	}
	return d
}

func capacityEntry(tableName string, units float64, rcp string) map[string]any {
	e := map[string]any{"CapacityUnits": units, "TableName": tableName}
	if rcp == "INDEXES" {
		e["Table"] = map[string]any{"CapacityUnits": units}
	}
	return e
}

func parseRCP(body map[string]any) (string, *apiError) {
	v, present := body["ReturnConsumedCapacity"]
	if !present || v == nil {
		return "NONE", nil
	}
	sv, ok := v.(string)
	if !ok {
		sv = fmt.Sprintf("%v", v)
	}
	switch sv {
	case "NONE", "TOTAL", "INDEXES":
		return sv, nil
	}
	return "", validationErr("1 validation error detected: Value '%s' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]", sv)
}

func parseReturnValues(body map[string]any) (string, *apiError) {
	v, present := body["ReturnValues"]
	if !present || v == nil {
		return "NONE", nil
	}
	sv, ok := v.(string)
	if !ok {
		sv = fmt.Sprintf("%v", v)
	}
	switch sv {
	case "NONE", "ALL_OLD":
		return sv, nil
	}
	return "", validationErr("1 validation error detected: Value '%s' at 'returnValues' failed to satisfy constraint: Member must satisfy enum value set: [NONE, ALL_OLD]", sv)
}

// ---------------------------------------------------------------------------
// Projection expressions

type projResult struct {
	paths [][]any
	used  map[string]bool
}

// parseProjection splits a ProjectionExpression into document paths,
// resolving # aliases via ExpressionAttributeNames.
func parseProjection(expr string, eanAny any) (*projResult, *apiError) {
	ean, _ := eanAny.(map[string]any)
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return nil, validationErr("The expression cannot be empty")
	}
	res := &projResult{used: map[string]bool{}}
	for _, part := range strings.Split(trimmed, ",") {
		segs, aerr := parsePath(strings.TrimSpace(part), ean, res.used)
		if aerr != nil {
			return nil, aerr
		}
		res.paths = append(res.paths, segs)
	}
	return res, nil
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentChar(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9')
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func parsePath(p string, ean map[string]any, used map[string]bool) ([]any, *apiError) {
	i, n := 0, len(p)
	readSeg := func() (string, *apiError) {
		if i < n && p[i] == '#' {
			start := i
			i++
			v0 := i
			for i < n && isIdentChar(p[i]) {
				i++
			}
			alias := p[v0:i]
			if alias == "" {
				return "", validationErr("Syntax error; token: #")
			}
			full := p[start:i]
			raw, ok := ean[full]
			if !ok {
				return "", validationErr("The expression contains unresolved placeholder: %s", full)
			}
			name, ok := raw.(string)
			if !ok {
				return "", validationErr("The expression contains unresolved placeholder: %s", full)
			}
			used[full] = true
			return name, nil
		}
		if i < n && isIdentStart(p[i]) {
			start := i
			for i < n && isIdentChar(p[i]) {
				i++
			}
			return p[start:i], nil
		}
		tok := p
		if i < n {
			tok = p[i : i+1]
		}
		return "", validationErr("Syntax error; token: %s", tok)
	}
	first, aerr := readSeg()
	if aerr != nil {
		return nil, aerr
	}
	segs := []any{first}
	for i < n {
		switch p[i] {
		case '.':
			i++
			seg, aerr := readSeg()
			if aerr != nil {
				return nil, aerr
			}
			segs = append(segs, seg)
		case '[':
			i++
			v0 := i
			for i < n && isDigit(p[i]) {
				i++
			}
			if i >= n || i == v0 || p[i] != ']' {
				return nil, validationErr("Syntax error; token: %s", p[v0:minInt(i+1, n)])
			}
			idx, convErr := strconv.Atoi(p[v0:i])
			if convErr != nil {
				return nil, validationErr("Syntax error; token: %s", p[v0:minInt(i+1, n)])
			}
			i++
			segs = append(segs, idx)
		default:
			return nil, validationErr("Syntax error; token: %s", p[i:minInt(i+1, n)])
		}
	}
	return segs, nil
}

func unusedNamesErr(ean map[string]any, used map[string]bool) *apiError {
	var unused []string
	for k := range ean {
		if !used[k] {
			unused = append(unused, k)
		}
	}
	if len(unused) == 0 {
		return nil
	}
	sort.Strings(unused)
	parts := make([]string, len(unused))
	for i, k := range unused {
		parts[i] = ":" + k
	}
	return validationErr("Value provided in ExpressionAttributeNames unused in expressions: keys: {%s}", strings.Join(parts, ", "))
}

// projectionFromBody resolves the optional ProjectionExpression /
// ExpressionAttributeNames pair of a request object. nil paths => full item.
func projectionFromBody(obj map[string]any) ([][]any, *apiError) {
	expr, hasExpr := strField(obj, "ProjectionExpression")
	if hasExpr {
		res, aerr := parseProjection(expr, obj["ExpressionAttributeNames"])
		if aerr != nil {
			return nil, aerr
		}
		if ean, ok := obj["ExpressionAttributeNames"].(map[string]any); ok && len(ean) > 0 {
			if aerr := unusedNamesErr(ean, res.used); aerr != nil {
				return nil, aerr
			}
		}
		return res.paths, nil
	}
	if ean, ok := obj["ExpressionAttributeNames"].(map[string]any); ok && len(ean) > 0 {
		return nil, unusedNamesErr(ean, map[string]bool{})
	}
	return nil, nil
}

func getAtPath(src map[string]any, path []any) (any, bool) {
	var cur any = src
	for _, seg := range path {
		switch s := seg.(type) {
		case string:
			m, ok := cur.(map[string]any)
			if !ok {
				return nil, false
			}
			cur, ok = m[s]
			if !ok {
				return nil, false
			}
		case int:
			l, ok := cur.([]any)
			if !ok {
				return nil, false
			}
			if s < 0 || s >= len(l) {
				return nil, false
			}
			cur = l[s]
		}
	}
	return cur, true
}

func newContainerFor(next any) any {
	if _, ok := next.(int); ok {
		return []any{}
	}
	return map[string]any{}
}

func setPath(dst map[string]any, path []any, val any) {
	var cur any = dst
	for i := 0; i < len(path)-1; i++ {
		next := path[i+1]
		switch s := path[i].(type) {
		case string:
			m, ok := cur.(map[string]any)
			if !ok {
				return
			}
			child, exists := m[s]
			if !exists || child == nil {
				child = newContainerFor(next)
				m[s] = child
			}
			cur = child
		case int:
			l, ok := cur.([]any)
			if !ok {
				return
			}
			for len(l) <= s {
				l = append(l, nil)
			}
			child := l[s]
			if child == nil {
				child = newContainerFor(next)
				l[s] = child
			}
			cur = child
		}
	}
	last := path[len(path)-1]
	switch s := last.(type) {
	case string:
		if m, ok := cur.(map[string]any); ok {
			m[s] = val
		}
	case int:
		if l, ok := cur.([]any); ok {
			for len(l) <= s {
				l = append(l, nil)
			}
			l[s] = val
		}
	}
}

func projectItem(item map[string]any, paths [][]any) map[string]any {
	dst := map[string]any{}
	for _, path := range paths {
		if val, ok := getAtPath(item, path); ok {
			setPath(dst, path, val)
		}
	}
	return dst
}

// ---------------------------------------------------------------------------
// Operations

type opFunc func(*store, map[string]any) (any, *apiError)

func opCreateTable(s *store, body map[string]any) (any, *apiError) {
	name, aerr := tableNameFrom(body)
	if aerr != nil {
		return nil, aerr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tables[name]; exists {
		return nil, resourceNotFoundErr()
	}
	t := &table{name: name, billing: "PROVISIONED", items: map[string]map[string]any{}}
	t.created = float64(time.Now().UnixNano()/int64(time.Millisecond)) / 1000.0

	rawKeySchema, ok := body["KeySchema"].([]any)
	if !ok || len(rawKeySchema) == 0 {
		return nil, validationErr("One or more parameter values are not valid: Table KeySchema is not specified")
	}
	if len(rawKeySchema) > 2 {
		return nil, validationErr("One or more parameter values are not valid: Table KeySchema must have no more than 2 elements")
	}
	keyNames := []string{}
	keyTypes := []string{}
	for _, elAny := range rawKeySchema {
		el, elOk := elAny.(map[string]any)
		if !elOk {
			return nil, validationErr("One or more parameter values are not valid: Invalid KeySchema")
		}
		an, _ := el["AttributeName"].(string)
		kt, ktOk := el["KeyType"].(string)
		if !ktOk || (kt != "HASH" && kt != "RANGE") {
			shown := kt
			if !ktOk {
				shown = fmt.Sprintf("%v", el["KeyType"])
			}
			return nil, validationErr("One or more parameter values are not valid: Invalid KeyType: %s", shown)
		}
		keyNames = append(keyNames, an)
		keyTypes = append(keyTypes, kt)
		t.keySch = append(t.keySch, map[string]any{"AttributeName": an, "KeyType": kt})
	}
	hashes, ranges := 0, 0
	for _, kt := range keyTypes {
		if kt == "HASH" {
			hashes++
		} else {
			ranges++
		}
	}
	if hashes != 1 || ranges > 1 {
		return nil, validationErr("One or more parameter values are not valid: Invalid KeySchema")
	}
	if keyTypes[0] != "HASH" {
		return nil, validationErr("One or more parameter values are not valid: Invalid KeySchema: The first KeySchemaElement is not a HASH key type")
	}

	rawADs, ok := body["AttributeDefinitions"].([]any)
	if !ok {
		return nil, validationErr("1 validation error detected: Value 'null' at 'attributeDefinitions' failed to satisfy constraint: Member must not be null")
	}
	defs := map[string]string{}
	for _, elAny := range rawADs {
		el, elOk := elAny.(map[string]any)
		if !elOk {
			return nil, validationErr("One or more parameter values are not valid: Invalid AttributeDefinition")
		}
		an, anOk := el["AttributeName"].(string)
		at, atOk := el["AttributeType"].(string)
		if !anOk || !atOk || (at != "S" && at != "N" && at != "B") {
			shown := at
			if !atOk {
				shown = fmt.Sprintf("%v", el["AttributeType"])
			}
			return nil, validationErr("One or more parameter values are not valid: Invalid AttributeType: %s", shown)
		}
		defs[an] = at
		t.attrDefs = append(t.attrDefs, map[string]any{"AttributeName": an, "AttributeType": at})
	}
	for _, an := range keyNames {
		at, defined := defs[an]
		if !defined {
			return nil, validationErr("One or more parameter values were invalid: Some index key attributes have no definition. If you are using a new table with key schema, please ensure that the key attribute pairs in KeySchema have attribute definitions set via AttributeDefinitions.")
		}
		t.key = append(t.key, keyAttr{name: an, typ: at})
	}
	if aerr := parseIndexes(body, "GlobalSecondaryIndexes", defs, &t.gsi); aerr != nil {
		return nil, aerr
	}
	if aerr := parseIndexes(body, "LocalSecondaryIndexes", defs, &t.lsi); aerr != nil {
		return nil, aerr
	}

	if bm, present := strField(body, "BillingMode"); present {
		if bm != "PROVISIONED" && bm != "PAY_PER_REQUEST" {
			return nil, validationErr("1 validation error detected: Value '%s' at 'billingMode' failed to satisfy constraint: Member must satisfy enum value set: [PAY_PER_REQUEST, PROVISIONED]", bm)
		}
		t.billing = bm
	}
	rcu, wcu := float64(0), float64(0)
	if pt, ok := body["ProvisionedThroughput"].(map[string]any); ok {
		rcu = numFrom(pt, "ReadCapacityUnits")
		wcu = numFrom(pt, "WriteCapacityUnits")
	}
	if t.billing == "PROVISIONED" && (rcu <= 0 || wcu <= 0) {
		return nil, validationErr("One or more parameter values were invalid: ReadCapacityUnits and WriteCapacityUnits are required when BillingMode is PROVISIONED")
	}
	t.through = map[string]any{
		"ReadCapacityUnits":      rcu,
		"WriteCapacityUnits":     wcu,
		"NumberOfDecreasesToday": float64(0),
	}

	s.tables[name] = t
	return map[string]any{"TableDescription": t.description()}, nil
}

func parseIndexes(body map[string]any, field string, defs map[string]string, dst *[]map[string]any) *apiError {
	raw, present := body[field].([]any)
	if !present {
		return nil
	}
	for _, elAny := range raw {
		el, ok := elAny.(map[string]any)
		if !ok {
			return validationErr("One or more parameter values are not valid: Invalid index definition in %s", field)
		}
		if _, ok := el["IndexName"].(string); !ok {
			return validationErr("One or more parameter values are not valid: Invalid index definition in %s", field)
		}
		if ks, ksOk := el["KeySchema"].([]any); ksOk {
			for _, kAny := range ks {
				kEl, kOk := kAny.(map[string]any)
				if !kOk {
					return validationErr("One or more parameter values are not valid: Invalid index definition in %s", field)
				}
				an, _ := kEl["AttributeName"].(string)
				if _, defined := defs[an]; !defined {
					return validationErr("One or more parameter values were invalid: Some index key attributes have no definition. If you are using a new table with key schema, please ensure that the key attribute pairs in KeySchema have attribute definitions set via AttributeDefinitions.")
				}
			}
		}
		*dst = append(*dst, el)
	}
	return nil
}

func opDescribeTable(s *store, body map[string]any) (any, *apiError) {
	name, aerr := tableNameFrom(body)
	if aerr != nil {
		return nil, aerr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tables[name]
	if !ok {
		return nil, resourceNotFoundErr()
	}
	return map[string]any{"Table": t.description()}, nil
}

func opDeleteTable(s *store, body map[string]any) (any, *apiError) {
	name, aerr := tableNameFrom(body)
	if aerr != nil {
		return nil, aerr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tables[name]
	if !ok {
		return nil, resourceNotFoundErr()
	}
	delete(s.tables, name)
	return map[string]any{"TableDescription": t.description()}, nil
}

func opListTables(s *store, body map[string]any) (any, *apiError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := []string{}
	for name := range s.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	if start, ok := strField(body, "ExclusiveStartTableName"); ok {
		filtered := names[:0]
		for _, nm := range names {
			if nm > start {
				filtered = append(filtered, nm)
			}
		}
		names = filtered
	}
	if lim, ok := body["Limit"].(float64); ok && lim > 0 && int(lim) < len(names) {
		names = names[:int(lim)]
	}
	return map[string]any{"TableNames": names}, nil
}

func opPutItem(s *store, body map[string]any) (any, *apiError) {
	name, aerr := tableNameFrom(body)
	if aerr != nil {
		return nil, aerr
	}
	t, aerr := s.getTable(name)
	if aerr != nil {
		return nil, aerr
	}
	rawItem, present := body["Item"]
	if !present || rawItem == nil {
		return nil, validationErr("1 validation error detected: Value 'null' at 'item' failed to satisfy constraint: Member must not be null")
	}
	item, ok := rawItem.(map[string]any)
	if !ok {
		return nil, validationErr("Item must be a map of attribute values")
	}
	for _, v := range item {
		if aerr := checkAttrValue(v); aerr != nil {
			return nil, aerr
		}
	}
	canon, aerr := t.itemKeyCanon(item)
	if aerr != nil {
		return nil, aerr
	}
	rcp, aerr := parseRCP(body)
	if aerr != nil {
		return nil, aerr
	}
	rv, aerr := parseReturnValues(body)
	if aerr != nil {
		return nil, aerr
	}
	stored := deepCopy(item).(map[string]any)
	size := approxSize(stored)
	s.mu.Lock()
	old, existed := t.items[canon]
	t.items[canon] = stored
	s.mu.Unlock()
	resp := map[string]any{}
	if rv == "ALL_OLD" && existed {
		resp["Attributes"] = old
	}
	if rcp == "TOTAL" || rcp == "INDEXES" {
		kb := (size + 1023) / 1024
		if kb < 1 {
			kb = 1
		}
		resp["ConsumedCapacity"] = []any{capacityEntry(t.name, float64(kb), rcp)}
	}
	return resp, nil
}

func opGetItem(s *store, body map[string]any) (any, *apiError) {
	name, aerr := tableNameFrom(body)
	if aerr != nil {
		return nil, aerr
	}
	t, aerr := s.getTable(name)
	if aerr != nil {
		return nil, aerr
	}
	keyRaw, ok := body["Key"].(map[string]any)
	if !ok {
		return nil, validationErr("1 validation error detected: Value 'null' at 'key' failed to satisfy constraint: Member must not be null")
	}
	canon, aerr := t.keyCanon(keyRaw)
	if aerr != nil {
		return nil, aerr
	}
	paths, aerr := projectionFromBody(body)
	if aerr != nil {
		return nil, aerr
	}
	rcp, aerr := parseRCP(body)
	if aerr != nil {
		return nil, aerr
	}
	consistent := false
	if cv, ok := body["ConsistentRead"].(bool); ok {
		consistent = cv
	}
	s.mu.Lock()
	item, found := t.items[canon]
	s.mu.Unlock()
	resp := map[string]any{}
	if found {
		out := item
		if paths != nil {
			out = projectItem(item, paths)
		}
		resp["Item"] = out
	}
	if rcp == "TOTAL" || rcp == "INDEXES" {
		blocks := 1
		if found {
			blocks = blockCount(approxSize(item))
		}
		mult := 0.5
		if consistent {
			mult = 1.0
		}
		resp["ConsumedCapacity"] = []any{capacityEntry(t.name, mult*float64(blocks), rcp)}
	}
	return resp, nil
}

func opDeleteItem(s *store, body map[string]any) (any, *apiError) {
	name, aerr := tableNameFrom(body)
	if aerr != nil {
		return nil, aerr
	}
	t, aerr := s.getTable(name)
	if aerr != nil {
		return nil, aerr
	}
	keyRaw, ok := body["Key"].(map[string]any)
	if !ok {
		return nil, validationErr("1 validation error detected: Value 'null' at 'key' failed to satisfy constraint: Member must not be null")
	}
	canon, aerr := t.keyCanon(keyRaw)
	if aerr != nil {
		return nil, aerr
	}
	rcp, aerr := parseRCP(body)
	if aerr != nil {
		return nil, aerr
	}
	rv, aerr := parseReturnValues(body)
	if aerr != nil {
		return nil, aerr
	}
	s.mu.Lock()
	old, existed := t.items[canon]
	if existed {
		delete(t.items, canon)
	}
	s.mu.Unlock()
	resp := map[string]any{}
	if rv == "ALL_OLD" && existed {
		resp["Attributes"] = old
	}
	if rcp == "TOTAL" || rcp == "INDEXES" {
		kb := 1
		if existed {
			kb = (approxSize(old) + 1023) / 1024
		}
		resp["ConsumedCapacity"] = []any{capacityEntry(t.name, float64(kb), rcp)}
	}
	return resp, nil
}

func opBatchGetItem(s *store, body map[string]any) (any, *apiError) {
	rawReq, present := body["RequestItems"]
	if !present || rawReq == nil {
		return nil, validationErr("1 validation error detected: Value 'null' at 'requestItems' failed to satisfy constraint: Member must not be null")
	}
	reqItems, ok := rawReq.(map[string]any)
	if !ok {
		return nil, validationErr("RequestItems must be a map of table names to batch requests")
	}
	if len(reqItems) == 0 {
		return nil, validationErr("RequestItems must not be empty")
	}
	rcp, aerr := parseRCP(body)
	if aerr != nil {
		return nil, aerr
	}
	var order []string
	for tname := range reqItems {
		order = append(order, tname)
	}
	sort.Strings(order)
	responses := map[string]any{}
	unprocessed := map[string]any{}
	consumed := []any{}
	for _, tname := range order {
		q, ok := reqItems[tname].(map[string]any)
		if !ok {
			return nil, validationErr("RequestItems for table %s must be a map", tname)
		}
		t, aerr := s.getTable(tname)
		if aerr != nil {
			return nil, aerr
		}
		keys, ok := q["Keys"].([]any)
		if !ok || len(keys) == 0 {
			return nil, validationErr("One or more parameter values were invalid: Missing the Key list in the request")
		}
		if len(keys) > 100 {
			return nil, validationErr("Too many items requested for the BatchGetItem call")
		}
		paths, aerr := projectionFromBody(q)
		if aerr != nil {
			return nil, aerr
		}
		consistent := false
		if cv, ok := q["ConsistentRead"].(bool); ok {
			consistent = cv
		}
		seen := map[string]bool{}
		items := []any{}
		var units float64
		for _, kAny := range keys {
			kraw, ok := kAny.(map[string]any)
			if !ok {
				return nil, validationErr("The provided key element does not match the schema")
			}
			canon, aerr := t.keyCanon(kraw)
			if aerr != nil {
				return nil, aerr
			}
			if seen[canon] {
				return nil, validationErr("Provided list of item keys contains duplicates.")
			}
			seen[canon] = true
			s.mu.Lock()
			item, found := t.items[canon]
			s.mu.Unlock()
			if found {
				out := item
				if paths != nil {
					out = projectItem(item, paths)
				}
				items = append(items, out)
				if consistent {
					units += float64(blockCount(approxSize(item)))
				} else {
					units += 0.5 * float64(blockCount(approxSize(item)))
				}
			}
		}
		responses[tname] = items
		if units == 0 {
			units = 0.5
		}
		if rcp == "TOTAL" || rcp == "INDEXES" {
			consumed = append(consumed, capacityEntry(tname, units, rcp))
		}
	}
	resp := map[string]any{"Responses": responses, "UnprocessedKeys": unprocessed}
	if rcp == "TOTAL" || rcp == "INDEXES" {
		resp["ConsumedCapacity"] = consumed
	}
	return resp, nil
}

// opTransactGetItems implements ticket-019: up to 100 Gets (strongly
// consistent, validated keys, optional projection), returned in request
// order; nonexistent keys are omitted from Responses.
func opTransactGetItems(s *store, body map[string]any) (any, *apiError) {
	listAny, present := body["TransactItems"]
	if !present || listAny == nil {
		return nil, validationErr("1 validation error detected: Value 'null' at 'transactItems' failed to satisfy constraint: Member must not be null")
	}
	list, ok := listAny.([]any)
	if !ok {
		return nil, validationErr("TransactItems must be a list of Get actions")
	}
	if len(list) == 0 {
		return nil, validationErr("1 validation error detected: Value '[]' at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1")
	}
	if len(list) > 100 {
		return nil, validationErr("Transaction cannot contain more than 100 items")
	}
	rcp, aerr := parseRCP(body)
	if aerr != nil {
		return nil, aerr
	}

	type txnGet struct {
		table *table
		canon string
		paths [][]any
	}
	reqs := make([]txnGet, len(list))
	seen := make(map[string]bool, len(list))

	// Phase 1: validate every Get up front, in request order.
	for i, raw := range list {
		el, elOk := raw.(map[string]any)
		if !elOk || len(el) != 1 {
			return nil, validationErr("One or more parameter values are not valid: Member must have exactly one of the following: Get")
		}
		get, getOk := el["Get"].(map[string]any)
		if !getOk {
			return nil, validationErr("One or more parameter values are not valid: Member must have exactly one of the following: Get")
		}
		tnameAny, present := get["TableName"]
		if !present || tnameAny == nil {
			return nil, validationErr("1 validation error detected: Value 'null' at 'tableName' failed to satisfy constraint: Member must not be null")
		}
		tname, tnameOk := tnameAny.(string)
		if !tnameOk {
			return nil, validationErr("1 validation error detected: Value '%v' at 'tableName' failed to satisfy constraint: Member must not be null", tnameAny)
		}
		t, aerr := s.getTable(tname)
		if aerr != nil {
			return nil, aerr
		}
		keyRaw, keyOk := get["Key"].(map[string]any)
		if !keyOk {
			return nil, validationErr("1 validation error detected: Value 'null' at 'key' failed to satisfy constraint: Member must not be null")
		}
		canon, aerr := t.keyCanon(keyRaw)
		if aerr != nil {
			return nil, aerr
		}
		dup := tname + "\x00" + canon
		if seen[dup] {
			return nil, validationErr("Provided list of item keys contains duplicates.")
		}
		seen[dup] = true
		paths, aerr := projectionFromBody(get)
		if aerr != nil {
			return nil, aerr
		}
		reqs[i] = txnGet{table: t, canon: canon, paths: paths}
	}

	// Phase 2: execute in request order.
	responses := []any{}
	consumed := []any{}
	capacityWanted := rcp == "TOTAL" || rcp == "INDEXES"
	for _, rq := range reqs {
		s.mu.Lock()
		item, found := rq.table.items[rq.canon]
		s.mu.Unlock()
		if capacityWanted {
			units := 2.0
			if found {
				if bc := blockCount(approxSize(item)); bc > 1 {
					units = 2.0 * float64(bc)
				}
			}
			consumed = append(consumed, capacityEntry(rq.table.name, units, rcp))
		}
		if found {
			out := item
			if rq.paths != nil {
				out = projectItem(item, rq.paths)
			}
			responses = append(responses, map[string]any{"Item": out, "TableName": rq.table.name})
		}
	}
	resp := map[string]any{"Responses": responses}
	if capacityWanted {
		resp["ConsumedCapacity"] = consumed
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// HTTP plumbing

var routes = map[string]opFunc{
	"CreateTable":      opCreateTable,
	"DescribeTable":    opDescribeTable,
	"DeleteTable":      opDeleteTable,
	"ListTables":       opListTables,
	"PutItem":          opPutItem,
	"GetItem":          opGetItem,
	"DeleteItem":       opDeleteItem,
	"BatchGetItem":     opBatchGetItem,
	"TransactGetItems": opTransactGetItems,
}

func makeHandler(st *store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target := strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "DynamoDB_20120810.")
		op, known := routes[target]
		if target == "" || !known {
			writeError(w, unknownOpErr())
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, serializationErr())
			return
		}
		body := map[string]any{}
		if len(raw) > 0 {
			var parsed map[string]any
			if uerr := json.Unmarshal(raw, &parsed); uerr != nil {
				writeError(w, serializationErr())
				return
			}
			if parsed != nil {
				body = parsed
			}
		}
		resp, aerr := op(st, body)
		if aerr != nil {
			writeError(w, aerr)
			return
		}
		if resp == nil {
			resp = map[string]any{}
		}
		writeJSON(w, http.StatusOK, resp)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	buf, err := json.Marshal(v)
	if err != nil {
		buf = []byte("{}")
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.Header().Set("Content-Length", strconv.Itoa(len(buf)))
	w.WriteHeader(status)
	w.Write(buf)
}

func writeError(w http.ResponseWriter, e *apiError) {
	body := map[string]any{"__type": e.typ}
	if e.msg != "" {
		body["message"] = e.msg
	}
	writeJSON(w, e.status, body)
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	addr := ":" + port
	srv := &http.Server{Addr: addr, Handler: makeHandler(newStore())}
	fmt.Fprintf(os.Stderr, "floci-go: listening on %s\n", addr)
	if err := srv.ListenAndServe(); err != nil {
		fmt.Fprintln(os.Stderr, "floci-go:", err)
		os.Exit(1)
	}
}
