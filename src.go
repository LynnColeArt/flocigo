package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// floci-go, ticket-014: GetItem.
//
// DynamoDB_20120810.GetItem reads one item by primary key (pk, or pk+sk on
// composite-key tables). Wire contract: the item is returned as
// {"Item": {...}}; when no item matches, "Item" is omitted entirely (the
// response body is just {}). ConsistentRead is honored: this store is a
// single authoritative in-memory replica, so every read observes the latest
// write; the flag changes the charged read capacity (1.0 strongly consistent
// vs 0.5 eventually consistent).
//
// Supported request members: TableName, Key, ConsistentRead,
// ProjectionExpression, ExpressionAttributeNames, AttributesToGet (legacy)
// and ReturnConsumedCapacity. Error envelopes follow DynamoDB's wire format
// (ValidationException, ResourceNotFoundException, SerializationException,
// UnknownOperationException).

const (
	typeS    = `S`
	typeN    = `N`
	typeB    = `B`
	typeSS   = `SS`
	typeNS   = `NS`
	typeBS   = `BS`
	typeM    = `M`
	typeL    = `L`
	typeBOOL = `BOOL`
	typeNULL = `NULL`
)

const msgAttrShape = `Supplied AttributeValue is empty, must contain exactly one of the supported datatypes`

const (
	targetGetItem       = `DynamoDB_20120810.GetItem`
	msgKeySchema        = `The provided key element does not match the schema`
	msgNumberConversion = `The parameter cannot be converted to a numeric value`

	errCodeValidation      = `com.amazonaws.dynamodb.v20120810#ValidationException`
	errCodeResourceMissing = `com.amazonaws.dynamodb.v20120810#ResourceNotFoundException`
	errCodeSerialization   = `com.amazon.coral.service#SerializationException`
	errCodeUnknownOp       = `com.amazon.coral.service#UnknownOperationException`
)

var (
	tableNameRe = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)
	numberRe    = regexp.MustCompile(`^-?[0-9]+(?:\.[0-9]*)?(?:[eE]([+-]?[0-9]+))?$`)
)

// apiErr is an operation-level failure rendered as a DynamoDB error envelope.
type apiErr struct {
	status  int
	errCode string
	msg     string
}

func (e *apiErr) Error() string { return e.errCode + `: ` + e.msg }

func validationErr(msg string) *apiErr {
	return &apiErr{status: 400, errCode: errCodeValidation, msg: msg}
}

func validationErrf(format string, args ...interface{}) *apiErr {
	return validationErr(fmt.Sprintf(format, args...))
}

func serializationErr() *apiErr {
	return &apiErr{status: 400, errCode: errCodeSerialization}
}

// ---------------------------------------------------------------------------
// Typed attribute values
// ---------------------------------------------------------------------------

// AttributeValue is a DynamoDB typed value; exactly one variant is populated
// and Type records which one (wire form: {"S": "..."}, {"N": "1"}, ...).
type AttributeValue struct {
	Type string
	S    string
	N    string
	B    []byte
	SS   []string
	NS   []string
	BS   [][]byte
	M    map[string]*AttributeValue
	L    []*AttributeValue
	Bool bool
	Null bool
}

func sAttr(v string) *AttributeValue { return &AttributeValue{Type: typeS, S: v} }
func nAttr(v string) *AttributeValue { return &AttributeValue{Type: typeN, N: v} }
func bAttr(v []byte) *AttributeValue { return &AttributeValue{Type: typeB, B: v} }
func ssAttr(v ...string) *AttributeValue { return &AttributeValue{Type: typeSS, SS: v} }
func nsAttr(v ...string) *AttributeValue { return &AttributeValue{Type: typeNS, NS: v} }
func bsAttr(v ...[]byte) *AttributeValue { return &AttributeValue{Type: typeBS, BS: v} }

func mAttr(v map[string]*AttributeValue) *AttributeValue {
	if v == nil {
		v = map[string]*AttributeValue{}
	}
	return &AttributeValue{Type: typeM, M: v}
}

func lAttr(v []*AttributeValue) *AttributeValue {
	if v == nil {
		v = []*AttributeValue{}
	}
	return &AttributeValue{Type: typeL, L: v}
}

func boolAttr(v bool) *AttributeValue { return &AttributeValue{Type: typeBOOL, Bool: v} }
func nullAttr() *AttributeValue { return &AttributeValue{Type: typeNULL, Null: true} }

// attrFromValue decodes one DynamoDB AttributeValue from its JSON envelope.
// JSON-level damage (wrong member types, nulls, unknown types, multiple
// members) surfaces as the corresponding DynamoDB wire error.
func attrFromValue(data []byte) (*AttributeValue, *apiErr) {
	if isJSONNull(data) {
		return nil, serializationErr()
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, serializationErr()
	}
	if len(raw) != 1 {
		return nil, validationErr(msgAttrShape)
	}
	for name, v := range raw {
		a := &AttributeValue{}
		switch name {
		case typeS:
			if err := json.Unmarshal(v, &a.S); err != nil {
				return nil, serializationErr()
			}
			a.Type = typeS
		case typeN:
			if err := json.Unmarshal(v, &a.N); err != nil {
				return nil, serializationErr()
			}
			a.Type = typeN
		case typeB:
			if err := json.Unmarshal(v, &a.B); err != nil {
				return nil, serializationErr()
			}
			a.Type = typeB
		case typeSS:
			if err := json.Unmarshal(v, &a.SS); err != nil {
				return nil, serializationErr()
			}
			a.Type = typeSS
		case typeNS:
			if err := json.Unmarshal(v, &a.NS); err != nil {
				return nil, serializationErr()
			}
			a.Type = typeNS
		case typeBS:
			if err := json.Unmarshal(v, &a.BS); err != nil {
				return nil, serializationErr()
			}
			a.Type = typeBS
		case typeBOOL:
			if err := json.Unmarshal(v, &a.Bool); err != nil {
				return nil, serializationErr()
			}
			a.Type = typeBOOL
		case typeNULL:
			if err := json.Unmarshal(v, &a.Null); err != nil {
				return nil, serializationErr()
			}
			a.Type = typeNULL
		case typeM:
			var rawM map[string]json.RawMessage
			if err := json.Unmarshal(v, &rawM); err != nil {
				return nil, serializationErr()
			}
			a.Type = typeM
			a.M = make(map[string]*AttributeValue, len(rawM))
			for k, rawValue := range rawM {
				child, childErr := attrFromValue(rawValue)
				if childErr != nil {
					return nil, childErr
				}
				a.M[k] = child
			}
		case typeL:
			var rawL []json.RawMessage
			if err := json.Unmarshal(v, &rawL); err != nil {
				return nil, serializationErr()
			}
			a.Type = typeL
			a.L = make([]*AttributeValue, 0, len(rawL))
			for _, rawValue := range rawL {
				child, childErr := attrFromValue(rawValue)
				if childErr != nil {
					return nil, childErr
				}
				a.L = append(a.L, child)
			}
		default:
			return nil, validationErr(msgAttrShape)
		}
		return a, nil
	}
	return nil, validationErr(msgAttrShape)
}

// UnmarshalJSON implements json.Unmarshaler on top of attrFromValue.
func (a *AttributeValue) UnmarshalJSON(data []byte) error {
	v, aerr := attrFromValue(data)
	if aerr != nil {
		return aerr
	}
	*a = *v
	return nil
}

// MarshalJSON renders the DynamoDB AttributeValue envelope. Map members are
// emitted in sorted order so responses are deterministic.
func (a *AttributeValue) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	switch a.Type {
	case typeS:
		b.WriteString(`"S":`)
		appendQuoted(&b, a.S)
	case typeN:
		b.WriteString(`"N":`)
		appendQuoted(&b, a.N)
	case typeB:
		b.WriteString(`"B":`)
		appendQuoted(&b, base64.StdEncoding.EncodeToString(a.B))
	case typeSS:
		b.WriteString(`"SS":`)
		appendQuotedList(&b, a.SS)
	case typeNS:
		b.WriteString(`"NS":`)
		appendQuotedList(&b, a.NS)
	case typeBS:
		b.WriteString(`"BS":[`)
		for i, x := range a.BS {
			if i > 0 {
				b.WriteByte(',')
			}
			appendQuoted(&b, base64.StdEncoding.EncodeToString(x))
		}
		b.WriteByte(']')
	case typeM:
		b.WriteString(`"M":{`)
		names := make([]string, 0, len(a.M))
		for k := range a.M {
			names = append(names, k)
		}
		sort.Strings(names)
		for i, k := range names {
			if i > 0 {
				b.WriteByte(',')
			}
			appendQuoted(&b, k)
			b.WriteByte(':')
			if child := a.M[k]; child == nil {
				b.WriteString(`null`)
			} else {
				encoded, err := child.MarshalJSON()
				if err != nil {
					return nil, err
				}
				b.Write(encoded)
			}
		}
		b.WriteByte('}')
	case typeL:
		b.WriteString(`"L":[`)
		for i, child := range a.L {
			if i > 0 {
				b.WriteByte(',')
			}
			if child == nil {
				b.WriteString(`null`)
			} else {
				encoded, err := child.MarshalJSON()
				if err != nil {
					return nil, err
				}
				b.Write(encoded)
			}
		}
		b.WriteByte(']')
	case typeBOOL:
		fmt.Fprintf(&b, `"BOOL":%t`, a.Bool)
	case typeNULL:
		fmt.Fprintf(&b, `"NULL":%t`, a.Null)
	default:
		return nil, fmt.Errorf(`unknown attribute value type %q`, a.Type)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

func appendQuoted(b *bytes.Buffer, s string) {
	encoded, _ := json.Marshal(s)
	b.Write(encoded)
}

func appendQuotedList(b *bytes.Buffer, values []string) {
	b.WriteByte('[')
	for i, v := range values {
		if i > 0 {
			b.WriteByte(',')
		}
		appendQuoted(b, v)
	}
	b.WriteByte(']')
}

// copyAttr deep-copies a value so stored items are never aliased by results.
func copyAttr(a *AttributeValue) *AttributeValue {
	if a == nil {
		return nil
	}
	c := &AttributeValue{Type: a.Type, S: a.S, N: a.N, Bool: a.Bool, Null: a.Null}
	if a.B != nil {
		c.B = append([]byte(nil), a.B...)
	}
	if a.SS != nil {
		c.SS = append([]string(nil), a.SS...)
	}
	if a.NS != nil {
		c.NS = append([]string(nil), a.NS...)
	}
	if a.BS != nil {
		for _, x := range a.BS {
			c.BS = append(c.BS, append([]byte(nil), x...))
		}
	}
	if a.M != nil {
		c.M = make(map[string]*AttributeValue, len(a.M))
		for k, v := range a.M {
			c.M[k] = copyAttr(v)
		}
	}
	if a.L != nil {
		c.L = make([]*AttributeValue, len(a.L))
		for i, v := range a.L {
			c.L[i] = copyAttr(v)
		}
	}
	return c
}

// ---------------------------------------------------------------------------
// Numbers and canonical keys
// ---------------------------------------------------------------------------

// canonNumber reduces a DynamoDB number literal to a canonical decimal form
// so numerically equal values ("1" vs "1.0" vs "1e0") identify the same key.
func canonNumber(s string) (string, bool) {
	m := numberRe.FindStringSubmatch(s)
	if m == nil {
		return ``, false
	}
	if m[1] != `` {
		exp, err := strconv.Atoi(m[1])
		if err != nil || exp > 1000 || exp < -1000 {
			return ``, false
		}
	}
	r, ok := new(big.Rat).SetString(strings.TrimSuffix(s, `.`))
	if !ok {
		return ``, false
	}
	return r.RatString(), true
}

func canonicalAttr(v *AttributeValue) string {
	switch v.Type {
	case typeS:
		return `s:` + v.S
	case typeN:
		c, _ := canonNumber(v.N)
		return `n:` + c
	case typeB:
		return `b:` + base64.RawStdEncoding.EncodeToString(v.B)
	}
	return `x`
}

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

// Item maps attribute names (case-sensitive) to typed values.
type Item map[string]*AttributeValue

// Table holds a key schema (pk[/sk]) and items keyed canonically. Stored
// items are private deep copies and never mutated after publication, so
// reader goroutines may consume them without holding the write lock.
type Table struct {
	Name     string
	HashKey  string
	RangeKey string // empty for hash-key-only tables
	keyTypes map[string]string
	mu       sync.RWMutex
	items    map[string]Item
}

// DB is the in-process store owning all tables.
type DB struct {
	mu     sync.RWMutex
	tables map[string]*Table
}

func NewDB() *DB { return &DB{tables: map[string]*Table{}} }

// CreateTable registers a table with its key schema. Full table lifecycle is
// driven by other operations; bootstrap and tests use this helper directly.
func (db *DB) CreateTable(name, hashKey, rangeKey string, keyTypes map[string]string) *Table {
	t := &Table{Name: name, HashKey: hashKey, RangeKey: rangeKey, keyTypes: keyTypes, items: map[string]Item{}}
	db.mu.Lock()
	db.tables[name] = t
	db.mu.Unlock()
	return t
}

func (db *DB) table(name string) *Table {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.tables[name]
}

// Put stores a deep copy of item under its canonical key (bootstrap helper).
func (t *Table) Put(item Item) error {
	key := map[string]*AttributeValue{}
	for _, d := range t.keyDefs() {
		v := item[d.name]
		if v == nil {
			return fmt.Errorf(`item is missing key attribute %q`, d.name)
		}
		key[d.name] = v
	}
	canon, aerr := t.validateKey(key)
	if aerr != nil {
		return aerr
	}
	stored := make(Item, len(item))
	for k, v := range item {
		stored[k] = copyAttr(v)
	}
	t.mu.Lock()
	t.items[canon] = stored
	t.mu.Unlock()
	return nil
}

// get returns the stored item for a canonical key. consistent records the
// requested read mode: with a single authoritative replica both modes see
// the latest committed state; the flag drives capacity accounting upstream.
func (t *Table) get(canon string, consistent bool) (Item, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	it, ok := t.items[canon]
	return it, ok
}

type keyDef struct {
	name     string
	wantType string
}

func (t *Table) keyDefs() []keyDef {
	defs := []keyDef{{t.HashKey, t.keyTypes[t.HashKey]}}
	if t.RangeKey != `` {
		defs = append(defs, keyDef{t.RangeKey, t.keyTypes[t.RangeKey]})
	}
	return defs
}

// validateKey checks a Key document against the table schema and returns the
// canonical lookup key. Missing/extra key attributes, wrong value types and
// empty scalars fail exactly like DynamoDB does.
func (t *Table) validateKey(provided map[string]*AttributeValue) (string, *apiErr) {
	defs := t.keyDefs()
	if len(provided) != len(defs) {
		return ``, validationErr(msgKeySchema)
	}
	for name := range provided {
		if name != t.HashKey && name != t.RangeKey {
			return ``, validationErr(msgKeySchema)
		}
	}
	var canon string
	for idx, d := range defs {
		v := provided[d.name]
		if v == nil {
			return ``, validationErr(msgKeySchema)
		}
		if aerr := checkKeyScalar(d.name, d.wantType, v); aerr != nil {
			return ``, aerr
		}
		part := canonicalAttr(v)
		if idx == 0 {
			canon = part
		} else {
			canon += "\x00" + part
		}
	}
	return canon, nil
}

func checkKeyScalar(name, want string, v *AttributeValue) *apiErr {
	switch want {
	case typeS:
		if v.Type != typeS {
			return validationErr(msgKeySchema)
		}
		if v.S == `` {
			return validationErrf(`One or more parameter values are not valid: The AttributeValue for a key attribute can not contain an empty string value. Key: %s`, name)
		}
	case typeN:
		if v.Type != typeN {
			return validationErr(msgKeySchema)
		}
		if _, ok := canonNumber(v.N); !ok {
			return validationErr(msgNumberConversion)
		}
	case typeB:
		if v.Type != typeB {
			return validationErr(msgKeySchema)
		}
		if len(v.B) == 0 {
			return validationErrf(`One or more parameter values are not valid: The AttributeValue for a key attribute can not contain an empty binary value. Key: %s`, name)
		}
	default:
		return validationErr(msgKeySchema)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Projection (ProjectionExpression and legacy AttributesToGet)
// ---------------------------------------------------------------------------

type pathSeg struct {
	name  string
	index int
	isIdx bool
}

type projPath struct{ segs []pathSeg }

func isNameChar(c byte) bool {
	return c == '_' || c == '-' ||
		(c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isExprSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// parseProjection parses a ProjectionExpression: a comma-separated list of
// document paths (name or #placeholder, dotted nesting, [index] list access).
// It also rejects ExpressionAttributeNames that are provided but never used.
func parseProjection(expr string, names map[string]string) ([]projPath, *apiErr) {
	var (
		paths []projPath
		used  = map[string]bool{}
		i     = 0
		n     = len(expr)
	)
	skipWS := func() {
		for i < n && isExprSpace(expr[i]) {
			i++
		}
	}
	syntax := func() *apiErr {
		tok := ``
		if i < n {
			tok = string(expr[i])
		}
		return validationErrf(`Invalid ProjectionExpression: Syntax error; token: %q, near: %q`, tok, expr[i:])
	}
	parseSegment := func() (pathSeg, *apiErr) {
		skipWS()
		if i >= n {
			return pathSeg{}, syntax()
		}
		if expr[i] == '#' {
			start := i
			i++
			phStart := i
			for i < n && isNameChar(expr[i]) {
				i++
			}
			if i == phStart {
				return pathSeg{}, syntax()
			}
			key := expr[start:i]
			name, ok := names[key]
			if !ok {
				return pathSeg{}, validationErr(`An expression attribute name used in ProjectionExpression is not defined`)
			}
			used[key] = true
			return pathSeg{name: name}, nil
		}
		if !isNameChar(expr[i]) {
			return pathSeg{}, syntax()
		}
		start := i
		for i < n && isNameChar(expr[i]) {
			i++
		}
		return pathSeg{name: expr[start:i]}, nil
	}

	for {
		seg, segErr := parseSegment()
		if segErr != nil {
			return nil, segErr
		}
		path := projPath{segs: []pathSeg{seg}}
		for {
			skipWS()
			if i < n && expr[i] == '.' {
				i++
				next, nextErr := parseSegment()
				if nextErr != nil {
					return nil, nextErr
				}
				path.segs = append(path.segs, next)
				continue
			}
			if i < n && expr[i] == '[' {
				i++
				skipWS()
				start := i
				for i < n && expr[i] >= '0' && expr[i] <= '9' {
					i++
				}
				if i == start {
					return nil, syntax()
				}
				idx, atoiErr := strconv.Atoi(expr[start:i])
				if atoiErr != nil {
					return nil, syntax()
				}
				skipWS()
				if i >= n || expr[i] != ']' {
					return nil, syntax()
				}
				i++
				path.segs = append(path.segs, pathSeg{isIdx: true, index: idx})
				continue
			}
			break
		}
		paths = append(paths, path)
		skipWS()
		if i >= n {
			break
		}
		if expr[i] != ',' {
			return nil, syntax()
		}
		i++
	}

	if names != nil && len(used) < len(names) {
		var unused []string
		for k := range names {
			if !used[k] {
				unused = append(unused, k)
			}
		}
		if len(unused) > 0 {
			sort.Strings(unused)
			return nil, validationErrf(`Value provided in ExpressionAttributeNames unused in expressions: keys: {%s}`, strings.Join(unused, `, `))
		}
	}
	return paths, nil
}

func pathsOverlap(x, y []pathSeg) bool {
	m := len(x)
	if len(y) < m {
		m = len(y)
	}
	for i := 0; i < m; i++ {
		if x[i].isIdx != y[i].isIdx {
			return false
		}
		if x[i].isIdx {
			if x[i].index != y[i].index {
				return false
			}
		} else if x[i].name != y[i].name {
			return false
		}
	}
	return true // one path is a prefix of the other (or they are equal)
}

func checkOverlap(paths []projPath) *apiErr {
	for a := 0; a < len(paths); a++ {
		for b := a + 1; b < len(paths); b++ {
			if pathsOverlap(paths[a].segs, paths[b].segs) {
				return validationErr(`Invalid ProjectionExpression: Two document paths overlap with each other; must be fixed or removed`)
			}
		}
	}
	return nil
}

func resolvePath(item Item, segs []pathSeg) *AttributeValue {
	var cur *AttributeValue
	for idx, seg := range segs {
		if idx == 0 {
			v, ok := item[seg.name]
			if !ok || v == nil {
				return nil
			}
			cur = v
			continue
		}
		if cur == nil {
			return nil
		}
		if seg.isIdx {
			if cur.Type != typeL || seg.index < 0 || seg.index >= len(cur.L) {
				return nil
			}
			cur = cur.L[seg.index]
		} else {
			if cur.Type != typeM {
				return nil
			}
			v, ok := cur.M[seg.name]
			if !ok || v == nil {
				return nil
			}
			cur = v
		}
	}
	return cur
}

type projNode struct {
	val    *AttributeValue
	kids   map[string]*projNode
	lkids  map[int]*projNode
	maxIdx int
}

func (n *projNode) insert(segs []pathSeg, v *AttributeValue) {
	if len(segs) == 0 {
		n.val = copyAttr(v)
		return
	}
	seg := segs[0]
	if seg.isIdx {
		if n.lkids == nil {
			n.lkids = map[int]*projNode{}
		}
		child := n.lkids[seg.index]
		if child == nil {
			child = &projNode{}
			n.lkids[seg.index] = child
		}
		if seg.index > n.maxIdx {
			n.maxIdx = seg.index
		}
		child.insert(segs[1:], v)
		return
	}
	if n.kids == nil {
		n.kids = map[string]*projNode{}
	}
	child := n.kids[seg.name]
	if child == nil {
		child = &projNode{}
		n.kids[seg.name] = child
	}
	child.insert(segs[1:], v)
}

func (n *projNode) render() *AttributeValue {
	if n.val != nil {
		return n.val
	}
	if n.lkids != nil {
		l := make([]*AttributeValue, n.maxIdx+1)
		for i := range l {
			l[i] = nullAttr() // sparse projected lists are NULL-filled
		}
		for idx, child := range n.lkids {
			l[idx] = child.render()
		}
		return &AttributeValue{Type: typeL, L: l}
	}
	m := make(map[string]*AttributeValue, len(n.kids))
	for name, child := range n.kids {
		m[name] = child.render()
	}
	return &AttributeValue{Type: typeM, M: m}
}

func projectItem(item Item, paths []projPath) map[string]*AttributeValue {
	root := &projNode{}
	for _, p := range paths {
		if v := resolvePath(item, p.segs); v != nil {
			root.insert(p.segs, v)
		}
	}
	out := map[string]*AttributeValue{}
	for name, child := range root.kids {
		out[name] = child.render()
	}
	return out
}

func projectTopLevel(item Item, names []string) map[string]*AttributeValue {
	out := map[string]*AttributeValue{}
	for _, n := range names {
		if v, ok := item[n]; ok && v != nil {
			out[n] = copyAttr(v)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// GetItem operation
// ---------------------------------------------------------------------------

type getItemInput struct {
	TableName                *string           `json:"TableName"`
	Key                      json.RawMessage   `json:"Key"`
	ConsistentRead           *bool             `json:"ConsistentRead"`
	AttributesToGet          []string          `json:"AttributesToGet"`
	ProjectionExpression     *string           `json:"ProjectionExpression"`
	ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames"`
	ReturnConsumedCapacity   *string           `json:"ReturnConsumedCapacity"`
}

// capacityUnits renders 1 as "1.0" on the wire, like DynamoDB does.
type capacityUnits float64

func (c capacityUnits) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatFloat(float64(c), 'f', 1, 64)), nil
}

type consumedCapacity struct {
	TableName     string        `json:"TableName,omitempty"`
	CapacityUnits capacityUnits `json:"CapacityUnits,omitempty"`
}

func isJSONNull(raw json.RawMessage) bool {
	return len(raw) == 0 || string(bytes.TrimSpace(raw)) == `null`
}

// doGetItem implements the GetItem semantics and returns the response body
// object, or an operation-level error.
func doGetItem(db *DB, body []byte) (map[string]interface{}, *apiErr) {
	var in getItemInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, serializationErr()
	}

	if in.TableName == nil {
		return nil, validationErr(`1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null`)
	}
	name := *in.TableName
	if !tableNameRe.MatchString(name) {
		return nil, validationErrf(`1 validation error detected: Value '%s' at 'tableName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+`, name)
	}

	if isJSONNull(in.Key) {
		return nil, validationErr(`1 validation error detected: Value null at 'key' failed to satisfy constraint: Member must not be null`)
	}

	if in.AttributesToGet != nil && in.ProjectionExpression != nil {
		return nil, validationErr(`Cannot specify both AttributesToGet and ProjectionExpression`)
	}
	if in.ExpressionAttributeNames != nil {
		if in.ProjectionExpression == nil {
			return nil, validationErr(`ExpressionAttributeNames can only be provided when expressions are specified`)
		}
		if len(in.ExpressionAttributeNames) == 0 {
			return nil, validationErr(`ExpressionAttributeNames must not be empty`)
		}
	}

	consume := `NONE`
	if in.ReturnConsumedCapacity != nil {
		consume = *in.ReturnConsumedCapacity
		switch consume {
		case `NONE`, `TOTAL`, `INDEXES`:
		default:
			return nil, validationErrf(`1 validation error detected: Value '%s' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]`, consume)
		}
	}

	tbl := db.table(name)
	if tbl == nil {
		return nil, &apiErr{status: 400, errCode: errCodeResourceMissing, msg: fmt.Sprintf(`Requested resource not found: Table: %s not found`, name)}
	}

	var rawKey map[string]json.RawMessage
	if err := json.Unmarshal(in.Key, &rawKey); err != nil {
		return nil, serializationErr()
	}
	provided := make(map[string]*AttributeValue, len(rawKey))
	for k, raw := range rawKey {
		av, aerr := attrFromValue(raw)
		if aerr != nil {
			return nil, aerr
		}
		provided[k] = av
	}
	canon, keyErr := tbl.validateKey(provided)
	if keyErr != nil {
		return nil, keyErr
	}

	if in.AttributesToGet != nil {
		if len(in.AttributesToGet) == 0 {
			return nil, validationErr(`One or more parameter values were invalid: List of AttibuteNames is empty`)
		}
		seen := map[string]bool{}
		for _, a := range in.AttributesToGet {
			if seen[a] {
				return nil, validationErr(`One or more parameter values were invalid: Duplicate attribute names are not allowed`)
			}
			seen[a] = true
		}
	}

	var paths []projPath
	if in.ProjectionExpression != nil {
		if len(*in.ProjectionExpression) == 0 {
			return nil, validationErr(`1 validation error detected: Value '' at 'projectionExpression' failed to satisfy constraint: Member must have length greater than or equal to 1`)
		}
		var perr *apiErr
		paths, perr = parseProjection(*in.ProjectionExpression, in.ExpressionAttributeNames)
		if perr != nil {
			return nil, perr
		}
		if overlapErr := checkOverlap(paths); overlapErr != nil {
			return nil, overlapErr
		}
	}

	consistent := in.ConsistentRead != nil && *in.ConsistentRead
	item, found := tbl.get(canon, consistent)

	out := map[string]interface{}{}
	if consume != `NONE` {
		units := capacityUnits(0.5)
		if consistent {
			units = capacityUnits(1)
		}
		out[`ConsumedCapacity`] = &consumedCapacity{TableName: name, CapacityUnits: units}
	}
	if found {
		switch {
		case in.ProjectionExpression != nil:
			out[`Item`] = projectItem(item, paths)
		case in.AttributesToGet != nil:
			out[`Item`] = projectTopLevel(item, in.AttributesToGet)
		default:
			out[`Item`] = item
		}
	}
	return out, nil
}

// processGetItem executes GetItem on the wire: HTTP status, response body and
// the short error type for the x-amzn-ErrorType header.
func processGetItem(db *DB, body []byte) (int, string, []byte) {
	out, aerr := doGetItem(db, body)
	if aerr != nil {
		return aerr.status, shortErrName(aerr.errCode), errorBody(aerr)
	}
	payload, merr := json.Marshal(out)
	if merr != nil {
		ae := serializationErr()
		return ae.status, shortErrName(ae.errCode), errorBody(ae)
	}
	return 200, ``, payload
}

func shortErrName(code string) string {
	if idx := strings.LastIndex(code, `#`); idx >= 0 && idx+1 < len(code) {
		return code[idx+1:]
	}
	return code
}

func errorBody(e *apiErr) []byte {
	var b bytes.Buffer
	b.WriteString(`{"__type":"`)
	b.WriteString(e.errCode)
	b.WriteString(`"`)
	if e.msg != `` {
		b.WriteString(`,"Message":`)
		appendQuoted(&b, e.msg)
	}
	b.WriteByte('}')
	return b.Bytes()
}

func newRequestID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + `-` + h[8:12] + `-` + h[12:16] + `-` + h[16:20] + `-` + h[20:32]
}

// ServeHTTP is the JSON wire layer: it routes on X-Amz-Target and renders
// DynamoDB-compatible envelopes (Content-Type, request id, error type).
func (db *DB) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		respondError(w, serializationErr())
		return
	}
	switch r.Header.Get(`X-Amz-Target`) {
	case targetGetItem:
		status, errType, payload := processGetItem(db, body)
		w.Header().Set(`Content-Type`, `application/x-amz-json-1.0`)
		w.Header().Set(`x-amzn-RequestId`, newRequestID())
		if errType != `` {
			w.Header().Set(`x-amzn-ErrorType`, errType)
		}
		w.WriteHeader(status)
		w.Write(payload)
	default:
		respondError(w, &apiErr{status: 400, errCode: errCodeUnknownOp})
	}
}

func respondError(w http.ResponseWriter, e *apiErr) {
	w.Header().Set(`Content-Type`, `application/x-amz-json-1.0`)
	w.Header().Set(`x-amzn-RequestId`, newRequestID())
	w.Header().Set(`x-amzn-ErrorType`, shortErrName(e.errCode))
	w.WriteHeader(e.status)
	w.Write(errorBody(e))
}

func main() {
	port := os.Getenv(`PORT`)
	if port == `` {
		port = `8000`
	}
	db := NewDB()
	fmt.Printf(`floci-go ticket-014 GetItem listening on :%s`, port)
	fmt.Println()
	if err := http.ListenAndServe(`:`+port, db); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
