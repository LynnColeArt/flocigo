// Package ddb implements a wire-compatible, in-memory subset of Amazon
// DynamoDB. This file holds the shared data model: attribute values, items,
// tables, the table store and the transaction lock registry used by
// TransactWriteItems.
package ddb

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"
)

// maxTransactItems is the DynamoDB limit on the number of actions in a
// single transaction.
const maxTransactItems = 25

const wirePrefix = "com.amazonaws.dynamodb.v20120810#"

const msgMissingAttr = "The provided expression refers to an attribute that does not exist in the item"

// APIError is a DynamoDB-style error rendered on the wire.
type APIError struct {
	Code    string
	Status  int
	Message string
	Data    map[string]any
}

func (e *APIError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return e.Code + ": " + e.Message
}

// Body renders the error as the JSON document returned by the API.
func (e *APIError) Body() []byte {
	body := map[string]any{"__type": wirePrefix + e.Code, "Message": e.Message}
	for k, v := range e.Data {
		body[k] = v
	}
	b, err := json.Marshal(body)
	if err != nil {
		b, _ = json.Marshal(map[string]any{"__type": wirePrefix + e.Code, "Message": e.Message})
	}
	return b
}

func validationErr(format string, args ...any) *APIError {
	return &APIError{Code: "ValidationException", Status: 400, Message: fmt.Sprintf(format, args...)}
}

func resourceNotFoundErr() *APIError {
	return &APIError{Code: "ResourceNotFoundException", Status: 400, Message: "Cannot do operations on a non-existent table"}
}

func internalErr(err error) *APIError {
	return &APIError{Code: "InternalServerError", Status: 500, Message: "Internal Server Error: " + err.Error()}
}

func transactionCanceledErr(reasons []*CancellationReason) *APIError {
	return &APIError{
		Code:    "TransactionCanceledException",
		Status:  400,
		Message: "Transaction cancelled, please request cancellation and retry.",
		Data:    map[string]any{"CancellationReasons": reasons},
	}
}

func conflictErr() *APIError {
	return &APIError{
		Code:    "TransactionConflictException",
		Status:  400,
		Message: "Operation was rejected because there is an ongoing transaction for the item",
	}
}

// schemaErr is reported when a key does not match the table's key schema.
func schemaErr() *APIError {
	return validationErr("The provided key element does not match the schema")
}

// CancellationReason describes why one action of a transaction was
// cancelled. Non-cancelled positions are reported as null.
type CancellationReason struct {
	Code    string                     `json:"Code"`
	Message *string                    `json:"Message"`
	Item    map[string]*AttributeValue `json:"Item,omitempty"`
}

func strPtr(s string) *string { return &s }

// AttributeValue is a DynamoDB attribute value.
type AttributeValue struct {
	NULL bool
	BOOL *bool
	N    string
	S    string
	B    []byte
	SS   []string
	NS   []string
	BS   [][]byte
	M    map[string]*AttributeValue
	L    []*AttributeValue
}

// TypeName returns the wire type tag of the value.
func (a *AttributeValue) TypeName() string {
	switch {
	case a == nil:
		return ""
	case a.NULL:
		return "NULL"
	case a.BOOL != nil:
		return "BOOL"
	case a.M != nil:
		return "M"
	case a.L != nil:
		return "L"
	case a.BS != nil:
		return "BS"
	case a.SS != nil:
		return "SS"
	case a.NS != nil:
		return "NS"
	case a.B != nil:
		return "B"
	case a.N != "":
		return "N"
	default:
		return "S"
	}
}

// MarshalJSON renders the value in DynamoDB wire format.
func (a *AttributeValue) MarshalJSON() ([]byte, error) {
	if a == nil {
		return []byte("null"), nil
	}
	var m map[string]any
	switch a.TypeName() {
	case "NULL":
		m = map[string]any{"NULL": true}
	case "BOOL":
		v := false
		if a.BOOL != nil {
			v = *a.BOOL
		}
		m = map[string]any{"BOOL": v}
	case "S":
		m = map[string]any{"S": a.S}
	case "N":
		m = map[string]any{"N": a.N}
	case "B":
		m = map[string]any{"B": base64.StdEncoding.EncodeToString(a.B)}
	case "SS":
		m = map[string]any{"SS": a.SS}
	case "NS":
		m = map[string]any{"NS": a.NS}
	case "BS":
		enc := make([]string, 0, len(a.BS))
		for _, b := range a.BS {
			enc = append(enc, base64.StdEncoding.EncodeToString(b))
		}
		m = map[string]any{"BS": enc}
	case "M":
		m = map[string]any{"M": a.M}
	case "L":
		m = map[string]any{"L": a.L}
	default:
		m = map[string]any{"S": a.S}
	}
	return json.Marshal(m)
}

// cloneValue deep-copies a value.
func cloneValue(v *AttributeValue) *AttributeValue {
	if v == nil {
		return nil
	}
	c := &AttributeValue{NULL: v.NULL, N: v.N, S: v.S}
	if v.BOOL != nil {
		b := *v.BOOL
		c.BOOL = &b
	}
	if v.B != nil {
		c.B = append([]byte(nil), v.B...)
	}
	if v.SS != nil {
		c.SS = append([]string(nil), v.SS...)
	}
	if v.NS != nil {
		c.NS = append([]string(nil), v.NS...)
	}
	if v.BS != nil {
		c.BS = make([][]byte, len(v.BS))
		for i, b := range v.BS {
			c.BS[i] = append([]byte(nil), b...)
		}
	}
	if v.M != nil {
		c.M = make(map[string]*AttributeValue, len(v.M))
		for k, mv := range v.M {
			c.M[k] = cloneValue(mv)
		}
	}
	if v.L != nil {
		c.L = make([]*AttributeValue, len(v.L))
		for i, lv := range v.L {
			c.L[i] = cloneValue(lv)
		}
	}
	return c
}

func cloneItem(item map[string]*AttributeValue) map[string]*AttributeValue {
	out := make(map[string]*AttributeValue, len(item))
	for k, v := range item {
		out[k] = cloneValue(v)
	}
	return out
}

func aN(s string) *AttributeValue { return &AttributeValue{N: s} }
func aS(s string) *AttributeValue { return &AttributeValue{S: s} }

// Numbers --------------------------------------------------------------------

func parseRat(s string) (*big.Rat, bool) {
	r := new(big.Rat)
	if _, ok := r.SetString(strings.TrimSpace(s)); !ok {
		return nil, false
	}
	return r, true
}

// canonicalNumber renders a decimal string in DynamoDB canonical form:
// no leading zeros, no trailing fraction zeros, no exponent.
func canonicalNumber(s string) string {
	s = strings.TrimSpace(s)
	r, ok := parseRat(s)
	if !ok {
		return s
	}
	return formatRat(r)
}

func formatRat(r *big.Rat) string {
	if r.Sign() == 0 {
		return "0"
	}
	den := new(big.Int).Set(r.Denom())
	two, five := big.NewInt(2), big.NewInt(5)
	a, b := 0, 0
	for new(big.Int).Mod(den, two).Sign() == 0 {
		en.Div(den, two)
		a++
	}
	for new(big.Int).Mod(den, five).Sign() == 0 {
		en.Div(den, five)
		b++
	}
	if den.IsInt64() && den.Int64() == 1 && a+b <= 32 {
		scale := a
		if b > scale {
			scale = b
		}
		ten := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
		scaled := new(big.Int).Mul(r.Num(), ten)
		scaled.Div(scaled, r.Denom())
		neg := scaled.Sign() < 0
		sd := new(big.Int).Abs(scaled).String()
		var out string
		if scale == 0 {
			out = sd
		} else {
			for len(sd) <= scale {
				sd = "0" + sd
			}
			out = sd[:len(sd)-scale] + "." + sd[len(sd)-scale:]
			out = strings.TrimRight(out, "0")
			out = strings.TrimSuffix(out, ".")
		}
		if neg && out != "0" {
			out = "-" + out
		}
		return out
	}
	out := r.FloatString(24)
	out = strings.TrimRight(out, "0")
	out = strings.TrimSuffix(out, ".")
	if out == "-0" || out == "0" {
		return "0"
	}
	return out
}

// compareNumbers compares two decimal strings numerically.
func compareNumbers(a, b string) (int, bool) {
	ra, okA := parseRat(a)
	rb, okB := parseRat(b)
	if !okA || !okB {
		return 0, false
	}
	return ra.Cmp(rb), true
}

// Value comparison -----------------------------------------------------------

func setsEqualStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	as := append([]string(nil), a...)
	bs := append([]string(nil), b...)
	sort.Strings(as)
	sort.Strings(bs)
	for i := range as {
		if as[i] != bs[i] {
			return false
		}
	}
	return true
}

func setsEqualNumbers(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	ca := make([]string, 0, len(a))
	for _, n := range a {
		ca = append(ca, canonicalNumber(n))
	}
	cb := make([]string, 0, len(b))
	for _, n := range b {
		cb = append(cb, canonicalNumber(n))
	}
	sort.Strings(ca)
	sort.Strings(cb)
	for i := range ca {
		if ca[i] != cb[i] {
			return false
		}
	}
	return true
}

func setsEqualBinary(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	am := map[string]bool{}
	for _, x := range a {
		am[base64.StdEncoding.EncodeToString(x)] = true
	}
	bm := map[string]bool{}
	for _, x := range b {
		bm[base64.StdEncoding.EncodeToString(x)] = true
	}
	if len(am) != len(bm) {
		return false
	}
	for k := range am {
		if !bm[k] {
			return false
		}
	}
	return true
}

// valuesEqual reports deep equality of two attribute values.
func valuesEqual(a, b *AttributeValue) bool {
	if a == nil || b == nil {
		return a == b
	}
	ta, tb := a.TypeName(), b.TypeName()
	if ta != tb {
		return false
	}
	switch ta {
	case "S":
		return a.S == b.S
	case "N":
		c, ok := compareNumbers(a.N, b.N)
		return ok && c == 0
	case "B":
		return bytes.Equal(a.B, b.B)
	case "BOOL":
		if a.BOOL == nil || b.BOOL == nil {
			return a.BOOL == b.BOOL
		}
		return *a.BOOL == *b.BOOL
	case "NULL":
		return true
	case "SS":
		return setsEqualStrings(a.SS, b.SS)
	case "NS":
		return setsEqualNumbers(a.NS, b.NS)
	case "BS":
		return setsEqualBinary(a.BS, b.BS)
	case "M":
		if len(a.M) != len(b.M) {
			return false
		}
		for k, av := range a.M {
			bv, ok := b.M[k]
			if !ok || !valuesEqual(av, bv) {
				return false
			}
		}
		return true
	case "L":
		if len(a.L) != len(b.L) {
			return false
		}
		for i := range a.L {
			if !valuesEqual(a.L[i], b.L[i]) {
				return false
			}
		}
		return true
	}
	return false
}

// compareValues orders two values; ok is false when they are not comparable.
func compareValues(a, b *AttributeValue) (int, bool) {
	ta, tb := a.TypeName(), b.TypeName()
	switch {
	case ta == "N" && tb == "N":
		return compareNumbers(a.N, b.N)
	case ta == "S" && tb == "S":
		return strings.Compare(a.S, b.S), true
	case ta == "B" && tb == "B":
		return bytes.Compare(a.B, b.B), true
	}
	return 0, false
}

// Keys and storage -----------------------------------------------------------

// isScalarKeyType reports whether v can be used as a key attribute.
func isScalarKeyType(v *AttributeValue) bool {
	switch v.TypeName() {
	case "S", "N", "B":
		return true
	}
	return false
}

func keyScalarString(v *AttributeValue) string {
	switch v.TypeName() {
	case "N":
		return "N:" + canonicalNumber(v.N)
	case "B":
		return "B:" + base64.StdEncoding.EncodeToString(v.B)
	default:
		return "S:" + v.S
	}
}

// EncodeKey renders the canonical string identity of an item key.
func EncodeKey(key map[string]*AttributeValue) string {
	names := make([]string, 0, len(key))
	for k := range key {
		names = append(names, k)
	}
	sort.Strings(names)
	var sb strings.Builder
	for i, n := range names {
		if i > 0 {
			sb.WriteString("\x00")
		}
		sb.WriteString(n)
		sb.WriteString("=")
		sb.WriteString(keyScalarString(key[n]))
	}
	return sb.String()
}

// Table is a single table and its items.
type Table struct {
	Name      string
	HashKey   string
	RangeKey  string
	CreatedAt time.Time

	mu    sync.Mutex
	items map[string]map[string]*AttributeValue
}

func (t *Table) keyOf(item map[string]*AttributeValue) (string, map[string]*AttributeValue, *APIError) {
	if len(item) == 0 {
		return "", nil, schemaErr()
	}
	key := map[string]*AttributeValue{}
	if h, ok := item[t.HashKey]; ok && isScalarKeyType(h) {
		key[t.HashKey] = h
	} else {
		return "", nil, schemaErr()
	}
	if t.RangeKey != "" {
		if r, ok := item[t.RangeKey]; ok && isScalarKeyType(r) {
			key[t.RangeKey] = r
		} else {
			return "", nil, schemaErr()
		}
	}
	norm := cloneItem(key)
	return EncodeKey(norm), norm, nil
}

func keyFromMap(t *Table, key map[string]*AttributeValue) (string, map[string]*AttributeValue, *APIError) {
	if len(key) == 0 {
		return "", nil, schemaErr()
	}
	expected := 1
	if t.RangeKey != "" {
		expected = 2
	}
	if len(key) != expected {
		return "", nil, schemaErr()
	}
	if _, ok := key[t.HashKey]; !ok {
		return "", nil, schemaErr()
	}
	if t.RangeKey != "" {
		if _, ok := key[t.RangeKey]; !ok {
			return "", nil, schemaErr()
		}
	}
	for k, v := range key {
		if k != t.HashKey && k != t.RangeKey {
			return "", nil, schemaErr()
		}
		if !isScalarKeyType(v) {
			return "", nil, schemaErr()
		}
	}
	norm := cloneItem(key)
	return EncodeKey(norm), norm, nil
}

// getItem returns a deep copy of the stored item.
func (t *Table) getItem(enc string) (map[string]*AttributeValue, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	it, ok := t.items[enc]
	if !ok {
		return nil, false
	}
	return cloneItem(it), true
}

func (t *Table) putItemRaw(enc string, item map[string]*AttributeValue) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.items[enc] = cloneItem(item)
}

func (t *Table) deleteItem(enc string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.items, enc)
}

// Get reads an item by key.
func (t *Table) Get(key map[string]*AttributeValue) (map[string]*AttributeValue, bool) {
	enc, _, err := keyFromMap(t, key)
	if err != nil {
		return nil, false
	}
	return t.getItem(enc)
}

// Put stores an item, deriving the key from the item itself.
func (t *Table) Put(item map[string]*AttributeValue) *APIError {
	enc, _, err := t.keyOf(item)
	if err != nil {
		return err
	}
	t.putItemRaw(enc, item)
	return nil
}

// LockKeyFor renders the global lock identity of an item.
func (t *Table) LockKeyFor(key map[string]*AttributeValue) string {
	return t.Name + "\x00" + EncodeKey(key)
}

// DB stores tables and coordinates transaction locks.
type DB struct {
	mu     sync.RWMutex
	tables map[string]*Table

	locksMu sync.Mutex
	locks   map[string]uint64
	lockSeq uint64
}

func NewDB() *DB {
	return &DB{tables: map[string]*Table{}}
}

// CreateTable creates a table with the given hash (and optional range) key.
func (db *DB) CreateTable(name, hash string, rangeKeys ...string) (*Table, *APIError) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if name == "" {
		return nil, validationErr("TableName cannot be empty")
	}
	if _, exists := db.tables[name]; exists {
		return nil, &APIError{Code: "ResourceInUseException", Status: 400, Message: "Table already exists: " + name}
	}
	t := &Table{Name: name, HashKey: hash, CreatedAt: time.Now(), items: map[string]map[string]*AttributeValue{}}
	for _, r := range rangeKeys {
		if r != "" {
			t.RangeKey = r
			break
		}
	}
	db.tables[name] = t
	return t, nil
}

// Table looks up a table by name.
func (db *DB) Table(name string) (*Table, *APIError) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	t, ok := db.tables[name]
	if !ok {
		return nil, resourceNotFoundErr()
	}
	return t, nil
}

// TxnHandle owns the item locks held by one in-flight transaction.
type TxnHandle struct {
	db *DB
	id uint64
}

// Release gives up the locks held by the transaction.
func (h *TxnHandle) Release() {
	h.db.locksMu.Lock()
	defer h.db.locksMu.Unlock()
	for k, v := range h.db.locks {
		if v == h.id {
			delete(h.db.locks, k)
		}
	}
}

// beginTxn atomically acquires all item locks or reports a conflict.
func (db *DB) beginTxn(lockKeys []string) (*TxnHandle, *APIError) {
	db.locksMu.Lock()
	defer db.locksMu.Unlock()
	if db.locks == nil {
		db.locks = map[string]uint64{}
	}
	for _, k := range lockKeys {
		if _, held := db.locks[k]; held {
			return nil, conflictErr()
		}
	}
	db.lockSeq++
	id := db.lockSeq
	for _, k := range lockKeys {
		db.locks[k] = id
	}
	return &TxnHandle{db: db, id: id}, nil
}

// AcquireItemLocks simulates a foreign in-flight transaction holding the
// given item locks; transactions that would touch those items are rejected
// with TransactionConflictException.
func (db *DB) AcquireItemLocks(lockKeys []string) (*TxnHandle, *APIError) {
	return db.beginTxn(lockKeys)
}
