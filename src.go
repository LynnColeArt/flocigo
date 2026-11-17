// Package dynamodb implements the DynamoDB-compatible Scan operation for
// floci-go (ticket flocigo/ticket-033).
//
// Behavior is specified by the Java sources (ExpressionEvaluator.java for
// condition evaluation; DynamoDbPartiQL*, DynamoDbUpdateExpressionSyntax and
// DynamoDbConditionKeys for expression grammar/placeholder rules) and by the
// DynamoDB Scan wire API (Items, Count, ScannedCount, LastEvaluatedKey,
// ConsumedCapacity, errors).
//
// Scan semantics implemented here:
//   - iterate all items of a table, which is stored as "pk equals" partitions
//     (items sharing a partition key live in one bucket); the scan walks
//     every partition in pk, then sk, order (deterministic),
//   - optional FilterExpression applied via the expression engine (legacy
//     ScanFilter operators are also supported),
//   - ConsistentRead, Limit (evaluated-item cap, i.e. applied before
//     filtering, as in DynamoDB), ExclusiveStartKey pagination,
//     ScanIndexForward ordering, Segment/TotalSegments parallel scans,
//     ProjectionExpression / AttributesToGet projection and Select=COUNT.
package dynamodb

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// errorTypePrefix matches the DynamoDB API (JSON protocol) error type prefix.
const errorTypePrefix = "com.amazonaws.dynamodb.v20120810#"

// APIError is a DynamoDB API error: HTTP status, short error code and message.
type APIError struct {
	Code    string
	Message string
	Status  int
}

func (e *APIError) Error() string { return e.Code + ": " + e.Message }

func newValidationError(format string, args ...interface{}) *APIError {
	return &APIError{Code: "ValidationException", Status: 400, Message: fmt.Sprintf(format, args...)}
}

func newResourceNotFound(table string) *APIError {
	return &APIError{
		Code:    "ResourceNotFoundException",
		Status:  400,
		Message: fmt.Sprintf("Requested resource not found: Table: %s not found", table),
	}
}

// errorPayload renders an APIError in the wire error shape.
func errorPayload(e *APIError) map[string]interface{} {
	typ := e.Code
	if !strings.HasPrefix(typ, errorTypePrefix) {
		typ = errorTypePrefix + typ
	}
	return map[string]interface{}{"__type": typ, "message": e.Message}
}

func marshalErrorPayload(e *APIError) []byte {
	b, err := json.Marshal(errorPayload(e))
	if err != nil {
		return []byte(`{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"internal error"}`)
	}
	return b
}

// ---------------------------------------------------------------------------
// Numbers (arbitrary precision decimals, as in DynamoDB Number semantics)
// ---------------------------------------------------------------------------

var numberPattern = regexp.MustCompile(`^[+-]?[0-9]+(\\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

// validateNumber mirrors the DynamoDB number format limits: at most 38
// significant digits and a bounded decimal exponent.
func validateNumber(s string) *APIError {
	if !numberPattern.MatchString(s) {
		return newValidationError("Invalid number %q", s)
	}
	mant := s
	exp := int64(0)
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		mant = s[:i]
		e := strings.TrimPrefix(s[i+1:], "+")
		n, err := strconv.ParseInt(e, 10, 64)
		if err != nil {
			return newValidationError("Invalid number %q", s)
		}
		exp = n
	}
	digits := strings.TrimPrefix(strings.TrimPrefix(mant, "+"), "-")
	digits = strings.ReplaceAll(digits, ".", "")
	digits = strings.TrimLeft(digits, "0")
	if len(digits) > 38 {
		return newValidationError("Number overflow. Attempting to store a number with more than 38 significant digits")
	}
	if exp > 1000 || exp < -1000 {
		return newValidationError("Number overflow. Attempting to store a number with an out-of-range exponent")
	}
	return nil
}

func numberRat(s string) (*big.Rat, bool) {
	r, ok := new(big.Rat).SetString(strings.TrimPrefix(s, "+"))
	if !ok {
		return nil, false
	}
	return r, true
}

// canonicalNumber renders a decimal in a scheme-independent form so that
// equal numbers (1, 1.0, 1e0) share one representation.
func canonicalNumber(s string) string {
	r, ok := numberRat(s)
	if !ok {
		return s
	}
	if r.IsInt() {
		return r.Num().String()
	}
	return r.Num().String() + "/" + r.Denom().String()
}

// ---------------------------------------------------------------------------
// Attribute values
// ---------------------------------------------------------------------------

// AttributeType is a DynamoDB attribute data type.
type AttributeType string

const (
	TypeString    AttributeType = "S"
	TypeNumber    AttributeType = "N"
	TypeBinary    AttributeType = "B"
	TypeBool      AttributeType = "BOOL"
	TypeNull      AttributeType = "NULL"
	TypeStringSet AttributeType = "SS"
	TypeNumberSet AttributeType = "NS"
	TypeBinarySet AttributeType = "BS"
	TypeMap       AttributeType = "M"
	TypeList      AttributeType = "L"
)

// AttributeValue mirrors the DynamoDB AttributeValue wire type. Exactly the
// payload selected by Type is meaningful.
type AttributeValue struct {
	Type AttributeType
	S    string
	N    string // decimal string, preserved exactly as supplied
	B    []byte
	BOOL bool
	SS   []string
	NS   []string
	BS   [][]byte
	M    map[string]*AttributeValue
	L    []*AttributeValue
}

func NewString(s string) *AttributeValue { return &AttributeValue{Type: TypeString, S: s} }
func NewNumber(n string) *AttributeValue { return &AttributeValue{Type: TypeNumber, N: n} }

func NewBinary(b []byte) *AttributeValue {
	c := append([]byte(nil), b...)
	return &AttributeValue{Type: TypeBinary, B: c}
}

func NewBool(v bool) *AttributeValue { return &AttributeValue{Type: TypeBool, BOOL: v} }

func NewNull() *Attribute_Value_placeholder { return nil } // placeholder (never used)

func NewStringSet(ss ...string) *AttributeValue {
	c := append([]string(nil), ss...)
	if c == nil {
		c = []string{}
	}
	return &AttributeValue{Type: TypeStringSet, SS: c}
}

func NewNumberSet(ns ...string) *AttributeValue {
	c := append([]string(nil), ns...)
	if c == nil {
		c = []string{}
	}
	return &AttributeValue{Type: TypeNumberSet, NS: c}
}

func NewBinarySet(bs ...[]byte) *AttributeValue {
	c := make([][]byte, 0, len(bs))
	for _, b := range bs {
		c = append(c, append([]byte(nil), b...))
	}
	if c == nil {
		c = [][]byte{}
	}
	return &AttributeValue{Type: TypeBinarySet, BS: c}
}

func NewMap(m map[string]*AttributeValue) *AttributeValue {
	if m == nil {
		m = map[string]*AttributeValue{}
	}
	return &AttributeValue{Type: TypeMap, M: m}
}

func NewList(l ...*AttributeValue) *AttributeValue {
	c := append([]*AttributeValue(nil), l...)
	if c == nil {
		c = []*AttributeValue{}
	}
	return &AttributeValue{Type: TypeList, L: c}
}

func NewNull() *AttributeValue { return &AttributeValue{Type: TypeNull, BOOL: true} }

// MarshalJSON encodes the value using the DynamoDB JSON protocol shapes.
func (v *AttributeValue) MarshalJSON() ([]byte, error) {
	if v == nil {
		return json.Marshal(map[string]interface{}{"NULL": true})
	}
	m := map[string]interface{}{}
	switch v.Type {
	case TypeString:
		m["S"] = v.S
	case TypeNumber:
		m["N"] = v.N
	case TypeBinary:
		m["B"] = base64.StdEncoding.EncodeToString(v.B)
	case TypeBool:
		m["BOOL"] = v.BOOL
	case TypeNull:
		m["NULL"] = v.BOOL
	case TypeStringSet:
		m["SS"] = v.SS
	case TypeNumberSet:
		m["NS"] = v.NS
	case TypeBinarySet:
		bs := make([]string, len(v.BS))
		for i, b := range v.BS {
			bs[i] = base64.StdEncoding.EncodeToString(b)
		}
		m["BS"] = bs
	case TypeMap:
		if v.M == nil {
			m["M"] = map[string]interface{}{}
		} else {
			m["M"] = v.M
		}
	case TypeList:
		if v.L == nil {
			m["L"] = []interface{}{}
		} else {
			m["L"] = v.L
		}
	default:
		return nil, fmt.Errorf("unknown attribute type %q", string(v.Type))
	}
	return json.Marshal(m)
}

const avSingleTypeError = "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes"

// parseAttributeValue decodes a decoded-JSON value into an AttributeValue.
func parseAttributeValue(raw interface{}) (*AttributeValue, *APIError) {
	if raw == nil {
		return nil, newValidationError(avSingleTypeError)
	}
	m, ok := raw.(map[string]interface{})
	if !ok {
		return nil, newValidationError(avSingleTypeError)
	}
	if len(m) == 0 {
		return nil, newValidationError(avSingleTypeError)
	}
	if len(m) > 1 {
		return nil, newValidationError("Supplied AttributeValue has more than one datatypes set")
	}
	for key, val := range m {
		switch key {
		case "S":
			s, ok := val.(string)
			if !ok {
				return nil, newValidationError("Supplied AttributeValue is wrong type: S must be a string")
			}
			return NewString(s), nil
		case "N":
			s, ok := val.(string)
			if !ok {
				return nil, newValidationError("Supplied AttributeValue is wrong type: N must be a string")
			}
			if aerr := validateNumber(s); aerr != nil {
				return nil, aerr
			}
			return NewNumber(s), nil
		case "B":
			s, ok := val.(string)
			if !ok {
				return nil, newValidationError("Supplied AttributeValue is wrong type: B must be a base64 string")
			}
			b, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return nil, newValidationError("Supplied AttributeValue is wrong type: B must be valid base64")
			}
			return NewBinary(b), nil
		case "BOOL":
			b, ok := val.(bool)
			if !ok {
				return nil, newValidationError("Supplied AttributeValue is wrong type: BOOL must be a bool")
			}
			return NewBool(b), nil
		case "NULL":
			b, ok := val.(bool)
			if !ok {
				return nil, newValidationError("Supplied AttributeValue is wrong type: NULL must be a bool")
			}
			return &AttributeValue{Type: TypeNull, BOOL: b}, nil
		case "SS":
			arr, ok := val.([]interface{})
			if !ok {
				return nil, newValidationError("Supplied AttributeValue is wrong type: SS must be a list of strings")
			}
			if len(arr) == 0 {
				return nil, newValidationError(avSingleTypeError)
			}
			ss := make([]string, len(arr))
			for i, e := range arr {
				s, ok := e.(string)
				if !ok {
					return nil, newValidationError("Supplied AttributeValue is wrong type: SS must be a list of strings")
				}
				ss[i] = s
			}
			return NewStringSet(ss...), nil
		case "NS":
			arr, ok := val.([]interface{})
			if !ok {
				return nil, newValidationError("Supplied AttributeValue is wrong type: NS must be a list of numbers")
			}
			if len(arr) == 0 {
				return nil, newValidationError(avSingleTypeError)
			}
			ns := make([]string, len(arr))
			for i, e := range arr {
				s, ok := e.(string)
				if !ok {
					return nil, newValidationError("Supplied AttributeValue is wrong type: NS must be a list of numbers")
				}
				if aerr := validateNumber(s); aerr != nil {
					return nil, aerr
				}
				ns[i] = s
			}
			return NewNumberSet(ns...), nil
		case "BS":
			arr, ok := val.([]interface{})
			if !ok {
				return nil, newValidationError("Supplied AttributeValue is wrong type: BS must be a list of base64 strings")
			}
			if len(arr) == 0 {
				return nil, newValidationError(avSingleTypeError)
			}
			bs := make([][]byte, len(arr))
			for i, e := range arr {
				s, ok := e.(string)
				if !ok {
					return nil, newValidationError("Supplied AttributeValue is wrong type: BS must be a list of base64 strings")
				}
				b, err := base64.StdEncoding.DecodeString(s)
				if err != nil {
					return nil, newValidationError("Supplied AttributeValue is wrong type: BS must be valid base64")
				}
				bs[i] = b
			}
			return NewBinarySet(bs...), nil
		case "M":
			mm, ok := val.(map[string]interface{})
			if !ok {
				return nil, newValidationError("Supplied AttributeValue is wrong type: M must be a map")
			}
			out := map[string]*AttributeValue{}
			for k, ev := range mm {
				av, aerr := parseAttributeValue(ev)
				if aerr != nil {
					return nil, aerr
				}
				out[k] = av
			}
			return NewMap(out), nil
		case "L":
			arr, ok := val.([]interface{})
			if !ok {
				return nil, newValidationError("Supplied AttributeValue is wrong type: L must be a list")
			}
			out := make([]*AttributeValue, 0, len(arr))
			for _, ev := range arr {
				av, aerr := parseAttributeValue(ev)
				if aerr != nil {
					return nil, aerr
				}
				out = append(out, av)
			}
			return NewList(out...), nil
		}
		return nil, newValidationError(avSingleTypeError)
	}
	return nil, newValidationError(avSingleTypeError)
}

func (v *AttributeValue) clone() *AttributeValue {
	if v == nil {
		return nil
	}
	c := &AttributeValue{Type: v.Type, S: v.S, N: v.N, BOOL: v.BOOL}
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
		c.M = map[string]*AttributeValue{}
		for k, mv := range v.M {
			c.M[k] = mv.clone()
		}
	}
	if v.L != nil {
		c.L = make([]*AttributeValue, len(v.L))
		for i, lv := range v.L {
			c.L[i] = lv.clone()
		}
	}
	return c
}

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

// byteSize approximates the DynamoDB item-size contribution of a value.
func (v *AttributeValue) byteSize() int {
	if v == nil {
		return 0
	}
	switch v.Type {
	case TypeString:
		return len(v.S)
	case TypeNumber:
		return (len(v.N) + 1) / 2
	case TypeBinary:
		return len(v.B)
	case TypeBool, TypeNull:
		return 1
	case TypeStringSet:
		n := 0
		for _, s := range v.SS {
			n += len(s)
		}
		return n
	case TypeNumberSet:
		n := 0
		for _, s := range v.NS {
			n += (len(s) + 1) / 2
		}
		return n
	case TypeBinarySet:
		n := 0
		for _, b := range v.BS {
			n += len(b)
		}
		return n
	case TypeMap:
		n := 0
		for k, mv := range v.M {
			n += len(k) + mv.byteSize()
		}
		return n
	case TypeList:
		n := 0
		for _, lv := range v.L {
			n += lv.byteSize()
		}
		return n
	}
	return 0
}

func canonSetEqual(canonical func(string) string, a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	ca := make([]string, len(a))
	cb := make([]string, len(b))
	for i, s := range a {
		ca[i] = canonical(s)
	}
	for i, s := range b {
		cb[i] = canonical(s)
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

// valueEqual reports deep equality (DynamoDB semantics: numbers compare
// numerically, sets compare order-insensitively).
func valueEqual(a, b *AttributeValue) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.Type != b.Type {
		return false
	}
	switch a.Type {
	case TypeString:
		return a.S == b.S
	case TypeNumber:
		ra, oka := numberRat(a.N)
		rb, okb := numberRat(b.N)
		if oka && okb {
			return ra.Cmp(rb) == 0
		}
		return a.N == b.N
	case TypeBinary:
		return bytes.Equal(a.B, b.B)
	case TypeBool:
		return a.BOOL == b.BOOL
	case TypeNull:
		return true
	case TypeStringSet:
		return canonSetEqual(func(s string) string { return s }, a.SS, b.SS)
	case TypeNumberSet:
		return canonSetEqual(canonicalNumber, a.NS, b.NS)
	case TypeBinarySet:
		if len(a.BS) != len(b.BS) {
			return false
		}
		ca := make([]string, len(a.BS))
		cb := make([]string, len(b.BS))
		for i := range a.BS {
			ca[i] = b64(a.BS[i])
		}
		for i := range b.BS {
			cb[i] = b64(b.BS[i])
		}
		sort.Strings(ca)
		sort.Strings(cb)
		for i := range ca {
			if ca[i] != cb[i] {
				return false
			}
		}
		return true
	case TypeMap:
		if len(a.M) != len(b.M) {
			return false
		}
		for k, av := range a.M {
			bv, ok := b.M[k]
			if !ok || !valueEqual(av, bv) {
				return false
			}
		}
		return true
	case TypeList:
		if len(a.L) != len(b.L) {
			return false
		}
		for i := range a.L {
			if !valueEqual(a.L[i], b.L[i]) {
				return false
			}
		}
		return true
	}
	return false
}

// compareValues orders two values of the same DynamoDB orderable type (S, N,
// B). ok=false when the types differ or the type has no ordering.
func compareValues(a, b *AttributeValue) (int, bool) {
	if a == nil || b == nil {
		return 0, false
	}
	if a.Type != b.Type {
		return 0, false
	}
	switch a.Type {
	case TypeString:
		return strings.Compare(a.S, b.S), true
	case TypeNumber:
		ra, oka := numberRat(a.N)
		rb, okb := numberRat(b.N)
		if !oka || !okb {
			return 0, false
		}
		return ra.Cmp(rb), true
	case TypeBinary:
		return bytes.Compare(a.B, b.B), true
	}
	return 0, false
}

// canonicalRepr renders a deterministic canonical representation (used for
// total ordering of arbitrary values, including key attributes).
func (v *AttributeValue) canonicalRepr() string {
	if v == nil {
		return "null"
	}
	switch v.Type {
	case TypeString:
		return v.S
	case TypeNumber:
		return canonicalNumber(v.N)
	case TypeBinary:
		return b64(v.B)
	case TypeBool:
		if v.BOOL {
			return "true"
		}
		return "false"
	case TypeNull:
		return "null"
	case TypeStringSet:
		ss := append([]string(nil), v.SS...)
		sort.Strings(ss)
		return "SS{" + strings.Join(ss, ",") + "}"
	case TypeNumberSet:
		ns := make([]string, 0, len(v.NS))
		for _, n := range v.NS {
			ns = append(ns, canonicalNumber(n))
		}
		sort.Strings(ns)
		return "NS{" + strings.Join(ns, ",") + "}"
	case TypeBinarySet:
		bs := make([]string, 0, len(v.BS))
		for _, b := range v.BS {
			bs = append(bs, b64(b))
		}
		sort.Strings(bs)
		return "BS{" + strings.Join(bs, ",") + "}"
	case TypeList:
		parts := make([]string, 0, len(v.L))
		for _, e := range v.L {
			parts = append(parts, e.canonicalRepr())
		}
		return "[" + strings.Join(parts, ",") + "]"
	case TypeMap:
		keys := make([]string, 0, len(v.M))
		for k := range v.M {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, k+"="+v.M[k].canonicalRepr())
		}
		return "{" + strings.Join(parts, ",") + "}"
	}
	return ""
}

// totalCompare gives a deterministic total order over any two values (used
// for scan ordering and ExclusiveStartKey positioning).
func totalCompare(a, b *AttributeValue) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	if c, ok := compareValues(a, b); ok {
		return c
	}
	if a.Type != b.Type {
		if c := strings.Compare(string(a.Type), string(b.Type)); c != 0 {
			return c
		}
	}
	return strings.Compare(a.canonicalRepr(), b.canonicalRepr())
}

// ---------------------------------------------------------------------------
// Document paths and projection
// ---------------------------------------------------------------------------

type PathSegment struct {
	Name    string // attribute name (when !IsIndex)
	Index   int    // list index (when IsIndex)
	IsIndex bool
}

type DocPath struct {
	Segments []PathSegment
}

func (p *DocPath) String() string {
	var sb strings.Builder
	for i, s := range p.Segments {
		if s.IsIndex {
			fmt.Fprintf(&sb, "[%d]", s.Index)
		} else {
			if i > 0 {
				sb.WriteByte('.')
			}
			sb.WriteString(s.Name)
		}
	}
	return sb.String()
}

// resolvePath navigates item along path. ok=false when a step is missing or
// invalid: per DynamoDB, evaluating an invalid/nonexistent path makes the
// condition false instead of failing the request.
func resolvePath(item Item, p *DocPath) (*AttributeValue, bool) {
	if p == nil || len(p.Segments) == 0 {
		return nil, false
	}
	first := p.Segments[0]
	if first.IsIndex {
		return nil, false
	}
	cur, ok := item[first.Name]
	if !ok || cur == nil {
		return nil, false
	}
	for _, s := range p.Segments[1:] {
		if s.IsIndex {
			if cur.Type != TypeList {
				return nil, false
			}
			if s.Index < 0 || s.Index >= len(cur.L) {
				return nil, false
			}
			cur = cur.L[s.Index]
		} else {
			if cur.Type != TypeMap {
				return nil, false
			}
			child, exists := cur.M[s.Name]
			if !exists {
				return nil, false
			}
			cur = child
		}
		if cur == nil {
			return nil, false
		}
	}
	return cur, true
}

// docPathOverlap reports whether one path is a prefix of (or equal to) the
// other (DynamoDB rejects overlapping projection paths).
func docPathOverlap(a, b *DocPath) bool {
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

func projectItem(item Item, paths []*DocPath) Item {
	out := Item{}
	for _, p := range paths {
		v, ok := resolvePath(item, p)
		if !ok {
			continue
		}
		putPath(out, p, v)
	}
	return out
}

func putPath(out Item, path *DocPath, v *AttributeValue) {
	segs := path.Segments
	if len(segs) == 0 {
		return
	}
	name := segs[0].Name
	if len(segs) == 1 {
		out[name] = v.clone()
		return
	}
	existing, exists := out[name]
	if !exists {
		existing = containerFor(segs[1])
	}
	if insertValue(existing, segs[1:], v) {
		out[name] = existing
	}
}

func containerFor(next PathSegment) *AttributeValue {
	if next.IsIndex {
		return NewList()
	}
	return NewMap(map[string]*AttributeValue{})
}

func insertValue(container *AttributeValue, segs []PathSegment, v *AttributeValue) bool {
	s := segs[0]
	if s.IsIndex {
		if container.Type != TypeList {
			return false
		}
		for len(container.L) <= s.Index {
			container.L = append(container.L, NewNull())
		}
		if len(segs) == 1 {
			container.L[s.Index] = v.clone()
			return true
		}
		child := container.L[s.Index]
		if child == nil || child.Type == TypeNull {
			child = containerFor(segs[1])
			container.L[s.Index] = child
		}
		return insertValue(child, segs[1:], v)
	}
	if container.Type != TypeMap {
		return false
	}
	child, exists := container.M[s.Name]
	if len(segs) == 1 {
		container.M[s.Name] = v.clone()
		return true
	}
	if !exists || child == nil {
		child = containerFor(segs[1])
	}
	if !insertValue(child, segs[1:], v) {
		return false
	}
	container.M[s.Name] = child
	return true
}

// ---------------------------------------------------------------------------
// Expression tokens / lexer (port of the DynamoDB condition grammar)
// ---------------------------------------------------------------------------

type tokenKind int

const (
	tokenEOF tokenKind = iota
	tokenIdent
	tokenValueRef
	tokenNameRef
	tokenNumber
	tokenLParen
	tokenRParen
	tokenComma
	tokenDot
	tokenLBracket
	tokenRBracket
	tokenOperator
	tokenBad
)

type token struct {
	kind tokenKind
	text string
	pos  int
}

func isIdentByte(c byte) bool {
	return c == '_' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentStartByte(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func lexExpression(src string) []token {
	var out []token
	i := 0
	for i < len(src) {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '#' || c == ':':
			j := i + 1
			for j < len(src) && isIdentByte(src[j]) {
				j++
			}
			kind := tokenNameRef
			if c == ':' {
				kind = tokenValueRef
			}
			out = append(out, token{kind, src[i:j], i})
			i = j
		case isIdentStartByte(c):
			j := i
			for j < len(src) && isIdentByte(src[j]) {
				j++
			}
			out = append(out, token{tokenIdent, src[i:j], i})
			i = j
		case c >= '0' && c <= '9':
			j := i
			for j < len(src) && src[j] >= '0' && src[j] <= '9' {
				j++
			}
			out = append(out, token{tokenNumber, src[i:j], i})
			i = j
		case c == '(':
			out = append(out, token{tokenLParen, "(", i})
			i++
		case c == ')':
			out = append(out, token{tokenRParen, ")", i})
			i++
		case c == ',':
			out = append(out, token{tokenComma, ",", i})
			i++
		case c == '.':
			out = append(out, token{tokenDot, ".", i})
			i++
		case c == '[':
			out = append(out, token{tokenLBracket, "[", i})
			i++
		case c == ']':
			out = append(out, token{tokenRBracket, "]", i})
			i++
		case c == '<':
			if i+1 < len(src) && (src[i+1] == '=' || src[i+1] == '>') {
				out = append(out, token{tokenOperator, src[i : i+2], i})
				i += 2
			} else {
				out = append(out, token{tokenOperator, "<", i})
				i++
			}
		case c == '>':
			if i+1 < len(src) && src[i+1] == '=' {
				out = append(out, token{tokenOperator, ">=", i})
				i += 2
			} else {
				out = append(out, token{tokenOperator, ">", i})
				i++
			}
		case c == '=':
			out = append(out, token{tokenOperator, "=", i})
			i++
		default:
			out = append(out, token{tokenBad, string(c), i})
			i++
		}
	}
	out = append(out, token{tokenEOF, "", len(src)})
	return out
}

// ---------------------------------------------------------------------------
// Expression AST + parser
// ---------------------------------------------------------------------------

type operandKind int

const (
	operandValue operandKind = iota
	operandPath
	operandSize
)

type operand struct {
	kind  operandKind
	value *AttributeValue
	path  *DocPath
	inner *operand
}

type condition interface{}

type orCondition struct{ subs []condition }
type andCondition struct{ subs []condition }
type notCondition struct{ sub condition }
type comparisonCondition struct {
	op          string
	left, right *operand
}
type betweenCondition struct{ value, low, high *operand }
type inCondition struct {
	value *operand
	list  []*operand
}
type functionCondition struct {
	name string
	args []*operand
}

const (
	fnSize             = "size"
	fnAttributeExists  = "attribute_exists"
	fnAttributeNotExists = "attribute_not_exists"
	fnBeginsWith       = "begins_with"
	fnContains         = "contains"
	fnAttributeType    = "attribute_type"
)

var conditionFunctions = map[string]bool{
	fnAttributeExists:   true,
	fnAttributeNotExists: true,
	fnBeginsWith:        true,
	fnContains:          true,
	fnAttributeType:     true,
}

type parsedCond struct {
	root       condition
	usedNames  map[string]bool
	usedValues map[string]bool
}

type exprParser struct {
	src        string
	prefix     string
	toks       []token
	i          int
	names      map[string]string
	values     map[string]*AttributeValue
	usedNames  map[string]bool
	usedValues map[string]bool
}

func newExprParser(src, prefix string, names map[string]string, values map[string]*AttributeValue) *exprParser {
	return &exprParser{
		src:        src,
		prefix:     prefix,
		toks:       lexExpression(src),
		names:      names,
		values:     values,
		usedNames:  map[string]bool{},
		usedValues: map[string]bool{},
	}
}

func (p *exprParser) peek() token { return p.toks[p.i] }

func (p *exprParser) peekAt(k int) token {
	if p.i+k < len(p.toks) {
		return p.toks[p.i+k]
	}
	return p.toks[len(p.toks)-1]
}

func (p *exprParser) next() token {
	t := p.toks[p.i]
	if p.i < len(p.toks)-1 {
		p.i++
	}
	return t
}

func (p *exprParser) syntaxErr() *APIError {
	t := p.peek()
	near := ""
	if t.pos <= len(p.src) {
		near = p.src[t.pos:]
	}
	return newValidationError("%sSyntax error; token: %q, near: %q", p.prefix, t.text, near)
}

func (p *exprParser) undefinedValue(name string) *APIError {
	return newValidationError("%sAn expression attribute value used in expression is not defined; attribute value: %s", p.prefix, name)
}

func (p *exprParser) undefinedName(name string) *APIError {
	return newValidationError("%sAn expression attribute name used in expression is not defined; attribute name: %s", p.prefix, name)
}

func (p *exprParser) reservedAttr(name string) *APIError {
	return newValidationError("%sAttribute name is a reserved keyword; reserved keyword: %s", p.prefix, name)
}

func isKeyword(tk token, kw string) bool {
	return tk.kind == tokenIdent && strings.EqualFold(tk.text, kw)
}

func (p *exprParser) parseCondition() (condition, *APIError) { return p.parseOr() }

func (p *exprParser) parseOr() (condition, *APIError) {
	first, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	subs := []condition{first}
	for isKeyword(p.peek(), "OR") {
		p.next()
		sub, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		subs = append(subs, sub)
	}
	if len(subs) == 1 {
		return subs[0], nil
	}
	return orCondition{subs: subs}, nil
}

func (p *exprParser) parseAnd() (condition, *APIError) {
	first, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	subs := []condition{first}
	for isKeyword(p.peek(), "AND") {
		p.next()
		sub, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		subs = append(subs, sub)
	}
	if len(subs) == 1 {
		return subs[0], nil
	}
	return andCondition{subs: subs}, nil
}

func (p *exprParser) parseNot() (condition, *APIError) {
	if isKeyword(p.peek(), "NOT") {
		p.next()
		sub, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return notCondition{sub: sub}, nil
	}
	return p.parsePrimary()
}

func (p *exprParser) parsePrimary() (condition, *APIError) {
	if p.peek().kind == tokenLParen {
		p.next()
		inner, err := p.parseCondition()
		if err != nil {
			return nil, err
		}
		if p.peek().kind != tokenRParen {
			return nil, p.syntaxErr()
		}
		p.next()
		return inner, nil
	}
	if p.peek().kind == tokenIdent {
		low := strings.ToLower(p.peek().text)
		if conditionFunctions[low] && p.peekAt(1).kind == tokenLParen {
			return p.parseFunctionCondition()
		}
	}
	left, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	nx := p.peek()
	if nx.kind == tokenOperator {
		p.next()
		right, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return comparisonCondition{op: nx.text, left: left, right: right}, nil
	}
	if isKeyword(nx, "BETWEEN") {
		p.next()
		low, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		if !isKeyword(p.peek(), "AND") {
			return nil, p.syntaxErr()
		}
		p.next()
		high, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return betweenCondition{value: left, low: low, high: high}, nil
	}
	if isKeyword(nx, "IN") {
		p.next()
		if p.peek().kind != tokenLParen {
			return nil, p.syntaxErr()
		}
		p.next()
		var list []*operand
		first, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		list = append(list, first)
		for p.peek().kind == tokenComma {
			p.next()
			item, err := p.parseOperand()
			if err != nil {
				return nil, err
			}
			list = append(list, item)
		}
		if p.peek().kind != tokenRParen {
			return nil, p.syntaxErr()
		}
		p.next()
		return inCondition{value: left, list: list}, nil
	}
	return nil, p.syntaxErr()
}

func (p *exprParser) parseFunctionCondition() (condition, *APIError) {
	name := strings.ToLower(p.next().text)
	p.next() // (
	var args []*operand
	switch name {
	case fnAttributeExists, fnAttributeNotExists:
		arg, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		if arg.kind != operandPath {
			return nil, newValidationError("%sThe first argument of a %s() function must be a document path", p.prefix, name)
		}
		args = []*operand{arg}
	case fnBeginsWith, fnContains, fnAttributeType:
		arg, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		if arg.kind != operandPath {
			return nil, newValidationError("%sThe first argument of a %s() function must be a document path", p.prefix, name)
		}
		if p.peek().kind != tokenComma {
			return nil, p.syntaxErr()
		}
		p.next()
		second, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		args = []*operand{arg, second}
	default:
		return nil, p.syntaxErr()
	}
	if p.peek().kind != tokenRParen {
		return nil, p.syntaxErr()
	}
	p.next()
	return functionCondition{name: name, args: args}, nil
}

func (p *exprParser) parseOperand() (*operand, *APIError) {
	tk := p.peek()
	switch tk.kind {
	case tokenValueRef:
		p.next()
		key := tk.text[1:]
		v, ok := p.values[key]
		if !ok {
			return nil, p.undefinedValue(tk.text)
		}
		p.usedValues[key] = true
		return &operand{kind: operandValue, value: v}, nil
	case tokenNameRef:
		p.next()
		key := tk.text[1:]
		name, ok := p.names[key]
		if !ok {
			return nil, p.undefinedName(tk.text)
		}
		p.usedNames[key] = true
		path, err := p.parsePathFrom(DocPath{Segments: []PathSegment{{Name: name}}})
		if err != nil {
			return nil, err
		}
		return &operand{kind: operandPath, path: path}, nil
	case tokenIdent:
		text := tk.text
		if strings.EqualFold(text, fnSize) && p.peekAt(1).kind == tokenLParen {
			p.next()
			p.next()
			inner, err := p.parseOperand()
			if err != nil {
				return nil, err
			}
			if p.peek().kind != tokenRParen {
				return nil, p.syntaxErr()
			}
			p.next()
			return &operand{kind: operandSize, inner: inner}, nil
		}
		p.next()
		if reservedWords[strings.ToUpper(text)] {
			return nil, p.reservedAttr(text)
		}
		path, err := p.parsePathFrom(DocPath{Segments: []PathSegment{{Name: text}}})
		if err != nil {
			return nil, err
		}
		return &operand{kind: operandPath, path: path}, nil
	default:
		return nil, p.syntaxErr()
	}
}

func (p *exprParser) parsePathFrom(base DocPath) (*DocPath, *APIError) {
	path := base
	for {
		switch p.peek().kind {
		case tokenDot:
			p.next()
			nt := p.peek()
			switch nt.kind {
			case tokenIdent:
				if reservedWords[strings.ToUpper(nt.text)] {
					return nil, p.reservedAttr(nt.text)
				}
				p.next()
				path.Segments = append(path.Segments, PathSegment{Name: nt.text})
			case tokenNameRef:
				p.next()
				key := nt.text[1:]
				name, ok := p.names[key]
				if !ok {
					return nil, p.undefinedName(nt.text)
				}
				p.usedNames[key] = true
				path.Segments = append(path.Segments, PathSegment{Name: name})
			default:
				return nil, p.syntaxErr()
			}
		case tokenLBracket:
			p.next()
			nt := p.peek()
			if nt.kind != tokenNumber {
				return nil, p.syntaxErr()
			}
			p.next()
			idx, err := strconv.Atoi(nt.text)
			if err != nil || idx < 0 {
				return nil, p.syntaxErr()
			}
			if p.peek().kind != tokenRBracket {
				return nil, p.syntaxErr()
			}
			p.next()
			path.Segments = append(path.Segments, PathSegment{Index: idx, IsIndex: true})
		default:
			return &path, nil
		}
	}
}

// parseFilterExpression parses a DynamoDB FilterExpression.
func parseFilterExpression(expr string, names map[string]string, values map[string]*AttributeValue) (*parsedCond, *APIError) {
	p := newExprParser(expr, "Invalid FilterExpression: ", names, values)
	root, err := p.parseCondition()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tokenEOF {
		return nil, p.syntaxErr()
	}
	return &parsedCond{root: root, usedNames: p.usedNames, usedValues: p.usedValues}, nil
}

// parseProjectionExpression parses a comma separated list of document paths.
func parseProjectionExpression(expr string, names map[string]string) ([]*DocPath, map[string]bool, *APIError) {
	p := newExprParser(expr, "Invalid ProjectionExpression: ", names, nil)
	var paths []*DocPath
	first := true
	for {
		if first && p.peek().kind == tokenEOF {
			return nil, nil, p.syntaxErr()
		}
		var base *DocPath
		switch p.peek().kind {
		case tokenIdent:
			nt := p.peek()
			if reservedWords[strings.ToUpper(nt.text)] {
				return nil, nil, p.reservedAttr(nt.text)
			}
			p.next()
			base = &DocPath{Segments: []PathSegment{{Name: nt.text}}}
		case tokenNameRef:
			p.next()
			key := p.toks[p.i-1].text[1:]
			name, ok := p.names[key]
			if !ok {
				return nil, nil, p.undefinedName("#" + key)
			}
			p.usedNames[key] = true
			base = &DocPath{Segments: []PathSegment{{Name: name}}}
		case tokenValueRef:
			return nil, nil, newValidationError("Invalid ProjectionExpression: ExpressionAttributeValues are not supported in ProjectionExpression")
		default:
			return nil, nil, p.syntaxErr()
		}
		path, err := p.parsePathFrom(*base)
		if err != nil {
			return nil, nil, err
		}
		paths = append(paths, path)
		first = false
		if p.peek().kind == tokenComma {
			p.next()
			continue
		}
		if p.peek().kind == tokenEOF {
			break
		}
		return nil, nil, p.syntaxErr()
	}
	used := make(map[string]bool, len(p.usedNames))
	for k := range p.usedNames {
		used[k] = true
	}
	return paths, used, nil
}

// ---------------------------------------------------------------------------
// Condition evaluation (behavioral port of ExpressionEvaluator.java)
// ---------------------------------------------------------------------------

func (o *operand) resolve(item Item) (*AttributeValue, bool) {
	switch o.kind {
	case operandValue:
		return o.value, true
	case operandPath:
		return resolvePath(item, o.path)
	case operandSize:
		inner, ok := o.inner.resolve(item)
		if !ok {
			return nil, false
		}
		n := sizeOfValue(inner)
		if n < 0 {
			return nil, false
		}
		return NewNumber(strconv.Itoa(n)), true
	}
	return nil, false
}

// sizeOfValue returns the DynamoDB size() of a value; -1 for BOOL/NULL.
func sizeOfValue(v *AttributeValue) int {
	if v == nil {
		return -1
	}
	switch v.Type {
	case TypeString:
		return len(v.S)
	case TypeNumber:
		return (len(v.N) + 1) / 2
	case TypeBinary:
		return len(v.B)
	case TypeStringSet:
		return len(v.SS)
	case TypeNumberSet:
		return len(v.NS)
	case TypeBinarySet:
		return len(v.BS)
	case TypeMap:
		return len(v.M)
	case TypeList:
		return len(v.L)
	}
	return -1
}

func evalComparison(op string, l, r *AttributeValue) bool {
	switch op {
	case "=":
		return valueEqual(l, r)
	case "<>":
		return !valueEqual(l, r)
	}
	c, ok := compareValues(l, r)
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

func attributeContains(container, operand *AttributeValue) bool {
	if container == nil || operand == nil {
		return false
	}
	switch container.Type {
	case TypeString:
		return operand.Type == TypeString && strings.Contains(container.S, operand.S)
	case TypeStringSet:
		if operand.Type != TypeString {
			return false
		}
		for _, s := range container.SS {
			if s == operand.S {
				return true
			}
		}
		return false
	case TypeNumberSet:
		if operand.Type != TypeNumber {
			return false
		}
		ro, ok := numberRat(operand.N)
		if !ok {
			return false
		}
		for _, n := range container.NS {
			if rn, ok := numberRat(n); ok && rn.Cmp(ro) == 0 {
				return true
			}
		}
		return false
	case TypeBinarySet:
		if operand.Type != TypeBinary {
			return false
		}
		for _, b := range container.BS {
			if bytes.Equal(b, operand.B) {
				return true
			}
		}
		return false
	case TypeList:
		for _, e := range container.L {
			if valueEqual(e, operand) {
				return true
			}
		}
		return false
	}
	return false
}

func evalCondition(cond condition, item Item) bool {
	switch c := cond.(type) {
	case orCondition:
		for _, s := range c.subs {
			if evalCondition(s, item) {
				return true
			}
		}
		return false
	case andCondition:
		for _, s := range c.subs {
			if !evalCondition(s, item) {
				return false
			}
		}
		return true
	case notCondition:
		return !evalCondition(c.sub, item)
	case comparisonCondition:
		l, okL := c.left.resolve(item)
		r, okR := c.right.resolve(item)
		if !okL || !okR {
			return false
		}
		return evalComparison(c.op, l, r)
	case betweenCondition:
		v, okV := c.value.resolve(item)
		lo, okLo := c.low.resolve(item)
		hi, okHi := c.high.resolve(item)
		if !okV || !okLo || !okHi {
			return false
		}
		c1, ok1 := compareValues(v, lo)
		c2, ok2 := compareValues(v, hi)
		return ok1 && ok2 && c1 >= 0 && c2 <= 0
	case inCondition:
		v, ok := c.value.resolve(item)
		if !ok {
			return false
		}
		for _, o := range c.list {
			if e, ok := o.resolve(item); ok && valueEqual(v, e) {
				return true
			}
		}
		return false
	case functionCondition:
		switch c.name {
		case fnAttributeExists:
			_, ok := c.args[0].resolve(item)
			return ok
		case fnAttributeNotExists:
			_, ok := c.args[0].resolve(item)
			return !ok
		case fnBeginsWith:
			v, okv := c.args[0].resolve(item)
			s, oks := c.args[1].resolve(item)
			if !okv || !oks {
				return false
			}
			if v.Type == TypeString && s.Type == TypeString {
				return strings.HasPrefix(v.S, s.S)
			}
			if v.Type == TypeBinary && s.Type == TypeBinary {
				return bytes.HasPrefix(v.B, s.B)
			}
			return false
		case fnContains:
			v, okv := c.args[0].resolve(item)
			s, oks := c.args[1].resolve(item)
			if !okv || !oks {
				return false
			}
			return attributeContains(v, s)
		case fnAttributeType:
			v, okv := c.args[0].resolve(item)
			s, oks := c.args[1].resolve(item)
			if !okv || !oks {
				return false
			}
			return s.Type == TypeString && string(v.Type) == s.S
		}
		return false
	}
	return false
}

// ---------------------------------------------------------------------------
// Legacy ScanFilter conversion
// ---------------------------------------------------------------------------

func valueOperand(v *AttributeValue) *operand { return &operand{kind: operandValue, value: v} }

func parseLegacyScanFilter(raw interface{}, conditionalOperator string) (condition, *APIError) {
	m, ok := raw.(map[string]interface{})
	if !ok {
		return nil, newValidationError("One or more parameter values were invalid: ScanFilter must be a map")
	}
	var conds []condition
	for attrName, specRaw := range m {
		spec, ok := specRaw.(map[string]interface{})
		if !ok {
			return nil, newValidationError("One or more parameter values were invalid: ScanFilter entries must be maps")
		}
		opStr, _ := spec["ComparisonOperator"].(string)
		listRaw, _ := spec["AttributeValueList"].([]interface{})
		var vals []*AttributeValue
		for _, vr := range listRaw {
			av, aerr := parseAttributeValue(vr)
			if aerr != nil {
				return nil, aerr
			}
			vals = append(vals, av)
		}
		c, aerr := legacyCondition(attrName, opStr, vals)
		if aerr != nil {
			return nil, aerr
		}
		conds = append(conds, c)
	}
	if len(conds) == 0 {
		return nil, nil
	}
	if conditionalOperator == "OR" {
		return orCondition{subs: conds}, nil
	}
	return andCondition{subs: conds}, nil
}

func legacyCondition(attr, op string, vals []*AttributeValue) (condition, *APIError) {
	arityErr := func(n int) *APIError {
		return newValidationError("One or more parameter values were invalid: The ComparisonOperator %s requires exactly %d AttributeValue(s) to be specified", op, n)
	}
	op = strings.ToUpper(op)
	path := &DocPath{Segments: []PathSegment{{Name: attr}}}
	lop := &operand{kind: operandPath, path: path}
	switch op {
	case "EQ", "NE", "LE", "LT", "GE", "GT":
		if len(vals) != 1 {
			return nil, arityErr(1)
		}
		var cmp string
		switch op {
		case "EQ":
			cmp = "="
		case "NE":
			cmp = "<>"
		case "LE":
			cmp = "<="
		case "LT":
			cmp = "<"
		case "GE":
			cmp = ">="
		case "GT":
			cmp = ">"
		}
		return comparisonCondition{op: cmp, left: lop, right: valueOperand(vals[0])}, nil
	case "NULL":
		if len(vals) != 0 {
			return nil, arityErr(0)
		}
		return functionCondition{name: fnAttributeNotExists, args: []*operand{lop}}, nil
	case "NOT_NULL":
		if len(vals) != 0 {
			return nil, arityErr(0)
		}
		return functionCondition{name: fnAttributeExists, args: []*operand{lop}}, nil
	case "CONTAINS", "NOT_CONTAINS":
		if len(vals) != 1 {
			return nil, arityErr(1)
		}
		fn := functionCondition{name: fnContains, args: []*operand{lop, valueOperand(vals[0])}}
		if op == "NOT_CONTAINS" {
			return notCondition{sub: fn}, nil
		}
		return fn, nil
	case "BEGINS_WITH":
		if len(vals) != 1 {
			return nil, arityErr(1)
		}
		return functionCondition{name: fnBeginsWith, args: []*operand{lop, valueOperand(vals[0])}}, nil
	case "IN":
		if len(vals) == 0 {
			return nil, newValidationError("One or more parameter values were invalid: The ComparisonOperator IN requires at least one AttributeValue")
		}
		list := make([]*operand, len(vals))
		for i, v := range vals {
			list[i] = valueOperand(v)
		}
		return inCondition{value: lop, list: list}, nil
	case "BETWEEN":
		if len(vals) != 2 {
			return nil, arityErr(2)
		}
		return betweenCondition{value: lop, low: valueOperand(vals[0]), high: valueOperand(vals[1])}, nil
	}
	return nil, newValidationError("One or more parameter values were invalid: Invalid ComparisonOperator: %s", op)
}

// ---------------------------------------------------------------------------
// Table storage: items partitioned by partition-key equality ("pk equals")
// ---------------------------------------------------------------------------

// Item is one table item: attribute name -> value.
type Item map[string]*AttributeValue

// Table stores items in partition-key buckets: all items whose partition key
// is equal live in one bucket; within a bucket items are addressed by sort
// key (if the table has one). Scans iterate every bucket.
type Table struct {
	Name         string
	PartitionKey string
	SortKey      string

	pkType     AttributeType
	skType     AttributeType
	partitions map[string]map[string]Item
}

func NewTable(name, partitionKey, sortKey string) *Table {
	return &Table{
		Name:         name,
		PartitionKey: partitionKey,
		SortKey:      sortKey,
		partitions:   map[string]map[string]Item{},
	}
}

// Registry holds the tables known to the emulator.
type Registry struct {
	Tables map[string]*Table
}

func NewRegistry() *Registry { return &Registry{Tables: map[string]*Table{}} }

func (r *Registry) AddTable(t *Table) { r.Tables[t.Name] = t }

func (r *Registry) GetTable(name string) *Table { return r.Tables[name] }

// DefaultRegistry is used by HandleScan.
var DefaultRegistry = NewRegistry()

func keyCanonicalString(v *AttributeValue) string {
	switch v.Type {
	case TypeNumber:
		return canonicalNumber(v.N)
	case TypeBinary:
		return b64(v.B)
	default:
		return v.S
	}
}

func (t *Table) PutItem(item Item) *APIError {
	pkv, ok := item[t.PartitionKey]
	if !ok || pkv == nil {
		return newValidationError("One or more parameter values were invalid: Missing the key %s in the item", t.PartitionKey)
	}
	switch pkv.Type {
	case TypeString, TypeNumber, TypeBinary:
	default:
		return newValidationError("One or more parameter values were invalid: The partition key %s must be a scalar attribute value", t.PartitionKey)
	}
	if t.pkType == "" {
		t.pkType = pkv.Type
	} else if t.pkType != pkv.Type {
		return newValidationError("One or more parameter values were invalid: The partition key %s has a different type than previous items", t.PartitionKey)
	}
	skc := ""
	if t.SortKey != "" {
		skv, ok := item[t.SortKey]
		if !ok || skv == nil {
			return newValidationError("One or more parameter values were invalid: Missing the key %s in the item", t.SortKey)
		}
		switch skv.Type {
		case TypeString, TypeNumber, TypeBinary:
		default:
			return newValidationError("One or more parameter values were invalid: The sort key %s must be a scalar attribute value", t.SortKey)
		}
		if t.skType == "" {
			t.skType = skv.Type
		} else if t.skType != skv.Type {
			return newValidationError("One or more parameter values were invalid: The sort key %s has a different type than previous items", t.SortKey)
		}
		skc = keyCanonicalString(skv)
	}
	pkc := keyCanonicalString(pkv)
	bucket, ok := t.partitions[pkc]
	if !ok {
		bucket = map[string]Item{}
		t.partitions[pkc] = bucket
	}
	bucket[skc] = item
	return nil
}

type scanEntry struct {
	item Item
	pk   *AttributeValue
	sk   *AttributeValue
}

func compareKeyStrings(a, b string, typ AttributeType) int {
	switch typ {
	case TypeNumber:
		ra, oka := numberRat(a)
		rb, okb := numberRat(b)
		if oka && okb {
			return ra.Cmp(rb)
		}
	case TypeBinary:
		ba, erra := base64.StdEncoding.DecodeString(a)
		bb, errb := base64.StdEncoding.DecodeString(b)
		if erra == nil && errb == nil {
			return bytes.Compare(ba, bb)
		}
	}
	return strings.Compare(a, b)
}

func sortKeysByType(keys []string, typ AttributeType) {
	sort.Slice(keys, func(i, j int) bool {
		return compareKeyStrings(keys[i], keys[j], typ) < 0
	})
}

// sortedEntries returns all items in deterministic ascending (pk, sk) order.
func (t *Table) sortedEntries() []*scanEntry {
	var out []*scanEntry
	pks := make([]string, 0, len(t.partitions))
	for pk := range t.partitions {
		pks = append(pks, pk)
	}
	sortKeysByType(pks, t.pkType)
	for _, pk := range pks {
		bucket := t.partitions[pk]
		sks := make([]string, 0, len(bucket))
		for sk := range bucket {
			sks = append(sks, sk)
		}
		sortKeysByType(sks, t.skType)
		for _, sk := range sks {
			it := bucket[sk]
			entry := &scanEntry{item: it, pk: it[t.PartitionKey]}
			if t.SortKey != "" {
				entry.sk = it[t.SortKey]
			}
			out = append(out, entry)
		}
	}
	return out
}

func (t *Table) keyOf(e *scanEntry) Item {
	key := Item{t.PartitionKey: e.pk}
	if t.SortKey != "" && e.sk != nil {
		key[t.SortKey] = e.sk
	}
	return key
}

func itemSize(item Item) int {
	n := 0
	for k, v := range item {
		n += len(k)
		if v != nil {
			n += v.byteSize()
		}
	}
	return n
}

func compareEntryToKey(t *Table, e *scanEntry, key Item) int {
	if c := totalCompare(e.pk, key[t.PartitionKey]); c != 0 {
		return c
	}
	if t.SortKey == "" {
		return 0
	}
	return totalCompare(e.sk, key[t.SortKey])
}

// findUpperBound: first index whose entry is strictly greater than start.
func findUpperBound(entries []*scanEntry, t *Table, start Item) int {
	return sort.Search(len(entries), func(i int) bool {
		return compareEntryToKey(t, entries[i], start) > 0
	})
}

// findLowerBound: first index whose entry is greater than or equal to start.
func findLowerBound(entries []*scanEntry, t *Table, start Item) int {
	return sort.Search(len(entries), func(i int) bool {
		return compareEntryToKey(t, entries[i], start) >= 0
	})
}

func anyInSegmentRange(entries []*scanEntry, from, to int, segOf func(*scanEntry) int, segment int) bool {
	if from < 0 {
		from = 0
	}
	for j := from; j <= to && j < len(entries); j++ {
		if segOf(entries[j]) == segment {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Scan request handling
// ---------------------------------------------------------------------------

func optString(req map[string]interface{}, field string) (string, bool, *APIError) {
	raw, present := req[field]
	if !present || raw == nil {
		return "", false, nil
	}
	s, ok := raw.(string)
	if !ok {
		return "", true, newValidationError("%s must be a string", field)
	}
	return s, true, nil
}

func optBool(req map[string]interface{}, field string) (bool, bool, *APIError) {
	raw, present := req[field]
	if !present || raw == nil {
		return false, false, nil
	}
	b, ok := raw.(bool)
	if !ok {
		return false, true, newValidationError("%s must be a bool", field)
	}
	return b, true, nil
}

func requestInt(raw interface{}, field string) (int, *APIError) {
	f, ok := raw.(float64)
	if !ok {
		return 0, newValidationError("%s must be a number", field)
	}
	if f != f || f > 9e18 || f < -9e18 {
		return 0, newValidationError("%s must be an Integer", field)
	}
	n := int(f)
	if float64(n) != f {
		return 0, newValidationError("%s must be an Integer", field)
	}
	return n, nil
}

func optInt(req map[string]interface{}, field string) (int, bool, *APIError) {
	raw, present := req[field]
	if !present || raw == nil {
		return 0, false, nil
	}
	n, aerr := requestInt(raw, field)
	if aerr != nil {
		return 0, true, aerr
	}
	return n, true, nil
}

// DoScan implements the DynamoDB Scan operation against a registry.
func DoScan(reg *Registry, req map[string]interface{}) (map[string]interface{}, *APIError) {
	tableName, present, aerr := optString(req, "TableName")
	if aerr != nil {
		return nil, aerr
	}
	if !present {
		tableName = ""
	}
	if len(tableName) < 3 || len(tableName) > 255 {
		return nil, newValidationError("TableName must be at least 3 characters long and at most 255 characters long")
	}
	table := reg.GetTable(tableName)
	if table == nil {
		return nil, newResourceNotFound(tableName)
	}

	if _, present := req["IndexName"]; present {
		name, _, nameErr := optString(req, "IndexName")
		if nameErr != nil {
			return nil, nameErr
		}
		return nil, newValidationError("The table does not have the specified index: %s", name)
	}

	limit, hasLimit, aerr := optInt(req, "Limit")
	if aerr != nil {
		return nil, aerr
	}
	if hasLimit {
		if limit < 1 {
			return nil, newValidationError("Limit must be greater than or equal to 1")
		}
	} else {
		limit = 0
	}

	totalSegments, hasTotal, aerr := optInt(req, "TotalSegments")
	if aerr != nil {
		return nil, aerr
	}
	if !hasTotal {
		totalSegments = 1
	}
	segment, hasSegment, aerr := optInt(req, "Segment")
	if aerr != nil {
		return nil, aerr
	}
	if !hasSegment {
		segment = 0
	}
	if totalSegments < 1 {
		return nil, newValidationError("TotalSegments must be greater than or equal to 1")
	}
	if totalSegments > 1000000 {
		return nil, newValidationError("TotalSegments must be less than or equal to 1000000")
	}
	if segment < 0 {
		return nil, newValidationError("Segment must be greater than or equal to 0")
	}
	if segment >= totalSegments {
		return nil, newValidationError("Segment must be less than TotalSegments")
	}

	consistent, _, aerr := optBool(req, "ConsistentRead")
	if aerr != nil {
		return nil, aerr
	}
	forward := true
	if fv, fhas, faerr := optBool(req, "ScanIndexForward"); faerr != nil {
		return nil, faerr
	} else if fhas {
		forward = fv
	}

	rcc := "NONE"
	if rv, rhas, raerr := optString(req, "ReturnConsumedCapacity"); raerr != nil {
		return nil, raerr
	} else if rhas {
		switch rv {
		case "NONE", "TOTAL", "INDEXES":
			rcc = rv
		default:
			return nil, newValidationError("ReturnConsumedCapacity must be one of NONE, TOTAL or INDEXES, but was %s", rv)
		}
	}

	names := map[string]string{}
	if raw, present := req["ExpressionAttributeNames"]; present && raw != nil {
		m, ok := raw.(map[string]interface{})
		if !ok {
			return nil, newValidationError("ExpressionAttributeNames must be a map")
		}
		for k, v := range m {
			s, ok := v.(string)
			if !ok {
				return nil, newValidationError("ExpressionAttributeNames values must be strings")
			}
			names[k] = s
		}
	}
	values := map[string]*AttributeValue{}
	if raw, present := req["ExpressionAttributeValues"]; present && raw != nil {
		m, ok := raw.(map[string]interface{})
		if !ok {
			return nil, newValidationError("ExpressionAttributeValues must be a map")
		}
		for k, v := range m {
			av, avErr := parseAttributeValue(v)
			if avErr != nil {
				return nil, avErr
			}
			values[k] = av
		}
	}

	selectVal, hasSelect, aerr := optString(req, "Select")
	if aerr != nil {
		return nil, aerr
	}
	if !hasSelect {
		selectVal = "ALL_ATTRIBUTES"
	}
	switch selectVal {
	case "ALL_ATTRIBUTES", "COUNT", "SPECIFIC_ATTRIBUTES", "ALL_PROJECTED_ATTRIBUTES":
	default:
		return nil, newValidationError("One or more parameter values were invalid: Select must be ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES, COUNT or SPECIFIC_ATTRIBUTES, but was %s", selectVal)
	}

	filterExpr, hasFilterExpr, aerr := optString(req, "FilterExpression")
	if aerr != nil {
		return nil, aerr
	}
	projectionExpr, hasProjectionExpr, aerr := optString(req, "ProjectionExpression")
	if aerr != nil {
		return nil, aerr
	}
	rawScanFilter, hasScanFilter := req["ScanFilter"]
	rawAtg, hasAtg := req["AttributesToGet"]
	condOp, hasCondOp, aerr := optString(req, "ConditionalOperator")
	if aerr != nil {
		return nil, aerr
	}
	if hasCondOp {
		if !strings.EqualFold(condOp, "AND") && !strings.EqualFold(condOp, "OR") {
			return nil, newValidationError("One or more parameter values were invalid: ConditionalOperator must be AND or OR")
		}
		condOp = strings.ToUpper(condOp)
	} else {
		condOp = "AND"
	}

	hasExpressionParams := hasFilterExpr || hasProjectionExpr || len(names) > 0 || len(values) > 0
	hasLegacyParams := hasScanFilter || hasAtg
	if hasExpressionParams && hasLegacyParams {
		return nil, newValidationError("Can not use both expression and non-expression parameters in the same request")
	}

	if hasAtg && !hasSelect && selectVal == "ALL_ATTRIBUTES" {
		selectVal = "SPECIFIC_ATTRIBUTES"
	}
	if hasFilterExpr || hasProjectionExpr {
		switch selectVal {
		case "ALL_ATTRIBUTES", "COUNT":
		default:
			return nil, newValidationError("One or more parameter values were invalid: Select type %s is not supported with expression parameters", selectVal)
		}
	}
	if !hasFilterExpr && !hasProjectionExpr && !hasAtg && selectVal == "ALL_PROJECTED_ATTRIBUTES" {
		return nil, newValidationError("One or more parameter values were invalid: Select type ALL_PROJECTED_ATTRIBUTES is only supported with an IndexName")
	}

	var projection []*DocPath
	projectionUsed := map[string]bool{}
	if hasProjectionExpr {
		if strings.TrimSpace(projectionExpr) == "" {
			return nil, newValidationError("Invalid ProjectionExpression: The expression cannot be empty")
		}
		paths, used, perr := parseProjectionExpression(projectionExpr, names)
		if perr != nil {
			return nil, perr
		}
		for k := range used {
			projectionUsed[k] = true
		}
		for i := 0; i < len(paths); i++ {
			for j := i + 1; j < len(paths); j++ {
				if docPathOverlap(paths[i], paths[j]) {
					return nil, newValidationError("Invalid ProjectionExpression: Two document paths overlap with each other")
				}
			}
		}
		projection = paths
	}

	var cond *parsedCond
	if hasFilterExpr {
		if strings.TrimSpace(filterExpr) == "" {
			return nil, newValidationError("Invalid FilterExpression: The expression cannot be empty")
		}
		pc, ferr := parseFilterExpression(filterExpr, names, values)
		if ferr != nil {
			return nil, ferr
		}
		cond = pc
	} else if hasScanFilter {
		node, lerr := parseLegacyScanFilter(rawScanFilter, condOp)
		if lerr != nil {
			return nil, lerr
		}
		if node != nil {
			cond = &parsedCond{root: node, usedNames: map[string]bool{}, usedValues: map[string]bool{}}
		}
	}

	if hasAtg {
		list, ok := rawAtg.([]interface{})
		if !ok {
			return nil, newValidationError("AttributesToGet must be a list of strings")
		}
		for _, raw := range list {
			name, ok := raw.(string)
			if !ok {
				return nil, newValidationError("AttributesToGet must be a list of strings")
			}
			projection = append(projection, &DocPath{Segments: []PathSegment{{Name: name}}})
		}
	}

	// All placeholders must be used (DynamoDB rejects unused ones).
	usedNames := map[string]bool{}
	usedValues := map[string]bool{}
	if cond != nil {
		for k := range cond.usedNames {
			usedNames[k] = true
		}
		for k := range cond.usedValues {
			usedValues[k] = true
		}
	}
	for k := range projectionUsed {
		usedNames[k] = true
	}
	vks := make([]string, 0, len(values))
	for k := range values {
		vks = append(vks, k)
	}
	sort.Strings(vks)
	for _, k := range vks {
		if !usedValues[k] {
			return nil, newValidationError("Invalid ExpressionAttributeValues: Value provided in ExpressionAttributeValues is unused in expression: %q", k)
		}
	}
	nks := make([]string, 0, len(names))
	for k := range names {
		nks = append(nks, k)
	}
	sort.Strings(nks)
	for _, k := range nks {
		if !usedNames[k] {
			return nil, newValidationError("Invalid ExpressionAttributeNames: Name provided in ExpressionAttributeNames is unused in expression: %q", k)
		}
	}

	var startKey Item
	if raw, present := req["ExclusiveStartKey"]; present && raw != nil {
		m, ok := raw.(map[string]interface{})
		if !ok {
			return nil, newValidationError("ExclusiveStartKey must be a map")
		}
		startKey = Item{}
		for k, v := range m {
			av, avErr := parseAttributeValue(v)
			if avErr != nil {
				return nil, newValidationError("The provided starting key is invalid: %s", avErr.Message)
			}
			startKey[k] = av
		}
		schemaErr := newValidationError("The provided starting key is invalid: The provided key element does not match the schema")
		if _, ok := startKey[table.PartitionKey]; !ok {
			return nil, schemaErr
		}
		if table.SortKey != "" {
			if _, ok := startKey[table.SortKey]; !ok {
				return nil, schemaErr
			}
		}
		for k := range startKey {
			if k != table.PartitionKey && (table.SortKey == "" || k != table.SortKey) {
				return nil, schemaErr
			}
		}
		if table.pkType != "" && startKey[table.PartitionKey].Type != table.pkType {
			return nil, schemaErr
		}
		if table.SortKey != "" && table.skType != "" && startKey[table.SortKey].Type != table.skType {
			return nil, schemaErr
		}
	}

	// Materialize the ascending item list (pk order, then sk order within
	// each pk-equals partition) and derive the iteration range.
	entries := table.sortedEntries()
	begin, end := 0, len(entries)
	if startKey != nil {
		begin = findUpperBound(entries, table, startKey)
		end = findLowerBound(entries, table, startKey)
	}

	segmentOf := func(e *scanEntry) int {
		h := fnv.New32a()
		h.Write([]byte(keyCanonicalString(e.pk)))
		return int(h.Sum32() % uint32(totalSegments))
	}

	useProjection := projection != nil && selectVal != "COUNT"

	var items []Item
	scannedCount := 0
	count := 0
	scannedBytes := 0
	var lastKey Item

	matches := func(e *scanEntry) bool {
		return cond == nil || evalCondition(cond.root, e.item)
	}
	accumulate := func(e *scanEntry) {
		scannedCount++
		scannedBytes += itemSize(e.item)
		if !matches(e) {
			return
		}
		count++
		if selectVal != "COUNT" {
			var out Item
			if useProjection {
				out = projectItem(e.item, projection)
			} else {
				out = e.item
			}
			items = append(items, out)
		}
	}

	if forward {
		for i := begin; i < len(entries); i++ {
			e := entries[i]
			if totalSegments > 1 && segmentOf(e) != segment {
				continue
			}
			accumulate(e)
			if limit > 0 && scannedCount == limit {
				if anyInSegmentRange(entries, i+1, len(entries)-1, segmentOf, segment) {
					lastKey = table.keyOf(e)
				}
				break
			}
		}
	} else {
		for i := end - 1; i >= 0; i-- {
			e := entries[i]
			if totalSegments > 1 && segmentOf(e) != segment {
				continue
			}
			accumulate(e)
			if limit > 0 && scannedCount == limit {
				if anyInSegmentRange(entries, 0, i-1, segmentOf, segment) {
					lastKey = table.keyOf(e)
				}
				break
			}
		}
	}

	resp := map[string]interface{}{
		"Count":        count,
		"ScannedCount": scannedCount,
	}
	if selectVal != "COUNT" && len(items) > 0 {
		resp["Items"] = items
	}
	if lastKey != nil {
		resp["LastEvaluatedKey"] = lastKey
	}
	if rcc == "TOTAL" || rcc == "INDEXES" {
		units := float64((scannedBytes + 4095) / 4096)
		if units < 1 {
			units = 1
		}
		if !consistent {
			units /= 2
		}
		capacity := map[string]interface{}{"TableName": table.Name, "CapacityUnits": units}
		if rcc == "INDEXES" {
			capacity["ReadCapacityUnits"] = units
			capacity["Table"] = map[string]interface{}{"ReadCapacityUnits": units, "CapacityUnits": units}
		}
		resp["ConsumedCapacity"] = capacity
	}
	return resp, nil
}

// HandleScan processes one DynamoDB Scan request (JSON protocol body) and
// returns the HTTP status code and the response payload.
func HandleScan(body []byte) (int, []byte) {
	var req map[string]interface{}
	if err := json.Unmarshal(body, &req); err != nil {
		e := &APIError{Code: "SerializationException", Status: 400, Message: "Invalid JSON in request body"}
		return e.Status, marshalErrorPayload(e)
	}
	resp, apiErr := DoScan(DefaultRegistry, req)
	if apiErr != nil {
		return apiErr.Status, marshalErrorPayload(apiErr)
	}
	out, err := json.Marshal(resp)
	if err != nil {
		e := &APIError{Code: "InternalServerError", Status: 500, Message: err.Error()}
		return e.Status, marshalErrorPayload(e)
	}
	return 200, out
}

// ---------------------------------------------------------------------------
// Reserved words (subset of the DynamoDB reserved word list)
// ---------------------------------------------------------------------------

var reservedWords = map[string]bool{
	"ABORT": true, "ABSOLUTE": true, "ACTION": true, "ADA": true, "ADD": true,
	"ADMIN": true, "AFTER": true, "AGENT": true, "AGGREGATE": true, "ALL": true,
	"ALTER": true, "ANALYZE": true, "AND": true, "ANY": true, "ARE": true,
	"AS": true, "ASC": true, "AT": true, "ATTRIBUTE": true, "AUTHORIZATION": true,
	"BACKUP": true, "BEGIN": true, "BETWEEN": true, "BINARY": true, "BLOB": true,
	"BOOLEAN": true, "BOTH": true, "BREADTH": true, "BREAK": true, "BROWSE": true,
	"BULK": true, "BY": true, "CALL": true, "CALLED": true, "CARDINALITY": true,
	"CASCADE": true, "CASCADED": true, "CASE": true, "CAST": true, "CATALOG": true,
	"CHAR": true, "CHARACTER": true, "CHECK": true, "CLASS": true, "CLOB": true,
	"CLOSE": true, "CLUSTER": true, "COALESCE": true, "COLLATE": true,
	"COLLATION": true, "COLUMN": true, "COMMIT": true, "COMPLETION": true,
	"COMPOUND": true, "COMPUTE": true, "CONDITION": true, "CONFLICT": true,
	"CONNECT": true, "CONNECTION": true, "CONSTRAINT": true, "CONSTRAINTS": true,
	"CONSTRUCTOR": true, "CONSUMED": true, "CONTAINS": true, "CONTINUE": true,
	"CONVERT": true, "COPY": true, "CORRESPONDING": true, "COUNT": true,
	"CREATE": true, "CUBE": true, "CURRENT": true, "CURSOR": true, "CYCLE": true,
	"DATA": true, "DATABASE": true, "DATE": true, "DATETIME": true, "DAY": true,
	"DEALLOCATE": true, "DEC": true, "DECIMAL": true, "DECLARE": true,
	"DEFAULT": true, "DEFINE": true, "DEFINED": true, "DELETE": true,
	"DEPTH": true, "DEREF": true, "DESC": true, "DESCRIBE": true,
	"DESCRIPTOR": true, "DESTROY": true, "DESTRUCTOR": true, "DETERMINISTIC": true,
	"DIAGNOSTIC": true, "DIRECTORY": true, "DISCONNECT": true, "DISTINCT": true,
	"DISTRIBUTED": true, "DIV": true, "DO": true, "DOMAIN": true, "DOUBLE": true,
	"DROP": true, "DUMP": true, "DYNAMIC": true, "EACH": true, "ELEMENT": true,
	"ELSE": true, "ELSEIF": true, "ENCODING": true, "ENCRYPTED": true, "END": true,
	"EQUALS": true, "ESCAPE": true, "EVERY": true, "EXCEPT": true, "EXCEPTION": true,
	"EXIT": true, "EXISTS": true, "EXPLAIN": true, "EXTERNAL": true, "EXTRACT": true,
	"FALSE": true, "FETCH": true, "FLOAT": true, "FLOOR": true, "FOR": true,
	"FOREIGN": true, "FOUND": true, "FREE": true, "FREEZE": true, "FROM": true,
	"FULL": true, "FUNCTION": true, "GET": true, "GLOB": true, "GLOBAL": true,
	"GO": true, "GOTO": true, "GRANT": true, "GROUP": true, "HANDLER": true,
	"HASH": true, "HAVING": true, "HEAP": true, "HOLD": true, "HOUR": true,
	"IF": true, "IGNORE": true, "IMPORT": true, "IN": true, "INDEX": true,
	"INDICATOR": true, "INSERT": true, "INT": true, "INTEGER": true,
	"INTERSECT": true, "INTERVAL": true, "INTO": true, "IS": true,
	"ISOLATION": true, "ITEM": true, "JOIN": true, "KEY": true, "LANGUAGE": true,
	"LARGE": true, "LEADING": true, "LEFT": true, "LEVEL": true, "LIKE": true,
	"LIMIT": true, "LINENO": true, "LIST": true, "LOAD": true, "LOCAL": true,
	"LOCK": true, "LOG": true, "LOGGED": true, "LONG": true, "MAP": true,
	"MATCH": true, "MODIFY": true, "MODULE": true, "NAMES": true, "NATIONAL": true,
	"NATURAL": true, "NCHAR": true, "NEW": true, "NEXT": true, "NO": true,
	"NONE": true, "NORMALIZE": true, "NOT": true, "NULL": true, "NULLABLE": true,
	"NUMBER": true, "NUMERIC": true, "OBJECT": true, "OF": true, "OFF": true,
	"OFFSET": true, "OID": true, "OLD": true, "ON": true, "ONLY": true,
	"OPEN": true, "OPERATOR": true, "OPTION": true, "OR": true, "ORDER": true,
	"OUT": true, "OUTER": true, "OUTPUT": true, "OVER": true, "OVERLAPS": true,
	"OVERRIDE": true, "OWNER": true, "PAD": true, "PARAMETER": true,
	"PARAMETERS": true, "PARTIAL": true, "PARTITION": true, "PATH": true,
	"PCTFREE": true, "PERCENT": true, "PERMANENT": true, "PLAN": true,
	"POSITION": true, "POSTFIX": true, "PRAGMA": true, "PRECEDING": true,
	"PREFIX": true, "PREORDER": true, "PREPARE": true, "PRESERVE": true,
	"PRIMARY": true, "PRINT": true, "PRIOR": true, "PRIVILEGES": true,
	"PROCEDURE": true, "PROCESS": true, "PROJECTION": true, "PROTECTED": true,
	"PROTOCOL": true, "PROVIDER": true, "PROXY": true, "PUBLIC": true,
	"PURGE": true, "QUALIFY": true, "QUERY": true, "QUOTE": true, "RAISE": true,
	"RANGE": true, "RAW": true, "READ": true, "READS": true, "REAL": true,
	"RECURSIV": true, "REF": true, "REFERENCING": true, "REGISTRY": true,
	"RELATIVE": true, "RELEASE": true, "REMAINDER": true, "RENAME": true,
	"REPEAT": true, "REPLACE": true, "REPLICA": true, "REPLICATION": true,
	"REQUIRE": true, "REQUIRED": true, "RESIGNAL": true, "RESOURCE": true,
	"RESTORE": true, "RESTRICT": true, "RETURN": true, "RETURNING": true,
	"RETURNS": true, "REVERSE": true, "REVOKE": true, "RIGHT": true,
	"ROLLBACK": true, "ROLLUP": true, "ROUTINE": true, "ROW": true,
	"ROWCOUNT": true, "ROWNUM": true, "ROWS": true, "RULE": true, "SAVEPOINT": true,
	"SCHEMA": true, "SCHEMAS": true, "SCOPE": true, "SCROLL": true, "SEARCH": true,
	"SECOND": true, "SECTION": true, "SECURE": true, "SELECT": true,
	"SEMANTICS": true, "SENSITIVE": true, "SEQUENCE": true, "SERIALIZABLE": true,
	"SESSION": true, "SET": true, "SETS": true, "SHARED": true, "SHOW": true,
	"SHRINK": true, "SIGNAL": true, "SIGNED": true, "SIMILAR": true, "SIZE": true,
	"SMALLINT": true, "SNAPSHOT": true, "SOME": true, "SORT": true, "SOURCE": true,
	"SPATIAL": true, "SPECIFIC": true, "SPECIFICTYPE": true, "SQL": true,
	"SQLCODE": true, "SQLERROR": true, "SQLEXCEPTION": true, "SQLSTATE": true,
	"SQLWARNING": true, "STABLE": true, "START": true, "STARTING": true,
	"STATE": true, "STATEMENT": true, "STATIC": true, "STATISTICS": true,
	"STDIN": true, "STDOUT": true, "STEP": true, "STOP": true, "STORAGE": true,
	"STREAM": true, "STRING": true, "STRUCTURE": true, "SUBCLASS": true,
	"SUBPARTITION": true, "SUBSTRING": true, "SUM": true, "SUPER": true,
	"SYNONYM": true, "SYSDATE": true, "SYSTEM": true, "TABLE": true, "TABLES": true,
	"TABLESAMPLE": true, "TEMP": true, "TEMPORARY": true, "TERMINATED": true,
	"THAN": true, "THEN": true, "THREAD": true, "TIME": true, "TIMESTAMP": true,
	"TIMEZONE_HOUR": true, "TIMEZONE_MINUTE": true, "TINYINT": true, "TO": true,
	"TOP": true, "TRAILING": true, "TRANSACTION": true, "TRANSFORM": true,
	"TRANSLATE": true, "TREAT": true, "TRIGGER": true, "TRIM": true,
	"TRUNCATE": true, "TRUE": true, "TRUSTED": true, "TYPE": true,
	"UNCOMMITTED": true, "UNDER": true, "UNDO": true, "UNENCRYPTED": true,
	"UNION": true, "UNIQUE": true, "UNKNOWN": true, "UNLOCK": true,
	"UNLOGGED": true, "UNSIGNED": true, "UNTIL": true, "UPDATE": true,
	"UPDATING": true, "UPGRADE": true, "UPPER": true, "USAGE": true, "USE": true,
	"USER": true, "USING": true, "VACUUM": true, "VALID": true, "VALIDATE": true,
	"VALUE": true, "VALUES": true, "VARBINARY": true, "VARCHAR": true,
	"VARIABLE": true, "VERSION": true, "VIEW": true, "VIRTUAL": true,
	"VOLATILE": true, "WHEN": true, "WHENEVER": true, "WHERE": true, "WHILE": true,
	"WINDOW": true, "WITH": true, "WITHIN": true, "WITHOUT": true, "WORK": true,
	"WRITE": true, "YEAR": true,
}