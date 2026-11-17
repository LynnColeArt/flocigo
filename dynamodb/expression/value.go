// Package expression is the semantic core of DynamoDB expression handling in
// floci-go. It implements evaluation of ConditionExpression and
// FilterExpression, parsing and application of UpdateExpression, and
// validation of KeyConditionExpression, replicating the behavior of the Java
// originals (ExpressionEvaluator.java, DynamoDbUpdateExpressionSyntax.java,
// DynamoDbConditionKeys.java) including the AWS error taxonomy.
//
// All validation failures are returned as *ExpressionError whose Message is
// the exact DynamoDB ValidationException body. Syntax errors include the
// offending token and nearby text, e.g.
//
//     Invalid ConditionExpression: Syntax error; token: "b", near: "b = :y"
package expression

import (
	"bytes"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

// Kind enumerates the DynamoDB attribute value types.
type Kind uint8

const (
	KindString Kind = iota
	KindNumber
	KindBinary
	KindStringSet
	KindNumberSet
	KindBinarySet
	KindMap
	KindList
	KindBool
	KindNull
)

// String returns the DynamoDB type tag of the kind (S, N, B, SS, NS, BS, M, L, BOOL, NULL).
func (k Kind) String() string {
	switch k {
	case KindString:
		return "S"
	case KindNumber:
		return "N"
	case KindBinary:
		return "B"
	case KindStringSet:
		return "SS"
	case KindNumberSet:
		return "NS"
	case KindBinarySet:
		return "BS"
	case KindMap:
		return "M"
	case KindList:
		return "L"
	case KindBool:
		return "BOOL"
	case KindNull:
		return "NULL"
	}
	return "UNKNOWN"
}

// Value is a DynamoDB attribute value. Numbers are kept as decimal strings,
// binary values as raw bytes.
type Value struct {
	Kind Kind
	Str  string   // content of S and N
	Bin  []byte   // content of B
	SS   []string // elements of SS
	NS   []string // elements of NS
	BS   [][]byte // elements of BS
	M    map[string]*Value
	L    []*Value
	Bool bool
}

func NewString(s string) *Value { return &Value{Kind: KindString, Str: s} }

func NewNumber(s string) *Value { return &Value{Kind: KindNumber, Str: s} }

func NewBinary(b []byte) *Value { return &Value{Kind: KindBinary, Bin: append([]byte(nil), b...)} }

func NewBool(b bool) *Value { return &Value{Kind: KindBool, Bool: b} }

func NewNull() *Value { return &Value{Kind: KindNull} }

func NewStringSet(ss []string) *Value {
	cp := make([]string, len(ss))
	copy(cp, ss)
	return &Value{Kind: KindStringSet, SS: cp}
}

func NewNumberSet(ns []string) *Value {
	cp := make([]string, len(ns))
	copy(cp, ns)
	return &Value{Kind: KindNumberSet, NS: cp}
}

func NewBinarySet(bs [][]byte) *Value {
	cp := make([][]byte, len(bs))
	for i, b := range bs {
		cp[i] = append([]byte(nil), b...)
	}
	return &Value{Kind: KindBinarySet, BS: cp}
}

func NewList(l []*Value) *Value {
	cp := make([]*Value, len(l))
	for i, v := range l {
		cp[i] = v.Clone()
	}
	return &Value{Kind: KindList, L: cp}
}

func NewMap(m map[string]*Value) *Value {
	cp := make(map[string]*Value, len(m))
	for k, v := range m {
		cp[k] = v.Clone()
	}
	return &Value{Kind: KindMap, M: cp}
}

// Clone returns a deep copy of the value.
func (v *Value) Clone() *Value {
	if v == nil {
		return nil
	}
	c := &Value{Kind: v.Kind, Str: v.Str, Bool: v.Bool}
	if v.Bin != nil {
		c.Bin = append([]byte(nil), v.Bin...)
	}
	if v.SS != nil {
		c.SS = append([]string(nil), v.SS...)
	}
	if v.NS != nil {
		c.NS = append([]string(nil), v.NS...)
	}
	for _, b := range v.BS {
		c.BS = append(c.BS, append([]byte(nil), b...))
	}
	for _, e := range v.L {
		c.L = append(c.L, e.Clone())
	}
	if v.M != nil {
		c.M = make(map[string]*Value, len(v.M))
		for k, e := range v.M {
			c.M[k] = e.Clone()
		}
	}
	return c
}

// IsSet reports whether v is one of the set types (SS, NS, BS).
func (v *Value) IsSet() bool {
	return v.Kind == KindStringSet || v.Kind == KindNumberSet || v.Kind == KindBinarySet
}

// Number returns the numeric content of an N value.
func (v *Value) Number() (*big.Rat, bool) {
	if v.Kind != KindNumber {
		return nil, false
	}
	return parseDecimal(v.Str)
}

// Equal reports deep equality (type sensitive; sets compare ignoring order,
// numbers compare numerically).
func (v *Value) Equal(o *Value) bool { return valuesEqual(v, o) }

func valuesEqual(a, b *Value) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Kind != b.Kind {
		return false
	}
	switch a.Kind {
	case KindString:
		return a.Str == b.Str
	case KindNumber:
		ra, ok1 := parseDecimal(a.Str)
		rb, ok2 := parseDecimal(b.Str)
		if !ok1 || !ok2 {
			return a.Str == b.Str
		}
		return ra.Cmp(rb) == 0
	case KindBinary:
		return bytes.Equal(a.Bin, b.Bin)
	case KindBool:
		return a.Bool == b.Bool
	case KindNull:
		return true
	case KindStringSet:
		return stringSetEqual(a.SS, b.SS)
	case KindNumberSet:
		return numberSetEqual(a.NS, b.NS)
	case KindBinarySet:
		return binarySetEqual(a.BS, b.BS)
	case KindList:
		if len(a.L) != len(b.L) {
			return false
		}
		for i := range a.L {
			if !valuesEqual(a.L[i], b.L[i]) {
				return false
			}
		}
		return true
	case KindMap:
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
	}
	return false
}

func stringSetEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for _, s := range a {
		if !containsString(b, s) {
			return false
		}
	}
	return true
}

func numberSetEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(b))
	for _, s := range b {
		c, ok := canonicalNumber(s)
		if !ok {
			return false
		}
		seen[c]++
	}
	for _, s := range a {
		c, ok := canonicalNumber(s)
		if !ok {
			return false
		}
		if seen[c] == 0 {
			return false
		}
		seen[c]--
	}
	return true
}

func binarySetEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range a {
		if !containsBinary(b, x) {
			return false
		}
	}
	return true
}

func containsString(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func containsBinary(list [][]byte, v []byte) bool {
	for _, x := range list {
		if bytes.Equal(x, v) {
			return true
		}
	}
	return false
}

var numberPattern = regexp.MustCompile(`^-?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$`)

// parseDecimal parses a DynamoDB number literal into an exact rational.
func parseDecimal(s string) (*big.Rat, bool) {
	if s == "" || !numberPattern.MatchString(s) {
		return nil, false
	}
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		expo, err := strconv.Atoi(s[i+1:])
		if err != nil || expo < -4000 || expo > 4000 {
			return nil, false
		}
	}
	r := new(big.Rat)
	if _, ok := r.SetString(s); !ok {
		return nil, false
	}
	return r, true
}

// canonicalNumber normalizes an N payload for set membership checks.
func canonicalNumber(s string) (string, bool) {
	r, ok := parseDecimal(s)
	if !ok {
		return "", false
	}
	return numberString(r), true
}

// numberString renders a rational exactly, the way DynamoDB stores numbers.
func numberString(r *big.Rat) string {
	if r.Sign() == 0 {
		return "0"
	}
	if r.IsInt() {
		return r.Num().String()
	}
	s := r.FloatString(38)
	if t := strings.TrimRight(s, "0"); t != "" && t != "-" && t != "." {
		s = t
	}
	s = strings.TrimSuffix(s, ".")
	return s
}

// setLength returns the number of elements of a set value.
func setLength(v *Value) int {
	switch v.Kind {
	case KindStringSet:
		return len(v.SS)
	case KindNumberSet:
		return len(v.NS)
	case KindBinarySet:
		return len(v.BS)
	}
	return 0
}

// setUnion merges set b into a copy of set a.
func setUnion(a, b *Value) *Value {
	out := a.Clone()
	switch a.Kind {
	case KindStringSet:
		for _, s := range b.SS {
			if !containsString(out.SS, s) {
				out.SS = append(out.SS, s)
			}
		}
	case KindNumberSet:
		seen := make(map[string]bool, len(out.NS))
		for _, s := range out.NS {
			if cc, ok := canonicalNumber(s); ok {
				seen[cc] = true
			}
		}
		for _, s := range b.NS {
			cc, ok := canonicalNumber(s)
			if !ok || seen[cc] {
				continue
			}
			seen[cc] = true
			out.NS = append(out.NS, s)
		}
	case KindBinarySet:
		for _, x := range b.BS {
			if !containsBinary(out.BS, x) {
				out.BS = append(out.BS, append([]byte(nil), x...))
			}
		}
	}
	return out
}

// setSubtract removes the elements of b from a copy of set a.
func setSubtract(a, b *Value) *Value {
	out := a.Clone()
	switch a.Kind {
	case KindStringSet:
		kept := make([]string, 0, len(out.SS))
		for _, s := range out.SS {
			if !containsString(b.SS, s) {
				kept = append(kept, s)
			}
		}
		out.SS = kept
	case KindNumberSet:
		dead := make(map[string]bool, len(b.NS))
		for _, s := range b.NS {
			if cc, ok := canonicalNumber(s); ok {
				dead[cc] = true
			}
		}
		kept := make([]string, 0, len(out.NS))
		for _, s := range out.NS {
			cc, ok := canonicalNumber(s)
			if ok && dead[cc] {
				continue
			}
			kept = append(kept, s)
		}
		out.NS = kept
	case KindBinarySet:
		kept := make([][]byte, 0, len(out.BS))
		for _, x := range out.BS {
			if !containsBinary(b.BS, x) {
				kept = append(kept, x)
			}
		}
		out.BS = kept
	}
	return out
}

// ValueFromAttributeValue builds a Value from a decoded AttributeValue map,
// e.g. {"S": "x"}, {"N": "1"}, {"B": ...}, {"M": {...}}, {"L": [...]},
// sets, BOOL and NULL.
func ValueFromAttributeValue(av map[string]interface{}) (*Value, error) {
	if s, ok := av["S"].(string); ok {
		return NewString(s), nil
	}
	if s, ok := av["N"].(string); ok {
		if _, ok2 := parseDecimal(s); !ok2 {
			return nil, exprErr("One or more parameter values were invalid: A value provided cannot be converted into a number: %s", s)
		}
		return NewNumber(s), nil
	}
	switch b := av["B"].(type) {
	case []byte:
		return NewBinary(b), nil
	case string:
		return NewBinary([]byte(b)), nil
	}
	if b, ok := av["BOOL"].(bool); ok {
		return NewBool(b), nil
	}
	if _, ok := av["NULL"]; ok {
		return NewNull(), nil
	}
	if raw, ok := av["SS"].([]interface{}); ok {
		out := []string{}
		for _, el := range raw {
			s, ok := el.(string)
			if !ok {
				return nil, invalidAttributeValueError()
			}
			out = append(out, s)
		}
		return NewStringSet(out), nil
	}
	if raw, ok := av["NS"].([]interface{}); ok {
		out := []string{}
		for _, el := range raw {
			s, ok := el.(string)
			if !ok {
				return nil, invalidAttributeValueError()
			}
			if _, ok2 := parseDecimal(s); !ok2 {
				return nil, exprErr("One or more parameter values were invalid: A value provided cannot be converted into a number: %s", s)
			}
			out = append(out, s)
		}
		return NewNumberSet(out), nil
	}
	if raw, ok := av["BS"].([]interface{}); ok {
		out := [][]byte{}
		for _, el := range raw {
			switch x := el.(type) {
			case []byte:
				out = append(out, x)
			case string:
				out = append(out, []byte(x))
			default:
				return nil, invalidAttributeValueError()
			}
		}
		return NewBinarySet(out), nil
	}
	if raw, ok := av["L"].([]interface{}); ok {
		out := []*Value{}
		for _, el := range raw {
			m, ok := el.(map[string]interface{})
			if !ok {
				return nil, invalidAttributeValueError()
			}
			v, err := ValueFromAttributeValue(m)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return NewList(out), nil
	}
	if raw, ok := av["M"].(map[string]interface{}); ok {
		out := map[string]*Value{}
		for k, el := range raw {
			m, ok := el.(map[string]interface{})
			if !ok {
				return nil, invalidAttributeValueError()
			}
			v, err := ValueFromAttributeValue(m)
			if err != nil {
				return nil, err
			}
			out[k] = v
		}
		return NewMap(out), nil
	}
	return nil, invalidAttributeValueError()
}

func invalidAttributeValueError() error {
	return &ExpressionError{Message: "One or more parameter values were invalid: An attribute value must not be empty"}
}

// AttributeValue renders the Value as an AttributeValue map.
func (v *Value) AttributeValue() map[string]interface{} {
	out := map[string]interface{}{}
	switch v.Kind {
	case KindString:
		out["S"] = v.Str
	case KindNumber:
		out["N"] = v.Str
	case KindBinary:
		out["B"] = v.Bin
	case KindBool:
		out["BOOL"] = v.Bool
	case KindNull:
		out["NULL"] = true
	case KindStringSet:
		out["SS"] = toRawStrings(v.SS)
	case KindNumberSet:
		out["NS"] = toRawStrings(v.NS)
	case KindBinarySet:
		out["BS"] = toRawBinaries(v.BS)
	case KindList:
		l := []interface{}{}
		for _, el := range v.L {
			l = append(l, el.AttributeValue())
		}
		out["L"] = l
	case KindMap:
		m := map[string]interface{}{}
		for k, el := range v.M {
			m[k] = el.AttributeValue()
		}
		out["M"] = m
	}
	return out
}

func toRawStrings(ss []string) []interface{} {
	out := []interface{}{}
	for _, s := range ss {
		out = append(out, s)
	}
	return out
}

func toRawBinaries(bs [][]byte) []interface{} {
	out := []interface{}{}
	for _, b := range bs {
		out = append(out, b)
	}
	return out
}
