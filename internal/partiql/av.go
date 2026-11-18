package partiql

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"strconv"
	"unicode/utf16"
)

// AV is a DynamoDB AttributeValue exactly as it travels on the wire: a map
// with exactly one type key and its payload. It is a type alias, so decoded
// JSON maps can be passed without conversion.
type AV = map[string]any

const (
	TypeS    = "S"
	TypeN    = "N"
	TypeB    = "B"
	TypeBOOL = "BOOL"
	TypeNULL = "NULL"
	TypeL    = "L"
	TypeM    = "M"
	TypeSS   = "SS"
	TypeNS   = "NS"
	TypeBS   = "BS"
)

var typeOrder = []string{TypeS, TypeN, TypeB, TypeBOOL, TypeNULL, TypeL, TypeM, TypeSS, TypeNS, TypeBS}

// KindOf returns the single type key of av ("" when av is malformed).
func KindOf(av AV) string {
	for _, k := range typeOrder {
		if _, ok := av[k]; ok {
			return k
		}
	}
	return ""
}

func avBad() *ValidationError {
	return badValueErr("An AttributeValue must have exactly one type key")
}

func scalarComparable(k string) bool { return k == TypeS || k == TypeN || k == TypeB }

func listElems(v any) ([]any, bool) { s, ok := v.([]any); return s, ok }

func mapAny(v any) (AV, bool) { m, ok := v.(AV); return m, ok }

func stringVal(v any) (string, bool) { s, ok := v.(string); return s, ok }

func boolVal(v any) (bool, bool) { b, ok := v.(bool); return b, ok }

func isAVNode(v any) bool { _, ok := v.(AV); return ok }

// decVal parses number payloads, accepting the wire string form plus the
// common in-memory numeric forms for convenience.
func decVal(v any) (Dec, bool) {
	switch x := v.(type) {
	case string:
		d, err := ParseDec(x)
		if err != nil {
			return Dec{}, false
		}
		return d, true
	case json.Number:
		d, err := ParseDec(string(x))
		if err != nil {
			return Dec{}, false
		}
		return d, true
	case float64:
		d, err := ParseDec(strconv.FormatFloat(x, 'f', -1, 64))
		if err != nil {
			return Dec{}, false
		}
		return d, true
	case int64:
		return Dec{U: big.NewInt(x), S: 0}, true
	case int:
		return Dec{U: big.NewInt(int64(x)), S: 0}, true
	}
	return Dec{}, false
}

func bytesVal(v any) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		return x, nil
	case string:
		b, err := base64.StdEncoding.DecodeString(x)
		if err != nil {
			b, err = base64.RawStdEncoding.DecodeString(x)
			if err != nil {
				return nil, badValueErr("Invalid binary value: %q", x)
			}
		}
		return b, nil
	}
	return nil, badValueErr("Invalid binary value")
}

// CompareStrings orders two strings by UTF-16 code unit, the ordering of
// java.lang.String.compareTo used by the Java evaluator.
func CompareStrings(a, b string) int {
	ua := utf16.Encode([]rune(a))
	ub := utf16.Encode([]rune(b))
	for i := 0; i < len(ua) && i < len(ub); i++ {
		if ua[i] != ub[i] {
			if ua[i] < ub[i] {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(ua) < len(ub):
		return -1
	case len(ua) > len(ub):
		return 1
	}
	return 0
}

// compareScalars compares two payloads of the same scalar kind.
func compareScalars(k string, a, b any) (int, error) {
	switch k {
	case TypeS:
		as, aok := stringVal(a)
		bs, bok := stringVal(b)
		if !aok || !bok {
			return 0, badValueErr("Invalid string value")
		}
		return CompareStrings(as, bs), nil
	case TypeN:
		ad, aok := decVal(a)
		bd, bok := decVal(b)
		if !aok || !bok {
			return 0, badValueErr("Invalid decimal value")
		}
		return ad.Cmp(bd), nil
	case TypeB:
		ab, err := bytesVal(a)
		if err != nil {
			return 0, err
		}
		bb, err := bytesVal(b)
		if err != nil {
			return 0, err
		}
		return bytes.Compare(ab, bb), nil
	}
	return 0, avBad()
}

// EqualDeep reports deep attribute-value equality; N compares numerically
// and sets compare unordered, mirroring the Java evaluator.
func EqualDeep(a, b AV) bool {
	ka := KindOf(a)
	kb := KindOf(b)
	if ka == "" || kb == "" || ka != kb {
		return false
	}
	switch ka {
	case TypeS:
		as, _ := stringVal(a[TypeS])
		bs, _ := stringVal(b[TypeS])
		return as == bs
	case TypeN:
		c, err := compareScalars(TypeN, a[TypeN], b[TypeN])
		return err == nil && c == 0
	case TypeB:
		ab, err := bytesVal(a[TypeB])
		if err != nil {
			return false
		}
		bb, err := bytesVal(b[TypeB])
		if err != nil {
			return false
		}
		return bytes.Equal(ab, bb)
	case TypeBOOL:
		ab, aok := boolVal(a[TypeBOOL])
		bb, bok := boolVal(b[TypeBOOL])
		return aok && bok && ab == bb
	case TypeNULL:
		_, aok := boolVal(a[TypeNULL])
		_, bok := boolVal(b[TypeNULL])
		return aok && bok
	case TypeL:
		la, _ := listElems(a[TypeL])
		lb, _ := listElems(b[TypeL])
		if len(la) != len(lb) {
			return false
		}
		for i := range la {
			ea, ok1 := mapAny(la[i])
			eb, ok2 := mapAny(lb[i])
			if !ok1 || !ok2 || !EqualDeep(ea, eb) {
				return false
			}
		}
		return true
	case TypeM:
		ma, ok1 := mapAny(a[TypeM])
		mb, ok2 := mapAny(b[TypeM])
		if !ok1 || !ok2 || len(ma) != len(mb) {
			return false
		}
		for k := range ma {
			ev, ok := mb[k]
			if !ok {
				return false
			}
			ea, ok1 := mapAny(ma[k])
			eb, ok2 := mapAny(ev)
			if !ok1 || !ok2 || !EqualDeep(ea, eb) {
				return false
			}
		}
		return true
	case TypeSS, TypeNS, TypeBS:
		return setEqual(ka, a, b)
	}
	return false
}

func setEqual(kind string, a, b AV) bool {
	ea, ok1 := listElems(a[kind])
	eb, ok2 := listElems(b[kind])
	if !ok1 || !ok2 || len(ea) != len(eb) {
		return false
	}
	return setContainsAll(kind, a, eb) && setContainsAll(kind, b, ea)
}

func setContainsAll(kind string, set AV, elems []any) bool {
	for _, e := range elems {
		if !setContains(kind, set, e) {
			return false
		}
	}
	return true
}

func setContains(kind string, set AV, elem any) bool {
	elems, _ := listElems(set[kind])
	switch kind {
	case TypeSS:
		es, ok := stringVal(elem)
		if !ok {
			return false
		}
		for _, e := range elems {
			s, ok := stringVal(e)
			if ok && s == es {
				return true
			}
		}
	case TypeNS:
		ed, ok := decVal(elem)
		if !ok {
			return false
		}
		for _, e := range elems {
			d, ok := decVal(e)
			if ok && d.Cmp(ed) == 0 {
				return true
			}
		}
	case TypeBS:
		eb, err := bytesVal(elem)
		if err != nil {
			return false
		}
		for _, e := range elems {
			db, err := bytesVal(e)
			if err == nil && bytes.Equal(db, eb) {
				return true
			}
		}
	}
	return false
}

// SizeOf implements size(): byte length for S/B, element counts for
// collections. N, BOOL and NULL are rejected like in the Java evaluator.
func SizeOf(av AV) (int, error) {
	k := KindOf(av)
	switch k {
	case TypeS:
		s, _ := stringVal(av[TypeS])
		return len(s), nil
	case TypeB:
		b, err := bytesVal(av[TypeB])
		if err != nil {
			return 0, err
		}
		return len(b), nil
	case TypeL, TypeSS, TypeNS, TypeBS:
		e, _ := listElems(av[k])
		return len(e), nil
	case TypeM:
		m, _ := mapAny(av[TypeM])
		return len(m), nil
	}
	return 0, badValueErr("size() is not supported for %s", k)
}

// DeepCopyAV returns an independent copy of av.
func DeepCopyAV(av AV) AV {
	out := make(AV, len(av))
	for k, v := range av {
		out[k] = CopyVal(v)
	}
	return out
}

// CopyVal deep-copies nested values, sharing only immutable scalars.
func CopyVal(v any) any {
	switch x := v.(type) {
	case AV:
		m := make(AV, len(x))
		for k, e := range x {
			m[k] = CopyVal(e)
		}
		return m
	case []any:
		s := make([]any, len(x))
		for i, e := range x {
			s[i] = CopyVal(e)
		}
		return s
	default:
		return v
	}
}

// avGet resolves p against item; ok is false when a segment is missing, out
// of bounds, or used against the wrong container type. Indexing into a set
// is rejected like in the Java document navigation.
func avGet(item AV, p Path) (any, bool) {
	cur := any(item)
	for _, e := range p.Elems {
		switch c := cur.(type) {
		case AV:
			if e.IsIdx {
				return nil, false
			}
			v, ok := c[e.Name]
			if !ok {
				return nil, false
			}
			cur = v
		case []any:
			if !e.IsIdx || e.Index >= len(c) {
				return nil, false
			}
			if len(c) > 0 && !isAVNode(c[0]) {
				return nil, false
			}
			cur = c[e.Index]
		default:
			return nil, false
		}
	}
	return cur, true
}
