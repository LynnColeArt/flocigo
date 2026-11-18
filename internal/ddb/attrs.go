package ddb

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"strings"
)

var attrTypeKeys = []string{"S", "N", "B", "BOOL", "NULL", "SS", "NS", "BS", "M", "L"}

// attrTypeOf returns the DynamoDB type tag of an attribute value.
func attrTypeOf(a map[string]any) string {
	if a == nil {
		return ""
	}
	found := ""
	for _, k := range attrTypeKeys {
		if _, ok := a[k]; ok {
			if found != "" {
				return ""
			}
			found = k
		}
	}
	return found
}

func attrString(a map[string]any) string {
	t := attrTypeOf(a)
	if t == "S" || t == "N" || t == "B" {
		s, _ := a[t].(string)
		return s
	}
	return ""
}

func attrBool(a map[string]any) bool {
	b, _ := a["BOOL"].(bool)
	return b
}

func attrBytes(a map[string]any) []byte {
	if attrTypeOf(a) != "B" {
		return nil
	}
	raw, err := base64.StdEncoding.DecodeString(attrString(a))
	if err != nil {
		return nil
	}
	return raw
}

// parseNumber parses a DynamoDB number string exactly.
func parseNumber(s string) (*big.Rat, bool) {
	t := strings.TrimSpace(s)
	if t == "" {
		return nil, false
	}
	r := new(big.Rat)
	if _, ok := r.SetString(t); !ok {
		return nil, false
	}
	return r, true
}

// bigRatDecimal renders a big.Rat as a plain decimal string.
func bigRatDecimal(r *big.Rat) string {
	num := new(big.Int).Set(r.Num())
	den := new(big.Int).Set(r.Denom())
	neg := num.Sign() < 0
	if neg {
		num.Neg(num)
	}
	q, rem := new(big.Int).QuoRem(num, den, new(big.Int))
	s := q.String()
	if rem.Sign() != 0 {
		frac := make([]byte, 0, 32)
		rem.Mul(rem, big.NewInt(10))
		for i := 0; i < 30 && rem.Sign() != 0; i++ {
			d, r2 := new(big.Int).QuoRem(rem, den, new(big.Int))
			frac = append(frac, byte('0'+d.Int64()))
			rem.Mul(r2, big.NewInt(10))
		}
		if len(frac) > 0 {
			s += "." + string(frac)
		}
	}
	if neg && s != "0" {
		return "-" + s
	}
	return s
}

// compareAttr compares two scalar attribute values (S, N, B).
func compareAttr(a, b map[string]any) int {
	ta, tb := attrTypeOf(a), attrTypeOf(b)
	if ta != tb {
		return strings.Compare(ta, tb)
	}
	switch ta {
	case "S":
		return strings.Compare(attrString(a), attrString(b))
	case "N":
		ra, oka := parseNumber(attrString(a))
		rb, okb := parseNumber(attrString(b))
		switch {
		case oka && okb:
			return ra.Cmp(rb)
		case oka:
			return -1
		case okb:
			return 1
		default:
			return strings.Compare(attrString(a), attrString(b))
		}
	case "B":
		return bytes.Compare(attrBytes(a), attrBytes(b))
	default:
		return 0
	}
}

// attrEq reports deep, type-aware equality of two attribute values.
func attrEq(a, b map[string]any) bool {
	ta, tb := attrTypeOf(a), attrTypeOf(b)
	if ta == "" || tb == "" || ta != tb {
		return false
	}
	switch ta {
	case "S", "N", "B":
		return compareAttr(a, b) == 0
	case "BOOL":
		return attrBool(a) == attrBool(b)
	case "NULL":
		return true
	case "SS", "BS":
		return stringSetEq(a[ta], b[ta])
	case "NS":
		return numberSetEq(a["NS"], b["NS"])
	case "L", "M":
		return deepJSONEqual(a[ta], b[ta])
	}
	return false
}

func stringSetEq(x, y any) bool {
	xs, ok1 := x.([]any)
	ys, ok2 := y.([]any)
	if !ok1 || !ok2 || len(xs) != len(ys) {
		return false
	}
	used := make([]bool, len(ys))
	for _, el := range xs {
		s1, ok := el.(string)
		if !ok {
			return false
		}
		matched := false
		for j, el2 := range ys {
			if used[j] {
				continue
			}
			if s2, ok2 := el2.(string); ok2 && s1 == s2 {
				used[j] = true
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func numberSetEq(x, y any) bool {
	xs, ok1 := x.([]any)
	ys, ok2 := y.([]any)
	if !ok1 || !ok2 || len(xs) != len(ys) {
		return false
	}
	nx := make([]*big.Rat, len(xs))
	for i, el := range xs {
		s, ok := el.(string)
		if !ok {
			return false
		}
		r, ok2 := parseNumber(s)
		if !ok2 {
			return false
		}
		nx[i] = r
	}
	used := make([]bool, len(ys))
	for _, el := range ys {
		s, ok := el.(string)
		if !ok {
			return false
		}
		r, ok2 := parseNumber(s)
		if !ok2 {
			return false
		}
		matched := false
		for j := range ys {
			if used[j] {
				continue
			}
			if r.Cmp(nx[j]) == 0 {
				used[j] = true
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func deepJSONEqual(x, y any) bool {
	bx, err1 := json.Marshal(x)
	by, err2 := json.Marshal(y)
	if err1 != nil || err2 != nil {
		return false
	}
	return bytes.Equal(bx, by)
}

// toAttr normalizes and deep-copies a decoded attribute value.
func toAttr(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, false
	}
	c := cloneAttr(m)
	normalizeAttr(c)
	return c, true
}

// normalizeAttr converts json.Number type members to strings in place.
func normalizeAttr(a map[string]any) {
	for _, k := range attrTypeKeys {
		v, ok := a[k]
		if !ok {
			continue
		}
		switch k {
		case "S", "N", "B":
			if n, ok := v.(json.Number); ok {
				a[k] = n.String()
			}
		case "SS", "NS", "BS":
			if lst, ok := v.([]any); ok {
				for i, el := range lst {
					if n, ok2 := el.(json.Number); ok2 {
						lst[i] = n.String()
					}
				}
			}
		case "L":
			if lst, ok := v.([]any); ok {
				for i, el := range lst {
					switch te := el.(type) {
					case map[string]any:
						normalizeAttr(te)
					case json.Number:
						lst[i] = te.String()
					}
				}
			}
		case "M":
			if mm, ok := v.(map[string]any); ok {
				for kk, el := range mm {
					switch te := el.(type) {
					case map[string]any:
						normalizeAttr(te)
					case json.Number:
						mm[kk] = te.String()
					}
				}
			}
		}
	}
}

// normalizeItemAttrs normalizes the attribute values of an item in place.
func normalizeItemAttrs(item map[string]any) {
	for k, v := range item {
		switch tv := v.(type) {
		case map[string]any:
			normalizeAttr(tv)
		case json.Number:
			item[k] = tv.String()
		}
	}
}

// cloneAttr deep-copies an attribute value.
func cloneAttr(a map[string]any) map[string]any {
	out := make(map[string]any, len(a))
	for k, v := range a {
		switch tv := v.(type) {
		case map[string]any:
			out[k] = cloneAttr(tv)
		case []any:
			arr := make([]any, len(tv))
			for i, el := range tv {
				if m, ok := el.(map[string]any); ok {
					arr[i] = cloneAttr(m)
				} else {
					arr[i] = el
				}
			}
			out[k] = arr
		default:
			out[k] = v
		}
	}
	return out
}

// validateAttrValue rejects empty sets anywhere in an attribute tree.
func validateAttrValue(a map[string]any) *apiErr {
	t := attrTypeOf(a)
	switch t {
	case "SS", "NS", "BS":
		lst, _ := a[t].([]any)
		if len(lst) == 0 {
			name := "string"
			if t == "NS" {
				name = "number"
			}
			if t == "BS" {
				name = "binary"
			}
			return ddbErrorf("ValidationException", "One or more parameter values were invalid: An %s set may not be empty", name)
		}
	case "L":
		lst, _ := a["L"].([]any)
		for _, el := range lst {
			if m, ok := el.(map[string]any); ok {
				if aerr := validateAttrValue(m); aerr != nil {
					return aerr
				}
			}
		}
	case "M":
		mm, _ := a["M"].(map[string]any)
		for _, el := range mm {
			if m, ok := el.(map[string]any); ok {
				if aerr := validateAttrValue(m); aerr != nil {
					return aerr
				}
			}
		}
	}
	return nil
}

// validateItemAttrValues validates every attribute value of an item.
func validateItemAttrValues(item map[string]any) *apiErr {
	for _, v := range item {
		if m, ok := v.(map[string]any); ok {
			if aerr := validateAttrValue(m); aerr != nil {
				return aerr
			}
		}
	}
	return nil
}
