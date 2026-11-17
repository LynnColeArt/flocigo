package ddb

import (
    "bytes"
    "encoding/base64"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "math/big"
    "sort"
    "strings"
)

// AttrVal represents a single DynamoDB AttributeValue. Exactly one of the
// type specific payload fields is populated; Type records which one.
type AttrVal struct {
    Type string // S, N, B, SS, NS, BS, M, L, BOOL, NULL
    S    string
    N    string
    B    []byte
    SS   []string
    NS   []string
    BS   [][]byte
    M    map[string]AttrVal
    L    []AttrVal
    BOOL bool
    Null bool
}

// Attribute value constructors.
func MakeS(v string) AttrVal { return AttrVal{Type: "S", S: v} }
func MakeN(v string) AttrVal { return AttrVal{Type: "N", N: v} }
func MakeB(v []byte) AttrVal { return AttrVal{Type: "B", B: v} }
func MakeBOOL(v bool) AttrVal { return AttrVal{Type: "BOOL", BOOL: v} }
func MakeNULL() AttrVal { return AttrVal{Type: "NULL", Null: true} }
func MakeSS(v []string) AttrVal { return AttrVal{Type: "SS", SS: v} }
func MakeNS(v []string) AttrVal { return AttrVal{Type: "NS", NS: v} }
func MakeBS(v [][]byte) AttrVal { return AttrVal{Type: "BS", BS: v} }
func MakeM(v map[string]AttrVal) AttrVal {
    if v == nil {
        v = map[string]AttrVal{}
    }
    return AttrVal{Type: "M", M: v}
}
func MakeL(v []AttrVal) AttrVal { return AttrVal{Type: "L", L: v} }

// MarshalJSON encodes the value in the DynamoDB JSON protocol shape
// ({"S": "..."} and friends).
func (v AttrVal) MarshalJSON() ([]byte, error) {
    switch v.Type {
    case "S":
        return json.Marshal(map[string]string{"S": v.S})
    case "N":
        return json.Marshal(map[string]string{"N": v.N})
    case "B":
        return json.Marshal(map[string]string{"B": base64.StdEncoding.EncodeToString(v.B)})
    case "SS":
        return json.Marshal(map[string][]string{"SS": v.SS})
    case "NS":
        return json.Marshal(map[string][]string{"NS": v.NS})
    case "BS":
        enc := make([]string, 0, len(v.BS))
        for _, b := range v.BS {
            enc = append(enc, base64.StdEncoding.EncodeToString(b))
        }
        return json.Marshal(map[string][]string{"BS": enc})
    case "M":
        return json.Marshal(map[string]map[string]AttrVal{"M": v.M})
    case "L":
        return json.Marshal(map[string][]AttrVal{"L": v.L})
    case "BOOL":
        return json.Marshal(map[string]bool{"BOOL": v.BOOL})
    case "NULL":
        return json.Marshal(map[string]bool{"NULL": true})
    default:
        return nil, fmt.Errorf("unknown attribute value type %q", v.Type)
    }
}

// UnmarshalJSON decodes a DynamoDB protocol AttributeValue, enforcing the
// value level rules the service applies (single type, non empty strings,
// non empty sets, valid numbers).
func (v *AttrVal) UnmarshalJSON(data []byte) error {
    var raw map[string]json.RawMessage
    if err := json.Unmarshal(data, &raw); err != nil {
        return serializationErr("Invalid AttributeValue")
    }
    if len(raw) == 0 {
        return serializationErr("Unknown AttributeValue type")
    }
    if len(raw) > 1 {
        return validation("Supplied AttributeValue has more than one datatypes set, should contain exactly one of the supported datatypes")
    }
    for typeKey, body := range raw {
        switch typeKey {
        case "S":
            var s string
            if err := json.Unmarshal(body, &s); err != nil {
                return serializationErr("Invalid S AttributeValue")
            }
            if len(s) == 0 {
                return validation("One or more parameter values are not valid: An AttributeValue may not contain an empty string")
            }
            v.Type, v.S = "S", s
        case "N":
            var s string
            if err := json.Unmarshal(body, &s); err != nil {
                return serializationErr("Invalid N AttributeValue")
            }
            if !validNumberFormat(s) {
                return validation("The parameter cannot be converted to a numeric value: " + s)
            }
            v.Type, v.N = "N", s
        case "B":
            var s string
            if err := json.Unmarshal(body, &s); err != nil {
                return serializationErr("Invalid B AttributeValue")
            }
            b, err := base64.StdEncoding.DecodeString(s)
            if err != nil {
                return serializationErr("Invalid B AttributeValue")
            }
            v.Type, v.B = "B", b
        case "SS":
            var s []string
            if err := json.Unmarshal(body, &s); err != nil {
                return serializationErr("Invalid SS AttributeValue")
            }
            if len(s) == 0 {
                return validation("One or more parameter values are not valid: An AttributeValue may not contain an empty set")
            }
            for _, e := range s {
                if len(e) == 0 {
                    return validation("One or more parameter values are not valid: An AttributeValue may not contain an empty string")
                }
            }
            v.Type, v.SS = "SS", s
        case "NS":
            var s []string
            if err := json.Unmarshal(body, &s); err != nil {
                return serializationErr("Invalid NS AttributeValue")
            }
            if len(s) == 0 {
                return validation("One or more parameter values are not valid: An AttributeValue may not contain an empty set")
            }
            for _, e := range s {
                if !validNumberFormat(e) {
                    return validation("The parameter cannot be converted to a numeric value: " + e)
                }
            }
            v.Type, v.NS = "NS", s
        case "BS":
            var s []string
            if err := json.Unmarshal(body, &s); err != nil {
                return serializationErr("Invalid BS AttributeValue")
            }
            if len(s) == 0 {
                return validation("One or more parameter values are not valid: An AttributeValue may not contain an empty set")
            }
            decoded := make([][]byte, 0, len(s))
            for _, e := range s {
                b, err := base64.StdEncoding.DecodeString(e)
                if err != nil {
                    return serializationErr("Invalid BS AttributeValue")
                }
                decoded = append(decoded, b)
            }
            v.Type, v.BS = "BS", decoded
        case "M":
            var m map[string]AttrVal
            if err := json.Unmarshal(body, &m); err != nil {
                return serializationErr("Invalid M AttributeValue")
            }
            v.Type, v.M = "M", m
        case "L":
            var l []AttrVal
            if err := json.Unmarshal(body, &l); err != nil {
                return serializationErr("Invalid L AttributeValue")
            }
            v.Type, v.L = "L", l
        case "BOOL":
            var b bool
            if err := json.Unmarshal(body, &b); err != nil {
                return serializationErr("Invalid BOOL AttributeValue")
            }
            v.Type, v.BOOL = "BOOL", b
        case "NULL":
            var b bool
            if err := json.Unmarshal(body, &b); err != nil {
                return serializationErr("Invalid NULL AttributeValue")
            }
            v.Type, v.Null = "NULL", true
        default:
            return serializationErr("Unknown AttributeValue type: " + typeKey)
        }
    }
    return nil
}

// validNumberFormat checks the DynamoDB number syntax:
// -?(0|[1-9][0-9]*)(\.[0-9]*)?([eE][+-]?[0-9]+)?
func validNumberFormat(s string) bool {
    i, n := 0, len(s)
    if n == 0 {
        return false
    }
    if s[i] == '-' {
        i++
    }
    start := i
    for i < n && s[i] >= '0' && s[i] <= '9' {
        i++
    }
    intDigits := i - start
    if intDigits == 0 {
        return false
    }
    if intDigits > 1 && s[start] == '0' {
        return false
    }
    if i < n && s[i] == '.' {
        i++
        for i < n && s[i] >= '0' && s[i] <= '9' {
            i++
        }
    }
    if i < n && (s[i] == 'e' || s[i] == 'E') {
        i++
        if i < n && (s[i] == '+' || s[i] == '-') {
            i++
        }
        es := i
        for i < n && s[i] >= '0' && s[i] <= '9' {
            i++
        }
        if i == es {
            return false
        }
    }
    return i == n
}

// compareNumbers compares two DynamoDB number representations exactly.
func compareNumbers(a, b string) (int, bool) {
    ra, ok := new(big.Rat).SetString(a)
    if !ok {
        return 0, false
    }
    rb, ok := new(big.Rat).SetString(b)
    if !ok {
        return 0, false
    }
    return ra.Cmp(rb), true
}

// copyAttr returns a deep copy of v.
func copyAttr(v AttrVal) AttrVal {
    switch v.Type {
    case "B":
        v.B = append([]byte(nil), v.B...)
    case "SS":
        v.SS = append([]string(nil), v.SS...)
    case "NS":
        v.NS = append([]string(nil), v.NS...)
    case "BS":
        out := make([][]byte, len(v.BS))
        for i, b := range v.BS {
            out[i] = append([]byte(nil), b...)
        }
        v.BS = out
    case "M":
        if v.M != nil {
            out := make(map[string]AttrVal, len(v.M))
            for k, mv := range v.M {
                out[k] = copyAttr(mv)
            }
            v.M = out
        }
    case "L":
        if v.L != nil {
            out := make([]AttrVal, len(v.L))
            for i, lv := range v.L {
                out[i] = copyAttr(lv)
            }
            v.L = out
        }
    }
    return v
}

// copyItem deep copies an attribute map.
func copyItem(m map[string]AttrVal) map[string]AttrVal {
    out := make(map[string]AttrVal, len(m))
    for k, v := range m {
        out[k] = copyAttr(v)
    }
    return out
}

func containsString(list []string, s string) bool {
    for _, e := range list {
        if e == s {
            return true
        }
    }
    return false
}

// attrEqual implements DynamoDB equality semantics (sets compare by
// membership, numbers by numeric value).
func attrEqual(a, b AttrVal) bool {
    if a.Type != b.Type {
        return false
    }
    switch a.Type {
    case "S":
        return a.S == b.S
    case "N":
        c, ok := compareNumbers(a.N, b.N)
        return ok && c == 0
    case "B":
        return bytes.Equal(a.B, b.B)
    case "SS":
        if len(a.SS) != len(b.SS) {
            return false
        }
        for _, e := range a.SS {
            if !containsString(b.SS, e) {
                return false
            }
        }
        return true
    case "NS":
        if len(a.NS) != len(b.NS) {
            return false
        }
        for _, e := range a.NS {
            found := false
            for _, f := range b.NS {
                if c, ok := compareNumbers(e, f); ok && c == 0 {
                    found = true
                    break
                }
            }
            if !found {
                return false
            }
        }
        return true
    case "BS":
        if len(a.BS) != len(b.BS) {
            return false
        }
        for _, e := range a.BS {
            found := false
            for _, f := range b.BS {
                if bytes.Equal(e, f) {
                    found = true
                    break
                }
            }
            if !found {
                return false
            }
        }
        return true
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
    case "BOOL":
        return a.BOOL == b.BOOL
    case "NULL":
        return true
    }
    return false
}

func sortNS(list []string) {
    sort.Slice(list, func(i, j int) bool {
        c, _ := compareNumbers(list[i], list[j])
        return c < 0
    })
}

func compareStringSlices(a, b []string) int {
    for i := 0; i < len(a); i++ {
        if i >= len(b) {
            return 1
        }
        if c := strings.Compare(a[i], b[i]); c != 0 {
            return c
        }
    }
    if len(a) < len(b) {
        return -1
    }
    return 0
}

// compareAttr orders two values. ok is false when the pair is not orderable
// (different types, or a type without an ordering).
func compareAttr(a, b AttrVal) (int, bool) {
    if a.Type != b.Type {
        return 0, false
    }
    switch a.Type {
    case "S":
        return strings.Compare(a.S, b.S), true
    case "B":
        return bytes.Compare(a.B, b.B), true
    case "N":
        return compareNumbers(a.N, b.N)
    case "SS":
        ca := append([]string(nil), a.SS...)
        cb := append([]string(nil), b.SS...)
        sort.Strings(ca)
        sort.Strings(cb)
        return compareStringSlices(ca, cb), true
    case "NS":
        ca := append([]string(nil), a.NS...)
        cb := append([]string(nil), b.NS...)
        sortNS(ca)
        sortNS(cb)
        for i := 0; i < len(ca); i++ {
            if i >= len(cb) {
                return 1, true
            }
            c, _ := compareNumbers(ca[i], cb[i])
            if c != 0 {
                return c, true
            }
        }
        if len(ca) < len(cb) {
            return -1, true
        }
        return 0, true
    case "BS":
        ca := append([][]byte(nil), a.BS...)
        cb := append([][]byte(nil), b.BS...)
        sort.Slice(ca, func(i, j int) bool { return bytes.Compare(ca[i], ca[j]) < 0 })
        sort.Slice(cb, func(i, j int) bool { return bytes.Compare(cb[i], cb[j]) < 0 })
        for i := 0; i < len(ca); i++ {
            if i >= len(cb) {
                return 1, true
            }
            if c := bytes.Compare(ca[i], cb[i]); c != 0 {
                return c, true
            }
        }
        if len(ca) < len(cb) {
            return -1, true
        }
        return 0, true
    case "L":
        for i := 0; i < len(a.L); i++ {
            if i >= len(b.L) {
                return 1, true
            }
            c, ok := compareAttr(a.L[i], b.L[i])
            if !ok {
                return 0, false
            }
            if c != 0 {
                return c, true
            }
        }
        if len(a.L) < len(b.L) {
            return -1, true
        }
        return 0, true
    }
    return 0, false
}

// attrValSize approximates the DynamoDB size of an attribute value in bytes.
func attrValSize(v AttrVal) int {
    switch v.Type {
    case "S":
        return len(v.S)
    case "N":
        return len(v.N)
    case "B":
        return len(v.B)
    case "SS":
        total := 3
        for _, e := range v.SS {
            total += len(e)
        }
        return total
    case "NS":
        total := 3
        for _, e := range v.NS {
            total += len(e)
        }
        return total
    case "BS":
        total := 3
        for _, e := range v.BS {
            total += len(e)
        }
        return total
    case "M":
        total := 3
        for k, mv := range v.M {
            total += len(k) + attrValSize(mv)
        }
        return total
    case "L":
        total := 3
        for _, lv := range v.L {
            total += attrValSize(lv)
        }
        return total
    case "BOOL", "NULL":
        return 1
    default:
        return 0
    }
}

// itemSize computes the DynamoDB size of an item in bytes.
func itemSize(item map[string]AttrVal) int {
    total := 0
    for name, v := range item {
        total += len(name) + attrValSize(v)
    }
    return total
}

// keyString renders a canonical representation of a primary key, used for
// duplicate detection inside a batch request.
func keyString(m map[string]AttrVal) string {
    names := make([]string, 0, len(m))
    for n := range m {
        names = append(names, n)
    }
    sort.Strings(names)
    var sb strings.Builder
    for _, n := range names {
        v := m[n]
        sb.WriteString(n)
        sb.WriteByte(':')
        sb.WriteString(v.Type)
        sb.WriteByte(':')
        switch v.Type {
        case "S":
            sb.WriteString(v.S)
        case "N":
            sb.WriteString(v.N)
        case "B":
            sb.WriteString(hex.EncodeToString(v.B))
        default:
            if raw, err := json.Marshal(v); err == nil {
                sb.WriteString(string(raw))
            }
        }
        sb.WriteByte('|')
    }
    return sb.String()
}

// sortedKeys returns the map keys in deterministic order.
func sortedKeys(m map[string]json.RawMessage) []string {
    keys := make([]string, 0, len(m))
    for k := range m {
        keys = append(keys, k)
    }
    sort.Strings(keys)
    return keys
}
