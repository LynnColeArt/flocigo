// floci-go - EventBridge module.
//
// eventbridge_pattern.go implements EventBridge event-pattern validation and
// matching: exact matches, OR arrays, plus the prefix / suffix / wildcard /
// cidr / numeric / exists / anything-but operators. Shape violations surfaced
// here become InvalidEventPatternException on the wire; the same rules govern
// TestEventPattern and the patterns stored by PutRule.
package main

import (
    "encoding/json"
    "fmt"
    "net"
    "sort"
    "strings"
)

// Only these keys are legal at the top level of an event pattern.
var ebPatternTopKeys = map[string]struct{}{
    "id":          {},
    "source":      {},
    "detail-type": {},
    "detail":      {},
    "resources":   {},
    "time":        {},
    "region":      {},
    "account":     {},
}

// Operator keys: an object of exactly one of these is an operator node; an
// object mixing an operator key with other keys is invalid.
var ebPatternOps = map[string]struct{}{
    "prefix":       {},
    "suffix":       {},
    "wildcard":     {},
    "numeric":      {},
    "cidr":         {},
    "exists":       {},
    "anything-but": {},
}

var ebNumericOps = map[string]struct{}{
    "=":  {},
    ">":  {},
    ">=": {},
    "<":  {},
    "<=": {},
}

// ebPatternError is surfaced as InvalidEventPatternException over the wire.
type ebPatternError struct{ msg string }

func (e *ebPatternError) Error() string { return e.msg }

func ebPatternFail(format string, args ...interface{}) error {
    return &ebPatternError{msg: fmt.Sprintf(format, args...)}
}

// ebParsePattern validates a JSON event pattern and returns its parsed form.
// The empty string is tolerated as the match-all pattern ({}), mirroring
// PutRule semantics; TestEventPattern uses ebParsePatternRequired instead.
func ebParsePattern(raw string) (map[string]interface{}, error) {
    trimmed := strings.TrimSpace(raw)
    if trimmed == "" {
        return map[string]interface{}{}, nil
    }
    var root interface{}
    if err := json.Unmarshal([]byte(trimmed), &root); err != nil {
        return nil, ebPatternFail("Event pattern is not valid JSON: %v", err)
    }
    obj, ok := root.(map[string]interface{})
    if !ok {
        return nil, ebPatternFail("Event pattern must be a JSON object")
    }
    if err := ebValidateTopPattern(obj); err != nil {
        return nil, err
    }
    return obj, nil
}

// ebParsePatternRequired is TestEventPattern oriented: the pattern parameter
// must carry a real pattern object, so an empty string is invalid there.
func ebParsePatternRequired(raw string) (map[string]interface{}, error) {
    if strings.TrimSpace(raw) == "" {
        return nil, ebPatternFail("EventPattern must be a JSON object")
    }
    return ebParsePattern(raw)
}

func ebValidateTopPattern(obj map[string]interface{}) error {
    keys := make([]string, 0, len(obj))
    for key := range obj {
        keys = append(keys, key)
    }
    sort.Strings(keys)
    for _, key := range keys {
        if _, ok := ebPatternTopKeys[key]; !ok {
            return ebPatternFail("Key '%s' is not supported in event pattern", key)
        }
        if key == "detail" {
            sub, ok := obj[key].(map[string]interface{})
            if !ok {
                return ebPatternFail("Value of field 'detail' must be a JSON object")
            }
            if err := ebValidatePatternObject("detail", sub, true); err != nil {
                return err
            }
            continue
        }
        if err := ebValidatePatternValue(key, obj[key], false); err != nil {
            return err
        }
    }
    return nil
}

// ebValidatePatternValue validates the value bound to a field: a scalar, an
// OR-array or an operator object. Inside the detail subtree nested object
// literals are legal.
func ebValidatePatternValue(path string, value interface{}, insideDetail bool) error {
    switch v := value.(type) {
    case string, float64, bool, nil:
        return nil
    case []interface{}:
        for _, elem := range v {
            if err := ebValidatePatternValue(path, elem, insideDetail); err != nil {
                return err
            }
        }
        return nil
    case map[string]interface{}:
        return ebValidatePatternObject(path, v, insideDetail)
    default:
        return ebPatternFail("Value of field '%s' has an unsupported type", path)
    }
}

// ebValidatePatternObject treats obj either as a single-operator object or as
// a literal object (only legal inside the detail subtree).
func ebValidatePatternObject(path string, obj map[string]interface{}, insideDetail bool) error {
    if len(obj) == 1 {
        for op, arg := range obj {
            if _, isOp := ebPatternOps[op]; isOp {
                return ebValidateOperator(path, op, arg, insideDetail)
            }
        }
    } else {
        for key := range obj {
            if _, isOp := ebPatternOps[key]; isOp {
                return ebPatternFail("Operator '%s' cannot be combined with other keys", key)
            }
        }
    }
    if !insideDetail {
        return ebPatternFail("Value of field '%s' must be a scalar or an array of scalars", path)
    }
    childKeys := make([]string, 0, len(obj))
    for key := range obj {
        childKeys = append(childKeys, key)
    }
    sort.Strings(childKeys)
    for _, key := range childKeys {
        if err := ebValidatePatternValue(path+"."+key, obj[key], insideDetail); err != nil {
            return err
        }
    }
    return nil
}

func ebValidateOperator(path, op string, arg interface{}, insideDetail bool) error {
    switch op {
    case "prefix", "suffix", "wildcard":
        if _, ok := arg.(string); !ok {
            return ebPatternFail("Operator '%s' of field '%s' requires a string value", op, path)
        }
    case "cidr":
        cidr, ok := arg.(string)
        if !ok {
            return ebPatternFail("Operator 'cidr' of field '%s' requires a string value", path)
        }
        if _, _, err := net.ParseCIDR(cidr); err != nil {
            return ebPatternFail("Operator 'cidr' of field '%s' requires a valid CIDR block", path)
        }
    case "exists":
        if _, ok := arg.(bool); !ok {
            return ebPatternFail("Operator 'exists' of field '%s' requires a boolean value", path)
        }
        if !insideDetail {
            return ebPatternFail("Operator 'exists' of field '%s' is only supported inside the 'detail' object", path)
        }
    case "numeric":
        return ebValidateNumeric(path, arg)
    case "anything-but":
        return ebValidateAnythingBut(path, arg)
    }
    return nil
}

func ebValidateNumeric(path string, arg interface{}) error {
    list, ok := arg.([]interface{})
    if !ok || (len(list) != 2 && len(list) != 4) {
        return ebPatternFail("Operator 'numeric' of field '%s' requires an array of 2 or 4 elements", path)
    }
    for i := 0; i+1 < len(list); i += 2 {
        op, ok := list[i].(string)
        if !ok {
            return ebPatternFail("Operator 'numeric' of field '%s' requires a string comparison operator", path)
        }
        if _, ok := ebNumericOps[op]; !ok {
            return ebPatternFail("Operator 'numeric' of field '%s' does not support comparison '%v'", path, op)
        }
        if _, ok := list[i+1].(float64); !ok {
            return ebPatternFail("Operator 'numeric' of field '%s' requires numeric comparison values", path)
        }
    }
    return nil
}

func ebValidateAnythingBut(path string, arg interface{}) error {
    switch v := arg.(type) {
    case string, float64, bool:
        return nil
    case []interface{}:
        if len(v) == 0 {
            return ebPatternFail("Operator 'anything-but' of field '%s' requires a non-empty array", path)
        }
        for _, elem := range v {
            switch elem.(type) {
            case string, float64, bool:
            default:
                return ebPatternFail("Operator 'anything-but' of field '%s' only supports scalar array elements", path)
            }
        }
        return nil
    case map[string]interface{}:
        if len(v) != 1 {
            return ebPatternFail("Operator 'anything-but' of field '%s' requires exactly one filter operator", path)
        }
        for key, filter := range v {
            switch key {
            case "prefix", "suffix", "wildcard", "cidr":
                switch filtered := filter.(type) {
                case string:
                case []interface{}:
                    if len(filtered) == 0 {
                        return ebPatternFail("Operator 'anything-but' of field '%s' requires a non-empty array", path)
                    }
                    for _, elem := range filtered {
                        if _, ok := elem.(string); !ok {
                            return ebPatternFail("Operator 'anything-but' of field '%s' requires string array elements", path)
                        }
                    }
                default:
                    return ebPatternFail("Operator 'anything-but' of field '%s' requires a string or array of strings for '%s'", path, key)
                }
            case "numeric":
                return ebValidateNumeric(path, filter)
            default:
                return ebPatternFail("Operator 'anything-but' of field '%s' does not support filter '%s'", path, key)
            }
        }
        return nil
    default:
        return ebPatternFail("Operator 'anything-but' of field '%s' has an unsupported value", path)
    }
}

// ---------------------------------------------------------------------------
// Matching
// ---------------------------------------------------------------------------

// ebField carries an event value plus its presence (required by 'exists').
type ebField struct {
    value   interface{}
    present bool
}

// ebPatternMatches reports whether an event envelope matches a parsed pattern.
// Every pattern key must match (AND); pattern arrays are OR lists.
func ebPatternMatches(pattern map[string]interface{}, event map[string]interface{}) bool {
    for key, pval := range pattern {
        var field ebField
        if raw, ok := event[key]; ok {
            field = ebField{value: raw, present: true}
        }
        if !ebMatchPatternValue(pval, field) {
            return false
        }
    }
    return true
}

func ebMatchPatternValue(pval interface{}, field ebField) bool {
    if list, ok := pval.([]interface{}); ok {
        for _, elem := range list {
            if ebMatchPatternValue(elem, field) {
                return true
            }
        }
        return false
    }
    return ebMatchScalarOrOperator(pval, field)
}

func ebMatchScalarOrOperator(pval interface{}, field ebField) bool {
    if obj, ok := pval.(map[string]interface{}); ok {
        if len(obj) == 1 {
            for op, arg := range obj {
                if _, isOp := ebPatternOps[op]; isOp {
                    return ebApplyOperator(op, arg, field)
                }
            }
        }
        // Literal object: the event side must be an object too.
        eventObj, ok := field.value.(map[string]interface{})
        if !ok {
            return false
        }
        for key, pv := range obj {
            var sub ebField
            if raw, present := eventObj[key]; present {
                sub = ebField{value: raw, present: true}
            }
            if !ebMatchPatternValue(pv, sub) {
                return false
            }
        }
        return true
    }
    return ebMatchScalar(pval, field)
}

func ebMatchScalar(pval interface{}, field ebField) bool {
    if list, ok := field.value.([]interface{}); ok {
        for _, elem := range list {
            if ebScalarEqual(pval, elem) {
                return true
            }
        }
        return false
    }
    return ebScalarEqual(pval, field.value)
}

func ebScalarEqual(pattern, value interface{}) bool {
    switch p := pattern.(type) {
    case string:
        v, ok := value.(string)
        return ok && p == v
    case float64:
        v, ok := value.(float64)
        return ok && p == v
    case bool:
        v, ok := value.(bool)
        return ok && p == v
    case nil:
        return value == nil
    }
    return false
}

func ebApplyOperator(op string, arg interface{}, field ebField) bool {
    switch op {
    case "exists":
        want, _ := arg.(bool)
        return field.present == want
    case "prefix":
        return ebStringFieldMatch(field, func(s string) bool {
            prefix, _ := arg.(string)
            return strings.HasPrefix(s, prefix)
        })
    case "suffix":
        return ebStringFieldMatch(field, func(s string) bool {
            suffix, _ := arg.(string)
            return strings.HasSuffix(s, suffix)
        })
    case "wildcard":
        return ebStringFieldMatch(field, func(s string) bool {
            pattern, _ := arg.(string)
            return ebWildcardMatch(pattern, s)
        })
    case "cidr":
        return ebStringFieldMatch(field, func(s string) bool {
            cidr, _ := arg.(string)
            return ebCIDRMatch(cidr, s)
        })
    case "numeric":
        number, ok := field.value.(float64)
        list, isList := arg.([]interface{})
        if !ok || !isList {
            return false
        }
        return ebNumericCompare(list, number)
    case "anything-but":
        return !ebAnythingButMatches(arg, field)
    }
    return false
}

// ebStringFieldMatch applies pred to a string field; array fields match when
// at least one element satisfies pred.
func ebStringFieldMatch(field ebField, pred func(string) bool) bool {
    switch v := field.value.(type) {
    case string:
        return pred(v)
    case []interface{}:
        for _, elem := range v {
            if s, ok := elem.(string); ok && pred(s) {
                return true
            }
        }
        return false
    default:
        return false
    }
}

func ebNumericCompare(list []interface{}, number float64) bool {
    for i := 0; i+1 < len(list); i += 2 {
        op, _ := list[i].(string)
        limit, ok := list[i+1].(float64)
        if !ok {
            return false
        }
        switch op {
        case "=":
            if number != limit {
                return false
            }
        case ">":
            if !(number > limit) {
                return false
            }
        case ">=":
            if !(number >= limit) {
                return false
            }
        case "<":
            if !(number < limit) {
                return false
            }
        case "<=":
            if !(number <= limit) {
                return false
            }
        default:
            return false
        }
    }
    return true
}

// ebAnythingButMatches: the anything-but operator matches when none of the
// event values is selected by the filter.
func ebAnythingButMatches(arg interface{}, field ebField) bool {
    matched := false
    if list, ok := field.value.([]interface{}); ok {
        for _, elem := range list {
            if ebAnythingButElem(arg, elem) {
                matched = true
                break
            }
        }
    } else {
        matched = ebAnythingButElem(arg, field.value)
    }
    return matched
}

func ebAnythingButElem(arg interface{}, elem interface{}) bool {
    switch filter := arg.(type) {
    case string:
        s, ok := elem.(string)
        return ok && s == filter
    case float64:
        n, ok := elem.(float64)
        return ok && n == filter
    case bool:
        b, ok := elem.(bool)
        return ok && b == filter
    case []interface{}:
        for _, e := range filter {
            if ebAnythingButElem(e, elem) {
                return true
            }
        }
        return false
    case map[string]interface{}:
        for key, f := range filter {
            switch key {
            case "prefix":
                if ebStringFilterAny(f, elem, strings.HasPrefix) {
                    return true
                }
            case "suffix":
                if ebStringFilterAny(f, elem, strings.HasSuffix) {
                    return true
                }
            case "wildcard":
                if ebStringFilterAny(f, elem, func(s, pattern string) bool { return ebWildcardMatch(pattern, s) }) {
                    return true
                }
            case "cidr":
                if ebStringFilterAny(f, elem, func(s, pattern string) bool { return ebCIDRMatch(pattern, s) }) {
                    return true
                }
            case "numeric":
                if number, ok := elem.(float64); ok {
                    if list, isList := f.([]interface{}); isList && ebNumericCompare(list, number) {
                        return true
                    }
                }
            }
        }
        return false
    default:
        return false
    }
}

// ebStringFilterAny applies a string (or list of strings) filter to elem.
func ebStringFilterAny(filter interface{}, elem interface{}, pred func(value, pattern string) bool) bool {
    s, ok := elem.(string)
    if !ok {
        return false
    }
    switch f := filter.(type) {
    case string:
        return pred(s, f)
    case []interface{}:
        for _, e := range f {
            if pattern, ok := e.(string); ok && pred(s, pattern) {
                return true
            }
        }
        return false
    default:
        return false
    }
}

// ebWildcardMatch implements EventBridge wildcard matching: '*' matches any
// (possibly empty) run of characters, everything else is literal and case
// sensitive; the whole field must match.
func ebWildcardMatch(pattern, value string) bool {
    var (
        patternIndex int
        valueIndex   int
        starPattern  = -1
        starMatch    = 0
    )
    for valueIndex < len(value) {
        if patternIndex < len(pattern) && pattern[patternIndex] == '*' {
            starPattern = patternIndex
            starMatch = valueIndex
            patternIndex++
            continue
        }
        if patternIndex < len(pattern) && pattern[patternIndex] == value[valueIndex] {
            patternIndex++
            valueIndex++
            continue
        }
        if starPattern >= 0 {
            starMatch++
            valueIndex = starMatch
            patternIndex = starPattern + 1
            continue
        }
        return false
    }
    for patternIndex < len(pattern) && pattern[patternIndex] == '*' {
        patternIndex++
    }
    return patternIndex == len(pattern)
}

func ebCIDRMatch(cidr, value string) bool {
    ip := net.ParseIP(strings.TrimSpace(value))
    if ip == nil {
        return false
    }
    _, network, err := net.ParseCIDR(cidr)
    if err != nil {
        return false
    }
    return network.Contains(ip)
}
