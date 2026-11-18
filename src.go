package events

// EventBridge event-pattern matcher, ported line-by-line from Floci's
// EventBridgeService.java (lines 1354-1503). The five translated methods are
// matchesNestedDetail, matchesDetailNode, matchesArrayField,
// matchesSingleElement and matchesExactValue; together they implement
// EventBridge content filtering with exact values, nested detail objects,
// array fields, single-element pattern arrays, and the numeric, prefix and
// exists operators.

import (
	"encoding/json"
	"strings"
)

// operatorKeys lists the content-filter operators recognized inside a pattern
// object; every other key is treated as a nested event field name.
var operatorKeys = map[string]bool{
	"prefix":  true,
	"numeric": true,
	"exists":  true,
}

// MatchEventPattern reports whether event satisfies an EventBridge event
// pattern. Both arguments may be pre-decoded JSON values (map[string]interface{},
// []interface{}, string, float64, bool, nil) or raw JSON ([]byte,
// json.RawMessage, or a JSON document held in a string). A pattern matches an
// event when every pattern key matches the corresponding event value.
func MatchEventPattern(event interface{}, pattern interface{}) bool {
	patternObject, ok := decodeJSONValue(pattern).(map[string]interface{})
	if !ok {
		return false
	}
	return matchesNestedDetail(patternObject, decodeJSONValue(event))
}

// decodeJSONValue decodes raw JSON payloads so the matcher always works on
// decoded values; already-decoded values are passed through unchanged.
func decodeJSONValue(value interface{}) interface{} {
	var decoded interface{}
	switch v := value.(type) {
	case []byte:
		if json.Unmarshal(v, &decoded) == nil {
			return decoded
		}
	case json.RawMessage:
		if json.Unmarshal(v, &decoded) == nil {
			return decoded
		}
	case string:
		if json.Unmarshal([]byte(v), &decoded) == nil {
			return decoded
		}
	}
	return value
}

// matchesNestedDetail walks a pattern object over an event value, mirroring
// Floci's matchesNestedDetail. Operator keys (prefix, numeric, exists) filter
// the value at the current position, any other key must exist in the event
// object and match recursively, and all keys must match (AND semantics).
func matchesNestedDetail(pattern map[string]interface{}, value interface{}) bool {
	event, isEventMap := value.(map[string]interface{})
	for key, patternValue := range pattern {
		if operatorKeys[key] {
			if !matchOperator(key, patternValue, value) {
				return false
			}
			continue
		}
		if !isEventMap {
			return false
		}
		eventValue, present := event[key]
		if !present {
			if missingKeyMatches(patternValue) {
				continue
			}
			return false
		}
		if !matchesDetailNode(patternValue, eventValue) {
			return false
		}
	}
	return true
}

// matchesDetailNode dispatches one pattern node against one event value,
// mirroring Floci's matchesDetailNode. Event arrays are matched field-wise,
// pattern arrays behave as an OR over their elements, pattern objects recurse
// into nested detail or operator filtering, and scalars use exact equality.
func matchesDetailNode(pattern interface{}, value interface{}) bool {
	if eventArray, isArray := value.([]interface{}); isArray {
		return matchesArrayField(pattern, eventArray)
	}
	switch p := pattern.(type) {
	case []interface{}:
		for _, patternElement := range p {
			if matchesSingleElement(patternElement, value) {
				return true
			}
		}
		return false
	case map[string]interface{}:
		return matchesNestedDetail(p, value)
	default:
		return matchesExactValue(pattern, value)
	}
}

// matchesArrayField matches a pattern against an event array field, mirroring
// Floci's matchesArrayField: any single element of the array satisfying the
// pattern is enough for the field to match.
func matchesArrayField(pattern interface{}, eventArray []interface{}) bool {
	for _, element := range eventArray {
		if matchesSingleElement(pattern, element) {
			return true
		}
	}
	return false
}

// matchesSingleElement matches a single pattern value - a scalar, an operator
// object, a nested detail object, or a sub-array of values - against a single
// event value, mirroring Floci's matchesSingleElement. Structurally equal
// values match exactly before the pattern-specific rules are applied.
func matchesSingleElement(pattern interface{}, value interface{}) bool {
	if matchesExactValue(pattern, value) {
		return true
	}
	switch p := pattern.(type) {
	case map[string]interface{}:
		return matchesNestedDetail(p, value)
	case []interface{}:
		for _, patternElement := range p {
			if matchesSingleElement(patternElement, value) {
				return true
			}
		}
		return false
	default:
		return matchesExactValue(pattern, value)
	}
}

// matchesExactValue compares a pattern value against an event value for
// equality, mirroring Floci's matchesExactValue. Numbers compare numerically
// (1, 1.0 and 1e0 are equal); other scalars, arrays and objects compare
// recursively and case-sensitively.
func matchesExactValue(pattern interface{}, value interface{}) bool {
	switch p := pattern.(type) {
	case map[string]interface{}:
		v, ok := value.(map[string]interface{})
		if !ok || len(p) != len(v) {
			return false
		}
		for key, patternValue := range p {
			eventValue, present := v[key]
			if !present || !matchesExactValue(patternValue, eventValue) {
				return false
			}
		}
		return true
	case []interface{}:
		v, ok := value.([]interface{})
		if !ok || len(p) != len(v) {
			return false
		}
		for i := range p {
			if !matchesExactValue(p[i], v[i]) {
				return false
			}
		}
		return true
	default:
		return exactScalarEqual(pattern, value)
	}
}

// matchOperator applies one content-filter operator to an event value.
func matchOperator(key string, patternValue interface{}, value interface{}) bool {
	switch key {
	case "prefix":
		return matchPrefix(patternValue, value)
	case "numeric":
		return matchNumeric(patternValue, value)
	case "exists":
		return matchExists(patternValue, value)
	}
	return false
}

// matchPrefix implements {"prefix": ...} for string event values. The pattern
// value may be a single prefix or an array of prefixes, any of which matches.
func matchPrefix(patternValue interface{}, value interface{}) bool {
	s, ok := value.(string)
	if !ok {
		return false
	}
	switch p := patternValue.(type) {
	case string:
		return strings.HasPrefix(s, p)
	case []interface{}:
		for _, element := range p {
			if prefix, ok := element.(string); ok && strings.HasPrefix(s, prefix) {
				return true
			}
		}
	}
	return false
}

// matchNumeric implements {"numeric": [op, number, ...]} using the EventBridge
// operators "<", "<=", "=", ">=" and ">". Every operator/number pair must
// hold for the event's numeric value to match, so compound ranges work.
func matchNumeric(patternValue interface{}, value interface{}) bool {
	pairs, ok := patternValue.([]interface{})
	if !ok || len(pairs) == 0 || len(pairs)%2 != 0 {
		return false
	}
	number, ok := numericValue(value)
	if !ok {
		return false
	}
	for i := 0; i+1 < len(pairs); i += 2 {
		operator, ok := pairs[i].(string)
		if !ok {
			return false
		}
		bound, ok := numericValue(pairs[i+1])
		if !ok || !compareNumeric(operator, number, bound) {
			return false
		}
	}
	return true
}

// compareNumeric evaluates a single numeric content-filter operator.
func compareNumeric(operator string, left, right float64) bool {
	switch operator {
	case "<":
		return left < right
	case "<=":
		return left <= right
	case "=":
		return left == right
	case ">=":
		return left >= right
	case ">":
		return left > right
	}
	return false
}

// matchExists implements {"exists": bool}. exists:true matches any field that
// is present; exists:false matches fields that are absent (short-circuited in
// matchesNestedDetail) or present with a null value.
func matchExists(patternValue interface{}, value interface{}) bool {
	want, ok := patternValue.(bool)
	if !ok {
		return false
	}
	if want {
		return true
	}
	return value == nil
}

// missingKeyMatches reports whether a pattern value matches an event object
// key that is absent, which is only ever true for exists:false filters.
func missingKeyMatches(patternValue interface{}) bool {
	switch p := patternValue.(type) {
	case map[string]interface{}:
		want, ok := p["exists"].(bool)
		return ok && !want
	case []interface{}:
		for _, element := range p {
			if missingKeyMatches(element) {
				return true
			}
		}
	}
	return false
}

// exactScalarEqual compares scalar values; numbers compare numerically so
// different JSON encodings of the same number stay equal.
func exactScalarEqual(a interface{}, b interface{}) bool {
	if left, ok := numericValue(a); ok {
		right, ok := numericValue(b)
		return ok && left == right
	}
	switch left := a.(type) {
	case string:
		right, ok := b.(string)
		return ok && left == right
	case bool:
		right, ok := b.(bool)
		return ok && left == right
	case nil:
		return b == nil
	}
	return false
}

// numericValue extracts a float64 from every numeric representation a JSON
// adapter can produce: float32/float64, all integer widths and json.Number.
func numericValue(value interface{}) (float64, bool) {
	switch n := value.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}

// Exported wrappers so callers outside the events package can exercise the
// translated Floci methods directly.

// MatchesNestedDetail reports whether a pattern object matches an event value.
func MatchesNestedDetail(pattern map[string]interface{}, value interface{}) bool {
	return matchesNestedDetail(pattern, value)
}

// MatchesDetailNode dispatches a pattern node against an event value.
func MatchesDetailNode(pattern interface{}, value interface{}) bool {
	return matchesDetailNode(pattern, value)
}

// MatchesArrayField matches a pattern against an event array field.
func MatchesArrayField(pattern interface{}, eventArray []interface{}) bool {
	return matchesArrayField(pattern, eventArray)
}

// MatchesSingleElement matches a single pattern value against one event value.
func MatchesSingleElement(pattern interface{}, value interface{}) bool {
	return matchesSingleElement(pattern, value)
}

// MatchesExactValue compares a pattern value and an event value for equality.
func MatchesExactValue(pattern interface{}, value interface{}) bool {
	return matchesExactValue(pattern, value)
}
