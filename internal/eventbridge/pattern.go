// Package eventbridge implements EventBridge event-pattern matching.
//
// It is a Go translation of the recursive JSON matcher in Floci's
// EventBridgeService.java (matchesNestedDetail, matchesDetailNode,
// matchesArrayField, matchesSingleElement and matchesExactValue,
// lines 1354-1503). Supported pattern constructs:
//
//   - exact-value: a pattern array listing acceptable values, e.g.
//     {"source": ["aws.ec2", "aws.sns"]}; a scalar pattern value is also
//     accepted as a shorthand for a single candidate.
//   - nested detail: a pattern object recursed field-by-field against the
//     event object (this is how {"detail": {...}} is matched); the event
//     must carry every field the pattern names.
//   - array-field: if the event value is itself a JSON array (e.g.
//     "resources"), the field matches when any single element matches.
//   - numeric: {"numeric": [">=", 0, "<", 100]}, with operators <, <=, >,
//     >= and = applied as a conjunction.
//   - prefix: {"prefix": "arn:aws:ec2:"} against a string event value.
//   - exists: {"exists": true} requires the field to be present,
//     {"exists": false} requires its absence.
package eventbridge

import (
	"bytes"
	"encoding/json"
	"errors"
	"math/big"
	"strconv"
	"strings"
)

// Matches reports whether a decoded event satisfies a decoded event
// pattern. Only the fields named by the pattern are examined; every other
// event field is ignored. JSON numbers compare numerically, so 1 matches
// 1.0.
func Matches(pattern, event map[string]any) bool {
	if len(pattern) == 0 {
		return true
	}
	for key, patternValue := range pattern {
		// An absent event field arrives here with present=false; only
		// an {"exists": false} matcher can still match it.
		eventValue, present := event[key]
		if !matchesDetailNode(patternValue, eventValue, present) {
			return false
		}
	}
	return true
}

// matchesNestedDetail matches a pattern object field-by-field against an
// event object: every field named by the pattern must be present in the
// event and match, while event fields the pattern does not mention are
// ignored.
func matchesNestedDetail(patternObject, eventObject map[string]any) bool {
	for key, patternValue := range patternObject {
		eventValue, present := eventObject[key]
		if !matchesDetailNode(patternValue, eventValue, present) {
			return false
		}
	}
	return true
}

// matchesDetailNode matches one pattern value against one event value.
// Pattern arrays go to matchesArrayField; pattern objects are either a
// matcher (numeric/prefix/exists) or a nested pattern object that recurses
// through matchesNestedDetail; anything else must equal the event value
// exactly. present reports whether the event object carried the field at
// all.
func matchesDetailNode(patternValue, eventValue any, present bool) bool {
	switch pattern := patternValue.(type) {
	case []any:
		return matchesArrayField(pattern, eventValue, present)
	case map[string]any:
		// A matcher object is recognized by its matcher key; any other
		// object is a nested pattern matched field-by-field.
		if raw, ok := pattern["numeric"]; ok {
			return matchesNumeric(raw, eventValue, present)
		}
		if raw, ok := pattern["prefix"]; ok {
			return matchesPrefix(raw, eventValue, present)
		}
		if raw, ok := pattern["exists"]; ok {
			return matchesExists(raw, present)
		}
		eventObject, ok := eventValue.(map[string]any)
		if !present || !ok {
			return false
		}
		return matchesNestedDetail(pattern, eventObject)
	default:
		return present && matchesExactValue(patternValue, eventValue)
	}
}

// matchesArrayField matches a pattern array of candidate values against
// the event value. A scalar event value matches when it equals - or
// satisfies a matcher among - the candidates. An event value that is
// itself a JSON array (e.g. "resources") matches when any single element
// matches; the pattern list is never required to match the event array
// element-for-element.
func matchesArrayField(pattern []any, eventValue any, present bool) bool {
	if !present {
		return false
	}
	if eventElements, ok := eventValue.([]any); ok {
		for _, element := range eventElements {
			if matchesSingleElement(element, pattern) {
				return true
			}
		}
		return false
	}
	for _, candidate := range pattern {
		if matchesDetailNode(candidate, eventValue, present) {
			return true
		}
	}
	return false
}

// matchesSingleElement matches one element of an event array against the
// pattern's candidate list; an array-valued field matches when a single
// element of it matches.
func matchesSingleElement(element any, pattern []any) bool {
	for _, candidate := range pattern {
		if matchesDetailNode(candidate, element, true) {
			return true
		}
	}
	return false
}

// matchesExactValue reports whether the event value equals the pattern
// value. JSON numbers compare numerically (1 matches 1.0); strings,
// booleans and null compare on type and value; values of different types
// never match.
func matchesExactValue(patternValue, eventValue any) bool {
	switch pattern := patternValue.(type) {
	case nil:
		return eventValue == nil
	case string:
		event, ok := eventValue.(string)
		return ok && event == pattern
	case bool:
		event, ok := eventValue.(bool)
		return ok && event == pattern
	}
	if patternNumber, ok := asNumber(patternValue); ok {
		eventNumber, ok := asNumber(eventValue)
		return ok && patternNumber.Cmp(eventNumber) == 0
	}
	return false
}

// matchesNumeric evaluates {"numeric": ["<", 0, ">=", 10, ...]}. The
// alternating operator/operand pairs form a conjunction; the event value
// must be a JSON number that satisfies every comparison. Unknown
// operators, non-numeric operands and odd-length lists do not match.
func matchesNumeric(matcher any, eventValue any, present bool) bool {
	if !present {
		return false
	}
	terms, ok := matcher.([]any)
	if !ok || len(terms)%2 != 0 {
		return false
	}
	value, ok := asNumber(eventValue)
	if !ok {
		return false
	}
	for i := 0; i < len(terms); i += 2 {
		operator, ok := terms[i].(string)
		if !ok {
			return false
		}
		bound, ok := asNumber(terms[i+1])
		if !ok {
			return false
		}
		comparison := value.Cmp(bound)
		switch operator {
		case "<":
			if comparison >= 0 {
				return false
			}
		case "<=":
			if comparison > 0 {
				return false
			}
		case ">":
			if comparison <= 0 {
				return false
			}
		case ">=":
			if comparison < 0 {
				return false
			}
		case "=":
			if comparison != 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// matchesPrefix evaluates {"prefix": "..."}: the event value must be a
// string with the given prefix.
func matchesPrefix(matcher any, eventValue any, present bool) bool {
	if !present {
		return false
	}
	prefix, ok := matcher.(string)
	if !ok {
		return false
	}
	value, ok := eventValue.(string)
	return ok && strings.HasPrefix(value, prefix)
}

// matchesExists evaluates {"exists": true|false} against the presence of
// the field in the enclosing event object.
func matchesExists(matcher any, present bool) bool {
	want, ok := matcher.(bool)
	if !ok {
		want = false
	}
	return want == present
}

// asNumber coerces a decoded JSON value (or a plain Go numeric) into an
// exact rational for comparison. Only real numbers convert; the string
// "5" does not.
func asNumber(value any) (*big.Rat, bool) {
	switch number := value.(type) {
	case json.Number:
		rat, ok := new(big.Rat).SetString(number.String())
		if !ok {
			return nil, false
		}
		return rat, true
	case float64:
		rat, ok := new(big.Rat).SetString(strconv.FormatFloat(number, 'f', -1, 64))
		if !ok {
			return nil, false
		}
		return rat, true
	case int:
		return new(big.Rat).SetInt64(int64(number)), true
	case int64:
		return new(big.Rat).SetInt64(number), true
	}
	return nil, false
}

// decodeObject decodes raw JSON as a JSON object, preserving numeric
// literals as json.Number so Matches compares numbers exactly.
func decodeObject(data []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("eventbridge: expected a JSON object")
	}
	return object, nil
}

// MatchJSON decodes the pattern and event JSON and reports whether the
// event satisfies the pattern. Numbers are decoded with UseNumber so
// pattern matching is exact; this is the entry point for target-routing
// code that holds events as raw JSON.
func MatchJSON(pattern, event []byte) (bool, error) {
	patternObject, err := decodeObject(pattern)
	if err != nil {
		return false, err
	}
	eventObject, err := decodeObject(event)
	if err != nil {
		return false, err
	}
	return Matches(patternObject, eventObject), nil
}
