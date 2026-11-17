// Package sfn is the Amazon States Language (ASL) emulator core used by
// floci-go. It implements the intrinsic functions, waitForTaskToken callback
// semantics (SendTaskSuccess/SendTaskFailure/SendTaskHeartbeat) and the
// CloudWatch-style execution history of AWS Step Functions.
package sfn

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Standard Step Functions state error names surfaced by this emulator.
const (
	// ErrNameParameterPathFormat covers malformed or unresolvable paths and
	// intrinsic functions inside Parameters / ItemSelector payloads.
	ErrNameParameterPathFormat = "States.ParameterPathFormatError"
	// ErrNameResultPathMatch covers ResultPath conflicts with non-object input.
	ErrNameResultPathMatch = "States.ResultPathMatchFailure"
	// ErrNameResultSelectorPath covers failed ResultSelector resolution.
	ErrNameResultSelectorPath = "States.ResultSelectorPathFailure"
	// ErrNameNoChoiceMatched is raised when no Choice rule matches and no Default.
	ErrNameNoChoiceMatched = "States.NoChoiceMatched"
	// ErrNameRuntime covers generic runtime path failures (InputPath/OutputPath).
	ErrNameRuntime = "States.Runtime"
	// ErrNameTimeout covers state-level and heartbeat timeouts.
	ErrNameTimeout = "States.Timeout"
	// ErrNameTaskFailed covers mock resource failures.
	ErrNameTaskFailed = "States.TaskFailed"
)

// stateError is the error type flowing through the interpreter; ErrName and
// Cause are what DescribeExecution and the history events report.
// machineScope errors (machine-level TimeoutSeconds) are not catchable.
type stateError struct {
	ErrName      string
	Cause        string
	machineScope bool
}

func newStateError(name, cause string) *stateError {
	return &stateError{ErrName: name, Cause: cause}
}

func (e *stateError) Error() string { return e.ErrName + ": " + e.Cause }

// wrapWith keeps already-typed errors intact and wraps everything else.
func wrapWith(err error, name string) *stateError {
	if se, ok := err.(*stateError); ok {
		return se
	}
	return newStateError(name, err.Error())
}

// decodeJSONValue decodes JSON into Go values using json.Number for numbers so
// integer fidelity (States.MathAdd, States.MathRandom, RetryCount) is kept.
func decodeJSONValue(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, errors.New("unexpected data after JSON value")
	}
	return v, nil
}

// jsonText serializes a Go value (with json.Number preserved) to compact JSON.
func jsonText(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "null"
	}
	return string(b)
}

func deepCopy(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = deepCopy(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = deepCopy(val)
		}
		return out
	default:
		return v
	}
}

func asNumber(v any) (json.Number, bool) {
	n, ok := v.(json.Number)
	return n, ok
}

func asFloat(v any) (float64, bool) {
	n, ok := asNumber(v)
	if !ok {
		return 0, false
	}
	f, err := n.Float64()
	if err != nil {
		return 0, false
	}
	return f, true
}

func isIntegralNumber(n json.Number) bool {
	return !strings.ContainsAny(n.String(), ".eE")
}

func numberInt64(n json.Number) (int64, bool) {
	if !isIntegralNumber(n) {
		return 0, false
	}
	i, err := strconv.ParseInt(n.String(), 10, 64)
	if err != nil {
		return 0, false
	}
	return i, true
}

var (
	errPathInvalid  = errors.New("invalid reference path")
	errPathNotFound = errors.New("path does not resolve to a value")
)

type pathSeg struct {
	key     string
	index   int
	isIndex bool
}

func isPathIdent(c byte) bool {
	return c == '_' || c == '-' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// isPathString reports whether a Parameters selector value must be treated as
// a reference path (the AWS rule: any value starting with $. or $.).
func isPathString(v string) bool {
	return v == "$" || v == "$$" || strings.HasPrefix(v, "$.") || strings.HasPrefix(v, "$$.")
}

// parsePathBody parses the body of a reference path: "$", "$$", dotted names,
// [index] and ['quoted key'] segments used by InputPath, OutputPath,
// ResultPath, Choice variables, Wait paths and intrinsic arguments.
func parsePathBody(path string) (ctxRoot bool, segs []pathSeg, err error) {
	if path == "$" {
		return false, nil, nil
	}
	if path == "$$" {
		return true, nil, nil
	}
	s := path
	if strings.HasPrefix(s, "$$") {
		ctxRoot, s = true, s[2:]
	} else if strings.HasPrefix(s, "$") {
		s = s[1:]
	} else {
		return false, nil, errPathInvalid
	}
	i := 0
	for i < len(s) {
		switch s[i] {
		case '.':
			i++
			start := i
			for i < len(s) && isPathIdent(s[i]) {
				i++
			}
			if start == i {
				return false, nil, errPathInvalid
			}
			segs = append(segs, pathSeg{key: s[start:i]})
		case '[':
			i++
			if i < len(s) && (s[i] == '\'' || s[i] == '"') {
				q := s[i]
				i++
				var sb strings.Builder
				for i < len(s) && s[i] != q {
					if s[i] == '\\' && i+1 < len(s) {
						sb.WriteByte(s[i+1])
						i += 2
						continue
					}
					sb.WriteByte(s[i])
					i++
				}
				if i >= len(s) {
					return false, nil, errPathInvalid
				}
				i++ // closing quote
				if i >= len(s) || s[i] != ']' {
					return false, nil, errPathInvalid
				}
				i++
				segs = append(segs, pathSeg{key: sb.String()})
			} else {
				start := i
				for i < len(s) && s[i] >= '0' && s[i] <= '9' {
					i++
				}
				if start == i || i >= len(s) || s[i] != ']' {
					return false, nil, errPathInvalid
				}
				idx, convErr := strconv.Atoi(s[start:i])
				if convErr != nil {
					return false, nil, errPathInvalid
				}
				i++
				segs = append(segs, pathSeg{index: idx, isIndex: true})
			}
		default:
			return false, nil, errPathInvalid
		}
	}
	return ctxRoot, segs, nil
}

// resolveRefPath resolves a path against the state input (root) and the
// context object (ctx); $$-prefixed paths address the context object.
func resolveRefPath(root, ctx any, path string) (any, error) {
	ctxRoot, segs, err := parsePathBody(path)
	if err != nil {
		return nil, err
	}
	cur := root
	if ctxRoot {
		cur = ctx
	}
	for _, sg := range segs {
		if sg.isIndex {
			arr, ok := cur.([]any)
			if !ok || sg.index < 0 || sg.index >= len(arr) {
				return nil, errPathNotFound
			}
			cur = arr[sg.index]
			continue
		}
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, errPathNotFound
		}
		v, ok := m[sg.key]
		if !ok {
			return nil, errPathNotFound
		}
		cur = v
	}
	return cur, nil
}

func resolveOptional(root, ctx any, path string) (any, bool) {
	v, err := resolveRefPath(root, ctx, path)
	if err != nil {
		return nil, false
	}
	return v, true
}

// mergeAtPath merges a state result into input at ResultPath, mirroring AWS
// semantics: "$" replaces the input, intermediates must be objects or arrays
// and conflicts raise States.ResultPathMatchFailure.
func mergeAtPath(input any, path string, result any) (any, *stateError) {
	_, segs, err := parsePathBody(path)
	if err != nil {
		return nil, newStateError(ErrNameRuntime, fmt.Sprintf("ResultPath %q is not a valid path: %v", path, err))
	}
	if len(segs) == 0 {
		return result, nil
	}
	node := deepCopy(input)
	if node == nil {
		node = map[string]any{}
	}
	cur := node
	for i := 0; i < len(segs)-1; i++ {
		sg := segs[i]
		if sg.isIndex {
			arr, ok := cur.([]any)
			if !ok || sg.index >= len(arr) {
				return nil, newStateError(ErrNameResultPathMatch, fmt.Sprintf("ResultPath %q: intermediate array element is missing", path))
			}
			cur = arr[sg.index]
			continue
		}
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, newStateError(ErrNameResultPathMatch, fmt.Sprintf("ResultPath %q cannot be merged into a non-object value", path))
		}
		child, exists := m[sg.key]
		if !exists {
			nc := map[string]any{}
			m[sg.key] = nc
			cur = nc
			continue
		}
		switch child.(type) {
		case map[string]any, []any:
			cur = child
		default:
			return nil, newStateError(ErrNameResultPathMatch, fmt.Sprintf("ResultPath %q conflicts with the scalar value at %q", path, sg.key))
		}
	}
	last := segs[len(segs)-1]
	if last.isIndex {
		arr, ok := cur.([]any)
		if !ok || last.index >= len(arr) {
			return nil, newStateError(ErrNameResultPathMatch, fmt.Sprintf("ResultPath %q: array element not addressable", path))
		}
		arr[last.index] = result
	} else {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, newStateError(ErrNameResultPathMatch, fmt.Sprintf("ResultPath %q cannot be merged into a non-object value", path))
		}
		m[last.key] = result
	}
	return node, nil
}

// mergeResultPath applies a state's ResultPath (nil means default ""; an
// empty string means a null ResultPath, i.e. the result is discarded).
func mergeResultPath(input any, resultPath *string, result any) (any, *stateError) {
	if resultPath == nil {
		return mergeAtPath(input, "$", result)
	}
	if *resultPath == "" {
		return input, nil
	}
	return mergeAtPath(input, *resultPath, result)
}
