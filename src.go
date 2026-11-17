// Package floci implements the Amazon States Language (ASL) interpreter for
// floci-go, i.e. an embedded "Step Functions" emulator (single-file drop-in).
//
// API (all on *Engine):
//   CreateStateMachine / DescribeStateMachine / DeleteStateMachine
//   StartExecution / DescribeExecution / ListExecutions / StopExecution
//   RunExecution / RunDefinition / WaitForExecution     (test conveniences)
//   RegisterTaskHandler / UnregisterTaskHandler / SetDefaultTaskPassthrough
//
// Supported states: Task (Retry = exponential backoff + jitter; Catch),
// Choice (full JSON comparison operator set incl. And/Or/Not), Parallel
// (branch fan-out), Map (ItemSelector + Iterator/ItemProcessor, MaxConcurrency
// fan-out, $$.Map.Item.Index/Value context), Wait (Seconds / SecondsPath /
// Timestamp / TimestampPath), Pass, Succeed, Fail. Typed ASL errors flow
// through Retry/Catch (States.ALL, States.TaskFailed, States.Timeout, ...).
//
// The JSONPath / expression evaluator (EvalJSONPath / EvalValueRef /
// EvalIntrinsic) is the shared implementation also used by the DDB emulator.
package main

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Service constants & error types
// ---------------------------------------------------------------------------

// Execution / state machine statuses (DescribeExecution / ListExecutions).
const (
	StatusRunning   = "RUNNING"
	StatusSucceeded = "SUCCEEDED"
	StatusFailed    = "FAILED"
	StatusTimedOut  = "TIMED_OUT"
	StatusAborted   = "ABORTED"
)

const (
	TypeStandard = "STANDARD"
	TypeExpress  = "EXPRESS"
)

// ASL runtime error names understood by Retry/Catch.
const (
	ErrStatesAll                     = "States.ALL"
	ErrStatesTaskFailed              = "States.TaskFailed"
	ErrStatesTimeout                 = "States.Timeout"
	ErrStatesHeartbeatTimeout        = "States.HeartbeatTimeout"
	ErrStatesPermissions             = "States.Permissions"
	ErrStatesResultPathMatchFailure  = "States.ResultPathMatchFailure"
	ErrStatesResultSelectorMatchFail = "States.ResultSelectorMatchFailure"
	ErrStatesParamPathFormatFailure  = "States.ParameterPathFormatFailure"
	ErrStatesParamPathFailure        = "States.ParameterPathFailure"
	ErrStatesNoChoiceMatched         = "States.NoChoiceMatched"
	ErrStatesRuntime                 = "States.Runtime"
	ErrStatesInputPathMatchFailure   = "States.InputPathMatchFailure"
	ErrStatesOutputPathMatchFailure  = "States.OutputPathMatchFailure"
)

// API error codes surfaced by the engine.
const (
	ErrCodeStateMachineDoesNotExist  = "StateMachineDoesNotExist"
	ErrCodeStateMachineAlreadyExists = "StateMachineAlreadyExists"
	ErrCodeStateMachineDeleting      = "StateMachineDeleting"
	ErrCodeExecutionDoesNotExist     = "ExecutionDoesNotExist"
	ErrCodeExecutionAlreadyExists    = "ExecutionAlreadyExists"
	ErrCodeInvalidExecutionInput     = "InvalidExecutionInput"
	ErrCodeInvalidParameter          = "InvalidParameter"
	ErrCodeInvalidDefinition         = "InvalidDefinition"
	ErrCodeDeadlineExceeded          = "DeadlineExceeded"
)

// SMError is a typed ASL runtime error that flows through Retry and Catch.
// Abort marks a StopExecution cancellation and Timer an execution-level
// timeout; both bypass Retry/Catch and terminate the execution.
type SMError struct {
	Name  string `json:"Error"`
	Cause string `json:"Cause"`
	Abort bool   `json:"-"`
	Timer bool   `json:"-"`
}

func (e *SMError) Error() string { return fmt.Sprintf("%s: %s", e.Name, e.Cause) }

func smErr(name, format string, a ...interface{}) *SMError {
	return &SMError{Name: name, Cause: fmt.Sprintf(format, a...)}
}

var errAborted = &SMError{Name: ErrStatesTaskFailed, Cause: "execution was stopped", Abort: true}

var errExecTimedOut = &SMError{Name: ErrStatesTimeout, Cause: "state machine execution timed out", Timer: true}

// APIError is an API-level error (bad request, missing resource, ...).
type APIError struct {
	Code    string
	Message string
}

func (e *APIError) Error() string { return e.Code + ": " + e.Message }

func apiErr(code, format string, a ...interface{}) error {
	return &APIError{Code: code, Message: fmt.Sprintf(format, a...)}
}

func errInvalidDef(format string, a ...interface{}) error {
	return apiErr(ErrCodeInvalidDefinition, format, a...)
}

// ---------------------------------------------------------------------------
// Shared JSON/expression value helpers (used by the ASL interpreter and the
// DDB emulator alike)
// ---------------------------------------------------------------------------

// jsonDecode decodes strict JSON, preserving numbers as json.Number.
func jsonDecode(s string) (interface{}, error) {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v interface{}
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

func marshalJSON(v interface{}) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func mustMarshalJSON(v interface{}) string {
	s, err := marshalJSON(v)
	if err != nil {
		return "null"
	}
	return s
}

// deepCopy clones maps/slices (scalars including json.Number are immutable).
func deepCopy(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		m := make(map[string]interface{}, len(t))
		for k, val := range t {
			m[k] = deepCopy(val)
		}
		return m
	case []interface{}:
		a := make([]interface{}, len(t))
		for i, val := range t {
			a[i] = deepCopy(val)
		}
		return a
	default:
		return v
	}
}

func asFloat(v interface{}) (float64, bool) {
	switch t := v.(type) {
	case json.Number:
		f, err := t.Float64()
		if err == nil {
			return f, true
		}
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	}
	return 0, false
}

func asInt(v interface{}) (int64, bool) {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i, true
		}
	case int:
		return int64(t), true
	case int64:
		return t, true
	case float64:
		if t == float64(int64(t)) {
			return int64(t), true
		}
	}
	return 0, false
}

func stringifyValue(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case string:
		return t
	case json.Number:
		return t.String()
	case bool:
		if t {
			return "true"
		}
		return "false"
	default:
		return mustMarshalJSON(deepCopy(v))
	}
}

func rfc3339(t time.Time) string { return t.Format(time.RFC3339Nano) }

func secondsToDuration(f float64) time.Duration {
	if f <= 0 {
		return 0
	}
	return time.Duration(f * float64(time.Second))
}

func parseTimestamp(s string) (time.Time, bool) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, true
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, true
	}
	return time.Time{}, false
}

// ---------------------------------------------------------------------------
// Shared JSONPath evaluator (same implementation used by the DDB emulator)
// ---------------------------------------------------------------------------

type jpTokKind int

const (
	jpKey jpTokKind = iota
	jpIndex
	jpWild
	jpRecurse
)

type jpTok struct {
	kind jpTokKind
	key  string
	idx  int
}

type jpPath struct {
	toks []jpTok
}

// parseJSONPath parses the ASL JSONPath subset:
//   $ and $$ roots, .field, ['field'], ["field"], [0], [-1], .*, [*], ..field
func parseJSONPath(path string) (*jpPath, bool, error) {
	s := strings.TrimSpace(path)
	if s == "" {
		return nil, false, fmt.Errorf("empty JSONPath")
	}
	isCtx := false
	if strings.HasPrefix(s, "$$") {
		isCtx = true
		s = s[2:]
	} else if strings.HasPrefix(s, "$") {
		s = s[1:]
	} else {
		return nil, false, fmt.Errorf("JSONPath %q must start with '$'", path)
	}
	p := &jpPath{}
	for len(s) > 0 {
		switch s[0] {
		case '.':
			if len(s) == 1 {
				return nil, false, fmt.Errorf("JSONPath %q has a trailing '.'", path)
			}
			if s[1] == '.' {
				s = s[2:]
				name, rest := scanName(s)
				if name == "" {
					return nil, false, fmt.Errorf("JSONPath %q: '..' requires a field name", path)
				}
				p.toks = append(p.toks, jpTok{kind: jpRecurse, key: name})
				s = rest
				continue
			}
			s = s[1:]
			if s[0] == '*' {
				p.toks = append(p.toks, jpTok{kind: jpWild})
				s = s[1:]
				continue
			}
			name, rest := scanName(s)
			if name == "" {
				return nil, false, fmt.Errorf("JSONPath %q: '.' requires a field name", path)
			}
			p.toks = append(p.toks, jpTok{kind: jpKey, key: name})
			s = rest
		case '[':
			i := 1
			inStr := byte(0)
			esc := false
			for i < len(s) {
				c := s[i]
				if inStr != 0 {
					if esc {
						esc = false
					} else if c == '\\' {
						esc = true
					} else if c == inStr {
						inStr = 0
					}
				} else if c == '\'' || c == '"' {
					inStr = c
				} else if c == ']' {
					break
				}
				i++
			}
			if i >= len(s) {
				return nil, false, fmt.Errorf("JSONPath %q: missing ']'", path)
			}
			inner := strings.TrimSpace(s[1:i])
			tok, terr := parseBracket(inner)
			if terr != nil {
				return nil, false, fmt.Errorf("JSONPath %q: %v", path, terr)
			}
			p.toks = append(p.toks, tok)
			s = s[i+1:]
		default:
			return nil, false, fmt.Errorf("JSONPath %q: unexpected %q (expected '.' or '[')", path, s[0])
		}
	}
	return p, isCtx, nil
}

func scanName(s string) (string, string) {
	i := 0
	for i < len(s) && s[i] != '.' && s[i] != '[' {
		i++
	}
	return s[:i], s[i:]
}

func parseBracket(inner string) (jpTok, error) {
	if inner == "*" {
		return jpTok{kind: jpWild}, nil
	}
	if len(inner) >= 2 && (inner[0] == '\'' || inner[0] == '"') {
		if inner[0] == '\'' {
			key, err := unquoteSingle(inner)
			if err != nil {
				return jpTok{}, err
			}
			return jpTok{kind: jpKey, key: key}, nil
		}
		v, derr := jsonDecode(inner)
		if derr != nil {
			return jpTok{}, derr
		}
		key, ok := v.(string)
		if !ok {
			return jpTok{}, fmt.Errorf("not a string selector")
		}
		return jpTok{kind: jpKey, key: key}, nil
	}
	n, err := strconv.Atoi(inner)
	if err != nil {
		return jpTok{}, fmt.Errorf("unsupported selector %q", inner)
	}
	return jpTok{kind: jpIndex, idx: n}, nil
}

func unquoteSingle(t string) (string, error) {
	if len(t) < 2 || t[0] != '\'' || t[len(t)-1] != '\'' {
		return "", fmt.Errorf("malformed single-quoted string %q", t)
	}
	body := t[1 : len(t)-1]
	return strings.ReplaceAll(body, `\'`, `'`), nil
}

func (p *jpPath) evalNodes(root interface{}) []interface{} {
	nodes := []interface{}{root}
	for _, t := range p.toks {
		if len(nodes) == 0 {
			return nil
		}
		var next []interface{}
		for _, n := range nodes {
			switch t.kind {
			case jpKey:
				if m, ok := n.(map[string]interface{}); ok {
					if v, ok := m[t.key]; ok {
						next = append(next, v)
					}
				}
			case jpIndex:
				if a, ok := n.([]interface{}); ok {
					i := t.idx
					if i < 0 {
						i += len(a)
					}
					if i >= 0 && i < len(a) {
						next = append(next, a[i])
					}
				}
			case jpWild:
				switch v := n.(type) {
				case []interface{}:
					next = append(next, v...)
				case map[string]interface{}:
					for _, k := range sortedKeys(v) {
						next = append(next, v[k])
					}
				}
			case jpRecurse:
				next = append(next, collectKey(n, t.key)...)
			}
		}
		nodes = next
	}
	return nodes
}

func sortedKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func collectKey(n interface{}, key string) []interface{} {
	var out []interface{}
	var walk func(v interface{})
	walk = func(v interface{}) {
		switch t := v.(type) {
		case map[string]interface{}:
			for _, k := range sortedKeys(t) {
				if k == key {
					out = append(out, t[k])
				}
				walk(t[k])
			}
		case []interface{}:
			for _, e := range t {
				walk(e)
			}
		}
	}
	walk(n)
	return out
}

// EvalJSONPath evaluates path against node. Returns the single matched value,
// or an array when wildcard/recursive selectors match multiple nodes. A
// non-matching path yields an error. Shared with the DDB emulator.
func EvalJSONPath(node interface{}, path string) (interface{}, error) {
	p, isCtx, err := parseJSONPath(path)
	if err != nil {
		return nil, err
	}
	if isCtx {
		return nil, fmt.Errorf("context root '$$' is not valid here")
	}
	return evalJP(p, node)
}

func evalJP(p *jpPath, root interface{}) (interface{}, error) {
	nodes := p.evalNodes(root)
	if len(nodes) == 0 {
		return nil, fmt.Errorf("JSONPath did not match")
	}
	if len(nodes) == 1 {
		return nodes[0], nil
	}
	return nodes, nil
}

// EvalValueRef resolves an ASL reference: "$.path", "$$.context.path", or an
// intrinsic function "States.Fn(...)". Shared with the DDB emulator.
func EvalValueRef(expr string, document interface{}, contextRoot map[string]interface{}) (interface{}, error) {
	expr = strings.TrimSpace(expr)
	if strings.HasPrefix(expr, "States.") {
		return evalIntrinsic(expr, document, contextRoot)
	}
	p, isCtx, err := parseJSONPath(expr)
	if err != nil {
		return nil, err
	}
	root := interface{}(document)
	if isCtx {
		root = contextRoot
	}
	return evalJP(p, root)
}

// evalRefFound: value + presence flag for lenient consumers (InputPath,
// ResultPath references, SecondsPath, ...).
func evalRefFound(expr string, document interface{}, contextRoot map[string]interface{}) (interface{}, bool, error) {
	if strings.HasPrefix(expr, "States.") {
		v, err := evalIntrinsic(expr, document, contextRoot)
		return v, err == nil, err
	}
	p, isCtx, err := parseJSONPath(expr)
	if err != nil {
		return nil, false, err
	}
	root := interface{}(document)
	if isCtx {
		root = contextRoot
	}
	nodes := p.evalNodes(root)
	if len(nodes) == 0 {
		return nil, false, nil
	}
	if len(nodes) == 1 {
		return nodes[0], true, nil
	}
	return nodes, true, nil
}

// ---------------------------------------------------------------------------
// ASL intrinsic functions (States.*), shared with the DDB emulator
// ---------------------------------------------------------------------------

// EvalIntrinsic evaluates "States.Fn(...)" expressions (ASL intrinsic
// functions). Shared with the DDB emulator.
func EvalIntrinsic(expr string, document interface{}, contextRoot map[string]interface{}) (interface{}, error) {
	return evalIntrinsic(expr, document, contextRoot)
}

func evalIntrinsic(expr string, doc interface{}, ctxm map[string]interface{}) (interface{}, error) {
	if !strings.HasPrefix(expr, "States.") {
		return nil, fmt.Errorf("not an intrinsic function: %q", expr)
	}
	inner := expr[len("States."):]
	if !strings.HasSuffix(inner, ")") {
		return nil, fmt.Errorf("malformed intrinsic %q", expr)
	}
	open := strings.IndexByte(inner, '(')
	if open <= 0 {
		return nil, fmt.Errorf("malformed intrinsic %q", expr)
	}
	name := inner[:open]
	argstr := inner[open+1 : len(inner)-1]
	argsRaw, aerr := splitTopLevelArgs(argstr)
	if aerr != nil {
		return nil, fmt.Errorf("malformed intrinsic %q: %v", expr, aerr)
	}
	arg := func(i int) (interface{}, error) {
		if i >= len(argsRaw) {
			return nil, fmt.Errorf("intrinsic %s is missing argument %d", name, i+1)
		}
		return evalIntrinsicArg(argsRaw[i], doc, ctxm)
	}
	switch name {
	case "Array":
		out := make([]interface{}, 0, len(argsRaw))
		for i := range argsRaw {
			v, verr := arg(i)
			if verr != nil {
				return nil, verr
			}
			out = append(out, v)
		}
		return out, nil
	case "ArrayPartition":
		a0, err := arg(0)
		if err != nil {
			return nil, err
		}
		a1, err := arg(1)
		if err != nil {
			return nil, err
		}
		arr, ok := a0.([]interface{})
		if !ok {
			return nil, fmt.Errorf("States.ArrayPartition requires an array")
		}
		n64, ok := asInt(a1)
		if !ok || n64 <= 0 {
			return nil, fmt.Errorf("States.ArrayPartition requires a positive chunk size")
		}
		size := int(n64)
		out := []interface{}{}
		for start := 0; start < len(arr); start += size {
			end := start + size
			if end > len(arr) {
				end = len(arr)
			}
			out = append(out, arr[start:end:end])
		}
		return out, nil
	case "ArrayGetItem":
		a0, err := arg(0)
		if err != nil {
			return nil, err
		}
		a1, err := arg(1)
		if err != nil {
			return nil, err
		}
		arr, ok := a0.([]interface{})
		if !ok {
			return nil, fmt.Errorf("States.ArrayGetItem requires an array")
		}
		idx64, ok := asInt(a1)
		if !ok {
			return nil, fmt.Errorf("States.ArrayGetItem requires an integer index")
		}
		if idx64 < 0 || int(idx64) >= len(arr) {
			return nil, fmt.Errorf("States.ArrayGetItem index out of range")
		}
		return arr[int(idx64)], nil
	case "ArrayLength":
		a0, err := arg(0)
		if err != nil {
			return nil, err
		}
		arr, ok := a0.([]interface{})
		if !ok {
			return nil, fmt.Errorf("States.ArrayLength requires an array")
		}
		return json.Number(strconv.Itoa(len(arr))), nil
	case "ArrayUnique":
		a0, err := arg(0)
		if err != nil {
			return nil, err
		}
		arr, ok := a0.([]interface{})
		if !ok {
			return nil, fmt.Errorf("States.ArrayUnique requires an array")
		}
		seen := make(map[string]bool, len(arr))
		out := make([]interface{}, 0, len(arr))
		for _, el := range arr {
			k := mustMarshalJSON(deepCopy(el))
			if !seen[k] {
				seen[k] = true
				out = append(out, el)
			}
		}
		return out, nil
	case "Base64Encode":
		a0, err := arg(0)
		if err != nil {
			return nil, err
		}
		s, ok := a0.(string)
		if !ok {
			return nil, fmt.Errorf("States.Base64Encode requires a string")
		}
		return base64.StdEncoding.EncodeToString([]byte(s)), nil
	case "Base64Decode":
		a0, err := arg(0)
		if err != nil {
			return nil, err
		}
		s, ok := a0.(string)
		if !ok {
			return nil, fmt.Errorf("States.Base64Decode requires a string")
		}
		b, derr := base64.StdEncoding.DecodeString(s)
		if derr != nil {
			return nil, fmt.Errorf("States.Base64Decode: %v", derr)
		}
		return string(b), nil
	case "Hash":
		a0, err := arg(0)
		if err != nil {
			return nil, err
		}
		a1, err := arg(1)
		if err != nil {
			return nil, err
		}
		data, oks := a0.(string)
		if !oks {
			return nil, fmt.Errorf("States.Hash requires a string input")
		}
		algo, oka := a1.(string)
		if !oka {
			return nil, fmt.Errorf("States.Hash requires an algorithm")
		}
		var sum []byte
		switch strings.ToUpper(algo) {
		case "MD5":
			s := md5.Sum([]byte(data))
			sum = s[:]
		case "SHA-1":
			s := sha1.Sum([]byte(data))
			sum = s[:]
		case "SHA-256":
			s := sha256.Sum256([]byte(data))
			sum = s[:]
		case "SHA-384":
			s := sha512.Sum384([]byte(data))
			sum = s[:]
		case "SHA-512":
			s := sha512.Sum512([]byte(data))
			sum = s[:]
		default:
			return nil, fmt.Errorf("unsupported hash algorithm %q", algo)
		}
		return hex.EncodeToString(sum), nil
	case "JsonMerge":
		a0, err := arg(0)
		if err != nil {
			return nil, err
		}
		a1, err := arg(1)
		if err != nil {
			return nil, err
		}
		ma, oka := a0.(map[string]interface{})
		mb, okb := a1.(map[string]interface{})
		if !oka || !okb {
			return deepCopy(a1), nil
		}
		return jsonMerge(ma, mb), nil
	case "JsonToString":
		a0, err := arg(0)
		if err != nil {
			return nil, err
		}
		return mustMarshalJSON(deepCopy(a0)), nil
	case "StringToJson":
		a0, err := arg(0)
		if err != nil {
			return nil, err
		}
		s, ok := a0.(string)
		if !ok {
			return nil, fmt.Errorf("States.StringToJson requires a string")
		}
		return jsonDecode(s)
	case "StringSplit":
		a0, err := arg(0)
		if err != nil {
			return nil, err
		}
		a1, err := arg(1)
		if err != nil {
			return nil, err
		}
		s, oks := a0.(string)
		dlm, okd := a1.(string)
		if !oks || !okd {
			return nil, fmt.Errorf("States.StringSplit requires string arguments")
		}
		if dlm == "" {
			return nil, fmt.Errorf("States.StringSplit requires a non-empty delimiter")
		}
		parts := strings.Split(s, dlm)
		out := make([]interface{}, 0, len(parts))
		for _, p := range parts {
			out = append(out, p)
		}
		return out, nil
	case "Format":
		v0, err := arg(0)
		if err != nil {
			return nil, err
		}
		tpl, ok := v0.(string)
		if !ok {
			return nil, fmt.Errorf("States.Format requires a string template")
		}
		var out strings.Builder
		next := 1
		for i := 0; i < len(tpl); i++ {
			c := tpl[i]
			if c == '{' && i+1 < len(tpl) && tpl[i+1] == '}' {
				if next >= len(argsRaw) {
					return nil, fmt.Errorf("States.Format has more placeholders than arguments")
				}
				v, verr := arg(next)
				if verr != nil {
					return nil, verr
				}
				next++
				out.WriteString(stringifyValue(v))
				i++
				continue
			}
			out.WriteByte(c)
		}
		if next < len(argsRaw) {
			return nil, fmt.Errorf("States.Format has fewer placeholders than arguments")
		}
		return out.String(), nil
	case "MathRandom":
		if len(argsRaw) == 0 {
			return json.Number(strconv.FormatInt(rand.Int63(), 10)), nil
		}
		a0, err := arg(0)
		if err != nil {
			return nil, err
		}
		a1, err := arg(1)
		if err != nil {
			return nil, err
		}
		lo64, ok1 := asInt(a0)
		hi64, ok2 := asInt(a1)
		if !ok1 || !ok2 || hi64 < lo64 {
			return nil, fmt.Errorf("States.MathRandom requires numeric start <= end")
		}
		span := hi64 - lo64
		if span <= 0 {
			return json.Number(strconv.FormatInt(lo64, 10)), nil
		}
		if span+1 <= 0 { // int64 overflow guard
			return json.Number(strconv.FormatInt(lo64+rand.Int63n(span), 10)), nil
		}
		return json.Number(strconv.FormatInt(lo64+rand.Int63n(span+1), 10)), nil
	case "MathAdd":
		a0, err := arg(0)
		if err != nil {
			return nil, err
		}
		a1, err := arg(1)
		if err != nil {
			return nil, err
		}
		i0, oki0 := asInt(a0)
		i1, oki1 := asInt(a1)
		if oki0 && oki1 {
			return json.Number(strconv.FormatInt(i0+i1, 10)), nil
		}
		f0, okf0 := asFloat(a0)
		f1, okf1 := asFloat(a1)
		if !okf0 || !okf1 {
			return nil, fmt.Errorf("States.MathAdd requires numeric arguments")
		}
		return json.Number(strconv.FormatFloat(f0+f1, 'g', -1, 64)), nil
	}
	return nil, fmt.Errorf("unknown intrinsic function %q", name)
}

func evalIntrinsicArg(raw string, doc interface{}, ctxm map[string]interface{}) (interface{}, error) {
	t := strings.TrimSpace(raw)
	if t == "" {
		return nil, fmt.Errorf("empty intrinsic argument")
	}
	if t[0] == '$' {
		return EvalValueRef(t, doc, ctxm)
	}
	if t[0] == '\'' {
		return unquoteSingle(t)
	}
	return jsonDecode(t)
}

func splitTopLevelArgs(s string) ([]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	var out []string
	var cur strings.Builder
	depth := 0
	var inStr byte
	esc := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inStr != 0 {
			cur.WriteByte(c)
			if esc {
				esc = false
			} else if c == '\\' {
				esc = true
			} else if c == inStr {
				inStr = 0
			}
			continue
		}
		switch c {
		case '\'', '"':
			inStr = c
			cur.WriteByte(c)
		case '(', '[', '{':
			depth++
			cur.WriteByte(c)
		case ')', ']', '}':
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("unbalanced expression")
			}
			cur.WriteByte(c)
		case ',':
			if depth == 0 {
				out = append(out, cur.String())
				cur.Reset()
			} else {
				cur.WriteByte(c)
			}
		default:
			cur.WriteByte(c)
		}
	}
	if inStr != 0 || depth != 0 {
		return nil, fmt.Errorf("unbalanced expression")
	}
	out = append(out, cur.String())
	return out, nil
}

func jsonMerge(a, b map[string]interface{}) map[string]interface{} {
	out := deepCopy(a).(map[string]interface{})
	for k, v := range b {
		if vm, ok := v.(map[string]interface{}); ok {
			if om, ok2 := out[k].(map[string]interface{}); ok2 {
				out[k] = jsonMerge(om, vm)
				continue
			}
		}
		out[k] = deepCopy(v)
	}
	return out
}

// ---------------------------------------------------------------------------
// ASL definition model, parsing and validation
// ---------------------------------------------------------------------------

type stateMachine struct {
	arn, name, roleArn, typ, definition string
	createdAt   time.Time
	timeoutSec  int
	def         *stateDef
}

type machineRec struct {
	sm      *stateMachine
	deleted bool
}

// stateDef is one composite: top-level state machine, Parallel branch or Map
// iterator/ItemProcessor.
type stateDef struct {
	startAt    string
	states     map[string]*stateDecl
	timeoutSec int
}

type stateDecl struct {
	name, typ, comment string
	next    string
	hasNext bool
	end     bool

	inputPath      *string
	resultPath     *string
	outputPath     *string
	resultSelector map[string]interface{}
	parameters     map[string]interface{}

	// Task
	resource         string
	timeoutSeconds   int
	heartbeatSeconds int
	retry            []retryDecl
	catch            []catchDecl

	// Choice
	choices []choiceRule
	deflt   string

	// Wait
	hasSeconds    bool
	seconds       int
	secondsPath   *string
	timestamp     string
	timestampPath *string

	// Pass
	result    interface{}
	hasResult bool

	// Parallel
	branches []*stateDef

	// Map
	itemsPath      *string
	itemSelector   map[string]interface{}
	maxConcurrency int
	iterator       *stateDef
	itemProcessor  *stateDef

	// Fail
	failError string
	failCause string
}

type retryDecl struct {
	patterns    []string
	interval    time.Duration
	maxAttempts int
	backoff     float64
	maxDelay    time.Duration
	jitter      string
}

type catchDecl struct {
	patterns   []string
	resultPath *string
	next       string
}

type choiceRule struct {
	variable string
	op       string
	value    interface{}
	and      []choiceRule
	or       []choiceRule
	not      *choiceRule
	next     string
}

var choiceOpNames = map[string]bool{
	"StringEquals": true, "StringEqualsPath": true,
	"StringLessThan": true, "StringLessThanPath": true,
	"StringGreaterThan": true, "StringGreaterThanPath": true,
	"StringLessThanEquals": true, "StringLessThanEqualsPath": true,
	"StringGreaterThanEquals": true, "StringGreaterThanEqualsPath": true,
	"NumericEquals": true, "NumericEqualsPath": true,
	"NumericLessThan": true, "NumericLessThanPath": true,
	"NumericGreaterThan": true, "NumericGreaterThanPath": true,
	"NumericLessThanEquals": true, "NumericLessThanEqualsPath": true,
	"NumericGreaterThanEquals": true, "NumericGreaterThanEqualsPath": true,
	"BooleanEquals": true, "BooleanEqualsPath": true,
	"TimestampEquals": true, "TimestampEqualsPath": true,
	"TimestampLessThan": true, "TimestampLessThanPath": true,
	"TimestampGreaterThan": true, "TimestampGreaterThanPath": true,
	"TimestampLessThanEquals": true, "TimestampLessThanEqualsPath": true,
	"TimestampGreaterThanEquals": true, "TimestampGreaterThanEqualsPath": true,
	"IsNull": true, "IsNotNull": true, "IsBool": true, "IsTrue": true, "IsFalse": true,
	"IsString": true, "IsNumeric": true, "IsTimestamp": true,
	"IsPresent": true, "IsAbsent": true,
}

func parseDefinition(definition string) (*stateDef, error) {
	root, err := jsonDecode(definition)
	if err != nil {
		return nil, errInvalidDef("definition is not valid JSON: %v", err)
	}
	m, ok := root.(map[string]interface{})
	if !ok {
		return nil, errInvalidDef("definition must be a JSON object")
	}
	return parseStateDef(m)
}

func parseStateDef(m map[string]interface{}) (*stateDef, error) {
	d := &stateDef{}
	startAt, _ := m["StartAt"].(string)
	if startAt == "" {
		return nil, errInvalidDef("StartAt is required")
	}
	statesRaw, ok := m["States"].(map[string]interface{})
	if !ok || len(statesRaw) == 0 {
		return nil, errInvalidDef("States must be a non-empty object")
	}
	d.startAt = startAt
	d.states = make(map[string]*stateDecl, len(statesRaw))
	for name, raw := range statesRaw {
		st, err := parseState(name, raw)
		if err != nil {
			return nil, err
		}
		d.states[name] = st
	}
	if v, ok := m["TimeoutSeconds"]; ok && v != nil {
		f, err := readNumber(v)
		if err != nil || f < 0 {
			return nil, errInvalidDef("TimeoutSeconds must be a non-negative number")
		}
		d.timeoutSec = int(f)
	}
	if err := validateStateDef(d); err != nil {
		return nil, err
	}
	return d, nil
}

func parseState(name string, raw interface{}) (*stateDecl, error) {
	m, ok := raw.(map[string]interface{})
	if !ok {
		return nil, errInvalidDef("state %q must be an object", name)
	}
	st := &stateDecl{name: name}
	st.comment, _ = m["Comment"].(string)
	st.typ, _ = m["Type"].(string)
	if v, ok := m["Next"].(string); ok && v != "" {
		st.next = v
		st.hasNext = true
	}
	if v, ok := m["End"]; ok {
		b, ok2 := v.(bool)
		if !ok2 {
			return nil, errInvalidDef("state %q: End must be a boolean", name)
		}
		st.end = b
	}
	st.inputPath = jsonPathField(m, "InputPath")
	st.resultPath = jsonPathField(m, "ResultPath")
	st.outputPath = jsonPathField(m, "OutputPath")
	if v, ok := m["ResultSelector"]; ok {
		sel, ok2 := v.(map[string]interface{})
		if !ok2 {
			return nil, errInvalidDef("state %q: ResultSelector must be an object", name)
		}
		st.resultSelector = sel
	}
	switch st.typ {
	case "Task":
		res, ok2 := m["Resource"].(string)
		if !ok2 || res == "" {
			return nil, errInvalidDef("Task state %q requires a Resource", name)
		}
		st.resource = res
		ts, err := intField(m, "TimeoutSeconds", 0)
		if err != nil {
			return nil, errInvalidDef("state %q: %v", name, err)
		}
		st.timeoutSeconds = ts
		hs, err := intField(m, "HeartbeatSeconds", 0)
		if err != nil {
			return nil, errInvalidDef("state %q: %v", name, err)
		}
		st.heartbeatSeconds = hs
		if p, okp := m["Parameters"]; okp {
			pm, okpm := p.(map[string]interface{})
			if !okpm {
				return nil, errInvalidDef("state %q: Parameters must be an object", name)
			}
			st.parameters = pm
		}
		rt, err := parseRetryList(m["Retry"], name)
		if err != nil {
			return nil, err
		}
		st.retry = rt
		ca, err := parseCatchList(m["Catch"], name)
		if err != nil {
			return nil, err
		}
		st.catch = ca
	case "Choice":
		rawChoices, okc := m["Choices"].([]interface{})
		if !okc || len(rawChoices) == 0 {
			return nil, errInvalidDef("Choice state %q requires a non-empty Choices array", name)
		}
		for i, rc := range rawChoices {
			rule, err := parseChoiceRule(rc)
			if err != nil {
				return nil, errInvalidDef("state %q: Choices[%d]: %v", name, i, err)
			}
			st.choices = append(st.choices, rule)
		}
		if v, okv := m["Default"].(string); okv {
			st.deflt = v
		}
	case "Wait":
		n := 0
		if v, okv := m["Seconds"]; okv && v != nil {
			f, err := readNumber(v)
			if err != nil || f < 0 {
				return nil, errInvalidDef("state %q: Seconds must be a non-negative number", name)
			}
			st.seconds = int(f)
			st.hasSeconds = true
			n++
		}
		if sp := jsonPathField(m, "SecondsPath"); sp != nil {
			st.secondsPath = sp
			n++
		}
		if v, okv := m["Timestamp"].(string); okv {
			if _, okt := parseTimestamp(v); !okt {
				return nil, errInvalidDef("state %q: Timestamp %q is not a valid RFC3339 timestamp", name, v)
			}
			st.timestamp = v
			n++
		}
		if tp := jsonPathField(m, "TimestampPath"); tp != nil {
			st.timestampPath = tp
			n++
		}
		if n != 1 {
			return nil, errInvalidDef("Wait state %q requires exactly one of Seconds, SecondsPath, Timestamp or TimestampPath", name)
		}
	case "Pass":
		if v, okv := m["Result"]; okv {
			st.hasResult = true
			st.result = v
		}
		if p, okp := m["Parameters"]; okp {
			pm, okpm := p.(map[string]interface{})
			if !okpm {
				return nil, errInvalidDef("state %q: Parameters must be an object", name)
			}
			st.parameters = pm
		}
		if st.hasResult && st.parameters != nil {
			return nil, errInvalidDef("Pass state %q cannot specify both Result and Parameters", name)
		}
	case "Parallel":
		rawBranches, okb := m["Branches"].([]interface{})
		if !okb || len(rawBranches) == 0 {
			return nil, errInvalidDef("Parallel state %q requires a non-empty Branches array", name)
		}
		for i, rb := range rawBranches {
			bm, okbm := rb.(map[string]interface{})
			if !okbm {
				return nil, errInvalidDef("state %q: Branches[%d] must be an object", name, i)
			}
			bdef, err := parseStateDef(bm)
			if err != nil {
				return nil, err
			}
			st.branches = append(st.branches, bdef)
		}
	case "Map":
		st.itemsPath = jsonPathField(m, "ItemsPath")
		if is, okis := m["ItemSelector"]; okis {
			ism, okism := is.(map[string]interface{})
			if !okism {
				return nil, errInvalidDef("state %q: ItemSelector must be an object", name)
			}
			st.itemSelector = ism
		}
		mc, err := intField(m, "MaxConcurrency", 0)
		if err != nil {
			return nil, errInvalidDef("state %q: %v", name, err)
		}
		st.maxConcurrency = mc
		if it, okit := m["Iterator"]; okit {
			itm, okitm := it.(map[string]interface{})
			if !okitm {
				return nil, errInvalidDef("state %q: Iterator must be an object", name)
			}
			iter, err := parseStateDef(itm)
			if err != nil {
				return nil, err
			}
			st.iterator = iter
		}
		if ip, okip := m["ItemProcessor"]; okip {
			ipm, okipm := ip.(map[string]interface{})
			if !okipm {
				return nil, errInvalidDef("state %q: ItemProcessor must be an object", name)
			}
			proc, err := parseStateDef(ipm)
			if err != nil {
				return nil, err
			}
			st.itemProcessor = proc
		}
		if st.iterator != nil && st.itemProcessor != nil {
			return nil, errInvalidDef("Map state %q cannot specify both Iterator and ItemProcessor", name)
		}
		if st.iterator == nil && st.itemProcessor == nil {
			return nil, errInvalidDef("Map state %q requires Iterator or ItemProcessor", name)
		}
	case "Succeed":
		// terminal by definition
	case "Fail":
		e, oke := m["Error"].(string)
		if !oke || e == "" {
			return nil, errInvalidDef("Fail state %q requires an Error name", name)
		}
		st.failError = e
		st.failCause, _ = m["Cause"].(string)
	default:
		return nil, errInvalidDef("state %q has unknown Type %q", name, st.typ)
	}
	return st, nil
}

func jsonPathField(m map[string]interface{}, key string) *string {
	v, ok := m[key]
	if !ok {
		return nil
	}
	if v == nil {
		s := "null"
		return &s
	}
	if s, ok2 := v.(string); ok2 {
		return &s
	}
	return nil
}

func readNumber(v interface{}) (float64, error) {
	f, ok := asFloat(v)
	if !ok {
		return 0, fmt.Errorf("expected a number, got %T", v)
	}
	return f, nil
}

func intField(m map[string]interface{}, key string, def int) (int, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return def, nil
	}
	f, err := readNumber(v)
	if err != nil {
		return 0, errInvalidDef("%q must be a number", key)
	}
	return int(f), nil
}

func floatField(m map[string]interface{}, key string, def float64) (float64, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return def, nil
	}
	f, err := readNumber(v)
	if err != nil {
		return 0, errInvalidDef("%q must be a number", key)
	}
	return f, nil
}

func parseRetryList(raw interface{}, stateName string) ([]retryDecl, error) {
	if raw == nil {
		return nil, nil
	}
	arr, ok := raw.([]interface{})
	if !ok {
		return nil, errInvalidDef("state %q: Retry must be an array", stateName)
	}
	out := make([]retryDecl, 0, len(arr))
	for i, ra := range arr {
		rm, ok := ra.(map[string]interface{})
		if !ok {
			return nil, errInvalidDef("state %q: Retry[%d] must be an object", stateName, i)
		}
		pats, err := parseErrorEquals(rm["ErrorEquals"])
		if err != nil {
			return nil, err
		}
		if len(pats) == 0 {
			return nil, errInvalidDef("state %q: Retry[%d] requires ErrorEquals", stateName, i)
		}
		r := retryDecl{patterns: pats, interval: time.Second, maxAttempts: 3, backoff: 2.0, jitter: "FULL"}
		iv, err := floatField(rm, "IntervalSeconds", 1)
		if err != nil {
			return nil, err
		}
		r.interval = time.Duration(iv * float64(time.Second))
		ma, err := intField(rm, "MaxAttempts", 3)
		if err != nil {
			return nil, err
		}
		if ma < 0 {
			return nil, errInvalidDef("state %q: Retry[%d]: MaxAttempts must be non-negative", stateName, i)
		}
		r.maxAttempts = ma
		bo, err := floatField(rm, "BackoffRate", 2.0)
		if err != nil {
			return nil, err
		}
		if bo <= 0 {
			return nil, errInvalidDef("state %q: Retry[%d]: BackoffRate must be positive", stateName, i)
		}
		r.backoff = bo
		md, err := floatField(rm, "MaxDelaySeconds", 0)
		if err != nil {
			return nil, err
		}
		if md < 0 {
			md = 0
		}
		r.maxDelay = time.Duration(md * float64(time.Second))
		jstr, _ := rm["JitterStrategy"].(string)
		js := strings.ToUpper(strings.TrimSpace(jstr))
		switch js {
		case "EQUAL", "NONE":
			r.jitter = js
		default:
			r.jitter = "FULL"
		}
		out = append(out, r)
	}
	return out, nil
}

func parseCatchList(raw interface{}, stateName string) ([]catchDecl, error) {
	if raw == nil {
		return nil, nil
	}
	arr, ok := raw.([]interface{})
	if !ok {
		return nil, errInvalidDef("state %q: Catch must be an array", stateName)
	}
	out := make([]catchDecl, 0, len(arr))
	for i, ra := range arr {
		rm, ok := ra.(map[string]interface{})
		if !ok {
			return nil, errInvalidDef("state %q: Catch[%d] must be an object", stateName, i)
		}
		pats, err := parseErrorEquals(rm["ErrorEquals"])
		if err != nil {
			return nil, err
		}
		if len(pats) == 0 {
			return nil, errInvalidDef("state %q: Catch[%d] requires ErrorEquals", stateName, i)
		}
		c := catchDecl{patterns: pats, resultPath: jsonPathField(rm, "ResultPath")}
		nx, okn := rm["Next"].(string)
		if !okn || nx == "" {
			return nil, errInvalidDef("state %q: Catch[%d] requires Next", stateName, i)
		}
		c.next = nx
		out = append(out, c)
	}
	return out, nil
}

func parseErrorEquals(raw interface{}) ([]string, error) {
	arr, ok := raw.([]interface{})
	if !ok {
		return nil, errInvalidDef("ErrorEquals must be an array of error names")
	}
	pats := make([]string, 0, len(arr))
	for _, e := range arr {
		s, ok := e.(string)
		if !ok {
			return nil, errInvalidDef("ErrorEquals entries must be strings")
		}
		pats = append(pats, s)
	}
	return pats, nil
}

func parseChoiceRule(raw interface{}) (choiceRule, error) {
	r := choiceRule{}
	m, ok := raw.(map[string]interface{})
	if !ok {
		return r, errInvalidDef("choice rule must be an object")
	}
	for k, v := range m {
		switch k {
		case "And":
			arr, ok2 := v.([]interface{})
			if !ok2 {
				return r, errInvalidDef("And must be an array of rules")
			}
			for _, sub := range arr {
				sr, err := parseChoiceRule(sub)
				if err != nil {
					return r, err
				}
				r.and = append(r.and, sr)
			}
		case "Or":
			arr, ok2 := v.([]interface{})
			if !ok2 {
				return r, errInvalidDef("Or must be an array of rules")
			}
			for _, sub := range arr {
				sr, err := parseChoiceRule(sub)
				if err != nil {
					return r, err
				}
				r.or = append(r.or, sr)
			}
		case "Not":
			sr, err := parseChoiceRule(v)
			if err != nil {
				return r, err
			}
			r.not = &sr
		case "Variable":
			s, ok2 := v.(string)
			if !ok2 {
				return r, errInvalidDef("Variable must be a JSONPath string")
			}
			r.variable = s
		case "Next":
			s, ok2 := v.(string)
			if !ok2 {
				return r, errInvalidDef("Next must be a string")
			}
			r.next = s
		case "Comment":
			// informational
		default:
			if !choiceOpNames[k] {
				return r, errInvalidDef("unsupported Choice operator %q", k)
			}
			if r.op != "" {
				return r, errInvalidDef("duplicate Choice operator in rule")
			}
			r.op = k
			r.value = v
		}
	}
	if r.op == "" && len(r.and) == 0 && len(r.or) == 0 && r.not == nil {
		return r, errInvalidDef("choice rule must include a comparison, And, Or or Not")
	}
	if r.op != "" && r.variable == "" {
		return r, errInvalidDef("choice rule with %q requires Variable", r.op)
	}
	return r, nil
}

func validateStateDef(d *stateDef) error {
	if _, ok := d.states[d.startAt]; !ok {
		return errInvalidDef("StartAt %q is not a defined state", d.startAt)
	}
	for name, st := range d.states {
		switch st.typ {
		case "Succeed", "Fail":
			// terminal; no routing requirements
		default:
			if st.hasNext && st.end {
				return errInvalidDef("state %q cannot set both Next and End", name)
			}
			if !st.hasNext && !st.end {
				return errInvalidDef("state %q must set either Next or End", name)
			}
			if st.hasNext {
				if _, ok := d.states[st.next]; !ok {
					return errInvalidDef("state %q: Next %q is not a defined state", name, st.next)
				}
			}
		}
		switch st.typ {
		case "Choice":
			if st.deflt != "" {
				if _, ok := d.states[st.deflt]; !ok {
					return errInvalidDef("state %q: Default %q is not a defined state", name, st.deflt)
				}
			}
			for i := range st.choices {
				if err := validateChoiceRule(&st.choices[i], d); err != nil {
					return errInvalidDef("state %q: %v", name, err)
				}
			}
		case "Task", "Parallel", "Map":
			for i := range st.catch {
				if _, ok := d.states[st.catch[i].next]; !ok {
					return errInvalidDef("state %q: Catch Next %q is not a defined state", name, st.catch[i].next)
				}
			}
		}
	}
	return nil
}

func validateChoiceRule(r *choiceRule, d *stateDef) error {
	for i := range r.and {
		if err := validateChoiceRule(&r.and[i], d); err != nil {
			return err
		}
	}
	for i := range r.or {
		if err := validateChoiceRule(&r.or[i], d); err != nil {
			return err
		}
	}
	if r.not != nil {
		if err := validateChoiceRule(r.not, d); err != nil {
			return err
		}
	}
	if r.op != "" {
		if r.next == "" {
			return errInvalidDef("choice rule with %q requires Next", r.op)
		}
		if _, ok := d.states[r.next]; !ok {
			return errInvalidDef("choice rule Next %q is not a defined state", r.next)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Execution runtime
// ---------------------------------------------------------------------------

type execRec struct {
	arn        string
	machineArn string
	name       string
	status     string
	input      string
	output     string
	errName    string
	errCause   string
	startedAt  time.Time
	stoppedAt  time.Time
	stopErr    string
	stopCause  string
	stopped    bool
	sm         *stateMachine
	cancel     chan struct{}
	done       chan struct{}
}

type execInstance struct {
	e        *Engine
	rec      *execRec
	cancel   <-chan struct{}
	timeoutC <-chan struct{}
	ctxRoot  map[string]interface{}
}

type errSlot struct {
	idx int
	e   *SMError
}

func (x *execInstance) runStateMachine(def *stateDef, input interface{}) (interface{}, *SMError) {
	state, ok := def.states[def.startAt]
	if !ok {
		return nil, smErr(ErrStatesTaskFailed, "StartAt %q is missing", def.startAt)
	}
	cur := input
	for {
		x.enterState(state.name)
		next, out, sErr := x.runState(state, deepCopy(cur))
		if sErr != nil {
			if sErr.Abort || sErr.Timer {
				return nil, sErr
			}
			if c, matched := matchCatchRule(state.catch, sErr); matched {
				payload := map[string]interface{}{"Error": sErr.Name, "Cause": sErr.Cause}
				merged, mErr := mergeResultPath(deepCopy(cur), c.resultPath, payload)
				if mErr != nil {
					return nil, mErr
				}
				cur = merged
				state = def.states[c.next]
				continue
			}
			return nil, sErr
		}
		if next == "" {
			return out, nil
		}
		cur = out
		state = def.states[next]
	}
}

func (x *execInstance) enterState(name string) {
	x.ctxRoot["State"] = map[string]interface{}{
		"Name":        name,
		"EnteredTime": rfc3339(x.e.now()),
	}
}

func terminalNext(st *stateDecl) string {
	if st.end {
		return ""
	}
	return st.next
}

func (x *execInstance) runState(st *stateDecl, input interface{}) (string, interface{}, *SMError) {
	eff, sErr := x.applyPath(input, st.inputPath, ErrStatesInputPathMatchFailure)
	if sErr != nil {
		return "", nil, sErr
	}
	switch st.typ {
	case "Pass":
		var res interface{}
		if st.hasResult {
			res = deepCopy(st.result)
		} else if st.parameters != nil {
			p, pErr := evalParameters(st.parameters, eff, x.ctxRoot)
			if pErr != nil {
				return "", nil, pErr
			}
			res = p
		} else {
			res = eff
		}
		out, fErr := x.finishState(st, eff, res)
		if fErr != nil {
			return "", nil, fErr
		}
		return terminalNext(st), out, nil

	case "Wait":
		d, wErr := x.waitDuration(st, eff)
		if wErr != nil {
			return "", nil, wErr
		}
		if slErr := x.sleep(d); slErr != nil {
			return "", nil, slErr
		}
		out, oErr := x.applyPath(eff, st.outputPath, ErrStatesOutputPathMatchFailure)
		if oErr != nil {
			return "", nil, oErr
		}
		return terminalNext(st), out, nil

	case "Choice":
		for i := range st.choices {
			matched, cErr := x.evalChoiceRule(&st.choices[i], eff)
			if cErr != nil {
				return "", nil, cErr
			}
			if matched {
				out, oErr := x.applyPath(eff, st.outputPath, ErrStatesOutputPathMatchFailure)
				if oErr != nil {
					return "", nil, oErr
				}
				return st.choices[i].next, out, nil
			}
		}
		if st.deflt != "" {
			out, oErr := x.applyPath(eff, st.outputPath, ErrStatesOutputPathMatchFailure)
			if oErr != nil {
				return "", nil, oErr
			}
			return st.deflt, out, nil
		}
		return "", nil, smErr(ErrStatesNoChoiceMatched, "no choice matched and no Default was set in state %q", st.name)

	case "Succeed":
		out, oErr := x.applyPath(eff, st.outputPath, ErrStatesOutputPathMatchFailure)
		if oErr != nil {
			return "", nil, oErr
		}
		return "", out, nil

	case "Fail":
		return "", nil, &SMError{Name: st.failError, Cause: st.failCause}

	case "Task":
		tIn := eff
		if st.parameters != nil {
			p, pErr := evalParameters(st.parameters, eff, x.ctxRoot)
			if pErr != nil {
				return "", nil, pErr
			}
			tIn = p
		}
		raw, rErr := x.runWithRetry(st, func() (interface{}, *SMError) {
			return x.runTaskBody(st, tIn)
		})
		if rErr != nil {
			return "", nil, rErr
		}
		out, fErr := x.finishState(st, eff, raw)
		if fErr != nil {
			return "", nil, fErr
		}
		return terminalNext(st), out, nil

	case "Parallel":
		raw, pErr := x.runParallel(st, eff)
		if pErr != nil {
			return "", nil, pErr
		}
		out, fErr := x.finishState(st, eff, raw)
		if fErr != nil {
			return "", nil, fErr
		}
		return terminalNext(st), out, nil

	case "Map":
		raw, mErr := x.runMap(st, eff)
		if mErr != nil {
			return "", nil, mErr
		}
		out, fErr := x.finishState(st, eff, raw)
		if fErr != nil {
			return "", nil, fErr
		}
		return terminalNext(st), out, nil
	}
	return "", nil, smErr(ErrStatesTaskFailed, "unsupported state type %q", st.typ)
}

func (x *execInstance) applyPath(v interface{}, p *string, failName string) (interface{}, *SMError) {
	if p == nil {
		return v, nil
	}
	s := *p
	if s == "null" {
		return map[string]interface{}{}, nil
	}
	val, found, err := evalRefFound(s, v, x.ctxRoot)
	if err != nil {
		return nil, smErr(failName, "path %q: %v", s, err)
	}
	if !found {
		return nil, smErr(failName, "path %q did not match the state input", s)
	}
	return val, nil
}

func (x *execInstance) waitDuration(st *stateDecl, eff interface{}) (time.Duration, *SMError) {
	switch {
	case st.secondsPath != nil:
		val, found, err := evalRefFound(*st.secondsPath, eff, x.ctxRoot)
		if err != nil || !found {
			return 0, smErr(ErrStatesRuntime, "SecondsPath %q did not resolve to a number", *st.secondsPath)
		}
		f, ok := asFloat(val)
		if !ok {
			return 0, smErr(ErrStatesRuntime, "SecondsPath %q did not resolve to a number", *st.secondsPath)
		}
		return secondsToDuration(f), nil
	case st.timestampPath != nil:
		val, found, err := evalRefFound(*st.timestampPath, eff, x.ctxRoot)
		if err != nil || !found {
			return 0, smErr(ErrStatesRuntime, "TimestampPath %q did not resolve to a timestamp", *st.timestampPath)
		}
		s, ok := val.(string)
		t, okt := parseTimestamp(s)
		if !ok || !okt {
			return 0, smErr(ErrStatesRuntime, "TimestampPath %q did not resolve to an RFC3339 timestamp", *st.timestampPath)
		}
		d := t.Sub(x.e.now())
		if d < 0 {
			d = 0
		}
		return d, nil
	case st.hasSeconds:
		return secondsToDuration(float64(st.seconds)), nil
	case st.timestamp != "":
		t, okt := parseTimestamp(st.timestamp)
		if !okt {
			return 0, smErr(ErrStatesRuntime, "Timestamp %q is not a valid RFC3339 timestamp", st.timestamp)
		}
		d := t.Sub(x.e.now())
		if d < 0 {
			d = 0
		}
		return d, nil
	}
	return 0, nil
}

func (x *execInstance) sleep(d time.Duration) *SMError {
	if d > 0 {
		t := time.NewTimer(d)
		select {
		case <-x.cancel:
			t.Stop()
			return errAborted
		case <-x.timeoutC:
			t.Stop()
			return errExecTimedOut
		case <-t.C:
			return nil
		}
	}
	select {
	case <-x.cancel:
		return errAborted
	case <-x.timeoutC:
		return errExecTimedOut
	default:
		return nil
	}
}

func matchRetryRule(rules []retryDecl, e *SMError) *retryDecl {
	for i := range rules {
		for _, p := range rules[i].patterns {
			if p == ErrStatesAll || p == e.Name {
				return &rules[i]
			}
		}
	}
	return nil
}

func matchCatchRule(rules []catchDecl, e *SMError) (*catchDecl, bool) {
	for i := range rules {
		for _, p := range rules[i].patterns {
			if p == ErrStatesAll || p == e.Name {
				return &rules[i], true
			}
		}
	}
	return nil, false
}

func retryDelay(r *retryDecl, failures int) time.Duration {
	d := float64(r.interval)
	for i := 0; i < failures && d <= 1e15; i++ {
		d *= r.backoff
	}
	if r.maxDelay > 0 && d > float64(r.maxDelay) {
		d = float64(r.maxDelay)
	}
	switch r.jitter {
	case "NONE":
		return time.Duration(d)
	case "EQUAL":
		half := d / 2
		return time.Duration(half + rand.Float64()*half)
	default: // FULL jitter
		return time.Duration(rand.Float64() * d)
	}
}

func (x *execInstance) runWithRetry(st *stateDecl, body func() (interface{}, *SMError)) (interface{}, *SMError) {
	failures := 0
	for {
		res, sErr := body()
		if sErr == nil {
			return res, nil
		}
		if sErr.Abort || sErr.Timer {
			return nil, sErr
		}
		rule := matchRetryRule(st.retry, sErr)
		if rule == nil || failures >= rule.maxAttempts {
			return nil, sErr
		}
		d := retryDelay(rule, failures)
		failures++
		if slErr := x.sleep(d); slErr != nil {
			return nil, slErr
		}
	}
}

func (x *execInstance) finishState(st *stateDecl, base interface{}, raw interface{}) (interface{}, *SMError) {
	res := raw
	if st.resultSelector != nil {
		sel, sErr := evalParameters(st.resultSelector, raw, x.ctxRoot)
		if sErr != nil {
			return nil, &SMError{Name: ErrStatesResultSelectorMatchFail, Cause: sErr.Cause}
		}
		res = sel
	}
	out, mErr := mergeResultPath(base, st.resultPath, res)
	if mErr != nil {
		return nil, mErr
	}
	return x.applyPath(out, st.outputPath, ErrStatesOutputPathMatchFailure)
}

func mergeResultPath(base interface{}, rp *string, result interface{}) (interface{}, *SMError) {
	path := "$",
	_ = path
	path = "$"
	if rp != nil {
		path = *rp
	}
	if path == "null" {
		return base, nil
	}
	if !strings.HasPrefix(path, "$") {
		path = "$." + path
	}
	p, _, err := parseJSONPath(path)
	if err != nil {
		return nil, smErr(ErrStatesResultPathMatchFailure, "invalid ResultPath %q: %v", path, err)
	}
	if len(p.toks) == 0 {
		return result, nil
	}
	merged, cerr := insertAtPath(deepCopy(base), p.toks, deepCopy(result))
	if cerr != nil {
		return nil, smErr(ErrStatesResultPathMatchFailure, "ResultPath %q: %v", path, cerr)
	}
	return merged, nil
}

func insertAtPath(root interface{}, toks []jpTok, val interface{}) (interface{}, error) {
	t := toks[0]
	rest := toks[1:]
	switch t.kind {
	case jpKey:
		m, ok := root.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("cannot write field %q into a non-object", t.key)
		}
		if len(rest) == 0 {
			m[t.key] = val
			return root, nil
		}
		child, ok := m[t.key]
		if !ok || child == nil {
			child = map[string]interface{}{}
		}
		next, err := insertAtPath(child, rest, val)
		if err != nil {
			return nil, err
		}
		m[t.key] = next
		return root, nil
	case jpIndex:
		arr, ok := root.([]interface{})
		if !ok {
			return nil, fmt.Errorf("cannot write an index into a non-array")
		}
		idx := t.idx
		if idx < 0 {
			idx += len(arr)
		}
		if idx < 0 || idx >= len(arr) {
			return nil, fmt.Errorf("array index out of range")
		}
		if len(rest) == 0 {
			arr[idx] = val
			return arr, nil
		}
		next, err := insertAtPath(arr[idx], rest, val)
		if err != nil {
			return nil, err
		}
		arr[idx] = next
		return arr, nil
	default:
		return nil, fmt.Errorf("ResultPath supports only field/index selectors")
	}
}

// evalParameters realizes an ASL Parameters / ResultSelector / ItemSelector
// object: keys ending in "."+"$" carry a JSONPath reference or an intrinsic
// function; all other entries are literals (nested objects are realized
// recursively). Shared with the DDB emulator's expression handling.
func evalParameters(params map[string]interface{}, doc interface{}, ctxm map[string]interface{}) (interface{}, *SMError) {
	out := make(map[string]interface{}, len(params))
	for k, v := range params {
		if strings.HasSuffix(k, "."+"$") {
			key := k[:len(k)-2]
			s, ok := v.(string)
			if !ok {
				return nil, smErr(ErrStatesParamPathFormatFailure, "field %q must carry a JSONPath or intrinsic string value", k)
			}
			if strings.HasPrefix(s, "States.") {
				fv, err := evalIntrinsic(s, doc, ctxm)
				if err != nil {
					return nil, smErr(ErrStatesParamPathFailure, "field %q: %v", k, err)
				}
				out[key] = fv
				continue
			}
			val, found, err := evalRefFound(s, doc, ctxm)
			if err != nil {
				return nil, smErr(ErrStatesParamPathFormatFailure, "field %q: %v", k, err)
			}
			if !found {
				return nil, smErr(ErrStatesParamPathFailure, "field %q: path %q did not match", k, s)
			}
			out[key] = val
			continue
		}
		switch t := v.(type) {
		case map[string]interface{}:
			sub, sErr := evalParameters(t, doc, ctxm)
			if sErr != nil {
				return nil, sErr
			}
			out[k] = sub
		case []interface{}:
			arr := make([]interface{}, len(t))
			for i, el := range t {
				if em, ok := el.(map[string]interface{}); ok {
					sub, sErr := evalParameters(em, doc, ctxm)
					if sErr != nil {
						return nil, sErr
					}
					arr[i] = sub
				} else {
					arr[i] = deepCopy(el)
				}
			}
			out[k] = arr
		default:
			out[k] = deepCopy(v)
		}
	}
	return out, nil
}

func (x *execInstance) evalChoiceRule(r *choiceRule, doc interface{}) (bool, *SMError) {
	if r.not != nil {
		ok, err := x.evalChoiceRule(r.not, doc)
		if err != nil {
			return false, err
		}
		return !ok, nil
	}
	if len(r.and) > 0 {
		for i := range r.and {
			ok, err := x.evalChoiceRule(&r.and[i], doc)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
		}
		return true, nil
	}
	if len(r.or) > 0 {
		for i := range r.or {
			ok, err := x.evalChoiceRule(&r.or[i], doc)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	}
	val, found, vErr := resolveChoiceVariable(r.variable, doc, x.ctxRoot)
	if vErr != nil {
		return false, vErr
	}
	return x.compareChoice(r.op, r.value, val, found, doc)
}

func resolveChoiceVariable(expr string, doc interface{}, ctxm map[string]interface{}) (interface{}, bool, *SMError) {
	if strings.HasPrefix(expr, "States.") {
		v, err := evalIntrinsic(expr, doc, ctxm)
		if err != nil {
			return nil, false, smErr(ErrStatesRuntime, "Choice Variable %q: %v", expr, err)
		}
		return v, true, nil
	}
	p, isCtx, err := parseJSONPath(expr)
	if err != nil {
		return nil, false, smErr(ErrStatesRuntime, "Choice Variable %q: %v", expr, err)
	}
	root := interface{}(doc)
	if isCtx {
		root = ctxm
	}
	nodes := p.evalNodes(root)
	if len(nodes) == 0 {
		return nil, false, nil
	}
	if len(nodes) == 1 {
		return nodes[0], true, nil
	}
	return nodes, true, nil
}

func (x *execInstance) compareChoice(op string, cmpRaw interface{}, val interface{}, found bool, doc interface{}) (bool, *SMError) {
	switch op {
	case "IsPresent":
		return found, nil
	case "IsAbsent":
		return !found, nil
	}
	if !found {
		return false, nil
	}
	switch op {
	case "IsNull":
		return val == nil, nil
	case "IsNotNull":
		return val != nil, nil
	case "IsBool":
		_, ok := val.(bool)
		return ok, nil
	case "IsTrue":
		b, ok := val.(bool)
		return ok && b, nil
	case "IsFalse":
		b, ok := val.(bool)
		return ok && !b, nil
	case "IsString":
		_, ok := val.(string)
		return ok, nil
	case "IsNumeric":
		_, ok := asFloat(val)
		return ok, nil
	case "IsTimestamp":
		s, ok := val.(string)
		_, okt := parseTimestamp(s)
		return ok && okt, nil
	}
	cmp := cmpRaw
	if strings.HasSuffix(op, "Path") {
		s, ok := cmpRaw.(string)
		if !ok {
			return false, nil
		}
		p, isCtx, err := parseJSONPath(s)
		if err != nil {
			return false, smErr(ErrStatesRuntime, "%s: %v", op, err)
		}
		root := interface{}(doc)
		if isCtx {
			root = x.ctxRoot
		}
		nodes := p.evalNodes(root)
		if len(nodes) == 0 {
			return false, nil
		}
		if len(nodes) == 1 {
			cmp = nodes[0]
		} else {
			cmp = nodes
		}
	}
	switch {
	case strings.HasPrefix(op, "String"):
		s1, ok1 := val.(string)
		s2, ok2 := cmp.(string)
		if !ok1 || !ok2 {
			return false, nil
		}
		switch op {
		case "StringEquals", "StringEqualsPath":
			return s1 == s2, nil
		case "StringLessThan", "StringLessThanPath":
			return s1 < s2, nil
		case "StringGreaterThan", "StringGreaterThanPath":
			return s1 > s2, nil
		case "StringLessThanEquals", "StringLessThanEqualsPath":
			return s1 <= s2, nil
		case "StringGreaterThanEquals", "StringGreaterThanEqualsPath":
			return s1 >= s2, nil
		}
	case strings.HasPrefix(op, "Numeric"):
		f1, ok1 := asFloat(val)
		f2, ok2 := asFloat(cmp)
		if !ok1 || !ok2 {
			return false, nil
		}
		switch op {
		case "NumericEquals", "NumericEqualsPath":
			return f1 == f2, nil
		case "NumericLessThan", "NumericLessThanPath":
			return f1 < f2, nil
		case "NumericGreaterThan", "NumericGreaterThanPath":
			return f1 > f2, nil
		case "NumericLessThanEquals", "NumericLessThanEqualsPath":
			return f1 <= f2, nil
		case "NumericGreaterThanEquals", "NumericGreaterThanEqualsPath":
			return f1 >= f2, nil
		}
	case strings.HasPrefix(op, "Boolean"):
		b1, ok1 := val.(bool)
		b2, ok2 := cmp.(bool)
		return ok1 && ok2 && b1 == b2, nil
	case strings.HasPrefix(op, "Timestamp"):
		s1, ok1 := val.(string)
		s2, ok2 := cmp.(string)
		if !ok1 || !ok2 {
			return false, nil
		}
		t1, p1 := parseTimestamp(s1)
		t2, p2 := parseTimestamp(s2)
		if !p1 || !p2 {
			return false, nil
		}
		switch op {
		case "TimestampEquals", "TimestampEqualsPath":
			return t1.Equal(t2), nil
		case "TimestampLessThan", "TimestampLessThanPath":
			return t1.Before(t2), nil
		case "TimestampGreaterThan", "TimestampGreaterThanPath":
			return t1.After(t2), nil
		case "TimestampLessThanEquals", "TimestampLessThanEqualsPath":
			return !t1.After(t2), nil
		case "TimestampGreaterThanEquals", "TimestampGreaterThanEqualsPath":
			return !t1.Before(t2), nil
		}
	}
	return false, smErr(ErrStatesRuntime, "unsupported comparison %q", op)
}

func (x *execInstance) runParallel(st *stateDecl, eff interface{}) (interface{}, *SMError) {
	n := len(st.branches)
	results := make([]interface{}, n)
	errCh := make(chan errSlot, n)
	var wg sync.WaitGroup
	for i, bdef := range st.branches {
		wg.Add(1)
		go func(i int, bdef *stateDef) {
			defer wg.Done()
			child := x.spawnChild(nil)
			out, bErr := child.runStateMachine(bdef, deepCopy(eff))
			if bErr != nil {
				errCh <- errSlot{idx: i, e: bErr}
				return
			}
			results[i] = deepCopy(out)
		}(i, bdef)
	}
	wg.Wait()
	close(errCh)
	if e := firstBranchError(errCh); e != nil {
		return nil, e
	}
	return results, nil
}

func (x *execInstance) runMap(st *stateDecl, eff interface{}) (interface{}, *SMError) {
	itemsPath := "$"
	if st.itemsPath != nil {
		itemsPath = *st.itemsPath
	}
	itemsV, found, refErr := evalRefFound(itemsPath, eff, x.ctxRoot)
	if refErr != nil || !found {
		return nil, smErr(ErrStatesRuntime, "Map state %q: ItemsPath %q did not match the state input", st.name, itemsPath)
	}
	items, ok := itemsV.([]interface{})
	if !ok {
		return nil, smErr(ErrStatesRuntime, "Map state %q: ItemsPath %q did not resolve to an array", st.name, itemsPath)
	}
	iter := st.itemProcessor
	if iter == nil {
		iter = st.iterator
	}
	n := len(items)
	results := make([]interface{}, n)
	errCh := make(chan errSlot, n)
	var sem chan struct{}
	if st.maxConcurrency > 0 {
		sem = make(chan struct{}, st.maxConcurrency)
	}
	var wg sync.WaitGroup
	for i, item := range items {
		wg.Add(1)
		go func(i int, item interface{}) {
			defer wg.Done()
			if sem != nil {
				sem <- struct{}{}
				defer func() { <-sem }()
			}
			ctxT := x.mapContext(item, i)
			var itemInput interface{}
			if st.itemSelector != nil {
				sel, selErr := evalParameters(st.itemSelector, deepCopy(item), ctxT)
				if selErr != nil {
					errCh <- errSlot{idx: i, e: selErr}
					return
				}
				itemInput = sel
			} else {
				itemInput = deepCopy(item)
			}
			child := x.spawnChild(ctxT)
			out, iErr := child.runStateMachine(iter, itemInput)
			if iErr != nil {
				errCh <- errSlot{idx: i, e: iErr}
				return
			}
			results[i] = deepCopy(out)
		}(i, item)
	}
	wg.Wait()
	close(errCh)
	if e := firstBranchError(errCh); e != nil {
		return nil, e
	}
	return results, nil
}

func (x *execInstance) mapContext(item interface{}, idx int) map[string]interface{} {
	c := deepCopy(x.ctxRoot).(map[string]interface{})
	c["Map"] = map[string]interface{}{
		"Item": map[string]interface{}{
			"Index": json.Number(strconv.Itoa(idx)),
			"Value": deepCopy(item),
		},
	}
	return c
}

func (x *execInstance) spawnChild(ctxT map[string]interface{}) *execInstance {
	if ctxT == nil {
		ctxT = deepCopy(x.ctxRoot).(map[string]interface{})
	}
	return &execInstance{
		e:        x.e,
		rec:      x.rec,
		cancel:   x.cancel,
		timeoutC: x.timeoutC,
		ctxRoot:  ctxT,
	}
}

func firstBranchError(ch chan errSlot) *SMError {
	var firstAbort *SMError
	firstAbortIdx := 1 << 30
	var firstFail *SMError
	firstFailIdx := 1 << 30
	for s := range ch {
		if s.e.Abort || s.e.Timer {
			if s.idx < firstAbortIdx {
				firstAbortIdx = s.idx
				firstAbort = s.e
			}
			continue
		}
		if s.idx < firstFailIdx {
			firstFailIdx = s.idx
			firstFail = s.e
		}
	}
	if firstFail != nil {
		return firstFail
	}
	return firstAbort
}

// TaskContext is passed to registered Task handlers.
type TaskContext struct {
	Resource        string
	StateName       string
	StateMachineArn string
	ExecutionArn    string
	Context         map[string]interface{} // deep copy of the $$. context object
}

// TaskHandler implements the behavior of a Task state's Resource. Returning a
// *SMError surfaces a typed ASL error (picked up by Retry/Catch); any other
// error becomes States.TaskFailed.
type TaskHandler func(tc *TaskContext, input interface{}) (interface{}, error)

func (x *execInstance) runTaskBody(st *stateDecl, input interface{}) (interface{}, *SMError) {
	h := x.e.taskHandler(st.resource)
	if h == nil {
		if x.e.passthroughTasks {
			// Default integration: report the realized input back as the task
			// result (a no-op integration; enables handler-less Task states in
			// tests and demos).
			return input, nil
		}
		return nil, smErr(ErrStatesTaskFailed, "no task handler registered for resource '%s'", st.resource)
	}
	tctx := &TaskContext{
		Resource:        st.resource,
		StateName:       st.name,
		StateMachineArn: x.rec.sm.arn,
		ExecutionArn:    x.rec.arn,
		Context:         deepCopy(x.ctxRoot).(map[string]interface{}),
	}
	var result interface{}
	var hErr error
	invoke := func() {
		defer func() {
			if r := recover(); r != nil {
				hErr = fmt.Errorf("task handler for %q panicked: %v", st.resource, r)
			}
		}()
		result, hErr = h(tctx, input)
	}
	if st.timeoutSeconds > 0 {
		done := make(chan struct{}, 1)
		go func() {
			invoke()
			done <- struct{}{}
		}()
		t := time.NewTimer(time.Duration(st.timeoutSeconds) * time.Second)
		select {
		case <-done:
			t.Stop()
		case <-x.cancel:
			t.Stop()
			return nil, errAborted
		case <-x.timeoutC:
			t.Stop()
			return nil, errExecTimedOut
		case <-t.C:
			return nil, smErr(ErrStatesTimeout, "state %q timed out after %d seconds", st.name, st.timeoutSeconds)
		}
	} else {
		select {
		case <-x.cancel:
			return nil, errAborted
		case <-x.timeoutC:
			return nil, errExecTimedOut
		default:
		}
		invoke()
	}
	if hErr != nil {
		if se, ok := hErr.(*SMError); ok {
			return nil, se
		}
		return nil, smErr(ErrStatesTaskFailed, "%v", hErr)
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Step Functions API surface
// ---------------------------------------------------------------------------

// Engine is the Step Functions (ASL) emulator surface. One instance hosts any
// number of state machines and executions. Task integrations are supplied via
// RegisterTaskHandler; the shared JSONPath/expression evaluator below is the
// same one the DDB emulator uses.
type Engine struct {
	region           string
	account          string
	passthroughTasks bool

	mu       sync.Mutex
	machines map[string]*machineRec
	names    map[string]string
	execs    map[string]*execRec
	handlers map[string]TaskHandler
	nextID   int64
}

// NewEngine creates an emulator bound to a default virtual account.
func NewEngine() *Engine {
	return NewEngineWithConfig("us-east-1", "000000000000")
}

// NewEngineWithConfig creates an emulator with explicit ARN components.
func NewEngineWithConfig(region, account string) *Engine {
	return &Engine{
		region:           region,
		account:          account,
		passthroughTasks: true,
		machines:         make(map[string]*machineRec),
		 names:           make(map[string]string),
		execs:            make(map[string]*execRec),
		handlers:         make(map[string]TaskHandler),
	}
}

func (e *Engine) now() time.Time { return time.Now().UTC() }

func (e *Engine) nextSeq() int64 {
	e.mu.Lock()
	e.nextID++
	n := e.nextID
	e.mu.Unlock()
	return n
}

// RegisterTaskHandler registers integration behavior for a Task Resource
// ARN. The special key "*" acts as a fallback for unregistered resources.
func (e *Engine) RegisterTaskHandler(resource string, h TaskHandler) {
	e.mu.Lock()
	e.handlers[resource] = h
	e.mu.Unlock()
}

// UnregisterTaskHandler removes a previously registered handler.
func (e *Engine) UnregisterTaskHandler(resource string) {
	e.mu.Lock()
	delete(e.handlers, resource)
	e.mu.Unlock()
}

// SetDefaultTaskPassthrough controls behavior for Tasks without a registered
// handler: enabled (default) the task echoes its realized input back as the
// result; disabled it fails with States.TaskFailed.
func (e *Engine) SetDefaultTaskPassthrough(v bool) {
	e.mu.Lock()
	e.passthroughTasks = v
	e.mu.Unlock()
}

func (e *Engine) taskHandler(resource string) TaskHandler {
	e.mu.Lock()
	h, ok := e.handlers[resource]
	if !ok {
		h = e.handlers["*"]
	}
	e.mu.Unlock()
	return h
}

type Tag struct {
	Key   string `json:"Key,omitempty"`
	Value string `json:"Value,omitempty"`
}

type CreateStateMachineInput struct {
	Name       string `json:"name"`
	Definition string `json:"definition"`
	RoleArn    string `json:"roleArn,omitempty"`
	Type       string `json:"type,omitempty"`
	Tags       []Tag  `json:"tags,omitempty"`
}

type CreateStateMachineOutput struct {
	StateMachineArn string    `json:"stateMachineArn"`
	CreationDate    time.Time `json:"creationDate"`
}

type DescribeStateMachineInput struct {
	StateMachineArn string `json:"stateMachineArn"`
}

type DescribeStateMachineOutput struct {
	StateMachineArn string    `json:"stateMachineArn"`
	Name            string    `json:"name"`
	Status          string    `json:"status"`
	Type            string    `json:"type"`
	RoleArn         string    `json:"roleArn,omitempty"`
	Definition      string    `json:"definition"`
	CreationDate    time.Time `json:"creationDate"`
}

type DeleteStateMachineInput struct {
	StateMachineArn string `json:"stateMachineArn"`
}

type DeleteStateMachineOutput struct{}

type StartExecutionInput struct {
	StateMachineArn string `json:"stateMachineArn"`
	Name            string `json:"name,omitempty"`
	Input           string `json:"input,omitempty"`
}

type StartExecutionOutput struct {
	ExecutionArn string    `json:"executionArn"`
	Name         string    `json:"name"`
	StartDate    time.Time `json:"startDate"`
}

type StopExecutionInput struct {
	ExecutionArn string `json:"executionArn"`
	Error        string `json:"error,omitempty"`
	Cause        string `json:"cause,omitempty"`
}

type StopExecutionOutput struct {
	StopDate time.Time `json:"stopDate"`
}

type DescribeExecutionInput struct {
	ExecutionArn string `json:"executionArn"`
}

type DescribeExecutionOutput struct {
	ExecutionArn    string    `json:"executionArn"`
	StateMachineArn string    `json:"stateMachineArn"`
	Name            string    `json:"name"`
	Status          string    `json:"status"`
	Input           string    `json:"input"`
	Output          string    `json:"output,omitempty"`
	Error           string    `json:"error,omitempty"`
	Cause           string    `json:"cause,omitempty"`
	StartDate       time.Time `json:"startDate"`
	StopDate        time.Time `json:"stopDate"`
}

type ListExecutionsInput struct {
	StateMachineArn string `json:"stateMachineArn"`
	StatusFilter    string `json:"statusFilter,omitempty"`
	MaxResults      int    `json:"maxResults,omitempty"`
}

type ExecutionListItem struct {
	ExecutionArn    string    `json:"executionArn"`
	StateMachineArn string    `json:"stateMachineArn"`
	Name            string    `json:"name"`
	Status          string    `json:"status"`
	StartDate       time.Time `json:"startDate"`
	StopDate        time.Time `json:"stopDate"`
}

type ListExecutionsOutput struct {
	Executions []ExecutionListItem `json:"executions"`
}

func (e *Engine) machineArn(name string) string {
	return fmt.Sprintf("arn:aws:states:%s:%s:stateMachine:%s", e.region, e.account, name)
}

func (e *Engine) executionArn(smName, execName string) string {
	return fmt.Sprintf("arn:aws:states:%s:%s:execution:%s:%s", e.region, e.account, smName, execName)
}

func (e *Engine) machineLocked(arnOrName string) (*machineRec, error) {
	if rec, ok := e.machines[arnOrName]; ok {
		return rec, nil
	}
	if arn, ok := e.names[arnOrName]; ok {
		if rec, ok := e.machines[arn]; ok {
			return rec, nil
		}
	}
	return nil, apiErr(ErrCodeStateMachineDoesNotExist, "state machine %q does not exist", arnOrName)
}

// CreateStateMachine parses, validates and persists an ASL definition.
func (e *Engine) CreateStateMachine(in *CreateStateMachineInput) (*CreateStateMachineOutput, error) {
	if in == nil {
		return nil, apiErr(ErrCodeInvalidParameter, "input is required")
	}
	if in.Name == "" {
		return nil, apiErr(ErrCodeInvalidParameter, "Name is required")
	}
	if in.Definition == "" {
		return nil, apiErr(ErrCodeInvalidParameter, "Definition is required")
	}
	def, err := parseDefinition(in.Definition)
	if err != nil {
		return nil, err
	}
	typ := strings.ToUpper(strings.TrimSpace(in.Type))
	if typ == "" {
		typ = TypeStandard
	}
	if typ != TypeStandard && typ != TypeExpress {
		return nil, apiErr(ErrCodeInvalidParameter, "Type must be STANDARD or EXPRESS")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.names[in.Name]; exists {
		return nil, apiErr(ErrCodeStateMachineAlreadyExists, "state machine %q already exists", in.Name)
	}
	sm := &stateMachine{
		arn:        e.machineArn(in.Name),
		name:       in.Name,
		roleArn:    in.RoleArn,
		typ:        typ,
		definition: in.Definition,
		createdAt:  e.now(),
		timeoutSec: def.timeoutSec,
		def:        def,
	}
	e.machines[sm.arn] = &machineRec{sm: sm}
	e.names[in.Name] = sm.arn
	return &CreateStateMachineOutput{StateMachineArn: sm.arn, CreationDate: sm.createdAt}, nil
}

// DescribeStateMachine returns the stored machine (including its raw
// definition). Deleted machines remain queryable with status DELETING.
func (e *Engine) DescribeStateMachine(in *DescribeStateMachineInput) (*DescribeStateMachineOutput, error) {
	if in == nil || in.StateMachineArn == "" {
		return nil, apiErr(ErrCodeInvalidParameter, "StateMachineArn is required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	rec, err := e.machineLocked(in.StateMachineArn)
	if err != nil {
		return nil, err
	}
	status := "ACTIVE"
	if rec.deleted {
		status = "DELETING"
	}
	return &DescribeStateMachineOutput{
		StateMachineArn: rec.sm.arn,
		Name:            rec.sm.name,
		Status:          status,
		Type:            rec.sm.typ,
		RoleArn:         rec.sm.roleArn,
		Definition:      rec.sm.definition,
		CreationDate:    rec.sm.createdAt,
	}, nil
}

// DeleteStateMachine marks the machine deleted; in-flight executions keep
// running but no new executions are accepted.
func (e *Engine) DeleteStateMachine(in *DeleteStateMachineInput) (*DeleteStateMachineOutput, error) {
	if in == nil || in.StateMachineArn == "" {
		return nil, apiErr(ErrCodeInvalidParameter, "StateMachineArn is required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	rec, err := e.machineLocked(in.StateMachineArn)
	if err != nil {
		return nil, err
	}
	rec.deleted = true
	return &DeleteStateMachineOutput{}, nil
}

// StartExecution begins an execution asynchronously (like the real API),
// returning immediately with the execution ARN.
func (e *Engine) StartExecution(in *StartExecutionInput) (*StartExecutionOutput, error) {
	if in == nil || in.StateMachineArn == "" {
		return nil, apiErr(ErrCodeInvalidParameter, "StateMachineArn is required")
	}
	e.mu.Lock()
	rec, err := e.machineLocked(in.StateMachineArn)
	if err != nil {
		e.mu.Unlock()
		return nil, err
	}
	if rec.deleted {
		e.mu.Unlock()
		return nil, apiErr(ErrCodeStateMachineDeleting, "state machine %q is being deleted", rec.sm.name)
	}
	sm := rec.sm
	input := in.Input
	if strings.TrimSpace(input) == "" {
		input = "{}"
	}
	inputDoc, derr := jsonDecode(input)
	if derr != nil {
		e.mu.Unlock()
		return nil, apiErr(ErrCodeInvalidExecutionInput, "Input is not valid JSON: %v", derr)
	}
	name := in.Name
	if name == "" {
		e.nextID++
		name = fmt.Sprintf("execution-%d", e.nextID)
	}
	execArn := e.executionArn(sm.name, name)
	if old, ok := e.execs[execArn]; ok && old.status == StatusRunning {
		e.mu.Unlock()
		return nil, apiErr(ErrCodeExecutionAlreadyExists, "an active execution named %q already exists for state machine %q", name, sm.name)
	}
	e.nextID++
	er := &execRec{
		arn:        execArn,
		machineArn: sm.arn,
		name:       name,
		status:     StatusRunning,
		input:      input,
		startedAt:  e.now(),
		sm:         sm,
		cancel:     make(chan struct{}),
		done:       make(chan struct{}),
	}
	e.execs[execArn] = er
	ctxRoot := contextRoot(er, sm, inputDoc)
	e.mu.Unlock()

	go func() {
		x := &execInstance{e: e, rec: er, cancel: er.cancel, ctxRoot: ctxRoot}
		if sm.timeoutSec > 0 {
			t := time.NewTimer(time.Duration(sm.timeoutSec) * time.Second)
			defer t.Stop()
			x.timeoutC = t.C
		}
		out, sErr := x.runStateMachine(sm.def, deepCopy(inputDoc))
		e.finalize(er, out, sErr)
	}()
	return &StartExecutionOutput{ExecutionArn: execArn, Name: name, StartDate: er.startedAt}, nil
}

func (e *Engine) finalize(er *execRec, out interface{}, sErr *SMError) {
	e.mu.Lock()
	defer e.mu.Unlock()
	switch {
	case sErr == nil:
		er.status = StatusSucceeded
		er.output = mustMarshalJSON(out)
	case sErr.Abort:
		er.status = StatusAborted
		name := er.stopErr
		if name == "" {
			name = ErrStatesTaskFailed
		}
		er.errName = name
		er.errCause = er.stopCause
	case sErr.Timer || sErr.Name == ErrStatesTimeout || sErr.Name == ErrStatesHeartbeatTimeout:
		er.status = StatusTimedOut
		er.errName = ErrStatesTimeout
		er.errCause = sErr.Cause
	default:
		er.status = StatusFailed
		er.errName = sErr.Name
		er.errCause = sErr.Cause
	}
	if er.stoppedAt.IsZero() {
		er.stoppedAt = e.now()
	}
	close(er.done)
}

func contextRoot(er *execRec, sm *stateMachine, inputDoc interface{}) map[string]interface{} {
	return map[string]interface{}{
		"Execution": map[string]interface{}{
			"Id":        er.arn,
			"Name":      er.name,
			"RoleArn":   sm.roleArn,
			"StartTime": rfc3339(er.startedAt),
			"Input":     deepCopy(inputDoc),
		},
		"StateMachine": map[string]interface{}{
			"Id":   sm.arn,
			"Name": sm.name,
		},
		"State": map[string]interface{}{
			"Name":        "",
			"EnteredTime": rfc3339(er.startedAt),
		},
	}
}

// DescribeExecution reports the current status/input/output of an execution.
func (e *Engine) DescribeExecution(in *DescribeExecutionInput) (*DescribeExecutionOutput, error) {
	if in == nil || in.ExecutionArn == "" {
		return nil, apiErr(ErrCodeInvalidParameter, "ExecutionArn is required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	er, ok := e.execs[in.ExecutionArn]
	if !ok {
		return nil, apiErr(ErrCodeExecutionDoesNotExist, "execution %q does not exist", in.ExecutionArn)
	}
	return &DescribeExecutionOutput{
		ExecutionArn:    er.arn,
		StateMachineArn: er.machineArn,
		Name:            er.name,
		Status:          er.status,
		Input:           er.input,
		Output:          er.output,
		Error:           er.errName,
		Cause:           er.errCause,
		StartDate:       er.startedAt,
		StopDate:        er.stoppedAt,
	}, nil
}

// ListExecutions returns executions of a state machine, newest first.
func (e *Engine) ListExecutions(in *ListExecutionsInput) (*ListExecutionsOutput, error) {
	if in == nil || in.StateMachineArn == "" {
		return nil, apiErr(ErrCodeInvalidParameter, "StateMachineArn is required")
	}
	e.mu.Lock()
	rec, err := e.machineLocked(in.StateMachineArn)
	if err != nil {
		e.mu.Unlock()
		return nil, err
	}
	items := []ExecutionListItem{}
	filter := strings.ToUpper(in.StatusFilter)
	for _, er := range e.execs {
		if er.machineArn != rec.sm.arn {
			continue
		}
		if filter != "" && er.status != filter {
			continue
		}
		items = append(items, ExecutionListItem{
			ExecutionArn:    er.arn,
			StateMachineArn: er.machineArn,
			Name:            er.name,
			Status:          er.status,
			StartDate:       er.startedAt,
			StopDate:        er.stoppedAt,
		})
	}
	e.mu.Unlock()
	sort.Slice(items, func(i, j int) bool {
		if items[i].StartDate.Equal(items[j].StartDate) {
			return items[i].ExecutionArn > items[j].ExecutionArn
		}
		return items[i].StartDate.After(items[j].StartDate)
	})
	if in.MaxResults > 0 && len(items) > in.MaxResults {
		items = items[:in.MaxResults]
	}
	return &ListExecutionsOutput{Executions: items}, nil
}

// StopExecution aborts a running execution (idempotent; safe to call twice).
func (e *Engine) StopExecution(in *StopExecutionInput) (*StopExecutionOutput, error) {
	if in == nil || in.ExecutionArn == "" {
		return nil, apiErr(ErrCodeInvalidParameter, "ExecutionArn is required")
	}
	e.mu.Lock()
	er, ok := e.execs[in.ExecutionArn]
	if !ok {
		e.mu.Unlock()
		return nil, apiErr(ErrCodeExecutionDoesNotExist, "execution %q does not exist", in.ExecutionArn)
	}
	if er.status != StatusRunning {
		d := er.stoppedAt
		if d.IsZero() {
			d = e.now()
		}
		e.mu.Unlock()
		return &StopExecutionOutput{StopDate: d}, nil
	}
	if er.stopped {
		d := er.stoppedAt
		e.mu.Unlock()
		return &StopExecutionOutput{StopDate: d}, nil
	}
	er.stopped = true
	er.stoppedAt = e.now()
	stopDate := er.stoppedAt
	if in.Error != "" {
		er.stopErr = in.Error
	} else {
		er.stopErr = ErrStatesTaskFailed
	}
	er.stopCause = in.Cause
	close(er.cancel)
	e.mu.Unlock()
	return &StopExecutionOutput{StopDate: stopDate}, nil
}

// WaitForExecution blocks until the execution reaches a terminal status (or
// the deadline elapses; timeout <= 0 waits indefinitely).
func (e *Engine) WaitForExecution(executionArn string, timeout time.Duration) (*DescribeExecutionOutput, error) {
	e.mu.Lock()
	er, ok := e.execs[executionArn]
	if !ok {
		e.mu.Unlock()
		return nil, apiErr(ErrCodeExecutionDoesNotExist, "execution %q does not exist", executionArn)
	}
	done := er.done
	e.mu.Unlock()
	if timeout > 0 {
		select {
		case <-done:
		case <-time.After(timeout):
			return nil, apiErr(ErrCodeDeadlineExceeded, "timed out waiting for execution %q", executionArn)
		}
	} else {
		<-done
	}
	return e.DescribeExecution(&DescribeExecutionInput{ExecutionArn: executionArn})
}

// RunExecution starts an execution and blocks until it completes.
func (e *Engine) RunExecution(in *StartExecutionInput, timeout time.Duration) (*DescribeExecutionOutput, error) {
	so, err := e.StartExecution(in)
	if err != nil {
		return nil, err
	}
	return e.WaitForExecution(so.ExecutionArn, timeout)
}

// RunDefinition is a convenience for tests: registers an ad-hoc state
// machine, runs input through it, and returns the JSON output. A failed
// execution surfaces as the *SMError that caused the failure.
func (e *Engine) RunDefinition(definition, input string) (string, *SMError, error) {
	name := fmt.Sprintf("adhoc-%d", e.nextSeq())
	co, err := e.CreateStateMachine(&CreateStateMachineInput{Name: name, Definition: definition})
	if err != nil {
		return "", nil, err
	}
	defer func() {
		_, _ = e.DeleteStateMachine(&DeleteStateMachineInput{StateMachineArn: co.StateMachineArn})
	}()
	desc, err := e.RunExecution(&StartExecutionInput{
		StateMachineArn: co.StateMachineArn,
		Input:           input,
	}, 0)
	if err != nil {
		return "", nil, err
	}
	switch desc.Status {
	case StatusSucceeded:
		return desc.Output, nil, nil
	case StatusFailed, StatusAborted, StatusTimedOut:
		return "", &SMError{Name: desc.Error, Cause: desc.Cause}, nil
	}
	return "", nil, apiErr(ErrCodeInvalidParameter, "execution ended with unexpected status %q", desc.Status)
}

// ---------------------------------------------------------------------------
// Self test / demo (main)
// ---------------------------------------------------------------------------

func main() {
	if !selfTest() {
		os.Exit(1)
	}
	fmt.Println("floci ASL interpreter: all self-tests passed")
}

func jsonEqual(a, b string) bool {
	va, aerr := jsonDecode(a)
	vb, berr := jsonDecode(b)
	if aerr != nil || berr != nil {
		return a == b
	}
	return reflect.DeepEqual(va, vb)
}

func selfTest() bool {
	ok := true
	fail := func(format string, a ...interface{}) {
		ok = false
		fmt.Printf("FAIL: %s\n", fmt.Sprintf(format, a...))
	}
	run := func(eng *Engine, def, input string) (string, *SMError) {
		out, smE, err := eng.RunDefinition(def, input)
		if err != nil {
			fail("engine error: %v", err)
			return "", nil
		}
		return out, smE
	}

	// 1. Choice + Pass + ResultPath + Succeed
	eng := NewEngine()
	choiceDef := `{
		"StartAt": "Check",
		"States": {
			"Check": {
				"Type": "Choice",
				"Choices": [
					{"Variable": "$.v", "NumericGreaterThanEquals": 10, "Next": "Big"},
					{"Variable": "$.v", "IsString": true, "Next": "Small"},
					{"Variable": "$.v", "IsNull": true, "Next": "Small"}
				],
				"Default": "Unknown"
			},
			"Big":     {"Type": "Pass", "Result": {"size": "big"}, "ResultPath": "result", "Next": "Fin"},
			"Small":   {"Type": "Pass", "Result": {"size": "small"}, "ResultPath": "result", "Next": "Fin"},
			"Unknown": {"Type": "Pass", "Result": {"size": "unknown"}, "ResultPath": "result", "Next": "Fin"},
			"Fin":     {"Type": "Succeed"}
		}
	}`
	out, _ := run(eng, choiceDef, `{"v":42}`)
	if !jsonEqual(out, `{"v":42,"result":{"size":"big"}}`) {
		fail("choice numeric: got %s", out)
	}
	out, _ = run(eng, choiceDef, `{"v":"forty-two"}`)
	if !jsonEqual(out, `{"v":"forty-two","result":{"size":"small"}}`) {
		fail("choice string: got %s", out)
	}
	out, _ = run(eng, choiceDef, `{"v":true}`)
	if !jsonEqual(out, `{"v":true,"result":{"size":"unknown"}}`) {
		fail("choice default: got %s", out)
	}

	// 2. Task passthrough + Parameters intrinsics + ResultSelector
	eng2 := NewEngine()
	taskDef := `{
		"StartAt": "T",
		"States": {
			"T": {
				"Type": "Task",
				"Resource": "arn:test:no-op",
				"Parameters": {
					"msg.$": "States.Format('v={}.', $.v)",
					"n.$”: "$.v"
				},
				"ResultSelector": {
					"n.$": "$.n",
					"m.$": "$.msg"
				},
				"ResultPath": "out",
				"Next": "Fin"
			},
			"Fin": {"Type": "Succeed"}
		}
	}`
	out, _ = run(eng2, taskDef, `{"v":7}`)
	if !jsonEqual(out, `{"v":7,"out":{"m":"v=7.","n":7}}`) {
		fail("task passthrough+intrinsics: got %s", out)
	}

	// 3. Task Retry: transient failures then success (exponential backoff, jitter)
	eng3 := NewEngine()
	eng3.SetDefaultTaskPassthrough(false)
	attempts := 0
	eng3.RegisterTaskHandler("arn:test:flaky", func(tc *TaskContext, in interface{}) (interface{}, error) {
		attempts++
		if attempts < 3 {
			return nil, &SMError{Name: "Flaky", Cause: "transient failure"}
		}
		return map[string]interface{}{"ok": true, "attempts": float64(attempts)}, nil
	})
	retryDef := `{
		"StartAt": "T",
		"States": {
			"T": {
				"Type": "Task",
				"Resource": "arn:test:flaky",
				"Retry": [{"ErrorEquals": ["Flaky"], "IntervalSeconds": 0, "MaxAttempts": 5, "BackoffRate": 2, "JitterStrategy": "NONE"}],
				"ResultPath": "t",
				"Next": "Fin"
			},
			"Fin": {"Type": "Succeed"}
		}
	}`
	out, _ = run(eng3, retryDef, `{}`)
	if !jsonEqual(out, `{"t":{"attempts":3,"ok":true}}`) {
		fail("task retry: got %s (attempts=%d)", out, attempts)
	}
	if attempts != 3 {
		fail("task retry attempts: got %d want 3", attempts)
	}

	// 4. Retry exhaustion surfaces the error
	eng4 := NewEngine()
	eng4.SetDefaultTaskPassthrough(false)
	exhAttempts := 0
	eng4.RegisterTaskHandler("arn:test:always-fail", func(tc *TaskContext, in interface{}) (interface{}, error) {
		exhAttempts++
		return nil, &SMError{Name: "Always", Cause: "boom"}
	})
	exhaustDef := `{
		"StartAt": "T",
		"States": {
			"T": {
				"Type": "Task",
				"Resource": "arn:test:always-fail",
				"Retry": [{"ErrorEquals": ["Always"], "IntervalSeconds": 0, "MaxAttempts": 2}],
				"Next": "Fin"
			},
			"Fin": {"Type": "Succeed"}
		}
	}`
	_, smE := run(eng4, exhaustDef, `{}`)
	if smE == nil || smE.Name != "Always" {
		fail("retry exhaustion: expected error Always, got %+v", smE)
	}
	if exhAttempts != 3 {
		fail("retry exhaustion attempts: got %d want 3", exhAttempts)
	}

	// 5. Catch on Task failure merges {Error, Cause} at ResultPath
	eng5 := NewEngine()
	eng5.SetDefaultTaskPassthrough(false)
	catchDef := `{
		"StartAt": "T",
		"States": {
			"T": {
				"Type": "Task",
				"Resource": "arn:test:missing",
				"Catch": [{"ErrorEquals": ["States.TaskFailed"], "ResultPath": "err", "Next": "Fixed"}],
				"Next": "Unreachable"
			},
			"Fixed": {"Type": "Pass", "Result": {"fixed": true}, "ResultPath": "was", "End": true},
			"Unreachable": {"Type": "Succeed"}
		}
	}`
	out, _ = run(eng5, catchDef, `{"n":1}`)
	if !jsonEqual(out, `{"err":{"Cause":"no task handler registered for resource 'arn:test:missing'","Error":"States.TaskFailed"},"n":1,"was":{"fixed":true}}`) {
		fail("task catch: got %s", out)
	}

	// 6. Parallel: combined results, branch failure, Catch, uncaught failure
	eng6 := NewEngine()
	parOk := `{
		"StartAt": "P",
		"States": {
			"P": {
				"Type": "Parallel",
				"Branches": [
					{"StartAt": "A", "States": {"A": {"Type": "Pass", "Result": 1, "End": true}}},
					{"StartAt": "B", "States": {"B": {"Type": "Pass", "Result": {"b": 2}, "End": true}}}
				],
				"End": true
			}
		}
	}`
	out, _ = run(eng6, parOk, `{}`)
	if !jsonEqual(out, `[1,{"b":2}]`) {
		fail("parallel: got %s", out)
	}
	parFail := `{
		"StartAt": "P",
		"States": {
			"P": {
				"Type": "Parallel",
				"Branches": [
					{"StartAt": "A", "States": {"A": {"Type": "Pass", "Result": "ok", "End": true}}},
					{"StartAt": "B", "States": {"B": {"Type": "Fail", "Error": "BranchBoom", "Cause": "branch blew up"}}}
				],
				"Catch": [{"ErrorEquals": ["BranchBoom"], "ResultPath": "oops", "Next": "Fixed"}],
				"Next": "Fin"
			},
			"Fixed": {"Type": "Pass", "Result": {"recovered": true}, "ResultPath": "rec", "End": true},
			"Fin":   {"Type": "Succeed"}
		}
	}`
	out, _ = run(eng6, parFail, `{"seed":9}`)
	if !jsonEqual(out, `{"oops":{"Cause":"branch blew up","Error":"BranchBoom"},"rec":{"recovered":true},"seed":9}`) {
		fail("parallel catch: got %s", out)
	}
	parFail2 := `{
		"StartAt": "P",
		"States": {
			"P": {
				"Type": "Parallel",
				"Branches": [
					{"StartAt": "B", "States": {"B": {"Type": "Fail", "Error": "BranchBoom", "Cause": "x"}}}
				],
				"End": true
			}
		}
	}`
	_, smE = run(eng6, parFail2, `{}`)
	if smE == nil || smE.Name != "BranchBoom" {
		fail("parallel uncaught: got %+v", smE)
	}

	// 7. Map: ItemSelector fan-out (incl. $$.Map.Item.Index) and legacy Iterator
	eng7 := NewEngine()
	mapDef := `{
		"StartAt": "Fan",
		"States": {
			"Fan": {
				"Type": "Map",
				"ItemsPath": "$.items",
				"ItemSelector": {"i.$": "$$.Map.Item.Index", "v.$": "$.x"},
				"MaxConcurrency": 2,
				"ItemProcessor": {
					"StartAt": "Work",
					"States": {
						"Work": {
							"Type": "Pass",
							"Parameters": {"i.$": "$.i", "doubled.$": "States.MathAdd($.v, $.v)"},
							"End": true
						}
					}
				},
				"ResultSelector": {"count.$": "States.ArrayLength($)"},
				"ResultPath": "res",
				"End": true
			}
		}
	}`
	out, _ = run(eng7, mapDef, `{"keep":true,"items":[{"x":1},{"x":2}]}`)
	if !jsonEqual(out, `{"items":[{"x":1},{"x":2}],"keep":true,"res":{"count":2}}`) {
		fail("map itemselector: got %s", out)
	}
	mapLegacy := `{
		"StartAt": "Fan",
		"States": {
			"Fan": {
				"Type": "Map",
				"ItemsPath": "$.arr",
				"Iterator": {
					"StartAt": "Double",
					"States": {
						"Double": {"Type": "Pass", "Parameters": {"v.$": "States.MathAdd($.x, $.x)"}, "End": true}
					}
				},
				"End": true
			}
		}
	}`
	out, _ = run(eng7, mapLegacy, `{"arr":[{"x":1},{"x":2}]}`)
	if !jsonEqual(out, `[{"v":2},{"v":4}]`) {
		fail("map legacy iterator: got %s", out)
	}

	// 8. Wait (Seconds, SecondsPath, Timestamp-in-past)
	eng8 := NewEngine()
	waitDef := `{
		"StartAt": "W",
		"States": {
			"W":  {"Type": "Wait", "Seconds": 0, "Next": "W2"},
			"W2": {"Type": "Wait", "SecondsPath": "$.s", "Next": "W3"},
			"W3": {"Type": "Wait", "Timestamp": "1990-01-01T00:00:00Z", "Next": "Fin"},
			"Fin": {"Type": "Pass", "Result": {"done": true}, "End": true}
		}
	}`
	out, _ = run(eng8, waitDef, `{"s":0}`)
	if !jsonEqual(out, `{"done":true}`) {
		fail("wait: got %s", out)
	}

	// 9. Fail state + InputPath/OutputPath
	eng9 := NewEngine()
	failDef := `{"StartAt": "Boom", "States": {"Boom": {"Type": "Fail", "Error": "Custom.Fail", "Cause": "because"}}}`
	_, smE = run(eng9, failDef, `{}`)
	if smE == nil || smE.Name != "Custom.Fail" || smE.Cause != "because" {
		fail("fail state: got %+v", smE)
	}
	ioDef := `{
		"StartAt": "P",
		"States": {
			"P": {"Type": "Pass", "InputPath": "$.a", "Result": {"r": 1}, "ResultPath": "r", "OutputPath": "$.r", "End": true}
		}
	}`
	out, _ = run(eng9, ioDef, `{"a":{"b":2}}`)
	if !jsonEqual(out, `{"r":1}`) {
		fail("input/output path: got %s", out)
	}

	// 10. Engine API: CRUD, Start/Describe/List/Stop, duplicate names, delete
	eng10 := NewEngine()
	def := `{"StartAt": "W", "States": {"W": {"Type": "Wait", "Seconds": 30, "Next": "Fin"}, "Fin": {"Type": "Succeed"}}}`
	co, cerr := eng10.CreateStateMachine(&CreateStateMachineInput{Name: "demo", Definition: def, RoleArn: "arn:aws:iam::000000000000:role/demo"})
	if cerr != nil {
		fail("create: %v", cerr)
	}
	dm, derr := eng10.DescribeStateMachine(&DescribeStateMachineInput{StateMachineArn: co.StateMachineArn})
	if derr != nil || dm.Name != "demo" || dm.Type != "STANDARD" || dm.Status != "ACTIVE" || !dm.CreationDate.After(time.Time{}) || dm.Definition != def {
		fail("describe machine: %+v err=%v", dm, derr)
	}
	dm2, derr2 := eng10.DescribeStateMachine(&DescribeStateMachineInput{StateMachineArn: "demo"})
	if derr2 != nil || dm2.Name != "demo" {
		fail("describe machine by name: %+v err=%v", dm2, derr2)
	}
	so, serr := eng10.StartExecution(&StartExecutionInput{StateMachineArn: co.StateMachineArn, Name: "run-1", Input: `{"a":1}`})
	if serr != nil {
		fail("start: %v", serr)
	}
	_, dupErr := eng10.StartExecution(&StartExecutionInput{StateMachineArn: co.StateMachineArn, Name: "run-1"})
	ac, okc := dupErr.(*APIError)
	if !okc || ac.Code != ErrCodeExecutionAlreadyExists {
		fail("duplicate execution: got %v", dupErr)
	}
	ls, lerr := eng10.ListExecutions(&ListExecutionsInput{StateMachineArn: co.StateMachineArn, StatusFilter: "RUNNING"})
	if lerr != nil || len(ls.Executions) != 1 || ls.Executions[0].Name != "run-1" {
		fail("list executions: %+v err=%v", ls, lerr)
	}
	time.Sleep(50 * time.Millisecond)
	st, stopErr := eng10.StopExecution(&StopExecutionInput{ExecutionArn: so.ExecutionArn, Error: "HumanAborted", Cause: "user requested"})
	if stopErr != nil || st.StopDate.IsZero() {
		fail("stop execution: %+v err=%v", st, stopErr)
	}
	wd, werr := eng10.WaitForExecution(so.ExecutionArn, 5*time.Second)
	if werr != nil || wd.Status != StatusAborted || wd.Error != "HumanAborted" || wd.Cause != "user requested" {
		fail("wait execution: %+v err=%v", wd, werr)
	}
	ls2, _ := eng10.ListExecutions(&ListExecutionsInput{StateMachineArn: co.StateMachineArn, StatusFilter: "ABORTED"})
	if len(ls2.Executions) != 1 {
		fail("list aborted: got %d", len(ls2.Executions))
	}
	if _, err := eng10.StopExecution(&StopExecutionInput{ExecutionArn: so.ExecutionArn}); err != nil {
		fail("second stop: %v", err)
	}
	if _, derr3 := eng10.DeleteStateMachine(&DeleteStateMachineInput{StateMachineArn: co.StateMachineArn}); derr3 != nil {
		fail("delete machine: %v", derr3)
	}
	dm3, derr4 := eng10.DescribeStateMachine(&DescribeStateMachineInput{StateMachineArn: "demo"})
	if derr4 != nil || dm3.Status != "DELETING" {
		fail("describe deleted: %+v err=%v", dm3, derr4)
	}
	if _, err := eng10.StartExecution(&StartExecutionInput{StateMachineArn: "demo"}); err == nil {
		fail("start on deleted machine: expected error")
	}
	if _, err := eng10.CreateStateMachine(&CreateStateMachineInput{Name: "bad", Definition: `{"StartAt":"A","States":{"A":{"Type":"Wat","End":true}}}`}); err == nil {
		fail("invalid definition: expected error")
	}

	// 11. Shared JSONPath / intrinsic evaluator units
	node, derr5 := jsonDecode(`{"a":{"b":[10,20]},"k":7}`)
	if derr5 != nil {
		fail("decode: %v", derr5)
	}
	if v, err := EvalJSONPath(node, "$.a.b[1]"); err != nil || !jsonEqual(mustMarshalJSON(v), "20") {
		fail("jsonpath index: %v %v", v, err)
	}
	if v, err := EvalJSONPath(node, "$.a.b[-1]"); err != nil || !jsonEqual(mustMarshalJSON(v), "20") {
		fail("jsonpath negative index: %v %v", v, err)
	}
	if v, err := EvalJSONPath(node, "$.a.b[*]"); err != nil || !jsonEqual(mustMarshalJSON(v), "[10,20]") {
		fail("jsonpath wildcard: %v %v", v, err)
	}
	if v, err := EvalJSONPath(node, "$..b"); err != nil || !jsonEqual(mustMarshalJSON(v), "[10,20]") {
		fail("jsonpath recurse: %v %v", v, err)
	}
	if _, err := EvalJSONPath(node, "$.nope.deep"); err == nil {
		fail("jsonpath miss: expected error")
	}
	if v, err := EvalValueRef("States.Format('n={}.', $.k)", node, nil); err != nil || v != "n=7." {
		fail("intrinsic format: %v %v", v, err)
	}
	if v, err := EvalValueRef("States.Array(1, 2, $.k)", node, nil); err != nil || !jsonEqual(mustMarshalJSON(v), "[1,2,7]") {
		fail("intrinsic array: %v %v", v, err)
	}

	return ok
}