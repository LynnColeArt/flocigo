package sfn

import (
	crand "crypto/rand"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"math/rand"
	"strconv"
	"strings"
	"unicode/utf8"
)

// IntrinsicResolver resolves reference-path arguments of an intrinsic
// function against the current state input and context object.
type IntrinsicResolver func(path string) (any, error)

const intrinsicPrefix = "States."

type intrinsicArgKind int

const (
	argString intrinsicArgKind = iota
	argNumber
	argBool
	argNull
	argPath
	argIntrinsic
)

type intrinsicArg struct {
	kind intrinsicArgKind
	s    string
	n    json.Number
	b    bool
	expr *intrinsicExpr
}

type intrinsicExpr struct {
	name string
	args []intrinsicArg
}

type intrinsicFunc func(vals []any) (any, error)

// intrinsicImpls holds every supported ASL intrinsic function. The semantics
// (including States.<Name> error names) follow the documented AWS behavior.
var intrinsicImpls = map[string]intrinsicFunc{
	"Format":       fnFormat,
	"StringToJson": fnStringToJson,
	"JsonToString": fnJsonToString,
	"Array":        fnArray,
	"ArrayGetItem": fnArrayGetItem,
	"ArrayLength":  fnArrayLength,
	"ArrayUnique":  fnArrayUnique,
	"Base64Encode": fnBase64Encode,
	"Base64Decode": fnBase64Decode,
	"Hash":         fnHash,
	"JsonMerge":    fnJsonMerge,
	"MathAdd":      fnMathAdd,
	"MathRandom":   fnMathRandom,
	"StringSplit":  fnStringSplit,
	"UUID":         fnUUID,
}

// IntrinsicFunctionNames lists every supported intrinsic function, prefixed
// with "States.".
func IntrinsicFunctionNames() []string {
	names := make([]string, 0, len(intrinsicImpls))
	for n := range intrinsicImpls {
		names = append(names, intrinsicPrefix+n)
	}
	return names
}

// ---------------------------------------------------------------------------
// Parser: States.Name(arg, arg) where args are single-quoted string literals
// (with \ and \' escapes, like AWS), numbers, true/false/null, nested
// intrinsic calls and reference paths like $.foo[0].
// ---------------------------------------------------------------------------

type intrinsicParser struct {
	s string
	i int
}

func (p *intrinsicParser) hasPrefix(s string) bool { return strings.HasPrefix(p.s[p.i:], s) }

func (p *intrinsicParser) peek() byte {
	if p.i < len(p.s) {
		return p.s[p.i]
	}
	return 0
}

func (p *intrinsicParser) skipWS() {
	for p.i < len(p.s) {
		c := p.s[p.i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			p.i++
			continue
		}
		break
	}
}

// EvalIntrinsic parses and evaluates a standalone intrinsic expression such
// as "States.Format('Hello {}', $.name)". The whole string must be exactly
// one intrinsic invocation, as AWS requires inside Parameters selectors.
func EvalIntrinsic(expr string, resolver IntrinsicResolver) (any, error) {
	p := &intrinsicParser{s: expr}
	p.skipWS()
	e, err := parseIntrinsicCall(p)
	if err != nil {
		return nil, newStateError(ErrNameParameterPathFormat, err.Error())
	}
	p.skipWS()
	if p.i != len(p.s) {
		return nil, newStateError(ErrNameParameterPathFormat, fmt.Sprintf("unexpected characters after intrinsic expression at position %d", p.i))
	}
	return evalIntrinsicExpr(e, resolver)
}

func isIntrinsicNameByte(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func isNumberByte(c byte) bool {
	return (c >= '0' && c <= '9') || c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-'
}

func parseIntrinsicCall(p *intrinsicParser) (*intrinsicExpr, error) {
	if !p.hasPrefix(intrinsicPrefix) {
		return nil, fmt.Errorf("expected an intrinsic function starting with %q", intrinsicPrefix)
	}
	p.i += len(intrinsicPrefix)
	start := p.i
	for p.i < len(p.s) && isIntrinsicNameByte(p.s[p.i]) {
		p.i++
	}
	name := p.s[start:p.i]
	if name == "" {
		return nil, errors.New("missing intrinsic function name after prefix")
	}
	p.skipWS()
	if p.peek() != '(' {
		return nil, fmt.Errorf("expected '(' after intrinsic function name %q", name)
	}
	p.i++
	p.skipWS()
	args := []intrinsicArg{}
	if p.peek() == ')' {
		p.i++
		return &intrinsicExpr{name: name, args: args}, nil
	}
	for {
		p.skipWS()
		arg, err := p.parseArg()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
		p.skipWS()
		c := p.peek()
		if c == ',' {
			p.i++
			continue
		}
		if c == ')' {
			p.i++
			return &intrinsicExpr{name: name, args: args}, nil
		}
		return nil, fmt.Errorf("expected ',' or ')' at position %d of intrinsic function %q", p.i, name)
	}
}

func (p *intrinsicParser) parseArg() (intrinsicArg, error) {
	c := p.peek()
	switch {
	case c == '\'':
		s, err := p.parseStringLit()
		if err != nil {
			return intrinsicArg{}, err
		}
		return intrinsicArg{kind: argString, s: s}, nil
	case c == '-' || (c >= '0' && c <= '9'):
		start := p.i
		if c == '-' {
			p.i++
		}
		for p.i < len(p.s) && isNumberByte(p.s[p.i]) {
			p.i++
		}
		text := p.s[start:p.i]
		if _, err := strconv.ParseFloat(text, 64); err != nil {
			return intrinsicArg{}, fmt.Errorf("invalid number literal %q", text)
		}
		return intrinsicArg{kind: argNumber, n: json.Number(text)}, nil
	case p.hasPrefix("true"):
		p.i += 4
		return intrinsicArg{kind: argBool, b: true}, nil
	case p.hasPrefix("false"):
		p.i += 5
		return intrinsicArg{kind: argBool, b: false}, nil
	case p.hasPrefix("null"):
		p.i += 4
		return intrinsicArg{kind: argNull}, nil
	case c == '$':
		path, err := p.scanArgPath()
		if err != nil {
			return intrinsicArg{}, err
		}
		return intrinsicArg{kind: argPath, s: path}, nil
	case c == 'S':
		sub, err := parseIntrinsicCall(p)
		if err != nil {
			return intrinsicArg{}, err
		}
		return intrinsicArg{kind: argIntrinsic, expr: sub}, nil
	case c == 0:
		return intrinsicArg{}, errors.New("unexpected end of intrinsic expression")
	default:
		return intrinsicArg{}, fmt.Errorf("invalid function argument character %q at position %d", string(rune(c)), p.i)
	}
}

// scanArgPath greedily reads a reference path argument (stopping at the first
// top-level ' ' , ) and allowing quoted keys inside brackets).
func (p *intrinsicParser) scanArgPath() (string, error) {
	start := p.i
	depth := 0
	q := byte(0)
	for p.i < len(p.s) {
		c := p.s[p.i]
		if q != 0 {
			if c == '\\' {
				p.i++
				if p.i >= len(p.s) {
					return "", errors.New("unterminated quoted path key")
				}
				p.i++
				continue
			}
			if c == q {
				q = 0
			}
			p.i++
			continue
		}
		switch c {
		case '[':
			depth++
			p.i++
		case ']':
			if depth == 0 {
				return p.s[start:p.i], nil
			}
			depth--
			p.i++
		case '\'', '"':
			if depth > 0 {
				q = c
			}
			p.i++
		case ' ', '\t', '\n', '\r', ',', ')':
			if depth == 0 {
				return p.s[start:p.i], nil
			}
			p.i++
		default:
			p.i++
		}
	}
	return p.s[start:p.i], nil
}

// parseStringLit reads a single-quoted literal. Only \' and \\ are escape
// sequences (as in the ASL spec); every other backslash is kept literally.
func (p *intrinsicParser) parseStringLit() (string, error) {
	p.i++
	var sb strings.Builder
	for p.i < len(p.s) {
		c := p.s[p.i]
		if c == '\\' {
			if p.i+1 >= len(p.s) {
				return "", errors.New("unterminated escape in string literal")
			}
			n := p.s[p.i+1]
			if n == '\'' || n == '\\' {
				sb.WriteByte(n)
			} else {
				sb.WriteByte(c)
				sb.WriteByte(n)
			}
			p.i += 2
			continue
		}
		if c == '\'' {
			p.i++
			return sb.String(), nil
		}
		sb.WriteByte(c)
		p.i++
	}
	return "", errors.New("unterminated string literal")
}

func evalIntrinsicExpr(e *intrinsicExpr, r IntrinsicResolver) (any, error) {
	vals := make([]any, 0, len(e.args))
	for i := range e.args {
		a := &e.args[i]
		var v any
		var err error
		switch a.kind {
		case argString:
			v = a.s
		case argNumber:
			v = a.n
		case argBool:
			v = a.b
		case argNull:
			v = nil
		case argPath:
			v, err = r(a.s)
			if err != nil {
				return nil, newStateError(ErrNameParameterPathFormat, fmt.Sprintf("could not resolve reference path %q in States.%s: %v", a.s, e.name, err))
			}
		case argIntrinsic:
			v, err = evalIntrinsicExpr(a.expr, r)
			if err != nil {
				return nil, err
			}
		}
		vals = append(vals, v)
	}
	fn, ok := intrinsicImpls[e.name]
	if !ok {
		return nil, newStateError(ErrNameParameterPathFormat, fmt.Sprintf("unknown intrinsic function States.%s", e.name))
	}
	return fn(vals)
}

// ---------------------------------------------------------------------------
// Intrinsic implementations
// ---------------------------------------------------------------------------

func fnFormat(vals []any) (any, error) {
	const name = "States.Format"
	if len(vals) < 1 {
		return nil, newStateError(name, "States.Format requires a format string argument")
	}
	f, ok := vals[0].(string)
	if !ok || f == "" {
		return nil, newStateError(name, "the format string must be a non-empty string")
	}
	placeholders := 0
	for i := 0; i < len(f); i++ {
		if f[i] == '{' {
			if i+1 >= len(f) || f[i+1] != '}' {
				return nil, newStateError(name, "mismatched '{' in format string (placeholders are exactly '{}')")
			}
			placeholders++
			i++
		} else if f[i] == '}' {
			return nil, newStateError(name, "unexpected '}' in format string")
		}
	}
	if placeholders != len(vals)-1 {
		return nil, newStateError(name, fmt.Sprintf("format string has %d placeholder(s) but %d argument(s) were supplied", placeholders, len(vals)-1))
	}
	argIdx := 1
	var sb strings.Builder
	for i := 0; i < len(f); i++ {
		if f[i] == '{' && i+1 < len(f) && f[i+1] == '}' {
			s, isStr := vals[argIdx].(string)
			if !isStr {
				return nil, newStateError(name, fmt.Sprintf("the argument for placeholder %d must be a string (convert values with States.JsonToString)", argIdx-1))
			}
			sb.WriteString(s)
			argIdx++
			i++
		} else {
			sb.WriteByte(f[i])
		}
	}
	return sb.String(), nil
}

func fnStringToJson(vals []any) (any, error) {
	const name = "States.StringToJson"
	if len(vals) != 1 {
		return nil, newStateError(name, "States.StringToJson takes exactly one argument")
	}
	s, ok := vals[0].(string)
	if !ok {
		return nil, newStateError(name, "the input to States.StringToJson must be a string containing JSON text")
	}
	v, err := decodeJSONValue([]byte(s))
	if err != nil {
		return nil, newStateError(name, fmt.Sprintf("the input %q is not valid JSON text: %v", s, err))
	}
	return v, nil
}

func fnJsonToString(vals []any) (any, error) {
	const name = "States.JsonToString"
	if len(vals) != 1 {
		return nil, newStateError(name, "States.JsonToString takes exactly one argument")
	}
	return jsonText(vals[0]), nil
}

func fnArray(vals []any) (any, error) {
	const name = "States.Array"
	if len(vals) == 0 {
		return nil, newStateError(name, "States.Array requires at least one argument")
	}
	out := make([]any, len(vals))
	copy(out, vals)
	return out, nil
}

func fnArrayGetItem(vals []any) (any, error) {
	const name = "States.ArrayGetItem"
	if len(vals) != 2 {
		return nil, newStateError(name, "States.ArrayGetItem takes exactly two arguments (array, index)")
	}
	arr, ok := vals[0].([]any)
	if !ok {
		return nil, newStateError(name, "the first argument to States.ArrayGetItem must be an array")
	}
	n, nOK := asNumber(vals[1])
	idx, iOK := numberInt64(n)
	if !nOK || !iOK {
		return nil, newStateError(name, "the second argument to States.ArrayGetItem must be an integer index")
	}
	if idx < 0 || idx >= int64(len(arr)) {
		return nil, newStateError(name, fmt.Sprintf("index %d is out of bounds for an array of length %d", idx, len(arr)))
	}
	return arr[idx], nil
}

func fnArrayLength(vals []any) (any, error) {
	const name = "States.ArrayLength"
	if len(vals) != 1 {
		return nil, newStateError(name, "States.ArrayLength takes exactly one argument")
	}
	arr, ok := vals[0].([]any)
	if !ok {
		return nil, newStateError(name, "the argument to States.ArrayLength must be an array")
	}
	return json.Number(strconv.Itoa(len(arr))), nil
}

func fnArrayUnique(vals []any) (any, error) {
	const name = "States.ArrayUnique"
	if len(vals) != 1 {
		return nil, newStateError(name, "States.ArrayUnique takes exactly one argument")
	}
	arr, ok := vals[0].([]any)
	if !ok {
		return nil, newStateError(name, "the argument to States.ArrayUnique must be an array")
	}
	seen := make(map[string]bool, len(arr))
	out := make([]any, 0, len(arr))
	for _, v := range arr {
		key := jsonText(v)
		if !seen[key] {
			seen[key] = true
			out = append(out, v)
		}
	}
	return out, nil
}

func fnBase64Encode(vals []any) (any, error) {
	const name = "States.Base64Encode"
	if len(vals) != 1 {
		return nil, newStateError(name, "States.Base64Encode takes exactly one argument")
	}
	s, ok := vals[0].(string)
	if !ok {
		return nil, newStateError(name, "the argument to States.Base64Encode must be a string")
	}
	return base64.StdEncoding.EncodeToString([]byte(s)), nil
}

func fnBase64Decode(vals []any) (any, error) {
	const name = "States.Base64Decode"
	if len(vals) != 1 {
		return nil, newStateError(name, "States.Base64Decode takes exactly one argument")
	}
	s, ok := vals[0].(string)
	if !ok {
		return nil, newStateError(name, "the argument to States.Base64Decode must be a string")
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, newStateError(name, fmt.Sprintf("the argument %q is not valid base64: %v", s, err))
	}
	return string(b), nil
}

func fnHash(vals []any) (any, error) {
	const name = "States.Hash"
	if len(vals) != 2 {
		return nil, newStateError(name, "States.Hash takes exactly two arguments (data, algorithm)")
	}
	s, ok := vals[0].(string)
	if !ok {
		return nil, newStateError(name, "the first argument to States.Hash must be a string")
	}
	algo, ok := vals[1].(string)
	if !ok {
		return nil, newStateError(name, "the second argument to States.Hash must be a string naming the algorithm")
	}
	var h hash.Hash
	switch strings.ToUpper(algo) {
	case "MD5":
		h = md5.New()
	case "SHA-1":
		h = sha1.New()
	case "SHA-256":
		h = sha256.New()
	case "SHA-384":
		h = sha512.New384()
	case "SHA-512":
		h = sha512.New()
	default:
		return nil, newStateError(name, fmt.Sprintf("unsupported hash algorithm %q (expected MD5, SHA-1, SHA-256, SHA-384 or SHA-512)", algo))
	}
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil)), nil
}

func fnJsonMerge(vals []any) (any, error) {
	const name = "States.JsonMerge"
	if len(vals) != 2 {
		return nil, newStateError(name, "States.JsonMerge takes exactly two arguments")
	}
	left, ok1 := vals[0].(map[string]any)
	right, ok2 := vals[1].(map[string]any)
	if !ok1 || !ok2 {
		return nil, newStateError(name, "both arguments to States.JsonMerge must be JSON objects")
	}
	// Copy the left side so intrinsic evaluation never mutates state input.
	merged, _ := deepCopy(left).(map[string]any)
	mergeJSONObjects(merged, right)
	return merged, nil
}

func mergeJSONObjects(dst, src map[string]any) {
	for k, v := range src {
		dm, dstIsObj := dst[k].(map[string]any)
		sm, srcIsObj := v.(map[string]any)
		if dstIsObj && srcIsObj {
			mergeJSONObjects(dm, sm)
			continue
		}
		dst[k] = v
	}
}

func fnMathAdd(vals []any) (any, error) {
	const name = "States.MathAdd"
	if len(vals) != 2 {
		return nil, newStateError(name, "States.MathAdd takes exactly two arguments")
	}
	an, ok1 := asNumber(vals[0])
	bn, ok2 := asNumber(vals[1])
	if !ok1 || !ok2 {
		return nil, newStateError(name, "both arguments to States.MathAdd must be numbers")
	}
	ia, aInt := numberInt64(an)
	ib, bInt := numberInt64(bn)
	if aInt && bInt {
		sum := ia + ib
		overflow := (ia > 0 && ib > 0 && sum < 0) || (ia < 0 && ib < 0 && sum >= 0)
		if !overflow {
			return json.Number(strconv.FormatInt(sum, 10)), nil
		}
	}
	af, _ := an.Float64()
	bf, _ := bn.Float64()
	return json.Number(strconv.FormatFloat(af+bf, 'f', -1, 64)), nil
}

func fnMathRandom(vals []any) (any, error) {
	const name = "States.MathRandom"
	if len(vals) != 2 {
		return nil, newStateError(name, "States.MathRandom takes exactly two arguments (start, end)")
	}
	an, ok1 := asNumber(vals[0])
	bn, ok2 := asNumber(vals[1])
	startI, aInt := numberInt64(an)
	endI, bInt := numberInt64(bn)
	if !ok1 || !ok2 || !aInt || !bInt {
		return nil, newStateError(name, "both arguments to States.MathRandom must be integers")
	}
	if startI >= endI {
		return nil, newStateError(name, "the start value must be strictly less than the end value")
	}
	diff := uint64(endI) - uint64(startI) // valid because end > start under two's complement
	if diff <= 1<<53 {
		n := rand.Int63n(int64(diff))
		return json.Number(strconv.FormatInt(startI+n, 10)), nil
	}
	f := float64(startI) + rand.Float64()*float64(endI-startI)
	return json.Number(strconv.FormatFloat(f, 'f', 0, 64)), nil
}

// fnStringSplit treats every character of the second argument as a delimiter,
// keeps empty segments, and supports an optional Java-style limit: the final
// element then holds the remainder. A limit of 0 yields an empty array.
func fnStringSplit(vals []any) (any, error) {
	const name = "States.StringSplit"
	if len(vals) < 2 || len(vals) > 3 {
		return nil, newStateError(name, "States.StringSplit takes two or three arguments (string, delimiters, [limit])")
	}
	s, ok := vals[0].(string)
	if !ok {
		return nil, newStateError(name, "the first argument to States.StringSplit must be a string")
	}
	sep, ok := vals[1].(string)
	if !ok || sep == "" {
		return nil, newStateError(name, "the second argument to States.StringSplit must be a non-empty string of delimiter characters")
	}
	limitPresent := false
	limit := 0
	if len(vals) == 3 {
		n, nIsNum := asNumber(vals[2])
		li, liIsInt := numberInt64(n)
		if !nIsNum || !liIsInt {
			return nil, newStateError(name, "the limit argument to States.StringSplit must be an integer")
		}
		if li < 0 {
			return nil, newStateError(name, "the limit argument to States.StringSplit must not be negative")
		}
		limitPresent = true
		limit = int(li)
	}
	if limitPresent && limit == 0 {
		return []any{}, nil
	}
	delims := make(map[rune]bool)
	for _, r := range sep {
		delims[r] = true
	}
	parts := []any{}
	start := 0
	i := 0
	for i < len(s) {
		r, sz := utf8.DecodeRuneInString(s[i:])
		if delims[r] {
			if limit > 0 && len(parts) == limit-1 {
				parts = append(parts, s[start:])
				return parts, nil
			}
			parts = append(parts, s[start:i])
			start = i + sz
		}
		i += sz
	}
	parts = append(parts, s[start:])
	return parts, nil
}

func fnUUID(vals []any) (any, error) {
	const name = "States.UUID"
	if len(vals) != 0 {
		return nil, newStateError(name, "States.UUID takes no arguments")
	}
	return newUUID(), nil
}

// newUUID returns a random RFC 4122 version 4 UUID (also used for task tokens).
func newUUID() string {
	var b [16]byte
	if _, err := crand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
