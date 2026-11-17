package sfn

import (
	"errors"
	"strings"
	"testing"
)

func resolverFromMap(m map[string]any) IntrinsicResolver {
	return func(path string) (any, error) {
		return resolveRefPath(m, nil, path)
	}
}

func errNameOf(t *testing.T, err error) string {
	t.Helper()
	var se *stateError
	if errors.As(err, &se) {
		return se.ErrName
	}
	t.Fatalf("expected a *stateError, got %T: %v", err, err)
	return ""
}

type intrinsicCase struct {
	name    string
	expr    string
	input   map[string]any
	want    string // expected jsonText of the result
	wantErr string // expected error name; empty means success
}

func TestIntrinsicFunctions(t *testing.T) {
	cases := []intrinsicCase{
		{name: "format basic", expr: `States.Format('Hello, {}!', $.name)`, input: map[string]any{"name": "World"}, want: `"Hello, World!"`},
		{name: "format two placeholders", expr: `States.Format('{} + {} = {}', $.a, $.b, $.c)`, input: map[string]any{"a": "2", "b": "3", "c": "5"}, want: `"2 + 3 = 5"`},
		{name: "format escape quote", expr: `States.Format('It\'s a {}', $.w)`, input: map[string]any{"w": "test"}, want: `"It's a test"`},
		{name: "format whitespace tolerance", expr: `States.Format( 'x{}' , $.n )`, input: map[string]any{"n": "y"}, want: `"xy"`},
		{name: "format nested path", expr: `States.Format('v={}', $.deep.a[1])`, input: map[string]any{"deep": map[string]any{"a": []any{"x", "y"}}}, want: `"v=y"`},
		{name: "format count mismatch", expr: `States.Format('{} {}', $.only)`, input: map[string]any{"only": "x"}, wantErr: "States.Format"},
		{name: "format empty format string", expr: `States.Format('')`, wantErr: "States.Format"},
		{name: "format null format string", expr: `States.Format(null, 'x')`, wantErr: "States.Format"},
		{name: "format non-string placeholder", expr: `States.Format('{}', $.n)`, input: map[string]any{"n": 1}, wantErr: "States.Format"},
		{name: "format unmatched open brace", expr: `States.Format('brace { open', $.x)`, wantErr: "States.Format"},
		{name: "format stray close brace", expr: `States.Format('stray }', $.x)`, wantErr: "States.Format"},
		{name: "string to json", expr: `States.StringToJson('{"k":[1,2,"x"]}')`, want: `{"k":[1,2,"x"]}`},
		{name: "string to json null", expr: `States.StringToJson('null')`, want: `null`},
		{name: "string to json invalid", expr: `States.StringToJson('{oops')`, wantErr: "States.StringToJson"},
		{name: "string to json non-string", expr: `States.StringToJson(42)`, wantErr: "States.StringToJson"},
		{name: "json to string object", expr: `States.JsonToString($.o)`, input: map[string]any{"o": map[string]any{"b": true, "a": nil}}, want: `"{\"a\":null,\"b\":true}"`},
		{name: "json to string number", expr: `States.JsonToString($.n)`, input: map[string]any{"n": 3.5}, want: `"3.5"`},
		{name: "json to string of intrinsic", expr: `States.JsonToString(States.MathAdd(1, 2))`, want: `"3"`},
		{name: "array", expr: `States.Array($.a, 'lit', true, null)`, input: map[string]any{"a": 7}, want: `[7,"lit",true,null]`},
		{name: "array needs arguments", expr: `States.Array()`, wantErr: "States.Array"},
		{name: "array get item", expr: `States.ArrayGetItem($.arr, 1)`, input: map[string]any{"arr": []any{"x", "y", "z"}}, want: `"y"`},
		{name: "array get item via path", expr: `States.ArrayGetItem($.arr, $.i)`, input: map[string]any{"arr": []any{"p", "q"}, "i": 1}, want: `"q"`},
		{name: "array get item out of bounds", expr: `States.ArrayGetItem($.arr, 9)`, input: map[string]any{"arr": []any{"x"}}, wantErr: "States.ArrayGetItem"},
		{name: "array get item negative", expr: `States.ArrayGetItem($.arr, -1)`, input: map[string]any{"arr": []any{"x"}}, wantErr: "States.ArrayGetItem"},
		{name: "array get item non-integer", expr: `States.ArrayGetItem($.arr, 0.5)`, input: map[string]any{"arr": []any{"x"}}, wantErr: "States.ArrayGetItem"},
		{name: "array length", expr: `States.ArrayLength($.arr)`, input: map[string]any{"arr": []any{1, 2}}, want: `2`},
		{name: "array length non-array", expr: `States.ArrayLength($.str)`, input: map[string]any{"str": "nope"}, wantErr: "States.ArrayLength"},
		{name: "array unique", expr: `States.ArrayUnique($.arr)`, input: map[string]any{"arr": []any{"a", "b", "a", 1, 1, 2}}, want: `["a","b",1,2]`},
		{name: "array unique objects", expr: `States.ArrayUnique($.arr)`, input: map[string]any{"arr": []any{map[string]any{"k": 1}, map[string]any{"k": 1}, map[string]any{"k": 2}}}, want: `[{"k":1},{"k":2}]`},
		{name: "base64 encode", expr: `States.Base64Encode('hello floci')`, want: `"aGVsbG8gZmxvY2k="`},
		{name: "base64 decode", expr: `States.Base64Decode('aGVsbG8gZmxvY2k=')`, want: `"hello floci"`},
		{name: "base64 decode invalid", expr: `States.Base64Decode('###')`, wantErr: "States.Base64Decode"},
		{name: "hash sha-256", expr: `States.Hash('hello', 'SHA-256')`, want: `"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"`},
		{name: "hash md5", expr: `States.Hash('hello', 'MD5')`, want: `"5d41402abc4b2a76b9719d911017c592"`},
		{name: "hash bad algorithm", expr: `States.Hash('hello', 'CRC32')`, wantErr: "States.Hash"},
		{name: "hash non-string data", expr: `States.Hash(5, 'MD5')`, wantErr: "States.Hash"},
		{name: "json merge", expr: `States.JsonMerge($.a, $.b)`, input: map[string]any{"a": map[string]any{"x": map[string]any{"p": 1}, "y": 1}, "b": map[string]any{"x": map[string]any{"q": 2}, "z": 3}}, want: `{"x":{"p":1,"q":2},"y":1,"z":3}`},
		{name: "json merge scalars error", expr: `States.JsonMerge($.a, $.b)`, input: map[string]any{"a": 1, "b": 2}, wantErr: "States.JsonMerge"},
		{name: "math add integers", expr: `States.MathAdd($.a, $.b)`, input: map[string]any{"a": 2, "b": 40}, want: `42`},
		{name: "math add floats", expr: `States.MathAdd($.a, $.b)`, input: map[string]any{"a": 1.5, "b": 2}, want: `3.5`},
		{name: "math add nested", expr: `States.MathAdd($.a, States.MathAdd(1, 2))`, input: map[string]any{"a": 10}, want: `13`},
		{name: "math add non-number", expr: `States.MathAdd($.a, 'x')`, input: map[string]any{"a": 1}, wantErr: "States.MathAdd"},
		{name: "math random equal bounds", expr: `States.MathRandom(5, 5)`, wantErr: "States.MathRandom"},
		{name: "math random non-integer", expr: `States.MathRandom(1.5, 3)`, wantErr: "States.MathRandom"},
		{name: "format nested intrinsic", expr: `States.Format('count={}', States.JsonToString($.n))`, input: map[string]any{"n": 7}, want: `"count=7"`},
		{name: "array length of intrinsic array", expr: `States.ArrayLength(States.Array('a', 'b', 'c'))`, want: `3`},
		{name: "string split comma", expr: `States.StringSplit('a,b,c', ',')`, want: `["a","b","c"]`},
		{name: "string split multiple delimiters", expr: `States.StringSplit('a-b c,d', '- ,')`, want: `["a","b","c","d"]`},
		{name: "string split with limit", expr: `States.StringSplit('a,b,c,d,e', ',', 3)`, want: `["a","b","c,d,e"]`},
		{name: "string split empty string", expr: `States.StringSplit('', ',')`, want: `[""]`},
		{name: "string split limit zero", expr: `States.StringSplit('a,b', ',', 0)`, want: `[]`},
		{name: "string split negative limit", expr: `States.StringSplit('a,b', ',', -1)`, wantErr: "States.StringSplit"},
		{name: "string split empty delimiter", expr: `States.StringSplit('a,b', '')`, wantErr: "States.StringSplit"},
		{name: "not a function", expr: `States.NotARealFunction(1)`, wantErr: ErrNameParameterPathFormat},
		{name: "unterminated call", expr: `States.Array(1`, wantErr: ErrNameParameterPathFormat},
		{name: "trailing garbage", expr: `States.Format('a') extra`, wantErr: ErrNameParameterPathFormat},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := EvalIntrinsic(tc.expr, resolverFromMap(tc.input))
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error %q, got value %s", tc.wantErr, jsonText(got))
				}
				if gotName := errNameOf(t, err); gotName != tc.wantErr {
					t.Fatalf("error name = %q, want %q (err: %v)", gotName, tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotJSON := jsonText(got); gotJSON != tc.want {
				t.Fatalf("result = %s, want %s", gotJSON, tc.want)
			}
		})
	}
}

func isHexLower(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func TestIntrinsicUUID(t *testing.T) {
	for i := 0; i < 5; i++ {
		v, err := EvalIntrinsic("States.UUID()", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		s, ok := v.(string)
		if !ok {
			t.Fatalf("UUID result is not a string: %T", v)
		}
		parts := strings.Split(s, "-")
		wantLens := []int{8, 4, 4, 4, 12}
		if len(parts) != len(wantLens) {
			t.Fatalf("not a v4 UUID: %s", s)
		}
		for j, wl := range wantLens {
			if len(parts[j]) != wl || !isHexLower(parts[j]) {
				t.Fatalf("malformed UUID %q (segment %d)", s, j)
			}
		}
		if parts[2][0] != '4' {
			t.Fatalf("UUID %q is not version 4", s)
		}
		c := parts[3][0]
		if c != '8' && c != '9' && c != 'a' && c != 'b' {
			t.Fatalf("UUID %q variant nibble is invalid", s)
		}
	}
}

func TestIntrinsicMathRandom(t *testing.T) {
	for i := 0; i < 100; i++ {
		v, err := EvalIntrinsic("States.MathRandom(1, 4)", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		f, ok := asFloat(v)
		if !ok || f < 1 || f >= 4 || f != float64(int64(f)) {
			t.Fatalf("out-of-range random value: %v", v)
		}
	}
	seen := map[float64]bool{}
	for i := 0; i < 200; i++ {
		v, err := EvalIntrinsic("States.MathRandom(1, 4)", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		f, _ := asFloat(v)
		seen[f] = true
	}
	if len(seen) < 2 {
		t.Fatal("random values never varied")
	}
}

func TestIntrinsicRegistryComplete(t *testing.T) {
	required := []string{
		"Format", "StringToJson", "JsonToString", "Array", "ArrayGetItem",
		"ArrayLength", "ArrayUnique", "Base64Encode", "Base64Decode", "Hash",
		"JsonMerge", "MathAdd", "MathRandom", "StringSplit", "UUID",
	}
	present := make(map[string]bool)
	for _, n := range IntrinsicFunctionNames() {
		present[strings.TrimPrefix(n, "States.")] = true
	}
	for _, r := range required {
		if !present[r] {
			t.Errorf("missing intrinsic function: States.%s", r)
		}
	}
	if len(present) != len(required) {
		t.Errorf("expected exactly %d intrinsic functions, found %d", len(required), len(present))
	}
}
