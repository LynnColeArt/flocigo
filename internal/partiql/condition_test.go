package partiql

import (
	"strings"
	"testing"
)

func sv(s string) AV { return AV{TypeS: s} }

func nv(s string) AV { return AV{TypeN: s} }

func bv(s string) AV { return AV{TypeB: s} }

func lv(elems ...AV) AV {
	l := make([]any, len(elems))
	for i, e := range elems {
		l[i] = e
	}
	return AV{TypeL: l}
}

func mv(m map[string]AV) AV {
	out := make(map[string]any, len(m))
	for k, e := range m {
		out[k] = e
	}
	return AV{TypeM: out}
}

func ssv(elems ...string) AV {
	ss := make([]any, len(elems))
	for i, e := range elems {
		ss[i] = e
	}
	return AV{TypeSS: ss}
}

func nsv(elems ...string) AV {
	ns := make([]any, len(elems))
	for i, e := range elems {
		ns[i] = e
	}
	return AV{TypeNS: ns}
}

func itemEqual(a, b AV) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		va, ok1 := mapAny(a[k])
		vb, ok2 := mapAny(b[k])
		if !ok1 || !ok2 || !EqualDeep(va, vb) {
			return false
		}
	}
	return true
}

var condItem = AV{
	"a":  sv("hello"),
	"n":  nv("5.0"),
	"b":  bv("YWJj"), // "abc"
	"l":  lv(sv("x"), nv("1")),
	"m":  mv(map[string]AV{"x": sv("y")}),
	"ss": ssv("p", "q"),
}

func evalBool(t *testing.T, item AV, expr string, names map[string]string, values map[string]AV) bool {
	t.Helper()
	pr, err := Parse(KindCondition, expr, names)
	if err != nil {
		t.Fatalf("Parse(%q): %v", expr, err)
	}
	got, err := pr.EvalCondition(item, values)
	if err != nil {
		t.Fatalf("Eval %q: %v", expr, err)
	}
	return got
}

func evalErrMsg(t *testing.T, item AV, kind ExprKind, expr string, names map[string]string, values map[string]AV) string {
	t.Helper()
	pr, err := Parse(kind, expr, names)
	if err != nil {
		if ve, ok := err.(*ValidationError); ok {
			return ve.Msg
		}
		t.Fatalf("unexpected error type: %T", err)
	}
	got, err := pr.EvalCondition(item, values)
	if err == nil {
		t.Fatalf("expected error, got result %v", got)
	}
	if ve, ok := err.(*ValidationError); ok {
		return ve.Msg
	}
	t.Fatalf("unexpected error type: %T", err)
	return ""
}

func TestConditionComparators(t *testing.T) {
	v := map[string]AV{
		":s": sv("hello"), ":o": sv("world"),
		":n5": nv("5"), ":n4": nv("4"), ":n6": nv("6"),
	}
	cases := []struct {
		expr string
		want bool
	}{
		{"a = :s", true},
		{"a <> :s", false},
		{"a <> :o", true},
		{"n = :n5", true}, // "5.0" == "5" numerically
		{"n > :n4", true},
		{"n >= :n5", true},
		{"n < :n6", true},
		{"n <= :n4", false},
		{"zz = :s", false},  // missing path yields false, no error
		{"a = :n5", false},  // scalar type mismatch compares false
		{"m.x = :s", false},
		{":s = :o", false},
		{":s = :s", true},
	}
	for _, c := range cases {
		if got := evalBool(t, condItem, c.expr, nil, v); got != c.want {
			t.Fatalf("%s = %v, want %v", c.expr, got, c.want)
		}
	}
}

func TestConditionBooleanLogic(t *testing.T) {
	v := map[string]AV{":s": sv("hello"), ":n5": nv("5")}
	if !evalBool(t, condItem, "(a = :s OR NOT a = :s) AND n = :n5", nil, v) {
		t.Fatal("or/and/parens")
	}
	if evalBool(t, condItem, "NOT a = :s AND n = :n5", nil, v) {
		t.Fatal("NOT must bind tighter than AND")
	}
	if !evalBool(t, condItem, "NOT (a <> :s AND n = :n5)", nil, v) {
		t.Fatal("negated group")
	}
	if !evalBool(t, condItem, "a = :s OR zz <> :s AND n = :n6", nil, v) {
		t.Fatal("or/and precedence")
	}
}

func TestConditionFunctions(t *testing.T) {
	v := map[string]AV{
		":pre": sv("hel"), ":bpp": bv("YWI="), ":x": sv("x"), ":q": sv("q"),
		":z": sv("z"), ":ell": sv("ell"), ":one": nv("1"), ":two": nv("2"),
		":five": nv("5"), ":tN": sv("N"), ":tS": sv("S"), ":tSS": sv("SS"),
	}
	cases := []struct {
		expr string
		want bool
	}{
		{"begins_with(a, :pre)", true},
		{"begins_with(a, :x)", false},
		{"begins_with(b, :bpp)", true},
		{"contains(l, :x)", true},
		{"contains(l, :z)", false},
		{"contains(ss, :q)", true},
		{"contains(ss, :z)", false},
		{"contains(a, :ell)", true},
		{"size(a) = :five", true},
		{"size(l) = :two", true},
		{"size(ss) > :one", true},
		{"attribute_exists(m.x)", true},
		{"attribute_exists(m.zz)", false},
		{"attribute_not_exists(m.x)", false},
		{"attribute_not_exists(zz)", true},
		{"attribute_type(n, :tN)", true},
		{"attribute_type(a, :tN)", false},
		{"attribute_type(a, :tS)", true},
		{"attribute_type(ss, :tSS)", true},
	}
	for _, c := range cases {
		if got := evalBool(t, condItem, c.expr, nil, v); got != c.want {
			t.Fatalf("%s = %v, want %v", c.expr, got, c.want)
		}
	}
}

func TestConditionInAndBetween(t *testing.T) {
	v := map[string]AV{
		":one": nv("1"), ":five": nv("5"), ":six": nv("6"), ":nine": nv("9"), ":three": nv("3"),
	}
	if !evalBool(t, condItem, "n IN (:one, :five)", nil, v) {
		t.Fatal("IN hit")
	}
	if evalBool(t, condItem, "n IN (:one, :six)", nil, v) {
		t.Fatal("IN miss")
	}
	if !evalBool(t, condItem, "n BETWEEN :one AND :nine", nil, v) {
		t.Fatal("BETWEEN hit")
	}
	if evalBool(t, condItem, "n BETWEEN :six AND :nine", nil, v) {
		t.Fatal("BETWEEN miss")
	}
	if !evalBool(t, condItem, "size(l) BETWEEN :one AND :three", nil, v) {
		t.Fatal("size BETWEEN")
	}
}

func TestConditionErrors(t *testing.T) {
	v := map[string]AV{
		":v": sv("hello"), ":vm": mv(map[string]AV{}), ":nOne": nv("1"), ":x": nv("1"),
	}
	if msg := evalErrMsg(t, condItem, KindCondition, "m = :vm", nil, v); msg != `Invalid ConditionExpression: Incorrect operand type for operator or value; operator: =, operand type: M` {
		t.Fatalf("document comparison: %s", msg)
	}
	if msg := evalErrMsg(t, condItem, KindFilter, "a === :v", nil, v); msg != `Invalid FilterExpression: Syntax error; token: "=", near: "a =="` {
		t.Fatalf("syntax: %s", msg)
	}
	if msg := evalErrMsg(t, condItem, KindCondition, "a = :v AND", nil, v); msg != `Invalid ConditionExpression: Syntax error; token: "<EOF>", near: "a = :v AND"` {
		t.Fatalf("eof: %s", msg)
	}
	if msg := evalErrMsg(t, condItem, KindCondition, "a != :v", nil, v); msg != `Invalid ConditionExpression: Syntax error; token: "!", near: "a !"` {
		t.Fatalf("illegal char: %s", msg)
	}
	if msg := evalErrMsg(t, condItem, KindCondition, "size = :v", nil, v); msg != "Attribute name is a reserved keyword; reserved keyword: size" {
		t.Fatalf("reserved: %s", msg)
	}
	if msg := evalErrMsg(t, condItem, KindCondition, "#a = :v", nil, v); msg != "An expression attribute name used in the expression is not defined; attribute name: #a" {
		t.Fatalf("name: %s", msg)
	}
	if msg := evalErrMsg(t, condItem, KindCondition, "a = :missing", nil, v); msg != "An expression attribute value used in the expression is not defined; attribute value: :missing" {
		t.Fatalf("value: %s", msg)
	}
	if msg := evalErrMsg(t, condItem, KindCondition, "contains(ss, :nOne)", nil, v); msg != `Invalid ConditionExpression: Incorrect operand type for operator or value; operator: contains, operand type: N` {
		t.Fatalf("contains: %s", msg)
	}
	var sb strings.Builder
	sb.WriteString("n IN (")
	for i := 0; i < 101; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(":x")
	}
	sb.WriteString(")")
	if msg := evalErrMsg(t, condItem, KindCondition, sb.String(), nil, v); msg != "Invalid ConditionExpression: IN operand list contains more than 100 elements" {
		t.Fatalf("IN limit: %s", msg)
	}
}

func TestUnusedExpressionAttributes(t *testing.T) {
	pr, err := Parse(KindCondition, "#a = :used", map[string]string{"#a": "zz"})
	if err != nil {
		t.Fatal(err)
	}
	err = CheckUnusedValues(map[string]AV{":used": sv("x"), ":unused": sv("y")}, pr.UsedV)
	if err == nil || err.Error() != "ValidationException: Value provided in ExpressionAttributeValues unused in expressions: keys: {:unused}" {
		t.Fatalf("unused values: %v", err)
	}
	err = CheckUnusedNames(map[string]string{"#a": "zz", "#b": "yy"}, pr.UsedN)
	if err == nil || err.Error() != "ValidationException: Value provided in ExpressionAttributeNames unused in expressions: keys: {#b}" {
		t.Fatalf("unused names: %v", err)
	}
	err = CheckUnusedValues(map[string]AV{":used": sv("x")}, pr.UsedV)
	if err != nil {
		t.Fatalf("all values used: %v", err)
	}
}
