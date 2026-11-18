package partiql

import (
	"strings"
	"testing"
)

func runUpdate(t *testing.T, item AV, src string, names map[string]string, values map[string]AV) AV {
	t.Helper()
	pr, err := Parse(KindUpdate, src, names)
	if err != nil {
		if ve, ok := err.(*ValidationError); ok {
			t.Fatalf("parse %q: %s", src, ve.Msg)
		}
		t.Fatalf("parse %q: %v", src, err)
	}
	target := DeepCopyAV(item)
	if err := pr.Apply(target, values); err != nil {
		if ve, ok := err.(*ValidationError); ok {
			t.Fatalf("apply %q: %s", src, ve.Msg)
		}
		t.Fatalf("apply %q: %v", src, err)
	}
	return target
}

func updateErrMsg(t *testing.T, item AV, src string, names map[string]string, values map[string]AV) string {
	t.Helper()
	pr, err := Parse(KindUpdate, src, names)
	if err != nil {
		if ve, ok := err.(*ValidationError); ok {
			return ve.Msg
		}
		t.Fatalf("unexpected error type: %T", err)
	}
	if err := pr.Apply(DeepCopyAV(item), values); err != nil {
		if ve, ok := err.(*ValidationError); ok {
			return ve.Msg
		}
		t.Fatalf("unexpected error type: %T", err)
	}
	t.Fatalf("expected error applying %q", src)
	return ""
}

var updItem = AV{
	"a":  sv("old"),
	"m":  mv(map[string]AV{"k": sv("v")}),
	"l":  lv(mv(map[string]AV{"k": sv("0")}), sv("x")),
	"n":  nv("5"),
}

func TestUpdateSets(t *testing.T) {
	v := map[string]AV{
		":v": sv("new"), ":one": nv("1"), ":ten": nv("10"), ":add": lv(sv("y")),
	}
	got := runUpdate(t, updItem, "SET a = :v, m.k2 = :v, l[1] = :one, n = n + :one", nil, v)
	want := AV{
		"a": sv("new"),
		"m": mv(map[string]AV{"k": sv("v"), "k2": sv("new")}),
		"l": lv(mv(map[string]AV{"k": sv("0")}), nv("1")),
		"n": nv("6"),
	}
	if !itemEqual(got, want) {
		t.Fatalf("SET result: %#v", got)
	}
	got = runUpdate(t, updItem, "SET n = :ten - n", nil, v)
	if !nvEqual(t, got["n"], "5") {
		t.Fatalf("arith: %#v", got["n"])
	}
	got = runUpdate(t, updItem, "SET l = list_append(l, :add)", nil, v)
	le, _ := listElems(got["l"][TypeL])
	if len(le) != 3 {
		t.Fatalf("list_append: %#v", got["l"])
	}
	got = runUpdate(t, updItem, "SET nn = if_not_exists(nn, :one)", nil, v)
	if !nvEqual(t, got["nn"], "1") {
		t.Fatalf("if_not_exists default: %#v", got["nn"])
	}
	got = runUpdate(t, updItem, "SET n = if_not_exists(nn, :ten)", nil, v)
	if !nvEqual(t, got["n"], "10") {
		t.Fatalf("if_not_exists existing default: %#v", got["n"])
	}
	got = runUpdate(t, updItem, "SET n = if_not_exists(n, :ten)", nil, v)
	if !nvEqual(t, got["n"], "5") {
		t.Fatalf("if_not_exists existing attr: %#v", got["n"])
	}
}

func nvEqual(t *testing.T, got AV, want string) bool {
	t.Helper()
	eq, _ := EqualDeep(got, nv(want))
	return eq
}

func TestUpdateRemove(t *testing.T) {
	got := runUpdate(t, updItem, "REMOVE a, m.k, l[0], zz", nil, nil)
	want := AV{
		"m": mv(map[string]AV{}),
		"l": lv(sv("x")),
		"n": nv("5"),
	}
	if !itemEqual(got, want) {
		t.Fatalf("REMOVE result: %#v", got)
	}
}

func TestUpdateAddDelete(t *testing.T) {
	item := AV{"n": nv("1"), "ss": ssv("a", "b"), "ns": nsv("1.5")}
	v := map[string]AV{
		":one": nv("1"), ":adds": ssv("b", "c"), ":nsAdd": nsv("1"), ":del": ssv("a"),
	}
	got := runUpdate(t, item, "ADD n :one, ss :adds, ns :nsAdd", nil, v)
	want := AV{"n": nv("2"), "ss": ssv("a", "b", "c"), "ns": nsv("1.5", "1")}
	if !itemEqual(got, want) {
		t.Fatalf("ADD result: %#v", got)
	}
	got = runUpdate(t, item, "DELETE ss :del", nil, v)
	want = AV{"n": nv("1"), "ss": ssv("b"), "ns": nsv("1.5")}
	if !itemEqual(got, want) {
		t.Fatalf("DELETE result: %#v", got)
	}
	got = runUpdate(t, item, "DELETE zz :del, ss :del", nil, v) // zz missing: no-op for zz
	want = AV{"n": nv("1"), "ss": ssv("b"), "ns": nsv("1.5")}
	if !itemEqual(got, want) {
		t.Fatalf("DELETE missing attr: %#v", got)
	}
}

func TestUpdateErrors(t *testing.T) {
	v := map[string]AV{
		":v": sv("new"), ":one": nv("1"), ":ssEmpty": ssv(), ":lAdd": lv(sv("y")),
	}
	pathMsg := "The document path provided in the expression is invalid for update"
	if msg := updateErrMsg(t, updItem, "SET zz.x = :v", nil, v); msg != pathMsg {
		t.Fatalf("missing parent: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "SET a.x = :v", nil, v); msg != pathMsg {
		t.Fatalf("nav into scalar: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "SET l[5] = :v", nil, v); msg != pathMsg {
		t.Fatalf("index oob: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "REMOVE zz.x", nil, v); msg != pathMsg {
		t.Fatalf("remove missing parent: %s", msg)
	}
	missingMsg := "The provided expression refers to an attribute that does not exist in the item"
	if msg := updateErrMsg(t, updItem, "SET c = zz", nil, v); msg != missingMsg {
		t.Fatalf("missing rhs: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "SET c = list_append(zz, :lAdd)", nil, v); msg != missingMsg {
		t.Fatalf("missing list_append arg: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "SET a = list_append(a, :lAdd)", nil, v); msg != `Invalid UpdateExpression: Incorrect operand type for operator or value; operator: list_append, operand type: S` {
		t.Fatalf("list_append type: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "SET s = s + :one", nil, v); msg != `Invalid UpdateExpression: Incorrect operand type for operator or value; operator: +, operand type: S` {
		t.Fatalf("arith type: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "ADD a :one", nil, v); msg != `Invalid UpdateExpression: Incorrect operand type for operator or value; operator: ADD, operand type: S` {
		t.Fatalf("ADD type: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "DELETE n :one", nil, v); msg != `Invalid UpdateExpression: Incorrect operand type for operator or value; operator: DELETE, operand type: N` {
		t.Fatalf("DELETE type: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "ADD ss :ssEmpty", nil, v); msg != "One or more parameter values were invalid: An string set may not be empty" {
		t.Fatalf("empty set: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "ADD zz :v", nil, map[string]AV{}); msg != "An expression attribute value used in the expression is not defined; attribute value: :v" {
		t.Fatalf("undefined value: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "SET year = :v", nil, v); msg != "Attribute name is a reserved keyword; reserved keyword: year" {
		t.Fatalf("reserved: %s", msg)
	}
}

func TestUpdateGrammarErrors(t *testing.T) {
	v := map[string]AV{":v": sv("x"), ":w": sv("y")}
	names := map[string]string{"#a": "a", "#n": "n"}
	if msg := updateErrMsg(t, updItem, "", nil, v); msg != `Invalid UpdateExpression: Syntax error; token: "<EOF>", near: ""` {
		t.Fatalf("empty: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "SET", nil, v); !strings.Contains(msg, `token: "<EOF>"`) {
		t.Fatalf("bare SET: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "SET #a = :v SET #a = :w", names, v); !strings.Contains(msg, `token: "SET"`) {
		t.Fatalf("repeat SET: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "ADD #n :v SET #a = :w", names, v); !strings.Contains(msg, `token: "SET"`) {
		t.Fatalf("order: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "SET #a = :v #a = :w", names, v); !strings.Contains(msg, `token: "#a"`) {
		t.Fatalf("missing comma: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "ADD a b", nil, v); !strings.Contains(msg, `token: "b"`) {
		t.Fatalf("add non-value: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "SET a = (:v)", nil, v); !strings.Contains(msg, `token: "("`) {
		t.Fatalf("parens: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "FOO a", nil, v); !strings.Contains(msg, `token: "FOO"`) {
		t.Fatalf("unknown action: %s", msg)
	}
}

func TestUpdateOverlaps(t *testing.T) {
	v := map[string]AV{":x": sv("1"), ":y": sv("2"), ":n": nv("1")}
	names := map[string]string{"#a": "a", "#n": "ss"}
	if msg := updateErrMsg(t, updItem, "SET #a = :x, #a = :y", names, v); msg != "Invalid UpdateExpression: Two document paths overlap with each other" {
		t.Fatalf("same leaf set: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "SET a.b = :x, a = :y", nil, v); msg != "Invalid UpdateExpression: Two document paths overlap with each other" {
		t.Fatalf("descendant: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "SET a = :x, a.b = :y", nil, v); msg != "Invalid UpdateExpression: Two document paths overlap with each other" {
		t.Fatalf("ancestor: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "SET a = :x REMOVE a", nil, v); msg != "Invalid UpdateExpression: Two document paths overlap with each other" {
		t.Fatalf("set+remove: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "ADD #n :n, #n :n", names, v); msg != "Invalid UpdateExpression: Attempting to ADD or DELETE the same item twice" {
		t.Fatalf("add dup: %s", msg)
	}
	if msg := updateErrMsg(t, updItem, "SET a = :x, m.k = :y", nil, v); msg != "" {
		_ = msg
	}
	got := runUpdate(t, updItem, "SET a = :x, m.k = :y", nil, v)
	if !svEqual(got["a"], "1") {
		t.Fatalf("no-overlap update failed: %#v", got)
	}
}

func svEqual(got AV, want string) bool {
	eq, _ := EqualDeep(got, sv(want))
	return eq
}

func TestUpdateUnusedValues(t *testing.T) {
	pr, err := Parse(KindUpdate, "SET a = :v", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = CheckUnusedValues(map[string]AV{":v": sv("x"), ":u": sv("y")}, pr.UsedV)
	if err == nil || err.Error() != "ValidationException: Value provided in ExpressionAttributeValues unused in expressions: keys: {:u}" {
		t.Fatalf("unused: %v", err)
	}
}
