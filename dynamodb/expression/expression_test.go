package expression

import "testing"

func testItem() map[string]*Value {
	return map[string]*Value{
		"s":   NewString("hello"),
		"n":   NewNumber("6"),
		"ss":  NewStringSet([]string{"a", "b"}),
		"ns":  NewNumberSet([]string{"1", "2"}),
		"l":   NewList([]*Value{NewString("x"), NewString("y")}),
		"m":   NewMap(map[string]*Value{"k": NewBool(true), "deep": NewMap(map[string]*Value{"dv": NewString("d")})}),
		"b":   NewBool(true),
		"nul": NewNull(),
		"bin": NewBinary([]byte("hi")),
	}
}

func exprAttrs() *ExpressionAttributes {
	return &ExpressionAttributes{
		Names: map[string]string{"#m": "m", "#k": "k", "#l": "l"},
		Values: map[string]*Value{
			":v": NewString("hello"), ":other": NewString("no"),
			":n": NewNumber("5"), ":n2": NewNumber("7"), ":two": NewNumber("2"), ":one": NewNumber("1"),
			":t": NewString("S"), ":pre": NewString("hel"),
			":a": NewString("a"), ":x": NewString("x"),
			":set2": NewStringSet([]string{"b", "c"}),
			":ssDel": NewStringSet([]string{"b", "c"}),
			":nsDel": NewNumberSet([]string{"2"}),
			":ss1":  NewStringSet([]string{"b", "a"}),
			":ss3":  NewStringSet([]string{"a"}),
			":val":  NewString("val"),
			":def":  NewString("def"),
			":nullv": NewNull(),
		},
	}
}

func updateAttrs() *ExpressionAttributes {
	return &ExpressionAttributes{
		Names: map[string]string{"#m": "m", "#k": "k", "#l": "l"},
		Values: map[string]*Value{
			":val":   NewString("val"),
			":def":   NewString("def"),
			":one":   NewNumber("1"),
			":extra": NewList([]*Value{NewString("c"), NewString("d")}),
			":set2":  NewStringSet([]string{"b", "c"}),
			":ssDel": NewStringSet([]string{"b", "c"}),
			":nsDel": NewNumberSet([]string{"1"}),
		},
	}
}

func mustNoErr(t *testing.T, err *ExpressionError) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Message)
	}
}

func mustError(t *testing.T, err *ExpressionError, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error %s, got nil", want)
	}
	if want != `` && err.Message != want {
		t.Fatalf("expected error %s, got %s", want, err.Message)
	}
}

func assertItemEqual(t *testing.T, got, want map[string]*Value) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("item size: got %d want %d", len(got), len(want))
	}
	for k, wv := range want {
		gv, ok := got[k]
		if !ok {
			t.Fatalf("missing attribute %s", k)
		}
		if !gv.Equal(wv) {
			t.Fatalf("attribute %s mismatch", k)
		}
	}
}

func TestConditionEvaluation(t *testing.T) {
	cases := []struct {
		expr string
		want bool
	}{
		{"s = :v", true},
		{"s <> :other", true},
		{"s = :other", false},
		{"s = s", true},
		{"n < :n2", true},
		{"n <= :n", true},
		{"n > :n2", false},
		{"n <> :n2", true},
		{"n BETWEEN :n AND :n2", true},
		{"n BETWEEN :n2 AND :n", false},
		{"s IN (:v, :other)", true},
		{"s IN (:other)", false},
		{"(s = :v OR n = :n2) AND NOT n >= :n2", true},
		{"NOT (n >= :n2)", true},
		{"NOT n = :v", true},
		{"attribute_exists(m.deep.dv)", true},
		{"attribute_not_exists(m.deep.dv)", false},
		{"attribute_not_exists(m.gone)", true},
		{"begins_with(s, :pre)", true},
		{"begins_with(s, :other)", false},
		{"contains(ss, :a)", true},
		{"contains(ns, :two)", true},
		{"contains(l, :x)", true},
		{"contains(l, :v)", false},
		{"attribute_type(s, :t)", true},
		{"attribute_type(n, :t)", false},
		{"size(s) = :n", true},
		{"size(l) = :two", true},
		{"nope = :v", false},
		{"nope.sub = :v", false},
		{"n = :v", false},
		{"nul = :nullv", true},
		{"ss = :ss1", true},
		{"ss = :ss3", false},
	}
	for _, c := range cases {
		got, err := EvalConditionExpression(c.expr, exprAttrs(), testItem())
		mustNoErr(t, err)
		if got != c.want {
			t.Fatalf("%s: got %v want %v", c.expr, got, c.want)
		}
	}
}

func TestConditionErrors(t *testing.T) {
	cases := []struct {
		expr string
		want string
	}{
		{"a == :v", `Invalid ConditionExpression: Syntax error; token: "=", near: "= :v"`},
		{"s =", `Invalid ConditionExpression: Syntax error; token: "", near: ""`},
		{"", `Invalid ConditionExpression: Syntax error; token: "", near: ""`},
		{"pk = :missing", `An expression attribute value used in expression is not defined; attribute value: :missing`},
		{"#a = :v", `An expression attribute name used in the document path is not defined; attribute name: #a`},
		{"size = :v", `Invalid ConditionExpression: Attribute name is a reserved keyword; reserved keyword: size`},
		{"in = :v", `Invalid ConditionExpression: Syntax error; token: "in", near: "in = :v"`},
		{"a = :x b = :y", `Invalid ConditionExpression: Syntax error; token: "b", near: "b = :y"`},
		{"attribute_exists(:v)", `Invalid ConditionExpression: Incorrect operand type for operator or function; operator or function: attribute_exists, operand type: S`},
		{"a @ :v", `Invalid ConditionExpression: Syntax error; token: "@", near: "a @"`},
		{"size(b) = :n", `Invalid ConditionExpression: Incorrect operand type for operator or function; operator or function: size, operand type: BOOL`},
	}
	for _, c := range cases {
		_, err := EvalConditionExpression(c.expr, exprAttrs(), testItem())
		mustError(t, err, c.want)
	}
}

func TestFilterExpressionPrefix(t *testing.T) {
	_, err := EvalFilterExpression("a == :v", exprAttrs(), testItem())
	mustError(t, err, `Invalid FilterExpression: Syntax error; token: "=", near: "= :v"`)
}

func TestUpdateApply(t *testing.T) {
	cases := []struct {
		expr string
		item map[string]*Value
		want map[string]*Value
	}{
		{expr: "SET #m.#k = :val", item: map[string]*Value{}, want: map[string]*Value{"m": NewMap(map[string]*Value{"k": NewString("val")})}},
		{expr: "SET #l[0] = :val", item: map[string]*Value{}, want: map[string]*Value{"l": NewList([]*Value{NewString("val")})}},
		{expr: "SET n = n + :one", item: map[string]*Value{"n": NewNumber("5")}, want: map[string]*Value{"n": NewNumber("6")}},
		{expr: "SET n = n - :one", item: map[string]*Value{"n": NewNumber("5")}, want: map[string]*Value{"n": NewNumber("4")}},
		{expr: "SET l2 = list_append(l, :extra)", item: map[string]*Value{"l": NewList([]*Value{NewString("a"), NewString("b")})}, want: map[string]*Value{"l": NewList([]*Value{NewString("a"), NewString("b")}), "l2": NewList([]*Value{NewString("a"), NewString("b"), NewString("c"), NewString("d")})}},
		{expr: "SET v = if_not_exists(v, :def)", item: map[string]*Value{"v": NewString("old")}, want: map[string]*Value{"v": NewString("old")}},
		{expr: "SET v = if_not_exists(v, :def)", item: map[string]*Value{}, want: map[string]*Value{"v": NewString("def")}},
		{expr: "SET l[0].b = :val", item: map[string]*Value{"l": NewList([]*Value{NewMap(map[string]*Value{"a": NewNumber("1")})})}, want: map[string]*Value{"l": NewList([]*Value{NewMap(map[string]*Value{"a": NewNumber("1"), "b": NewString("val")})})}},
		{expr: "REMOVE m.k, l[0]", item: map[string]*Value{"m": NewMap(map[string]*Value{"k": NewString("x"), "keep": NewNumber("1")}), "l": NewList([]*Value{NewString("a"), NewString("b"), NewString("c")})}, want: map[string]*Value{"m": NewMap(map[string]*Value{"keep": NewNumber("1")}), "l": NewList([]*Value{NewString("b"), NewString("c")})}},
		{expr: "REMOVE l[0], l[1]", item: map[string]*Value{"l": NewList([]*Value{NewString("x"), NewString("y"), NewString("z")})}, want: map[string]*Value{"l": NewList([]*Value{NewString("z")})}},
		{expr: "REMOVE nope", item: map[string]*Value{}, want: map[string]*Value{}},
		{expr: "ADD n :one", item: map[string]*Value{"n": NewNumber("41")}, want: map[string]*Value{"n": NewNumber("42")}},
		{expr: "ADD ss :set2", item: map[string]*Value{"ss": NewStringSet([]string{"a", "b"})}, want: map[string]*Value{"ss": NewStringSet([]string{"a", "b", "c"})}},
		{expr: "DELETE ns2 :nsDel", item: map[string]*Value{"ns2": NewNumberSet([]string{"1"})}, want: map[string]*Value{}},
		{expr: "DELETE ss :ssDel", item: map[string]*Value{"ss": NewStringSet([]string{"a", "b", "c"})}, want: map[string]*Value{"ss": NewStringSet([]string{"a"})}},
		{expr: "DELETE ss :ssDel", item: map[string]*Value{}, want: map[string]*Value{}},
		{expr: "SET a = :val REMOVE m", item: map[string]*Value{"m": NewMap(map[string]*Value{})}, want: map[string]*Value{"a": NewString("val")}},
	}
	for i, c := range cases {
		got, err := ApplyUpdateExpression(c.item, c.expr, updateAttrs())
		mustNoErr(t, err)
		assertItemEqual(t, got, c.want)
		_ = i
	}
}

func TestUpdateErrors(t *testing.T) {
	cases := []struct {
		expr string
		item map[string]*Value
		want string
	}{
		{"SET m = :val, m.k = :val", map[string]*Value{}, `Invalid UpdateExpression: Two document paths overlap with each other; path1: m, path2: m.k`},
		{"SET a = :val SET b = :val", map[string]*Value{}, `Invalid UpdateExpression: Syntax error; token: "SET", near: "SET b = :val"`},
		{"SET a.b = :val", map[string]*Value{"a": NewString("str")}, `The document path provided in the expression is invalid for update`},
		{"SET l[1] = :val", map[string]*Value{"l": NewList([]*Value{NewString("x")})}, `The provided expression refers to an attribute that does not exist in the item`},
		{"SET b = nope", map[string]*Value{}, `The provided expression refers to an attribute that does not exist in the item`},
		{"SET b = a.x", map[string]*Value{"a": NewString("str")}, `The document path provided in the expression is invalid for update`},
		{"SET a = 5", map[string]*Value{}, `The provided expression refers to an attribute that does not exist in the item`},
		{"SET #n = :val", map[string]*Value{}, `An expression attribute name used in the document path is not defined; attribute name: #n`},
		{"SET a = :undef", map[string]*Value{}, `An expression attribute value used in expression is not defined; attribute value: :undef`},
		{"SET size = :val", map[string]*Value{}, `Invalid UpdateExpression: Attribute name is a reserved keyword; reserved keyword: size`},
		{"", map[string]*Value{}, `Invalid UpdateExpression: Syntax error; token: "", near: ""`},
		{"SET l2 = list_append(s, :extra)", map[string]*Value{"s": NewString("str")}, `An operand in the update expression has an incorrect type; operand: "s"`},
		{"ADD m :val", map[string]*Value{"m": NewMap(map[string]*Value{})}, `Incorrect operand type for operator or function; operator or function: ADD, operand type: S`},
		{"DELETE n :one", map[string]*Value{"n": NewNumber("5")}, `Incorrect operand type for operator or function; operator or function: DELETE, operand type: N`},
		{"SET a = size(b)", map[string]*Value{}, `Invalid UpdateExpression: Syntax error; token: "size", near: "size ( b )"`},
	}
	for _, c := range cases {
		_, err := ApplyUpdateExpression(c.item, c.expr, updateAttrs())
		mustError(t, err, c.want)
	}
}

func keyTestSetup() ([]KeySchemaElement, map[string]string, *ExpressionAttributes) {
	schema := []KeySchemaElement{
		{AttributeName: "pk", KeyType: "HASH"},
		{AttributeName: "sk", KeyType: "RANGE"},
	}
	types := map[string]string{"pk": "S", "sk": "S"}
	attrs := &ExpressionAttributes{
		Values: map[string]*Value{
			":v":   NewString("a"),
			":pre": NewString("he"),
			":n":   NewNumber("5"),
		},
	}
	return schema, types, attrs
}

func TestKeyConditionValidation(t *testing.T) {
	schema, types, attrs := keyTestSetup()

	pk, sk, err := ParseAndValidateKeyCondition("pk = :v", attrs, schema, types, "")
	mustNoErr(t, err)
	if pk == nil || pk.AttributeName != "pk" || pk.Comparison != "=" || sk != nil {
		t.Fatalf("unexpected pk part: %+v", pk)
	}

	pk, sk, err = ParseAndValidateKeyCondition("pk = :v AND sk > :v", attrs, schema, types, "")
	mustNoErr(t, err)
	if pk == nil || sk == nil || sk.Comparison != ">" || sk.AttributeName != "sk" {
		t.Fatalf("unexpected parts: %+v %+v", pk, sk)
	}

	pk, sk, err = ParseAndValidateKeyCondition("pk = :v AND sk BETWEEN :v AND :pre", attrs, schema, types, "")
	mustNoErr(t, err)
	if sk == nil || sk.Comparison != "BETWEEN" || len(sk.Values) != 2 {
		t.Fatalf("unexpected between part: %+v", sk)
	}

	_, _, err = ParseAndValidateKeyCondition("pk = :v AND begins_with(sk, :pre)", attrs, schema, types, "")
	mustNoErr(t, err)

	_, _, err = ParseAndValidateKeyCondition("sk = :v AND pk = :v", attrs, schema, types, "")
	mustError(t, err, "Query condition missed key schema element: pk")

	_, _, err = ParseAndValidateKeyCondition("pk < :v", attrs, schema, types, "")
	mustError(t, err, "Query key condition not supported")

	_, _, err = ParseAndValidateKeyCondition("pk = :v AND pk = :v", attrs, schema, types, "")
	mustError(t, err, "Query key condition not supported")

	_, _, err = ParseAndValidateKeyCondition("pk = :v AND foo = :v", attrs, schema, types, "")
	mustError(t, err, "Query condition missed key schema element: sk")

	_, _, err = ParseAndValidateKeyCondition("pk IN (:v)", attrs, schema, types, "")
	mustError(t, err, "Query key condition not supported")

	_, _, err = ParseAndValidateKeyCondition("pk === :v", attrs, schema, types, "")
	mustError(t, err, `Invalid KeyConditionExpression: Syntax error; token: "=", near: "= = :v"`)

	_, _, err = ParseAndValidateKeyCondition("pk = :n", attrs, schema, types, "")
	mustError(t, err, "One or more parameter values were invalid: Type mismatch for Index Key pk Expected: S Actual: N IndexName: undefined")

	_, _, err = ParseAndValidateKeyCondition("pk = :v AND begins_with(sk, :v)", attrs, schema, map[string]string{"pk": "S", "sk": "N"}, "")
	mustError(t, err, `Invalid KeyConditionExpression: Incorrect operand type for operator or function; operator or function: begins_with, operand type: N`)
}

func TestLegacyKeyConditions(t *testing.T) {
	schema, types, _ := keyTestSetup()
	conds := []KeyCondition{
		{AttributeName: "pk", Operator: "EQ", Values: []*Value{NewString("a")}},
		{AttributeName: "sk", Operator: "BETWEEN", Values: []*Value{NewString("a"), NewString("z")}},
	}
	mustNoErr(t, ValidateKeyConditions(conds, schema, types, ""))

	_, err := ValidateKeyConditions(nil, schema, types, "")
	mustError(t, err, "Either the KeyConditions or KeyConditionExpression parameter must be specified in the request.")

	dup := []KeyCondition{
		{AttributeName: "pk", Operator: "EQ", Values: []*Value{NewString("a")}},
		{AttributeName: "pk", Operator: "EQ", Values: []*Value{NewString("b")}},
	}
	mustError(t, ValidateKeyConditions(dup, schema, types, ""), "One or more parameter values were invalid: Only one condition per key is supported")

	wrongOp := []KeyCondition{{AttributeName: "pk", Operator: "LT", Values: []*Value{NewString("a")}}}
	mustError(t, ValidateKeyConditions(wrongOp, schema, types, ""), "Query key condition not supported")

	missing := []KeyCondition{{AttributeName: "sk", Operator: "EQ", Values: []*Value{NewString("a")}}}
	mustError(t, ValidateKeyConditions(missing, schema, types, ""), "Query condition missed key schema element: pk")
}

func TestValueBasics(t *testing.T) {
	if !NewNumberSet([]string{"1.0", "1.1"}).Equal(NewNumberSet([]string{"1", "1.1"})) {
		t.Fatal("number set equality must be numeric and order-insensitive")
	}
	if NewNumberSet([]string{"1.0", "1.1"}).Equal(NewNumberSet([]string{"1", "1.2"})) {
		t.Fatal("number sets should not compare equal")
	}
	r, ok := parseDecimal("1e2")
	if !ok || numberString(r) != "100" {
		t.Fatal("exponent notation must parse")
	}
	r2, _ := parseDecimal("2.5")
	if numberString(r2) != "2.5" {
		t.Fatal("decimal rendering mismatch")
	}
	s := "1e-50"
	if _, ok3 := parseDecimal(s); !ok3 {
		t.Fatal("small exponent accepted")
	}
	if _, ok4 := parseDecimal("1e99999999"); ok4 {
		t.Fatal("huge exponent must be rejected")
	}

	src := map[string]interface{}{
		"M": map[string]interface{}{
			"n": map[string]interface{}{"N": "1.5"},
			"l": map[string]interface{}{"L": []interface{}{map[string]interface{}{"S": "a"}}},
		},
	}
	v, verr := ValueFromAttributeValue(src)
	if verr != nil {
		t.Fatalf("conversion failed: %v", verr)
	}
	want := NewMap(map[string]*Value{"n": NewNumber("1.5"), "l": NewList([]*Value{NewString("a")})})
	if !v.Equal(want) {
		t.Fatal("round trip mismatch")
	}
	back, berr := ValueFromAttributeValue(v.AttributeValue())
	if berr != nil || !back.Equal(want) {
		t.Fatal("AttributeValue round trip mismatch")
	}
	if _, nerr := ValueFromAttributeValue(map[string]interface{}{"N": "abc"}); nerr == nil {
		t.Fatal("invalid number must fail")
	}
}
