package partiql

import "testing"

func TestKeyConditionParse(t *testing.T) {
	names := map[string]string{"#pk": "pk", "#sk": "sk"}
	pr, err := Parse(KindKeyCondition, "#pk = :v", names)
	if err != nil {
		t.Fatal(err)
	}
	kc := pr.Key
	if kc.Partition.Op != "EQ" || kc.Partition.Attr != "pk" || kc.Sort != nil {
		t.Fatalf("partition: %#v", kc.Partition)
	}
	if len(kc.Refs) != 1 || kc.Refs[0] != ":v" {
		t.Fatalf("refs: %#v", kc.Refs)
	}
	pr, err = Parse(KindKeyCondition, "#pk = :v AND #sk > :v2", names)
	if err != nil {
		t.Fatal(err)
	}
	if pr.Key.Sort == nil || pr.Key.Sort.Op != "GT" || pr.Key.Sort.Attr != "sk" {
		t.Fatalf("sort clause: %#v", pr.Key.Sort)
	}
	if len(pr.Key.Attrs) != 2 || pr.Key.Attrs[0] != "pk" || pr.Key.Attrs[1] != "sk" {
		t.Fatalf("attrs: %#v", pr.Key.Attrs)
	}
	pr, err = Parse(KindKeyCondition, "#pk = :v AND begins_with(#sk, :v2)", names)
	if err != nil {
		t.Fatal(err)
	}
	if pr.Key.Sort == nil || pr.Key.Sort.Op != "BEGINS_WITH" {
		t.Fatalf("begins_with clause: %#v", pr.Key.Sort)
	}
	pr, err = Parse(KindKeyCondition, "#pk = :v AND #sk BETWEEN :lo AND :hi", names)
	if err != nil {
		t.Fatal(err)
	}
	if len(pr.Key.Sort.Refs) != 2 || pr.Key.Sort.Refs[0] != ":lo" {
		t.Fatalf("between refs: %#v", pr.Key.Sort.Refs)
	}
}

func TestKeyConditionUnsupported(t *testing.T) {
	names := map[string]string{"#pk": "pk", "#sk": "sk", "#p": "p"}
	cases := []string{
		"#pk = :v OR #sk = :v2",
		"NOT #pk = :v",
		"#pk < :v", // partition key must be EQ
		"#sk BETWEEN :lo AND :hi",
		"begins_with(#sk, :v)",
		"attribute_exists(#pk)",
		"#pk.nested = :v",
		"#pk = :v AND #pk = :v2",
		"#pk = :v AND #sk = :v2 AND #pk2 = :v3",
		"#pk = #other",
	}
	for _, expr := range cases {
		_, err := Parse(KindKeyCondition, expr, names)
		if err == nil {
			t.Fatalf("%q should be unsupported", expr)
		}
		ve, ok := err.(*ValidationError)
		if !ok || ve.Msg != "Query key condition not supported" {
			t.Fatalf("%q: %v", expr, err)
		}
	}
}

func TestKeyConditionValidateAndValues(t *testing.T) {
	names := map[string]string{"#pk": "pk", "#sk": "sk"}
	pr, err := Parse(KindKeyCondition, "#pk = :v AND #sk > :v2", names)
	if err != nil {
		t.Fatal(err)
	}
	if err := pr.Key.Validate("pk", "sk"); err != nil {
		t.Fatalf("valid schema: %v", err)
	}
	err = pr.Key.Validate("pk", "other")
	if err == nil || err.Error() != "ValidationException: Query condition missed key schema element: other" {
		t.Fatalf("missed sort key: %v", err)
	}
	err = pr.Key.Validate("wrong", "sk")
	if err == nil || err.Error() != "ValidationException: Query condition missed key schema element: wrong" {
		t.Fatalf("missed partition key: %v", err)
	}
	err = pr.Key.CheckValues(map[string]AV{":v": sv("x")})
	if err == nil || err.Error() != "An expression attribute value used in the expression is not defined; attribute value: :v2" {
		t.Fatalf("missing value: %v", err)
	}
	if err := pr.Key.CheckValues(map[string]AV{":v": sv("x"), ":v2": sv("y")}); err != nil {
		t.Fatalf("values present: %v", err)
	}
}
