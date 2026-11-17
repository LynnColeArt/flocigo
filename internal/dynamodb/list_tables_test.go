package dynamodb

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func pageIntPtr(v int) *int { return &v }

func pageStrPtr(v string) *string { return &v }

func runListTablesPage(t *testing.T, all []string, in ListTablesInput) ListTablesOutput {
	t.Helper()
	out, err := listTableNames(append([]string(nil), all...), in)
	if err != nil {
		t.Fatalf("ListTables failed: %v", err)
	}
	return out
}

// TestListTablesPaginationAcrossPages checks alphabetical ordering plus
// cursor pagination driven by Limit and ExclusiveStartTableName.
func TestListTablesPaginationAcrossPages(t *testing.T) {
	all := []string{"Echo", "Alpha", "Delta", "Bravo", "Charlie"} // deliberately unsorted

	single := runListTablesPage(t, all, ListTablesInput{})
	if want := []string{"Alpha", "Bravo", "Charlie", "Delta", "Echo"}; !reflect.DeepEqual(single.TableNames, want) {
		t.Fatalf("single page = %v, want %v", single.TableNames, want)
	}
	if single.LastEvaluatedTableName != nil {
		t.Fatalf("exhausted listing must not carry a cursor: %+v", single)
	}

	first := runListTablesPage(t, all, ListTablesInput{Limit: pageIntPtr(2)})
	if want := []string{"Alpha", "Bravo"}; !reflect.DeepEqual(first.TableNames, want) {
		t.Fatalf("first page = %v, want %v", first.TableNames, want)
	}
	if first.LastEvaluatedTableName == nil || *first.LastEvaluatedTableName != "Bravo" {
		t.Fatalf("first page must resume at Bravo: %+v", first)
	}

	second := runListTablesPage(t, all, ListTablesInput{
		Limit:                   pageIntPtr(2),
		ExclusiveStartTableName: first.LastEvaluatedTableName,
	})
	if want := []string{"Charlie", "Delta"}; !reflect.DeepEqual(second.TableNames, want) {
		t.Fatalf("second page = %v, want %v", second.TableNames, want)
	}
	if second.LastEvaluatedTableName == nil || *second.LastEvaluatedTableName != "Delta" {
		t.Fatalf("second page must resume at Delta: %+v", second)
	}

	third := runListTablesPage(t, all, ListTablesInput{Limit: pageIntPtr(2), ExclusiveStartTableName: second.LastEvaluatedTableName})
	if want := []string{"Echo"}; !reflect.DeepEqual(third.TableNames, want) {
		t.Fatalf("third page = %v, want %v", third.TableNames, want)
	}
	if third.LastEvaluatedTableName != nil {
		t.Fatalf("final page must not carry a cursor: %+v", third)
	}
}

// TestListTablesCursorAndLimitGuards covers the exact-page boundary, an
// exhausted cursor behind the last table and the invalid request cases.
func TestListTablesCursorAndLimitGuards(t *testing.T) {
	all := []string{"Bravo", "Alpha"}

	exact := runListTablesPage(t, all, ListTablesInput{Limit: pageIntPtr(2)})
	if exact.LastEvaluatedTableName != nil {
		t.Fatalf("a full page with nothing behind it must not carry a cursor: %+v", exact)
	}

	exhausted := runListTablesPage(t, all, ListTablesInput{ExclusiveStartTableName: pageStrPtr("Bravo")})
	raw, _ := json.Marshal(exhausted)
	if string(raw) != "{}" {
		t.Fatalf("exhausted cursor must marshal to {}, got %s", raw)
	}

	if _, err := listTableNames(all, ListTablesInput{Limit: pageIntPtr(0)}); err == nil {
		t.Fatal("Limit 0 must fail")
	} else if e, ok := err.(*opError); !ok || e.Type != typeValidationException {
		t.Fatalf("Limit 0 must raise ValidationException, got %v", err)
	}

	_, err := listTableNames(all, ListTablesInput{ExclusiveStartTableName: pageStrPtr("ab")})
	if err == nil {
		t.Fatal("a 2 character cursor name must fail")
	}
	if !strings.Contains(err.Error(), "greater than or equal to 3") {
		t.Fatalf("cursor shape error must quote the length constraint: %v", err)
	}
}

// TestListTablesValidationMessages pins the exact wire messages.
func TestListTablesValidationMessages(t *testing.T) {
	_, err := listTableNames(nil, ListTablesInput{Limit: pageIntPtr(0)})
	e, ok := err.(*opError)
	if !ok {
		t.Fatalf("want *opError, got %v", err)
	}
	want := "1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"
	if e.Message != want {
		t.Fatalf("message = %q, want %q", e.Message, want)
	}
	body := opErrorBody(e)
	if !strings.Contains(string(body), "com.amazon.coral.validate#ValidationException") {
		t.Fatalf("error body must carry the wire type: %s", body)
	}
}