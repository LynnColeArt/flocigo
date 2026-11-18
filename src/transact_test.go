package ddb

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func newTestDB(t *testing.T) (*DB, *Table) {
	t.Helper()
	db := NewDB()
	tb, aerr := db.CreateTable("Test", "pk", "sk")
	if aerr != nil {
		t.Fatal(aerr)
	}
	return db, tb
}

func mustPut(t *testing.T, tb *Table, item map[string]*AttributeValue) {
	t.Helper()
	if err := tb.Put(item); err != nil {
		t.Fatal(err)
	}
}

func keyFor(pk, sk string) map[string]*AttributeValue {
	return map[string]*AttributeValue{"pk": aS(pk), "sk": aS(sk)}
}

func callTransact(t *testing.T, db *DB, body string) (int, map[string]any) {
	t.Helper()
	status, resp := HandleTransactWriteItems(db, []byte(body))
	m := map[string]any{}
	if len(resp) > 0 {
		if err := json.Unmarshal(resp, &m); err != nil {
			t.Fatalf("invalid response JSON %q: %v", resp, err)
		}
	}
	return status, m
}

func TestTransactWriteItemsSuccess(t *testing.T) {
	db, tb := newTestDB(t)
	mustPut(t, tb, map[string]*AttributeValue{
		"pk": aS("u1"), "sk": aS("profile"), "name": aS("old"), "count": aN("1"), "stale": aS("yes"),
	})
	mustPut(t, tb, map[string]*AttributeValue{
		"pk": aS("z"), "sk": aS("k"), "v": aN("1"),
	})

	body := `{
	 "TransactItems": [
	  {"Put": {"TableName": "Test", "Item": {"pk": {"S": "u2"}, "sk": {"S": "profile"}, "v": {"S": "created"}}}},
	  {"Delete": {"TableName": "Test", "Key": {"pk": {"S": "u1"}, "sk": {"S": "gone"}}}},
	  {"Update": {"TableName": "Test", "Key": {"pk": {"S": "u1"}, "sk": {"S": "profile"}},
	   "UpdateExpression": "SET #n = :nn ADD count :one REMOVE stale",
	   "ExpressionAttributeNames": {"#n": "name"},
	   "ExpressionAttributeValues": {":nn": {"S": "new"}, ":one": {"N": "1"}}}},
	  {"ConditionCheck": {"TableName": "Test", "Key": {"pk": {"S": "z"}, "sk": {"S": "k"}},
	   "ConditionExpression": "attribute_exists(pk)"}}
	 ]}`
	status, m := callTransact(t, db, body)
	if status != 200 || len(m) != 0 {
		t.Fatalf("expected 200 {}, got %d %v", status, m)
	}
	if it, ok := tb.Get(keyFor("u2", "profile")); !ok || it["v"] == nil || it["v"].S != "created" {
		t.Fatalf("put not applied: %v", it)
	}
	it, ok := tb.Get(keyFor("u1", "profile"))
	if !ok {
		t.Fatalf("update dropped the item")
	}
	if it["name"] == nil || it["name"].S != "new" || it["count"] == nil || it["count"].N != "2" {
		t.Fatalf("update applied wrong values: %v %v", it["name"], it["count"])
	}
	if _, exists := it["stale"]; exists {
		t.Fatalf("stale attribute not removed")
	}
	if _, ok := tb.Get(keyFor("u1", "gone")); ok {
		t.Fatalf("delete of non-existent key should be a no-op success")
	}
}

func TestTransactAtomicityOnConditionFailure(t *testing.T) {
	db, tb := newTestDB(t)
	mustPut(t, tb, map[string]*AttributeValue{
		"pk": aS("z"), "sk": aS("k"), "v": aN("5"),
	})
	body := `{
	 "TransactItems": [
	  {"Put": {"TableName": "Test", "Item": {"pk": {"S": "u9"}, "sk": {"S": "profile"}, "v": {"S": "x"}}}},
	  {"ConditionCheck": {"TableName": "Test", "Key": {"pk": {"S": "z"}, "sk": {"S": "k"}},
	   "ConditionExpression": "v < :lim", "ExpressionAttributeValues": {":lim": {"N": "1"}}}}
	 ]}`
	status, m := callTransact(t, db, body)
	if status != 400 {
		t.Fatalf("expected 400, got %d %v", status, m)
	}
	if code, _ := m["__type"].(string); !strings.HasSuffix(code, "#TransactionCanceledException") {
		t.Fatalf("unexpected error code %q", code)
	}
	reasons, ok := m["CancellationReasons"].([]any)
	if !ok || len(reasons) != 2 {
		t.Fatalf("expected two cancellation reasons, got %v", m["CancellationReasons"])
	}
	if reasons[0] != nil {
		t.Fatalf("successful action position must be null, got %v", reasons[0])
	}
	failed, ok := reasons[1].(map[string]any)
	if !ok || failed["Code"] != "ConditionalCheckFailed" || failed["Message"] != "The conditional request failed" {
		t.Fatalf("unexpected failure reason: %v", reasons[1])
	}
	// Atomicity: the successful Put must NOT have been applied.
	if _, exists := tb.Get(keyFor("u9", "profile")); exists {
		t.Fatalf("atomicity violated: put was applied despite the cancelled transaction")
	}
	if it, _ := tb.Get(keyFor("z", "k")); it["v"] == nil || it["v"].N != "5" {
		t.Fatalf("existing item was modified: %v", it)
	}
}

func TestTransactValidationLimits(t *testing.T) {
	db, _ := newTestDB(t)

	status, m := callTransact(t, db, `{"TransactItems": []}`)
	if status != 400 {
		t.Fatalf("expected 400, got %d", status)
	}
	if code, _ := m["__type"].(string); !strings.HasSuffix(code, "#ValidationException") {
		t.Fatalf("unexpected code for empty request: %q", code)
	}
	if msg, _ := m["Message"].(string); !strings.Contains(msg, "can not be empty") {
		t.Fatalf("unexpected message: %v", m["Message"])
	}

	var items []string
	for i := 0; i < 26; i++ {
		items = append(items, fmt.Sprintf(`{"Put": {"TableName": "Test", "Item": {"pk": {"S": "k%d"}, "sk": {"S": "s"}}}}`, i))
	}
	body := fmt.Sprintf(`{"TransactItems": [%s]}`, strings.Join(items, ","))
	status, m = callTransact(t, db, body)
	if status != 400 {
		t.Fatalf("expected 400 for 26 actions, got %d", status)
	}
	if msg, _ := m["Message"].(string); !strings.Contains(msg, "more than 25") {
		t.Fatalf("unexpected message: %v", m["Message"])
	}

	// Exactly 25 actions with unique keys succeeds.
	body = fmt.Sprintf(`{"TransactItems": [%s]}`, strings.Join(items[:25], ","))
	status, m = callTransact(t, db, body)
	if status != 200 || len(m) != 0 {
		t.Fatalf("expected 200 {} for 25 actions, got %d %v", status, m)
	}
}

func TestTransactDuplicateKeyRejected(t *testing.T) {
	db, _ := newTestDB(t)
	body := `{
	 "TransactItems": [
	  {"Put": {"TableName": "Test", "Item": {"pk": {"S": "dup"}, "sk": {"S": "s"}, "v": {"S": "1"}}}},
	  {"Delete": {"TableName": "Test", "Key": {"pk": {"S": "dup"}, "sk": {"S": "s"}}}}
	 ]}`
	status, m := callTransact(t, db, body)
	if status != 400 {
		t.Fatalf("expected 400, got %d", status)
	}
	msg, _ := m["Message"].(string)
	if !strings.Contains(msg, "multiple operations on one item") || !strings.Contains(msg, "[0, 1]") {
		t.Fatalf("unexpected message: %q", msg)
	}
}

func TestTransactConflictWithInFlightTransaction(t *testing.T) {
	db, tb := newTestDB(t)
	mustPut(t, tb, map[string]*AttributeValue{
		"pk": aS("u1"), "sk": aS("s"), "v": aS("orig"),
	})
	body := `{
	 "TransactItems": [
	  {"Put": {"TableName": "Test", "Item": {"pk": {"S": "u1"}, "sk": {"S": "s"}, "v": {"S": "conflict"}}}}
	 ]}`

	lock, aerr := db.AcquireItemLocks([]string{tb.LockKeyFor(keyFor("u1", "s"))})
	if aerr != nil {
		t.Fatal(aerr)
	}
	defer lock.Release()

	status, m := callTransact(t, db, body)
	if status != 400 {
		t.Fatalf("expected 400, got %d %v", status, m)
	}
	if code, _ := m["__type"].(string); !strings.HasSuffix(code, "#TransactionConflictException") {
		t.Fatalf("expected conflict, got %q", code)
	}

	// Once the in-flight transaction releases the items, the request succeeds.
	lock.Release()
	status, m = callTransact(t, db, body)
	if status != 200 || len(m) != 0 {
		t.Fatalf("expected 200 {} after lock release, got %d %v", status, m)
	}
	if it, _ := tb.Get(keyFor("u1", "s")); it["v"] == nil || it["v"].S != "conflict" {
		t.Fatalf("expected effect after release: %v", it)
	}
}

func TestTransactUpdateKeyProtection(t *testing.T) {
	db, _ := newTestDB(t)
	body := `{
	 "TransactItems": [
	  {"Update": {"TableName": "Test", "Key": {"pk": {"S": "a"}, "sk": {"S": "s"}},
	   "UpdateExpression": "SET pk = :v", "ExpressionAttributeValues": {":v": {"S": "x"}}}}
	 ]}`
	status, m := callTransact(t, db, body)
	if status != 400 {
		t.Fatalf("expected 400, got %d", status)
	}
	msg, _ := m["Message"].(string)
	if !strings.Contains(msg, "part of the key") {
		t.Fatalf("unexpected message: %q", msg)
	}
}

func TestTransactUpdateArithmeticAndIfNotExists(t *testing.T) {
	db, tb := newTestDB(t)
	mustPut(t, tb, map[string]*AttributeValue{
		"pk": aS("u1"), "sk": aS("s"), "count": aN("2.5"),
	})
	body := `{
	 "TransactItems": [
	  {"Update": {"TableName": "Test", "Key": {"pk": {"S": "u1"}, "sk": {"S": "s"}},
	   "UpdateExpression": "SET count = count + :one, name = if_not_exists(missing, :d)",
	   "ExpressionAttributeValues": {":one": {"N": "0.5"}, ":d": {"S": "default"}}}}
	 ]}`
	status, m := callTransact(t, db, body)
	if status != 200 || len(m) != 0 {
		t.Fatalf("expected 200 {}, got %d %v", status, m)
	}
	it, _ := tb.Get(keyFor("u1", "s"))
	if it["count"] == nil || it["count"].N != "3" {
		t.Fatalf("arithmetic wrong: %v", it["count"])
	}
	if it["name"] == nil || it["name"].S != "default" {
		t.Fatalf("if_not_exists wrong: %v", it["name"])
	}
}

func TestTransactConsumedCapacity(t *testing.T) {
	db, _ := newTestDB(t)
	body := `{
	 "ReturnConsumedCapacity": "TOTAL",
	 "TransactItems": [
	  {"Put": {"TableName": "Test", "Item": {"pk": {"S": "a"}, "sk": {"S": "s"}}}},
	  {"Put": {"TableName": "Test", "Item": {"pk": {"S": "b"}, "sk": {"S": "s"}}}}
	 ]}`
	status, m := callTransact(t, db, body)
	if status != 200 {
		t.Fatalf("expected 200, got %d %v", status, m)
	}
	caps, ok := m["ConsumedCapacity"].([]any)
	if !ok || len(caps) != 1 {
		t.Fatalf("unexpected consumed capacity: %v", m["ConsumedCapacity"])
	}
	c0 := caps[0].(map[string]any)
	if c0["TableName"] != "Test" || c0["CapacityUnits"] != float64(2) {
		t.Fatalf("unexpected capacity entry: %v", c0)
	}
}

func TestTransactConditionSyntaxError(t *testing.T) {
	db, _ := newTestDB(t)
	body := `{
	 "TransactItems": [
	  {"ConditionCheck": {"TableName": "Test", "Key": {"pk": {"S": "a"}, "sk": {"S": "s"}},
	   "ConditionExpression": "attribute_exists("}}
	 ]}`
	status, m := callTransact(t, db, body)
	if status != 400 {
		t.Fatalf("expected 400, got %d", status)
	}
	if code, _ := m["__type"].(string); !strings.HasSuffix(code, "#ValidationException") {
		t.Fatalf("unexpected code %q", code)
	}
	if msg, _ := m["Message"].(string); !strings.Contains(msg, "Invalid ConditionExpression") {
		t.Fatalf("unexpected message: %v", m["Message"])
	}
}
