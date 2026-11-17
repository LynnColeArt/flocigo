package ddb

import (
    "encoding/json"
    "reflect"
    "strconv"
    "strings"
    "sync"
    "testing"
)

// ---------------------------------------------------------------------------
// In-memory Store implementation used by the tests.
// ---------------------------------------------------------------------------

type memTable struct {
    schema TableSchema
    items  map[string]map[string]AttrVal
}

type MemStore struct {
    mu     sync.RWMutex
    tables map[string]*memTable
}

func newMemStore() *MemStore {
    return &MemStore{tables: map[string]*memTable{}}
}

func (s *MemStore) addTable(schema TableSchema) *MemStore {
    s.tables[schema.Name] = &memTable{schema: schema, items: map[string]map[string]AttrVal{}}
    return s
}

func (s *MemStore) Schema(table string) (TableSchema, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    t, ok := s.tables[table]
    if !ok {
        return TableSchema{}, resourceNotFound(table)
    }
    return t.schema, nil
}

func (s *MemStore) GetItem(table string, key map[string]AttrVal, _ bool) (map[string]AttrVal, bool, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    t, ok := s.tables[table]
    if !ok {
        return nil, false, resourceNotFound(table)
    }
    item, ok := t.items[keyString(key)]
    if !ok {
        return nil, false, nil
    }
    return copyItem(item), true, nil
}

func (s *MemStore) PutItem(table string, item map[string]AttrVal) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    t, ok := s.tables[table]
    if !ok {
        return resourceNotFound(table)
    }
    t.items[keyString(primaryKeySubset(t.schema, item))] = copyItem(item)
    return nil
}

func (s *MemStore) DeleteItem(table string, key map[string]AttrVal) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    t, ok := s.tables[table]
    if !ok {
        return resourceNotFound(table)
    }
    delete(t.items, keyString(key))
    return nil
}

type flakyStore struct {
    *MemStore
    failOn map[string]bool
}

func (s *flakyStore) PutItem(table string, item map[string]AttrVal) error {
    if s.failOn[table] {
        return throughputErr()
    }
    return s.MemStore.PutItem(table, item)
}

func (s *flakyStore) DeleteItem(table string, key map[string]AttrVal) error {
    if s.failOn[table] {
        return throughputErr()
    }
    return s.MemStore.DeleteItem(table, key)
}

// ---------------------------------------------------------------------------
// Test helpers.
// ---------------------------------------------------------------------------

func newTableS(name, hash string) TableSchema {
    return TableSchema{Name: name, HashKey: hash, KeyTypes: map[string]string{hash: "S"}}
}

func newTableSR(name, hash, sortK string) TableSchema {
    return TableSchema{Name: name, HashKey: hash, RangeKey: sortK, KeyTypes: map[string]string{hash: "S", sortK: "S"}}
}

func mustJSON(t *testing.T, v interface{}) []byte {
    t.Helper()
    b, err := json.Marshal(v)
    if err != nil {
        t.Fatal(err)
    }
    return b
}

func invoke(t *testing.T, h Handler, body []byte) map[string]interface{} {
    t.Helper()
    resp, ae := h.Invoke(body)
    if ae != nil {
        t.Fatalf("unexpected error: %v", ae)
    }
    raw, err := json.Marshal(resp)
    if err != nil {
        t.Fatal(err)
    }
    var out map[string]interface{}
    if err := json.Unmarshal(raw, &out); err != nil {
        t.Fatal(err)
    }
    return out
}

func expectError(t *testing.T, h Handler, body []byte, errType, msgPart string) {
    t.Helper()
    _, ae := h.Invoke(body)
    if ae == nil {
        t.Fatalf("expected %s error, got success", errType)
    }
    if ae.Type != errType {
        t.Fatalf("expected %s, got %s (%s)", errType, ae.Type, ae.Message)
    }
    if msgPart != "" && !strings.Contains(ae.Message, msgPart) {
        t.Fatalf("expected message containing %q, got %q", msgPart, ae.Message)
    }
}

func mustPut(t *testing.T, s *MemStore, table string, item map[string]AttrVal) {
    t.Helper()
    if err := s.PutItem(table, item); err != nil {
        t.Fatal(err)
    }
}

func idKeys(prefix string, n int) []map[string]AttrVal {
    keys := make([]map[string]AttrVal, 0, n)
    for i := 0; i < n; i++ {
        keys = append(keys, map[string]AttrVal{"id": MakeS(prefix + strconv.Itoa(i))})
    }
    return keys
}

func putOp(id string) map[string]interface{} {
    return map[string]interface{}{"PutRequest": map[string]interface{}{"Item": map[string]AttrVal{"id": MakeS(id)}}}
}

func writeBody(t *testing.T, items map[string]interface{}) []byte {
    return mustJSON(t, map[string]interface{}{"RequestItems": items})
}

// ---------------------------------------------------------------------------
// BatchGetItem tests.
// ---------------------------------------------------------------------------

func TestBatchGetItemBasics(t *testing.T) {
    store := newMemStore()
    store.addTable(newTableS("Users", "id"))
    store.addTable(newTableSR("Threads", "forum", "subject"))
    mustPut(t, store, "Users", map[string]AttrVal{"id": MakeS("u1"), "name": MakeS("alice")})
    mustPut(t, store, "Threads", map[string]AttrVal{"forum": MakeS("f"), "subject": MakeS("s1"), "msg": MakeS("hello")})

    body := mustJSON(t, map[string]interface{}{
        "RequestItems": map[string]interface{}{
            "Threads": map[string]interface{}{
                "Keys":                 []map[string]AttrVal{{"forum": MakeS("f"), "subject": MakeS("s1")}},
                "ProjectionExpression": "msg",
            },
            "Users": map[string]interface{}{
                "Keys": []map[string]AttrVal{{"id": MakeS("u1")}, {"id": MakeS("gone")}},
            },
        },
    })
    resp := invoke(t, NewBatchGetItemHandler(store), body)

    responses := resp["Responses"].(map[string]interface{})
    users := responses["Users"].([]interface{})
    if len(users) != 1 {
        t.Fatalf("expected 1 Users item, got %d", len(users))
    }
    u := users[0].(map[string]interface{})
    if u["name"].(map[string]interface{})["S"] != "alice" {
        t.Fatalf("unexpected user item %v", u)
    }
    if u["id"] == nil {
        t.Fatal("full item must include the key attribute")
    }
    threads := responses["Threads"].([]interface{})
    if len(threads) != 1 {
        t.Fatalf("expected 1 Threads item, got %d", len(threads))
    }
    th := threads[0].(map[string]interface{})
    if len(th) != 1 || th["msg"].(map[string]interface{})["S"] != "hello" {
        t.Fatalf("projection should return only msg, got %v", th)
    }
    if raw := mustJSON(t, resp["UnprocessedKeys"]); string(raw) != "{}" {
        t.Fatalf("expected empty UnprocessedKeys, got %s", raw)
    }
}

func TestBatchGetItemProjectionSemantics(t *testing.T) {
    store := newMemStore()
    store.addTable(newTableS("Docs", "id"))
    mustPut(t, store, "Docs", map[string]AttrVal{
        "id":   MakeS("d"),
        "spec": MakeM(map[string]AttrVal{"count": MakeN("3"), "note": MakeS("x"), "meta": MakeM(map[string]AttrVal{"deep": MakeS("yes")})}),
        "tags": MakeL([]AttrVal{MakeS("a"), MakeS("b"), MakeS("c")}),
    })
    body := mustJSON(t, map[string]interface{}{
        "RequestItems": map[string]interface{}{
            "Docs": map[string]interface{}{
                "Keys":                 []map[string]AttrVal{{"id": MakeS("d")}},
                "ProjectionExpression": "spec.count, spec.meta.deep, tags[1], tags[2], absent",
            },
        },
    })
    resp := invoke(t, NewBatchGetItemHandler(store), body)
    item := resp["Responses"].(map[string]interface{})["Docs"].([]interface{})[0].(map[string]interface{})
    want := map[string]interface{}{
        "spec": map[string]interface{}{
            "M": map[string]interface{}{
                "count": map[string]interface{}{"N": "3"},
                "meta": map[string]interface{}{
                    "M": map[string]interface{}{"deep": map[string]interface{}{"S": "yes"}},
                },
            },
        },
        "tags": map[string]interface{}{"L": []interface{}{
            map[string]interface{}{"NULL": true},
            map[string]interface{}{"S": "b"},
            map[string]interface{}{"S": "c"},
        }},
    }
    if !reflect.DeepEqual(item, want) {
        t.Fatalf("projection mismatch:\n got %v\nwant %v", item, want)
    }
}

func TestBatchGetItemValidationErrors(t *testing.T) {
    store := newMemStore()
    store.addTable(newTableS("Users", "id"))
    store.addTable(newTableS("Others", "id"))
    h := NewBatchGetItemHandler(store)

    keysFor := func(table string, keys []map[string]AttrVal) []byte {
        return mustJSON(t, map[string]interface{}{
            "RequestItems": map[string]interface{}{table: map[string]interface{}{"Keys": keys}},
        })
    }

    // More than 100 keys for a single table trips the shape level limit.
    expectError(t, h, keysFor("Users", idKeys("k", 101)), "ValidationException", "less than or equal to 100")

    // More than 100 keys across tables trips the service level limit.
    cross := mustJSON(t, map[string]interface{}{
        "RequestItems": map[string]interface{}{
            "Users":  map[string]interface{}{"Keys": idKeys("u", 60)},
            "Others": map[string]interface{}{"Keys": idKeys("o", 41)},
        },
    })
    expectError(t, h, cross, "ValidationException", "Too many items requested for batch retrieval")

    // Unknown tables fail the whole request.
    expectError(t, h, keysFor("Nope", []map[string]AttrVal{{"id": MakeS("a")}}), "ResourceNotFoundException", "Table: Nope not found")

    // Duplicate keys within one table.
    expectError(t, h, keysFor("Users", []map[string]AttrVal{{"id": MakeS("a")}, {"id": MakeS("a")}}), "ValidationException", "Provided list of item keys contains duplicates")

    // Key type must match the schema.
    expectError(t, h, keysFor("Users", []map[string]AttrVal{{"id": MakeN("1")}}), "ValidationException", "Type mismatch for key id expected: S actual: N")

    // Empty key lists are rejected.
    expectError(t, h, keysFor("Users", []map[string]AttrVal{}), "ValidationException", "Member must have length greater than 0")

    // Extra attributes in a key do not match the schema.
    expectError(t, h, keysFor("Users", []map[string]AttrVal{{"id": MakeS("a"), "other": MakeS("b")}}), "ValidationException", "The provided key element does not match the schema")

    // Overlapping projection paths.
    overlap := mustJSON(t, map[string]interface{}{
        "RequestItems": map[string]interface{}{
            "Users": map[string]interface{}{
                "Keys":                 []map[string]AttrVal{{"id": MakeS("a")}},
                "ProjectionExpression": "id, id.x",
            },
        },
    })
    expectError(t, h, overlap, "ValidationException", "Two document paths overlap with each other")
}

func TestBatchGetItemUnprocessedKeysEcho(t *testing.T) {
    defer func() { batchGetMaxTotalBytes = 16 * 1024 * 1024 }()
    batchGetMaxTotalBytes = 1500
    store := newMemStore()
    store.addTable(newTableS("Big", "id"))
    for _, id := range []string{"k0", "k1", "k2"} {
        mustPut(t, store, "Big", map[string]AttrVal{"id": MakeS(id), "v": MakeS(strings.Repeat("x", 900))})
    }
    body := mustJSON(t, map[string]interface{}{
        "RequestItems": map[string]interface{}{
            "Big": map[string]interface{}{
                "Keys":                 []map[string]AttrVal{{"id": MakeS("k0")}, {"id": MakeS("k1")}, {"id": MakeS("k2")}},
                "ProjectionExpression": "v",
            },
        },
    })
    resp := invoke(t, NewBatchGetItemHandler(store), body)
    if got := len(resp["Responses"].(map[string]interface{})["Big"].([]interface{})); got != 1 {
        t.Fatalf("expected 1 processed item, got %d", got)
    }
    unp := resp["UnprocessedKeys"].(map[string]interface{})
    ka := unp["Big"].(map[string]interface{})
    if got := len(ka["Keys"].([]interface{})); got != 2 {
        t.Fatalf("expected 2 unprocessed keys, got %d", got)
    }
    if ka["ProjectionExpression"] != "v" {
        t.Fatalf("expected ProjectionExpression echo, got %v", ka["ProjectionExpression"])
    }
}

// ---------------------------------------------------------------------------
// BatchWriteItem tests.
// ---------------------------------------------------------------------------

func TestBatchWriteItemBasics(t *testing.T) {
    store := newMemStore()
    store.addTable(newTableS("W1", "id"))
    store.addTable(newTableS("W2", "id"))
    mustPut(t, store, "W1", map[string]AttrVal{"id": MakeS("z"), "old": MakeS("x")})

    body := writeBody(t, map[string]interface{}{
        "W1": []interface{}{
            putOp("a"),
            map[string]interface{}{"DeleteRequest": map[string]interface{}{"Key": map[string]AttrVal{"id": MakeS("z")}}},
            map[string]interface{}{"ConditionCheck": map[string]interface{}{
                "Key":                 map[string]AttrVal{"id": MakeS("q")},
                "ConditionExpression": "attribute_not_exists(id)",
            }},
        },
        "W2": []interface{}{putOp("b")},
    })
    resp := invoke(t, NewBatchWriteItemHandler(store), body)
    if raw := mustJSON(t, resp["UnprocessedItems"]); string(raw) != "{}" {
        t.Fatalf("expected empty UnprocessedItems, got %s", raw)
    }
    if _, ok := store.tables["W1"].items[keyString(map[string]AttrVal{"id": MakeS("a")})]; !ok {
        t.Fatal("expected item a to exist")
    }
    if _, ok := store.tables["W1"].items[keyString(map[string]AttrVal{"id": MakeS("z")})]; ok {
        t.Fatal("expected item z to be deleted")
    }
    if _, ok := store.tables["W2"].items[keyString(map[string]AttrVal{"id": MakeS("b")})]; !ok {
        t.Fatal("expected item b in W2")
    }
    if _, ok := store.tables["W1"].items[keyString(map[string]AttrVal{"id": MakeS("q")})]; ok {
        t.Fatal("ConditionCheck must not create items")
    }
}

func TestBatchWriteItemConditionFailedSkippedSilently(t *testing.T) {
    store := newMemStore()
    store.addTable(newTableS("W", "id"))
    mustPut(t, store, "W", map[string]AttrVal{"id": MakeS("x"), "val": MakeS("1")})
    body := writeBody(t, map[string]interface{}{
        "W": []interface{}{
            map[string]interface{}{"ConditionCheck": map[string]interface{}{
                "Key":                       map[string]AttrVal{"id": MakeS("x")},
                "ConditionExpression":       "val = :v",
                "ExpressionAttributeValues": map[string]AttrVal{":v": MakeS("9")},
            }},
            putOp("b"),
        },
    })
    resp := invoke(t, NewBatchWriteItemHandler(store), body)
    if raw := mustJSON(t, resp["UnprocessedItems"]); string(raw) != "{}" {
        t.Fatalf("failed condition must not surface in UnprocessedItems, got %s", raw)
    }
    x := store.tables["W"].items[keyString(map[string]AttrVal{"id": MakeS("x")})]
    if x == nil || x["val"].S != "1" {
        t.Fatalf("item x must be unchanged, got %v", x)
    }
    if _, ok := store.tables["W"].items[keyString(map[string]AttrVal{"id": MakeS("b")})]; !ok {
        t.Fatal("expected item b to exist after successful put")
    }
}

func TestBatchWriteItemValidationErrors(t *testing.T) {
    store := newMemStore()
    store.addTable(newTableS("W1", "id"))
    store.addTable(newTableS("W2", "id"))
    h := NewBatchWriteItemHandler(store)

    one := make([]interface{}, 0, 26)
    for i := 0; i < 26; i++ {
        one = append(one, putOp("s"+strconv.Itoa(i)))
    }
    expectError(t, h, writeBody(t, map[string]interface{}{"W1": one}), "ValidationException", "less than or equal to 25")

    twenty := make([]interface{}, 0, 20)
    for i := 0; i < 20; i++ {
        twenty = append(twenty, putOp("a"+strconv.Itoa(i)))
    }
    six := make([]interface{}, 0, 6)
    for i := 0; i < 6; i++ {
        six = append(six, putOp("b"+strconv.Itoa(i)))
    }
    expectError(t, h, writeBody(t, map[string]interface{}{"W1": twenty, "W2": six}), "ValidationException", "Too many items requested for batch write")

    expectError(t, h, writeBody(t, map[string]interface{}{"Nope": []interface{}{putOp("a")}}), "ResourceNotFoundException", "Table: Nope not found")

    expectError(t, h, writeBody(t, map[string]interface{}{"W1": []interface{}{putOp("d"), putOp("d")}}), "ValidationException", "Provided list of item keys contains duplicates")

    expectError(t, h, writeBody(t, map[string]interface{}{
        "W1": []interface{}{map[string]interface{}{"PutRequest": map[string]interface{}{"Item": map[string]AttrVal{"val": MakeS("x")}}}},
    }), "ValidationException", "Missing the key id in the item")

    big := strings.Repeat("v", maxItemBytes+16)
    expectError(t, h, writeBody(t, map[string]interface{}{
        "W1": []interface{}{map[string]interface{}{"PutRequest": map[string]interface{}{"Item": map[string]AttrVal{"id": MakeS("big"), "val": MakeS(big)}}}},
    }), "ValidationException", "Item size has exceeded the maximum allowed size")

    expectError(t, h, writeBody(t, map[string]interface{}{"W1": []interface{}{map[string]interface{}{}}}), "ValidationException", "exactly one of PutRequest")
}

func TestBatchWriteItemConditionValidation(t *testing.T) {
    store := newMemStore()
    store.addTable(newTableS("W", "id"))
    h := NewBatchWriteItemHandler(store)
    base := func(cond string) []byte {
        return writeBody(t, map[string]interface{}{
            "W": []interface{}{map[string]interface{}{
                "ConditionCheck": map[string]interface{}{
                    "Key":                       map[string]AttrVal{"id": MakeS("a")},
                    "ConditionExpression":       cond,
                    "ExpressionAttributeValues": map[string]AttrVal{":v": MakeS("a"), ":w": MakeS("b")},
                },
            }},
        })
    }
    expectError(t, h, base("#h = :v"), "ValidationException", "not defined in the expression")
    expectError(t, h, base("id = :v"), "ValidationException", "unused in expressions")
    expectError(t, h, base("id ="), "ValidationException", "Syntax error")
}

func TestBatchWriteItemUnprocessedItems(t *testing.T) {
    base := newMemStore()
    base.addTable(newTableS("A", "id"))
    base.addTable(newTableS("B", "id"))
    store := &flakyStore{MemStore: base, failOn: map[string]bool{"B": true}}
    h := NewBatchWriteItemHandler(store)
    body := writeBody(t, map[string]interface{}{
        "A": []interface{}{putOp("a")},
        "B": []interface{}{
            putOp("b1"),
            map[string]interface{}{"DeleteRequest": map[string]interface{}{"Key": map[string]AttrVal{"id": MakeS("b2")}}},
            putOp("b3"),
        },
    })
    resp := invoke(t, h, body)
    unp := resp["UnprocessedItems"].(map[string]interface{})
    if _, ok := unp["A"]; ok {
        t.Fatal("table A should have been processed")
    }
    b := unp["B"].([]interface{})
    if len(b) != 3 {
        t.Fatalf("expected 3 unprocessed requests for B, got %d", len(b))
    }
    first := b[0].(map[string]interface{})["PutRequest"]
    if first == nil {
        t.Fatal("expected PutRequest echo")
    }
    if id := first.(map[string]interface{})["Item"].(map[string]interface{})["id"].(map[string]interface{})["S"]; id != "b1" {
        t.Fatalf("unexpected echo %v", first)
    }
    if _, ok := b[1].(map[string]interface{})["DeleteRequest"]; !ok {
        t.Fatal("expected DeleteRequest echo")
    }
    if _, ok := b[2].(map[string]interface{})["PutRequest"]; !ok {
        t.Fatal("expected PutRequest echo")
    }
    if _, ok := store.MemStore.tables["A"].items[keyString(map[string]AttrVal{"id": MakeS("a")})]; !ok {
        t.Fatal("expected the A put to be applied")
    }
}

// ---------------------------------------------------------------------------
// Condition expression engine tests.
// ---------------------------------------------------------------------------

func TestConditionExpressionSemantics(t *testing.T) {
    item := map[string]AttrVal{
        "id":  MakeS("abc"),
        "num": MakeN("10"),
        "set": MakeSS([]string{"x", "y"}),
        "doc": MakeM(map[string]AttrVal{"k": MakeS("v")}),
        "lst": MakeL([]AttrVal{MakeN("1"), MakeN("2")}),
    }
    run := func(expr string, values map[string]AttrVal, want bool) {
        t.Helper()
        node, err := compileCondition(expr, nil, values)
        if err != nil {
            t.Fatalf("%s: unexpected error %v", expr, err)
        }
        got, evalErr := node.eval(&condEvalCtx{item: item, values: values})
        if evalErr != nil {
            t.Fatalf("%s: unexpected eval error %v", expr, evalErr)
        }
        if got != want {
            t.Fatalf("%s: got %v want %v", expr, got, want)
        }
    }
    run("id = :v", map[string]AttrVal{":v": MakeS("abc")}, true)
    run("id <> :v", map[string]AttrVal{":v": MakeS("zzz")}, false)
    run("num > :n", map[string]AttrVal{":n": MakeN("9")}, true)
    run("num BETWEEN :lo AND :hi", map[string]AttrVal{":lo": MakeN("1"), ":hi": MakeN("20")}, true)
    run("id IN (:a, :b)", map[string]AttrVal{":a": MakeS("zz"), ":b": MakeS("abc")}, true)
    run("contains(set, :x)", map[string]AttrVal{":x": MakeS("x")}, true)
    run("contains(doc.k, :s)", map[string]AttrVal{":s": MakeS("v")}, true)
    run("begins_with(id, :p)", map[string]AttrVal{":p": MakeS("ab")}, true)
    run("attribute_exists(doc.missing)", nil, false)
    run("attribute_exists(doc.k)", nil, true)
    run("attribute_type(num, :t)", map[string]AttrVal{":t": MakeS("N")}, true)
    run("size(set) = :n", map[string]AttrVal{":n": MakeN("2")}, true)
    run("size(doc) < :n", map[string]AttrVal{":n": MakeN("5")}, true)
    run("NOT id = :v", map[string]AttrVal{":v": MakeS("zzz")}, true)
    run("(id = :a OR num = :n) AND attribute_type(id, :t)", map[string]AttrVal{":a": MakeS("abc"), ":n": MakeN("10"), ":t": MakeS("S")}, true)
    run("missing = :v", map[string]AttrVal{":v": MakeS("a")}, false)
    run("missing[0] = :v", map[string]AttrVal{":v": MakeS("a")}, false)

    // Traversing through a scalar raises a document path error at eval time.
    node, err := compileCondition("id.x = :v", nil, map[string]AttrVal{":v": MakeS("a")})
    if err != nil {
        t.Fatal(err)
    }
    _, evalErr := node.eval(&condEvalCtx{item: item, values: map[string]AttrVal{":v": MakeS("a")}})
    if evalErr == nil || !strings.Contains(evalErr.Message, "invalid for the item") {
        t.Fatalf("expected invalid document path error, got %v", evalErr)
    }
}

func TestConditionExpressionValidation(t *testing.T) {
    if _, err := compileCondition("#h = :v", nil, map[string]AttrVal{":v": MakeS("a")}); err == nil || !strings.Contains(err.Message, "not defined") {
        t.Fatalf("expected undefined name error, got %v", err)
    }
    if _, err := compileCondition("id = :v", nil, map[string]AttrVal{":v": MakeS("a"), ":w": MakeS("b")}); err == nil || !strings.Contains(err.Message, "unused in expressions") {
        t.Fatalf("expected unused value error, got %v", err)
    }
    if _, err := compileCondition("id =", nil, nil); err == nil || !strings.Contains(err.Message, "Syntax error") {
        t.Fatalf("expected syntax error, got %v", err)
    }
}
