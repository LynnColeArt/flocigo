package dynamodb

import (
    "encoding/json"
    "reflect"
    "strings"
    "testing"
)

type reqMap = map[string]interface{}

func avS(s string) *AttributeValue { return NewString(s) }
func avN(s string) *AttributeValue { return NewNumber(s) }

func mustPut(t *testing.T, tbl *Table, pk, sk string, attrs map[string]*AttributeValue) {
    t.Helper()
    item := Item{"pk": avS(pk), "sk": avN(sk)}
    for k, v := range attrs {
        item[k] = v
    }
    if aerr := tbl.PutItem(item); aerr != nil {
        t.Fatalf("PutItem(%s,%s): %v", pk, sk, aerr)
    }
}

func mustDo(t *testing.T, reg *Registry, req reqMap) map[string]interface{} {
    t.Helper()
    resp, aerr := DoScan(reg, req)
    if aerr != nil {
        t.Fatalf("DoScan(%v) unexpected error: %v", req, aerr)
    }
    return resp
}

func doErr(t *testing.T, reg *Registry, req reqMap) *APIError {
    t.Helper()
    _, aerr := DoScan(reg, req)
    if aerr == nil {
        t.Fatalf("DoScan(%v) expected an error", req)
    }
    return aerr
}

func itemsOf(t *testing.T, resp map[string]interface{}) []Item {
    t.Helper()
    raw, present := resp["Items"]
    if !present {
        return nil
    }
    items, ok := raw.([]Item)
    if !ok {
        t.Fatalf("Items has unexpected type %T", raw)
    }
    return items
}

func keyStrings(items []Item) []string {
    out := []string{}
    for _, it := range items {
        out = append(out, it["pk"].S+"/"+it["sk"].N)
    }
    return out
}

func expectKeys(t *testing.T, got []string, want ...string) {
    t.Helper()
    if got == nil {
        got = []string{}
    }
    if !reflect.DeepEqual(got, want) {
        t.Fatalf("keys = %v, want %v", got, want)
    }
}

func newPair() (*Registry, *Table) {
    reg := NewRegistry()
    tbl := NewTable("orders", "pk", "sk")
    reg.AddTable(tbl)
    return reg, tbl
}

func TestScanIteratesAllPkPartitionsAndOrdering(t *testing.T) {
    reg, tbl := newPair()
    for _, k := range [][2]string{{"B", "2"}, {"A", "2"}, {"B", "1"}, {"C", "1"}, {"A", "1"}, {"C", "2"}} {
        mustPut(t, tbl, k[0], k[1], nil)
    }

    resp := mustDo(t, reg, reqMap{"TableName": "orders"})
    if resp["Count"] != 6 || resp["ScannedCount"] != 6 {
        t.Fatalf("counts = %v / %v, want 6 / 6", resp["Count"], resp["ScannedCount"])
    }
    expectKeys(t, keyStrings(itemsOf(t, resp)), "A/1", "A/2", "B/1", "B/2", "C/1", "C/2")
    if _, has := resp["LastEvaluatedKey"]; has {
        t.Fatalf("unexpected LastEvaluatedKey")
    }

    resp = mustDo(t, reg, reqMap{"TableName": "orders", "ScanIndexForward": false})
    expectKeys(t, keyStrings(itemsOf(t, resp)), "C/2", "C/1", "B/2", "B/1", "A/2", "A/1")
}

func TestScanLimitPaginationForwardAndBackward(t *testing.T) {
    reg, tbl := newPair()
    for i := 1; i <= 5; i++ {
        mustPut(t, tbl, "A", strconvItoa(i), nil)
    }

    var pages [][]string
    esk := reqMap(nil)
    totalScanned := 0
    for {
        req := reqMap{"TableName": "orders", "Limit": float64(2)}
        if esk != nil {
            req["ExclusiveStartKey"] = esk
        }
        resp := mustDo(t, reg, req)
        pages = append(pages, keyStrings(itemsOf(t, resp)))
        totalScanned += resp["ScannedCount"].(int)
        lek, has := resp["LastEvaluatedKey"]
        if !has {
            break
        }
        esk = lek.(Item)
    }
    if !reflect.DeepEqual(pages, [][]string{{"A/1", "A/2"}, {"A/3", "A/4"}, {"A/5"}}) {
        t.Fatalf("forward pages = %v", pages)
    }
    if totalScanned != 5 {
        t.Fatalf("total scanned = %d, want 5", totalScanned)
    }

    pages = nil
    esk = reqMap(nil)
    for {
        req := reqMap{"TableName": "orders", "Limit": float64(2), "ScanIndexForward": false}
        if esk != nil {
            req["ExclusiveStartKey"] = esk
        }
        resp := mustDo(t, reg, req)
        pages = append(pages, keyStrings(itemsOf(t, resp)))
        lek, has := resp["LastEvaluatedKey"]
        if !has {
            break
        }
        esk = lek.(Item)
    }
    if !reflect.DeepEqual(pages, [][]string{{"A/5", "A/4"}, {"A/3", "A/2"}, {"A/1"}}) {
        t.Fatalf("backward pages = %v", pages)
    }
}

func strconvItoa(n int) string {
    return string([]byte{byte('0' + n)})
}

func TestScanLimitWithFilterCountsEvaluatedItems(t *testing.T) {
    reg, tbl := newPair()
    tbl.Name = "mixtbl"
    reg.Tables = map[string]*Table{"mixtbl": tbl}
    flag := func(y string) map[string]*AttributeValue {
        return map[string]*AttributeValue{"f": avS(y)}
    }
    mustPut(t, tbl, "A", "1", flag("n"))
    mustPut(t, tbl, "A", "2", flag("y"))
    mustPut(t, tbl, "A", "3", flag("n"))
    mustPut(t, tbl, "A", "4", flag("y"))
    mustPut(t, tbl, "A", "5", flag("n"))

    esk := reqMap(nil)
    wantScanned := []int{2, 2, 1}
    page := 0
    total := 0
    for {
        req := reqMap{
            "TableName":        "mixtbl",
            "Limit":            float64(2),
            "FilterExpression": "f = :y",
            "ExpressionAttributeValues": reqMap{":y": map[string]interface{}{"S": "y"}},
        }
        if esk != nil {
            req["ExclusiveStartKey"] = esk
        }
        resp := mustDo(t, reg, req)
        if resp["ScannedCount"].(int) != wantScanned[page] {
            t.Fatalf("page %d scanned = %d, want %d", page, resp["ScannedCount"].(int), wantScanned[page])
        }
        total += resp["ScannedCount"].(int)
        page++
        lek, has := resp["LastEvaluatedKey"]
        if !has {
            break
        }
        esk = lek.(Item)
    }
    if total != 5 {
        t.Fatalf("total scanned = %d, want 5", total)
    }

    resp := mustDo(t, reg, reqMap{
        "TableName":        "mixtbl",
        "FilterExpression": "f = :y",
        "ExpressionAttributeValues": reqMap{":y": map[string]interface{}{"S": "y"}},
    })
    if resp["Count"] != 2 {
        t.Fatalf("Count = %v, want 2", resp["Count"])
    }
    expectKeys(t, keyStrings(itemsOf(t, resp)), "A/2", "A/4")
}

func TestScanExclusiveStartKeyPositioning(t *testing.T) {
    reg, tbl := newPair()
    for i := 1; i <= 5; i++ {
        mustPut(t, tbl, "A", strconvItoa(i), nil)
    }
    esk := Item{"pk": avS("A"), "sk": avN("2.5")}

    resp := mustDo(t, reg, reqMap{"TableName": "orders", "ExclusiveStartKey": esk})
    expectKeys(t, keyStrings(itemsOf(t, resp)), "A/3", "A/4", "A/5")

    resp = mustDo(t, reg, reqMap{"TableName": "orders", "ExclusiveStartKey": esk, "ScanIndexForward": false})
    expectKeys(t, keyStrings(itemsOf(t, resp)), "A/2", "A/1")

    lek, _ := tbl.sortedEntries()[2].item, true
    _ = lek
    resp = mustDo(t, reg, reqMap{"TableName": "orders", "Limit": float64(1), "ExclusiveStartKey": Item{"pk": avS("A"), "sk": avN("2")}})
    expectKeys(t, keyStrings(itemsOf(t, resp)), "A/3")
    wantLEK := Item{"pk": avS("A"), "sk": avN("3")}
    if !reflect.DeepEqual(resp["LastEvaluatedKey"], wantLEK) {
        t.Fatalf("LastEvaluatedKey = %v, want %v", resp["LastEvaluatedKey"], wantLEK)
    }
}

func runFilterItems(t *testing.T, reg *Registry, cond string, eav reqMap) ([]string, int, int) {
    t.Helper()
    req := reqMap{"TableName": "catalog", "FilterExpression": cond}
    if eav != nil {
        req["ExpressionAttributeValues"] = eav
    }
    resp := mustDo(t, reg, req)
    return keyStrings(itemsOf(t, resp)), resp["Count"].(int), resp["ScannedCount"].(int)
}

func TestScanFilterExpressionOperators(t *testing.T) {
    reg := NewRegistry()
    tbl := NewTable("catalog", "pk", "sk")
    reg.AddTable(tbl)
    mustPut(t, tbl, "A", "1", map[string]*AttributeValue{
        "n": avN("5"),
        "s": avS("apple"),
        "m": NewMap(map[string]*AttributeValue{"x": avS("1"), "y": avS("2")}),
        "l": NewList(avS("a"), avS("b"), avS("c")),
    })
    mustPut(t, tbl, "A", "2", map[string]*AttributeValue{"n": avN("10"), "s": avS("banana")})
    mustPut(t, tbl, "B", "1", map[string]*AttributeValue{"n": avN("20"), "s": avS("apricot")})
    mustPut(t, tbl, "B", "2", map[string]*AttributeValue{"n": avN("1"), "s": avS("zzz")})

    keys, count, scanned := runFilterItems(t, reg, "n > :x", reqMap{":x": map[string]interface{}{"N": "7"}})
    expectKeys(t, keys, "A/2", "B/1")
    if count != 2 || scanned != 4 {
        t.Fatalf("count/scanned = %d/%d, want 2/4", count, scanned)
    }

    keys, _, _ = runFilterItems(t, reg, "(n > :x AND s = :y) OR contains(l, :v)", reqMap{
        ":x": map[string]interface{}{"N": "7"},
        ":y": map[string]interface{}{"S": "banana"},
        ":v": map[string]interface{}{"S": "b"},
    })
    expectKeys(t, keys, "A/1", "A/2")

    keys, _, _ = runFilterItems(t, reg, "s BETWEEN :lo AND :hi", reqMap{
        ":lo": map[string]interface{}{"S": "ap"},
        ":hi": map[string]interface{}{"S": "aq"},
    })
    expectKeys(t, keys, "A/1", "B/1")

    keys, _, _ = runFilterItems(t, reg, "NOT (n >= :lo)", reqMap{":lo": map[string]interface{}{"N": "5"}})
    expectKeys(t, keys, "B/2")

    keys, _, _ = runFilterItems(t, reg, "size(m) = :k", reqMap{":k": map[string]interface{}{"N": "2"}})
    expectKeys(t, keys, "A/1")

    keys, _, _ = runFilterItems(t, reg, "begins_with(s, :p)", reqMap{":p": map[string]interface{}{"S": "ap"}})
    expectKeys(t, keys, "A/1", "B/1")

    keys, _, _ = runFilterItems(t, reg, "s IN (:a, :b)", reqMap{
        ":a": map[string]interface{}{"S": "zzz"},
        ":b": map[string]interface{}{"S": "banana"},
    })
    expectKeys(t, keys, "A/2", "B/2")

    keys, _, _ = runFilterItems(t, reg, "attribute_type(n, :tt)", reqMap{":tt": map[string]interface{}{"S": "N"}})
    expectKeys(t, keys, "A/1", "A/2", "B/1", "B/2")

    keys, _, _ = runFilterItems(t, reg, "attribute_not_exists(m)", nil)
    expectKeys(t, keys, "A/2", "B/1", "B/2")
}

func TestScanFilterTypeMismatchAndNumbers(t *testing.T) {
    reg := NewRegistry()
    tbl := NewTable("ones", "pk", "sk")
    reg.AddTable(tbl)
    mustPut(t, tbl, "A", "1", map[string]*AttributeValue{"n": avN("5"), "s": avS("5")})

    keys, _, scanned := runFilterItems(t, reg, "n = :s", reqMap{":s": map[string]interface{}{"S": "5"}})
    expectKeys(t, keys)
    if scanned != 1 {
        t.Fatalf("scanned = %d, want 1", scanned)
    }

    keys, _, _ = runFilterItems(t, reg, "n <> :s", reqMap{":s": map[string]interface{}{"S": "5"}})
    expectKeys(t, keys, "A/1")

    keys, _, _ = runFilterItems(t, reg, "n < :s", reqMap{":s": map[string]interface{}{"S": "5"}})
    expectKeys(t, keys)

    keys, _, _ = runFilterItems(t, reg, "n = :n2", reqMap{":n2": map[string]interface{}{"N": "5.0"}})
    expectKeys(t, keys, "A/1")

    keys, _, _ = runFilterItems(t, reg, "n = :n3", reqMap{":n3": map[string]interface{}{"N": "5e0"}})
    expectKeys(t, keys, "A/1")

    keys, _, _ = runFilterItems(t, reg, "nope <> :x", reqMap{":x": map[string]interface{}{"S": "z"}})
    expectKeys(t, keys)
}

func TestScanSelectCount(t *testing.T) {
    reg, tbl := newPair()
    tbl.Name = "countme"
    reg.Tables = map[string]*Table{"countme": tbl}
    for i := 1; i <= 3; i++ {
        mustPut(t, tbl, "A", strconvItoa(i), nil)
    }
    resp := mustDo(t, reg, reqMap{"TableName": "countme", "Select": "COUNT"})
    if _, has := resp["Items"]; has {
        t.Fatalf("Select=COUNT must not return Items")
    }
    if resp["Count"] != 3 || resp["ScannedCount"] != 3 {
        t.Fatalf("counts = %v / %v, want 3 / 3", resp["Count"], resp["ScannedCount"])
    }
}

func TestScanProjectionExpression(t *testing.T) {
    reg := NewRegistry()
    tbl := NewTable("proj", "pk", "sk")
    reg.AddTable(tbl)
    mustPut(t, tbl, "A", "1", map[string]*AttributeValue{
        "a": NewMap(map[string]*AttributeValue{"b": avS("1"), "c": avS("2")}),
        "d": NewList(avS("0"), avN("1")),
        "e": avS("keep"),
    })

    resp := mustDo(t, reg, reqMap{
        "TableName":              "proj",
        "ProjectionExpression":   "a.b, #d[1]",
        "ExpressionAttributeNames": reqMap{"#d": "d"},
    })
    items := itemsOf(t, resp)
    if len(items) != 1 {
        t.Fatalf("got %d items, want 1", len(items))
    }
    want := Item{
        "a": NewMap(map[string]*AttributeValue{"b": avS("1")}),
        "d": NewList(NewNull(), avN("1")),
    }
    if !reflect.DeepEqual(items[0], want) {
        t.Fatalf("projected item = %v, want %v", items[0], want)
    }

    resp = mustDo(t, reg, reqMap{"TableName": "proj", "ProjectionExpression": "zzz.x"})
    if _, has := resp["Items"]; has {
        t.Fatalf("projected item with only missing paths must be omitted")
    }
    if resp["Count"] != 1 {
        t.Fatalf("Count = %v, want 1", resp["Count"])
    }

    aerr := doErr(t, reg, reqMap{"TableName": "proj", "ProjectionExpression": "a, a.b"})
    if !strings.Contains(aerr.Message, "overlap") {
        t.Fatalf("message = %q, want overlap error", aerr.Message)
    }
}

func TestScanConsumedCapacity(t *testing.T) {
    reg := NewRegistry()
    tbl := NewTable("cap1", "pk", "")
    reg.AddTable(tbl)
    if aerr := tbl.PutItem(Item{"pk": avS("a"), "v": avN("1")}); aerr != nil {
        t.Fatalf("PutItem: %v", aerr)
    }

    resp := mustDo(t, reg, reqMap{"TableName": "cap1", "ReturnConsumedCapacity": "TOTAL"})
    cc, ok := resp["ConsumedCapacity"].(map[string]interface{})
    if !ok || cc["TableName"] != "cap1" {
        t.Fatalf("ConsumedCapacity = %v", resp["ConsumedCapacity"])
    }
    if u, _ := cc["CapacityUnits"].(float64); u != 0.5 {
        t.Fatalf("CapacityUnits = %v, want 0.5", cc["CapacityUnits"])
    }

    resp = mustDo(t, reg, reqMap{"TableName": "cap1", "ConsistentRead": true, "ReturnConsumedCapacity": "INDEXES"})
    cc = resp["ConsumedCapacity"].(map[string]interface{})
    if u, _ := cc["CapacityUnits"].(float64); u != 1 {
        t.Fatalf("CapacityUnits = %v, want 1", cc["CapacityUnits"])
    }
    if ru, _ := cc["ReadCapacityUnits"].(float64); ru != 1 {
        t.Fatalf("ReadCapacityUnits = %v, want 1", cc["ReadCapacityUnits"])
    }
    tc, ok := cc["Table"].(map[string]interface{})
    if !ok {
        t.Fatalf("INDEXES must include Table")
    }
    if u, _ := tc["CapacityUnits"].(float64); u != 1 {
        t.Fatalf("Table CapacityUnits = %v, want 1", tc["CapacityUnits"])
    }
}

func TestScanParallelSegments(t *testing.T) {
    reg, tbl := newPair()
    for _, p := range []string{"A", "B", "C", "D", "E", "F"} {
        mustPut(t, tbl, p, "1", nil)
    }
    seen := map[string]bool{}
    totalScanned := 0
    for _, seg := range []float64{0, 1} {
        resp := mustDo(t, reg, reqMap{"TableName": "orders", "TotalSegments": float64(2), "Segment": seg})
        for _, it := range itemsOf(t, resp) {
            k := it["pk"].S + "/" + it["sk"].N
            if seen[k] {
                t.Fatalf("item %s returned by multiple segments", k)
            }
            seen[k] = true
        }
        totalScanned += resp["ScannedCount"].(int)
    }
    if len(seen) != 6 {
        t.Fatalf("segments covered %d items, want 6", len(seen))
    }
    if totalScanned != 6 {
        t.Fatalf("total scanned = %d, want 6", totalScanned)
    }
}

func TestScanLegacyScanFilterAndAttributesToGet(t *testing.T) {
    reg := NewRegistry()
    tbl := NewTable("ltbl", "pk", "sk")
    reg.AddTable(tbl)
    mustPut(t, tbl, "A", "1", map[string]*AttributeValue{"n": avN("5"), "s": avS("apple")})
    mustPut(t, tbl, "A", "2", map[string]*AttributeValue{"n": avN("10"), "s": avS("banana")})
    mustPut(t, tbl, "B", "1", map[string]*AttributeValue{"n": avN("20"), "s": avS("apricot")})

    resp := mustDo(t, reg, reqMap{
        "TableName": "ltbl",
        "ScanFilter": reqMap{
            "n": reqMap{
                "ComparisonOperator": "GT",
                "AttributeValueList": []interface{}{reqMap{"N": "7"}},
            },
        },
    })
    expectKeys(t, keyStrings(itemsOf(t, resp)), "A/2", "B/1")

    resp = mustDo(t, reg, reqMap{
        "TableName":         "ltbl",
        "ScanFilter":        reqMap{
            "nope": reqMap{"ComparisonOperator": "NULL", "AttributeValueList": []interface{}{}},
        },
        "AttributesToGet":   []interface{}{"s"},
    })
    items := itemsOf(t, resp)
    if len(items) != 3 {
        t.Fatalf("got %d items, want 3", len(items))
    }
    for _, it := range items {
        if _, ok := it["s"]; !ok || len(it) != 1 {
            t.Fatalf("projected item = %v, want only s", it)
        }
    }

    resp = mustDo(t, reg, reqMap{
        "TableName":           "ltbl",
        "ScanFilter":          reqMap{
            "n": reqMap{"ComparisonOperator": "GT", "AttributeValueList": []interface{}{reqMap{"N": "15"}}},
            "s": reqMap{"ComparisonOperator": "EQ", "AttributeValueList": []interface{}{reqMap{"S": "apple"}}},
        },
        "ConditionalOperator": "OR",
    })
    expectKeys(t, keyStrings(itemsOf(t, resp)), "A/1", "B/1")
}

func TestScanValidationErrors(t *testing.T) {
    reg := NewRegistry()
    tbl := NewTable("music", "pk", "sk")
    reg.AddTable(tbl)
    mustPut(t, tbl, "A", "1", map[string]*AttributeValue{"n": avN("5")})

    legacyFilter := reqMap{
        "n": reqMap{
            "ComparisonOperator": "EQ",
            "AttributeValueList": []interface{}{reqMap{"N": "5"}},
        },
    }

    cases := []struct {
        name string
        req  reqMap
        code string
        msg  string
    }{
        {"missing table name", reqMap{}, "ValidationException", "TableName"},
        {"short table name", reqMap{"TableName": "ab"}, "ValidationException", "TableName"},
        {"unknown table", reqMap{"TableName": "missing"}, "ResourceNotFoundException", ""},
        {"limit below one", reqMap{"TableName": "music", "Limit": float64(0)}, "ValidationException", "Limit"},
        {"truncated filter", reqMap{"TableName": "music", "FilterExpression": "n ="}, "ValidationException", ""},
        {"undefined value", reqMap{"TableName": "music", "FilterExpression": "n > :v"}, "ValidationException", "not defined"},
        {"undefined name", reqMap{"TableName": "music", "FilterExpression": "#k = :v"}, "ValidationException", "not defined"},
        {"reserved keyword", reqMap{
            "TableName":                 "music",
            "FilterExpression":          "size > :v",
            "ExpressionAttributeValues": reqMap{":v": reqMap{"N": "1"}},
        }, "ValidationException", "reserved keyword"},
        {"legacy and expression", reqMap{
            "TableName":        "music",
            "FilterExpression": "n > :v",
            "ScanFilter":       legacyFilter,
        }, "ValidationException", "non-expression"},
        {"invalid number", reqMap{
            "TableName":                 "music",
            "FilterExpression":          "n > :v",
            "ExpressionAttributeValues": reqMap{":v": reqMap{"N": "abc"}},
        }, "ValidationException", "Invalid number"},
        {"unused value", reqMap{
            "TableName":                 "music",
            "FilterExpression":          "n = :a",
            "ExpressionAttributeValues": reqMap{":a": reqMap{"N": "1"}, ":b": reqMap{"N": "2"}},
        }, "ValidationException", "unused"},
        {"unused name", reqMap{
            "TableName":                 "music",
            "FilterExpression":          "n = :n",
            "ExpressionAttributeValues": reqMap{":n": reqMap{"N": "1"}},
            "ExpressionAttributeNames":  reqMap{"#n": "n"},
        }, "ValidationException", "unused"},
        {"starting key schema", reqMap{
            "TableName":          "music",
            "ExclusiveStartKey":  reqMap{"pk": reqMap{"S": "A"}},
        }, "ValidationException", "provided starting key"},
        {"segment out of range", reqMap{
            "TableName":     "music",
            "TotalSegments": float64(2),
            "Segment":       float64(2),
        }, "ValidationException", "less than TotalSegments"},
        {"index not found", reqMap{"TableName": "music", "IndexName": "gsi1"}, "ValidationException", "specified index"},
        {"bad select", reqMap{"TableName": "music", "Select": "BOGUS"}, "ValidationException", "Select"},
    }
    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            aerr := doErr(t, reg, tc.req)
            if aerr.Code != tc.code {
                t.Fatalf("code = %s, want %s (message: %s)", aerr.Code, tc.code, aerr.Message)
            }
            if tc.msg != "" && !strings.Contains(aerr.Message, tc.msg) {
                t.Fatalf("message %q does not contain %q", aerr.Message, tc.msg)
            }
        })
    }

    resp := mustDo(t, reg, reqMap{"TableName": "music", "ExclusiveStartKey": Item{"pk": avS("A"), "sk": avN("1")}})
    if resp["ScannedCount"] != 0 {
        t.Fatalf("ExclusiveStartKey at last item must scan nothing, got %v", resp["ScannedCount"])
    }
}

func TestHandleScanWireProtocol(t *testing.T) {
    tbl := NewTable("wiretbl", "pk", "")
    DefaultRegistry.AddTable(tbl)
    if aerr := tbl.PutItem(Item{"pk": avS("a"), "v": avN("1")}); aerr != nil {
        t.Fatalf("PutItem: %v", aerr)
    }

    status, body := HandleScan([]byte(`{"TableName":"wiretbl","ReturnConsumedCapacity":"TOTAL"}`))
    if status != 200 {
        t.Fatalf("status = %d, body = %s", status, body)
    }
    var out map[string]interface{}
    if err := json.Unmarshal(body, &out); err != nil {
        t.Fatalf("bad response json: %v", err)
    }
    if out["Count"] != float64(1) {
        t.Fatalf("Count = %v, want 1", out["Count"])
    }
    items, ok := out["Items"].([]interface{})
    if !ok || len(items) != 1 {
        t.Fatalf("Items = %v", out["Items"])
    }
    first := items[0].(map[string]interface{})
    if first["v"].(map[string]interface{})["N"] != "1" {
        t.Fatalf("number encoding broken: %v", first)
    }
    cc := out["ConsumedCapacity"].(map[string]interface{})
    if u, _ := cc["CapacityUnits"].(float64); u != 0.5 {
        t.Fatalf("CapacityUnits = %v, want 0.5", cc["CapacityUnits"])
    }

    status, body = HandleScan([]byte(`{"TableName":"goneaway"}`))
    if status != 400 {
        t.Fatalf("status = %d, want 400", status)
    }
    var errOut map[string]interface{}
    if err := json.Unmarshal(body, &errOut); err != nil {
        t.Fatalf("bad error json: %v", err)
    }
    if typ, _ := errOut["__type"].(string); !strings.HasSuffix(typ, "ResourceNotFoundException") {
        t.Fatalf("__type = %v, want ResourceNotFoundException", errOut["__type"])
    }

    status, _ = HandleScan([]byte("{oops"))
    if status != 400 {
        t.Fatalf("status = %d, want 400 for invalid json", status)
    }

    status, body = HandleScan([]byte(`{"TableName":"wiretbl","FilterExpression":"v < :x","ExpressionAttributeValues":{":x":{"N":"2"}}}`))
    if status != 200 {
        t.Fatalf("status = %d, body = %s", status, body)
    }
    out = map[string]interface{}{}
    if err := json.Unmarshal(body, &out); err != nil {
        t.Fatalf("bad response json: %v", err)
    }
    if out["Count"] != float64(1) {
        t.Fatalf("Count = %v, want 1", out["Count"])
    }
}