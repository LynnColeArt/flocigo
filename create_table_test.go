package main

import (
    "bytes"
    "encoding/json"
    "io"
    "net/http"
    "net/http/httptest"
    "os"
    "path/filepath"
    "strings"
    "testing"
)

const targetCreateTable = "DynamoDB_20120810.CreateTable"
const resourceInUseType = "com.amazonaws.dynamodb.v20120810#ResourceInUseException"

func newStore(t *testing.T) (*Store, string) {
    t.Helper()
    path := filepath.Join(t.TempDir(), "floci.json")
    store, err := OpenStore(path)
    if err != nil {
        t.Fatalf("OpenStore(%s): %v", path, err)
    }
    return store, path
}

func newTestServer(t *testing.T, store *Store) *httptest.Server {
    t.Helper()
    ts := httptest.NewServer(NewServer(store, "us-east-1", "000000000000"))
    t.Cleanup(ts.Close)
    return ts
}

// post sends a DynamoDB JSON-protocol request and decodes the JSON response.
func post(t *testing.T, ts *httptest.Server, target string, body any) (int, map[string]any) {
    t.Helper()
    raw, err := json.Marshal(body)
    if err != nil {
        t.Fatalf("marshal request: %v", err)
    }
    req, err := http.NewRequest(http.MethodPost, ts.URL+"/", bytes.NewReader(raw))
    if err != nil {
        t.Fatalf("new request: %v", err)
    }
    req.Header.Set("Content-Type", "application/x-amz-json-1.0")
    req.Header.Set("X-Amz-Target", target)
    res, err := ts.Client().Do(req)
    if err != nil {
        t.Fatalf("do request: %v", err)
    }
    defer res.Body.Close()
    data, err := io.ReadAll(res.Body)
    if err != nil {
        t.Fatalf("read response: %v", err)
    }
    out := make(map[string]any)
    if len(data) > 0 {
        if err := json.Unmarshal(data, &out); err != nil {
            t.Fatalf("decode response %q: %v", data, err)
        }
    }
    return res.StatusCode, out
}

// keyTableBody builds a CreateTable request for a table keyed by "pk"
// (HASH, string) plus an optional "sk" (RANGE, number).
func keyTableBody(name string, withSortKey bool) map[string]any {
    defs := []map[string]string{{"AttributeName": "pk", "AttributeType": "S"}}
    keys := []map[string]string{{"AttributeName": "pk", "KeyType": "HASH"}}
    if withSortKey {
        defs = append(defs, map[string]string{"AttributeName": "sk", "AttributeType": "N"})
        keys = append(keys, map[string]string{"AttributeName": "sk", "KeyType": "RANGE"})
    }
    return map[string]any{
        "TableName":             name,
        "AttributeDefinitions":  defs,
        "KeySchema":             keys,
        "ProvisionedThroughput": map[string]any{"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    }
}

func TestCreateTableReturnsActiveTableDescription(t *testing.T) {
    store, _ := newStore(t)
    ts := newTestServer(t, store)

    code, out := post(t, ts, targetCreateTable, keyTableBody("users", true))
    if code != http.StatusOK {
        t.Fatalf("CreateTable status = %d, body = %v", code, out)
    }
    desc, ok := out["TableDescription"].(map[string]any)
    if !ok {
        t.Fatalf("TableDescription missing from response: %v", out)
    }
    if desc["TableName"] != "users" {
        t.Errorf("TableName = %v, want users", desc["TableName"])
    }
    if desc["TableStatus"] != "ACTIVE" {
        t.Errorf("TableStatus = %v, want ACTIVE", desc["TableStatus"])
    }
    wantArn := "arn:aws:dynamodb:us-east-1:000000000000:table/users"
    if desc["TableArn"] != wantArn {
        t.Errorf("TableArn = %v, want %s", desc["TableArn"], wantArn)
    }
    if id, ok := desc["TableId"].(string); !ok || len(id) != 36 {
        t.Errorf("TableId = %v, want a UUID string", desc["TableId"])
    }
    if cdt, ok := desc["CreationDateTime"].(float64); !ok || cdt <= 0 {
        t.Errorf("CreationDateTime = %v, want a positive epoch-seconds number", desc["CreationDateTime"])
    }
    if desc["ItemCount"] != float64(0) {
        t.Errorf("ItemCount = %v, want 0", desc["ItemCount"])
    }
    if desc["TableSizeBytes"] != float64(0) {
        t.Errorf("TableSizeBytes = %v, want 0", desc["TableSizeBytes"])
    }

    schema, ok := desc["KeySchema"].([]any)
    if !ok || len(schema) != 2 {
        t.Fatalf("KeySchema = %v, want pk(HASH)/sk(RANGE)", desc["KeySchema"])
    }
    pk, _ := schema[0].(map[string]any)
    sk, _ := schema[1].(map[string]any)
    if pk["AttributeName"] != "pk" || pk["KeyType"] != "HASH" {
        t.Errorf("partition key element = %v, want pk/HASH", pk)
    }
    if sk["AttributeName"] != "sk" || sk["KeyType"] != "RANGE" {
        t.Errorf("sort key element = %v, want sk/RANGE", sk)
    }

    defs, ok := desc["AttributeDefinitions"].([]any)
    if !ok || len(defs) != 2 {
        t.Fatalf("AttributeDefinitions = %v, want pk(S)/sk(N)", desc["AttributeDefinitions"])
    }
    pkDef, _ := defs[0].(map[string]any)
    skDef, _ := defs[1].(map[string]any)
    if pkDef["AttributeName"] != "pk" || pkDef["AttributeType"] != "S" {
        t.Errorf("pk definition = %v, want pk/S", pkDef)
    }
    if skDef["AttributeName"] != "sk" || skDef["AttributeType"] != "N" {
        t.Errorf("sk definition = %v, want sk/N", skDef)
    }

    throughput, ok := desc["ProvisionedThroughput"].(map[string]any)
    if !ok {
        t.Fatalf("ProvisionedThroughput missing: %v", desc)
    }
    if throughput["ReadCapacityUnits"] != float64(5) || throughput["WriteCapacityUnits"] != float64(5) {
        t.Errorf("ProvisionedThroughput = %v, want 5/5", throughput)
    }
    if throughput["NumberOfDecreasesToday"] != float64(0) {
        t.Errorf("NumberOfDecreasesToday = %v, want 0", throughput["NumberOfDecreasesToday"])
    }
    if _, present := desc["BillingModeSummary"]; present {
        t.Errorf("BillingModeSummary present for a provisioned table: %v", desc["BillingModeSummary"])
    }
}

func TestCreateTableHashKeyOnly(t *testing.T) {
    store, _ := newStore(t)
    ts := newTestServer(t, store)

    code, out := post(t, ts, targetCreateTable, keyTableBody("pk-only", false))
    if code != http.StatusOK {
        t.Fatalf("status = %d, body = %v", code, out)
    }
    desc := out["TableDescription"].(map[string]any)
    schema, ok := desc["KeySchema"].([]any)
    if !ok || len(schema) != 1 {
        t.Fatalf("KeySchema = %v, want a single HASH element", desc["KeySchema"])
    }
    pk, _ := schema[0].(map[string]any)
    if pk["AttributeName"] != "pk" || pk["KeyType"] != "HASH" {
        t.Errorf("key element = %v, want pk/HASH", pk)
    }
}

func TestCreateTableDuplicateNameResourceInUse(t *testing.T) {
    store, _ := newStore(t)
    ts := newTestServer(t, store)

    if code, out := post(t, ts, targetCreateTable, keyTableBody("dup-table", true)); code != http.StatusOK {
        t.Fatalf("first CreateTable status = %d, body = %v", code, out)
    }
    code, out := post(t, ts, targetCreateTable, keyTableBody("dup-table", true))
    if code != http.StatusBadRequest {
        t.Fatalf("duplicate CreateTable status = %d, want 400, body = %v", code, out)
    }
    if typ, _ := out["__type"].(string); typ != resourceInUseType {
        t.Errorf("__type = %q, want %q", typ, resourceInUseType)
    }
    if msg, _ := out["message"].(string); !strings.Contains(msg, "dup-table") {
        t.Errorf("message = %q, want it to name the existing table", msg)
    }
}

func TestCreateTablePersistsToStoreFileAndBlocksAfterRestart(t *testing.T) {
    path := filepath.Join(t.TempDir(), "data", "floci.json")

    store1, err := OpenStore(path)
    if err != nil {
        t.Fatalf("open store 1: %v", err)
    }
    ts1 := newTestServer(t, store1)

    if code, out := post(t, ts1, targetCreateTable, keyTableBody("persisted", true)); code != http.StatusOK {
        t.Fatalf("create on first boot status = %d, body = %v", code, out)
    }

    raw, err := os.ReadFile(path)
    if err != nil {
        t.Fatalf("read store file: %v", err)
    }
    var onDisk struct {
        Tables map[string]Table `json:"tables"`
    }
    if err := json.Unmarshal(raw, &onDisk); err != nil {
        t.Fatalf("store file %s is not valid JSON: %v", raw, err)
    }
    rec, ok := onDisk.Tables["persisted"]
    if !ok {
        t.Fatalf("store file does not contain table 'persisted': %s", raw)
    }
    if rec.TableName != "persisted" || rec.PartitionKey != "pk" || rec.PartitionKeyType != "S" {
        t.Errorf("stored pk schema wrong: %+v", rec)
    }
    if rec.SortKey != "sk" || rec.SortKeyType != "N" {
        t.Errorf("stored sk schema wrong: %+v", rec)
    }
    if rec.TableStatus != "ACTIVE" {
        t.Errorf("stored status = %q, want ACTIVE", rec.TableStatus)
    }

    // Simulate a restart: a fresh store opened from the same file must know
    // about the already-created table.
    store2, err := OpenStore(path)
    if err != nil {
        t.Fatalf("open store 2: %v", err)
    }
    ts2 := newTestServer(t, store2)

    code, out := post(t, ts2, targetCreateTable, keyTableBody("persisted", true))
    if code != http.StatusBadRequest {
        t.Fatalf("re-create after restart status = %d, want 400, body = %v", code, out)
    }
    if typ, _ := out["__type"].(string); typ != resourceInUseType {
        t.Errorf("__type after restart = %q, want %q", typ, resourceInUseType)
    }
    // Other tables remain creatable after the restart.
    if code, out := post(t, ts2, targetCreateTable, keyTableBody("fresh", false)); code != http.StatusOK {
        t.Fatalf("create new table after restart status = %d, body = %v", code, out)
    }
}

func TestCreateTableOnDemandBillingMode(t *testing.T) {
    store, _ := newStore(t)
    ts := newTestServer(t, store)

    body := keyTableBody("on-demand", true)
    delete(body, "ProvisionedThroughput")
    body["BillingMode"] = "PAY_PER_REQUEST"
    code, out := post(t, ts, targetCreateTable, body)
    if code != http.StatusOK {
        t.Fatalf("status = %d, body = %v", code, out)
    }
    desc := out["TableDescription"].(map[string]any)
    if _, present := desc["ProvisionedThroughput"]; present {
        t.Errorf("ProvisionedThroughput present for on-demand table: %v", desc["ProvisionedThroughput"])
    }
    summary, ok := desc["BillingModeSummary"].(map[string]any)
    if !ok {
        t.Fatalf("BillingModeSummary missing for on-demand table: %v", desc)
    }
    if summary["BillingMode"] != "PAY_PER_REQUEST" {
        t.Errorf("BillingMode = %v", summary["BillingMode"])
    }
    if v, ok := summary["LastUpdateToPayPerRequestDateTime"].(float64); !ok || v <= 0 {
        t.Errorf("LastUpdateToPayPerRequestDateTime = %v", summary["LastUpdateToPayPerRequestDateTime"])
    }

    // Zero-capacity throughput alongside on-demand billing is tolerated.
    body2 := keyTableBody("on-demand-zero", true)
    body2["BillingMode"] = "PAY_PER_REQUEST"
    body2["ProvisionedThroughput"] = map[string]any{"ReadCapacityUnits": 0, "WriteCapacityUnits": 0}
    if code, out := post(t, ts, targetCreateTable, body2); code != http.StatusOK {
        t.Fatalf("on-demand with zero throughput status = %d, body = %v", code, out)
    }
}

func TestCreateTableValidation(t *testing.T) {
    store, _ := newStore(t)
    ts := newTestServer(t, store)

    bad := []map[string]any{
        // Table name shorter than 3 characters.
        {"TableName": "ab"},
        // Illegal characters in table name.
        {"TableName": "bad/name"},
        // Missing HASH key.
        {
            "TableName":            "no-hash",
            "AttributeDefinitions": []map[string]string{{"AttributeName": "pk", "AttributeType": "S"}},
            "KeySchema":            []map[string]string{{"AttributeName": "pk", "KeyType": "RANGE"}},
        },
        // Key schema attribute missing from definitions.
        {
            "TableName":            "missing-def",
            "AttributeDefinitions": []map[string]string{{"AttributeName": "pk", "AttributeType": "S"}},
            "KeySchema": []map[string]string{
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            },
        },
        // Unsupported attribute type.
        {
            "TableName":            "bad-type",
            "AttributeDefinitions": []map[string]string{{"AttributeName": "pk", "AttributeType": "X"}},
            "KeySchema":            []map[string]string{{"AttributeName": "pk", "KeyType": "HASH"}},
        },
        // Unsupported billing mode.
        {
            "TableName":            "bad-mode",
            "AttributeDefinitions": []map[string]string{{"AttributeName": "pk", "AttributeType": "S"}},
            "KeySchema":            []map[string]string{{"AttributeName": "pk", "KeyType": "HASH"}},
            "BillingMode":          "WHATEVER",
        },
    }
    for i, body := range bad {
        code, out := post(t, ts, targetCreateTable, body)
        if code != http.StatusBadRequest {
            t.Errorf("case %d: status = %d, want 400, body = %v", i, code, out)
            continue
        }
        typ, _ := out["__type"].(string)
        if !strings.HasSuffix(typ, "ValidationException") {
            t.Errorf("case %d: __type = %q, want ValidationException", i, typ)
        }
    }
}

func TestCreateTableNameMatchingIsCaseSensitive(t *testing.T) {
    store, _ := newStore(t)
    ts := newTestServer(t, store)

    if code, out := post(t, ts, targetCreateTable, keyTableBody("Casey", false)); code != http.StatusOK {
        t.Fatalf("status = %d, body = %v", code, out)
    }
    if code, out := post(t, ts, targetCreateTable, keyTableBody("casey", false)); code != http.StatusOK {
        t.Fatalf("creating a case-variant name status = %d, body = %v (table names are case-sensitive)", code, out)
    }
}

func TestUnsupportedOperationFails(t *testing.T) {
    store, _ := newStore(t)
    ts := newTestServer(t, store)
    code, out := post(t, ts, "DynamoDB_20120810.DescribeTable", map[string]any{})
    if code != http.StatusBadRequest {
        t.Fatalf("status = %d, want 400", code)
    }
    typ, _ := out["__type"].(string)
    if !strings.HasSuffix(typ, "UnknownOperationException") {
        t.Errorf("__type = %q, want UnknownOperationException", typ)
    }
}

func TestOpenStoreRejectsCorruptFile(t *testing.T) {
    path := filepath.Join(t.TempDir(), "floci.json")
    if err := os.WriteFile(path, []byte("{ this is not json"), 0o644); err != nil {
        t.Fatal(err)
    }
    if _, err := OpenStore(path); err == nil {
        t.Fatal("OpenStore succeeded on a corrupt store file, want error")
    }
}
