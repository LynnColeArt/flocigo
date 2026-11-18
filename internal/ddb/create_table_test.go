package ddb

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// createTableCall posts a CreateTable request against the service.
func createTableCall(t *testing.T, svc *TableService, payload string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
	req.Header.Set("Content-Type", ddbContentTypeJSON)
	req.Header.Set("X-Amz-Target", ddbTargetPrefix+"CreateTable")
	rr := httptest.NewRecorder()
	svc.ServeHTTP(rr, req)
	return rr
}

// payloadCreate builds a CreateTable body for a table keyed by pk (+sk).
func payloadCreate(name, pk, sk, billing string) string {
	ads := fmt.Sprintf(`[{"AttributeName":"%s","AttributeType":"S"}]`, pk)
	keys := fmt.Sprintf(`[{"AttributeName":"%s","KeyType":"HASH"}]`, pk)
	if sk != "" {
		ads = fmt.Sprintf(`[{"AttributeName":"%s","AttributeType":"S"},{"AttributeName":"%s","AttributeType":"S"}]`, pk, sk)
		keys = fmt.Sprintf(`[{"AttributeName":"%s","KeyType":"HASH"},{"AttributeName":"%s","KeyType":"RANGE"}]`, pk, sk)
	}
	if billing == "" {
		billing = BillingModePayPerRequest
	}
	return fmt.Sprintf(`{"TableName":%q,"AttributeDefinitions":%s,"KeySchema":%s,"BillingMode":"%s"}`, name, ads, keys, billing)
}

func TestCreateTableActiveWireDescription(t *testing.T) {
	svc, err := NewTableService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rr := createTableCall(t, svc, payloadCreate("Games", "gameId", "round", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("CreateTable status = %d, body: %s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); ct != ddbContentTypeJSON {
		t.Errorf("Content-Type = %q, want %q", ct, ddbContentTypeJSON)
	}
	var resp struct {
		TableDescription tableDescription `json:"TableDescription"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	d := resp.TableDescription
	if d.TableName != "Games" {
		t.Errorf("TableName = %q", d.TableName)
	}
	if d.TableStatus != StatusActive {
		t.Errorf("TableStatus = %q, want ACTIVE", d.TableStatus)
	}
	if d.CreationDateTime <= 0 {
		t.Errorf("CreationDateTime = %v", d.CreationDateTime)
	}
	wantKeys := []KeySchemaElement{
		{AttributeName: "gameId", KeyType: "HASH"},
		{AttributeName: "round", KeyType: "RANGE"},
	}
	if !reflect.DeepEqual(d.KeySchema, wantKeys) {
		t.Errorf("KeySchema = %+v, want %+v", d.KeySchema, wantKeys)
	}
	wantAttrs := []AttributeDefinition{
		{AttributeName: "gameId", AttributeType: "S"},
		{AttributeName: "round", AttributeType: "S"},
	}
	if !reflect.DeepEqual(d.AttributeDefinitions, wantAttrs) {
		t.Errorf("AttributeDefinitions = %+v, want %+v", d.AttributeDefinitions, wantAttrs)
	}
	if !strings.HasSuffix(d.TableArn, ":table/Games") {
		t.Errorf("TableArn = %q", d.TableArn)
	}
	if d.TableId == "" {
		t.Error("TableId is empty")
	}
	if d.ItemCount != 0 || d.TableSizeBytes != 0 {
		t.Errorf("ItemCount = %d, TableSizeBytes = %d", d.ItemCount, d.TableSizeBytes)
	}
	if d.BillingModeSummary == nil || d.BillingModeSummary.BillingMode != BillingModePayPerRequest {
		t.Errorf("BillingModeSummary = %+v", d.BillingModeSummary)
	}
	if d.ProvisionedThroughput == nil || d.ProvisionedThroughput.ReadCapacityUnits != 0 || d.ProvisionedThroughput.WriteCapacityUnits != 0 || d.ProvisionedThroughput.NumberOfDecreasesToday != 0 {
		t.Errorf("ProvisionedThroughput = %+v", d.ProvisionedThroughput)
	}

	stored, ok := svc.Store().GetTable("Games")
	if !ok {
		t.Fatal("Games not found in the store")
	}
	if stored.HashKey() != "gameId" || stored.SK != "round" || stored.Status != StatusActive {
		t.Errorf("stored table = pk %q, sk %q, status %q", stored.HashKey(), stored.SK, stored.Status)
	}
	if key := stored.ItemKey("g1", "r2"); key != "g1#r2" {
		t.Errorf("ItemKey = %q, want g1#r2", key)
	}
}

func TestCreateTableDuplicateReturnsResourceInUse(t *testing.T) {
	svc, err := NewTableService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if rr := createTableCall(t, svc, payloadCreate("Games", "pk", "", "")); rr.Code != http.StatusOK {
		t.Fatalf("first create status = %d, body: %s", rr.Code, rr.Body.String())
	}
	rr := createTableCall(t, svc, payloadCreate("Games", "pk", "", ""))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("duplicate status = %d, body: %s", rr.Code, rr.Body.String())
	}
	if et := rr.Header().Get("X-Amzn-ErrorType"); et != "ResourceInUseException" {
		t.Errorf("X-Amzn-ErrorType = %q", et)
	}
	var wire struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &wire); err != nil {
		t.Fatal(err)
	}
	if wire.Type != "com.amazonaws.dynamodb.v20120810#ResourceInUseException" {
		t.Errorf("__type = %q", wire.Type)
	}
	if wire.Message != "Table already exists: Games" {
		t.Errorf("message = %q", wire.Message)
	}
}

func TestCreateTableStoreFileHoldsPKSKSchema(t *testing.T) {
	dir := t.TempDir()
	svc, err := NewTableService(dir)
	if err != nil {
		t.Fatal(err)
	}
	if rr := createTableCall(t, svc, payloadCreate("Sessions", "pk", "sk", "")); rr.Code != http.StatusOK {
		t.Fatalf("create status = %d, body: %s", rr.Code, rr.Body.String())
	}
	raw, err := os.ReadFile(filepath.Join(dir, "ddb", "tables.json"))
	if err != nil {
		t.Fatalf("read store file: %v", err)
	}
	var file tableStoreFile
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("decode store file: %v", err)
	}
	stored := file.Tables["Sessions"]
	if stored == nil {
		t.Fatal("Sessions missing from the store file")
	}
	if stored.PK != "pk" || stored.SK != "sk" {
		t.Errorf("pk/sk in store file = %q/%q, want pk/sk", stored.PK, stored.SK)
	}
	if stored.Status != StatusActive {
		t.Errorf("status in store file = %q", stored.Status)
	}
	if !strings.Contains(string(raw), "\"keySchema\"") {
		t.Errorf("store file missing keySchema: %s", raw)
	}

	// A second service (emulator restart) must reload the table from disk.
	restarted, err := NewTableService(dir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	if _, ok := restarted.Store().GetTable("Sessions"); !ok {
		t.Fatal("table was not reloaded from the store file")
	}
	rr := createTableCall(t, restarted, payloadCreate("Sessions", "pk", "sk", ""))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("duplicate after restart status = %d, body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "ResourceInUseException") {
		t.Errorf("body = %s", rr.Body.String())
	}
}

func TestCreateTableValidationErrors(t *testing.T) {
	svc, err := NewTableService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		payload string
	}{
		{"too short table name", payloadCreate("ab", "pk", "", "")},
		{"bad attribute type", `{"TableName":"Valid1","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"X"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}]}`},
		{"missing attribute definition", `{"TableName":"Valid1","AttributeDefinitions":[],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}]}`},
		{"first key must be HASH", `{"TableName":"Valid1","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"RANGE"}]}`},
		{"invalid billing mode", `{"TableName":"Valid1","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"BillingMode":"FREE"}`},
	}
	for _, tc := range cases {
		rr := createTableCall(t, svc, tc.payload)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400 (body %s)", tc.name, rr.Code, rr.Body.String())
			continue
		}
		var wire struct {
			Type string `json:"__type"`
		}
		if err := json.Unmarshal(rr.Body.Bytes(), &wire); err != nil {
			t.Errorf("%s: decode error body: %v", tc.name, err)
			continue
		}
		if !strings.HasSuffix(wire.Type, "#ValidationException") {
			t.Errorf("%s: __type = %q, want ValidationException", tc.name, wire.Type)
		}
	}
}

func TestCreateTableProvisionedThroughputEcho(t *testing.T) {
	svc, err := NewTableService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	payload := `{"TableName":"Provisioned1","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":7}}`
	rr := createTableCall(t, svc, payload)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rr.Code, rr.Body.String())
	}
	var resp struct {
		TableDescription tableDescription `json:"TableDescription"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	d := resp.TableDescription
	if d.BillingModeSummary != nil {
		t.Errorf("BillingModeSummary = %+v, want omitted", d.BillingModeSummary)
	}
	if d.ProvisionedThroughput == nil || d.ProvisionedThroughput.ReadCapacityUnits != 5 || d.ProvisionedThroughput.WriteCapacityUnits != 7 {
		t.Errorf("ProvisionedThroughput = %+v", d.ProvisionedThroughput)
	}
}

func TestCreateTableWithGlobalSecondaryIndex(t *testing.T) {
	svc, err := NewTableService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	payload := `{"TableName":"Orders1","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"gsi1pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"GlobalSecondaryIndexes":[{"IndexName":"byGsi","KeySchema":[{"AttributeName":"gsi1pk","KeyType":"HASH"}],"Projection":{"ProjectionType":"ALL"}}],"BillingMode":"PAY_PER_REQUEST"}`
	rr := createTableCall(t, svc, payload)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rr.Code, rr.Body.String())
	}
	var resp struct {
		TableDescription tableDescription `json:"TableDescription"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	gsis := resp.TableDescription.GlobalSecondaryIndexes
	if len(gsis) != 1 {
		t.Fatalf("GlobalSecondaryIndexes = %+v", gsis)
	}
	g := gsis[0]
	if g.IndexName != "byGsi" || g.IndexStatus != StatusActive || g.IndexArn == "" {
		t.Errorf("GSI = %+v", g)
	}
	if g.Projection == nil || g.Projection.ProjectionType != "ALL" {
		t.Errorf("GSI Projection = %+v", g.Projection)
	}
}

func TestStoreCreateTableDuplicate(t *testing.T) {
	store, err := OpenTableStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	table := &StoredTable{
		Name:                 "dup1",
		Status:               StatusActive,
		PK:                   "pk",
		KeySchema:            []KeySchemaElement{{AttributeName: "pk", KeyType: KeyTypeHash}},
		AttributeDefinitions: []AttributeDefinition{{AttributeName: "pk", AttributeType: "S"}},
		BillingMode:          BillingModePayPerRequest,
		Items:                map[string]json.RawMessage{},
	}
	if _, err := store.CreateTable(table); err != nil {
		t.Fatalf("first CreateTable: %v", err)
	}
	if _, err := store.CreateTable(table); !errors.Is(err, ErrResourceInUse) {
		t.Fatalf("second CreateTable err = %v, want ErrResourceInUse", err)
	}
	if tables := store.ListTables(); len(tables) != 1 || tables[0].Name != "dup1" {
		t.Errorf("ListTables = %+v", tables)
	}
}

func TestUnknownTargetReturnsUnknownOperation(t *testing.T) {
	svc, err := NewTableService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"TableName":"Whatever1"}`))
	req.Header.Set("X-Amz-Target", ddbTargetPrefix+"DescribeTable")
	rr := httptest.NewRecorder()
	svc.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rr.Code)
	}
	var wire struct {
		Type string `json:"__type"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &wire); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(wire.Type, "UnknownOperationException") {
		t.Errorf("__type = %q", wire.Type)
	}
}
