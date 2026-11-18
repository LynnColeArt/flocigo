package flocigo

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

// doWire posts a JSON request body with the given X-Amz-Target and returns
// the recorded response.
func doWire(t *testing.T, srv *Server, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set(headerAmznTarget, target)
	req.Header.Set("Content-Type", contentTypeJSON)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec
}

// seedTable registers a music-style table (HASH id, RANGE album).
func seedTable(t *testing.T, srv *Server, name string, payPerRequest bool) *Table {
	t.Helper()
	tbl := NewTable(name,
		[]KeySchemaElement{
			{AttributeName: "id", KeyType: KeyTypeHash},
			{AttributeName: "album", KeyType: KeyTypeRange},
		},
		[]AttributeDefinition{
			{AttributeName: "id", AttributeType: AttributeTypeString},
			{AttributeName: "album", AttributeType: AttributeTypeString},
		},
	)
	if payPerRequest {
		tbl.BillingMode = BillingModePayPerRequest
		tbl.LastUpdateToPayPerRequest = tbl.CreationTime
	}
	if err := srv.Tables.Create(tbl); err != nil {
		t.Fatalf("Create(%q): %v", name, err)
	}
	return tbl
}

func TestDescribeTableResourceNotFound(t *testing.T) {
	srv := NewServer()

	_, err := srv.DescribeTable(context.Background(), "ghost")
	var rnfe *ResourceNotFoundException
	if !errors.As(err, &rnfe) {
		t.Fatalf("DescribeTable(missing) error = %v, want *ResourceNotFoundException", err)
	}
	if rnfe.TableName != "ghost" {
		t.Fatalf("TableName = %q, want %q", rnfe.TableName, "ghost")
	}
	const wantMsg = "Requested resource not found: Table: ghost not found"
	if rnfe.Error() != wantMsg {
		t.Fatalf("message = %q, want %q", rnfe.Error(), wantMsg)
	}

	rec := doWire(t, srv, targetDescribeTable, `{"TableName":"ghost"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get(headerAmznErrorType); got != "ResourceNotFoundException" {
		t.Fatalf("x-Amzn-ErrorType = %q, want %q", got, "ResourceNotFoundException")
	}
	if got := rec.Header().Get(headerAmznRequestID); got == "" {
		t.Fatal("X-Amzn-RequestId header missing")
	}
	if got := rec.Header().Get("Content-Type"); got != contentTypeJSON {
		t.Fatalf("Content-Type = %q, want %q", got, contentTypeJSON)
	}
	var body wireErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v body = %s", err, rec.Body.String())
	}
	if body.Type != wireTypeResourceNotFound {
		t.Fatalf("__type = %q, want %q", body.Type, wireTypeResourceNotFound)
	}
	if body.Message != wantMsg {
		t.Fatalf("message = %q, want %q", body.Message, wantMsg)
	}
}

func TestDescribeTableProvisioned(t *testing.T) {
	srv := NewServer()
	tbl := seedTable(t, srv, "music", false)

	mustPut := func(item Item) {
		t.Helper()
		if err := srv.Tables.Put("music", item); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	mustPut(Item{"id": StringAttr("a"), "album": StringAttr("cool album")})                                     // 18 bytes
	mustPut(Item{"id": StringAttr("b"), "album": StringAttr("zz")})                                              // 10 bytes
	mustPut(Item{"id": StringAttr("b"), "album": StringAttr("zz"), "year": NumberAttr("2020")}) // replaces #2, 18 bytes

	rec := doWire(t, srv, targetDescribeTable, `{"TableName":"music"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var out struct {
		Table TableDescription `json:"Table"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v body = %s", err, rec.Body.String())
	}
	d := out.Table

	if d.TableName != "music" {
		t.Fatalf("TableName = %q, want %q", d.TableName, "music")
	}
	if d.TableStatus != TableStatusActive {
		t.Fatalf("TableStatus = %q, want %q", d.TableStatus, TableStatusActive)
	}
	if d.CreationDateTime != tbl.CreationTime {
		t.Fatalf("CreationDateTime = %v, want %v", d.CreationDateTime, tbl.CreationTime)
	}
	if d.TableId == "" || d.TableId != tbl.ID {
		t.Fatalf("TableId = %q, want %q", d.TableId, tbl.ID)
	}
	wantArn := "arn:aws:dynamodb:us-east-1:123456789012:table/music"
	if d.TableArn != wantArn {
		t.Fatalf("TableArn = %q, want %q", d.TableArn, wantArn)
	}
	if d.ItemCount != 2 {
		t.Fatalf("ItemCount = %d, want 2", d.ItemCount)
	}
	if d.TableSizeBytes != 36 {
		t.Fatalf("TableSizeBytes = %d, want 36", d.TableSizeBytes)
	}
	wantKeys := []KeySchemaElement{
		{AttributeName: "id", KeyType: KeyTypeHash},
		{AttributeName: "album", KeyType: KeyTypeRange},
	}
	if !reflect.DeepEqual(d.KeySchema, wantKeys) {
		t.Fatalf("KeySchema = %+v, want %+v", d.KeySchema, wantKeys)
	}
	wantDefs := []AttributeDefinition{
		{AttributeName: "id", AttributeType: AttributeTypeString},
		{AttributeName: "album", AttributeType: AttributeTypeString},
	}
	if !reflect.DeepEqual(d.AttributeDefinitions, wantDefs) {
		t.Fatalf("AttributeDefinitions = %+v, want %+v", d.AttributeDefinitions, wantDefs)
	}
	if d.BillingModeSummary == nil || d.BillingModeSummary.BillingMode != BillingModeProvisioned {
		t.Fatalf("BillingModeSummary = %+v, want PROVISIONED", d.BillingModeSummary)
	}
	pt := d.ProvisionedThroughput
	if pt == nil || pt.ReadCapacityUnits != 5 || pt.WriteCapacityUnits != 5 || pt.NumberOfDecreasesToday != 0 {
		t.Fatalf("ProvisionedThroughput = %+v, want 0/5/5", pt)
	}
}

func TestDescribeTablePayPerRequest(t *testing.T) {
	srv := NewServer()
	tbl := seedTable(t, srv, "on-demand", true)

	rec := doWire(t, srv, targetDescribeTable, `{"TableName":"on-demand"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var raw struct {
		Table map[string]json.RawMessage `json:"Table"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode: %v body = %s", err, rec.Body.String())
	}
	if _, present := raw.Table["ProvisionedThroughput"]; present {
		t.Fatalf("ProvisionedThroughput must be absent for PAY_PER_REQUEST tables, body = %s", rec.Body.String())
	}
	var summary BillingModeSummary
	if err := json.Unmarshal(raw.Table["BillingModeSummary"], &summary); err != nil {
		t.Fatalf("decode BillingModeSummary: %v", err)
	}
	if summary.BillingMode != BillingModePayPerRequest {
		t.Fatalf("BillingMode = %q, want %q", summary.BillingMode, BillingModePayPerRequest)
	}
	if summary.LastUpdateToPayPerRequestDateTime == nil || *summary.LastUpdateToPayPerRequestDateTime != tbl.CreationTime {
		t.Fatalf("LastUpdateToPayPerRequestDateTime = %v, want %v", summary.LastUpdateToPayPerRequestDateTime, tbl.CreationTime)
	}
	var wrapped struct {
		Table TableDescription `json:"Table"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &wrapped); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if wrapped.Table.ItemCount != 0 || wrapped.Table.TableSizeBytes != 0 {
		t.Fatalf("new table must report zero items/size, got %+v", wrapped.Table)
	}
}

func TestDescribeTableValidation(t *testing.T) {
	srv := NewServer()

	rec := doWire(t, srv, targetDescribeTable, `{}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if got := rec.Header().Get(headerAmznErrorType); got != "ValidationException" {
		t.Fatalf("x-Amzn-ErrorType = %q, want %q", got, "ValidationException")
	}
	var body wireErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v body = %s", err, rec.Body.String())
	}
	if body.Type != wireTypeValidation {
		t.Fatalf("__type = %q, want %q", body.Type, wireTypeValidation)
	}
	if !strings.Contains(body.Message, "1 validation error detected") {
		t.Fatalf("message = %q, want a validation error message", body.Message)
	}

	emptyName := doWire(t, srv, targetDescribeTable, `{"TableName":""}`)
	if emptyName.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", emptyName.Code, http.StatusBadRequest)
	}
	var emptyBody wireErrorBody
	if err := json.Unmarshal(emptyName.Body.Bytes(), &emptyBody); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if emptyBody.Type != wireTypeValidation {
		t.Fatalf("__type = %q, want %q", emptyBody.Type, wireTypeValidation)
	}
}

func TestUnknownOperation(t *testing.T) {
	srv := NewServer()
	rec := doWire(t, srv, "DynamoDB_20120810.ListTables", "{}")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	var body wireErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v body = %s", err, rec.Body.String())
	}
	if body.Type != wireTypeUnknownOperation {
		t.Fatalf("__type = %q, want %q", body.Type, wireTypeUnknownOperation)
	}
	if body.Message != "" {
		t.Fatalf("message = %q, want empty for UnknownOperationException", body.Message)
	}
}