package ddb

import (
	"reflect"
	"testing"
	"time"
)

// --- error path: ResourceNotFoundException (ticket requirement) ------------

func TestDescribeTableUnknownTableIsResourceNotFound(t *testing.T) {
	svc := &Service{}

	resp, err := svc.DescribeTable([]byte(`{"TableName":"ghost"}`))
	if resp != nil || err == nil {
		t.Fatalf("want fault, got resp=%v err=%v", resp, err)
	}

	fault, ok := err.(*ddbFault)
	if !ok {
		t.Fatalf("want *ddbFault, got %T", err)
	}
	if fault.code != "ResourceNotFoundException" {
		t.Fatalf("code = %q", fault.code)
	}
	if fault.wireType != "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException" {
		t.Fatalf("wire type = %q", fault.wireType)
	}
	if fault.httpStatus != 400 {
		t.Fatalf("status = %d", fault.httpStatus)
	}
	if want := "Requested resource not found: Table: ghost not found"; fault.message != want {
		t.Fatalf("message = %q, want %q", fault.message, want)
	}
}

func TestDescribeTableRequestValidation(t *testing.T) {
	svc := &Service{}

	if _, err := svc.DescribeTable([]byte(`not-json`)); err == nil {
		t.Fatal("malformed payload must be rejected")
	}
	_, err := svc.DescribeTable([]byte(`{}`))
	if err == nil {
		t.Fatal("missing TableName must be rejected")
	}
	if f, ok := err.(*ddbFault); !ok || f.code != "SerializationException" {
		t.Fatalf("want SerializationException, got %v", err)
	}
}

// --- happy path via reflect-compatible probe store shapes ------------------

type describeProbeKey struct {
	AttributeName string
	KeyType       string
}

type describeProbeItem struct {
	Attrs     map[string]any
	SizeBytes int64
}

type describeProbeTable struct {
	Name        string
	Status      string
	CreatedAt   time.Time
	BillingMode string
	KeySchema   []describeProbeKey
	Items       map[string]*describeProbeItem
}

type describeProbeService struct {
	tables map[string]*describeProbeTable
}

func TestDescribeTableRenderDescription(t *testing.T) {
	tbl := &describeProbeTable{
		Name:        "music",
		Status:      "ACTIVE",
		CreatedAt:   time.Unix(1716700000, 0),
		BillingMode: "PAY_PER_REQUEST",
		KeySchema: []describeProbeKey{
			{AttributeName: "Artist", KeyType: "HASH"},
			{AttributeName: "Song", KeyType: "RANGE"},
		},
		Items: map[string]*describeProbeItem{
			"artist|one": {Attrs: map[string]any{"Artist": map[string]any{"S": "Miles"}}, SizeBytes: 120},
			"artist|two": {Attrs: map[string]any{"Artist": map[string]any{"S": "Coltrane"}}, SizeBytes: 340},
		},
	}

	resp, err := DescribeTableOp(&describeProbeService{tables: map[string]*describeProbeTable{"music": tbl}},
		[]byte(`{"TableName":"music"}`))
	if err != nil {
		t.Fatalf("unexpected fault: %v", err)
	}
	wire, ok := resp.(map[string]any)
	if !ok {
		t.Fatalf("response shape: %T", resp)
	}
	table, ok := wire["Table"].(map[string]any)
	if !ok {
		t.Fatalf("missing Table document: %#v", wire)
	}

	if table["TableName"] != "music" {
		t.Fatalf("TableName = %v", table["TableName"])
	}
	if table["TableStatus"] != "ACTIVE" {
		t.Fatalf("TableStatus = %v", table["TableStatus"])
	}
	if got, _ := table["BillingModeSummary"].(map[string]any)["BillingMode"]; got != "PAY_PER_REQUEST" {
		t.Fatalf("BillingMode = %v", got)
	}
	wantKeys := []describeTableKey{{"Artist", "HASH"}, {"Song", "RANGE"}}
	if !reflect.DeepEqual(table["KeySchema"], wantKeys) {
		t.Fatalf("KeySchema = %#v", table["KeySchema"])
	}
	if table["CreationDateTime"] != 1716700000.0 {
		t.Fatalf("CreationDateTime = %v", table["CreationDateTime"])
	}
	if table["ItemCount"] != 2 {
		t.Fatalf("ItemCount = %v", table["ItemCount"])
	}
	if table["TableSizeBytes"] != int64(460) {
		t.Fatalf("TableSizeBytes = %v", table["TableSizeBytes"])
	}
}

type describeNestedTable struct {
	CreatedAt time.Time
	KeySchema []describeProbeKey
	Items     map[string]map[string]map[string]any // pk -> sk -> attrs
}

type describeNestedService struct {
	Tables map[string]*describeNestedTable
}

func TestDescribeTableDefaultsForMinimalStoreShape(t *testing.T) {
	tbl := &describeNestedTable{
		CreatedAt: time.Unix(1700000000, 0),
		KeySchema: []describeProbeKey{{AttributeName: "PK", KeyType: "HASH"}},
		Items: map[string]map[string]map[string]any{
			"pk#1": {"sk#1": {"S": "a"}, "sk#2": {"S": "b"}},
			"pk#2": {"sk#3": {"S": "c"}},
		},
	}

	resp, err := DescribeTableOp(&describeNestedService{Tables: map[string]*describeNestedTable{"demo": tbl}},
		[]byte(`{"TableName":"demo"}`))
	if err != nil {
		t.Fatalf("unexpected fault: %v", err)
	}
	wire, ok := resp.(map[string]any)
	if !ok {
		t.Fatalf("response shape: %T", resp)
	}
	table, ok := wire["Table"].(map[string]any)
	if !ok {
		t.Fatalf("missing Table document: %#v", wire)
	}

	if table["TableStatus"] != "ACTIVE" {
		t.Fatalf("default TableStatus = %v", table["TableStatus"])
	}
	if got, _ := table["BillingModeSummary"].(map[string]any)["BillingMode"]; got != "PROVISIONED" {
		t.Fatalf("default BillingMode = %v", got)
	}
	if table["ItemCount"] != 3 {
		t.Fatalf("ItemCount = %v, want 3", table["ItemCount"])
	}
	if table["CreationDateTime"] != 1700000000.0 {
		t.Fatalf("CreationDateTime = %v", table["CreationDateTime"])
	}
	if size, _ := table["TableSizeBytes"].(int64); size <= 0 {
		t.Fatalf("TableSizeBytes = %v", table["TableSizeBytes"])
	}
	keys, _ := table["KeySchema"].([]describeTableKey)
	if len(keys) != 1 || keys[0].KeyType != "HASH" || keys[0].AttributeName != "PK" {
		t.Fatalf("KeySchema = %#v", keys)
	}
}
