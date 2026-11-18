// Package flocigo is a Go port of the Floci Amazon DynamoDB emulator.
//
// This file implements ticket flocigo/ticket-031: the
// DynamoDB_20120810.DescribeTable operation over the AWS JSON 1.0 wire
// protocol (request {"TableName": ...}, response {"Table": {...}}),
// including its ResourceNotFoundException error path.
package flocigo

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Operation targets served by this port.
const (
	apiTargetPrefix     = "DynamoDB_20120810."
	targetDescribeTable = apiTargetPrefix + "DescribeTable"
)

// Wire protocol constants (AWS JSON 1.0, as spoken by DynamoDB).
const (
	contentTypeJSON     = "application/x-amz-json-1.0"
	headerAmznTarget    = "X-Amz-Target"
	headerAmznRequestID = "X-Amzn-RequestId"
	headerAmznErrorType = "X-Amzn-ErrorType"

	maxRequestBodySize = 1 << 20 // DynamoDB's 1 MiB per-request payload limit

	defaultRegion    = "us-east-1"
	defaultAccountID = "123456789012"
)

// Table status values (TableDescription.TableStatus).
const (
	TableStatusCreating = "CREATING"
	TableStatusUpdating = "UPDATING"
	TableStatusDeleting = "DELETING"
	TableStatusActive   = "ACTIVE"
)

// Read/write capacity modes (BillingModeSummary.BillingMode).
const (
	BillingModeProvisioned   = "PROVISIONED"
	BillingModePayPerRequest = "PAY_PER_REQUEST"
)

// Primary key types (KeySchemaElement.KeyType).
const (
	KeyTypeHash  = "HASH"
	KeyTypeRange = "RANGE"
)

// Scalar attribute types (AttributeDefinition.AttributeType).
const (
	AttributeTypeString = "S"
	AttributeTypeNumber = "N"
	AttributeTypeBinary = "B"
)

// AWS JSON 1.0 error __type values, exactly as the service emits them.
const (
	wireTypeResourceNotFound = "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException"
	wireTypeValidation       = "com.amazonaws.dynamodb.v20120810#ValidationException"
	wireTypeSerialization    = "com.amazon.coral.service#SerializationException"
	wireTypeInternalFailure  = "com.amazon.coral.service#InternalFailure"
	wireTypeUnknownOperation = "com.amazon.coral.service#UnknownOperationException"
)

// KeySchemaElement names one attribute of a table's primary key.
type KeySchemaElement struct {
	AttributeName string `json:"AttributeName"`
	KeyType       string `json:"KeyType"` // HASH or RANGE
}

// AttributeDefinition declares the data type of one key attribute.
type AttributeDefinition struct {
	AttributeName string `json:"AttributeName"`
	AttributeType string `json:"AttributeType"` // S, N or B
}

// BillingModeSummary reports a table's read/write capacity mode.
type BillingModeSummary struct {
	BillingMode                       string   `json:"BillingMode"`
	LastUpdateToPayPerRequestDateTime *float64 `json:"LastUpdateToPayPerRequestDateTime,omitempty"`
}

// ProvisionedThroughputDescription reports reserved capacity settings.
type ProvisionedThroughputDescription struct {
	NumberOfDecreasesToday int64 `json:"NumberOfDecreasesToday"`
	ReadCapacityUnits      int64 `json:"ReadCapacityUnits"`
	WriteCapacityUnits     int64 `json:"WriteCapacityUnits"`
}

// TableDescription is the DescribeTable response payload. Members are ordered
// alphabetically so that raw wire bytes line up with the service's output.
type TableDescription struct {
	AttributeDefinitions  []AttributeDefinition             `json:"AttributeDefinitions"`
	BillingModeSummary    *BillingModeSummary               `json:"BillingModeSummary,omitempty"`
	CreationDateTime      float64                           `json:"CreationDateTime"`
	ItemCount             int64                             `json:"ItemCount"`
	KeySchema             []KeySchemaElement                `json:"KeySchema"`
	ProvisionedThroughput *ProvisionedThroughputDescription `json:"ProvisionedThroughput,omitempty"`
	TableArn              string                            `json:"TableArn"`
	TableId               string                            `json:"TableId"`
	TableName             string                            `json:"TableName"`
	TableSizeBytes        int64                             `json:"TableSizeBytes"`
	TableStatus           string                            `json:"TableStatus"`
}

// AttributeValue is a stored attribute value tagged with its DynamoDB type
// (S, N, B, SS, NS, BS, L, M, BOOL or NULL).
type AttributeValue struct {
	Type string
	S    string
	N    string
	B    []byte
	SS   []string
	NS   []string
	BS   [][]byte
	L    []*AttributeValue
	M    map[string]*AttributeValue
	BOOL bool
	NULL bool
}

// Item is a stored item keyed by attribute name.
type Item map[string]*AttributeValue

// Constructors for the most common attribute value shapes.
func StringAttr(s string) *AttributeValue { return &AttributeValue{Type: AttributeTypeString, S: s} }
func NumberAttr(n string) *AttributeValue { return &AttributeValue{Type: AttributeTypeNumber, N: n} }
func BinaryAttr(b []byte) *AttributeValue { return &AttributeValue{Type: AttributeTypeBinary, B: b} }
func BoolAttr(v bool) *AttributeValue     { return &AttributeValue{Type: "BOOL", BOOL: v} }
func NullAttr() *AttributeValue           { return &AttributeValue{Type: "NULL", NULL: true} }

// Table is the emulator's internal table record. Its descriptive fields are
// written at CreateTable time and are immutable afterwards; DescribeTable
// projects them verbatim into the response.
type Table struct {
	ID                        string
	Name                      string
	Status                    string
	BillingMode               string
	ReadCapacityUnits         int64
	WriteCapacityUnits        int64
	LastUpdateToPayPerRequest float64 // epoch seconds; 0 = never on-demand
	CreationTime              float64 // epoch seconds, millisecond precision
	KeySchema                 []KeySchemaElement
	AttributeDefinitions      []AttributeDefinition

	mu        sync.RWMutex
	items     map[string]Item
	sizeBytes int64
}

// NewTable builds an ACTIVE PROVISIONED table with the service's default
// throughput (5 RCU / 5 WCU), ready to be registered with a TableStore.
func NewTable(name string, keySchema []KeySchemaElement, attributeDefinitions []AttributeDefinition) *Table {
	return &Table{
		ID:                   newUUID(),
		Name:                 name,
		Status:               TableStatusActive,
		BillingMode:          BillingModeProvisioned,
		ReadCapacityUnits:    5,
		WriteCapacityUnits:   5,
		CreationTime:         epochSeconds(),
		KeySchema:            cloneKeySchema(keySchema),
		AttributeDefinitions: cloneAttributeDefinitions(attributeDefinitions),
	}
}

// ItemCount is the number of stored items.
func (t *Table) ItemCount() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return int64(len(t.items))
}

// SizeBytes is the aggregate size of all stored items, in bytes.
func (t *Table) SizeBytes() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.sizeBytes
}

// Put upserts item into the table, maintaining the item count and the
// aggregate size that DescribeTable reports.
func (t *Table) Put(item Item) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.items == nil {
		t.items = make(map[string]Item)
	}
	key := t.keyOf(item)
	if old, ok := t.items[key]; ok {
		t.sizeBytes -= itemSizeBytes(old)
	}
	t.items[key] = item
	t.sizeBytes += itemSizeBytes(item)
}

// keyOf builds the canonical storage key for item from the primary key
// schema, which is immutable once the table is registered.
func (t *Table) keyOf(item Item) string {
	var b strings.Builder
	for _, ks := range t.KeySchema {
		b.WriteByte('#')
		av := item[ks.AttributeName]
		switch {
		case av == nil:
			b.WriteString("nil:")
		case av.Type == AttributeTypeString:
			b.WriteString("S:")
			b.WriteString(av.S)
		case av.Type == AttributeTypeNumber:
			b.WriteString("N:")
			b.WriteString(canonicalNumber(av.N))
		case av.Type == AttributeTypeBinary:
			b.WriteString("B:")
			b.Write(av.B)
		default:
			b.WriteString(av.Type)
			b.WriteByte(':')
		}
	}
	return b.String()
}

// keySchemaSnapshot returns a lock-protected copy of the key schema.
func (t *Table) keySchemaSnapshot() []KeySchemaElement {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return cloneKeySchema(t.KeySchema)
}

// attributeDefinitionsSnapshot returns a lock-protected copy of the attribute
// definitions.
func (t *Table) attributeDefinitionsSnapshot() []AttributeDefinition {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return cloneAttributeDefinitions(t.AttributeDefinitions)
}

// TableStore is the emulator's registry of tables, safe for concurrent use.
type TableStore struct {
	mu     sync.RWMutex
	tables map[string]*Table
}

// NewTableStore returns an empty table registry.
func NewTableStore() *TableStore {
	return &TableStore{tables: make(map[string]*Table)}
}

// Get returns the table named name, or (nil, false) if it does not exist.
func (s *TableStore) Get(name string) (*Table, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tbl, ok := s.tables[name]
	return tbl, ok
}

// Create registers a table built by NewTable. Registering a duplicate name
// fails (the ResourceAlreadyExistsException case, owned by CreateTable).
func (s *TableStore) Create(tbl *Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tables[tbl.Name]; exists {
		return fmt.Errorf("dynamodb: table %q already exists", tbl.Name)
	}
	s.tables[tbl.Name] = tbl
	return nil
}

// Delete removes the named table and reports whether it existed.
func (s *TableStore) Delete(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tables[name]; !ok {
		return false
	}
	delete(s.tables, name)
	return true
}

// Put upserts item into the named table; a missing table yields
// *ResourceNotFoundException, mirroring PutItem's error path.
func (s *TableStore) Put(name string, item Item) error {
	tbl, ok := s.Get(name)
	if !ok {
		return &ResourceNotFoundException{TableName: name, Message: resourceNotFoundMessage(name)}
	}
	tbl.Put(item)
	return nil
}

// apiError lets the wire layer translate emulator errors into AWS JSON 1.0
// error envelopes without special-casing concrete error types.
type apiError interface {
	error
	HTTPStatus() int
	WireType() string
}

// ResourceNotFoundException mirrors the DynamoDB error of the same name,
// returned by operations against a table that does not exist.
type ResourceNotFoundException struct {
	TableName string
	Message   string
}

func (e *ResourceNotFoundException) Error() string    { return e.Message }
func (e *ResourceNotFoundException) HTTPStatus() int  { return http.StatusBadRequest }
func (e *ResourceNotFoundException) WireType() string { return wireTypeResourceNotFound }

// ValidationException mirrors the DynamoDB request validation error.
type ValidationException struct {
	Message string
}

func (e *ValidationException) Error() string    { return e.Message }
func (e *ValidationException) HTTPStatus() int  { return http.StatusBadRequest }
func (e *ValidationException) WireType() string { return wireTypeValidation }

// WireError is a generic protocol-level error carrying a full __type value,
// used for the Coral-level SerializationException and friends.
type WireError struct {
	Type    string // full AWS JSON 1.0 __type value
	Message string
	Status  int // 0 means 400
}

func (e *WireError) Error() string { return e.Message }
func (e *WireError) HTTPStatus() int {
	if e.Status == 0 {
		return http.StatusBadRequest
	}
	return e.Status
}
func (e *WireError) WireType() string { return e.Type }

// resourceNotFoundMessage reproduces the service's exact error text for a
// missing table.
func resourceNotFoundMessage(name string) string {
	return fmt.Sprintf("Requested resource not found: Table: %s not found", name)
}

// elementOverhead is DynamoDB's per-element size overhead for set members,
// list elements and map entries.
const elementOverhead = 3

// itemSizeBytes approximates the serialized size of an item: the sum of the
// UTF-8 length of every attribute name plus its value's size, the model
// DynamoDB uses for the TableSizeBytes sum.
func itemSizeBytes(item Item) int64 {
	var total int64
	for name, av := range item {
		total += int64(len(name)) + attributeValueSize(av)
	}
	return total
}

// attributeValueSize approximates one attribute value's size in bytes:
// strings count UTF-8 bytes, binary counts raw bytes, booleans and nulls
// count one byte, numbers count their decimal digits, and collections add
// three bytes of overhead per element or member.
func attributeValueSize(av *AttributeValue) int64 {
	if av == nil {
		return 1
	}
	switch av.Type {
	case AttributeTypeString:
		return nonZero(int64(len(av.S)))
	case AttributeTypeNumber:
		return nonZero(int64(len(av.N)))
	case AttributeTypeBinary:
		return nonZero(int64(len(av.B)))
	case "BOOL", "NULL":
		return 1
	case "SS":
		var total int64
		for _, s := range av.SS {
			total += elementOverhead + nonZero(int64(len(s)))
		}
		return total
	case "NS":
		var total int64
		for _, s := range av.NS {
			total += elementOverhead + nonZero(int64(len(s)))
		}
		return total
	case "BS":
		var total int64
		for _, b := range av.BS {
			total += elementOverhead + nonZero(int64(len(b)))
		}
		return total
	case "L":
		var total int64
		for _, e := range av.L {
			total += elementOverhead + attributeValueSize(e)
		}
		return total
	case "M":
		var total int64
		for k, v := range av.M {
			total += elementOverhead + int64(len(k)) + attributeValueSize(v)
		}
		return total
	default:
		return 1
	}
}

func nonZero(n int64) int64 {
	if n <= 0 {
		return 1
	}
	return n
}

// canonicalNumber normalizes a numeric attribute value so that numerically
// equal numbers (e.g. "01" and "1") share one storage key.
func canonicalNumber(n string) string {
	f, err := strconv.ParseFloat(n, 64)
	if err != nil {
		return n
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// Server exposes the ported DynamoDB operations over the AWS JSON 1.0 wire
// protocol. This ticket mounts DynamoDB_20120810.DescribeTable; later
// tickets add further targets to the same dispatch.
type Server struct {
	Tables    *TableStore
	Region    string
	AccountID string
}

// NewServer returns a Server with its own TableStore and default endpoint
// identity (region us-east-1, account 123456789012).
func NewServer() *Server {
	return &Server{
		Tables:    NewTableStore(),
		Region:    defaultRegion,
		AccountID: defaultAccountID,
	}
}

// DescribeTable returns the description of the named table, or
// *ResourceNotFoundException when no such table exists.
func (s *Server) DescribeTable(_ context.Context, tableName string) (*TableDescription, error) {
	tbl, ok := s.Tables.Get(tableName)
	if !ok {
		return nil, &ResourceNotFoundException{
			TableName: tableName,
			Message:   resourceNotFoundMessage(tableName),
		}
	}
	return s.tableDescription(tbl), nil
}

// tableDescription projects a stored table onto the DescribeTable response
// payload, deriving the billing-mode dependent blocks and the ARN.
func (s *Server) tableDescription(tbl *Table) *TableDescription {
	desc := &TableDescription{
		AttributeDefinitions: tbl.attributeDefinitionsSnapshot(),
		CreationDateTime:     tbl.CreationTime,
		ItemCount:            tbl.ItemCount(),
		KeySchema:            tbl.keySchemaSnapshot(),
		TableArn:             fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s", s.Region, s.AccountID, tbl.Name),
		TableId:              tbl.ID,
		TableName:            tbl.Name,
		TableSizeBytes:       tbl.SizeBytes(),
		TableStatus:          tbl.Status,
	}
	switch tbl.BillingMode {
	case BillingModePayPerRequest:
		lastUpdate := tbl.LastUpdateToPayPerRequest
		if lastUpdate == 0 {
			lastUpdate = tbl.CreationTime
		}
		desc.BillingModeSummary = &BillingModeSummary{
			BillingMode:                       BillingModePayPerRequest,
			LastUpdateToPayPerRequestDateTime: &lastUpdate,
		}
	default: // PROVISIONED and the legacy unset mode
		desc.BillingModeSummary = &BillingModeSummary{BillingMode: BillingModeProvisioned}
		desc.ProvisionedThroughput = &ProvisionedThroughputDescription{
			ReadCapacityUnits:  tbl.ReadCapacityUnits,
			WriteCapacityUnits: tbl.WriteCapacityUnits,
		}
	}
	return desc
}

// wireErrorBody is the AWS JSON 1.0 error envelope: the error code goes into
// "__type" and the human-readable text into "message".
type wireErrorBody struct {
	Type    string `json:"__type"`
	Message string `json:"message,omitempty"`
}

//ServeHTTP implements the AWS JSON 1.0 protocol entry point.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(headerAmznRequestID, newUUID())
	w.Header().Set("Content-Type", contentTypeJSON)
	switch r.Header.Get(headerAmznTarget) {
	case targetDescribeTable:
		s.handleDescribeTable(w, r)
	default:
		s.writeError(w, &WireError{Type: wireTypeUnknownOperation})
	}
}

// describeTableRequest is the DescribeTable request shape.
type describeTableRequest struct {
	TableName *string `json:"TableName"`
}

// describeTableResponse is the DescribeTable response envelope.
type describeTableResponse struct {
	Table *TableDescription `json:"Table"`
}

// handleDescribeTable validates the request, then answers either with
// {"Table": {...}} or with the ResourceNotFoundException error envelope.
func (s *Server) handleDescribeTable(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBodySize+1))
	if err != nil {
		s.writeError(w, &WireError{Type: wireTypeSerialization, Message: "Unexpected marshalling exception"})
		return
	}
	if len(body) > maxRequestBodySize {
		s.writeError(w, &WireError{Type: wireTypeSerialization, Message: "Request body exceeds the maximum allowed size"})
		return
	}
	var req describeTableRequest
	if err := json.Unmarshal(body, &req); err != nil {
		s.writeError(w, &WireError{Type: wireTypeSerialization, Message: fmt.Sprintf("Unexpected marshalling exception: %v", err)})
		return
	}
	switch {
	case req.TableName == nil:
		s.writeError(w, &ValidationException{Message: "1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null"})
	case *req.TableName == "":
		s.writeError(w, &ValidationException{Message: "1 validation error detected: Value '' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 1"})
	default:
		desc, derr := s.DescribeTable(r.Context(), *req.TableName)
		if derr != nil {
			s.writeAPIError(w, derr)
			return
		}
		out, merr := json.Marshal(describeTableResponse{Table: desc})
		if merr != nil {
			s.writeError(w, &WireError{Type: wireTypeInternalFailure, Status: http.StatusInternalServerError})
			return
		}
		_, _ = w.Write(out)
	}
}

// writeAPIError adapts any emulator error into the AWS JSON 1.0 envelope;
// errors that do not implement apiError fall back to InternalFailure.
func (s *Server) writeAPIError(w http.ResponseWriter, err error) {
	if apiErr, ok := err.(apiError); ok {
		s.writeError(w, apiErr)
		return
	}
	s.writeError(w, &WireError{Type: wireTypeInternalFailure, Status: http.StatusInternalServerError})
}

// writeError renders an apiError: the short code is mirrored into the
// x-Amzn-ErrorType header and the full envelope into the body.
func (s *Server) writeError(w http.ResponseWriter, apiErr apiError) {
	w.Header().Set(headerAmznErrorType, shortWireCode(apiErr.WireType()))
	w.WriteHeader(apiErr.HTTPStatus())
	body, _ := json.Marshal(wireErrorBody{Type: apiErr.WireType(), Message: apiErr.Error()})
	_, _ = w.Write(body)
}

// shortWireCode strips the namespace prefix from a full __type so that the
// error header reads e.g. "ResourceNotFoundException".
func shortWireCode(fullType string) string {
	if i := strings.LastIndexByte(fullType, '#'); i >= 0 {
		return fullType[i+1:]
	}
	return fullType
}

// epochSeconds returns the current time as fractional epoch seconds with
// millisecond precision, matching the service's CreationDateTime format.
func epochSeconds() float64 {
	return float64(time.Now().UnixMilli()) / 1000
}

// newUUID returns a random RFC 4122 v4 UUID string; used for TableId and for
// the X-Amzn-RequestId response header.
func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand fails only when the OS entropy source is broken; the
		// well-formed nil UUID is preferable to panicking the request path.
		return "00000000-0000-4000-8000-000000000000"
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func cloneKeySchema(in []KeySchemaElement) []KeySchemaElement {
	out := make([]KeySchemaElement, len(in))
	copy(out, in)
	return out
}

func cloneAttributeDefinitions(in []AttributeDefinition) []AttributeDefinition {
	out := make([]AttributeDefinition, len(in))
	copy(out, in)
	return out
}