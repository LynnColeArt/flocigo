package main

import (
    "crypto/rand"
    "encoding/hex"
    "fmt"
    "math"
    "time"
)

// Protocol constants.
const (
    targetPrefix         = "DynamoDB_20120810"
    tableStatusActive    = "ACTIVE"
    keyHash              = "HASH"
    keyRange             = "RANGE"
    billingProvisioned   = "PROVISIONED"
    billingPayPerRequest = "PAY_PER_REQUEST"
    attrString           = "S"
    attrNumber           = "N"
    attrBinary           = "B"
)

// AttributeDefinition declares a named attribute and its type (S, N or B).
type AttributeDefinition struct {
    AttributeName string `json:"AttributeName"`
    AttributeType string `json:"AttributeType"`
}

// KeySchemaElement names the HASH (pk) or RANGE (sk) key.
type KeySchemaElement struct {
    AttributeName string `json:"AttributeName"`
    KeyType       string `json:"KeyType"`
}

// ProvisionedThroughputInput is the request-side throughput document.
type ProvisionedThroughputInput struct {
    ReadCapacityUnits  *int64 `json:"ReadCapacityUnits"`
    WriteCapacityUnits *int64 `json:"WriteCapacityUnits"`
}

// ProvisionedThroughputDescription is the response-side throughput document.
type ProvisionedThroughputDescription struct {
    NumberOfDecreasesToday int64 `json:"NumberOfDecreasesToday"`
    ReadCapacityUnits      int64 `json:"ReadCapacityUnits"`
    WriteCapacityUnits     int64 `json:"WriteCapacityUnits"`
}

// BillingModeSummary is returned only for PAY_PER_REQUEST tables.
type BillingModeSummary struct {
    BillingMode                       string  `json:"BillingMode"`
    LastUpdateToPayPerRequestDateTime float64 `json:"LastUpdateToPayPerRequestDateTime"`
}

// TableDescription matches the DynamoDB CreateTable/DescribeTable response
// document. Fields are declared alphabetically for stable wire output.
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
    TableStatus           string                            `json:"TableStatus"`
    TableSizeBytes        int64                             `json:"TableSizeBytes"`
}

// description renders the wire TableDescription for a stored table.
func (t *Table) description() *TableDescription {
    d := &TableDescription{
        AttributeDefinitions: t.AttributeDefinitions,
        CreationDateTime:     t.CreationDateTime,
        ItemCount:            t.ItemCount,
        KeySchema:            t.KeySchema,
        TableArn:             t.TableArn,
        TableId:              t.TableID,
        TableName:            t.TableName,
        TableStatus:          t.TableStatus,
        TableSizeBytes:       t.TableSizeBytes,
    }
    if t.BillingMode == billingPayPerRequest {
        d.BillingModeSummary = &BillingModeSummary{
            BillingMode:                       billingPayPerRequest,
            LastUpdateToPayPerRequestDateTime: t.CreationDateTime,
        }
    } else {
        d.ProvisionedThroughput = &ProvisionedThroughputDescription{
            NumberOfDecreasesToday: 0,
            ReadCapacityUnits:      t.ReadCapacityUnits,
            WriteCapacityUnits:     t.WriteCapacityUnits,
        }
    }
    return d
}

// apiError is an x-amz-json-1.0 error document. Type is the full __type.
type apiError struct {
    Status  int
    Type    string
    Message string
}

func (e *apiError) Error() string { return fmt.Sprintf("%s: %s", e.Type, e.Message) }

func ddbErr(code, message string) *apiError {
    return &apiError{Status: 400, Type: "com.amazonaws.dynamodb.v20120810#" + code, Message: message}
}

func errValidation(message string) *apiError {
    return ddbErr("ValidationException", message)
}

func errResourceInUse(table string) *apiError {
    return ddbErr("ResourceInUseException", "Table already exists: "+table)
}

// Protocol-level failures use the coral service prefixes, matching DynamoDB.
func errUnknownOperation() *apiError {
    return &apiError{Status: 400, Type: "com.amazon.coral.service#UnknownOperationException"}
}

func errSerialization() *apiError {
    return &apiError{Status: 400, Type: "com.amazon.coral.service#SerializationException"}
}

// epochSeconds renders t as epoch seconds with millisecond precision, which
// is how DynamoDB serializes timestamps on the wire (numeric, e.g. 1.7177e9).
func epochSeconds(t time.Time) float64 {
    return math.Round(float64(t.UnixNano())/float64(time.Millisecond)) / 1000
}

// newUUID returns a random RFC 4122 version 4 UUID string (used for TableId).
func newUUID() string {
    var b [16]byte
    if _, err := rand.Read(b[:]); err != nil {
        return fmt.Sprintf("floci-%016x", time.Now().UnixNano())
    }
    b[6] = (b[6] & 0x0f) | 0x40 // version 4
    b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
    return hex.EncodeToString(b[0:4]) + "-" + hex.EncodeToString(b[4:6]) + "-" +
        hex.EncodeToString(b[6:8]) + "-" + hex.EncodeToString(b[8:10]) + "-" + hex.EncodeToString(b[10:16])
}

// newRequestID returns an x-amzn-RequestId header value.
func newRequestID() string { return newUUID() }
