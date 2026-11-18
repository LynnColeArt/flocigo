package ddb

import (
	"crypto/rand"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"io"
	
)

// ddbWriteError writes the AWS JSON 1.0 wire fault for an apiErr.
func ddbWriteError(w http.ResponseWriter, e *apiErr) {
	payload, _ := json.Marshal(map[string]string{"__type": e.Type, "message": e.Message})
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusBadRequest)
	_, _ = w.Write(payload)
}

// ddbWriteResult writes the AWS JSON 1.0 success payload.
func ddbWriteResult(w http.ResponseWriter, out map[string]any) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(out)
}

// ddbDecodeRequest reads the request body as a JSON map (empty map when the
// body is empty).
func ddbDecodeRequest(r *http.Request) (map[string]any, *apiErr) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, errInternal("read request body: " + err.Error())
	}
	if len(body) == 0 {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, errValidation("malformed JSON request body: " + err.Error())
	}
	return out, nil
}

// ddbError builds an apiErr.
func ddbError(shape, msg string) *apiErr {
	return &apiErr{Type: ddbShape + shape, Message: msg}
}

// ddbErrorf is fmt.Sprintf applied to ddbError.
func ddbErrorf(shape, format string, a ...any) *apiErr {
	return ddbError(shape, fmt.Sprintf(format, a...))
}

func reqString(req map[string]any, key string) string {
	v, _ := req[key].(string)
	return v
}

// reqListValue reads a request field as a []any slice (always non-nil).
func reqListValue(req map[string]any, key string) ([]any, bool) {
	v, ok := req[key].([]any)
	return v, ok
}

// reqMapValue reads a request field as a map (always non-nil).
func reqMapValue(req map[string]any, key string) (map[string]any, bool) {
	v, ok := req[key].(map[string]any)
	return v, ok
}

// reqIntValue reads a request field as an int (json.Number is float64).
func reqIntValue(req map[string]any, key string) (int, bool) {
	f, ok := req[key].(float64)
	if !ok {
		return 0, false
	}
	return int(f), true
}

// reqBoolValue reads a request field as a bool.
func reqBoolValue(req map[string]any, key string) (bool, bool) {
	b, ok := req[key].(bool)
	return b, ok
}

// newUUID returns a random UUID string.
func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// reqValueString renders a JSON number/string as a string for error messages.
func reqValueString(v any) string {
	if f, ok := v.(float64); ok {
		return fmt.Sprintf("%v", int(f))
	}
	return fmt.Sprintf("%v", v)
}

// checkReturnValues validates the ReturnValues enum.
func checkReturnValues(v string) *apiErr {
	switch v {
	case "", "NONE", "ALL_OLD", "ALL_NEW", "UPDATED_OLD", "UPDATED_NEW":
		return nil
	}
	return errValidation("ReturnValues must be NONE | ALL_OLD | ALL_NEW | UPDATED_OLD | UPDATED_NEW")
}

// checkRCC validates the ReturnConsumedCapacity enum.
func checkRCC(v string) *apiErr {
	switch v {
	case "", "INDEXES", "TOTAL", "NONE":
		return nil
	}
	return errValidation("ReturnConsumedCapacity must be INDEXES | TOTAL | NONE")
}

// conditionFailed is the awsjson1.0 conditional-check fault.
func conditionFailed(...any) *apiErr {
	return ddbError("ConditionalCheckFailedException", "The conditional request failed")
}

// addConsumedCapacity is a no-op shim for wire parity.
func addConsumedCapacity(...any) {}

// typedGetItem routes through the HTTP GetItem handler with a synthesized
// request body so TransactGetItems matches the wire semantics exactly.
func (s *Service) typedGetItem(in *GetItemInput) (map[string]any, *apiErr) {
	body, _ := json.Marshal(map[string]any{
		"TableName":                  in.TableName,
		"Key":                        in.Key,
		"ProjectionExpression":        in.ProjectionExpression,
		"ExpressionAttributeNames":    in.ExpressionAttributeNames,
		"ConsistentRead":             in.ConsistentRead,
	})
	r := &http.Request{Body: io.NopCloser(bytes.NewReader(body))}
	return s.doGetItem(r)
}

// GetItemInput is one TransactGetItems Get item.
type GetItemInput struct {
	TableName                string
	Key                      map[string]any
	ProjectionExpression     string
	ExpressionAttributeNames map[string]string
	ConsistentRead           bool
}

// Standard fault message templates.
const (
	msgBadTableName       = "Table name length must be between 3 and 255 characters"
	msgBadAttributeName   = "Attribute name length must be between 1 and 255 characters"
	msgBadAttributeType   = "Attribute type must be S / N / B"
	msgBadKeySchemaCount  = "key schema must have exactly 1 partition key (and optional sort key)"
	msgBadIndexProjection = "projection must be ALL / KEYS_ONLY / INCLUDE"
	msgBadIndexCount      = "Table has too many indexes"
)

// Standard fault shapes.
const (
	shapeValidation = "ValidationException"
	shapeInUse      = "ResourceInUseException"
	shapeNotFound   = "ResourceNotFoundException"
	shapeLimit      = "LimitExceededException"
	shapeInternal   = "InternalFailure"
)

// contentTypeJSON is the AWS JSON 1.0 content type used by DynamoDB.
const ddbContentTypeJSON = "application/x-amz-json-1.0"

const ddbTargetPrefix = "DynamoDB_20120810."

const BillingModePayPerRequest = "PAY_PER_REQUEST"
const BillingModeProvisioned = "PROVISIONED"
