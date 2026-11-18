// Package ddb implements an in-memory, wire-compatible DynamoDB service
// speaking the awsJson 1.1 protocol (DynamoDB_20120810 targets).
package ddb

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

const targetPrefix = "DynamoDB_20120810."

const msgBadTableName = "One or more parameter values were invalid: TableName must be at least 3 characters long and at most 255 characters long"

// Service is the DynamoDB-compatible HTTP service.
type Service struct {
	store *Store
}

// New creates a Service over a fresh in-memory store. Any arguments are
// tolerated; a *Store passed as an argument is used as the backing state.
func New(args ...any) *Service {
	for _, a := range args {
		if st, ok := a.(*Store); ok && st != nil {
			return &Service{store: st}
		}
	}
	return &Service{store: newStore()}
}

// NewService is an alias for New.
func NewService(args ...any) *Service { return New(args...) }

// NewStore creates a fresh backing store.
func NewStore(args ...any) *Store { return newStore() }

// ServeHTTP dispatches awsJson requests by X-Amz-Target.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	op := strings.TrimPrefix(r.Header.Get("X-Amz-Target"), targetPrefix)
	switch op {
	case "CreateTable":
		s.createTable(w, r)
	case "GetItem":
		s.getItem(w, r)
	case "PutItem":
		s.putItem(w, r)
	case "DeleteItem":
		s.deleteItem(w, r)
	case "UpdateItem":
		t.updateItemGuard(w, r)
	case "Query":
		s.query(w, r)
	default:
		w.Header().Set("X-Amzn-Errortype", "UnknownOperationException")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"__type": "UnknownOperationException"})
	}
}

// apiErr describes a wire error.
type apiErr struct {
	Code  string
	Msg   string
	Extra map[string]any
}

func ddbError(code, msg string) *apiErr { return &apiErr{Code: code, Msg: msg} }

func ddbErrorf(code, format string, args ...any) *apiErr {
	return &apiErr{Code: code, Msg: fmt.Sprintf(format, args...)}
}

func ddbWriteResult(w http.ResponseWriter, out map[string]any) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(out)
}

func ddbWriteError(w http.ResponseWriter, aerr *apiErr) {
	status := http.StatusBadRequest
	if aerr.Code == "InternalServerError" {
		status = http.StatusInternalServerError
	} else if aerr.Code == "ServiceUnavailable" {
		status = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.Header().Set("X-Amzn-Errortype", aerr.Code)
	w.WriteHeader(status)
	body := map[string]any{
		"__type":  "com.amazonaws.dynamodb.v20120810#" + aerr.Code,
		"message": aerr.Msg,
	}
	for k, v := range aerr.Extra {
		body[k] = v
	}
	_ = json.NewEncoder(w).Encode(body)
}

func ddbDecodeRequest(r *http.Request) (map[string]any, *apiErr) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, ddbError("SerializationException", "Unexpected error while parsing request body")
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, ddbError("SerializationException", "Unexpected error while parsing request body")
	}
	if v == nil {
		return nil, ddbError("SerializationException", "Unexpected error while parsing request body")
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, ddbError("SerializationException", "Unexpected error while parsing request body")
	}
	return m, nil
}

func reqString(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	s, _ := m[key].(string)
	return s
}

func reqHas(m map[string]any, key string) bool {
	if m == nil {
		return false
	}
	_, ok := m[key]
	return ok
}

func reqMapValue(m map[string]any, key string) (map[string]any, bool) {
	if m == nil {
		return nil, false
	}
	v, ok := m[key]
	if !ok {
		return nil, false
	}
	mm, ok := v.(map[string]any)
	return mm, ok
}

func reqListValue(m map[string]any, key string) ([]any, bool) {
	if m == nil {
		return nil, false
	}
	v, ok := m[key]
	if !ok {
		return nil, false
	}
	l, ok := v.([]any)
	return l, ok
}

func reqBoolValue(m map[string]any, key string) (bool, bool) {
	if m == nil {
		return false, false
	}
	v, ok := m[key]
	if !ok {
		return false, false
	}
	b, ok := v.(bool)
	return b, ok
}

func reqIntValue(m map[string]any, key string) (int64, bool) {
	if m == nil {
		return 0, false
	}
	v, ok := m[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	case float64:
		return int64(n), true
	case int:
		return int64(n), true
	case string:
		i, err := strconv.ParseInt(strings.TrimSpace(n), 10, 64)
		return i, err == nil
	}
	return 0, false
}

func reqValueString(v any) string {
	switch n := v.(type) {
	case json.Number:
		return n.String()
	case float64:
		return strconv.FormatFloat(n, 'g', -1, 64)
	case bool:
		return strconv.FormatBool(n)
	case string:
		return n
	default:
		return fmt.Sprintf("%v", v)
	}
}

func consumedCapacity(tn string, units float64) map[string]any {
	return map[string]any{"TableName": tn, "CapacityUnits": units}
}

func addConsumedCapacity(out map[string]any, rcc, tn string, units float64) {
	if rcc == "TOTAL" || rcc == "INDEXES" {
		out["ConsumedCapacity"] = consumedCapacity(tn, units)
	}
}

var returnValuesEnum = map[string]bool{
	"NONE": true, "ALL_OLD": true, "UPDATED_OLD": true, "ALL_NEW": true, "UPDATED_NEW": true,
}

func checkReturnValues(v string) *apiErr {
	if v == "" || returnValuesEnum[v] {
		return nil
	}
	return ddbErrorf("ValidationException", "1 validation error detected: Value '%s' at 'returnValues' failed to satisfy constraint: Member must satisfy enum value set: [ALL_NEW, ALL_OLD, NONE, UPDATED_NEW, UPDATED_OLD]", v)
}

func checkRCC(v string) *apiErr {
	if v == "" || v == "NONE" || v == "TOTAL" || v == "INDEXES" {
		return nil
	}
	return ddbErrorf("ValidationException", "1 validation error detected: Value '%s' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, NONE, TOTAL]", v)
}

func conditionFailed(req map[string]any, prev map[string]any, hasPrev bool) *apiErr {
	extra := map[string]any{}
	if reqString(req, "ReturnValuesOnConditionCheckFailure") == "ALL_OLD" && hasPrev {
		extra["Item"] = prev
	}
	return &apiErr{Code: "ConditionalCheckFailedException", Msg: "The conditional request failed", Extra: extra}
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
