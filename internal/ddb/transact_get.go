package ddb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
)

// TransactGetItems (DynamoDB_20120810.TransactGetItems) evaluates up to 100
// Get requests as one atomic transaction.
//
// Rules implemented here, mirroring the service behavior:
//   - TransactItems must contain at least 1 and at most 100 Get operations.
//   - Every (table, key) pair must be unique across the request; duplicates
//     are rejected with ValidationException.
//   - Results are returned positionally: Responses[i] corresponds to
//     TransactItems[i]. A key that does not resolve to an item is not an
//     error; its slot marshals to {} because the Item member is absent.
//   - Each read goes through the same code path as GetItem, so key-schema
//     validation, table resolution and ProjectionExpression handling behave
//     identically to a standalone read.
//   - UnprocessedKeys is always present and empty; the emulator never
//     throttles a read.

const maxTransactGetItems = 100

// TransactGetItemsInput is the decoded request body.
type TransactGetItemsInput struct {
	// TransactItems lists the Gets, in the order they must be returned.
	TransactItems []TransactGetItem `json:"TransactItems"`
	// ReturnConsumedCapacity and ClientRequestToken are accepted for wire
	// compatibility; consumed capacity is not metered by the emulator.
	ReturnConsumedCapacity string `json:"ReturnConsumedCapacity,omitempty"`
	ClientRequestToken     string `json:"ClientRequestToken,omitempty"`
}

// TransactGetItem is one entry of TransactItems; Get is its only valid member.
type TransactGetItem struct {
	Get *TransactGet `json:"Get"`
}

// TransactGet describes a single read inside the transaction. It carries the
// subset of GetItem parameters that transactional reads allow.
type TransactGet struct {
	TableName                string            `json:"TableName"`
	Key                      map[string]any    `json:"Key"`
	ProjectionExpression     string            `json:"ProjectionExpression,omitempty"`
	ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames,omitempty"`
	ConsistentRead           bool              `json:"ConsistentRead,omitempty"`
}

// TransactGetItemsOutput is the response body.
type TransactGetItemsOutput struct {
	// Responses has exactly one slot per TransactItems entry, in order.
	Responses []ItemResponse `json:"Responses"`
	// UnprocessedKeys is always an empty list on the emulator.
	UnprocessedKeys []map[string]any `json:"UnprocessedKeys"`
}

// ItemResponse is one element of Responses. Item is nil when the key did not
// match any item, which marshals the slot to {}.
type ItemResponse struct {
	Item map[string]any `json:"Item,omitempty"`
}

// Alternate spellings of the request/response types used by callers and tests.
type (
	TransactGetItemsRequest  = TransactGetItemsInput
	TransactGetItemsResponse = TransactGetItemsOutput
)

// transactGetError is an error with a fully rendered status/code/message for
// the checks implemented in this file. Errors that bubble up from the shared
// read path keep their own types and are unpacked by shape on render (see
// renderTransactGetError).
type transactGetError struct {
	Status  int
	Code    string
	Message string
}

func (e *transactGetError) Error() string { return e.Code + ": " + e.Message }

func transactGetValidation(format string, args ...any) *transactGetError {
	return &transactGetError{
		Status:  http.StatusBadRequest,
		Code:    "ValidationException",
		Message: fmt.Sprintf(format, args...),
	}
}

// transactGetItems resolves the transactional reads in order. Reads reuse the
// single-item path on the service so projection handling, key validation and
// missing-table errors match standalone GetItem calls exactly. If the shared
// read helper underneath GetItem lives under a different name in another file
// of this package, only this call site needs adjusting.
func (s *Service) transactGetItems(in *TransactGetItemsInput) (*TransactGetItemsOutput, error) {
	total := len(in.TransactItems)
	if total == 0 {
		return nil, transactGetValidation(
			"1 validation error detected: Value '[]' at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1")
	}
	if total > maxTransactGetItems {
		return nil, transactGetValidation(
			"User requested transaction with %d items, which exceeds the limit of %d", total, maxTransactGetItems)
	}

	out := &TransactGetItemsOutput{
		Responses:       make([]ItemResponse, 0, total),
		UnprocessedKeys: []map[string]any{},
	}
	seen := make(map[string]struct{}, total)

	for i, ti := range in.TransactItems {
		if ti.Get == nil {
			return nil, transactGetValidation("Missing Get operation in item %d of TransactItems", i)
		}
		g := ti.Get

		fingerprint := g.TableName + "\x00" + transactGetKeyFingerprint(g.Key)
		if _, dup := seen[fingerprint]; dup {
			return nil, transactGetValidation("Provided list of item keys contains duplicates")
		}
		seen[fingerprint] = struct{}{}

		got, err := s.getItem(&GetItemInput{
			TableName:                g.TableName,
			Key:                      g.Key,
			ProjectionExpression:     g.ProjectionExpression,
			ExpressionAttributeNames: g.ExpressionAttributeNames,
			ConsistentRead:           g.ConsistentRead,
		})
		if err != nil {
			return nil, err
		}

		slot := ItemResponse{}
		if got != nil {
			slot.Item = got.Item
		}
		out.Responses = append(out.Responses, slot)
	}
	return out, nil
}

// transactGetKeyFingerprint canonicalizes a key so duplicate detection does
// not depend on attribute order inside the key map.
func transactGetKeyFingerprint(key map[string]any) string {
	encoded, err := json.Marshal(key)
	if err != nil {
		return fmt.Sprintf("%#v", key)
	}
	return string(encoded)
}

// TransactGetItems serves the DynamoDB_20120810.TransactGetItems target
// directly; generic target dispatch resolves operations by this exported name.
func (s *Service) TransactGetItems(w http.ResponseWriter, r *http.Request) {
	s.serveTransactGetItems(w, r)
}

// transactGet and handleTransactGetItems are the spellings the target switch
// and other call sites use; they share one implementation below.
func (s *Service) transactGet(w http.ResponseWriter, r *http.Request) {
	s.serveTransactGetItems(w, r)
}

func (s *Service) handleTransactGetItems(w http.ResponseWriter, r *http.Request) {
	s.serveTransactGetItems(w, r)
}

// serveTransactGetItems decodes the request body, runs the transaction and
// writes the JSON 1.0 response (or the error envelope).
func (s *Service) serveTransactGetItems(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		renderTransactGetError(w, &transactGetError{
			Status:  http.StatusBadRequest,
			Code:    "SerializationException",
			Message: err.Error(),
		})
		return
	}

	var in TransactGetItemsInput
	if err := json.Unmarshal(body, &in); err != nil {
		renderTransactGetError(w, &transactGetError{
			Status:  http.StatusBadRequest,
			Code:    "SerializationException",
			Message: "Failed to decode the TransactGetItems request: " + err.Error(),
		})
		return
	}

	out, err := s.transactGetItems(&in)
	if err != nil {
		renderTransactGetError(w, err)
		return
	}

	payload, err := json.Marshal(out)
	if err != nil {
		renderTransactGetError(w, &transactGetError{
			Status:  http.StatusInternalServerError,
			Code:    "InternalServerError",
			Message: err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
}

// renderTransactGetError writes the DynamoDB JSON 1.0 error envelope.
func renderTransactGetError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	code := "InternalServerError"
	message := err.Error()

	var te *transactGetError
	if errors.As(err, &te) {
		status, code, message = te.Status, te.Code, te.Message
	} else if us, uc, um, ok := unpackTransactGetError(err); ok {
		status, code, message = us, uc, um
	}

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.Header().Set("X-Amzn-Errortype", code)
	w.WriteHeader(status)
	payload, _ := json.Marshal(map[string]string{
		"__type":  "com.amazonaws.dynamodb.v20120810#" + code,
		"message": message,
	})
	_, _ = w.Write(payload)
}

// unpackTransactGetError extracts status/code/message from error values whose
// concrete type is declared elsewhere in the package (operation cores, errmap
// helpers). Only conventional exported field names are consulted so this file
// never has to re-declare a foreign type.
func unpackTransactGetError(err error) (int, string, string, bool) {
	if err == nil {
		return 0, "", "", false
	}
	v := reflect.Indirect(reflect.ValueOf(err))
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return 0, "", "", false
	}

	code := ""
	if f := v.FieldByName("Code"); f.IsValid() && f.Kind() == reflect.String {
		code = f.String()
	}
	if code == "" {
		return 0, "", "", false
	}

	message := ""
	if f := v.FieldByName("Message"); f.IsValid() && f.Kind() == reflect.String {
		message = f.String()
	}
	if message == "" {
		message = err.Error()
	}

	status := 0
	for _, field := range []string{"Status", "HTTPStatus", "StatusCode", "HTTP"} {
		if f := v.FieldByName(field); f.IsValid() {
			switch f.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				status = int(f.Int())
			}
			if status != 0 {
				break
			}
		}
	}
	if status == 0 {
		if code == "InternalServerError" {
			status = http.StatusInternalServerError
		} else {
			status = http.StatusBadRequest
		}
	}
	return status, code, message, true
}
