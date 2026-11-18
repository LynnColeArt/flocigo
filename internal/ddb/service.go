package ddb

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

// ddbTargetPrefix is the X-Amz-Target prefix of the DynamoDB JSON API revision.
const ddbTargetPrefix = "DynamoDB_20120810."

// ddbContentTypeJSON is the Amazon JSON 1.0 protocol media type.
const ddbContentTypeJSON = "application/x-amz-json-1.0"

// ddbError is a DynamoDB protocol error. Code is the error name without the
// fully-qualified __type prefix; Status is the HTTP response status.
type ddbError struct {
	Status  int
	Code    string
	Message string
}

func (e *ddbError) Error() string { return e.Code + ": " + e.Message }

// ddbErrValidation builds a 400 ValidationException.
func ddbErrValidation(msg string) *ddbError {
	return &ddbError{Status: http.StatusBadRequest, Code: "ValidationException", Message: msg}
}

// TableService serves the DynamoDB operations implemented by this package as
// a single http.Handler speaking the Amazon JSON 1.0 protocol. Mount it at
// the DynamoDB endpoint of the floci mux (routing happens on X-Amz-Target).
type TableService struct {
	store   *TableStore
	region  string
	account string
}

// NewTableService mounts the service over the table store rooted at dataDir;
// an empty dataDir selects $FLOCI_DATA_DIR or .floci.
func NewTableService(dataDir string) (*TableService, error) {
	store, err := OpenTableStore(dataDir)
	if err != nil {
		return nil, err
	}
	return &TableService{store: store, region: ddbDefaultRegion(), account: ddbDefaultAccount()}, nil
}

// Store exposes the backing table store (for reading persisted tables).
func (s *TableService) Store() *TableStore { return s.store }

// arnOf builds a table ARN for the emulator region/account.
func (s *TableService) arnOf(name string) string {
	return fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s", s.region, s.account, name)
}

// indexArnOf builds an index ARN below a table ARN.
func (s *TableService) indexArnOf(table, index string) string {
	return s.arnOf(table) + "/index/" + index
}

func ddbDefaultRegion() string {
	for _, key := range []string{"AWS_REGION", "AWS_DEFAULT_REGION"} {
		if r := os.Getenv(key); r != "" {
			return r
		}
	}
	return "us-east-1"
}

func ddbDefaultAccount() string {
	if a := os.Getenv("FLOCI_ACCOUNT_ID"); a != "" {
		return a
	}
	return "000000000000"
}

// ServeHTTP dispatches the JSON protocol request on the X-Amz-Target header.
func (s *TableService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		ddbWriteError(w, &ddbError{Status: http.StatusBadRequest, Code: "SerializationException"})
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		ddbWriteError(w, &ddbError{Status: http.StatusBadRequest, Code: "SerializationException"})
		return
	}
	target := strings.TrimSpace(r.Header.Get("X-Amz-Target"))
	switch operation := strings.TrimPrefix(target, ddbTargetPrefix); operation {
	case "CreateTable":
		s.serveCreateTable(w, body)
	default:
		ddbWriteError(w, &ddbError{Status: http.StatusBadRequest, Code: "UnknownOperationException"})
	}
}

// ddbWriteResponse flushes a successful JSON protocol response.
func ddbWriteResponse(w http.ResponseWriter, value any) {
	raw, err := json.Marshal(value)
	if err != nil {
		ddbWriteError(w, &ddbError{Status: http.StatusInternalServerError, Code: "InternalServerError", Message: err.Error()})
		return
	}
	header := w.Header()
	header.Set("Content-Type", ddbContentTypeJSON)
	header.Set("X-Amzn-Requestid", ddbRequestID())
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

// ddbWriteError flushes a JSON protocol error (__type/message + headers).
func ddbWriteError(w http.ResponseWriter, apiErr *ddbError) {
	prefix := "com.amazonaws.dynamodb.v20120810"
	switch apiErr.Code {
	case "UnknownOperationException", "SerializationException":
		prefix = "com.amazon.coral.service"
	}
	wire := struct {
		Type    string `json:"__type"`
		Message string `json:"message,omitempty"`
	}{Type: prefix + "#" + apiErr.Code, Message: apiErr.Message}
	raw, _ := json.Marshal(wire)
	header := w.Header()
	header.Set("Content-Type", ddbContentTypeJSON)
	if apiErr.Code != "" {
		header.Set("X-Amzn-ErrorType", apiErr.Code)
	}
	header.Set("X-Amzn-Requestid", ddbRequestID())
	w.WriteHeader(apiErr.Status)
	_, _ = w.Write(raw)
}

// ddbRequestID mimics an Amazon request id.
func ddbRequestID() string {
	return ddbRandHex(16)
}
