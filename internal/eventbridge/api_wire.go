package eventbridge

// api_wire.go — AWS JSON 1.1 wire plumbing for the ticket-029 operations:
// request decode, response encode, the __type error envelope, and the small
// router fragment that service.go calls for the five new operations.
//
// Suggested one-line integration in service.go's ServeHTTP, before the
// existing switch:
//
//     if s.handleOps(w, r) { return }

import (
    "encoding/json"
    "fmt"
    "net/http"
)

// X-Amz-Target values handled by this ticket.
const (
    targetPutTargets       = "AWSEvents.PutTargets"
    targetListTargetsByRule = "AWSEvents.ListTargetsByRule"
    targetRemoveTargets    = "AWSEvents.RemoveTargets"
    targetPutEvents        = "AWSEvents.PutEvents"
    targetTestEventPattern = "AWSEvents.TestEventPattern"
)

// Error codes surfaced in __type.
const (
    ebCodeResourceNotFound    = "ResourceNotFoundException"
    ebCodeInvalidParameter    = "InvalidParameterException"
    ebCodeInvalidEventPattern = "InvalidEventPatternException"
    ebCodeLimitExceeded       = "LimitExceededException"
    ebCodeTotalEntriesLimit   = "TotalEntriesLimitExceededException"
    ebCodeValidation          = "ValidationException"
    ebCodeInternal            = "InternalException"
)

const ebTypePrefix = "com.amazonaws.awsevents#"

// ebError is an EventBridge API error carrying the HTTP status plus the code
// and message rendered into the AWS JSON 1.1 error envelope.
type ebError struct {
    Status  int
    Code    string
    Message string
}

func (e *ebError) Error() string { return e.Code + ": " + e.Message }

func ebErrorf(status int, code, format string, args ...any) *ebError {
    return &ebError{Status: status, Code: code, Message: fmt.Sprintf(format, args...)}
}

func newResourceNotFoundf(format string, args ...any) *ebError {
    return ebErrorf(http.StatusBadRequest, ebCodeResourceNotFound, format, args...)
}

func newInvalidParameterf(format string, args ...any) *ebError {
    return ebErrorf(http.StatusBadRequest, ebCodeInvalidParameter, format, args...)
}

func newInvalidEventPatternf(format string, args ...any) *ebError {
    return ebErrorf(http.StatusBadRequest, ebCodeInvalidEventPattern, format, args...)
}

func newLimitExceededf(format string, args ...any) *ebError {
    return ebErrorf(http.StatusBadRequest, ebCodeLimitExceeded, format, args...)
}

func newValidationf(format string, args ...any) *ebError {
    return ebErrorf(http.StatusBadRequest, ebCodeValidation, format, args...)
}

type wireErrorBody struct {
    Type    string `json:"__type"`
    Message string `json:"message"`
}

func (e *ebError) write(w http.ResponseWriter) {
    w.Header().Set("Content-Type", "application/x-amz-json-1.1")
    w.WriteHeader(e.Status)
    _ = json.NewEncoder(w).Encode(wireErrorBody{Type: ebTypePrefix + e.Code, Message: e.Message})
}

// writeEventBridgeError renders an error on the wire.
func writeEventBridgeError(w http.ResponseWriter, err error) {
    if e, ok := err.(*ebError); ok {
        e.write(w)
        return
    }
    (&ebError{Status: http.StatusInternalServerError, Code: ebCodeInternal, Message: err.Error()}).write(w)
}

// decodeEventBridgeRequest decodes a JSON request body; numbers are kept as
// json.Number so pattern matching and envelope building treat them
// uniformly.
func decodeEventBridgeRequest(r *http.Request, v any) error {
    dec := json.NewDecoder(r.Body)
    dec.UseNumber()
    return dec.Decode(v)
}

// writeEventBridgeJSON encodes a response body.
func writeEventBridgeJSON(w http.ResponseWriter, v any) {
    b, err := json.Marshal(v)
    if err != nil {
        writeEventBridgeError(w, ebErrorf(http.StatusInternalServerError, ebCodeInternal, "encode response: %v", err))
        return
    }
    w.Header().Set("Content-Type", "application/x-amz-json-1.1")
    w.WriteHeader(http.StatusOK)
    _, _ = w.Write(b)
}

// handleOps serves the five ticket-029 operations by X-Amz-Target. The
// existing router in service.go calls this before its own switch; it reports
// false for targets it does not own so the pre-existing cases stay in
// charge.
func (s *Service) handleOps(w http.ResponseWriter, r *http.Request) bool {
    switch r.Header.Get("X-Amz-Target") {
    case targetPutTargets:
        s.handlePutTargets(w, r)
    case targetListTargetsByRule:
        s.handleListTargetsByRule(w, r)
    case targetRemoveTargets:
        s.handleRemoveTargets(w, r)
    case targetPutEvents:
        s.handlePutEvents(w, r)
    case targetTestEventPattern:
        s.handleTestEventPattern(w, r)
    default:
        return false
    }
    return true
}
