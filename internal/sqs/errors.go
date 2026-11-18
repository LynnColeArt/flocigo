package sqs

// SQS wire-level errors, rendered as <ErrorResponse> XML documents
// with sender semantics and an HTTP status.

import (
	"fmt"
	"log"
	"net/http"
	"strings"
)

// apiError is a single SQS error ready to be rendered on the wire.
type apiError struct {
	HTTPCode int
	Code     string
	Message  string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("%s (HTTP %d): %s", e.Code, e.HTTPCode, e.Message)
}

// senderError builds a client-side (Sender) SQS fault.
func senderError(httpStatus int, code, message string) *apiError {
	return &apiError{HTTPCode: httpStatus, Code: code, Message: message}
}

// errMissingParameter reports a required request parameter.
func errMissingParameter(parameter string) *apiError {
	return senderError(http.StatusBadRequest, "MissingParameter",
		fmt.Sprintf("The request must contain the parameter %s.", parameter))
}

// errInvalidParameterValue reports a bad attribute or request value.
func errInvalidParameterValue(parameter, reason string) *apiError {
	return senderError(http.StatusBadRequest, "InvalidParameterValue",
		fmt.Sprintf("Invalid value for the parameter %s. Reason: %s.", parameter, reason))
}

// errInvalidAttributeName reports an unknown queue attribute name.
func errInvalidAttributeName(name string) *apiError {
	return senderError(http.StatusBadRequest, "InvalidAttributeName",
		fmt.Sprintf("Unknown Attribute %s.", name))
}

// errQueueNameExists is CreateQueue's idempotency conflict: a queue
// with the same name exists whose attributes differ from the request.
// diffs holds the differing attribute names, sorted.
func errQueueNameExists(diffs []string) *apiError {
	return senderError(http.StatusBadRequest, "QueueNameExists",
		fmt.Sprintf("A queue already exists with the same name and a different value for attribute the following attribute(s) : %s.",
			strings.Join(diffs, ", ")))
}

// errQueueFullNameLengthLimitExceeded reports queue names beyond the
// 80-character SQS limit.
func errQueueFullNameLengthLimitExceeded(name string) *apiError {
	return senderError(http.StatusBadRequest, "QueueFullNameLengthLimitExceeded",
		fmt.Sprintf("The length of queue name '%s' is greater than 80 characters.", name))
}

// errInvalidAction reports an unknown Action on the endpoint.
func errInvalidAction(action string) *apiError {
	return senderError(http.StatusBadRequest, "InvalidAction",
		fmt.Sprintf("The action %s is not supported by this endpoint.", action))
}

// errMissingAction reports a request without an Action parameter.
func errMissingAction() *apiError {
	return senderError(http.StatusBadRequest, "MissingAction",
		"The request must contain the parameter Action.")
}

// errMalformedRequest reports a request whose payload cannot be parsed.
func errMalformedRequest() *apiError {
	return senderError(http.StatusBadRequest, "InvalidParameterValue",
		"Invalid request. The request body could not be parsed.")
}

// errInternalError reports unexpected server-side failures; the cause
// is logged, never exposed on the wire.
func errInternalError(cause error) *apiError {
	if cause != nil {
		log.Printf("floci-go: sqs internal error: %v", cause)
	}
	return senderError(http.StatusInternalServerError, "InternalError", "Internal Server Error")
}
