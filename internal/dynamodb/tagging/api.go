package tagging

import (
	"encoding/json"
	"errors"
	"io"
)

// Wire payloads and handlers for the DynamoDB_20120810 tagging control-plane
// actions. The methods are designed to plug straight into floci-go's action
// table; Actions() returns the ready-made entries.

type tagResourceRequest struct {
	ResourceArn string `json:"ResourceArn"`
	Tags        []Tag  `json:"Tags"`
}

type untagResourceRequest struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

type listTagsOfResourceRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

// ListTagsOfResourceResponse is the wire result of ListTagsOfResource.
type ListTagsOfResourceResponse struct {
	Tags []Tag `json:"Tags"`
}

// Handler implements the tagging control-plane actions on top of a Store.
type Handler struct {
	Store *Store
}

// Actions returns the action entries keyed by their X-Amz-Target names:
//
//	DynamoDB_20120810.TagResource, DynamoDB_20120810.UntagResource and
//	DynamoDB_20120810.ListTagsOfResource.
func (h *Handler) Actions() map[string]func([]byte) (any, error) {
	return map[string]func([]byte) (any, error){
		"DynamoDB_20120810.TagResource":        h.TagResourceAction,
		"DynamoDB_20120810.UntagResource":      h.UntagResourceAction,
		"DynamoDB_20120810.ListTagsOfResource": h.ListTagsOfResourceAction,
	}
}

// TagResourceAction handles DynamoDB_20120810.TagResource.
func (h *Handler) TagResourceAction(body []byte) (any, error) {
	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errf(ErrValidation, "malformed TagResource payload: %v", err)
	}
	if err := h.Store.TagResource(req.ResourceArn, req.Tags); err != nil {
		return nil, err
	}
	return struct{}{}, nil
}

// UntagResourceAction handles DynamoDB_20120810.UntagResource.
func (h *Handler) UntagResourceAction(body []byte) (any, error) {
	var req untagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errf(ErrValidation, "malformed UntagResource payload: %v", err)
	}
	if err := h.Store.UntagResource(req.ResourceArn, req.TagKeys); err != nil {
		return nil, err
	}
	return struct{}{}, nil
}

// ListTagsOfResourceAction handles DynamoDB_20120810.ListTagsOfResource.
func (h *Handler) ListTagsOfResourceAction(body []byte) (any, error) {
	var req listTagsOfResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errf(ErrValidation, "malformed ListTagsOfResource payload: %v", err)
	}
	tags, err := h.Store.ListTagsOfResource(req.ResourceArn)
	if err != nil {
		return nil, err
	}
	return ListTagsOfResourceResponse{Tags: tags}, nil
}

// APIError is the wire form of an AWS JSON protocol error.
type APIError struct {
	Type    string `json:"__type"`
	Message string `json:"Message,omitempty"`
}

// APIErrorOf converts a typed tagging error into its wire form.
func APIErrorOf(err error) (APIError, bool) {
	var e *Error
	if errors.As(err, &e) {
		return APIError{Type: ErrorTypePrefix + e.Code, Message: e.Message}, true
	}
	return APIError{}, false
}

// StatusCode maps a tagging error to its AWS JSON protocol HTTP status.
// DynamoDB reports every tagging failure (including resource-not-found) as
// HTTP 400; anything unrecognized falls back to 500.
func StatusCode(err error) int {
	if _, ok := APIErrorOf(err); ok {
		return 400
	}
	return 500
}

// EncodeResult renders a successful action result as a response body.
func EncodeResult(result any) ([]byte, error) {
	return json.Marshal(result)
}

// EncodeError renders an error as the AWS JSON protocol error body.
func EncodeError(err error) ([]byte, error) {
	apiErr, ok := APIErrorOf(err)
	if !ok {
		apiErr = APIError{Type: ErrorTypePrefix + "InternalFailure", Message: err.Error()}
	}
	return json.Marshal(apiErr)
}

// WriteError writes the AWS JSON protocol error body for err to w. Callers
// attach the HTTP status from StatusCode(err).
func WriteError(w io.Writer, err error) error {
	body, encErr := EncodeError(err)
	if encErr != nil {
		return encErr
	}
	_, encErr = w.Write(body)
	return encErr
}
