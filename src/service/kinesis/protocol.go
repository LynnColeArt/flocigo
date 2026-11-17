package kinesis

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Wire plumbing constants for the AWS JSON 1.0 binding of Kinesis.
const (
	// TargetNamespace is the X-Amz-Target prefix of all operations.
	TargetNamespace = "Kinesis_20131202"

	contentTypeJSON = "application/x-amz-json-1.0"
	headerAmzTarget = "X-Amz-Target"
	headerRequestID = "x-amzn-RequestId"
	headerErrorType = "x-amzn-ErrorType"
)

// apiError models a JSON 1.0 protocol error (HTTP 400 plus a body of
// {"__type": code, "message": ...}).
type apiError struct {
	Code    string
	Message string
}

// Error implements the error interface.
func (e *apiError) Error() string { return e.Code + ": " + e.Message }

// newErr formats an apiError.
func newErr(code, format string, args ...interface{}) *apiError {
	return &apiError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// expiredIterator is returned whenever GetRecords is handed a token that
// no longer matches live state (deleted stream/shard or corrupt token).
func expiredIterator() *apiError {
	return &apiError{
		Code:    "ExpiredIteratorException",
		Message: "The provided shard iterator is expired or no longer valid (the shard may have been split or merged, or the stream was deleted).",
	}
}

// argRequired is the standard missing-parameter error.
func argRequired(name string) *apiError {
	return newErr("InvalidArgumentException", "%s is required.", name)
}

// ServeHTTP implements the Kinesis JSON 1.0 HTTP binding: a POST per
// operation with an X-Amz-Target header, a JSON request body, a JSON
// response and an x-amzn-RequestId header.
func (p *Provider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(headerRequestID, newRequestID())
	w.Header().Set("Content-Type", contentTypeJSON)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		p.writeError(w, newErr("SerializationException", "failed to read the request body: %v", err))
		return
	}

	target := r.Header.Get(headerAmzTarget)
	if !strings.HasPrefix(target, TargetNamespace+".") {
		p.writeError(w, newErr("UnknownOperationException", "unknown operation %q", target))
		return
	}
	out, opErr := p.dispatch(strings.TrimPrefix(target, TargetNamespace+"."), body)
	if opErr != nil {
		p.writeError(w, opErr)
		return
	}

	if out == nil {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
		return
	}
	blob, mErr := json.Marshal(out)
	if mErr != nil {
		p.writeError(w, newErr("InternalFailure", "failed to marshal the response: %v", mErr))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(blob)
}

// writeError renders e as an AWS JSON 1.0 error response.
func (p *Provider) writeError(w http.ResponseWriter, e *apiError) {
	w.Header().Set(headerErrorType, e.Code)
	w.WriteHeader(http.StatusBadRequest)
	_ = json.NewEncoder(w).Encode(map[string]string{"__type": e.Code, "message": e.Message})
}

// dispatch routes the operation to its handler.
func (p *Provider) dispatch(op string, body []byte) (interface{}, *apiError) {
	switch op {
	case "CreateStream":
		return p.opCreateStream(body)
	case "DeleteStream":
		return p.opDeleteStream(body)
	case "ListStreams":
		return p.opListStreams(body)
	case "DescribeStream":
		return p.opDescribeStream(body)
	case "DescribeStreamSummary":
		return p.opDescribeStreamSummary(body)
	case "ListShards":
		return p.opListShards(body)
	case "GetShardIterator":
		return p.opGetShardIterator(body)
	case "GetRecords":
		return p.opGetRecords(body)
	case "PutRecord":
		return p.opPutRecord(body)
	case "PutRecords":
		return p.opPutRecords(body)
	case "SplitShard":
		return p.opSplitShard(body)
	case "MergeShards":
		return p.opMergeShards(body)
	default:
		return nil, newErr("UnknownOperationException", "unknown operation %q", op)
	}
}

// decode unmarshals an operation payload; malformed JSON becomes the
// SerializationException the real service reports.
func decode(body []byte, into interface{}) *apiError {
	if err := json.Unmarshal(body, into); err != nil {
		return newErr("SerializationException", "failed to decode the request body: %v", err)
	}
	return nil
}

// newRequestID mints an AWS style request id (UUID-shaped).
func newRequestID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]),
		hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]),
		hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:16]))
}
