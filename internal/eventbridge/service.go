// Package eventbridge implements the Amazon EventBridge (events) wire API
// for floci: event buses and their rules. It speaks the target-dispatched
// AWS JSON protocol used by the DynamoDB protocol adapter (ticket 003):
// "X-Amz-Target: AWSEvents.<Op>" POST requests with x-amz-json bodies and
// "__type" error envelopes. Each event bus is persisted with its rules
// table in the floci store file (see ticket 028).
package eventbridge

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
)

const (
	defaultRegion  = "us-east-1"
	defaultAccount = "000000000000"
	defaultBusName = "default"

	// maxPageSize is the page size used by ListEventBuses and ListRules
	// when the caller omits Limit, and the hard cap they clamp to.
	maxPageSize = 100

	stateEnabled  = "ENABLED"
	stateDisabled = "DISABLED"
)

// Service serves the EventBridge (events) API over the AWS JSON protocol.
//
// The zero value is ready to use and keeps all state in memory; set
// StorePath (or construct via NewService) to persist into the floci store
// file.
type Service struct {
	// Region and AccountID are stamped into generated ARNs; when empty,
	// us-east-1 and account 000000000000 are used.
	Region    string
	AccountID string

	// StorePath is the floci store file path. When empty, state lives in
	// memory only.
	StorePath string

	mu    sync.Mutex
	buses map[string]*busRec
}

// New returns a Service, optionally bound to a store file path.
func New(paths ...string) *Service { return NewService(paths...) }

// NewService returns a Service, optionally bound to a store file path (or
// a directory, which binds to eventbridge.json inside it). Without an
// argument, the path is taken from FLOCI_EVENTBRIDGE_STORE when set.
func NewService(paths ...string) *Service {
	s := &Service{}
	if len(paths) > 0 {
		s.StorePath = resolveStorePath(paths[0])
	} else if v := os.Getenv("FLOCI_EVENTBRIDGE_STORE"); v != "" {
		s.StorePath = resolveStorePath(v)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.load()
	return s
}

// Handler returns the service as a single http.Handler so it can be
// mounted directly by the floci binary (mux.Handle("/", svc)).
func (s *Service) Handler() http.Handler { return s }

// region returns the region used when stamping ARNs.
func (s *Service) region() string {
	if s.Region != "" {
		return s.Region
	}
	return defaultRegion
}

// account returns the account id used when stamping ARNs.
func (s *Service) account() string {
	if s.AccountID != "" {
		return s.AccountID
	}
	return defaultAccount
}

// ServeHTTP dispatches "X-Amz-Target: AWSEvents.<Op>" requests, mirroring
// the wire behavior of the DynamoDB JSON protocol adapter.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ct := responseContentType(r)
	if r.Method != http.MethodPost {
		writeWireError(w, ct, http.StatusBadRequest, "UnsupportedOperationException", "HTTP method "+r.Method+" is not supported.")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		writeWireError(w, ct, http.StatusBadRequest, "SerializationException", "Unable to read the request body.")
		return
	}
	op := opFromTarget(r.Header.Get("X-Amz-Target"))
	if op == "" {
		writeWireError(w, ct, http.StatusBadRequest, "MissingActionException", "Request is missing the X-Amz-Target header.")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureState()
	defer func() {
		if p := recover(); p != nil {
			writeWireError(w, ct, http.StatusInternalServerError, "InternalException", "An internal error occurred while processing the request.")
		}
	}()

	switch op {
	case "CreateEventBus":
		s.opCreateEventBus(w, ct, body)
	case "DescribeEventBus":
		s.opDescribeEventBus(w, ct, body)
	case "DeleteEventBus":
		s.opDeleteEventBus(w, ct, body)
	case "ListEventBuses":
		s.opListEventBuses(w, ct, body)
	case "PutRule":
		s.opPutRule(w, ct, body)
	case "DescribeRule":
		s.opDescribeRule(w, ct, body)
	case "ListRules":
		s.opListRules(w, ct, body)
	case "DeleteRule":
		s.opDeleteRule(w, ct, body)
	case "EnableRule":
		s.opToggleRule(w, ct, body, stateEnabled)
	case "DisableRule":
		s.opToggleRule(w, ct, body, stateDisabled)
	default:
		writeWireError(w, ct, http.StatusBadRequest, "UnknownOperationException", "")
	}
}

// opFromTarget extracts the operation from an X-Amz-Target value such as
// "AWSEvents.PutRule", "com.amazonaws.eventbridge#PutRule" or "PutRule".
func opFromTarget(target string) string {
	target = strings.TrimSpace(target)
	for i := len(target) - 1; i >= 0; i-- {
		if c := target[i]; c == '.' || c == '#' {
			return target[i+1:]
		}
	}
	return target
}

// responseContentType mirrors the protocol version of the request body,
// matching the x-amz-json behavior of the DDB JSON adapter.
func responseContentType(r *http.Request) string {
	if strings.Contains(r.Header.Get("Content-Type"), "x-amz-json-1.0") {
		return "application/x-amz-json-1.0"
	}
	return "application/x-amz-json-1.1"
}

// encodeWire serializes a response body without HTML escaping, keeping
// byte-for-byte parity with the AWS JSON protocol.
func encodeWire(v any) []byte {
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte{10})
}

func writeWireJSON(w http.ResponseWriter, ct string, v any) {
	w.Header().Set("Content-Type", ct)
	w.Header().Set("x-amzn-RequestId", newRequestID())
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(encodeWire(v))
}

// wireError is the "__type" error envelope shared with the DDB JSON
// protocol adapter.
type wireError struct {
	Type    string `json:"__type"`
	Message string `json:"Message,omitempty"`
}

func writeWireError(w http.ResponseWriter, ct string, status int, code, message string) {
	w.Header().Set("Content-Type", ct)
	w.Header().Set("x-amzn-ErrorType", code)
	w.Header().Set("x-amzn-RequestId", newRequestID())
	w.WriteHeader(status)
	_, _ = w.Write(encodeWire(wireError{Type: code, Message: message}))
}

func newRequestID() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return "00000000-0000-0000-0000-000000000000"
	}
	h := hex.EncodeToString(b)
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[0:8], h[8:12], h[12:16], h[16:20], h[20:24])
}

// decodeWireBody decodes an awsjson request body; empty bodies decode into
// the zero value of the input shape.
func decodeWireBody(body []byte, v any) error {
	if len(strings.TrimSpace(string(body))) == 0 {
		return nil
	}
	return json.Unmarshal(body, v)
}

// Close releases any file-backed storage the EventBridge service holds.
func (s *Service) Close() error { return nil }
