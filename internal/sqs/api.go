package sqs

// The SQS HTTP surface: form-encoded SQS query-protocol requests (GET
// query or POST body; the official AWS SDKs always POST) answered with
// XML documents.

import (
	"crypto/rand"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// sqsNamespace is the XML namespace of every SQS response document.
const sqsNamespace = "http://queue.amazonaws.com/doc/2012-11-05/"

// Defaults a bare floci-go install runs with.
const (
	DefaultAccountID = "000000000000"
	DefaultRegion    = "us-east-1"
)

// Server implements the floci-go SQS API on top of a file-backed queue
// store and doubles as an http.Handler (mount it at any path).
type Server struct {
	// Store persists queues as files.
	Store *Store

	// AccountID is the AWS account id used in queue URLs and ARNs.
	AccountID string

	// Region is the AWS region used in queue ARNs.
	Region string

	// BaseURL, when set, is used verbatim (minus a trailing slash) as
	// the scheme://host prefix of queue URLs instead of deriving it
	// from the request host.
	BaseURL string
}

// NewServer builds a Server with a fresh queue store at dataDir.
func NewServer(dataDir string) (*Server, error) {
	st, err := NewStore(dataDir)
	if err != nil {
		return nil, err
	}
	return NewServerWithStore(st, DefaultAccountID, DefaultRegion), nil
}

// NewServerWithStore builds a Server bound to an existing store.
func NewServerWithStore(st *Store, accountID, region string) *Server {
	return &Server{Store: st, AccountID: accountID, Region: region}
}

// ServeHTTP dispatches a request's Action to its SQS handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeError(w, errMalformedRequest())
		return
	}

	switch action := r.Form.Get("Action"); action {
	case "CreateQueue":
		s.CreateQueue(w, r, r.Form)
	case "":
		writeError(w, errMissingAction())
	default:
		writeError(w, errInvalidAction(action))
	}
}

func (s *Server) accountID() string {
	if s.AccountID == "" {
		return DefaultAccountID
	}
	return s.AccountID
}

func (s *Server) region() string {
	if s.Region == "" {
		return DefaultRegion
	}
	return s.Region
}

// queueURL builds a queue URL from the request, like the SQS
// emulators do for local clients:
//
//	http://<host>/<account id>/<queue name>
//
// Proxy headers keep the URL aligned with what the client dialed.
func (s *Server) queueURL(r *http.Request, name string) string {
	if s.BaseURL != "" {
		return strings.TrimRight(s.BaseURL, "/") + "/" + s.accountID() + "/" + name
	}

	scheme := "http"
	switch p := r.Header.Get("X-Forwarded-Proto"); {
	case p != "":
		scheme = p
	case r.TLS != nil:
		scheme = "https"
	}

	host := r.Host
	if forwarded := firstForwardedHost(r.Header.Get("X-Forwarded-Host")); forwarded != "" {
		host = forwarded
	}
	if host == "" {
		host = "localhost"
	}

	return scheme + "://" + host + "/" + s.accountID() + "/" + name
}

func firstForwardedHost(header string) string {
	if header == "" {
		return ""
	}
	if i := strings.IndexByte(header, ','); i >= 0 {
		header = header[:i]
	}
	return strings.TrimSpace(header)
}

type createQueueDocument struct {
	XMLName  xml.Name          `xml:"CreateQueueResponse"`
	Xmlns    string            `xml:"xmlns,attr"`
	Result   createQueueResult `xml:"CreateQueueResult"`
	Metadata responseMetadata  `xml:"ResponseMetadata"`
}

type createQueueResult struct {
	QueueURL string `xml:"QueueUrl"`
}

type responseMetadata struct {
	RequestID string `xml:"RequestId"`
}

type errorDocument struct {
	XMLName   xml.Name   `xml:"ErrorResponse"`
	Xmlns     string     `xml:"xmlns,attr"`
	Error     errorEntry `xml:"Error"`
	RequestID string     `xml:"RequestId"`
}

type errorEntry struct {
	Type    string `xml:"Type"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
	Detail  string `xml:"Detail"`
}

// writeXML renders a response document with the SQS content type.
func writeXML(w http.ResponseWriter, status int, doc interface{}) {
	body, err := xml.MarshalIndent(doc, "", "  ")
	if err != nil {
		http.Error(w, "floci-go: response marshal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(body)
}

// writeError renders an SQS ErrorResponse wire document.
func writeError(w http.ResponseWriter, e *apiError) {
	faultType := "Sender"
	if e.HTTPCode >= http.StatusInternalServerError {
		faultType = "Receiver"
	}
	writeXML(w, e.HTTPCode, &errorDocument{
		Xmlns:     sqsNamespace,
		Error:     errorEntry{Type: faultType, Code: e.Code, Message: e.Message},
		RequestID: newRequestID(),
	})
}

// newRequestID generates an AWS-style RFC 4122 v4 request id for
// ResponseMetadata.RequestId.
func newRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail on supported platforms; fall back
		// to a time-derived id on exotic systems.
		nanos := time.Now().UnixNano()
		for i := range b {
			b[i] = byte(nanos >> (uint(i) % 64))
		}
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
