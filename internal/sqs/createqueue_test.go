package sqs

// Tests for the CreateQueue action: wire format, queue-file
// persistence and idempotency.

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// newTestServer wires a Server to a live httptest server backed by a
// fresh queue store.
func newTestServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return srv, ts
}

// actionForm builds an SQS action's form parameters from key/value
// pairs.
func actionForm(action string, pairs ...string) url.Values {
	form := url.Values{"Action": {action}}
	for i := 0; i+1 < len(pairs); i += 2 {
		form.Add(pairs[i], pairs[i+1])
	}
	return form
}

// post sends a form-encoded SQS request and returns status and body,
// asserting the SQS XML content type.
func post(t *testing.T, ts *httptest.Server, form url.Values) (int, string) {
	t.Helper()
	resp, err := http.PostForm(ts.URL, form)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/xml") {
		t.Fatalf("Content-Type = %q, want text/xml", ct)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp.StatusCode, string(body)
}

func createQueueCall(t *testing.T, ts *httptest.Server, pairs ...string) (int, string) {
	t.Helper()
	return post(t, ts, actionForm("CreateQueue", pairs...))
}

func requireStatus(t *testing.T, status, want int, body string) {
	t.Helper()
	if status != want {
		t.Fatalf("status = %d, want %d\nbody: %s", status, want, body)
	}
}

// requireQueueURL unmarshals a successful CreateQueue response.
func requireQueueURL(t *testing.T, body string) string {
	t.Helper()
	var doc struct {
		QueueURL string `xml:"CreateQueueResult>QueueUrl"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("unmarshal CreateQueue response %s: %v", body, err)
	}
	if doc.QueueURL == "" {
		t.Fatalf("no QueueUrl in response\nbody: %s", body)
	}
	return doc.QueueURL
}

// requireAPIError asserts that an ErrorResponse carries the wanted
// code with sender semantics, and returns its message.
func requireAPIError(t *testing.T, body, wantCode string) string {
	t.Helper()
	var doc struct {
		Type    string `xml:"Error>Type"`
		Code    string `xml:"Error>Code"`
		Message string `xml:"Error>Message"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("unmarshal ErrorResponse %s: %v", body, err)
	}
	if doc.Type != "Sender" {
		t.Fatalf("Error.Type = %q, want Sender (body: %s)", doc.Type, body)
	}
	if doc.Code != wantCode {
		t.Fatalf("Error.Code = %q, want %q (message: %s)", doc.Code, wantCode, doc.Message)
	}
	return doc.Message
}

// tagValue extracts an element's text from a response.
func tagValue(t *testing.T, body, tag string) string {
	t.Helper()
	m := regexp.MustCompile(`<` + tag + `>(.*)</` + tag + `>`).FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("missing <%s> in response\nbody: %s", tag, body)
	}
	return m[1]
}

// testQueueURL mirrors the QueueUrl floci-go derives from a request
// against ts.
func testQueueURL(ts *httptest.Server, queue string) string {
	return "http://" + strings.TrimPrefix(ts.URL, "http://") + "/" + DefaultAccountID + "/" + queue
}

// Creating a queue writes the queue file and answers with the queue
// URL; omitted attributes are stored with their service defaults.
func TestCreateQueueReturnsQueueURLAndCreatesQueueFile(t *testing.T) {
	srv, ts := newTestServer(t)

	status, body := createQueueCall(t, ts,
		"QueueName", "orders",
		"Attribute.1.name", "DelaySeconds",
		"Attribute.1.value", "10",
		"Tag.1.key", "env",
		"Tag.1.value", "test",
	)
	requireStatus(t, status, http.StatusOK, body)

	if !strings.Contains(body, `<CreateQueueResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">`) {
		t.Fatalf("missing CreateQueueResponse envelope\nbody: %s", body)
	}
	wantURL := testQueueURL(ts, "orders")
	if got := requireQueueURL(t, body); got != wantURL {
		t.Fatalf("QueueUrl = %q, want %q", got, wantURL)
	}
	if id := tagValue(t, body, "RequestId"); len(id) != 36 {
		t.Fatalf("RequestId = %q, want a 36-character id", id)
	}

	q, ok, err := srv.Store.Load("orders")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !ok {
		t.Fatal("queue file for orders was not created")
	}
	if q.Name != "orders" {
		t.Fatalf("name = %q", q.Name)
	}
	if q.Arn != "arn:aws:sqs:us-east-1:"+DefaultAccountID+":orders" {
		t.Fatalf("Arn = %q", q.Arn)
	}
	if got := q.Attributes["DelaySeconds"]; got != "10" {
		t.Fatalf("DelaySeconds = %q, want 10", got)
	}
	if got := q.Attributes[attrVisibilityTimeout]; got != "30" {
		t.Fatalf("VisibilityTimeout = %q, want default 30", got)
	}
	if got := q.Attributes[attrMaximumMessageSize]; got != "262144" {
		t.Fatalf("MaximumMessageSize = %q, want default 262144", got)
	}
	if got := q.Tags["env"]; got != "test" {
		t.Fatalf("tag env = %q, want test", got)
	}
	if q.CreatedAt.IsZero() || q.LastModified.IsZero() {
		t.Fatal("timestamps not persisted")
	}
	if q.LastModified.Before(q.CreatedAt) {
		t.Fatal("LastModified is before CreatedAt")
	}
}

// Re-creating a queue with matching attributes (in any request order)
// is idempotent: the same QueueUrl and an untouched queue file.
func TestCreateQueueIsIdempotentWhenAttributesMatch(t *testing.T) {
	srv, ts := newTestServer(t)

	status, body := createQueueCall(t, ts,
		"QueueName", "orders",
		"Attribute.1.name", "DelaySeconds",
		"Attribute.1.value", "10",
		"Attribute.2.name", "VisibilityTimeout",
		"Attribute.2.value", "45",
	)
	requireStatus(t, status, http.StatusOK, body)
	url1 := requireQueueURL(t, body)
	before, ok, err := srv.Store.Load("orders")
	if err != nil || !ok {
		t.Fatalf("Load: %v", err)
	}

	// Same attributes, different order in the request.
	status, body = createQueueCall(t, ts,
		"QueueName", "orders",
		"Attribute.1.name", "VisibilityTimeout",
		"Attribute.1.value", "45",
		"Attribute.2.name", "DelaySeconds",
		"Attribute.2.value", "10",
	)
	requireStatus(t, status, http.StatusOK, body)
	if got := requireQueueURL(t, body); got != url1 {
		t.Fatalf("idempotent create returned %q, want %q", got, url1)
	}

	after, ok, err := srv.Store.Load("orders")
	if err != nil || !ok {
		t.Fatalf("Load: %v", err)
	}
	if !before.CreatedAt.Equal(after.CreatedAt) {
		t.Fatalf("CreatedAt changed: %v -> %v", before.CreatedAt, after.CreatedAt)
	}
	if !before.LastModified.Equal(after.LastModified) {
		t.Fatalf("LastModified changed: %v -> %v", before.LastModified, after.LastModified)
	}
	if !reflect.DeepEqual(before.Attributes, after.Attributes) {
		t.Fatalf("attributes changed: %v -> %v", before.Attributes, after.Attributes)
	}
}

// A re-create whose attributes differ fails with QueueNameExists,
// naming the differing attribute, and leaves the queue file untouched.
func TestCreateQueueQueueNameExistsWhenAttributesDiffer(t *testing.T) {
	srv, ts := newTestServer(t)

	status, body := createQueueCall(t, ts,
		"QueueName", "orders",
		"Attribute.1.name", "DelaySeconds",
		"Attribute.1.value", "10",
	)
	requireStatus(t, status, http.StatusOK, body)

	status, body = createQueueCall(t, ts,
		"QueueName", "orders",
		"Attribute.1.name", "DelaySeconds",
		"Attribute.1.value", "20",
	)
	requireStatus(t, status, http.StatusBadRequest, body)
	message := requireAPIError(t, body, "QueueNameExists")
	if !strings.Contains(message, "the following attribute(s) : DelaySeconds.") {
		t.Fatalf("QueueNameExists message = %q", message)
	}

	q, ok, err := srv.Store.Load("orders")
	if err != nil || !ok {
		t.Fatalf("Load: %v", err)
	}
	if got := q.Attributes["DelaySeconds"]; got != "10" {
		t.Fatalf("failed create changed DelaySeconds to %q", got)
	}
}

// All differing attributes are reported, sorted by name.
func TestCreateQueueQueueNameExistsListsAllDifferingAttributes(t *testing.T) {
	_, ts := newTestServer(t)

	status, body := createQueueCall(t, ts, "QueueName", "orders")
	requireStatus(t, status, http.StatusOK, body)

	status, body = createQueueCall(t, ts,
		"QueueName", "orders",
		"Attribute.1.name", "DelaySeconds",
		"Attribute.1.value", "5",
		"Attribute.2.name", "VisibilityTimeout",
		"Attribute.2.value", "90",
	)
	requireStatus(t, status, http.StatusBadRequest, body)
	message := requireAPIError(t, body, "QueueNameExists")
	if !strings.Contains(message, "the following attribute(s) : DelaySeconds, VisibilityTimeout.") {
		t.Fatalf("QueueNameExists message = %q", message)
	}
}

// Omitted attributes count as their service defaults.
func TestCreateQueueOmittedAttributesCountAsDefaults(t *testing.T) {
	_, ts := newTestServer(t)

	status, body := createQueueCall(t, ts,
		"QueueName", "orders",
		"Attribute.1.name", "DelaySeconds",
		"Attribute.1.value", "10",
	)
	requireStatus(t, status, http.StatusOK, body)

	status, body = createQueueCall(t, ts, "QueueName", "orders")
	requireStatus(t, status, http.StatusBadRequest, body)
	requireAPIError(t, body, "QueueNameExists")
}

// FIFO queue names (.fifo) require and imply the FifoQueue attribute.
func TestCreateQueueFifoQueues(t *testing.T) {
	srv, ts := newTestServer(t)

	status, body := createQueueCall(t, ts,
		"QueueName", "jobs.fifo",
		"Attribute.1.name", "FifoQueue",
		"Attribute.1.value", "true",
	)
	requireStatus(t, status, http.StatusOK, body)
	url1 := requireQueueURL(t, body)
	if !strings.HasSuffix(url1, "/"+DefaultAccountID+"/jobs.fifo") {
		t.Fatalf("QueueUrl = %q, want it to keep the .fifo suffix", url1)
	}
	q, ok, err := srv.Store.Load("jobs.fifo")
	if err != nil || !ok {
		t.Fatalf("Load jobs.fifo: %v", err)
	}
	if got := q.Attributes[attrFifoQueue]; got != "true" {
		t.Fatalf("FifoQueue = %q, want true", got)
	}

	// Idempotent re-create of the FIFO queue.
	status, body = createQueueCall(t, ts,
		"QueueName", "jobs.fifo",
		"Attribute.1.name", "FifoQueue",
		"Attribute.1.value", "true",
	)
	requireStatus(t, status, http.StatusOK, body)
	if got := requireQueueURL(t, body); got != url1 {
		t.Fatalf("QueueUrl = %q, want %q", got, url1)
	}

	// A plain name is not allowed with FifoQueue=true.
	status, body = createQueueCall(t, ts,
		"QueueName", "plain",
		"Attribute.1.name", "FifoQueue",
		"Attribute.1.value", "true",
	)
	requireStatus(t, status, http.StatusBadRequest, body)
	requireAPIError(t, body, "InvalidParameterValue")

	// .fifo names require the FifoQueue attribute.
	status, body = createQueueCall(t, ts, "QueueName", "plain.fifo")
	requireStatus(t, status, http.StatusBadRequest, body)
	requireAPIError(t, body, "InvalidParameterValue")
}

// Request validation: names, attributes and missing parameters.
func TestCreateQueueValidation(t *testing.T) {
	_, ts := newTestServer(t)

	longName := strings.Repeat("x", 81)
	maxName := strings.Repeat("n", 80)

	tests := []struct {
		name     string
		pairs    []string
		wantCode string
	}{
		{"missing queue name", nil, "MissingParameter"},
		{"queue name too long", []string{"QueueName", longName}, "QueueFullNameLengthLimitExceeded"},
		{"invalid characters", []string{"QueueName", "bad*name"}, "InvalidParameterValue"},
		{"delay out of range", []string{"QueueName", "orders", "Attribute.1.name", "DelaySeconds", "Attribute.1.value", "901"}, "InvalidParameterValue"},
		{"delay not numeric", []string{"QueueName", "orders", "Attribute.1.name", "DelaySeconds", "Attribute.1.value", "often"}, "InvalidParameterValue"},
		{"unknown attribute", []string{"QueueName", "orders", "Attribute.1.name", "MessageGroupId"}, "InvalidAttributeName"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, body := createQueueCall(t, ts, tt.pairs...)
			requireStatus(t, status, http.StatusBadRequest, body)
			requireAPIError(t, body, tt.wantCode)
		})
	}

	// The 80-character boundary is accepted.
	status, body := createQueueCall(t, ts, "QueueName", maxName)
	requireStatus(t, status, http.StatusOK, body)
}

// The legacy attribute encoding (Attribute.entry.N.key/value) is
// supported for older AWS SDKs.
func TestParseAttributesAcceptsLegacyEncoding(t *testing.T) {
	attrs, apiErr := parseAttributes(url.Values{
		"QueueName":               {"orders"},
		"Attribute.entry.1.key":   {"DelaySeconds"},
		"Attribute.entry.1.value": {"10"},
	})
	if apiErr != nil {
		t.Fatalf("parseAttributes: %v", apiErr)
	}
	if got := attrs["DelaySeconds"]; got != "10" {
		t.Fatalf("DelaySeconds = %q, want 10", got)
	}
}

func TestParseAttributesCurrentEncoding(t *testing.T) {
	attrs, apiErr := parseAttributes(url.Values{
		"Attribute.1.name":  {"VisibilityTimeout"},
		"Attribute.1.value": {"45"},
		"Attribute.2.name":  {"RedriveAllowPolicy"},
		"Attribute.2.value": {"true"},
	})
	if apiErr != nil {
		t.Fatalf("parseAttributes: %v", apiErr)
	}
	if got := attrs["VisibilityTimeout"]; got != "45" {
		t.Fatalf("VisibilityTimeout = %q, want 45", got)
	}
	if got := attrs["RedriveAllowPolicy"]; got != "true" {
		t.Fatalf("RedriveAllowPolicy = %q, want true", got)
	}
}

func TestParseAttributesRequiresAttributeEntryName(t *testing.T) {
	_, apiErr := parseAttributes(url.Values{"Attribute.1.value": {"10"}})
	if apiErr == nil || apiErr.Code != "MissingParameter" {
		t.Fatalf("apiErr = %v, want MissingParameter", apiErr)
	}
}

// Redrive policies are canonical JSON: key order must not break
// idempotency.
func TestCreateQueueRedrivePolicyKeyOrderDoesNotBreakIdempotency(t *testing.T) {
	_, ts := newTestServer(t)

	policy1 := `{"maxReceiveCount": 5, "deadLetterTargetArn": "arn:aws:sqs:us-east-1:000000000000:dlq"}`
	policy2 := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq","maxReceiveCount":5}`

	status, body := createQueueCall(t, ts,
		"QueueName", "orders",
		"Attribute.1.name", "RedrivePolicy",
		"Attribute.1.value", policy1,
	)
	requireStatus(t, status, http.StatusOK, body)

	status, body = createQueueCall(t, ts,
		"QueueName", "orders",
		"Attribute.1.name", "RedrivePolicy",
		"Attribute.1.value", policy2,
	)
	requireStatus(t, status, http.StatusOK, body)

	status, body = createQueueCall(t, ts,
		"QueueName", "orders",
		"Attribute.1.name", "RedrivePolicy",
		"Attribute.1.value", `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq","maxReceiveCount":6}`,
	)
	requireStatus(t, status, http.StatusBadRequest, body)
	requireAPIError(t, body, "QueueNameExists")
}

func TestCreateQueueRedrivePolicyValidation(t *testing.T) {
	_, ts := newTestServer(t)
	status, body := createQueueCall(t, ts,
		"QueueName", "orders",
		"Attribute.1.name", "RedrivePolicy",
		"Attribute.1.value", `{"maxReceiveCount":5}`,
	)
	requireStatus(t, status, http.StatusBadRequest, body)
	requireAPIError(t, body, "InvalidParameterValue")
}

func TestServerUnknownAndMissingActions(t *testing.T) {
	_, ts := newTestServer(t)

	status, body := post(t, ts, actionForm("Banana"))
	requireStatus(t, status, http.StatusBadRequest, body)
	requireAPIError(t, body, "InvalidAction")

	status, body = post(t, ts, url.Values{})
	requireStatus(t, status, http.StatusBadRequest, body)
	requireAPIError(t, body, "MissingAction")
}

// GET with query parameters also reaches the action (some clients
// hand-build SQS URLs).
func TestCreateQueueAnswersGETToo(t *testing.T) {
	_, ts := newTestServer(t)
	resp, err := http.Get(ts.URL + "/?Action=CreateQueue&QueueName=getorders")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	requireStatus(t, resp.StatusCode, http.StatusOK, string(body))
	if got := requireQueueURL(t, string(body)); !strings.HasSuffix(got, "/"+DefaultAccountID+"/getorders") {
		t.Fatalf("QueueUrl = %q", got)
	}
}

// BaseURL replaces host-derived queue URL prefixes.
func TestQueueURLHonorsConfiguredBaseURL(t *testing.T) {
	st, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	srv := NewServerWithStore(st, "999999999999", "eu-central-1")
	srv.BaseURL = "http://floci.internal:4566/"

	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	status, body := createQueueCall(t, ts, "QueueName", "orders")
	requireStatus(t, status, http.StatusOK, body)
	if got := requireQueueURL(t, body); got != "http://floci.internal:4566/999999999999/orders" {
		t.Fatalf("QueueUrl = %q", got)
	}
}

func TestStoreGetOrCreateIsAtomic(t *testing.T) {
	st, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	q1 := &Queue{Name: "orders", Attributes: map[string]string{"DelaySeconds": "0"}}
	existing, created, err := st.GetOrCreate(q1)
	if err != nil || !created {
		t.Fatalf("first GetOrCreate: created=%v err=%v", created, err)
	}
	if existing != q1 {
		t.Fatal("first GetOrCreate must return the new queue")
	}
	if _, err := os.Stat(st.Path("orders")); err != nil {
		t.Fatalf("queue file missing: %v", err)
	}

	q2 := &Queue{Name: "orders", Attributes: map[string]string{"DelaySeconds": "5"}}
	existing, created, err = st.GetOrCreate(q2)
	if err != nil || created {
		t.Fatalf("second GetOrCreate: created=%v err=%v", created, err)
	}
	if got := existing.Attributes["DelaySeconds"]; got != "0" {
		t.Fatalf("existing queue attribute = %q, want 0", got)
	}

	if _, _, err := st.GetOrCreate(&Queue{Name: "../escape"}); err == nil {
		t.Fatal("invalid queue file names must be rejected")
	}
}

func TestStoreListQueues(t *testing.T) {
	srv, ts := newTestServer(t)
	createQueueCall(t, ts, "QueueName", "b")
	createQueueCall(t, ts, "QueueName", "a")
	queues, err := srv.Store.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(queues) != 2 {
		t.Fatalf("List found %d queues, want 2", len(queues))
	}
	if queues[0].Name != "a" || queues[1].Name != "b" {
		t.Fatalf("List order = %v, want [a b]", []string{queues[0].Name, queues[1].Name})
	}
}
