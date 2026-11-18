package sqs

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
)

var gqaTestNow = time.Unix(1700000000, 0)

type gqaTestMessage struct {
	visibleAt time.Time
	received  bool
}

func (m gqaTestMessage) MessageVisibility() (time.Time, bool) {
	return m.visibleAt, m.received
}

type gqaTestQueue struct {
	fifo     bool
	attrs    map[string]string
	messages []gqaTestMessage
	created  time.Time
	modified time.Time
}

func (q *gqaTestQueue) ConfiguredAttributes() map[string]string { return q.attrs }

func (q *gqaTestQueue) QueueMessages() []QueueMessageState {
	out := make([]QueueMessageState, 0, len(q.messages))
	for _, m := range q.messages {
		out = append(out, m)
	}
	return out
}

func (q *gqaTestQueue) QueueTimestamps() (time.Time, time.Time) { return q.created, q.modified }

func (q *gqaTestQueue) IsFIFO() bool { return q.fifo }

type gqaTestResolver struct {
	queue QueueSnapshot
	err   error
}

func (r *gqaTestResolver) Resolve(string) (QueueSnapshot, error) { return r.queue, r.err }

func gqaPostForm(handler http.HandlerFunc, form url.Values) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	handler(rec, req)
	return rec
}

func TestApproximateCounts(t *testing.T) {
	messages := []QueueMessageState{
		gqaTestMessage{},                                                        // deliverable now
		gqaTestMessage{visibleAt: gqaTestNow.Add(time.Second)},                  // send delay pending
		gqaTestMessage{visibleAt: gqaTestNow.Add(30 * time.Second), received: true}, // in flight
		gqaTestMessage{visibleAt: gqaTestNow.Add(-time.Second), received: true}, // visibility expired
	}
	counts := ApproximateCounts(messages, gqaTestNow)
	if counts.Visible != 2 {
		t.Errorf("Visible = %d, want 2", counts.Visible)
	}
	if counts.NotVisible != 1 {
		t.Errorf("NotVisible = %d, want 1", counts.NotVisible)
	}
	if counts.Delayed != 1 {
		t.Errorf("Delayed = %d, want 1", counts.Delayed)
	}
}

func TestQueueAttributesAll(t *testing.T) {
	q := &gqaTestQueue{
		attrs: map[string]string{
			"VisibilityTimeout": "45",
			"QueueArn":          "arn:aws:sqs:us-west-2:111122223333:orders",
		},
		messages: []gqaTestMessage{
			{},
			{visibleAt: gqaTestNow.Add(time.Second)},
			{visibleAt: gqaTestNow.Add(time.Minute), received: true},
		},
		created:  time.Unix(1600000000, 0),
		modified: time.Unix(1650000000, 0),
	}

	attributes, err := QueueAttributes(q, "https://sqs.us-west-2.amazonaws.com/111122223333/orders", []string{"All"}, gqaTestNow)
	if err != nil {
		t.Fatalf("QueueAttributes: unexpected error: %v", err)
	}

	for i := 1; i < len(attributes); i++ {
		if attributes[i-1].Name >= attributes[i].Name {
			t.Fatalf("All result not sorted: %q before %q", attributes[i-1].Name, attributes[i].Name)
		}
	}

	byName := make(map[string]string, len(attributes))
	for _, attribute := range attributes {
		if _, dup := byName[attribute.Name]; dup {
			t.Fatalf("duplicate attribute %s", attribute.Name)
		}
		byName[attribute.Name] = attribute.Value
	}

	want := map[string]string{
		"ApproximateNumberOfMessages":           "1",
		"ApproximateNumberOfMessagesNotVisible": "1",
		"ApproximateNumberOfMessagesDelayed":    "1",
		"VisibilityTimeout":                     "45",
		"DelaySeconds":                          "0",
		"MaximumMessageSize":                    "262144",
		"MessageRetentionPeriod":                "345600",
		"ReceiveMessageWaitTimeSeconds":         "0",
		"SqsManagedSseEnabled":                  "true",
		"QueueArn":                              "arn:aws:sqs:us-west-2:111122223333:orders",
		"CreatedTimestamp":                      "1600000000",
		"LastModifiedTimestamp":                 "1650000000",
	}
	for name, value := range want {
		if byName[name] != value {
			t.Errorf("%s = %q, want %q", name, byName[name], value)
		}
	}
	for _, name := range []string{"FifoQueue", "RedrivePolicy", "KmsMasterKeyId", "ContentBasedDeduplication"} {
		if _, ok := byName[name]; ok {
			t.Errorf("attribute %s unexpectedly present on a standard queue", name)
		}
	}
}

func TestQueueAttributesFIFO(t *testing.T) {
	q := &gqaTestQueue{
		fifo:  true,
		attrs: map[string]string{"DeduplicationScope": "message"},
	}
	names := []string{"FifoThroughputLimit", "DeduplicationScope", "ContentBasedDeduplication", "FifoQueue"}
	attributes, err := QueueAttributes(q, "https://sqs.us-east-1.amazonaws.com/111122223333/queue.fifo", names, gqaTestNow)
	if err != nil {
		t.Fatalf("QueueAttributes: unexpected error: %v", err)
	}
	if len(attributes) != len(names) {
		t.Fatalf("got %d attributes, want %d", len(attributes), len(names))
	}
	for i, attribute := range attributes {
		if attribute.Name != names[i] {
			t.Errorf("attributes[%d] = %s, want %s (request order preserved)", i, attribute.Name, names[i])
		}
	}
	values := map[string]string{}
	for _, attribute := range attributes {
		values[attribute.Name] = attribute.Value
	}
	if values["FifoQueue"] != "true" {
		t.Errorf("FifoQueue = %q, want %q", values["FifoQueue"], "true")
	}
	if values["ContentBasedDeduplication"] != "false" {
		t.Errorf("ContentBasedDeduplication = %q, want default false", values["ContentBasedDeduplication"])
	}
	if values["DeduplicationScope"] != "message" {
		t.Errorf("DeduplicationScope = %q, want configured value", values["DeduplicationScope"])
	}
	if values["FifoThroughputLimit"] != "perQueue" {
		t.Errorf("FifoThroughputLimit = %q, want default perQueue", values["FifoThroughputLimit"])
	}
}

func TestQueueAttributesExplicitNames(t *testing.T) {
	q := &gqaTestQueue{}
	url := "http://localhost:4566/000000000000/letters"
	names := []string{"DelaySeconds", "VisibilityTimeout", "DelaySeconds"}
	attributes, err := QueueAttributes(q, url, names, gqaTestNow)
	if err != nil {
		t.Fatalf("QueueAttributes: unexpected error: %v", err)
	}
	if len(attributes) != 2 {
		t.Fatalf("got %d attributes, want 2 (deduplicated)", len(attributes))
	}
	if attributes[0].Name != "DelaySeconds" || attributes[0].Value != "0" {
		t.Errorf("attributes[0] = %s=%s, want DelaySeconds=0", attributes[0].Name, attributes[0].Value)
	}
	if attributes[1].Name != "VisibilityTimeout" || attributes[1].Value != "30" {
		t.Errorf("attributes[1] = %s=%s, want VisibilityTimeout=30", attributes[1].Name, attributes[1].Value)
	}

	// Conditional attributes that were never configured are omitted, not defaulted.
	attributes, err = QueueAttributes(q, url, []string{"RedrivePolicy"}, gqaTestNow)
	if err != nil {
		t.Fatalf("QueueAttributes(RedrivePolicy): unexpected error: %v", err)
	}
	if len(attributes) != 0 {
		t.Errorf("unset RedrivePolicy returned %v, want omitted", attributes)
	}
}

func TestQueueAttributesUnknownName(t *testing.T) {
	q := &gqaTestQueue{}
	_, err := QueueAttributes(q, "http://localhost:4566/000000000000/q", []string{"NotAnAttribute"}, gqaTestNow)
	if err == nil {
		t.Fatal("expected InvalidAttributeName error")
	}
	if got := err.Error(); got != "Unknown Attribute NotAnAttribute." {
		t.Errorf("error message = %q", got)
	}
}

func TestQueueAttributesARNFallback(t *testing.T) {
	q := &gqaTestQueue{}
	attributes, err := QueueAttributes(q, "http://localhost:4566/000000000000/letters", []string{"QueueArn"}, gqaTestNow)
	if err != nil {
		t.Fatalf("QueueAttributes: unexpected error: %v", err)
	}
	if len(attributes) != 1 || attributes[0].Value != "arn:aws:sqs:us-east-1:000000000000:letters" {
		t.Errorf("QueueArn fallback = %v, want arn:aws:sqs:us-east-1:000000000000:letters", attributes)
	}
}

func TestGqaNamesFromForm(t *testing.T) {
	form := url.Values{}
	form.Set("AttributeName.2", "QueueArn")
	form.Set("AttributeName.1", "QueueArn")
	form.Set("AttributeName.3", "DelaySeconds")
	form.Set("AttributeName.member.4", "VisibilityTimeout")
	form.Set("Unrelated.1", "ignoreme")
	names, err := gqaNamesFromForm(form)
	if err != nil {
		t.Fatalf("gqaNamesFromForm: unexpected error: %v", err)
	}
	want := []string{"QueueArn", "DelaySeconds", "VisibilityTimeout"}
	if !reflect.DeepEqual(names, want) {
		t.Errorf("names = %v, want %v", names, want)
	}

	bad := url.Values{"AttributeName.1": []string{"Nope"}}
	if _, err := gqaNamesFromForm(bad); err == nil || err.Error() != "Unknown Attribute Nope." {
		t.Errorf("invalid name error = %v", err)
	}
}

func TestGetQueueAttributesHappyPath(t *testing.T) {
	q := &gqaTestQueue{
		attrs: map[string]string{"VisibilityTimeout": "60"},
		messages: []gqaTestMessage{
			{},
			{},
			{visibleAt: gqaTestNow.Add(time.Minute), received: true},
		},
	}
	handler := GetQueueAttributes(&gqaTestResolver{queue: q})

	form := url.Values{}
	form.Set("QueueUrl", "http://localhost:4566/000000000000/demo")
	form.Set("AttributeName.1", "ApproximateNumberOfMessages")
	form.Set("AttributeName.2", "ApproximateNumberOfMessagesNotVisible")
	form.Set("AttributeName.3", "VisibilityTimeout")

	rec := gqaPostForm(handler, form)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "text/xml" {
		t.Errorf("Content-Type = %q, want text/xml", got)
	}
	body := rec.Body.String()
	for _, fragment := range []string{
		"<?xml version=\"1.0\"?>",
		"<GetQueueAttributesResponse xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\">",
		"<Attribute><Name>ApproximateNumberOfMessages</Name><Value>2</Value></Attribute>",
		"<Attribute><Name>ApproximateNumberOfMessagesNotVisible</Name><Value>1</Value></Attribute>",
		"<Attribute><Name>VisibilityTimeout</Name><Value>60</Value></Attribute>",
		"</GetQueueAttributesResult>",
		"<ResponseMetadata><RequestId>",
	} {
		if !strings.Contains(body, fragment) {
			t.Errorf("response body missing %q\nbody: %s", fragment, body)
		}
	}
}

func TestGetQueueAttributesOmittedAttributeNames(t *testing.T) {
	q := &gqaTestQueue{attrs: map[string]string{"VisibilityTimeout": "60"}}
	handler := GetQueueAttributes(&gqaTestResolver{queue: q})
	form := url.Values{}
	form.Set("QueueUrl", "http://localhost:4566/000000000000/demo")
	rec := gqaPostForm(handler, form)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "<GetQueueAttributesResult></GetQueueAttributesResult>") {
		t.Errorf("omitted AttributeNames should yield an empty result, body: %s", rec.Body.String())
	}
}

func TestGetQueueAttributesMissingQueueURL(t *testing.T) {
	handler := GetQueueAttributes(&gqaTestResolver{})
	rec := gqaPostForm(handler, url.Values{})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "<Code>MissingParameter</Code>") || !strings.Contains(body, "QueueUrl") {
		t.Errorf("body missing MissingParameter detail: %s", body)
	}
}

func TestGetQueueAttributesQueueDoesNotExist(t *testing.T) {
	handler := GetQueueAttributes(&gqaTestResolver{err: &QueueNotFoundError{QueueURL: "http://localhost:4566/000000000000/ghost"}})
	form := url.Values{}
	form.Set("QueueUrl", "http://localhost:4566/000000000000/ghost")
	form.Set("AttributeName.1", "All")
	rec := gqaPostForm(handler, form)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "<Code>QueueDoesNotExist</Code>") || !strings.Contains(body, "wsdl version") {
		t.Errorf("body missing QueueDoesNotExist detail: %s", body)
	}
}

func TestGetQueueAttributesUnknownAttributeOnWire(t *testing.T) {
	q := &gqaTestQueue{}
	handler := GetQueueAttributes(&gqaTestResolver{queue: q})
	form := url.Values{}
	form.Set("QueueUrl", "http://localhost:4566/000000000000/demo")
	form.Set("AttributeName.1", "Nope")
	rec := gqaPostForm(handler, form)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "<Code>InvalidAttributeName</Code>") {
		t.Errorf("body missing InvalidAttributeName: %s", rec.Body.String())
	}
}

func TestGetQueueAttributesResolverFailure(t *testing.T) {
	handler := GetQueueAttributes(&gqaTestResolver{err: errors.New("boom")})
	form := url.Values{}
	form.Set("QueueUrl", "http://localhost:4566/000000000000/demo")
	rec := gqaPostForm(handler, form)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "<Code>InternalError</Code>") {
		t.Errorf("body missing InternalError: %s", rec.Body.String())
	}
}
