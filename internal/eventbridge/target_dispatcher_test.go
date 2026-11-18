package eventbridge

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// recordingQueue is a QueueSender double capturing SendMessage calls.
type recordingQueue struct {
	arns   []string
	bodies []string
}

func (r *recordingQueue) SendMessage(_ context.Context, queueARN, body string, _ map[string]string) (string, error) {
	r.arns = append(r.arns, queueARN)
	r.bodies = append(r.bodies, body)
	return "sqs-message-id", nil
}

// recordingInvoker is a LambdaInvoker double capturing payloads.
type recordingInvoker struct {
	payloads [][]byte
}

func (r *recordingInvoker) Invoke(_ context.Context, _ string, payload []byte) error {
	r.payloads = append(r.payloads, payload)
	return nil
}

// quietDispatcher keeps failure notices out of the test log.
func quietDispatcher(d *TargetDispatcher) *TargetDispatcher {
	d.Log = log.New(io.Discard, "", 0)
	return d
}

func sampleDispatchEvent() map[string]any {
	return map[string]any{
		"version":     "0",
		"id":          "6a7e8feb-b491-4cf7-a9f1-bf3703467718",
		"detail-type": "OrderPlaced",
		"source":      "com.example.orders",
		"account":     "111122223333",
		"time":        "2026-02-14T10:00:00Z",
		"region":      "us-east-1",
		"resources":   []any{"arn:aws:sqs:us-east-1:111122223333:orders"},
		"detail":      map[string]any{"orderId": "42"},
	}
}

func TestDispatchSQSRoutesThroughAdapter(t *testing.T) {
	q := &recordingQueue{}
	d := quietDispatcher(NewTargetDispatcher(q))
	evt := sampleDispatchEvent()
	want, err := json.Marshal(evt)
	if err != nil {
		t.Fatal(err)
	}

	results := d.Dispatch(context.Background(), evt,
		&Target{ID: "orders", ARN: "arn:aws:sqs:us-east-1:111122223333:orders"})
	if len(results) != 1 {
		t.Fatalf("results = %d, want 1", len(results))
	}
	r := results[0]
	if !r.OK() {
		t.Fatalf("status = %q, err = %v, want delivered", r.Status, r.Err)
	}
	if r.Kind != TargetKindSQS {
		t.Fatalf("kind = %q, want sqs", r.Kind)
	}
	if r.MessageID != "sqs-message-id" {
		t.Fatalf("message id = %q, want sqs-message-id", r.MessageID)
	}
	if len(q.arns) != 1 || q.arns[0] != "arn:aws:sqs:us-east-1:111122223333:orders" {
		t.Fatalf("adapter arns = %v, want the target queue", q.arns)
	}
	if len(q.bodies) != 1 || q.bodies[0] != string(want) {
		t.Fatalf("body = %s, want %s", q.bodies[0], want)
	}
}

func TestDispatchSQSWithoutAdapterFails(t *testing.T) {
	d := quietDispatcher(&TargetDispatcher{})
	results := d.Dispatch(context.Background(), sampleDispatchEvent(),
		&Target{ID: "orders", ARN: "arn:aws:sqs:us-east-1:111122223333:orders"})
	if results[0].OK() {
		t.Fatal("delivery succeeded without an SQS adapter, want failure")
	}
	if !errors.Is(results[0].Err, ErrNoQueueSender) {
		t.Fatalf("err = %v, want ErrNoQueueSender", results[0].Err)
	}
}

func TestDispatchLambdaFailsLoudly(t *testing.T) {
	d := quietDispatcher(NewTargetDispatcher(&recordingQueue{}))
	evt := sampleDispatchEvent()
	target := &Target{ID: "fn", ARN: "arn:aws:lambda:us-east-1:111122223333:function:orders"}

	results := d.Dispatch(context.Background(), evt, target)
	r := results[0]
	if r.Status != DispatchNotImplemented {
		t.Fatalf("status = %q, want not-implemented", r.Status)
	}
	if !errors.Is(r.Err, ErrLambdaNotImplemented) {
		t.Fatalf("err = %v, want ErrLambdaNotImplemented", r.Err)
	}
	if _, err := d.Strict(context.Background(), evt, target); !errors.Is(err, ErrLambdaNotImplemented) {
		t.Fatalf("Strict err = %v, want ErrLambdaNotImplemented", err)
	}
}

func TestDispatchLambdaThroughInvoker(t *testing.T) {
	inv := &recordingInvoker{}
	d := quietDispatcher(&TargetDispatcher{Queues: &recordingQueue{}, Lambda: inv})
	evt := sampleDispatchEvent()
	want, err := json.Marshal(evt)
	if err != nil {
		t.Fatal(err)
	}

	results := d.Dispatch(context.Background(), evt,
		&Target{ID: "fn", ARN: "arn:aws:lambda:us-east-1:111122223333:function:orders"})
	if !results[0].OK() {
		t.Fatalf("status = %q, err = %v, want delivered", results[0].Status, results[0].Err)
	}
	if len(inv.payloads) != 1 || string(inv.payloads[0]) != string(want) {
		t.Fatalf("payload = %s, want %s", inv.payloads[0], want)
	}
}

func TestDispatchHTTPPostsEvent(t *testing.T) {
	evt := sampleDispatchEvent()
	var gotMethod, gotPath, gotCT, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotCT = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	d := quietDispatcher(&TargetDispatcher{})

	results := d.Dispatch(context.Background(), evt,
		&Target{ID: "hook", URL: srv.URL + "/hooks/orders"})
	if len(results) != 1 || !results[0].OK() {
		t.Fatalf("results = %+v, want delivered", results)
	}
	if gotMethod != http.MethodPost {
		t.Fatalf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/hooks/orders" {
		t.Fatalf("path = %q, want /hooks/orders", gotPath)
	}
	if gotCT != "application/json" {
		t.Fatalf("content type = %q, want application/json", gotCT)
	}
	want, _ := json.Marshal(evt)
	if gotBody != string(want) {
		t.Fatalf("body = %s, want %s", gotBody, want)
	}
}

func TestDispatchHTTPFailsOnNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	d := quietDispatcher(&TargetDispatcher{})

	results := d.Dispatch(context.Background(), sampleDispatchEvent(),
		&Target{ID: "hook", URL: srv.URL})
	r := results[0]
	if r.OK() || r.Err == nil || !strings.Contains(r.Err.Error(), "500") {
		t.Fatalf("result = %+v, want failed with status 500", r)
	}
}

func TestDispatchIsolatesTargets(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	d := quietDispatcher(&TargetDispatcher{Queues: &recordingQueue{}})
	targets := []*Target{
		{ID: "queue", ARN: "arn:aws:sqs:us-east-1:111122223333:orders"},
		{ID: "fn", ARN: "arn:aws:lambda:us-east-1:111122223333:function:orders"},
		{ID: "hook", URL: srv.URL},
		{ID: "bogus", ARN: "arn:aws:dynamodb:us-east-1:111122223333:table/orders"},
	}

	results := d.Dispatch(context.Background(), sampleDispatchEvent(), targets...)
	if len(results) != 4 {
		t.Fatalf("results = %d, want 4", len(results))
	}
	if !results[0].OK() {
		t.Fatalf("sqs result = %+v, want delivered", results[0])
	}
	if results[1].Status != DispatchNotImplemented {
		t.Fatalf("lambda result = %+v, want not-implemented", results[1])
	}
	if !results[2].OK() {
		t.Fatalf("http result = %+v, want delivered", results[2])
	}
	if results[3].Status != DispatchFailed || !errors.Is(results[3].Err, ErrUnknownTarget) {
		t.Fatalf("unknown result = %+v, want failed with ErrUnknownTarget", results[3])
	}
	delivered := 0
	for _, r := range results {
		if r.OK() {
			delivered++
		}
	}
	if delivered != 2 {
		t.Fatalf("delivered = %d, want 2", delivered)
	}
	if _, err := d.Strict(context.Background(), sampleDispatchEvent(), targets...); !errors.Is(err, ErrLambdaNotImplemented) {
		t.Fatalf("Strict err = %v, want the first failure ErrLambdaNotImplemented", err)
	}
}

func TestTargetKind(t *testing.T) {
	cases := []struct {
		name string
		t    *Target
		want TargetKind
	}{
		{"sqs arn", &Target{ARN: "arn:aws:sqs:eu-west-1:123456789012:events"}, TargetKindSQS},
		{"lambda arn", &Target{ARN: "arn:aws:lambda:eu-west-1:123456789012:function:fn"}, TargetKindLambda},
		{"https url", &Target{URL: "https://example.com/hooks"}, TargetKindHTTP},
		{"http url", &Target{URL: "http://127.0.0.1:8080/hooks"}, TargetKindHTTP},
		{"url wins over arn", &Target{ARN: "arn:aws:sqs:eu-west-1:123456789012:events", URL: "https://example.com/hooks"}, TargetKindHTTP},
		{"other service", &Target{ARN: "arn:aws:dynamodb:eu-west-1:123456789012:table/t"}, TargetKindUnknown},
		{"not an arn", &Target{ARN: "not-an-arn"}, TargetKindUnknown},
		{"nil target", nil, TargetKindUnknown},
	}
	for _, tc := range cases {
		if got := tc.t.Kind(); got != tc.want {
			t.Errorf("%s: kind = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestTargetInputConstantReplacesEvent(t *testing.T) {
	q := &recordingQueue{}
	d := quietDispatcher(NewTargetDispatcher(q))
	results := d.Dispatch(context.Background(), sampleDispatchEvent(),
		&Target{ID: "constant", ARN: "arn:aws:sqs:us-east-1:111122223333:orders", Input: `{"constant":true}`})
	if !results[0].OK() {
		t.Fatalf("result = %+v, want delivered", results[0])
	}
	if q.bodies[0] != `{"constant":true}` {
		t.Fatalf("body = %s, want the Input constant", q.bodies[0])
	}
}

func TestTargetInputInvalidJSONFails(t *testing.T) {
	d := quietDispatcher(NewTargetDispatcher(&recordingQueue{}))
	results := d.Dispatch(context.Background(), sampleDispatchEvent(),
		&Target{ID: "bad", ARN: "arn:aws:sqs:us-east-1:111122223333:orders", Input: "{not json"})
	if results[0].OK() {
		t.Fatal("delivery succeeded with invalid Input, want failure")
	}
}
