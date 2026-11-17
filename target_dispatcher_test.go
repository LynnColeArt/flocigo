package floci

import (
    "context"
    "encoding/json"
    "errors"
    "io"
    "net/http"
    "net/http/httptest"
    "strings"
    "sync"
    "testing"
)

func newTestDispatcher(t *testing.T, sqs SQSSender) *TargetDispatcher {
    t.Helper()
    d, err := NewTargetDispatcher(
        EventEncoderFunc(func(event any) ([]byte, error) { return json.Marshal(event) }),
        sqs,
        nil,
    )
    if err != nil {
        t.Fatalf("NewTargetDispatcher: %v", err)
    }
    return d
}

type recordingSQS struct {
    mu    sync.Mutex
    calls []sqsSend
    err   error
}

type sqsSend struct {
    queue string
    body  string
}

func (r *recordingSQS) SendMessage(ctx context.Context, queue, body string) error {
    r.mu.Lock()
    defer r.mu.Unlock()
    r.calls = append(r.calls, sqsSend{queue: queue, body: body})
    return r.err
}

func (r *recordingSQS) sends() []sqsSend {
    r.mu.Lock()
    defer r.mu.Unlock()
    return append([]sqsSend(nil), r.calls...)
}

func TestTargetDispatcherRouteHTTP(t *testing.T) {
    var mu sync.Mutex
    var method, contentType, body string
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        mu.Lock()
        defer mu.Unlock()
        method = r.Method
        contentType = r.Header.Get("Content-Type")
        raw, _ := io.ReadAll(r.Body)
        body = string(raw)
        w.WriteHeader(http.StatusOK)
    }))
    defer srv.Close()

    d := newTestDispatcher(t, &recordingSQS{})
    if err := d.Register(Target{ID: "http-1", URL: srv.URL}); err != nil {
        t.Fatalf("register: %v", err)
    }
    if err := d.Dispatch(context.Background(), map[string]any{"source": "floci.test", "detail-type": "order.created"}, "http-1"); err != nil {
        t.Fatalf("dispatch: %v", err)
    }

    mu.Lock()
    defer mu.Unlock()
    if method != http.MethodPost {
        t.Fatalf("want POST, got %s", method)
    }
    if contentType != "application/json" {
        t.Fatalf("want content-type application/json, got %q", contentType)
    }
    want := `{"detail-type":"order.created","source":"floci.test"}`
    if body != want {
        t.Fatalf("want body %s, got %s", want, body)
    }
}

func TestTargetDispatcherRouteSQSByArn(t *testing.T) {
    rec := &recordingSQS{}
    d := newTestDispatcher(t, rec)
    if err := d.Register(Target{ID: "sqs-1", ARN: "arn:aws:sqs:us-east-1:000000000000:orders"}); err != nil {
        t.Fatalf("register: %v", err)
    }
    if err := d.Dispatch(context.Background(), map[string]any{"id": "e-1"}, "sqs-1"); err != nil {
        t.Fatalf("dispatch: %v", err)
    }
    calls := rec.sends()
    if len(calls) != 1 {
        t.Fatalf("want 1 SendMessage, got %d", len(calls))
    }
    if calls[0].queue != "orders" {
        t.Fatalf("want queue orders, got %q", calls[0].queue)
    }
    if calls[0].body != `{"id":"e-1"}` {
        t.Fatalf("unexpected sqs body %q", calls[0].body)
    }
}

func TestTargetDispatcherRouteSQSByQueueURL(t *testing.T) {
    rec := &recordingSQS{}
    d := newTestDispatcher(t, rec)
    q := "http://localhost:4566/000000000000/orders"
    if err := d.Register(Target{ID: "sqs-2", QueueURL: q}); err != nil {
        t.Fatalf("register: %v", err)
    }
    if err := d.Dispatch(context.Background(), map[string]any{"id": "e-2"}, "sqs-2"); err != nil {
        t.Fatalf("dispatch: %v", err)
    }
    calls := rec.sends()
    if len(calls) != 1 || calls[0].queue != q {
        t.Fatalf("unexpected sends: %+v", calls)
    }
}

func TestTargetDispatcherRouteLambdaFailsLoudly(t *testing.T) {
    rec := &recordingSQS{}
    d := newTestDispatcher(t, rec)
    // Registering a lambda target is allowed; only routing is deferred.
    if err := d.Register(Target{ID: "lambda-1", ARN: "arn:aws:lambda:us-east-1:000000000000:function:orders"}); err != nil {
        t.Fatalf("register: %v", err)
    }
    err := d.Dispatch(context.Background(), map[string]any{"id": "e-3"}, "lambda-1")
    if err == nil {
        t.Fatal("want loud failure for lambda target")
    }
    if !errors.Is(err, ErrNotImplemented) || !errors.Is(err, ErrLambdaNotImplemented) {
        t.Fatalf("want ErrLambdaNotImplemented, got %v", err)
    }
    msg := err.Error()
    if !strings.Contains(msg, "lambda") || !strings.Contains(msg, "not implemented") {
        t.Fatalf("error should name lambda invoke as not implemented: %s", msg)
    }
    if len(rec.sends()) != 0 {
        t.Fatal("no SQS send should happen for a lambda target")
    }
}

func TestTargetDispatcherContinuesAndAggregatesFailures(t *testing.T) {
    var mu sync.Mutex
    hits := 0
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        mu.Lock()
        hits++
        mu.Unlock()
        w.WriteHeader(http.StatusOK)
    }))
    defer srv.Close()

    rec := &recordingSQS{err: errors.New("sqs down")}
    d := newTestDispatcher(t, rec)
    if err := d.Register(Target{ID: "queue", QueueURL: "http://localhost:4566/000000000000/orders"}); err != nil {
        t.Fatalf("register: %v", err)
    }
    if err := d.Register(Target{ID: "endpoint", URL: srv.URL}); err != nil {
        t.Fatalf("register: %v", err)
    }
    if err := d.Register(Target{ID: "fn", ARN: "arn:aws:lambda:us-east-1:000000000000:function:orders"}); err != nil {
        t.Fatalf("register: %v", err)
    }
    err := d.Dispatch(context.Background(), map[string]any{"id": "e-4"}, "queue", "endpoint", "fn")
    if err == nil {
        t.Fatal("want aggregate error")
    }
    mu.Lock()
    got := hits
    mu.Unlock()
    if got != 1 {
        t.Fatalf("http target must still be delivered after the sqs failure, hits=%d", got)
    }
    if !strings.Contains(err.Error(), `target "queue"`) || !strings.Contains(err.Error(), `target "fn"`) {
        t.Fatalf("aggregate error should name both failing targets: %s", err)
    }
    if len(rec.sends()) != 1 {
        t.Fatalf("want 1 sqs attempt, got %d", len(rec.sends()))
    }
}

func TestTargetDispatcherHTTPNon2xxFails(t *testing.T) {
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusInternalServerError)
    }))
    defer srv.Close()

    d := newTestDispatcher(t, &recordingSQS{})
    if err := d.Register(Target{ID: "http-bad", URL: srv.URL}); err != nil {
        t.Fatalf("register: %v", err)
    }
    err := d.Dispatch(context.Background(), map[string]any{"id": "e-5"}, "http-bad")
    if err == nil || !strings.Contains(err.Error(), "500") {
        t.Fatalf("want 500 failure, got %v", err)
    }
}

type failingEncoder struct{}

func (failingEncoder) EncodeEvent(event any) ([]byte, error) {
    return nil, errors.New("bad event")
}

func TestTargetDispatcherEncodeErrorIsReported(t *testing.T) {
    rec := &recordingSQS{}
    d, err := NewTargetDispatcher(failingEncoder{}, rec, nil)
    if err != nil {
        t.Fatalf("NewTargetDispatcher: %v", err)
    }
    if err := d.Register(Target{ID: "queue", QueueURL: "http://localhost:4566/000000000000/orders"}); err != nil {
        t.Fatalf("register: %v", err)
    }
    if err := d.Dispatch(context.Background(), map[string]any{"id": "e-6"}, "queue"); err == nil {
        t.Fatal("want encoder error")
    }
    if len(rec.sends()) != 0 {
        t.Fatal("no target should be reached when encoding fails")
    }
}

func TestTargetDispatcherRequiresAdapters(t *testing.T) {
    if _, err := NewTargetDispatcher(nil, SQSSenderFunc(func(context.Context, string, string) error { return nil }), nil); err == nil {
        t.Fatal("want error for nil json adapter")
    }
    if _, err := NewTargetDispatcher(EventEncoderFunc(func(any) ([]byte, error) { return nil, nil }), nil, nil); err == nil {
        t.Fatal("want error for nil sqs adapter")
    }
}

func TestTargetDispatcherUnknownTarget(t *testing.T) {
    d := newTestDispatcher(t, &recordingSQS{})
    err := d.Dispatch(context.Background(), map[string]any{"id": "e-7"}, "missing")
    if err == nil || !strings.Contains(err.Error(), `unknown target "missing"`) {
        t.Fatalf("want unknown-target error, got %v", err)
    }
}

func TestTargetKindInference(t *testing.T) {
    cases := []struct {
        name   string
        target Target
        want   TargetKind
        err    bool
    }{
        {"sqs arn", Target{ID: "a", ARN: "arn:aws:sqs:us-east-1:1:orders"}, TargetKindSQS, false},
        {"lambda arn", Target{ID: "b", ARN: "arn:aws:lambda:us-east-1:1:function:orders"}, TargetKindLambda, false},
        {"http url", Target{ID: "c", URL: "https://example.com/hook"}, TargetKindHTTP, false},
        {"explicit kind", Target{ID: "d", Kind: TargetKindHTTP, URL: "https://example.com/hook"}, TargetKindHTTP, false},
        {"inference impossible", Target{ID: "e"}, "", true},
        {"unknown kind", Target{ID: "f", Kind: "carrier-pigeon", URL: "https://example.com"}, "", true},
    }
    for _, tc := range cases {
        got, err := tc.target.ResolvedKind()
        if tc.err {
            if err == nil {
                t.Fatalf("%s: want inference failure, got %s", tc.name, got)
            }
            continue
        }
        if err != nil {
            t.Fatalf("%s: %v", tc.name, err)
        }
        if got != tc.want {
            t.Fatalf("%s: want %s, got %s", tc.name, tc.want, got)
        }
    }
}

func TestNewTargetClassification(t *testing.T) {
    sqs, err := NewTarget("a", "arn:aws:sqs:us-east-1:1:orders")
    if err != nil || sqs.Kind != TargetKindSQS {
        t.Fatalf("sqs classification: %v %s", err, sqs.Kind)
    }
    fn, err := NewTarget("b", "arn:aws:lambda:us-east-1:1:function:orders")
    if err != nil || fn.Kind != TargetKindLambda {
        t.Fatalf("lambda classification: %v %s", err, fn.Kind)
    }
    httpT, err := NewTarget("c", "https://example.com/hook")
    if err != nil || httpT.Kind != TargetKindHTTP || httpT.URL != "https://example.com/hook" {
        t.Fatalf("http classification: %v %s %s", err, httpT.Kind, httpT.URL)
    }
    if _, err := NewTarget("d", "not-a-destination"); err == nil {
        t.Fatal("want error for unclassifiable destination")
    }
}

func TestTargetDispatcherRegistrationOrder(t *testing.T) {
    d := newTestDispatcher(t, &recordingSQS{})
    for _, id := range []string{"a", "b", "c"} {
        if err := d.Register(Target{ID: id, QueueURL: "http://localhost:4566/000000000000/" + id}); err != nil {
            t.Fatalf("register %s: %v", id, err)
        }
    }
    ids := make([]string, 0, 3)
    for _, tg := range d.Targets() {
        ids = append(ids, tg.ID)
    }
    if strings.Join(ids, ",") != "a,b,c" {
        t.Fatalf("want registration order preserved, got %v", ids)
    }
}

func TestTargetDispatcherRegisterValidation(t *testing.T) {
    d := newTestDispatcher(t, &recordingSQS{})
    if err := d.Register(Target{ID: "", URL: "https://example.com"}); err == nil {
        t.Fatal("want error for missing id")
    }
    if err := d.Register(Target{ID: "x", Kind: TargetKindHTTP}); err == nil {
        t.Fatal("want error for http target without url")
    }
    if err := d.Register(Target{ID: "x", Kind: TargetKindSQS}); err == nil {
        t.Fatal("want error for sqs target without queue")
    }
    if err := d.Register(Target{ID: "x", Kind: TargetKindLambda}); err == nil {
        t.Fatal("want error for lambda target without arn")
    }
    if err := d.Register(Target{ID: "x", Kind: TargetKindSQS, QueueURL: "http://q/orders"}); err != nil {
        t.Fatalf("valid sqs target rejected: %v", err)
    }
}
