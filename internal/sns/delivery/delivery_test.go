package delivery

import (
    "context"
    "encoding/json"
    "errors"
    "io"
    "net/http"
    "net/http/httptest"
    "net/url"
    "strings"
    "sync"
    "testing"
    "time"
)

var _ SQSAdapter = (*fakeSQS)(nil)

func strValue(s string) *string { return &s }

func testMessage() Message {
    return Message{
        TopicARN:  "arn:aws:sns:us-east-1:123456789012:mytopic",
        MessageID: "11111111-2222-3333-4444-555555555555",
        Subject:   "order",
        Body:      "hello world",
        Timestamp: time.Date(2026, 2, 16, 10, 11, 12, 0, time.UTC),
    }
}

func sqsSubscription(raw bool) Subscription {
    return Subscription{
        SubscriptionARN:    "arn:aws:sns:us-east-1:123456789012:mytopic:f38fd1f5-9db3-4f8b-a1c2-2f2f2f2f2f2f",
        TopicARN:           "arn:aws:sns:us-east-1:123456789012:mytopic",
        Protocol:           ProtocolSQS,
        Endpoint:           "arn:aws:sqs:us-east-1:123456789012:myqueue",
        RawMessageDelivery: raw,
    }
}

type sqsCall struct {
    queueARN string
    body     string
    attrs    map[string]MessageAttributeValue
}

type fakeSQS struct {
    mu    sync.Mutex
    calls []sqsCall
}

func (f *fakeSQS) SendMessage(_ context.Context, queueARN, body string, attrs map[string]MessageAttributeValue) error {
    f.mu.Lock()
    defer f.mu.Unlock()
    var stored map[string]MessageAttributeValue
    if attrs != nil {
        stored = make(map[string]MessageAttributeValue, len(attrs))
        for k, v := range attrs {
            stored[k] = v
        }
    }
    f.calls = append(f.calls, sqsCall{queueARN: queueARN, body: body, attrs: stored})
    return nil
}

func (f *fakeSQS) count() int {
    f.mu.Lock()
    defer f.mu.Unlock()
    return len(f.calls)
}

func TestFanoutSQSEnvelope(t *testing.T) {
    adapter := &fakeSQS{}
    d := NewDispatcher(adapter)

    res := d.Fanout(context.Background(), testMessage(), []Subscription{sqsSubscription(false)})

    if len(res) != 1 || !res[0].Matched || res[0].Err != nil {
        t.Fatalf("unexpected fanout result: %+v", res)
    }
    if adapter.count() != 1 {
        t.Fatalf("adapter calls = %d, want 1", adapter.count())
    }
    call := adapter.calls[0]
    if call.queueARN != "arn:aws:sqs:us-east-1:123456789012:myqueue" {
        t.Errorf("queue ARN = %q", call.queueARN)
    }
    var env Envelope
    if err := json.Unmarshal([]byte(call.body), &env); err != nil {
        t.Fatalf("SQS body is not an SNS envelope: %v", err)
    }
    if env.Type != "Notification" {
        t.Errorf("Type = %q, want Notification", env.Type)
    }
    if env.MessageId != "11111111-2222-3333-4444-555555555555" {
        t.Errorf("MessageId = %q", env.MessageId)
    }
    if env.TopicArn != "arn:aws:sns:us-east-1:123456789012:mytopic" {
        t.Errorf("TopicArn = %q", env.TopicArn)
    }
    if env.Subject != "order" {
        t.Errorf("Subject = %q", env.Subject)
    }
    if env.Message != "hello world" {
        t.Errorf("Message = %q", env.Message)
    }
    if env.Timestamp != "2026-02-16T10:11:12.000Z" {
        t.Errorf("Timestamp = %q", env.Timestamp)
    }
    if call.attrs != nil {
        t.Errorf("non-raw SQS delivery must not forward message attributes")
    }
}

func TestFanoutSQSRawDelivery(t *testing.T) {
    adapter := &fakeSQS{}
    d := NewDispatcher(adapter)

    msg := testMessage()
    msg.Subject = ""
    msg.Body = "raw body"
    msg.Attributes = map[string]MessageAttributeValue{
        "color": {DataType: "String", StringValue: strValue("blue")},
        "count": {DataType: "Number", StringValue: strValue("3")},
    }

    res := d.Fanout(context.Background(), msg, []Subscription{sqsSubscription(true)})

    if res[0].Err != nil {
        t.Fatalf("unexpected error: %v", res[0].Err)
    }
    if adapter.count() != 1 {
        t.Fatalf("adapter calls = %d, want 1", adapter.count())
    }
    call := adapter.calls[0]
    if call.body != "raw body" {
        t.Errorf("raw body = %q", call.body)
    }
    if got := call.attrs["color"]; got.DataType != "String" || got.StringValue == nil || *got.StringValue != "blue" {
        t.Errorf("color attribute = %+v", got)
    }
    if got := call.attrs["count"]; got.DataType != "Number" || got.StringValue == nil || *got.StringValue != "3" {
        t.Errorf("count attribute = %+v", got)
    }
}

func TestFanoutSQSEnvelopeCarriesAttributes(t *testing.T) {
    adapter := &fakeSQS{}
    d := NewDispatcher(adapter)

    msg := testMessage()
    msg.Subject = ""
    msg.Attributes = map[string]MessageAttributeValue{
        "count": {DataType: "Number", StringValue: strValue("3.14")},
        "blob":  {DataType: "Binary", BinaryValue: []byte("xyz")},
    }

    if res := d.Fanout(context.Background(), msg, []Subscription{sqsSubscription(false)}); res[0].Err != nil {
        t.Fatalf("unexpected error: %v", res[0].Err)
    }

    var env Envelope
    if err := json.Unmarshal([]byte(adapter.calls[0].body), &env); err != nil {
        t.Fatalf("decode envelope: %v", err)
    }
    count, ok := env.MessageAttributes["count"]
    if !ok || count.DataType != "Number" || count.StringValue == nil || *count.StringValue != "3.14" {
        t.Errorf("count attribute = %+v", count)
    }
    blob, ok := env.MessageAttributes["blob"]
    if !ok || blob.DataType != "Binary" || string(blob.BinaryValue) != "xyz" {
        t.Errorf("blob attribute = %+v", blob)
    }
}

func TestHTTPDeliveryPostsEnvelope(t *testing.T) {
    var (
        mu     sync.Mutex
        method string
        header http.Header
        body   []byte
    )
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        mu.Lock()
        defer mu.Unlock()
        method = r.Method
        header = r.Header.Clone()
        body, _ = io.ReadAll(r.Body)
        w.WriteHeader(http.StatusOK)
    }))
    defer srv.Close()

    sub := Subscription{
        SubscriptionARN: "arn:aws:sns:us-east-1:123456789012:mytopic:11111111-2222-3333-4444-555555555555",
        Protocol:        ProtocolHTTP,
        Endpoint:        srv.URL,
    }
    d := &Dispatcher{BaseEndpoint: "http://sns.test:4566"}

    res := d.Fanout(context.Background(), testMessage(), []Subscription{sub})
    if len(res) != 1 || !res[0].Matched || res[0].Err != nil {
        t.Fatalf("unexpected fanout result: %+v", res)
    }

    mu.Lock()
    defer mu.Unlock()
    if method != http.MethodPost {
        t.Errorf("method = %q, want POST", method)
    }
    if got := header.Get("content-type"); got != "text/plain; charset=UTF-8" {
        t.Errorf("content-type = %q", got)
    }
    if got := header.Get("x-amz-sns-message-type"); got != "Notification" {
        t.Errorf("x-amz-sns-message-type = %q", got)
    }
    if got := header.Get("x-amz-sns-message-id"); got != "11111111-2222-3333-4444-555555555555" {
        t.Errorf("x-amz-sns-message-id = %q", got)
    }
    if got := header.Get("x-amz-sns-topic-arn"); got != "arn:aws:sns:us-east-1:123456789012:mytopic" {
        t.Errorf("x-amz-sns-topic-arn = %q", got)
    }
    if got := header.Get("x-amz-sns-subscription-arn"); got != sub.SubscriptionARN {
        t.Errorf("x-amz-sns-subscription-arn = %q", got)
    }
    if got := header.Get("x-amz-sns-rawdelivery"); got != "false" {
        t.Errorf("x-amz-sns-rawdelivery = %q", got)
    }
    var env Envelope
    if err := json.Unmarshal(body, &env); err != nil {
        t.Fatalf("HTTP payload is not an SNS envelope: %v", err)
    }
    if env.Type != "Notification" || env.Message != "hello world" || env.Subject != "order" {
        t.Errorf("envelope = %+v", env)
    }
    want := url.QueryEscape(sub.SubscriptionARN)
    if !strings.Contains(env.UnsubscribeURL, "Action=Unsubscribe") || !strings.Contains(env.UnsubscribeURL, want) {
        t.Errorf("UnsubscribeURL = %q", env.UnsubscribeURL)
    }
}

func TestHTTPDeliveryRawPayload(t *testing.T) {
    var mu sync.Mutex
    var body []byte
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        mu.Lock()
        defer mu.Unlock()
        body, _ = io.ReadAll(r.Body)
        w.WriteHeader(http.StatusOK)
    }))
    defer srv.Close()

    sub := Subscription{Protocol: ProtocolHTTP, Endpoint: srv.URL, RawMessageDelivery: true}
    msg := testMessage()
    msg.Subject = ""

    d := &Dispatcher{}
    if res := d.Fanout(context.Background(), msg, []Subscription{sub}); res[0].Err != nil {
        t.Fatalf("unexpected error: %v", res[0].Err)
    }

    mu.Lock()
    defer mu.Unlock()
    if string(body) != "hello world" {
        t.Errorf("raw payload = %q", body)
    }
}

func TestHTTPDeliveryFailsOnServerError(t *testing.T) {
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusInternalServerError)
    }))
    defer srv.Close()

    d := &Dispatcher{}
    res := d.Fanout(context.Background(), testMessage(), []Subscription{{Protocol: ProtocolHTTP, Endpoint: srv.URL}})
    if res[0].Err == nil {
        t.Fatal("expected delivery error for HTTP 500")
    }
}

func TestLambdaDeliveryDeferred(t *testing.T) {
    d := &Dispatcher{}
    sub := Subscription{Protocol: ProtocolLambda, Endpoint: "arn:aws:lambda:us-east-1:123456789012:function:fn"}
    res := d.Fanout(context.Background(), testMessage(), []Subscription{sub})
    if !res[0].Matched {
        t.Fatal("lambda subscription has no policy and must count as matched")
    }
    if !errors.Is(res[0].Err, ErrLambdaNotImplemented) {
        t.Fatalf("err = %v, want ErrLambdaNotImplemented", res[0].Err)
    }
}

func TestUnsupportedProtocol(t *testing.T) {
    d := &Dispatcher{}
    sub := Subscription{Protocol: "email", Endpoint: "user@example.com"}
    res := d.Fanout(context.Background(), testMessage(), []Subscription{sub})
    if !errors.Is(res[0].Err, ErrUnsupportedProtocol) {
        t.Fatalf("err = %v, want ErrUnsupportedProtocol", res[0].Err)
    }
}

func TestFanoutAppliesFilterPolicy(t *testing.T) {
    adapter := &fakeSQS{}
    d := NewDispatcher(adapter)

    policy, err := ParseFilterPolicy(`{"store": ["amazon", "example_corp"]}`)
    if err != nil {
        t.Fatalf("parse policy: %v", err)
    }
    sub := sqsSubscription(false)
    sub.FilterPolicy = policy

    msg := testMessage()
    msg.Subject = ""
    msg.Attributes = map[string]MessageAttributeValue{
        "store": {DataType: "String", StringValue: strValue("other")},
    }

    res := d.Fanout(context.Background(), msg, []Subscription{sub})
    if adapter.count() != 0 {
        t.Fatalf("adapter must not be called for filtered-out subscriptions")
    }
    if res[0].Matched || res[0].Err != nil {
        t.Fatalf("filtered-out result = %+v", res[0])
    }

    msg.Attributes["store"] = MessageAttributeValue{DataType: "String", StringValue: strValue("example_corp")}
    res = d.Fanout(context.Background(), msg, []Subscription{sub})
    if !res[0].Matched || res[0].Err != nil || adapter.count() != 1 {
        t.Fatalf("matching result = %+v", res[0])
    }
}

func TestFanoutFilterPolicyBodyScope(t *testing.T) {
    adapter := &fakeSQS{}
    d := NewDispatcher(adapter)

    policy, err := ParseFilterPolicy(`{"type": "order"}`)
    if err != nil {
        t.Fatalf("parse policy: %v", err)
    }
    sub := sqsSubscription(false)
    sub.FilterPolicy = policy
    sub.FilterPolicyScope = FilterScopeBody

    msg := testMessage()
    msg.Subject = ""
    msg.Body = `{"type": "order"}`

    res := d.Fanout(context.Background(), msg, []Subscription{sub})
    if !res[0].Matched || res[0].Err != nil || adapter.count() != 1 {
        t.Fatalf("matching result = %+v", res[0])
    }

    msg.Body = `{"type": "refund"}`
    res = d.Fanout(context.Background(), msg, []Subscription{sub})
    if res[0].Matched || adapter.count() != 1 {
        t.Fatalf("non-matching result = %+v", res[0])
    }
}

func TestFanoutGeneratesMissingMessageID(t *testing.T) {
    adapter := &fakeSQS{}
    d := NewDispatcher(adapter)

    msg := testMessage()
    msg.MessageID = ""

    if res := d.Fanout(context.Background(), msg, []Subscription{sqsSubscription(false)}); res[0].Err != nil {
        t.Fatalf("unexpected error: %v", res[0].Err)
    }
    var env Envelope
    if err := json.Unmarshal([]byte(adapter.calls[0].body), &env); err != nil {
        t.Fatalf("decode envelope: %v", err)
    }
    if len(env.MessageId) != 36 {
        t.Errorf("generated MessageId = %q", env.MessageId)
    }
}

func TestNewMessageIDShape(t *testing.T) {
    parts := strings.Split(NewMessageID(), "-")
    if len(parts) != 5 || len(parts[0]) != 8 || len(parts[1]) != 4 || len(parts[2]) != 4 || len(parts[3]) != 4 || len(parts[4]) != 12 {
        t.Fatalf("id parts = %q", parts)
    }
    if parts[2][0] != '4' {
        t.Fatalf("id is not a v4 UUID: %s", parts[2])
    }
}
