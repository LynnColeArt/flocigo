// Package delivery implements Amazon SNS message fanout for floci-go.
//
// A published message is delivered to every qualifying subscription of its
// topic:
//
//   - sqs: handed to the SQS service through the SQSAdapter interface. With
//     RawMessageDelivery the raw body plus SNS message attributes are
//     forwarded; otherwise the standard SNS notification envelope is embedded
//     in the SQS message body.
//   - http and https: the SNS notification envelope is POSTed to the
//     endpoint with the canonical x-amz-sns-* headers.
//   - lambda: deliberately deferred; deliveries fail with
//     ErrLambdaNotImplemented until the Lambda adapter lands.
//
// A subscription's filter policy (attribute or message-body scoped) decides
// whether it receives a message at all.
package delivery

import (
    "bytes"
    "context"
    "crypto/rand"
    "encoding/base64"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "strconv"
    "strings"
    "time"
)

// Subscription protocols handled (or deliberately deferred) by this package.
const (
    ProtocolSQS    = "sqs"
    ProtocolHTTP   = "http"
    ProtocolHTTPS  = "https"
    ProtocolLambda = "lambda"
)

// snsTimestampFormat matches the SNS wire timestamp (e.g.
// 2026-02-16T10:11:12.000Z).
const snsTimestampFormat = "2006-01-02T15:04:05.000Z07:00"

// Errors returned by the dispatcher.
var (
    // ErrLambdaNotImplemented is returned for Lambda targets. Lambda event
    // delivery is deferred and will be owned by the Lambda adapter.
    ErrLambdaNotImplemented = errors.New("sns: delivery to Lambda endpoints is not implemented")
    // ErrSQSAdapterUnavailable is returned for SQS fanout when the dispatcher
    // has no SQSAdapter configured.
    ErrSQSAdapterUnavailable = errors.New("sns: no SQS adapter is configured")
    // ErrUnsupportedProtocol is returned for protocols the emulator cannot
    // deliver to (e.g. email, sms).
    ErrUnsupportedProtocol = errors.New("sns: unsupported subscription protocol")
    // ErrInvalidFilterPolicy wraps all subscription filter policy errors.
    ErrInvalidFilterPolicy = errors.New("sns: invalid subscription filter policy")
)

// SQSAdapter is the boundary to the SQS service. It is invoked for every SQS
// fanout target; the queue is identified by its ARN (the subscription
// endpoint). When raw message delivery is enabled the adapter is expected to
// map MessageAttributeValue entries onto SQS message attributes.
type SQSAdapter interface {
    SendMessage(ctx context.Context, queueARN, body string, attrs map[string]MessageAttributeValue) error
}

// MessageAttributeValue mirrors an SNS message attribute value.
type MessageAttributeValue struct {
    DataType    string  // "String", "Number", "Binary", or "<Type>.<label>"
    StringValue *string
    BinaryValue []byte
}

// StringValuePtr is a small helper for building attributes.
func StringValuePtr(s string) *string { return &s }

// MarshalJSON renders the attribute in the shape carried inside SNS
// envelopes: {"Type": "<DataType>", "Value": "<string or base64>"}.
func (a MessageAttributeValue) MarshalJSON() ([]byte, error) {
    dt := a.DataType
    if dt == "" {
        dt = "String"
    }
    var value string
    if dataTypePrefix(dt) == "Binary" {
        value = base64.StdEncoding.EncodeToString(a.BinaryValue)
    } else if a.StringValue != nil {
        value = *a.StringValue
    }
    return json.Marshal(map[string]string{"Type": dt, "Value": value})
}

// UnmarshalJSON parses back the {"Type", "Value"} attribute representation,
// decoding base64 payloads for Binary attributes.
func (a *MessageAttributeValue) UnmarshalJSON(data []byte) error {
    var raw struct {
        Type  string          `json:"Type"`
        Value json.RawMessage `json:"Value"`
    }
    if err := json.Unmarshal(data, &raw); err != nil {
        return err
    }
    if raw.Type == "" {
        raw.Type = "String"
    }
    a.DataType = raw.Type
    a.StringValue = nil
    a.BinaryValue = nil
    if len(raw.Value) == 0 || string(raw.Value) == "null" {
        return nil
    }
    var value string
    if err := json.Unmarshal(raw.Value, &value); err != nil {
        return nil // tolerate non-string payloads
    }
    if dataTypePrefix(raw.Type) == "Binary" {
        if decoded, err := base64.StdEncoding.DecodeString(value); err == nil {
            a.BinaryValue = decoded
            return nil
        }
    }
    a.StringValue = &value
    return nil
}

// Message is an SNS message as accepted for fanout.
type Message struct {
    TopicARN   string
    MessageID  string
    Subject    string
    Body       string
    Timestamp  time.Time
    Attributes map[string]MessageAttributeValue
}

// Subscription describes a single topic subscription during fanout. The
// Endpoint for sqs subscriptions is the queue ARN.
type Subscription struct {
    SubscriptionARN    string
    TopicARN           string
    Protocol           string
    Endpoint           string
    RawMessageDelivery bool
    FilterPolicyScope  string
    FilterPolicy       FilterPolicy
}

// Envelope is the standard SNS notification envelope delivered to HTTP(S)
// endpoints and embedded in the SQS message body for non-raw delivery.
type Envelope struct {
    Type              string                           `json:"Type"`
    MessageId         string                           `json:"MessageId"`
    TopicArn          string                           `json:"TopicArn"`
    Subject           string                           `json:"Subject,omitempty"`
    Message           string                           `json:"Message"`
    Timestamp         string                           `json:"Timestamp"`
    UnsubscribeURL    string                           `json:"UnsubscribeURL,omitempty"`
    MessageAttributes map[string]MessageAttributeValue `json:"MessageAttributes,omitempty"`
}

// DeliveryOutcome records the result of fanning out to one subscription.
type DeliveryOutcome struct {
    Subscription Subscription
    Matched      bool  // false when the filter policy excluded the message
    Err          error // nil when delivered or filtered out
}

// Dispatcher fans out published messages to topic subscriptions.
type Dispatcher struct {
    // SQS receives fanout for sqs subscriptions. May be nil; delivery then
    // fails with ErrSQSAdapterUnavailable.
    SQS SQSAdapter
    // HTTPClient performs HTTP(S) endpoint delivery. When nil, a default
    // client (5s timeout) is used.
    HTTPClient *http.Client
    // BaseEndpoint, when set, is used to build UnsubscribeURL values.
    BaseEndpoint string
}

// NewDispatcher builds a dispatcher bound to the given SQS adapter (which may
// be nil) with a default HTTP client.
func NewDispatcher(sqs SQSAdapter) *Dispatcher {
    return &Dispatcher{SQS: sqs, HTTPClient: &http.Client{Timeout: 5 * time.Second}}
}

// NewMessageID generates a random v4 UUID for MessageId fields when the
// caller did not assign one.
func NewMessageID() string {
    var b [16]byte
    if _, err := rand.Read(b[:]); err != nil {
        return fmt.Sprintf("%d", time.Now().UnixNano())
    }
    b[6] = (b[6] & 0x0f) | 0x40
    b[8] = (b[8] & 0x3f) | 0x80
    return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// Fanout delivers m to every subscription in order. A failed delivery does
// not stop the remaining deliveries; per-subscription results are returned so
// the publish flow can log or ignore them the way real SNS does.
func (d *Dispatcher) Fanout(ctx context.Context, m Message, subs []Subscription) []DeliveryOutcome {
    if m.MessageID == "" {
        m.MessageID = NewMessageID()
    }
    if m.Timestamp.IsZero() {
        m.Timestamp = time.Now()
    }
    out := make([]DeliveryOutcome, 0, len(subs))
    for _, sub := range subs {
        res := DeliveryOutcome{Subscription: sub}
        if !sub.filterMatches(m) {
            out = append(out, res)
            continue
        }
        res.Matched = true
        res.Err = d.Deliver(ctx, sub, m)
        out = append(out, res)
    }
    return out
}

// Deliver sends one message to one subscription.
func (d *Dispatcher) Deliver(ctx context.Context, sub Subscription, m Message) error {
    switch strings.ToLower(sub.Protocol) {
    case ProtocolSQS:
        return d.deliverSQS(ctx, sub, m)
    case ProtocolHTTP, ProtocolHTTPS:
        return d.deliverHTTP(ctx, sub, m)
    case ProtocolLambda:
        return ErrLambdaNotImplemented
    default:
        return fmt.Errorf("%w: %q", ErrUnsupportedProtocol, sub.Protocol)
    }
}

func (d *Dispatcher) deliverSQS(ctx context.Context, sub Subscription, m Message) error {
    if d.SQS == nil {
        return ErrSQSAdapterUnavailable
    }
    if sub.RawMessageDelivery {
        return d.SQS.SendMessage(ctx, sub.Endpoint, m.Body, m.Attributes)
    }
    body, err := json.Marshal(d.envelope(sub, m))
    if err != nil {
        return fmt.Errorf("sns: encode message envelope: %w", err)
    }
    return d.SQS.SendMessage(ctx, sub.Endpoint, string(body), nil)
}

func (d *Dispatcher) deliverHTTP(ctx context.Context, sub Subscription, m Message) error {
    var payload []byte
    if sub.RawMessageDelivery {
        payload = []byte(m.Body)
    } else {
        var err error
        if payload, err = json.Marshal(d.envelope(sub, m)); err != nil {
            return fmt.Errorf("sns: encode message envelope: %w", err)
        }
    }
    req, err := http.NewRequestWithContext(ctx, http.MethodPost, sub.Endpoint, bytes.NewReader(payload))
    if err != nil {
        return fmt.Errorf("sns: build http delivery request: %w", err)
    }
    req.Header.Set("Content-Type", "text/plain; charset=UTF-8")
    req.Header.Set("x-amz-sns-message-type", "Notification")
    req.Header.Set("x-amz-sns-message-id", m.MessageID)
    req.Header.Set("x-amz-sns-topic-arn", m.TopicARN)
    if sub.SubscriptionARN != "" {
        req.Header.Set("x-amz-sns-subscription-arn", sub.SubscriptionARN)
    }
    req.Header.Set("x-amz-sns-rawdelivery", strconv.FormatBool(sub.RawMessageDelivery))

    resp, err := d.client().Do(req)
    if err != nil {
        return fmt.Errorf("sns: http delivery to %s: %w", sub.Endpoint, err)
    }
    defer resp.Body.Close()
    _, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
    if resp.StatusCode < 200 || resp.StatusCode > 299 {
        return fmt.Errorf("sns: http delivery to %s: unexpected status %q", sub.Endpoint, resp.Status)
    }
    return nil
}

func (d *Dispatcher) client() *http.Client {
    if d.HTTPClient != nil {
        return d.HTTPClient
    }
    return http.DefaultClient
}

// envelope renders the standard SNS notification envelope.
func (d *Dispatcher) envelope(sub Subscription, m Message) Envelope {
    e := Envelope{
        Type:      "Notification",
        MessageId: m.MessageID,
        TopicArn:  m.TopicARN,
        Subject:   m.Subject,
        Message:   m.Body,
        Timestamp: m.Timestamp.UTC().Format(snsTimestampFormat),
    }
    if len(m.Attributes) > 0 {
        e.MessageAttributes = m.Attributes
    }
    if d.BaseEndpoint != "" {
        arn := sub.SubscriptionARN
        if arn == "" {
            arn = m.TopicARN
        }
        if arn != "" {
            e.UnsubscribeURL = fmt.Sprintf("%s/?Action=Unsubscribe&SubscriptionArn=%s",
                strings.TrimSuffix(d.BaseEndpoint, "/"), url.QueryEscape(arn))
        }
    }
    return e
}
