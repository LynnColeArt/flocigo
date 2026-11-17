// Package floci implements the Floci event router (the Go port of Floci).
//
// Ticket flocigo/ticket-027: TargetDispatcher routes events that matched a
// rule to the rule's targets. SQS targets are delivered with SQS SendMessage
// through the SQS adapter from ticket 004, HTTP targets are delivered as an
// HTTP POST of the encoded event, and Lambda targets are deferrable: until
// the Lambda execution model is decided they fail loudly with
// ErrLambdaNotImplemented.
package floci

import (
    "bytes"
    "context"
    "errors"
    "fmt"
    "io"
    "net/http"
    "strings"
    "time"
)

// TargetKind selects the transport used to deliver a matched event.
type TargetKind string

const (
    // TargetKindSQS delivers the event as an SQS SendMessage via the SQS
    // adapter from ticket 004.
    TargetKindSQS TargetKind = "sqs"
    // TargetKindLambda invokes a Lambda function. Lambda routing is
    // deferrable and currently fails loudly (see ErrLambdaNotImplemented).
    TargetKindLambda TargetKind = "lambda"
    // TargetKindHTTP POSTs the event to the target URL.
    TargetKindHTTP TargetKind = "http"
)

// Target describes one destination of a matched event (an EventBridge style
// rule target).
type Target struct {
    // ID uniquely identifies the target (the EventBridge target "Id").
    ID string `json:"id"`
    // Kind selects the transport. Optional: it is inferred from ARN,
    // QueueURL and URL when left empty.
    Kind TargetKind `json:"kind,omitempty"`
    // ARN is the SQS or Lambda ARN of the destination.
    ARN string `json:"arn,omitempty"`
    // QueueURL is the SQS queue URL (or bare queue name) for SQS targets.
    QueueURL string `json:"queueUrl,omitempty"`
    // URL is the destination URL for HTTP targets.
    URL string `json:"url,omitempty"`
}

// ResolvedKind returns the transport used for t, inferring it from the
// destination fields when Kind is empty.
func (t Target) ResolvedKind() (TargetKind, error) {
    if t.Kind != "" {
        switch t.Kind {
        case TargetKindSQS, TargetKindLambda, TargetKindHTTP:
            return t.Kind, nil
        default:
            return "", fmt.Errorf("target %q: unknown kind %q", t.ID, t.Kind)
        }
    }
    switch {
    case strings.HasPrefix(t.ARN, "arn:aws:lambda:"):
        return TargetKindLambda, nil
    case strings.HasPrefix(t.ARN, "arn:aws:sqs:"), t.QueueURL != "":
        return TargetKindSQS, nil
    case t.URL != "":
        return TargetKindHTTP, nil
    default:
        return "", fmt.Errorf("target %q: cannot infer kind, set kind, arn, queueUrl or url", t.ID)
    }
}

// NewTarget builds a Target from a destination identifier: an SQS ARN, a
// Lambda ARN or an http(s) URL.
func NewTarget(id, destination string) (Target, error) {
    t := Target{ID: id}
    switch {
    case strings.HasPrefix(destination, "arn:aws:sqs:"):
        t.Kind, t.ARN = TargetKindSQS, destination
    case strings.HasPrefix(destination, "arn:aws:lambda:"):
        t.Kind, t.ARN = TargetKindLambda, destination
    case strings.HasPrefix(destination, "http://"), strings.HasPrefix(destination, "https://"):
        t.Kind, t.URL = TargetKindHTTP, destination
    default:
        return Target{}, fmt.Errorf("target %q: cannot classify destination %q", id, destination)
    }
    return t, nil
}

// queueNameFromSqsArn extracts the queue name from an SQS ARN of the form
// arn:aws:sqs:<region>:<account>:<queue-name>.
func queueNameFromSqsArn(arn string) string {
    if i := strings.LastIndex(arn, ":"); i >= 0 {
        return arn[i+1:]
    }
    return arn
}

// EventEncoder is the JSON adapter from ticket 003: it renders an event into
// the canonical Floci JSON wire format used as SQS message bodies and HTTP
// request bodies.
type EventEncoder interface {
    EncodeEvent(event any) ([]byte, error)
}

// EventEncoderFunc adapts a plain function to EventEncoder.
type EventEncoderFunc func(event any) ([]byte, error)

// EncodeEvent implements EventEncoder.
func (f EventEncoderFunc) EncodeEvent(event any) ([]byte, error) { return f(event) }

// SQSSender is the SQS adapter from ticket 004 used for SQS targets. The
// queue may be a full queue URL or a bare queue name; the SQS adapter turns
// it into the SQS SendMessage wire call.
type SQSSender interface {
    SendMessage(ctx context.Context, queue, messageBody string) error
}

// SQSSenderFunc adapts a plain function to SQSSender.
type SQSSenderFunc func(ctx context.Context, queue, messageBody string) error

// SendMessage implements SQSSender.
func (f SQSSenderFunc) SendMessage(ctx context.Context, queue, messageBody string) error {
    return f(ctx, queue, messageBody)
}

// ErrNotImplemented marks routes whose execution model has not been decided.
var ErrNotImplemented = errors.New("not implemented")

// ErrLambdaNotImplemented is returned for every Lambda target until the
// Lambda execution model is decided. Ticket flocigo/ticket-027 defers Lambda
// routing and requires a loud failure instead of a silent drop.
var ErrLambdaNotImplemented = fmt.Errorf("lambda invoke: %w: the Lambda execution model has not been decided yet", ErrNotImplemented)

// TargetDispatcher routes matched events to their configured targets:
//
//   - SQS: SQS SendMessage via the ticket 004 adapter; the encoded event is
//     the message body.
//   - HTTP: HTTP POST of the encoded event to the target URL.
//   - Lambda: deferrable; fails loudly with ErrLambdaNotImplemented.
type TargetDispatcher struct {
    encoder    EventEncoder
    sqs        SQSSender
    httpClient *http.Client
    targets    map[string]Target
    order      []string
}

// NewTargetDispatcher wires the dispatcher to the JSON adapter (ticket 003)
// for event encoding and to the SQS adapter (ticket 004) for SQS targets.
// httpClient may be nil to select a sensible default client.
func NewTargetDispatcher(encoder EventEncoder, sqs SQSSender, httpClient *http.Client) (*TargetDispatcher, error) {
    if encoder == nil {
        return nil, errors.New("target dispatcher: JSON adapter (EventEncoder) is required")
    }
    if sqs == nil {
        return nil, errors.New("target dispatcher: SQS adapter (SQSSender) is required")
    }
    if httpClient == nil {
        httpClient = &http.Client{Timeout: 10 * time.Second}
    }
    return &TargetDispatcher{
        encoder:    encoder,
        sqs:        sqs,
        httpClient: httpClient,
        targets:    make(map[string]Target),
    }, nil
}

// Register adds (or replaces) a target. Destination fields are validated
// eagerly so malformed targets are rejected at registration time.
func (d *TargetDispatcher) Register(t Target) error {
    if strings.TrimSpace(t.ID) == "" {
        return errors.New("target dispatcher: target id must not be empty")
    }
    kind, err := t.ResolvedKind()
    if err != nil {
        return fmt.Errorf("target dispatcher: %w", err)
    }
    n := t
    n.Kind = kind
    n.QueueURL = strings.TrimSpace(n.QueueURL)
    n.URL = strings.TrimSpace(n.URL)
    switch kind {
    case TargetKindSQS:
        if n.QueueURL == "" && n.ARN != "" {
            n.QueueURL = queueNameFromSqsArn(n.ARN)
        }
        if n.QueueURL == "" {
            return fmt.Errorf("target dispatcher: target %q: sqs target requires queueUrl or arn", t.ID)
        }
    case TargetKindHTTP:
        if !strings.HasPrefix(n.URL, "http://") && !strings.HasPrefix(n.URL, "https://") {
            return fmt.Errorf("target dispatcher: target %q: http target requires an absolute http(s) url, got %q", t.ID, t.URL)
        }
    case TargetKindLambda:
        if !strings.HasPrefix(n.ARN, "arn:aws:lambda:") {
            return fmt.Errorf("target dispatcher: target %q: lambda target requires a function arn", t.ID)
        }
    }
    if _, exists := d.targets[n.ID]; !exists {
        d.order = append(d.order, n.ID)
    }
    d.targets[n.ID] = n
    return nil
}

// Target returns the registered target with the given id.
func (d *TargetDispatcher) Target(id string) (Target, bool) {
    t, ok := d.targets[id]
    return t, ok
}

// Targets returns the registered targets in registration order.
func (d *TargetDispatcher) Targets() []Target {
    out := make([]Target, 0, len(d.order))
    for _, id := range d.order {
        out = append(out, d.targets[id])
    }
    return out
}

// Dispatch encodes the matched event with the JSON adapter (ticket 003) and
// delivers it to every listed target. Delivery continues past the first
// failure and the returned error aggregates one entry per failed target.
func (d *TargetDispatcher) Dispatch(ctx context.Context, event any, targetIDs ...string) error {
    payload, err := d.encoder.EncodeEvent(event)
    if err != nil {
        return fmt.Errorf("target dispatcher: encode event: %w", err)
    }
    return d.DispatchPayload(ctx, payload, targetIDs...)
}

// DispatchPayload delivers an already encoded event to the given targets.
func (d *TargetDispatcher) DispatchPayload(ctx context.Context, payload []byte, targetIDs ...string) error {
    if len(targetIDs) == 0 {
        return nil
    }
    var errs []error
    for _, id := range targetIDs {
        t, ok := d.targets[id]
        if !ok {
            errs = append(errs, fmt.Errorf("target dispatcher: unknown target %q", id))
            continue
        }
        if err := d.Route(ctx, t, payload); err != nil {
            errs = append(errs, fmt.Errorf("target dispatcher: target %q: %w", id, err))
        }
    }
    return errors.Join(errs...)
}

// Route delivers one encoded event to one target.
func (d *TargetDispatcher) Route(ctx context.Context, t Target, payload []byte) error {
    kind, err := t.ResolvedKind()
    if err != nil {
        return err
    }
    switch kind {
    case TargetKindSQS:
        queue := t.QueueURL
        if queue == "" && t.ARN != "" {
            queue = queueNameFromSqsArn(t.ARN)
        }
        if queue == "" {
            return errors.New("sqs target has no queue url or arn")
        }
        return d.sqs.SendMessage(ctx, queue, string(payload))
    case TargetKindLambda:
        // Lambda routing is deferrable: fail loudly until the Lambda
        // execution model is decided.
        return ErrLambdaNotImplemented
    case TargetKindHTTP:
        return d.postHTTP(ctx, t.URL, payload)
    default:
        return fmt.Errorf("unsupported target kind %q", kind)
    }
}

// postHTTP POSTs the encoded event to the target URL and fails on any
// non-2xx response.
func (d *TargetDispatcher) postHTTP(ctx context.Context, url string, payload []byte) error {
    req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
    if err != nil {
        return fmt.Errorf("http post: build request: %w", err)
    }
    req.Header.Set("Content-Type", "application/json")
    resp, err := d.httpClient.Do(req)
    if err != nil {
        return fmt.Errorf("http post: %w", err)
    }
    defer resp.Body.Close()
    respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
    if resp.StatusCode < 200 || resp.StatusCode >= 300 {
        return fmt.Errorf("http post: unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
    }
    return nil
}
