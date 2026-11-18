package eventbridge

// Ticket flocigo/ticket-027 - TargetDispatcher (translation of
// TargetDispatcher.java): routes events that matched a rule to the
// rule's targets - SQS queues via SendMessage, Lambda functions via
// Invoke and HTTP endpoints via POST.
//
// SQS delivery goes through the SQS adapter seam (QueueSender, backed
// by the ticket-004 SQS adapter). Lambda delivery is deferred until
// the lambda execution model is decided: without a LambdaInvoker,
// lambda targets fail loudly with ErrLambdaNotImplemented instead of
// silently dropping matched events. HTTP targets POST the event JSON
// to the target URL; any 2xx response counts as delivered and only
// the failing target fails otherwise.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// TargetKind classifies which route carries a matched event to its
// target.
type TargetKind string

const (
	// TargetKindSQS delivers the event with an SQS SendMessage through
	// the ticket-004 SQS adapter.
	TargetKindSQS TargetKind = "sqs"
	// TargetKindLambda delivers the event with a Lambda Invoke.
	TargetKindLambda TargetKind = "lambda"
	// TargetKindHTTP delivers the event with a plain POST.
	TargetKindHTTP TargetKind = "http"
	// TargetKindUnknown means the target names no dispatchable route.
	TargetKindUnknown TargetKind = "unknown"
)

// Target is one destination registered for a rule. SQS queues and
// lambda functions are addressed by ARN, HTTP endpoints by URL; Input
// optionally replaces the event with a constant JSON document.
type Target struct {
	ID    string
	ARN   string
	URL   string
	Role  string
	Input string
}

// Kind reports which route the target uses. A URL wins when both a URL
// and an ARN are present.
func (t *Target) Kind() TargetKind {
	if t == nil {
		return TargetKindUnknown
	}
	if strings.HasPrefix(t.URL, "http://") || strings.HasPrefix(t.URL, "https://") {
		return TargetKindHTTP
	}
	switch arnService(t.ARN) {
	case "sqs":
		return TargetKindSQS
	case "lambda":
		return TargetKindLambda
	}
	return TargetKindUnknown
}

// arnService returns the service segment of an ARN
// (arn:<partition>:<service>:...) or "" for non-ARNS.
func arnService(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) >= 3 {
		return parts[2]
	}
	return ""
}

// DispatchStatus is the per-target outcome of a dispatch.
type DispatchStatus string

const (
	// DispatchDelivered: the event reached the target.
	DispatchDelivered DispatchStatus = "delivered"
	// DispatchFailed: the route exists but delivery failed.
	DispatchFailed DispatchStatus = "failed"
	// DispatchNotImplemented: the route is deliberately deferred
	// (lambda, ticket 027).
	DispatchNotImplemented DispatchStatus = "not-implemented"
)

// DispatchResult records what happened to one target.
type DispatchResult struct {
	TargetID  string
	Kind      TargetKind
	Status    DispatchStatus
	MessageID string // SQS message id when an SQS target delivers
	Err       error
}

// OK reports whether the event reached the target.
func (r DispatchResult) OK() bool { return r.Status == DispatchDelivered }

// QueueSender is the SQS adapter seam (ticket 004). The SQS service
// adapter implements it so EventBridge needs neither a queue store nor
// a loopback HTTP call of its own.
type QueueSender interface {
	// SendMessage enqueues body (an event JSON document) on the queue
	// identified by queueARN - an arn:aws:sqs:... ARN; adapters may
	// equally accept a queue URL - and returns the SQS message id.
	SendMessage(ctx context.Context, queueARN, body string, attrs map[string]string) (string, error)
}

// LambdaInvoker is the deferred lambda seam (Invoke). It stays nil
// until the lambda execution model is decided; while nil, lambda
// targets fail loudly with ErrLambdaNotImplemented.
type LambdaInvoker interface {
	Invoke(ctx context.Context, functionARN string, payload []byte) error
}

// Loud sentinel failures surfaced by the dispatcher.
var (
	// ErrLambdaNotImplemented marks the deliberate deferral of lambda
	// target delivery (ticket 027).
	ErrLambdaNotImplemented = errors.New("eventbridge: lambda targets are not implemented: lambda execution model undecided (ticket 027 deferral)")
	// ErrNoQueueSender marks a dispatcher missing the SQS adapter.
	ErrNoQueueSender = errors.New("eventbridge: no SQS adapter configured: wire a QueueSender (ticket 004) into the dispatcher")
	// ErrUnknownTarget marks targets that name no dispatchable route.
	ErrUnknownTarget = errors.New("eventbridge: target names no dispatchable route: want an arn:aws:sqs or arn:aws:lambda ARN or an http/https URL")
)

// TargetDispatcher routes matched events to rule targets. Zero-value
// fields are legal: SQS targets then fail with ErrNoQueueSender and
// lambda targets with ErrLambdaNotImplemented.
type TargetDispatcher struct {
	// Queues delivers SQS targets (the ticket-004 SQS adapter).
	Queues QueueSender
	// Lambda delivers lambda targets; nil until the lambda execution
	// model is decided.
	Lambda LambdaInvoker
	// Client performs HTTP POSTs. Nil selects a 10s timeout client.
	Client *http.Client
	// Log receives failure notices; nil selects the standard logger.
	Log *log.Logger
}

// NewTargetDispatcher builds a dispatcher whose SQS targets are
// delivered through queues (the ticket-004 SQS adapter).
func NewTargetDispatcher(queues QueueSender) *TargetDispatcher {
	return &TargetDispatcher{Queues: queues}
}

// Dispatch delivers event to every target and returns one result per
// target, in target order. A failing target never blocks the others;
// use Strict when a single aggregate error is wanted.
func (d *TargetDispatcher) Dispatch(ctx context.Context, event map[string]any, targets ...*Target) []DispatchResult {
	results := make([]DispatchResult, 0, len(targets))
	for _, t := range targets {
		results = append(results, d.dispatchOne(ctx, event, t))
	}
	return results
}

// Strict dispatches and fails loudly: it returns the first failing
// target's error, so a lambda target surfaces ErrLambdaNotImplemented.
func (d *TargetDispatcher) Strict(ctx context.Context, event map[string]any, targets ...*Target) ([]DispatchResult, error) {
	results := d.Dispatch(ctx, event, targets...)
	for _, r := range results {
		if r.Err != nil {
			return results, r.Err
		}
	}
	return results, nil
}

func (d *TargetDispatcher) dispatchOne(ctx context.Context, event map[string]any, t *Target) DispatchResult {
	if t == nil {
		return d.finish(DispatchResult{Kind: TargetKindUnknown, Status: DispatchFailed, Err: ErrUnknownTarget})
	}
	var res DispatchResult
	switch t.Kind() {
	case TargetKindSQS:
		res = d.dispatchSQS(ctx, event, t)
	case TargetKindLambda:
		res = d.dispatchLambda(ctx, event, t)
	case TargetKindHTTP:
		res = d.dispatchHTTP(ctx, event, t)
	default:
		res = DispatchResult{
			TargetID: t.ID,
			Kind:     TargetKindUnknown,
			Status:   DispatchFailed,
			Err:      fmt.Errorf("%w: arn=%q url=%q", ErrUnknownTarget, t.ARN, t.URL),
		}
	}
	return d.finish(res)
}

// dispatchSQS routes the event into the queue via SendMessage through
// the SQS adapter, in-process, the same Send the SQS wire op exposes.
func (d *TargetDispatcher) dispatchSQS(ctx context.Context, event map[string]any, t *Target) DispatchResult {
	res := DispatchResult{TargetID: t.ID, Kind: TargetKindSQS, Status: DispatchFailed}
	if d.Queues == nil {
		res.Err = ErrNoQueueSender
		return res
	}
	body, err := d.payload(t, event)
	if err != nil {
		res.Err = err
		return res
	}
	messageID, err := d.Queues.SendMessage(ctx, t.ARN, body, nil)
	if err != nil {
		res.Err = fmt.Errorf("sqs send to %s: %w", t.ARN, err)
		return res
	}
	res.Status = DispatchDelivered
	res.MessageID = messageID
	return res
}

// dispatchLambda invokes the function through the LambdaInvoker seam.
// With the execution model still undecided (no invoker) the target
// fails loudly, keeping the deferral visible on every dispatch.
func (d *TargetDispatcher) dispatchLambda(ctx context.Context, event map[string]any, t *Target) DispatchResult {
	res := DispatchResult{TargetID: t.ID, Kind: TargetKindLambda, Status: DispatchFailed}
	if d.Lambda == nil {
		res.Status = DispatchNotImplemented
		res.Err = fmt.Errorf("%w: %s", ErrLambdaNotImplemented, t.ARN)
		return res
	}
	payload, err := d.payload(t, event)
	if err != nil {
		res.Err = err
		return res
	}
	if err := d.Lambda.Invoke(ctx, t.ARN, payload); err != nil {
		res.Err = fmt.Errorf("lambda invoke %s: %w", t.ARN, err)
		return res
	}
	res.Status = DispatchDelivered
	return res
}

// dispatchHTTP POSTs the event JSON to the target URL with an
// application/json content type; any 2xx response counts as delivered.
func (d *TargetDispatcher) dispatchHTTP(ctx context.Context, event map[string]any, t *Target) DispatchResult {
	res := DispatchResult{TargetID: t.ID, Kind: TargetKindHTTP, Status: DispatchFailed}
	body, err := d.payload(t, event)
	if err != nil {
		res.Err = err
		return res
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.URL, bytes.NewReader(body))
	if err != nil {
		res.Err = fmt.Errorf("http target %s: %w", t.URL, err)
		return res
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client().Do(req)
	if err != nil {
		res.Err = fmt.Errorf("http target %s: %w", t.URL, err)
		return res
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		res.Err = fmt.Errorf("http target %s: unexpected status %d", t.URL, resp.StatusCode)
		return res
	}
	res.Status = DispatchDelivered
	return res
}

// payload renders the bytes a target receives: the constant Input
// document when the target declares one, otherwise the event as JSON.
func (d *TargetDispatcher) payload(t *Target, event map[string]any) ([]byte, error) {
	if t != nil && t.Input != "" {
		var probe any
		if err := json.Unmarshal([]byte(t.Input), &probe); err != nil {
			return nil, fmt.Errorf("target %s Input is not valid JSON: %w", t.ID, err)
		}
		return []byte(t.Input), nil
	}
	if event == nil {
		event = map[string]any{}
	}
	return json.Marshal(event)
}

func (d *TargetDispatcher) client() *http.Client {
	if d.Client != nil {
		return d.Client
	}
	return &http.Client{Timeout: 10 * time.Second}
}

func (d *TargetDispatcher) logger() *log.Logger {
	if d.Log != nil {
		return d.Log
	}
	return log.Default()
}

// finish logs every failed dispatch loudly and passes the result
// through.
func (d *TargetDispatcher) finish(res DispatchResult) DispatchResult {
	if res.Err != nil {
		d.logger().Printf("eventbridge: dispatch to target %q (%s) failed: %v", res.TargetID, res.Kind, res.Err)
	}
	return res
}
