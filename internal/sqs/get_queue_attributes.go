// Package sqs hosts the SQS queue operations. This file implements
// GetQueueAttributes: the queue depth counters
// (ApproximateNumberOfMessages{,NotVisible,Delayed} — the visibility
// statistics of the queue) plus the configuration and metadata attributes of
// the SQS 2012-11-05 API.
//
// The operation is written against the narrow QueueSnapshot /
// QueueMessageState views instead of the wire layer's concrete queue store,
// so sqswire can bind it with a small adapter (see those interface docs)
// without an import cycle or shared mutable API surface.
package sqs

import (
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ActionGetQueueAttributes is the SQS query-protocol action implemented here.
const ActionGetQueueAttributes = "GetQueueAttributes"

// gqaXMLNS is the XML namespace of the SQS 2012-11-05 query protocol.
const gqaXMLNS = "http://queue.amazonaws.com/doc/2012-11-05/"

const gqaInternalErrorMessage = "We encountered an internal error. Please try again."

// QueueMessageState is the per-message state the queue depth statistics are
// computed from. Example adapter over a concrete store message:
//
//	func (m *sqswire.Message) MessageVisibility() (time.Time, bool) {
//		return m.VisibleAt, m.ReceiveCount > 0
//	}
type QueueMessageState interface {
	// MessageVisibility reports the earliest time the message is again
	// deliverable to consumers (zero time means deliverable now) and whether
	// it has been delivered at least once. A pending send delay reports
	// received=false with a future timestamp; a message held in flight by a
	// consumer reports received=true with a future timestamp.
	MessageVisibility() (visibleAt time.Time, received bool)
}

// QueueSnapshot is the read-only view of a live queue GetQueueAttributes
// needs.
type QueueSnapshot interface {
	// ConfiguredAttributes returns the values the queue was created or
	// reconfigured with. Missing entries fall back to the documented API
	// defaults; conditional attributes (Policy, RedrivePolicy, Kms*, ...)
	// must stay absent until they are set.
	ConfiguredAttributes() map[string]string

	// QueueMessages enumerates every message stored in the queue,
	// independent of its visibility state. Deleted messages are gone.
	QueueMessages() []QueueMessageState

	// QueueTimestamps reports creation and last attribute modification.
	QueueTimestamps() (createdAt time.Time, modifiedAt time.Time)

	// IsFIFO reports whether the queue is a FIFO queue.
	IsFIFO() bool
}

// QueueResolver resolves a queue URL to its live snapshot. Unknown queues are
// signalled with an error wrapping *QueueNotFoundError.
type QueueResolver interface {
	Resolve(queueURL string) (QueueSnapshot, error)
}

// QueueNotFoundError marks a resolution attempt for a queue URL that does not
// exist; the HTTP surface renders it as the QueueDoesNotExist structured
// error of the query protocol.
type QueueNotFoundError struct {
	QueueURL string
	Err      error
}

func (e *QueueNotFoundError) Error() string { return "sqs: queue not found: " + e.QueueURL }

func (e *QueueNotFoundError) Unwrap() error { return e.Err }

// QueueCounts is the SQS queue depth ("visibility stats") break-down reported
// through the ApproximateNumberOfMessages* queue attributes.
type QueueCounts struct {
	Visible    int // deliverable to consumers right now
	NotVisible int // in flight: delivered, visibility timeout not yet expired
	Delayed    int // send delay not yet elapsed
}

// QueueAttribute is one name/value pair of a GetQueueAttributes result.
type QueueAttribute struct {
	Name  string
	Value string
}

// gqaValidAttrNames is every queue attribute name the action understands;
// anything else is an InvalidAttributeName error.
var gqaValidAttrNames = map[string]bool{
	"All": true,
	"ApproximateNumberOfMessages":           true,
	"ApproximateNumberOfMessagesNotVisible": true,
	"ApproximateNumberOfMessagesDelayed":    true,
	"ContentBasedDeduplication":             true,
	"CreatedTimestamp":                      true,
	"DeduplicationScope":                    true,
	"DelaySeconds":                          true,
	"FifoQueue":                             true,
	"FifoThroughputLimit":                   true,
	"KmsDataKeyReusePeriodSeconds":          true,
	"KmsMasterKeyId":                        true,
	"LastModifiedTimestamp":                 true,
	"MaximumMessageSize":                    true,
	"MessageRetentionPeriod":                true,
	"Policy":                                true,
	"QueueArn":                              true,
	"ReceiveMessageWaitTimeSeconds":         true,
	"RedriveAllowPolicy":                    true,
	"RedrivePolicy":                         true,
	"SqsManagedSseEnabled":                  true,
	"VisibilityTimeout":                     true,
}

// IsValidAttributeName reports whether name is a valid AttributeNames entry
// for GetQueueAttributes.
func IsValidAttributeName(name string) bool { return gqaValidAttrNames[name] }

// gqaInvalidAttrError marks an unknown AttributeNames entry; the HTTP surface
// renders it as the InvalidAttributeName structured error.
type gqaInvalidAttrError struct{ name string }

func (e gqaInvalidAttrError) Error() string { return "Unknown Attribute " + e.name + "." }

// ApproximateCounts classifies the queue depth statistics at time now:
// visible messages are deliverable immediately, not-visible (in flight)
// messages were delivered and their visibility has not expired yet, and
// delayed messages are still waiting out a send delay.
func ApproximateCounts(messages []QueueMessageState, now time.Time) QueueCounts {
	var counts QueueCounts
	for _, message := range messages {
		visibleAt, received := message.MessageVisibility()
		switch {
		case visibleAt.IsZero() || !now.Before(visibleAt):
			counts.Visible++
		case received:
			counts.NotVisible++
		default:
			counts.Delayed++
		}
	}
	return counts
}

// QueueAttributes computes the GetQueueAttributes result. An empty names
// slice yields an empty result — the API behavior when AttributeNames is
// omitted. "All" selects every supported attribute (returned in
// alphabetical order so wire output is deterministic); an explicit list is
// returned in request order, de-duplicated, with conditional attributes that
// are not configured (RedrivePolicy, Kms*, Policy, ...) omitted, mirroring
// the real service. Unknown names fail with gqaInvalidAttrError.
func QueueAttributes(q QueueSnapshot, queueURL string, names []string, now time.Time) ([]QueueAttribute, error) {
	for _, name := range names {
		if !gqaValidAttrNames[name] {
			return nil, gqaInvalidAttrError{name}
		}
	}
	if len(names) == 0 {
		return nil, nil
	}

	available := gqaCollectAvailable(q, queueURL, now)

	if containsAllAttrName(names) {
		ordered := make([]string, 0, len(available))
		for name := range available {
			ordered = append(ordered, name)
		}
		sort.Strings(ordered)
		result := make([]QueueAttribute, 0, len(ordered))
		for _, name := range ordered {
			result = append(result, QueueAttribute{Name: name, Value: available[name]})
		}
		return result, nil
	}

	seen := make(map[string]bool, len(names))
	result := make([]QueueAttribute, 0, len(names))
	for _, name := range names {
		if seen[name] {
			continue
		}
		seen[name] = true
		if value, ok := available[name]; ok {
			result = append(result, QueueAttribute{Name: name, Value: value})
		}
	}
	return result, nil
}

func containsAllAttrName(names []string) bool {
	for _, name := range names {
		if name == "All" {
			return true
		}
	}
	return false
}

// gqaCollectAvailable resolves every attribute the queue can currently
// report, layering the depth counters and timestamps over the documented API
// defaults.
func gqaCollectAvailable(q QueueSnapshot, queueURL string, now time.Time) map[string]string {
	configured := q.ConfiguredAttributes()
	if configured == nil {
		configured = map[string]string{}
	}

	depth := ApproximateCounts(q.QueueMessages(), now)
	out := map[string]string{
		"ApproximateNumberOfMessages":           strconv.Itoa(depth.Visible),
		"ApproximateNumberOfMessagesNotVisible": strconv.Itoa(depth.NotVisible),
		"ApproximateNumberOfMessagesDelayed":    strconv.Itoa(depth.Delayed),
		"DelaySeconds":                          gqaConfiguredString(configured, "DelaySeconds", "0"),
		"MaximumMessageSize":                    gqaConfiguredString(configured, "MaximumMessageSize", "262144"),
		"MessageRetentionPeriod":                gqaConfiguredString(configured, "MessageRetentionPeriod", "345600"),
		"ReceiveMessageWaitTimeSeconds":         gqaConfiguredString(configured, "ReceiveMessageWaitTimeSeconds", "0"),
		"VisibilityTimeout":                     gqaConfiguredString(configured, "VisibilityTimeout", "30"),
		"SqsManagedSseEnabled":                  gqaConfiguredString(configured, "SqsManagedSseEnabled", "true"),
		"QueueArn":                              gqaConfiguredString(configured, "QueueArn", gqaARNFromURL(queueURL)),
	}

	createdAt, modifiedAt := q.QueueTimestamps()
	if createdAt.IsZero() {
		createdAt = gqaParseUnix(configured["CreatedTimestamp"])
	}
	if modifiedAt.IsZero() {
		modifiedAt = gqaParseUnix(configured["LastModifiedTimestamp"])
	}
	if modifiedAt.IsZero() {
		modifiedAt = createdAt
	}
	if !createdAt.IsZero() {
		out["CreatedTimestamp"] = strconv.FormatInt(createdAt.Unix(), 10)
	}
	if !modifiedAt.IsZero() {
		out["LastModifiedTimestamp"] = strconv.FormatInt(modifiedAt.Unix(), 10)
	}

	if q.IsFIFO() {
		out["FifoQueue"] = "true"
		out["ContentBasedDeduplication"] = gqaConfiguredString(configured, "ContentBasedDeduplication", "false")
		out["DeduplicationScope"] = gqaConfiguredString(configured, "DeduplicationScope", "queue")
		out["FifoThroughputLimit"] = gqaConfiguredString(configured, "FifoThroughputLimit", "perQueue")
	}

	// Attributes that only exist once they have been configured.
	for _, name := range []string{"Policy", "RedrivePolicy", "RedriveAllowPolicy", "KmsMasterKeyId", "KmsDataKeyReusePeriodSeconds"} {
		if value := strings.TrimSpace(configured[name]); value != "" {
			out[name] = value
		}
	}

	return out
}

// gqaConfiguredString returns the configured value for name, falling back to
// the documented default when the queue was created without one.
func gqaConfiguredString(attrs map[string]string, name, fallback string) string {
	if value := strings.TrimSpace(attrs[name]); value != "" {
		return value
	}
	return fallback
}

// gqaParseUnix parses "<seconds since epoch>" attribute values. Zero time is
// returned for values the store does not track.
func gqaParseUnix(value string) time.Time {
	seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(seconds, 0)
}

// gqaARNFromURL rebuilds a QueueArn ("arn:aws:sqs:<region>:<account>:<name>")
// from the queue URL shape https://sqs.<region>.amazonaws.com/<account>/<name>
// (or the emulator shape http://host/<account>/<name>). It is only the
// fallback for snapshots that do not carry a stored QueueArn.
func gqaARNFromURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "arn:aws:sqs:us-east-1:000000000000:"
	}
	region := "us-east-1"
	host := parsed.Hostname()
	parts := strings.SplitN(host, ".", 3)
	if parts[0] == "sqs" && len(parts) >= 2 && parts[1] != "" && strings.HasSuffix(host, ".amazonaws.com") {
		region = parts[1]
	}
	var segments []string
	for _, segment := range strings.Split(parsed.Path, "/") {
		if segment != "" {
			segments = append(segments, segment)
		}
	}
	account, name := "000000000000", ""
	if len(segments) >= 2 {
		account, name = segments[0], segments[1]
	} else if len(segments) == 1 {
		name = segments[0]
	}
	return fmt.Sprintf("arn:aws:sqs:%s:%s:%s", region, account, name)
}

// gqaNamesFromForm collects the AttributeNames list from the query-protocol
// form. Both the flat AttributeName.N form and the AttributeNames.member.N
// form are tolerated, entries are ordered by index, de-duplicated, and
// validated against the known attribute vocabulary.
func gqaNamesFromForm(form url.Values) ([]string, error) {
	indexed := make(map[int]string)
	for key, values := range form {
		suffix := ""
		switch {
		case strings.HasPrefix(key, "AttributeName."):
			suffix = strings.TrimPrefix(key, "AttributeName.")
		case strings.HasPrefix(key, "AttributeNames."):
			suffix = strings.TrimPrefix(key, "AttributeNames.")
		default:
			continue
		}
		suffix = strings.TrimPrefix(suffix, "member.")
		index, err := strconv.Atoi(suffix)
		if err != nil || index < 1 {
			continue
		}
		if len(values) == 0 {
			continue
		}
		name := values[0]
		if name == "" {
			continue
		}
		if existing, ok := indexed[index]; ok && existing != name {
			return nil, gqaInvalidAttrError{existing}
		}
		indexed[index] = name
	}
	if len(indexed) == 0 {
		return nil, nil
	}
	order := make([]int, 0, len(indexed))
	for index := range indexed {
		order = append(order, index)
	}
	sort.Ints(order)
	names := make([]string, 0, len(order))
	seen := make(map[string]bool, len(order))
	for _, index := range order {
		name := indexed[index]
		if !gqaValidAttrNames[name] {
			return nil, gqaInvalidAttrError{name}
		}
		if seen[name] {
			continue
		}
		seen[name] = true
		names = append(names, name)
	}
	return names, nil
}

// GetQueueAttributes returns the http.HandlerFunc for the SQS
// GetQueueAttributes action reading queue state through res. The SQS query
// protocol serves every action from one endpoint, so the sqswire dispatcher
// routes Action=GetQueueAttributes here; res is the adapter that resolves a
// QueueUrl against the live queue store (returning an error that wraps
// *QueueNotFoundError for unknown queues).
func GetQueueAttributes(res QueueResolver) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			gqaWriteError(w, http.StatusBadRequest, "InvalidParameterValue", "Could not parse the request parameters.")
			return
		}

		queueURL := r.FormValue("QueueUrl")
		if queueURL == "" {
			gqaWriteError(w, http.StatusBadRequest, "MissingParameter", "The request must contain the parameter QueueUrl.")
			return
		}

		snapshot, err := res.Resolve(queueURL)
		if err != nil {
			var missing *QueueNotFoundError
			if errors.As(err, &missing) {
				gqaWriteError(w, http.StatusBadRequest, "QueueDoesNotExist", "The specified queue does not exist for this wsdl version.")
				return
			}
			gqaWriteError(w, http.StatusInternalServerError, "InternalError", gqaInternalErrorMessage)
			return
		}

		names, err := gqaNamesFromForm(r.Form)
		if err != nil {
			gqaWriteError(w, http.StatusBadRequest, "InvalidAttributeName", err.Error())
			return
		}

		attributes, err := QueueAttributes(snapshot, queueURL, names, time.Now())
		if err != nil {
			gqaWriteError(w, http.StatusBadRequest, "InvalidAttributeName", err.Error())
			return
		}

		gqaWriteResponse(w, attributes)
	}
}

// gqaWriteResponse renders the GetQueueAttributesResponse envelope of the
// query protocol byte-for-byte: XML declaration, 2012-11-05 namespace,
// repeated <Attribute><Name/><Value/> pairs, and a fresh RequestId.
func gqaWriteResponse(w http.ResponseWriter, attributes []QueueAttribute) {
	var body strings.Builder
	body.WriteString("<?xml version=\"1.0\"?>")
	body.WriteString("<GetQueueAttributesResponse xmlns=\"")
	body.WriteString(gqaXMLNS)
	body.WriteString("\"><GetQueueAttributesResult>")
	for _, attribute := range attributes {
		body.WriteString("<Attribute><Name>")
		body.WriteString(gqaEscape(attribute.Name))
		body.WriteString("</Name><Value>")
		body.WriteString(gqaEscape(attribute.Value))
		body.WriteString("</Value></Attribute>")
	}
	body.WriteString("</GetQueueAttributesResult><ResponseMetadata><RequestId>")
	body.WriteString(gqaRequestID())
	body.WriteString("</RequestId></ResponseMetadata></GetQueueAttributesResponse>")

	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(body.String()))
}

// gqaWriteError renders an SQS structured error response (sender-fault).
func gqaWriteError(w http.ResponseWriter, status int, code, message string) {
	var body strings.Builder
	body.WriteString("<?xml version=\"1.0\"?>")
	body.WriteString("<ErrorResponse xmlns=\"")
	body.WriteString(gqaXMLNS)
	body.WriteString("\"><Error><Type>Sender</Type><Code>")
	body.WriteString(gqaEscape(code))
	body.WriteString("</Code><Message>")
	body.WriteString(gqaEscape(message))
	body.WriteString("</Message><Detail/></Error><RequestId>")
	body.WriteString(gqaRequestID())
	body.WriteString("</RequestId></ErrorResponse>")

	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body.String()))
}

// gqaEscape escapes XML text content. Quotes are left as-is because values
// only ever appear inside element text, which keeps payloads such as
// RedrivePolicy or Policy JSON byte-for-byte faithful.
var gqaEscaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")

func gqaEscape(value string) string { return gqaEscaper.Replace(value) }

// gqaRequestID generates a lowercase v4 UUID used as the RequestId, matching
// the SQS wire format.
func gqaRequestID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "00000000-0000-4000-8000-000000000000"
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16])
}
