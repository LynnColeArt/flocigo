package sqswire

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	// sqsNamespace is the SQS API namespace used in every response document.
	sqsNamespace = "http://queue.amazonaws.com/doc/2012-11-05/"
	// queueDoesNotExistMessage is SQS's canonical queue-not-found text.
	queueDoesNotExistMessage = "The specified queue does not exist for this wsdl version."
)

type metadata struct {
	RequestID string `xml:"RequestId"`
}

// wireMessage is a message inside a ReceiveMessage response.
type wireMessage struct {
	MessageID     string     `xml:"MessageId"`
	ReceiptHandle string     `xml:"ReceiptHandle"`
	MD5OfBody     string     `xml:"MD5OfBody"`
	Body          string     `xml:"Body"`
	Attributes    []wireAttr `xml:"Attribute,omitempty"`
}

type wireAttr struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

type createQueueResult struct {
	QueueURL string `xml:"QueueUrl"`
}

type createQueueResponse struct {
	XMLName  xml.Name          `xml:"CreateQueueResponse"`
	Xmlns    string            `xml:"xmlns,attr"`
	Result   createQueueResult `xml:"CreateQueueResult"`
	Metadata metadata          `xml:"ResponseMetadata"`
}

type getQueueURLResult struct {
	QueueURL string `xml:"QueueUrl"`
}

type getQueueURLResponse struct {
	XMLName  xml.Name          `xml:"GetQueueUrlResponse"`
	Xmlns    string            `xml:"xmlns,attr"`
	Result   getQueueURLResult `xml:"GetQueueUrlResult"`
	Metadata metadata          `xml:"ResponseMetadata"`
}

type sendMessageResult struct {
	MD5OfMessageBody string `xml:"MD5OfMessageBody"`
	MessageID        string `xml:"MessageId"`
}

type sendMessageResponse struct {
	XMLName  xml.Name          `xml:"SendMessageResponse"`
	Xmlns    string            `xml:"xmlns,attr"`
	Result   sendMessageResult `xml:"SendMessageResult"`
	Metadata metadata          `xml:"ResponseMetadata"`
}

type receiveMessageResult struct {
	Messages []wireMessage `xml:"Message"`
}

type receiveMessageResponse struct {
	XMLName  xml.Name             `xml:"ReceiveMessageResponse"`
	Xmlns    string               `xml:"xmlns,attr"`
	Result   receiveMessageResult `xml:"ReceiveMessageResult"`
	Metadata metadata             `xml:"ResponseMetadata"`
}

// ServeHTTP answers SQS query-protocol requests, dispatching on Action.
// DeleteMessage (ticket-023) replies with HTTP 200 and an empty body.
func (s *Store) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqID := NewMessageID()
	w.Header().Set("X-Amzn-RequestId", reqID)
	if err := r.ParseForm(); err != nil {
		s.writeError(w, reqID, "InvalidParameterValue", "Could not parse the request form.", http.StatusBadRequest)
		return
	}
	switch r.Form.Get("Action") {
	case "CreateQueue":
		s.handleCreateQueue(w, r, reqID)
	case "GetQueueUrl":
		s.handleGetQueueURL(w, r, reqID)
	case "SendMessage":
		s.handleSendMessage(w, r, reqID)
	case "ReceiveMessage":
		s.handleReceiveMessage(w, r, reqID)
	case "DeleteMessage": // ticket-023
		s.handleDeleteMessage(w, r, reqID)
	case "DeleteQueue":
		s.handleDeleteQueue(w, r, reqID)
	case "PurgeQueue":
		s.handlePurgeQueue(w, r, reqID)
	default:
		s.writeError(w, reqID, "InvalidAction",
			fmt.Sprintf("The action %s is not valid for this web service.", r.Form.Get("Action")),
			http.StatusBadRequest)
	}
}

// handleDeleteMessage removes the message addressed by ReceiptHandle.
//
// Wire behavior, matching SQS:
//   - success is HTTP 200 with an EMPTY body (no XML envelope);
//   - the current (most recently issued) handle deletes the message and it
//     is never redelivered;
//   - stale or unknown handles are a successful no-op;
//   - a missing ReceiptHandle is MissingParameter (400);
//   - an unknown queue is QueueDoesNotExist (400).
func (s *Store) handleDeleteMessage(w http.ResponseWriter, r *http.Request, reqID string) {
	queueURL := r.Form.Get("QueueUrl")
	if queueURL == "" {
		s.writeError(w, reqID, "MissingParameter", "The request must contain the parameter QueueUrl.", http.StatusBadRequest)
		return
	}
	receiptHandle := r.Form.Get("ReceiptHandle")
	if receiptHandle == "" {
		s.writeError(w, reqID, "MissingParameter", "The request must contain the parameter ReceiptHandle.", http.StatusBadRequest)
		return
	}
	q, ok := s.GetQueue(queueURL)
	if !ok {
		s.writeError(w, reqID, "QueueDoesNotExist", queueDoesNotExistMessage, http.StatusBadRequest)
		return
	}
	s.DeleteMessage(q, receiptHandle)
	// SQS replies to DeleteMessage with an empty body, no XML envelope.
	w.WriteHeader(http.StatusOK)
}

func (s *Store) handleCreateQueue(w http.ResponseWriter, r *http.Request, reqID string) {
	name := r.Form.Get("QueueName")
	if name == "" {
		s.writeError(w, reqID, "MissingParameter", "The request must contain the parameter QueueName.", http.StatusBadRequest)
		return
	}
	if !queueNameRe.MatchString(name) {
		s.writeError(w, reqID, "InvalidParameterValue", "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 characters", http.StatusBadRequest)
		return
	}
	visibility := DefaultVisibilityTimeout
	if raw := r.Form.Get("VisibilityTimeout"); raw != "" {
		secs, err := strconv.Atoi(raw)
		if err != nil || secs < 0 || secs > 43200 {
			s.writeError(w, reqID, "InvalidParameterValue",
				"Value for parameter VisibilityTimeout is invalid. Reason: VisibilityTimeout must be an integer between 0 and 43200.",
				http.StatusBadRequest)
			return
		}
		visibility = time.Duration(secs) * time.Second
	}
	s.CreateQueue(name, visibility)
	resp := createQueueResponse{
		Xmlns:    sqsNamespace,
		Result:   createQueueResult{QueueURL: QueueURL(scheme(r), r.Host, name)},
		Metadata: metadata{RequestID: reqID},
	}
	s.writeXML(w, http.StatusOK, xmlDocument(resp))
}

func (s *Store) handleGetQueueURL(w http.ResponseWriter, r *http.Request, reqID string) {
	name := r.Form.Get("QueueName")
	if name == "" {
		s.writeError(w, reqID, "MissingParameter", "The request must contain the parameter QueueName.", http.StatusBadRequest)
		return
	}
	if _, ok := s.QueueByName(name); !ok {
		s.writeError(w, reqID, "QueueDoesNotExist", queueDoesNotExistMessage, http.StatusBadRequest)
		return
	}
	resp := getQueueURLResponse{
		Xmlns:    sqsNamespace,
		Result:   getQueueURLResult{QueueURL: QueueURL(scheme(r), r.Host, name)},
		Metadata: metadata{RequestID: reqID},
	}
	s.writeXML(w, http.StatusOK, xmlDocument(resp))
}

func (s *Store) handleSendMessage(w http.ResponseWriter, r *http.Request, reqID string) {
	queueURL := r.Form.Get("QueueUrl")
	if queueURL == "" {
		s.writeError(w, reqID, "MissingParameter", "The request must contain the parameter QueueUrl.", http.StatusBadRequest)
		return
	}
	body := r.Form.Get("MessageBody")
	if body == "" {
		s.writeError(w, reqID, "MissingParameter", "The request must contain the parameter MessageBody.", http.StatusBadRequest)
		return
	}
	delay := time.Duration(0)
	if raw := r.Form.Get("DelaySeconds"); raw != "" {
		secs, err := strconv.Atoi(raw)
		if err != nil || secs < 0 || secs > 900 {
			s.writeError(w, reqID, "InvalidParameterValue",
				"Value for parameter DelaySeconds is invalid. Reason: DelaySeconds must be an integer between 0 and 900.",
				http.StatusBadRequest)
			return
		}
		delay = time.Duration(secs) * time.Second
	}
	q, ok := s.GetQueue(queueURL)
	if !ok {
		s.writeError(w, reqID, "QueueDoesNotExist", queueDoesNotExistMessage, http.StatusBadRequest)
		return
	}
	m := s.SendMessage(q, body, delay)
	resp := sendMessageResponse{
		Xmlns:    sqsNamespace,
		Result:   sendMessageResult{MD5OfMessageBody: m.MD5OfBody, MessageID: m.ID},
		Metadata: metadata{RequestID: reqID},
	}
	s.writeXML(w, http.StatusOK, xmlDocument(resp))
}

func (s *Store) handleReceiveMessage(w http.ResponseWriter, r *http.Request, reqID string) {
	queueURL := r.Form.Get("QueueUrl")
	if queueURL == "" {
		s.writeError(w, reqID, "MissingParameter", "The request must contain the parameter QueueUrl.", http.StatusBadRequest)
		return
	}
	max, _, err := formInt(r.Form, "MaxNumberOfMessages", 1)
	if err != nil || max < 1 || max > 10 {
		s.writeError(w, reqID, "InvalidParameterValue",
			"Value for parameter MaxNumberOfMessages is invalid. Reason: Must be between 1 and 10.",
			http.StatusBadRequest)
		return
	}
	wait, _, err := formInt(r.Form, "WaitTimeSeconds", 0)
	if err != nil || wait < 0 || wait > 20 {
		s.writeError(w, reqID, "InvalidParameterValue",
			"Value for parameter WaitTimeSeconds is invalid. Reason: WaitTimeSeconds must be an integer between 0 and 20.",
			http.StatusBadRequest)
		return
	}
	visibilitySeconds := -1
	if raw := r.Form.Get("VisibilityTimeout"); raw != "" {
		secs, verr := strconv.Atoi(raw)
		if verr != nil || secs < 0 || secs > 43200 {
			s.writeError(w, reqID, "InvalidParameterValue",
				"Value for parameter VisibilityTimeout is invalid. Reason: VisibilityTimeout must be an integer between 0 and 43200.",
				http.StatusBadRequest)
			return
		}
		visibilitySeconds = secs
	}
	q, ok := s.GetQueue(queueURL)
	if !ok {
		s.writeError(w, reqID, "QueueDoesNotExist", queueDoesNotExistMessage, http.StatusBadRequest)
		return
	}
	msgs := s.ReceiveMessages(q, max, wait, visibilitySeconds)
	if len(msgs) == 0 {
		body := xml.Header +
			`<ReceiveMessageResponse xmlns="` + sqsNamespace + `">` + "\n" +
				"  <ReceiveMessageResult/>\n" +
				"  <ResponseMetadata>\n" +
				"    <RequestId>" + reqID + "</RequestId>\n" +
				"  </ResponseMetadata>\n" +
				"</ReceiveMessageResponse>"
		s.writeXML(w, http.StatusOK, body)
		return
	}
	wire := make([]wireMessage, 0, len(msgs))
	requested := attributeRequests(r.Form)
	for _, m := range msgs {
		wire = append(wire, wireMessage{
			MessageID:     m.ID,
			ReceiptHandle: m.ReceiptHandle,
			MD5OfBody:     m.MD5OfBody,
			Body:          m.Body,
			Attributes:    messageAttributes(m, requested),
		})
	}
	resp := receiveMessageResponse{
		Xmlns:    sqsNamespace,
		Result:   receiveMessageResult{Messages: wire},
		Metadata: metadata{RequestID: reqID},
	}
	s.writeXML(w, http.StatusOK, xmlDocument(resp))
}

func (s *Store) handleDeleteQueue(w http.ResponseWriter, r *http.Request, reqID string) {
	queueURL := r.Form.Get("QueueUrl")
	if queueURL == "" {
		s.writeError(w, reqID, "MissingParameter", "The request must contain the parameter QueueUrl.", http.StatusBadRequest)
		return
	}
	s.DeleteQueue(queueURL)
	w.WriteHeader(http.StatusOK)
}

func (s *Store) handlePurgeQueue(w http.ResponseWriter, r *http.Request, reqID string) {
	queueURL := r.Form.Get("QueueUrl")
	if queueURL == "" {
		s.writeError(w, reqID, "MissingParameter", "The request must contain the parameter QueueUrl.", http.StatusBadRequest)
		return
	}
	q, ok := s.GetQueue(queueURL)
	if !ok {
		s.writeError(w, reqID, "QueueDoesNotExist", queueDoesNotExistMessage, http.StatusBadRequest)
		return
	}
	s.PurgeQueue(q)
	w.WriteHeader(http.StatusOK)
}

// formInt parses an optional integer form value, returning def when absent.
func formInt(form url.Values, key string, def int) (int, bool, error) {
	raw := form.Get(key)
	if raw == "" {
		return def, false, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, true, err
	}
	return v, true, nil
}

// attributeRequests collects AttributeName / AttributeName.N list entries.
func attributeRequests(form url.Values) []string {
	var names []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("AttributeName.%d", i))
		if v == "" {
			break
		}
		names = append(names, v)
	}
	names = append(names, form["AttributeName"]...)
	return names
}

// messageAttributes renders the requested SQS message attributes.
func messageAttributes(m *Message, requested []string) []wireAttr {
	if len(requested) == 0 {
		return nil
	}
	values := map[string]string{
		"SenderId":                         AccountID,
		"SentTimestamp":                    strconv.FormatInt(m.SentTimestamp.UnixMilli(), 10),
		"ApproximateReceiveCount":          strconv.Itoa(m.ReceiveCount),
		"ApproximateFirstReceiveTimestamp": strconv.FormatInt(m.FirstDeliveredAt.UnixMilli(), 10),
	}
	var out []wireAttr
	include := func(name string) bool {
		for _, want := range requested {
			if want == "All" || want == name {
				return true
			}
		}
		return false
	}
	for _, name := range []string{"SenderId", "SentTimestamp", "ApproximateReceiveCount", "ApproximateFirstReceiveTimestamp"} {
		if include(name) {
			out = append(out, wireAttr{Name: name, Value: values[name]})
		}
	}
	return out
}

func scheme(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

func xmlDocument(v interface{}) string {
	raw, err := xml.MarshalIndent(v, "", "  ")
	if err != nil {
		return "" // response shapes are static; this cannot happen
	}
	return xml.Header + string(raw)
}

func (s *Store) writeXML(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/xml; charset=UTF-8")
	w.WriteHeader(status)
	fmt.Fprint(w, body)
}

func (s *Store) writeError(w http.ResponseWriter, reqID, code, message string, status int) {
	body := xml.Header +
		`<ErrorResponse xmlns="` + sqsNamespace + `">` + "\n" +
			"  <Error>\n" +
			"    <Type>Sender</Type>\n" +
			"    <Code>" + xmlEscape(code) + "</Code>\n" +
			"    <Message>" + xmlEscape(message) + "</Message>\n" +
			"    <Detail/>\n" +
			"  </Error>\n" +
			"  <RequestId>" + reqID + "</RequestId>\n" +
			"</ErrorResponse>"
	w.Header().Set("Content-Type", "text/xml; charset=UTF-8")
	w.WriteHeader(status)
	fmt.Fprint(w, body)
}

func xmlEscape(s string) string {
	return strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
		"'", "&apos;",
	).Replace(s)
}
