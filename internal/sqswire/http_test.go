package sqswire

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// recvMsg mirrors the message element of ReceiveMessage responses.
type recvMsg struct {
	MessageID     string `xml:"MessageId"`
	ReceiptHandle string `xml:"ReceiptHandle"`
	MD5OfBody     string `xml:"MD5OfBody"`
	Body          string `xml:"Body"`
}

type errorBody struct {
	Error struct {
		Type    string `xml:"Type"`
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	} `xml:"Error"`
}

func newServer(tb *testing.T) (*httptest.Server, *Store) {
	tb.Helper()
	store := NewStore()
	ts := httptest.NewServer(store)
	tb.Cleanup(ts.Close)
	return ts, store
}

func postForm(tb *testing.T, ts *httptest.Server, params map[string]string) (int, string, http.Header) {
	tb.Helper()
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	resp, err := http.Post(ts.URL, "application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
	if err != nil {
		tb.Fatalf("POST %s: %v", ts.URL, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		tb.Fatalf("reading response: %v", err)
	}
	return resp.StatusCode, string(raw), resp.Header
}

func createQueue(tb *testing.T, ts *httptest.Server, name string, opts ...string) string {
	tb.Helper()
	params := map[string]string{"Action": "CreateQueue", "QueueName": name}
	if len(opts) > 0 && opts[0] != "" {
		params["VisibilityTimeout"] = opts[0]
	}
	status, body, _ := postForm(tb, ts, params)
	if status != http.StatusOK {
		tb.Fatalf("CreateQueue %q: status %d body %s", name, status, body)
	}
	var resp struct {
		Result struct {
			QueueURL string `xml:"QueueUrl"`
		} `xml:"CreateQueueResult"`
	}
	if err := xml.Unmarshal([]byte(body), &resp); err != nil {
		tb.Fatalf("CreateQueue %q: bad XML: %v\n%s", name, err, body)
	}
	if resp.Result.QueueURL == "" {
		tb.Fatalf("CreateQueue %q returned no QueueUrl: %s", name, body)
	}
	return resp.Result.QueueURL
}

func sendMessage(tb *testing.T, ts *httptest.Server, queueURL, body string) (id, md5OfBody string) {
	tb.Helper()
	status, raw, _ := postForm(tb, ts, map[string]string{
		"Action":      "SendMessage",
		"QueueUrl":    queueURL,
		"MessageBody": body,
	})
	if status != http.StatusOK {
		tb.Fatalf("SendMessage: status %d body %s", status, raw)
	}
	var resp struct {
		Result struct {
			MessageID string `xml:"MessageId"`
			MD5       string `xml:"MD5OfMessageBody"`
		} `xml:"SendMessageResult"`
	}
	if err := xml.Unmarshal([]byte(raw), &resp); err != nil {
		tb.Fatalf("SendMessage: bad XML: %v\n%s", err, raw)
	}
	return resp.Result.MessageID, resp.Result.MD5
}

func receive(tb *testing.T, ts *httptest.Server, queueURL string, extra map[string]string) []recvMsg {
	tb.Helper()
	params := map[string]string{"Action": "ReceiveMessage", "QueueUrl": queueURL}
	for k, v := range extra {
		params[k] = v
	}
	status, raw, _ := postForm(tb, ts, params)
	if status != http.StatusOK {
		tb.Fatalf("ReceiveMessage: status %d body %s", status, raw)
	}
	var resp struct {
		Result struct {
			Messages []recvMsg `xml:"Message"`
		} `xml:"ReceiveMessageResult"`
	}
	if err := xml.Unmarshal([]byte(raw), &resp); err != nil {
		tb.Fatalf("ReceiveMessage: bad XML: %v\n%s", err, raw)
	}
	return resp.Result.Messages
}

// ticket-023: DeleteMessage removes the message by ReceiptHandle and answers
// with an empty HTTP 200 response.
func TestDeleteMessageRemovesByReceiptHandleWithEmptyResponse(t *testing.T) {
	ts, _ := newServer(t)
	queueURL := createQueue(t, ts, "delete-me")

	id, md5OfBody := sendMessage(t, ts, queueURL, "hello world")
	if md5OfBody != MD5Hex("hello world") {
		t.Fatalf("SendMessage MD5OfMessageBody = %q, want %q", md5OfBody, MD5Hex("hello world"))
	}

	msgs := receive(t, ts, queueURL, map[string]string{"MaxNumberOfMessages": "1"})
	if len(msgs) != 1 {
		t.Fatalf("ReceiveMessage returned %d messages, want 1", len(msgs))
	}
	if msgs[0].MessageID != id || msgs[0].Body != "hello world" {
		t.Fatalf("unexpected message: %+v", msgs[0])
	}
	handle := msgs[0].ReceiptHandle
	if handle == "" {
		t.Fatal("ReceiveMessage returned an empty ReceiptHandle")
	}

	status, body, _ := postForm(t, ts, map[string]string{
		"Action":        "DeleteMessage",
		"QueueUrl":      queueURL,
		"ReceiptHandle": handle,
	})
	if status != http.StatusOK {
		t.Fatalf("DeleteMessage status = %d, want 200 (body %q)", status, body)
	}
	if body != "" {
		t.Fatalf("DeleteMessage body = %q, want an empty response", body)
	}

	if got := receive(t, ts, queueURL, nil); len(got) != 0 {
		t.Fatalf("deleted message was redelivered: %+v", got)
	}
}

func TestDeleteMessageUnknownHandleIsNoOpWithEmptyResponse(t *testing.T) {
	ts, _ := newServer(t)
	queueURL := createQueue(t, ts, "unknown-handle")
	sendMessage(t, ts, queueURL, "keep me")

	status, body, _ := postForm(t, ts, map[string]string{
		"Action":        "DeleteMessage",
		"QueueUrl":      queueURL,
		"ReceiptHandle": "not-a-real-receipt-handle",
	})
	if status != http.StatusOK || body != "" {
		t.Fatalf("DeleteMessage with unknown handle: got (%d, %q), want (200, empty)", status, body)
	}

	msgs := receive(t, ts, queueURL, nil)
	if len(msgs) != 1 || msgs[0].Body != "keep me" {
		t.Fatalf("no-op delete must not remove the message: %+v", msgs)
	}

	status, body, _ = postForm(t, ts, map[string]string{
		"Action":        "DeleteMessage",
		"QueueUrl":      queueURL,
		"ReceiptHandle": msgs[0].ReceiptHandle,
	})
	if status != http.StatusOK || body != "" {
		t.Fatalf("DeleteMessage with current handle: got (%d, %q), want (200, empty)", status, body)
	}
	if got := receive(t, ts, queueURL, nil); len(got) != 0 {
		t.Fatalf("deleted message was redelivered: %+v", got)
	}
}

func TestDeleteMessageStaleHandleAfterRedeliveryIsNoOp(t *testing.T) {
	ts, _ := newServer(t)
	queueURL := createQueue(t, ts, "stale-handle", "1") // visibility of 1s

	sendMessage(t, ts, queueURL, "payload")

	first := receive(t, ts, queueURL, nil)
	if len(first) != 1 {
		t.Fatalf("first receive returned %d messages, want 1", len(first))
	}
	stale := first[0].ReceiptHandle

	time.Sleep(1500 * time.Millisecond) // let the visibility lapse
	second := receive(t, ts, queueURL, nil)
	if len(second) != 1 {
		t.Fatalf("redelivery returned %d messages, want 1", len(second))
	}
	if second[0].ReceiptHandle == stale {
		t.Fatal("redelivery must mint a fresh receipt handle")
	}

	// Deleting with the stale handle succeeds but must not remove anything.
	status, body, _ := postForm(t, ts, map[string]string{
		"Action":        "DeleteMessage",
		"QueueUrl":      queueURL,
		"ReceiptHandle": stale,
	})
	if status != http.StatusOK || body != "" {
		t.Fatalf("DeleteMessage with stale handle: got (%d, %q), want (200, empty)", status, body)
	}

	time.Sleep(1500 * time.Millisecond) // let the new visibility lapse
	third := receive(t, ts, queueURL, nil)
	if len(third) != 1 {
		t.Fatalf("stale handle removed the message; receive got %+v", third)
	}

	status, body, _ = postForm(t, ts, map[string]string{
		"Action":        "DeleteMessage",
		"QueueUrl":      queueURL,
		"ReceiptHandle": third[0].ReceiptHandle,
	})
	if status != http.StatusOK || body != "" {
		t.Fatalf("DeleteMessage with fresh handle: got (%d, %q), want (200, empty)", status, body)
	}
	if got := receive(t, ts, queueURL, nil); len(got) != 0 {
		t.Fatalf("deleted message was redelivered: %+v", got)
	}
}

func TestDeleteMessageValidationAndUnknownQueue(t *testing.T) {
	ts, _ := newServer(t)
	queueURL := createQueue(t, ts, "validation")

	cases := []struct {
		name       string
		params     map[string]string
		wantStatus int
		wantCode   string
	}{
		{
			name:       "missing QueueUrl",
			params:     map[string]string{"Action": "DeleteMessage", "ReceiptHandle": "some-handle"},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MissingParameter",
		},
		{
			name:       "missing ReceiptHandle",
			params:     map[string]string{"Action": "DeleteMessage", "QueueUrl": queueURL},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MissingParameter",
		},
		{
			name: "unknown queue",
			params: map[string]string{
				"Action":        "DeleteMessage",
				"QueueUrl":      "http://localhost:4566/000000000000/no-such-queue",
				"ReceiptHandle": "some-handle",
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "QueueDoesNotExist",
		},
	}
	for _, tc := range cases {
		status, body, _ := postForm(t, ts, tc.params)
		if status != tc.wantStatus {
			t.Errorf("%s: status = %d, want %d (body %s)", tc.name, status, tc.wantStatus, body)
			continue
		}
		var resp errorBody
		if err := xml.Unmarshal([]byte(body), &resp); err != nil {
			t.Errorf("%s: bad error XML: %v\n%s", tc.name, err, body)
			continue
		}
		if resp.Error.Code != tc.wantCode {
			t.Errorf("%s: code = %q, want %q", tc.name, resp.Error.Code, tc.wantCode)
		}
		if tc.name == "missing ReceiptHandle" && !strings.Contains(resp.Error.Message, "ReceiptHandle") {
			t.Errorf("%s: message %q does not name the parameter", tc.name, resp.Error.Message)
		}
	}
}

func TestStoreDeleteMessageRemovesCurrentHandleOnly(t *testing.T) {
	store := NewStore()
	q, created := store.CreateQueue("direct", time.Second)
	if !created {
		t.Fatal("queue should be newly created")
	}
	store.SendMessage(q, "body", 0)
	if len(q.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(q.Messages))
	}
	batch := store.ReceiveMessages(q, 1, 0, -1)
	if len(batch) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(batch))
	}
	handle := batch[0].ReceiptHandle

	if store.DeleteMessage(q, "gibberish") {
		t.Fatal("unknown handle must be a no-op (false)")
	}
	if !store.DeleteMessage(q, handle) {
		t.Fatal("current handle must delete the message")
	}
	if len(q.Messages) != 0 {
		t.Fatalf("expected an empty queue, got %d messages", len(q.Messages))
	}
}
