package sqs

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var messageIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func newTestQueueFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "queue.jsonl")
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write queue file: %v", err)
	}
	return path
}

func readQueueLines(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read queue file: %v", err)
	}
	trimmed := strings.TrimRight(string(data), "\n")
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "\n")
}

func TestSendMessageReturnsMessageIDAndBodyMD5(t *testing.T) {
	qf := newTestQueueFile(t, "")
	res, err := SendMessage(qf, "hello world", 0, nil)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if !messageIDPattern.MatchString(res.MessageId) {
		t.Fatalf("MessageId = %q, want a version-4 UUID", res.MessageId)
	}
	sum := md5.Sum([]byte("hello world"))
	if want := hex.EncodeToString(sum[:]); res.MD5OfMessageBody != want {
		t.Fatalf("MD5OfMessageBody = %q, want %q", res.MD5OfMessageBody, want)
	}
	if res.MD5OfMessageAttributes != "" {
		t.Fatalf("MD5OfMessageAttributes = %q, want empty without attributes", res.MD5OfMessageAttributes)
	}
}

func TestSendMessageAppendsRecordToQueueFile(t *testing.T) {
	header := `{"type":"queue","name":"orders"}` + "\n"
	qf := newTestQueueFile(t, header)
	res, err := SendMessage(qf, "body-1", 15, nil)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	lines := readQueueLines(t, qf)
	if len(lines) != 2 {
		t.Fatalf("queue file has %d lines, want 2: %q", len(lines), lines)
	}
	if want := header[:len(header)-1]; lines[0] != want {
		t.Fatalf("header line changed: got %q, want %q", lines[0], want)
	}
	var rec QueueMessageRecord
	if err := json.Unmarshal([]byte(lines[1]), &rec); err != nil {
		t.Fatalf("decode appended record: %v", err)
	}
	if rec.Type != "message" {
		t.Errorf("Type = %q, want message", rec.Type)
	}
	if rec.MessageID != res.MessageId {
		t.Errorf("MessageID = %q, want %q", rec.MessageID, res.MessageId)
	}
	if rec.Body != "body-1" {
		t.Errorf("Body = %q, want body-1", rec.Body)
	}
	if rec.MD5OfBody != res.MD5OfMessageBody {
		t.Errorf("MD5OfBody = %q, want %q", rec.MD5OfBody, res.MD5OfMessageBody)
	}
	if got := rec.VisibleFrom - rec.SentTimestamp; got != 15000 {
		t.Errorf("visibility offset = %d ms, want 15000 ms for a 15s delay", got)
	}
}

func TestSendMessageRepairsMissingTrailingNewline(t *testing.T) {
	qf := newTestQueueFile(t, `{"type":"queue"}`) // no trailing newline
	if _, err := SendMessage(qf, "m1", 0, nil); err != nil {
		t.Fatalf("SendMessage(m1): %v", err)
	}
	if _, err := SendMessage(qf, "m2", 0, nil); err != nil {
		t.Fatalf("SendMessage(m2): %v", err)
	}
	lines := readQueueLines(t, qf)
	if len(lines) != 3 {
		t.Fatalf("queue file has %d lines, want 3", len(lines))
	}
	var last QueueMessageRecord
	if err := json.Unmarshal([]byte(lines[2]), &last); err != nil {
		t.Fatalf("decode third record: %v", err)
	}
	if last.Body != "m2" {
		t.Fatalf("third record Body = %q, want m2", last.Body)
	}
}

func TestSendMessageValidation(t *testing.T) {
	qf := newTestQueueFile(t, "")
	cases := []struct {
		name      string
		queueFile string
		body      string
		delay     int64
		want      error
	}{
		{"missing queue file", filepath.Join(t.TempDir(), "absent.jsonl"), "x", 0, errQueueFileNotFound},
		{"empty body", qf, "", 0, errEmptyMessageBody},
		{"body too long", qf, strings.Repeat("a", maxMessageBodyBytes+1), 0, errMessageBodyTooLong},
		{"negative delay", qf, "x", -1, errInvalidDelaySeconds},
		{"delay over max", qf, "x", maxDelaySeconds + 1, errInvalidDelaySeconds},
		{"invalid utf-8 body", qf, "\xff\xfe", 0, errInvalidMessageContents},
	}
	for _, tc := range cases {
		if _, err := SendMessage(tc.queueFile, tc.body, tc.delay, nil); !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
	}
	if _, err := SendMessage(qf, strings.Repeat("a", maxMessageBodyBytes), maxDelaySeconds, nil); err != nil {
		t.Errorf("max-size body with max delay rejected: %v", err)
	}
}

func TestSendMessageWithAttributes(t *testing.T) {
	qf := newTestQueueFile(t, "")
	attrs := map[string]MessageAttributeValue{
		"alpha": {DataType: "String", String: "one"},
		"beta":  {DataType: "Binary", Binary: []byte{0x01, 0x02, 0x03}},
	}
	res, err := SendMessage(qf, "attr body", 0, attrs)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if _, err := hex.DecodeString(res.MD5OfMessageAttributes); err != nil || len(res.MD5OfMessageAttributes) != 32 {
		t.Fatalf("MD5OfMessageAttributes = %q, want 32 hex chars", res.MD5OfMessageAttributes)
	}

	// Building the same attribute map in a different insertion order must
	// produce the same digest because names are sorted before hashing.
	attrsReordered := map[string]MessageAttributeValue{
		"beta":  {DataType: "Binary", Binary: []byte{0x01, 0x02, 0x03}},
		"alpha": {DataType: "String", String: "one"},
	}
	resReordered, err := SendMessage(qf, "attr body", 0, attrsReordered)
	if err != nil {
		t.Fatalf("SendMessage(reordered): %v", err)
	}
	if res.MD5OfMessageAttributes != resReordered.MD5OfMessageAttributes {
		t.Fatalf("digest not stable: %q vs %q", res.MD5OfMessageAttributes, resReordered.MD5OfMessageAttributes)
	}

	lines := readQueueLines(t, qf)
	var first QueueMessageRecord
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("decode first record: %v", err)
	}
	if first.Attributes["alpha"].DataType != "String" {
		t.Errorf("alpha DataType = %q, want String", first.Attributes["alpha"].DataType)
	}
	if first.MD5OfAttributes != res.MD5OfMessageAttributes {
		t.Errorf("record MD5OfAttributes mismatch")
	}
}

func TestAttributeTransportTypeAndNumberNormalization(t *testing.T) {
	if got := attributeTransportType("Number.price"); got != "Number" {
		t.Errorf("attributeTransportType(Number.price) = %q, want Number", got)
	}
	if got := attributeTransportType("String"); got != "String" {
		t.Errorf("attributeTransportType(String) = %q, want String", got)
	}
	cases := map[string]string{
		"0":         "0",
		"0123":      "123",
		"+6.0":      "6",
		"0123.4500": "123.45",
		"-0012.5":   "-12.5",
		"12.345":    "12.345",
		"00.5":      "0.5",
	}
	for in, want := range cases {
		got, err := normalizeAttributeNumber(in)
		if err != nil {
			t.Errorf("normalizeAttributeNumber(%q): %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("normalizeAttributeNumber(%q) = %q, want %q", in, got, want)
		}
	}
	if _, err := normalizeAttributeNumber("abc"); err == nil {
		t.Errorf("normalizeAttributeNumber(abc): want error for non-number value")
	}
}
