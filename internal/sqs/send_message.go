package sqs

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// SQS message limits (AWS hard limits for standard queues).
const (
	maxMessageBodyBytes = 262144 // 256 KiB maximum message body size
	maxDelaySeconds     = 900    // 15 minute per-message delay
)

// Errors returned by SendMessage. The AWS error code is embedded in the
// text so the SQS wire layer can map them onto protocol error responses.
var (
	errQueueFileNotFound       = errors.New("QueueDoesNotExist: queue file does not exist")
	errEmptyMessageBody        = errors.New("MissingParameter: MessageBody must not be empty")
	errMessageBodyTooLong      = errors.New("MessageTooLong: message body exceeds 262144 bytes")
	errInvalidDelaySeconds     = errors.New("InvalidParameterValue: DelaySeconds must be between 0 and 900")
	errInvalidMessageContents  = errors.New("InvalidMessageContents: message body contains invalid characters")
	errInvalidMessageAttribute = errors.New("InvalidParameterValue: invalid message attribute")
)

// MessageAttributeValue holds one SQS message attribute: a transport data
// type ("String", "Number" or "Binary", optionally with a custom label
// suffix such as "Number.price") plus its transport value.

// SendMessageResult contains the fields SQS returns for SendMessage.
type SendMessageResult struct {
	MessageId              string `json:"messageId" xml:"MessageId"`
	MD5OfMessageBody       string `json:"md5OfMessageBody" xml:"MD5OfMessageBody"`
	MD5OfMessageAttributes string `json:"md5OfMessageAttributes,omitempty" xml:"MD5OfMessageAttributes,omitempty"`
}

// QueueMessageRecord is the single JSON line appended to a queue file for
// every message sent to the queue. Queue files are append-only logs;
// visibility, receipt and deletion state is tracked by the queue reader in
// separate records, never by rewriting sent messages.
type QueueMessageRecord struct {
	Type            string                           `json:"type"` // always "message"
	MessageID       string                           `json:"messageId"`
	Body            string                           `json:"body"`
	MD5OfBody       string                           `json:"md5OfBody"`
	SentTimestamp   int64                            `json:"sentTimestamp"` // unix epoch milliseconds
	VisibleFrom     int64                            `json:"visibleFrom"`   // unix epoch ms; SentTimestamp + delaySeconds
	Attributes      map[string]MessageAttributeValue `json:"attributes,omitempty"`
	MD5OfAttributes string                           `json:"md5OfAttributes,omitempty"`
}

// SendMessage appends a message to the queue's queue file and returns the
// generated MessageId together with MD5OfMessageBody, exactly as the AWS
// SendMessage operation responds. queueFile is the path of the queue file
// created by CreateQueue; the file must already exist. delaySeconds is the
// per-message delivery delay in 0..900 (0 for none) and attributes may be
// nil. The visibility start (VisibleFrom) is derived from the delay so
// ReceiveMessage can skip messages that are not yet visible.
func SendMessage(queueFile string, body string, delaySeconds int64, attributes map[string]MessageAttributeValue) (*SendMessageResult, error) {
	if body == "" {
		return nil, errEmptyMessageBody
	}
	if len(body) > maxMessageBodyBytes {
		return nil, errMessageBodyTooLong
	}
	if !utf8.ValidString(body) {
		return nil, errInvalidMessageContents
	}
	if delaySeconds < 0 || delaySeconds > maxDelaySeconds {
		return nil, errInvalidDelaySeconds
	}

	messageID, err := newMessageID()
	if err != nil {
		return nil, err
	}
	res := &SendMessageResult{
		MessageId:        messageID,
		MD5OfMessageBody: md5HexUTF8(body),
	}

	rec := &QueueMessageRecord{
		Type:          "message",
		MessageID:     messageID,
		Body:          body,
		MD5OfBody:     res.MD5OfMessageBody,
		SentTimestamp: time.Now().UnixMilli(),
	}
	rec.VisibleFrom = rec.SentTimestamp + delaySeconds*1000
	if len(attributes) > 0 {
		attrs, attrsMD5, err := normalizeMessageAttributes(attributes)
		if err != nil {
			return nil, err
		}
		rec.Attributes = attrs
		rec.MD5OfAttributes = attrsMD5
		res.MD5OfMessageAttributes = attrsMD5
	}

	if err := AppendMessageRecord(queueFile, rec); err != nil {
		return nil, err
	}
	return res, nil
}

// sendMu serializes queue-file appends within this package so concurrent
// SendMessage calls never interleave partial records.
var sendMu sync.Mutex

// AppendMessageRecord marshals rec as one JSON line and appends it to the
// end of queueFile with a single O_APPEND write. The queue file must exist
// (missing files map to an SQS QueueDoesNotExist error) and a missing
// trailing newline from previous writers is repaired before appending.
func AppendMessageRecord(queueFile string, rec *QueueMessageRecord) error {
	line, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("sqs: encode message record: %w", err)
	}
	line = append(line, '\n')

	sendMu.Lock()
	defer sendMu.Unlock()

	f, err := os.OpenFile(queueFile, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: %s", errQueueFileNotFound, queueFile)
		}
		return fmt.Errorf("sqs: open queue file %s: %w", queueFile, err)
	}
	defer f.Close()

	if info, err := f.Stat(); err == nil && info.Size() > 0 {
		var tail [1]byte
		if _, err := f.ReadAt(tail[:], info.Size()-1); err == nil && tail[0] != '\n' {
			if _, err := f.Write([]byte("\n")); err != nil {
				return fmt.Errorf("sqs: repair queue file %s: %w", queueFile, err)
			}
		}
	}
	if _, err := f.Write(line); err != nil {
		return fmt.Errorf("sqs: append message to %s: %w", queueFile, err)
	}
	return nil
}

// normalizeMessageAttributes validates the given attributes and computes
// the AWS MD5OfMessageAttributes digest: attributes are sorted by name and
// the digest absorbs, per attribute, the name, its transport-encoded value
// and its transport type, each prefixed with a big-endian 4-byte length.
func normalizeMessageAttributes(in map[string]MessageAttributeValue) (map[string]MessageAttributeValue, string, error) {
	names := make([]string, 0, len(in))
	for name := range in {
		if name == "" {
			return nil, "", fmt.Errorf("%w: empty attribute name", errInvalidMessageAttribute)
		}
		names = append(names, name)
	}
	sort.Strings(names)


	out := make(map[string]MessageAttributeValue, len(in))
	digest := md5.New()
	var lenbuf bytes.Buffer
	for _, name := range names {
		v := in[name]
		transport := attributeTransportType(v.DataType)
		var value []byte
		switch transport {
		case "String":
			if !utf8.ValidString(v.StringValue) {
			return nil, "", fmt.Errorf("%w: %s: invalid UTF-8 value", errInvalidMessageAttribute, name)
			}
			value = []byte(v.StringValue)
		case "Number":
			norm, err := normalizeAttributeNumber(v.StringValue)
			if err != nil {
			return nil, "", fmt.Errorf("%w: %s: %v", errInvalidMessageAttribute, name, err)
			}
			value = []byte(norm)
		case "Binary":
			value = v.BinaryValue
		default:
			return nil, "", fmt.Errorf("%w: %s: unsupported data type %q", errInvalidMessageAttribute, name, v.DataType)
		}
	
			lenbuf.Reset()
			writeLenPrefixed(&lenbuf, name)
			digest.Write(lenbuf.Bytes())
			lenbuf.Reset()
			writeLenPrefixed(&lenbuf, string(value))
			digest.Write(lenbuf.Bytes())
			lenbuf.Reset()
			writeLenPrefixed(&lenbuf, transport)
			digest.Write(lenbuf.Bytes())
		out[name] = v
	}
	return out, hex.EncodeToString(digest.Sum(nil)), nil
}

// writeLenPrefixed absorbs length-prefixed data into the digest and returns
// w so calls can be chained.

// attributeTransportType strips a custom label suffix: "Number.price"
// becomes "Number"; plain types are returned unchanged.
func attributeTransportType(dataType string) string {
	if i := strings.IndexByte(dataType, '.'); i > 0 && i < len(dataType)-1 {
		return dataType[:i]
	}
	return dataType
}

// normalizeAttributeNumber normalizes an SQS Number transport value the way
// AWS does before hashing: a leading "+" and leading zeros of the integral
// part are removed, as are trailing zeros of the fractional part together
// with a dangling ".".
func normalizeAttributeNumber(s string) (string, error) {
	if _, err := strconv.ParseFloat(s, 64); err != nil {
		return "", fmt.Errorf("%w: not a number (%q)", errInvalidMessageAttribute, s)
	}
	s = strings.TrimPrefix(s, "+")
	sign := ""
	if strings.HasPrefix(s, "-") {
		sign, s = "-", s[1:]
	}
	if i := strings.IndexByte(s, '.'); i >= 0 {
		frac := strings.TrimRight(s[i+1:], "0")
		if frac == "" {
			s = s[:i]
		} else {
			s = s[:i+1] + frac
		}
	}
	j := 0
	for j+1 < len(s) && s[j] == '0' && s[j+1] != '.' {
		j++
	}
	s = sign + s[j:]
	if s == "" || s == "-" {
		return "0", nil
	}
	return s, nil
}

// md5HexUTF8 returns the lowercase hex MD5 digest of the UTF-8 bytes of s,
// which is what SQS reports as MD5OfMessageBody / MD5OfBody.
func md5HexUTF8(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

// newMessageID returns a random RFC 4122 version-4 UUID, the format SQS
// uses for MessageId values.
func newMessageID() (string, error) {
	var u [16]byte
	if _, err := rand.Read(u[:]); err != nil {
		return "", fmt.Errorf("sqs: generate message id: %w", err)
	}
	u[6] = (u[6] & 0x0f) | 0x40
	u[8] = (u[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16]), nil
}
