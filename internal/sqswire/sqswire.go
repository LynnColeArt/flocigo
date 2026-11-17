// Package sqswire keeps the SQS-facing state of floci-go: queues, messages,
// receipt handles and the operations behind them.
//
// DeleteMessage (ticket-023) removes a message by ReceiptHandle and replies
// with an empty HTTP 200, matching SQS on the wire:
//   - the current (most recently issued) receipt handle deletes the message
//     and it is never redelivered;
//   - stale or unknown receipt handles are a successful no-op;
//   - only a missing queue is an error (QueueDoesNotExist).
package sqswire

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultVisibilityTimeout matches SQS's default visibility period.
	DefaultVisibilityTimeout = 30 * time.Second
	// AccountID is the account segment used when building queue URLs.
	AccountID = "000000000000"
)

// queueNameRe mirrors SQS queue name rules (1-80 chars).
var queueNameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`)

// Message is a single SQS message living in a queue.
type Message struct {
	ID               string
	Body             string
	MD5OfBody        string
	SentTimestamp    time.Time
	FirstDeliveredAt time.Time
	ReceiveCount     int
	// VisibleAt is the instant the message becomes deliverable again.
	VisibleAt time.Time
	// ReceiptHandle is the handle from the most recent delivery.
	ReceiptHandle string
}

// Queue is an ordered, in-memory queue.
type Queue struct {
	Name              string
	VisibilityTimeout time.Duration
	Messages          []*Message
}

// Store holds all queues and serializes access to them.
type Store struct {
	mu     sync.Mutex
	queues map[string]*Queue
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{queues: map[string]*Queue{}}
}

// CreateQueue returns the existing queue or creates it.
func (s *Store) CreateQueue(name string, visibility time.Duration) (*Queue, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if q, ok := s.queues[name]; ok {
		return q, false
	}
	if visibility <= 0 {
		visibility = DefaultVisibilityTimeout
	}
	q := &Queue{Name: name, VisibilityTimeout: visibility}
	s.queues[name] = q
	return q, true
}

// QueueByName looks a queue up by its bare name.
func (s *Store) QueueByName(name string) (*Queue, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[name]
	return q, ok
}

// GetQueue resolves a queue URL such as
// http://localhost:4566/000000000000/my-queue to its queue.
func (s *Store) GetQueue(queueURL string) (*Queue, bool) {
	name, ok := QueueNameFromURL(queueURL)
	if !ok {
		return nil, false
	}
	return s.QueueByName(name)
}

// QueueNameFromURL extracts the trailing path segment of a queue URL.
func QueueNameFromURL(raw string) (string, bool) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", false
	}
	path := strings.Trim(u.Path, "/")
	if path == "" {
		return "", false
	}
	parts := strings.Split(path, "/")
	return parts[len(parts)-1], true
}

// QueueURL builds the canonical absolute queue URL for name.
func QueueURL(scheme, host, name string) string {
	return fmt.Sprintf("%s://%s/%s/%s", scheme, host, AccountID, name)
}

// SendMessage enqueues body, making it visible after delay.
func (s *Store) SendMessage(q *Queue, body string, delay time.Duration) *Message {
	m := &Message{
		ID:            NewMessageID(),
		Body:          body,
		MD5OfBody:     MD5Hex(body),
		SentTimestamp: time.Now().UTC(),
		VisibleAt:     time.Now().Add(delay),
	}
	s.mu.Lock()
	q.Messages = append(q.Messages, m)
	s.mu.Unlock()
	return m
}

// ReceiveMessages turns up to max visible messages in flight again, minting
// a fresh ReceiptHandle per message and waiting up to waitSeconds for the
// first candidate. visibilitySeconds < 0 falls back to the queue default.
func (s *Store) ReceiveMessages(q *Queue, max, waitSeconds, visibilitySeconds int) []*Message {
	if max <= 0 {
		max = 1
	}
	if max > 10 {
		max = 10
	}
	visibility := time.Duration(visibilitySeconds) * time.Second
	if visibilitySeconds < 0 {
		visibility = q.VisibilityTimeout
		if visibility <= 0 {
			visibility = DefaultVisibilityTimeout
		}
	}
	if waitSeconds < 0 {
		waitSeconds = 0
	}
	deadline := time.Now().Add(time.Duration(waitSeconds) * time.Second)
	for {
		s.mu.Lock()
		now := time.Now()
		var batch []*Message
		for _, m := range q.Messages {
			if len(batch) == max {
				break
			}
			if m.VisibleAt.After(now) {
				continue
			}
			batch = append(batch, m)
		}
		if len(batch) > 0 {
			for _, m := range batch {
				m.ReceiveCount++
				if m.ReceiveCount == 1 {
					m.FirstDeliveredAt = now
				}
				m.ReceiptHandle = NewReceiptHandle(m.ID, m.ReceiveCount)
				m.VisibleAt = now.Add(visibility)
			}
			s.mu.Unlock()
			return batch
		}
		s.mu.Unlock()
		if !time.Now().Before(deadline) {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// DeleteMessage removes the message currently carrying receiptHandle.
//
// SQS wire semantics: only the most recently issued receipt handle deletes a
// message; a stale or unknown handle is a successful no-op.
func (s *Store) DeleteMessage(q *Queue, receiptHandle string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, m := range q.Messages {
		if m.ReceiptHandle == receiptHandle {
			q.Messages = append(q.Messages[:i], q.Messages[i+1:]...)
			return true
		}
	}
	return false
}

// DeleteQueue removes a queue, reporting whether it existed.
func (s *Store) DeleteQueue(queueURL string) bool {
	name, ok := QueueNameFromURL(queueURL)
	if !ok {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, existed := s.queues[name]
	delete(s.queues, name)
	return existed
}

// PurgeQueue drops every message in the queue.
func (s *Store) PurgeQueue(q *Queue) {
	s.mu.Lock()
	q.Messages = nil
	s.mu.Unlock()
}

// MD5Hex is the hex MD5 digest SQS returns alongside message bodies.
func MD5Hex(body string) string {
	sum := md5.Sum([]byte(body))
	return hex.EncodeToString(sum[:])
}

// NewMessageID mints a UUIDv4-shaped message identifier.
func NewMessageID() string {
	return newUUID()
}

// NewReceiptHandle mints an opaque, per-delivery receipt handle.
func NewReceiptHandle(messageID string, delivery int) string {
	payload := fmt.Sprintf("%s|%d|%s", messageID, delivery, newUUID())
	return base64.StdEncoding.EncodeToString([]byte(payload))
}

func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail on supported platforms.
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
