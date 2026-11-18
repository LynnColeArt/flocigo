package sqs

import "testing"

// Minimal stand-ins for the message shapes the store keeps: slice bags of
// messages with per-message receipt handles, receipt-handle-keyed
// in-flight bags (plain and entry-wrapped), and queue registries keyed by
// queue URL.
type dmMessage struct {
	ID            string
	Body          string
	MD5OfBody     string
	ReceiptHandle string
}

type dmQueue struct {
	Name     string
	URL      string
	Messages []*dmMessage
	inflight map[string]*dmMessage
}

type dmReceipt struct {
	Msg *dmMessage
}

type dmWrappedQueue struct {
	inflight map[string]*dmReceipt
}

type dmStore struct {
	Queues map[string]*dmQueue
}

func TestDeleteMessageRemovesMessageByReceiptHandle(t *testing.T) {
	q := &dmQueue{
		Messages: []*dmMessage{
			{ID: "m1", Body: "alpha", ReceiptHandle: "rh-1"},
			{ID: "m2", Body: "beta", ReceiptHandle: "rh-2"},
		},
	}
	if err := DeleteMessage(q, "rh-1"); err != nil {
		t.Fatalf("DeleteMessage: unexpected error %v", err)
	}
	if len(q.Messages) != 1 {
		t.Fatalf("expected 1 message left, got %d", len(q.Messages))
	}
	if q.Messages[0].ReceiptHandle != "rh-2" {
		t.Fatalf("wrong message removed: %+v", q.Messages[0])
	}
}

func TestDeleteMessageClearsInFlightBookkeeping(t *testing.T) {
	m := &dmMessage{ID: "m1", Body: "alpha", ReceiptHandle: "rh-1"}
	q := &dmQueue{
		Messages: []*dmMessage{m},
		inflight: map[string]*dmMessage{"rh-1": m},
	}
	if err := DeleteMessage(q, "rh-1"); err != nil {
		t.Fatalf("DeleteMessage: unexpected error %v", err)
	}
	if len(q.Messages) != 0 {
		t.Fatalf("message still queued: %+v", q.Messages)
	}
	if len(q.inflight) != 0 {
		t.Fatalf("in-flight entry kept: %+v", q.inflight)
	}
}

func TestDeleteMessageDropsReceiptKeyedEntryWrappers(t *testing.T) {
	q := &dmWrappedQueue{
		inflight: map[string]*dmReceipt{
			"rh-1": {Msg: &dmMessage{ID: "m1", Body: "alpha", ReceiptHandle: "rh-1"}},
		},
	}
	if err := DeleteMessage(q, "rh-1"); err != nil {
		t.Fatalf("DeleteMessage: unexpected error %v", err)
	}
	if len(q.inflight) != 0 {
		t.Fatalf("bookkeeping entry kept: %+v", q.inflight)
	}
}

func TestDeleteMessageIsIdempotentOnMisses(t *testing.T) {
	q := &dmQueue{
		Messages: []*dmMessage{
			{ID: "m1", Body: "alpha", ReceiptHandle: "rh-1"},
			{ID: "m2", Body: "beta", ReceiptHandle: "rh-2"},
		},
	}
	for i := 0; i < 2; i++ {
		if err := DeleteMessage(q, "rh-1"); err != nil {
			t.Fatalf("DeleteMessage: unexpected error %v", err)
		}
	}
	if err := DeleteMessage(q, "no-such-handle"); err != nil {
		t.Fatalf("unknown handle must succeed: %v", err)
	}
	if len(q.Messages) != 1 {
		t.Fatalf("wrong survivor count: %d", len(q.Messages))
	}
	if q.Messages[0].ReceiptHandle != "rh-2" {
		t.Fatalf("wrong survivor: %+v", q.Messages[0])
	}
}

func TestDeleteMessageBlankOrMissingHandleIsNoop(t *testing.T) {
	q := &dmQueue{
		Messages: []*dmMessage{{ID: "m1", Body: "alpha", ReceiptHandle: ""}},
	}
	if err := DeleteMessage(q, ""); err != nil {
		t.Fatalf("blank handle must succeed: %v", err)
	}
	if len(q.Messages) != 1 {
		t.Fatalf("blank handle deleted a message: %+v", q.Messages)
	}
	if err := DeleteMessage(nil, "rh-1"); err != nil {
		t.Fatalf("nil root must succeed: %v", err)
	}
}

func TestDeleteMessageReachesQueuesThroughRegistries(t *testing.T) {
	store := &dmStore{
		Queues: map[string]*dmQueue{
			"http://localhost:4576/000000000000/alpha": {
				Name:     "alpha",
				Messages: []*dmMessage{{ID: "m1", Body: "alpha", ReceiptHandle: "rh-1"}},
			},
			"http://localhost:4576/000000000000/beta": {Name: "beta"},
		},
	}
	if err := DeleteMessage(store, "rh-1"); err != nil {
		t.Fatalf("DeleteMessage: unexpected error %v", err)
	}
	if got := store.Queues["http://localhost:4576/000000000000/alpha"]; len(got.Messages) != 0 {
		t.Fatalf("message not removed through the registry: %+v", got.Messages)
	}
	if len(store.Queues) != 2 {
		t.Fatalf("queue registry must be left intact: %d", len(store.Queues))
	}
}

func TestDeleteMessageQueueReceiverIsIdempotent(t *testing.T) {
	var q Queue
	if err := q.DeleteMessage("rh-1"); err != nil {
		t.Fatalf("empty queue delete must succeed: %v", err)
	}
}

func TestDeleteMessageAbsorbsStoreAndQueueURLShape(t *testing.T) {
	store := &dmStore{
		Queues: map[string]*dmQueue{
			"http://localhost:4576/000000000000/alpha": {
				Name:     "alpha",
				Messages: []*dmMessage{{ID: "m1", Body: "alpha", ReceiptHandle: "rh-1"}},
			},
		},
	}
	if err := DeleteMessage(store, "http://localhost:4576/000000000000/alpha", "rh-1"); err != nil {
		t.Fatalf("DeleteMessage: unexpected error %v", err)
	}
	if got := store.Queues["http://localhost:4576/000000000000/alpha"]; len(got.Messages) != 0 {
		t.Fatalf("url-shaped argument was misused as a handle: %+v", got.Messages)
	}
	if len(store.Queues) != 1 {
		t.Fatalf("queue registry must be left intact: %d", len(store.Queues))
	}
}
