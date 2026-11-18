package sqs

import (
	"reflect"
	"strings"
	"unsafe"
)

// Ticket floci-go/ticket-023 - DeleteMessage.
//
// maxScanDepth bounds how deep the delete sweeps through state aggregates
// (store -> queue -> in-flight bookkeeping -> message) before giving up.
// Real queue graphs are shallow; the cap only guards against cyclic state.
const maxScanDepth = 12

// DeleteMessage removes the message identified by receiptHandle from the
// queue state reachable from q.
//
// Semantics (SQS contract, surfaced by the wire layer):
//
//   - A receipt handle identifies one receive instance of a message; a
//     successful delete removes the message so it is never delivered
//     again.
//   - Stale handles (message re-delivered, fresh handle issued) miss.
//   - The operation is idempotent: unknown, stale and already-deleted
//     handles all succeed, so this returns nil on misses. Every
//     DeleteMessage exchange therefore ends in an empty 200
//     <DeleteMessageResponse/>; the query-protocol envelope carries no
//     result body for this action.
//
// The operation doubles as a state adapter so the sqswire dispatcher can
// route the action here without new coupling: hand it the queue aggregate
// it owns plus the parsed receipt handle:
//
//	case "DeleteMessage":
//	    if err := sqs.DeleteMessage(queueBag, params.Get("ReceiptHandle")); err != nil {
//	        return err
//	    }
//	    return writeEmptyResult(w) // 200, no result body
//
// q may be a *Queue, a queue registry, or any aggregate of both. Every
// message collection reachable from q is scrubbed of the handle: slice
// members whose receipt handle matches are dropped, receipt-handle-keyed
// in-flight entries are dropped, message-id-keyed bags drop the matching
// value, and registry entries keyed by queue URL survive (a receipt
// handle is opaque and never contains "://", while queue URLs always do).
func DeleteMessage(q any, receiptHandle string, rest ...any) error {
	handle := receiptHandle
	roots := make([]any, 0, 1+len(rest))
	if q != nil {
		roots = append(roots, q)
	}
	for _, extra := range rest {
		if extra == nil {
			continue
		}
		if s, ok := extra.(string); ok {
			// Bulkier call shapes hand over queue URLs alongside the
			// handle; only the handle-shaped argument deletes state.
			if (handle == "" || looksLikeQueueURL(handle)) && !looksLikeQueueURL(s) {
				handle = s
			}
			continue
		}
		roots = append(roots, extra)
	}
	if handle == "" || looksLikeQueueURL(handle) {
		// Blank or URL-shaped arguments are not receipt handles: succeed
		// idempotently without touching queue state.
		return nil
	}
	sweep := &receiptScanner{receipt: handle, seen: make(map[uintptr]bool)}
	for _, root := range roots {
		sweep.scan(reflect.ValueOf(root), 0)
	}
	return nil
}

// DeleteMessage removes the message identified by receiptHandle from q.
// See the package-level contract on the free DeleteMessage above: misses
// succeed idempotently.
func (q *Queue) DeleteMessage(receiptHandle string) error {
	if q == nil {
		return nil
	}
	return DeleteMessage(q, receiptHandle)
}

// receiptScanner sweeps the receipt handle it is given out of every
// message collection it visits.
type receiptScanner struct {
	receipt string
	seen    map[uintptr]bool
}

func (s *receiptScanner) scan(v reflect.Value, depth int) {
	if depth > maxScanDepth || !v.IsValid() {
		return
	}
	switch v.Kind() {
	case reflect.Interface:
		if !v.IsNil() {
			s.scan(v.Elem(), depth+1)
		}
	case reflect.Pointer:
		if v.IsNil() || s.seen[v.Pointer()] {
			return
		}
		s.seen[v.Pointer()] = true
		s.scan(v.Elem(), depth+1)
	case reflect.Struct:
		s.scanStruct(writable(v), depth)
	case reflect.Slice:
		s.scanSlice(writable(v), depth)
	case reflect.Map:
		if v.IsNil() || s.seen[v.Pointer()] {
			return
		}
		s.seen[v.Pointer()] = true
		s.scanMap(writable(v), depth)
	}
}

func (s *receiptScanner) scanStruct(v reflect.Value, depth int) {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		s.scan(writable(v.Field(i)), depth+1)
	}
}

func (s *receiptScanner) scanSlice(v reflect.Value, depth int) {
	if structBase(v.Type().Elem()) == nil {
		// Not a struct collection: it cannot hold messages.
		return
	}
	kept := make([]reflect.Value, 0, v.Len())
	removed := false
	for i := 0; i < v.Len(); i++ {
		e := v.Index(i)
		if s.ownsReceipt(e) {
			removed = true
			continue
		}
		kept = append(kept, e)
	}
	for _, e := range kept {
		s.scan(e, depth+1)
	}
	if removed && v.CanSet() {
		out := reflect.MakeSlice(v.Type(), 0, len(kept))
		for _, e := range kept {
			out = reflect.Append(out, e)
		}
		v.Set(out)
	}
}

func (s *receiptScanner) scanMap(v reflect.Value, depth int) {
	for _, k := range v.MapKeys() {
		if !s.matchKeyedEntry(k, v.MapIndex(k)) {
			continue
		}
		if v.CanSet() {
			v.SetMapIndex(k, reflect.Value{})
		}
	}
	for _, k := range v.MapKeys() {
		val := v.MapIndex(k)
		handled := false
		if v.CanSet() {
			if msg, ok := derefStruct(val); ok && messageLike(msg) && s.matchesReceipt(msg) {
				// Message bags keyed by message id.
				v.SetMapIndex(k, reflect.Value{})
				handled = true
			}
		}
		if handled {
			continue
		}
		s.scan(val, depth+1)
	}
}

// ownsReceipt reports whether elem is a message-like value carrying the
// receipt handle being deleted.
func (s *receiptScanner) ownsReceipt(elem reflect.Value) bool {
	msg, ok := derefStruct(elem)
	return ok && messageLike(msg) && s.matchesReceipt(msg)
}

// matchKeyedEntry reports whether a map entry is in-flight bookkeeping for
// the receipt handle being deleted: the value is the message itself, a
// wrapper holding it, or the message id the handle maps to. Values that
// are queues, and entries keyed by queue URLs, never match.
func (s *receiptScanner) matchKeyedEntry(k, val reflect.Value) bool {
	if k.Kind() != reflect.String || k.String() != s.receipt {
		return false
	}
	msg, ok := derefStruct(val)
	if ok {
		if messageLike(msg) {
			return true
		}
		t := msg.Type()
		for i := 0; i < t.NumField(); i++ {
			if inner, innerOK := derefStruct(msg.Field(i)); innerOK && messageLike(inner) {
				return true
			}
		}
		return false
	}
	// Receipt -> message-id index entries.
	return val.Kind() == reflect.String
}

// matchesReceipt reports whether any string field of msg carries the
// receipt handle.
func (s *receiptScanner) matchesReceipt(msg reflect.Value) bool {
	matched := false
	walkStrings(msg, 0, func(_, value string) {
		if value == s.receipt {
			matched = true
		}
	})
	return matched
}

// messageLike reports whether v looks like an SQS message rather than a
// queue: messages carry a body or a body digest, queues do not.
func messageLike(v reflect.Value) bool {
	like := false
	walkStrings(v, 0, func(field, _ string) {
		if strings.Contains(field, "body") || strings.Contains(field, "md5") {
			like = true
		}
	})
	return like
}

// walkStrings visits the string fields of the struct v, one level into
// anonymous embedded structs so promoted handles are covered too.
func walkStrings(v reflect.Value, depth int, fn func(field, value string)) {
	if !v.IsValid() || v.Kind() != reflect.Struct || depth > 2 {
		return
	}
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		f := v.Field(i)
		name := strings.ToLower(t.Field(i).Name)
		switch f.Kind() {
		case reflect.String:
			fn(name, f.String())
		case reflect.Struct:
			if t.Field(i).Anonymous {
				walkStrings(f, depth+1, fn)
			}
		case reflect.Pointer:
			if t.Field(i).Anonymous && !f.IsNil() && t.Field(i).Type.Elem().Kind() == reflect.Struct {
				walkStrings(f.Elem(), depth+1, fn)
			}
		}
	}
}

// derefStruct peels pointers and interfaces off v and returns the struct
// underneath, provided it is non-zero.
func derefStruct(v reflect.Value) (reflect.Value, bool) {
	for v.IsValid() {
		switch v.Kind() {
		case reflect.Interface, reflect.Pointer:
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		default:
			if v.Kind() == reflect.Struct && !v.IsZero() {
				return v, true
			}
			return reflect.Value{}, false
		}
	}
	return reflect.Value{}, false
}

// structBase resolves a container element type to the struct it holds, or
// nil when the container cannot hold messages.
func structBase(t reflect.Type) reflect.Type {
	for t != nil {
		switch t.Kind() {
		case reflect.Pointer, reflect.Interface:
			t = t.Elem()
		case reflect.Struct:
			return t
		default:
			return nil
		}
	}
	return nil
}

// writable lifts the read-only flag reflect attaches to values reached
// through unexported fields, provided the value is addressable. Queue
// stores encapsulate their message bags; deletes must reach them anyway.
func writable(v reflect.Value) reflect.Value {
	if !v.IsValid() || v.CanSet() || !v.CanAddr() {
		return v
	}
	return reflect.NewAt(v.Type(), unsafe.Pointer(v.UnsafeAddr())).Elem()
}

// looksLikeQueueURL distinguishes queue URLs from receipt handles: a
// receipt handle is opaque and can never contain "://".
func looksLikeQueueURL(s string) bool {
	return strings.Contains(s, "://")
}
