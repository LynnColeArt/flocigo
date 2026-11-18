// Package sqs implements the floci-go SQS service: the queue core plus the
// AWS query-protocol HTTP surface around it.
//
// ticket-022 (ReceiveMessage): ReceiveMessage pops visible messages in send
// order and hides each checkout for the visibility timeout behind a
// per-message time.AfterFunc timer that re-reveals it; long polling parks the
// caller on a per-Receive channel that Send and expiration timers signal.
package sqs

import (
    "bytes"
    "crypto/md5"
    "crypto/rand"
    "encoding/base64"
    "encoding/binary"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "net/http"
    "net/url"
    "sort"
    "strconv"
    "strings"
    "sync"
    "time"
)

// SQS protocol limits used when clamping request parameters.
const (
    MaxReceiveBatch             = 10
    MaxWaitTimeSeconds          = 20
    MaxVisibilityTimeoutSeconds = 43200
    MaxDelaySeconds             = 900

    // DefaultVisibilityTimeout is the AWS SQS default of 30 seconds.
    DefaultVisibilityTimeout = 30 * time.Second
)

const (
    maxVisibilityTimeout = MaxVisibilityTimeoutSeconds * time.Second
    maxWaitTime          = MaxWaitTimeSeconds * time.Second
    maxDelay             = MaxDelaySeconds * time.Second

    // sqsNamespace is the XML namespace SQS query-protocol responses carry.
    sqsNamespace = "http://queue.amazonaws.com/doc/2012-11-05/"
)

// SenderID is the stable principal id reported as SenderId.
const SenderID = "AIDAIT2YO5URIRW7OHVHIG"

// Errors the API layer maps onto SQS error codes.
var (
    ErrQueueNotFound          = fmt.Errorf("the specified queue does not exist")
    ErrMalformedReceiptHandle = fmt.Errorf("the input receipt handle is invalid")
)

// MessageAttributeValue is one decoded message attribute.
type MessageAttributeValue struct {
    DataType    string // String, Number, Binary, or a transport.custom variant
    StringValue string
    BinaryValue []byte
}

// Message is one queued message plus its delivery bookkeeping. The visibility
// fields are guarded by the owning queue's mutex.
type Message struct {
    MessageID              string
    Body                   string
    MD5OfBody              string
    MessageAttributes      map[string]MessageAttributeValue
    MD5OfMessageAttributes string

    SenderID                  string
    SentTimestamp             int64 // unix epoch milliseconds
    ApproximateReceiveCount   int
    ApproximateFirstReceiveAt int64 // unix epoch milliseconds

    // ReceiptHandle is the opaque handle minted for the newest checkout.
    ReceiptHandle string

    invisibleUntil time.Time
    receiveGen     int
    deleted        bool
    revealTimer    *time.Timer
}

// Delivery is the immutable snapshot Receive hands back, so response
// serialization cannot race a later checkout of the same message.
type Delivery struct {
    MessageID                 string
    ReceiptHandle             string
    Body                      string
    MD5OfBody                 string
    MessageAttributes         map[string]MessageAttributeValue
    MD5OfMessageAttributes    string
    SenderID                  string
    SentTimestamp             int64
    ApproximateReceiveCount   int
    ApproximateFirstReceiveAt int64
}

// Queue is one SQS queue: an ordered message list with per-message visibility
// bookkeeping plus a wait set for long polling. Messages mutate under mu.
type Queue struct {
    Name string

    mu                sync.Mutex
    messages          []*Message
    defaultVisibility time.Duration
    defaultDelay      time.Duration

    waitersMu sync.Mutex
    waiters   map[chan struct{}]struct{}
}

// Manager owns every queue served by the process.
type Manager struct {
    mu     sync.Mutex
    queues map[string]*Queue
}

// NewManager returns an empty manager.
func NewManager() *Manager {
    return &Manager{queues: make(map[string]*Queue)}
}

// GetQueue returns the named queue, creating it when absent (CreateQueue).
func (m *Manager) GetQueue(name string) *Queue {
    m.mu.Lock()
    defer m.mu.Unlock()
    if q, ok := m.queues[name]; ok {
        return q
    }
    q := &Queue{
        Name:              name,
        defaultVisibility: DefaultVisibilityTimeout,
        waiters:           make(map[chan struct{}]struct{}),
    }
    m.queues[name] = q
    return q
}

// LookupQueue returns an existing queue or ErrQueueNotFound.
func (m *Manager) LookupQueue(name string) (*Queue, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    q, ok := m.queues[name]
    if !ok {
        return nil, ErrQueueNotFound
    }
    return q, nil
}

// QueueFromURL resolves a QueueUrl (or a bare queue name) to its queue.
func (m *Manager) QueueFromURL(rawurl string) (*Queue, error) {
    name := rawurl
    if i := strings.LastIndex(rawurl, "/"); i >= 0 {
        name = rawurl[i+1:]
    }
    if name == "" {
        return nil, ErrQueueNotFound
    }
    return m.LookupQueue(name)
}

// VisibilityTimeout returns the queue default visibility timeout.
func (q *Queue) VisibilityTimeout() time.Duration {
    q.mu.Lock()
    defer q.mu.Unlock()
    return q.defaultVisibility
}

// SetVisibilityTimeout stores the queue default visibility timeout (clamped).
func (q *Queue) SetVisibilityTimeout(d time.Duration) {
    q.mu.Lock()
    q.defaultVisibility = clampDuration(d, 0, maxVisibilityTimeout)
    q.mu.Unlock()
}

// DelaySeconds returns the queue default delivery delay.
func (q *Queue) DelaySeconds() time.Duration {
    q.mu.Lock()
    defer q.mu.Unlock()
    return q.defaultDelay
}

// SetDelaySeconds stores the queue default delivery delay (clamped).
func (q *Queue) SetDelaySeconds(d time.Duration) {
    q.mu.Lock()
    q.defaultDelay = clampDuration(d, 0, maxDelay)
    q.mu.Unlock()
}

// Send appends a message in send order. A positive delay hides it until the
// delay timer fires, which also wakes long pollers. It is the write side of
// ReceiveMessage and exists so pop, timer and long-poll behavior round-trip.
func (q *Queue) Send(body string, attrs map[string]MessageAttributeValue, delay time.Duration) *Message {
    now := time.Now()
    m := &Message{
        MessageID:         newUUID(),
        Body:              body,
        MD5OfBody:         md5Hex(body),
        MessageAttributes: attrs,
        SenderID:          SenderID,
        SentTimestamp:     unixMilli(now),
    }
    if len(attrs) > 0 {
        m.MD5OfMessageAttributes = md5OfMessageAttributes(attrs)
    }
    q.mu.Lock()
    q.messages = append(q.messages, m)
    if delay > 0 {
        m.hideLocked(q, delay, now)
    }
    q.mu.Unlock()
    q.notifyWaiters()
    return m
}

// Receive pops up to maxSize visible messages in send order, skipping
// messages that are in flight (or delayed) or deleted. Every popped message
// is checked out: it gets a fresh ReceiptHandle and is hidden for the
// visibility timeout, after which a Go timer re-reveals it and wakes waiting
// receivers. When nothing is visible and wait is positive, Receive long
// polls: it registers a private wake channel and blocks on it until a Send or
// an expiry timer signals, or the deadline (capped at 20s) passes.
func (q *Queue) Receive(maxSize int, visibility, wait time.Duration) []*Delivery {
    maxSize = clampInt(maxSize, 1, MaxReceiveBatch)
    visibility = clampDuration(visibility, 0, maxVisibilityTimeout)
    wait = clampDuration(wait, 0, maxWaitTime)

    deadline := time.Now().Add(wait)
    for {
        q.mu.Lock()
        popped := q.takeVisibleLocked(maxSize, visibility, time.Now())
        q.mu.Unlock()
        if len(popped) > 0 {
            return popped
        }
        if !time.Now().Before(deadline) {
            return nil
        }

        wake := q.addWaiter()
        // Re-check after registering so a Send or reveal that raced the
        // registration cannot strand us on an already-signalled channel.
        q.mu.Lock()
        popped = q.takeVisibleLocked(maxSize, visibility, time.Now())
        q.mu.Unlock()
        if len(popped) > 0 {
            q.removeWaiter(wake)
            return popped
        }

        select {
        case <-wake:
            q.removeWaiter(wake)
        case <-time.After(time.Until(deadline)):
            q.removeWaiter(wake)
            q.mu.Lock()
            popped = q.takeVisibleLocked(maxSize, visibility, time.Now())
            q.mu.Unlock()
            return popped
        }
    }
}

// DeleteMessage removes the message addressed by receipt. As in SQS the
// delete is best effort: well-formed handles whose checkout is stale (or
// unknown) succeed idempotently, garbage handles yield
// ErrMalformedReceiptHandle.

// takeVisibleLocked pops up to maxSize visible messages, checks each out and
// returns immutable snapshots. The caller holds q.mu.
func (q *Queue) takeVisibleLocked(maxSize int, visibility time.Duration, now time.Time) []*Delivery {
    q.compactLocked()
    popped := make([]*Delivery, 0, maxSize)
    for _, m := range q.messages {
        if len(popped) == maxSize {
            break
        }
        if now.Before(m.invisibleUntil) {
            continue // in flight or delayed: standard queues skip it
        }
        q.checkoutLocked(m, visibility, now)
        popped = append(popped, m.snapshot())
    }
    return popped
}

// checkoutLocked hands a message to a consumer: bump receive bookkeeping,
// mint a receipt handle, hide the message and arm its reveal timer.
func (q *Queue) checkoutLocked(m *Message, visibility time.Duration, now time.Time) {
    m.ApproximateReceiveCount++
    if m.ApproximateFirstReceiveAt == 0 {
        m.ApproximateFirstReceiveAt = unixMilli(now)
    }
    m.hideLocked(q, visibility, now)
    m.ReceiptHandle = newReceiptHandle(q, m)
}

// hideLocked marks the message invisible until now+d and arms the Go timer
// that re-reveals it at that instant. The caller holds q.mu.
func (m *Message) hideLocked(q *Queue, d time.Duration, now time.Time) {
    m.receiveGen++
    if m.revealTimer != nil {
        m.revealTimer.Stop()
    }
    at := now.Add(d)
    gen := m.receiveGen
    m.invisibleUntil = at
    m.revealTimer = time.AfterFunc(d, func() { q.reveal(m, at, gen) })
}

// reveal runs on the visibility timer: unless the message was deleted or
// rescheduled since, it becomes visible again and long pollers are woken.
func (q *Queue) reveal(m *Message, at time.Time, gen int) {
    q.mu.Lock()
    if m.deleted || m.receiveGen != gen || !m.invisibleUntil.Equal(at) {
        q.mu.Unlock()
        return
    }
    m.invisibleUntil = time.Time{}
    q.mu.Unlock()
    q.notifyWaiters()
}

// compactLocked drops deleted messages, preserving send order. It runs under
// q.mu.
func (q *Queue) compactLocked() {
    kept := q.messages[:0]
    for _, m := range q.messages {
        if !m.deleted {
            kept = append(kept, m)
        }
    }
    q.messages = kept
}

// snapshot builds the immutable wire view of the message. It runs under q.mu.
func (m *Message) snapshot() *Delivery {
    attrs := make(map[string]MessageAttributeValue, len(m.MessageAttributes))
    for k, v := range m.MessageAttributes {
        attrs[k] = v
    }
    return &Delivery{
        MessageID:                 m.MessageID,
        ReceiptHandle:             m.ReceiptHandle,
        Body:                      m.Body,
        MD5OfBody:                 m.MD5OfBody,
        MessageAttributes:         attrs,
        MD5OfMessageAttributes:    m.MD5OfMessageAttributes,
        SenderID:                  m.SenderID,
        SentTimestamp:             m.SentTimestamp,
        ApproximateReceiveCount:   m.ApproximateReceiveCount,
        ApproximateFirstReceiveAt: m.ApproximateFirstReceiveAt,
    }
}

// addWaiter registers and returns a fresh wake channel for one Receive call.
func (q *Queue) addWaiter() chan struct{} {
    wake := make(chan struct{}, 1)
    q.waitersMu.Lock()
    q.waiters[wake] = struct{}{}
    q.waitersMu.Unlock()
    return wake
}

// removeWaiter unregisters a wake channel (no-op when already gone).
func (q *Queue) removeWaiter(wake chan struct{}) {
    q.waitersMu.Lock()
    delete(q.waiters, wake)
    q.waitersMu.Unlock()
}

// notifyWaiters wakes every parked Receive without blocking.
func (q *Queue) notifyWaiters() {
    q.waitersMu.Lock()
    for wake := range q.waiters {
        select {
        case wake <- struct{}{}:
        default:
        }
    }
    q.waitersMu.Unlock()
}

// receiptHandleData is the payload encoded inside every receipt handle. The
// receive generation lets stale reveal timers be ignored; the nonce keeps
// handles unique per checkout.
type receiptHandleData struct {
    Version    int    `json:"v"`
    Queue      string `json:"q"`
    MessageID  string `json:"m"`
    ReceiveGen int    `json:"g"`
    ExpiresAt  int64  `json:"e"` // unix milliseconds
    Nonce      []byte `json:"n"`
}

// newReceiptHandle mints the opaque handle handed back with each checkout. It
// runs under q.mu, so receiveGen and invisibleUntil are stable.
func newReceiptHandle(q *Queue, m *Message) string {
    nonce := make([]byte, 8)
    rand.Read(nonce)
    raw, err := json.Marshal(receiptHandleData{
        Version:    1,
        Queue:      q.Name,
        MessageID:  m.MessageID,
        ReceiveGen: m.receiveGen,
        ExpiresAt:  unixMilli(m.invisibleUntil),
        Nonce:      nonce,
    })
    if err != nil {
        return "" // unreachable: every field encodes to JSON
    }
    return base64.RawStdEncoding.EncodeToString(raw)
}

// decodeReceiptHandle parses a handle produced by newReceiptHandle.
func decodeReceiptHandle(receipt string) (receiptHandleData, bool) {
    raw, err := base64.RawStdEncoding.DecodeString(receipt)
    if err != nil {
        return receiptHandleData{}, false
    }
    var data receiptHandleData
    if json.Unmarshal(raw, &data) != nil || data.Version != 1 || data.MessageID == "" {
        return receiptHandleData{}, false
    }
    return data, true
}

func newUUID() string {
    var b [16]byte
    rand.Read(b[:])
    b[6] = (b[6] & 0x0f) | 0x40
    b[8] = (b[8] & 0x3f) | 0x80
    return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func md5Hex(s string) string {
    sum := md5.Sum([]byte(s))
    return hex.EncodeToString(sum[:])
}

func unixMilli(t time.Time) int64 {
    return t.UnixNano() / int64(time.Millisecond)
}

func clampInt(v, lo, hi int) int {
    if v < lo {
        return lo
    }
    if v > hi {
        return hi
    }
    return v
}

func clampDuration(d, lo, hi time.Duration) time.Duration {
    if d < lo {
        return lo
    }
    if d > hi {
        return hi
    }
    return d
}

// sortedAttrNames returns attribute names in lexical order, the order AWS
// hashes message attributes in.
func sortedAttrNames(attrs map[string]MessageAttributeValue) []string {
    names := make([]string, 0, len(attrs))
    for name := range attrs {
        names = append(names, name)
    }
    sort.Strings(names)
    return names
}

// md5OfMessageAttributes implements the AWS message-attribute digest: the
// 4-byte big-endian length-prefixed UTF-8 name, transport type (the label
// before the first dot) and value, concatenated in name order.
func md5OfMessageAttributes(attrs map[string]MessageAttributeValue) string {
    buf := new(bytes.Buffer)
    for _, name := range sortedAttrNames(attrs) {
        attr := attrs[name]
        writeLenPrefixed(buf, name)
        transport := dataTypeTransport(attr.DataType)
        writeLenPrefixed(buf, transport)
        if transport == "Binary" {
            writeLenPrefixed(buf, string(attr.BinaryValue))
        } else { // String, Number and custom variants hash the text value
            writeLenPrefixed(buf, attr.StringValue)
        }
    }
    sum := md5.Sum(buf.Bytes())
    return hex.EncodeToString(sum[:])
}

// dataTypeTransport reduces a declared data type to its transport class, e.g.
// "Number.float" and "Number.custom.x" hash as "Number".
func dataTypeTransport(dataType string) string {
    if i := strings.IndexByte(dataType, '.'); i >= 0 {
        return dataType[:i]
    }
    return dataType
}

func writeLenPrefixed(buf *bytes.Buffer, s string) {
    var header [4]byte
    binary.BigEndian.PutUint32(header[:], uint32(len(s)))
    buf.Write(header[:])
    buf.WriteString(s)
}

// sqsNamespace is re-declared above with the other constants.

// Server serves the SQS query-protocol HTTP API.
type Server struct {
    // Manager owns the queues served by this server.
    Manager *Manager
}

// NewServer wires a Server to a Manager.
func NewServer(manager *Manager) *Server {
    return &Server{Manager: manager}
}

// ServeHTTP dispatches on the Action form parameter.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    if err := r.ParseForm(); err != nil {
        s.error(w, "InvalidParameterValue", "malformed request: "+err.Error())
        return
    }
    switch action := r.FormValue("Action"); action {
    case "CreateQueue":
        s.createQueue(w, r)
    case "SendMessage":
        s.sendMessage(w, r)
    case "ReceiveMessage":
        s.receiveMessage(w, r)
    case "DeleteMessage":
        s.deleteMessage(w, r)
    case "":
        s.error(w, "MissingAction", "The request is missing the parameter Action.")
    default:
        s.error(w, "InvalidAction", "The action "+action+" is not valid for this web service.")
    }
}

// createQueue creates or returns the queue, honouring VisibilityTimeout and
// DelaySeconds attributes; it exists so ReceiveMessage has queues to poll.
func (s *Server) createQueue(w http.ResponseWriter, r *http.Request) {
    name := r.FormValue("QueueName")
    if name == "" {
        s.missingParameter(w, "QueueName")
        return
    }
    queue := s.Manager.GetQueue(name)
    for _, attr := range namedFormList(r.Form, "Attribute") {
        switch attr.Name {
        case "VisibilityTimeout":
            if n, err := strconv.Atoi(attr.Value); err == nil {
                queue.SetVisibilityTimeout(time.Duration(n) * time.Second)
            }
        case "DelaySeconds":
            if n, err := strconv.Atoi(attr.Value); err == nil {
                queue.SetDelaySeconds(time.Duration(n) * time.Second)
            }
        }
    }
    result := fmt.Sprintf(`<CreateQueueResult><QueueUrl>%s</QueueUrl></CreateQueueResult>`, escapeXML(s.queueURL(r, name)))
    respondXML(w, http.StatusOK, responseEnvelope("CreateQueue", result))
}

// sendMessage enqueues MessageBody with optional DelaySeconds (per-send or
// queue default) and MessageAttribute.N.* parameters.
func (s *Server) sendMessage(w http.ResponseWriter, r *http.Request) {
    queue := s.queueFor(w, r)
    if queue == nil {
        return
    }
    body := r.FormValue("MessageBody")
    if body == "" {
        s.missingParameter(w, "MessageBody")
        return
    }
    delay := queue.DelaySeconds()
    if _, ok := r.Form["DelaySeconds"]; ok {
        delay = time.Duration(clampInt(intForm(r, "DelaySeconds", 0), 0, MaxDelaySeconds)) * time.Second
    }
    message := queue.Send(body, parseMessageAttributes(r.Form), delay)
    result := fmt.Sprintf(`<SendMessageResult><MD5OfMessageBody>%s</MD5OfMessageBody>`, message.MD5OfBody)
    if message.MD5OfMessageAttributes != "" {
        result += fmt.Sprintf(`<MD5OfMessageAttributes>%s</MD5OfMessageAttributes>`, message.MD5OfMessageAttributes)
    }
    result += fmt.Sprintf(`<MessageId>%s</MessageId></SendMessageResult>`, message.MessageID)
    respondXML(w, http.StatusOK, responseEnvelope("SendMessage", result))
}

// receiveMessage pops up to MaxNumberOfMessages messages, blocking up to
// WaitTimeSeconds on the queue wake channels (long poll) and using the
// per-request VisibilityTimeout override when supplied.
func (s *Server) receiveMessage(w http.ResponseWriter, r *http.Request) {
    queue := s.queueFor(w, r)
    if queue == nil {
        return
    }
    maxMessages := clampInt(intForm(r, "MaxNumberOfMessages", 1), 1, MaxReceiveBatch)
    visibility := queue.VisibilityTimeout()
    if _, ok := r.Form["VisibilityTimeout"]; ok {
        visibility = time.Duration(clampInt(intForm(r, "VisibilityTimeout", 0), 0, MaxVisibilityTimeoutSeconds)) * time.Second
    }
    wait := time.Duration(clampInt(intForm(r, "WaitTimeSeconds", 0), 0, MaxWaitTimeSeconds)) * time.Second

    deliveries := queue.Receive(maxMessages, visibility, wait)

    systemFilter := nameFormSet(r.Form, "AttributeName")
    messageFilter := nameFormSet(r.Form, "MessageAttributeName")
    resultXML := "<ReceiveMessageResult/>"
    if len(deliveries) > 0 {
        var inner strings.Builder
        inner.WriteString("<ReceiveMessageResult>")
        for _, d := range deliveries {
            inner.WriteString(deliveryXML(d, systemFilter, messageFilter))
        }
        inner.WriteString("</ReceiveMessageResult>")
        resultXML = inner.String()
    }
    respondXML(w, http.StatusOK, responseEnvelope("ReceiveMessage", resultXML))
}

// deleteMessage consumes the message addressed by ReceiptHandle; deletes on
// stale or unknown well-formed handles succeed idempotently, as in SQS.
func (s *Server) deleteMessage(w http.ResponseWriter, r *http.Request) {
    queue := s.queueFor(w, r)
    if queue == nil {
        return
    }
    receipt := r.FormValue("ReceiptHandle")
    if receipt == "" {
        s.missingParameter(w, "ReceiptHandle")
        return
    }
    if err := queue.DeleteMessage(receipt); err != nil {
        s.error(w, "ReceiptHandleIsInvalid", "The input receipt handle is invalid.")
        return
    }
    respondXML(w, http.StatusOK, responseEnvelope("DeleteMessage", ""))
}

// deliveryXML renders one <Message> element: identifiers, MD5s and body, then
// system attributes in lexical order, then the requested message attributes.
func deliveryXML(d *Delivery, systemFilter, messageFilter map[string]bool) string {
    var out strings.Builder
    out.WriteString("<Message>")
    out.WriteString(fmt.Sprintf(`<MessageId>%s</MessageId>`, d.MessageID))
    out.WriteString(fmt.Sprintf(`<ReceiptHandle>%s</ReceiptHandle>`, escapeXML(d.ReceiptHandle)))
    out.WriteString(fmt.Sprintf(`<MD5OfBody>%s</MD5OfBody>`, d.MD5OfBody))
    out.WriteString(fmt.Sprintf(`<Body>%s</Body>`, escapeXML(d.Body)))
    for _, name := range deliverySystemAttributes(d, systemFilter) {
        out.WriteString(fmt.Sprintf(`<Attribute><Name>%s</Name><Value>%s</Value></Attribute>`, name, systemAttributeValue(d, name)))
    }
    attrs := filterMessageAttributes(d.MessageAttributes, messageFilter)
    if len(attrs) > 0 {
        out.WriteString(fmt.Sprintf(`<MD5OfMessageAttributes>%s</MD5OfMessageAttributes>`, md5OfMessageAttributes(attrs)))
        for _, name := range sortedAttrNames(attrs) {
            attr := attrs[name]
            valueXML := fmt.Sprintf(`<StringValue>%s</StringValue>`, escapeXML(attr.StringValue))
            if dataTypeTransport(attr.DataType) == "Binary" {
                valueXML = fmt.Sprintf(`<BinaryValue>%s</BinaryValue>`, base64.StdEncoding.EncodeToString(attr.BinaryValue))
            }
            out.WriteString(fmt.Sprintf(`<MessageAttribute><Name>%s</Name><Value>%s<DataType>%s</DataType></Value></MessageAttribute>`,
                escapeXML(name), valueXML, escapeXML(attr.DataType)))
        }
    }
    out.WriteString("</Message>")
    return out.String()
}

// deliverySystemAttributes lists the system attributes to report in lexical
// order, honouring the AttributeName filter (All or empty filter returns all).
func deliverySystemAttributes(d *Delivery, filter map[string]bool) []string {
    values := map[string]string{
        "SenderId":                         d.SenderID,
        "SentTimestamp":                    strconv.FormatInt(d.SentTimestamp, 10),
        "ApproximateReceiveCount":          strconv.Itoa(d.ApproximateReceiveCount),
        "ApproximateFirstReceiveTimestamp": strconv.FormatInt(d.ApproximateFirstReceiveAt, 10),
    }
    names := make([]string, 0, len(values))
    for name := range values {
        names = append(names, name)
    }
    sort.Strings(names)
    if len(filter) == 0 || filter["All"] {
        return names
    }
    var wanted []string
    for _, name := range names {
        if filter[name] {
            wanted = append(wanted, name)
        }
    }
    return wanted
}

func systemAttributeValue(d *Delivery, name string) string {
    switch name {
    case "SenderId":
        return d.SenderID
    case "SentTimestamp":
        return strconv.FormatInt(d.SentTimestamp, 10)
    case "ApproximateReceiveCount":
        return strconv.Itoa(d.ApproximateReceiveCount)
    case "ApproximateFirstReceiveTimestamp":
        return strconv.FormatInt(d.ApproximateFirstReceiveAt, 10)
    }
    return ""
}

// filterMessageAttributes picks the requested subset (All or empty filter
// returns everything); unknown names are silently ignored, as in SQS.
func filterMessageAttributes(attrs map[string]MessageAttributeValue, filter map[string]bool) map[string]MessageAttributeValue {
    if len(attrs) == 0 {
        return nil
    }
    if len(filter) == 0 || filter["All"] {
        return attrs
    }
    var picked map[string]MessageAttributeValue
    for _, name := range sortedAttrNames(attrs) {
        if filter[name] {
            if picked == nil {
                picked = make(map[string]MessageAttributeValue)
            }
            picked[name] = attrs[name]
        }
    }
    return picked
}

// queueFor resolves QueueUrl, writing the NonExistentQueue error on miss.
func (s *Server) queueFor(w http.ResponseWriter, r *http.Request) *Queue {
    queue, err := s.Manager.QueueFromURL(r.FormValue("QueueUrl"))
    if err != nil {
        s.error(w, "AWS.SimpleQueueService.NonExistentQueue",
            "The specified queue does not exist for this wsdl version.")
        return nil
    }
    return queue
}

// queueURL builds the canonical queue URL for a queue name.
func (s *Server) queueURL(r *http.Request, name string) string {
    host := r.Host
    if host == "" {
        host = "localhost"
    }
    return "http://" + host + "/000000000000/" + name
}

// intForm parses an integer form parameter, falling back to def.
func intForm(r *http.Request, key string, def int) int {
    if v := r.FormValue(key); v != "" {
        if n, err := strconv.Atoi(v); err == nil {
            return n
        }
    }
    return def
}

// namedFormList reads prefix.N.Name / prefix.N.Value pairs (queue attributes).
func namedFormList(values url.Values, prefix string) []struct{ Name, Value string } {
    var out []struct{ Name, Value string }
    for i := 1; ; i++ {
        base := fmt.Sprintf("%s.%d", prefix, i)
        name := values.Get(base + ".Name")
        if name == "" {
            break
        }
        out = append(out, struct{ Name, Value string }{name, values.Get(base + ".Value")})
    }
    return out
}

// nameFormSet reads a numbered list parameter (prefix.1, prefix.2, ...).
func nameFormSet(values url.Values, prefix string) map[string]bool {
    set := make(map[string]bool)
    for i := 1; ; i++ {
        v := values.Get(fmt.Sprintf("%s.%d", prefix, i))
        if v == "" {
            break
        }
        set[v] = true
    }
    return set
}

// parseMessageAttributes decodes the MessageAttribute.N.* form parameters.
func parseMessageAttributes(values url.Values) map[string]MessageAttributeValue {
    indices := map[int]bool{}
    for key := range values {
        if !strings.HasPrefix(key, "MessageAttribute.") {
            continue
        }
        rest := strings.TrimPrefix(key, "MessageAttribute.")
        if dot := strings.IndexByte(rest, '.'); dot >= 0 {
            rest = rest[:dot]
        }
        if n, err := strconv.Atoi(rest); err == nil {
            indices[n] = true
        }
    }
    if len(indices) == 0 {
        return nil
    }
    attrs := make(map[string]MessageAttributeValue, len(indices))
    for n := range indices {
        base := fmt.Sprintf("MessageAttribute.%d", n)
        name := values.Get(base + ".Name")
        if name == "" {
            continue
        }
        dataType := values.Get(base + ".Value.DataType")
        if dataType == "" {
            dataType = "String"
        }
        attr := MessageAttributeValue{DataType: dataType}
        if raw, ok := values[base+".Value.StringValue"]; ok && len(raw) > 0 {
            attr.StringValue = raw[0]
        }
        if encoded := values.Get(base + ".Value.BinaryValue"); encoded != "" {
            if blob, err := base64.StdEncoding.DecodeString(encoded); err == nil {
                attr.BinaryValue = blob
            }
        }
        attrs[name] = attr
    }
    if len(attrs) == 0 {
        return nil
    }
    return attrs
}

// escapeXML escapes the predefined XML entities without touching whitespace
// bytes, mirroring SQS escaping of element text.
func escapeXML(s string) string {
    return strings.NewReplacer(
        "&", "&amp;",
        "<", "&lt;",
        ">", "&gt;",
        `"`, "&quot;",
        "'", "&apos;",
    ).Replace(s)
}

// responseEnvelope wraps result in the standard response document with prolog.
func responseEnvelope(action, result string) string {
    return fmt.Sprintf(`<?xml version="1.0"?><%sResponse xmlns="%s">%s<ResponseMetadata><RequestId>%s</RequestId></ResponseMetadata></%sResponse>`,
        action, sqsNamespace, result, newUUID(), action)
}

// missingParameter writes the SQS MissingParameter sender error.
func (s *Server) missingParameter(w http.ResponseWriter, parameter string) {
    s.error(w, "MissingParameter", "The request must contain the parameter "+parameter+".")
}

// error writes an SQS query-protocol error document.
func (s *Server) error(w http.ResponseWriter, code, message string) {
    respondXML(w, http.StatusBadRequest, fmt.Sprintf(
        `<ErrorResponse xmlns="%s"><Error><Type>Sender</Type><Code>%s</Code><Message>%s</Message><Detail/></Error><ResponseMetadata><RequestId>%s</RequestId></ResponseMetadata></ErrorResponse>`,
        sqsNamespace, code, escapeXML(message), newUUID()))
}

// respondXML writes an XML document with the SQS text/xml content type.
func respondXML(w http.ResponseWriter, status int, body string) {
    w.Header().Set("Content-Type", "text/xml")
    w.WriteHeader(status)
    _, _ = w.Write([]byte(body))
}
