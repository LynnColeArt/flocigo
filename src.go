package main

// floci-go / ticket-024: GetQueueAttributes -- queue depth + visibility stats.
//
// Exposes the SQS query-protocol over HTTP and answers GetQueueAttributes
// with live in-memory counters:
//
//   - ApproximateNumberOfMessages: messages available for retrieval.
//   - ApproximateNumberOfMessagesNotVisible: in flight, i.e. received but
//     still inside their visibility window.
//   - ApproximateNumberOfMessagesDelayed: delayed and not yet receivable.
//
// plus the remaining standard attributes (QueueArn, VisibilityTimeout,
// DelaySeconds, MaximumMessageSize, MessageRetentionPeriod,
// ReceiveMessageWaitTimeSeconds, CreatedTimestamp, LastModifiedTimestamp,
// redrive policies when configured, FIFO family for .fifo queues). The
// companion actions required to drive the counters (CreateQueue,
// SendMessage, ReceiveMessage, DeleteMessage, SetQueueAttributes,
// DeleteQueue, PurgeQueue, ListQueues) are implemented as well.

import (
    "crypto/md5"
    "crypto/rand"
    "encoding/base64"
    "encoding/hex"
    "encoding/xml"
    "errors"
    "fmt"
    "log"
    "net/http"
    "net/url"
    "os"
    "sort"
    "strconv"
    "strings"
    "sync"
    "time"
)

const (
    defaultRegion  = "us-east-1"
    defaultAccount = "000000000000"

    AttrAll                                   = "All"
    AttrApproximateNumberOfMessages           = "ApproximateNumberOfMessages"
    AttrApproximateNumberOfMessagesDelayed    = "ApproximateNumberOfMessagesDelayed"
    AttrApproximateNumberOfMessagesNotVisible = "ApproximateNumberOfMessagesNotVisible"
    AttrContentBasedDeduplication             = "ContentBasedDeduplication"
    AttrCreatedTimestamp                      = "CreatedTimestamp"
    AttrDelaySeconds                          = "DelaySeconds"
    AttrDeduplicationScope                    = "DeduplicationScope"
    AttrFifoQueue                             = "FifoQueue"
    AttrFifoThroughputLimit                   = "FifoThroughputLimit"
    AttrLastModifiedTimestamp                 = "LastModifiedTimestamp"
    AttrMaximumMessageSize                    = "MaximumMessageSize"
    AttrMessageRetentionPeriod                = "MessageRetentionPeriod"
    AttrQueueArn                              = "QueueArn"
    AttrReceiveMessageWaitTimeSeconds         = "ReceiveMessageWaitTimeSeconds"
    AttrRedriveAllowPolicy                    = "RedriveAllowPolicy"
    AttrRedrivePolicy                         = "RedrivePolicy"
    AttrSqsManagedSseEnabled                  = "SqsManagedSseEnabled"
    AttrVisibilityTimeout                     = "VisibilityTimeout"

    errCodeNonExistentQueue = "AWS.SimpleQueueService.NonExistentQueue"
    errMsgNonExistentQueue  = "The specified queue does not exist for this wsdl version."
    errCodeInvalidAttr      = "InvalidAttributeName"
    errCodeMissingParam     = "MissingParameter"
    errCodeInvalidParam     = "InvalidParameterValue"

    defaultVisibilityTimeout  = 30
    defaultDelaySeconds       = 0
    defaultMaxMessageSize     = 262144
    defaultRetentionPeriod    = 345600
    defaultReceiveWaitSeconds = 0
)

var knownQueueAttributes = map[string]bool{
    AttrAll: true,
    AttrApproximateNumberOfMessages:           true,
    AttrApproximateNumberOfMessagesDelayed:    true,
    AttrApproximateNumberOfMessagesNotVisible: true,
    AttrContentBasedDeduplication:             true,
    AttrCreatedTimestamp:                      true,
    AttrDelaySeconds:                          true,
    AttrDeduplicationScope:                    true,
    AttrFifoQueue:                             true,
    AttrFifoThroughputLimit:                   true,
    AttrLastModifiedTimestamp:                 true,
    AttrMaximumMessageSize:                    true,
    AttrMessageRetentionPeriod:                true,
    AttrQueueArn:                              true,
    AttrReceiveMessageWaitTimeSeconds:         true,
    AttrRedriveAllowPolicy:                    true,
    AttrRedrivePolicy:                         true,
    AttrSqsManagedSseEnabled:                  true,
    AttrVisibilityTimeout:                     true,
}

var (
    ErrQueueDoesNotExist    = errors.New(errCodeNonExistentQueue)
    ErrInvalidAttributeName = errors.New(errCodeInvalidAttr)
)

// ---------------------------------------------------------------------------
// Domain model
// ---------------------------------------------------------------------------

// Message is a single in-memory SQS message.
type Message struct {
    MessageID      string
    Body           string
    MD5OfBody      string
    SentAt         time.Time
    AvailableAt    time.Time // SentAt + delay: first moment the message may be received
    ReceiveCount   int
    FirstReceiveAt time.Time
    InvisibleUntil time.Time // end of the current visibility window, if any
    ReceiptHandle  string
}

func (m *Message) delayed(now time.Time) bool {
    return now.Before(m.AvailableAt)
}

func (m *Message) inflight(now time.Time) bool {
    return m.InvisibleUntil.After(now)
}

func (m *Message) visible(now time.Time) bool {
    return !m.delayed(now) && !m.inflight(now)
}

// Queue is an SQS queue in the emulator.
type Queue struct {
    Name       string
    URL        string
    ARN        string
    FIFO       bool
    CreatedAt  time.Time
    ModifiedAt time.Time
    Attributes map[string]string
    Messages   []*Message
}

func (q *Queue) attributeInt(name string, fallback int) int {
    if raw, ok := q.Attributes[name]; ok {
        if parsed, err := strconv.Atoi(raw); err == nil {
            return parsed
        }
    }
    return fallback
}

// Depth is the GetQueueAttributes visibility-statistics triple.
type Depth struct {
    Visible    int // ApproximateNumberOfMessages
    NotVisible int // ApproximateNumberOfMessagesNotVisible
    Delayed    int // ApproximateNumberOfMessagesDelayed
}

// classifyQueueMessages buckets every queued message by its current
// visibility state: delayed (first delivery still pending behind its delay),
// in flight (received, visibility timeout not yet elapsed) or otherwise
// visible depth.
func classifyQueueMessages(q *Queue, now time.Time) Depth {
    var d Depth
    for _, m := range q.Messages {
        switch {
        case m.delayed(now) && m.ReceiveCount == 0:
            d.Delayed++
        case m.inflight(now):
            d.NotVisible++
        case m.delayed(now):
            d.NotVisible++
        default:
            d.Visible++
        }
    }
    return d
}

// Attribute is one name/value queue attribute result.
type Attribute struct {
    Name  string
    Value string
}

// Receipt is the consumer-visible representation of a received message.
type Receipt struct {
    MessageID     string
    ReceiptHandle string
    MD5OfBody     string
    Body          string
    SenderID      string
    SentTimestamp int64
    FirstReceive  int64
    ReceiveCount  int
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

type Store struct {
    mu     sync.Mutex
    queues map[string]*Queue
    now    func() time.Time
}

func NewStore() *Store {
    return &Store{queues: make(map[string]*Queue), now: time.Now}
}

// SetClock overrides the store clock so tests can fast-forward delay and
// visibility windows.
func (s *Store) SetClock(now func() time.Time) {
    s.mu.Lock()
    s.now = now
    s.mu.Unlock()
}

func (s *Store) HasQueue(name string) bool {
    s.mu.Lock()
    defer s.mu.Unlock()
    _, ok := s.queues[name]
    return ok
}

func (s *Store) GetOrCreateQueue(name, queueURL string, attributes map[string]string) *Queue {
    s.mu.Lock()
    defer s.mu.Unlock()
    if q, ok := s.queues[name]; ok {
        for key, value := range attributes {
            q.Attributes[key] = value
        }
        if len(attributes) > 0 {
            q.ModifiedAt = s.now()
        }
        return q
    }
    now := s.now()
    q := &Queue{
        Name:       name,
        URL:        queueURL,
        ARN:        fmt.Sprintf("arn:aws:sqs:%s:%s:%s", defaultRegion, defaultAccount, name),
        FIFO:       strings.HasSuffix(name, ".fifo"),
        CreatedAt:  now,
        ModifiedAt: now,
        Attributes: make(map[string]string),
    }
    for key, value := range attributes {
        q.Attributes[key] = value
    }
    if q.FIFO {
        if _, ok := q.Attributes[AttrFifoQueue]; !ok {
            q.Attributes[AttrFifoQueue] = "true"
        }
    }
    s.queues[name] = q
    return q
}

func (s *Store) SetQueueAttributes(queue string, attributes map[string]string) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    q, ok := s.queues[queue]
    if !ok {
        return ErrQueueDoesNotExist
    }
    for key, value := range attributes {
        q.Attributes[key] = value
    }
    q.ModifiedAt = s.now()
    return nil
}

func (s *Store) DeleteQueue(queue string) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    if _, ok := s.queues[queue]; !ok {
        return ErrQueueDoesNotExist
    }
    delete(s.queues, queue)
    return nil
}

func (s *Store) PurgeQueue(queue string) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    q, ok := s.queues[queue]
    if !ok {
        return ErrQueueDoesNotExist
    }
    q.Messages = nil
    return nil
}

func (s *Store) ListQueues(prefix string) []string {
    s.mu.Lock()
    defer s.mu.Unlock()
    names := make([]string, 0, len(s.queues))
    for name := range s.queues {
        if prefix == "" || strings.HasPrefix(name, prefix) {
            names = append(names, name)
        }
    }
    sort.Strings(names)
    urls := make([]string, 0, len(names))
    for _, name := range names {
        urls = append(urls, s.queues[name].URL)
    }
    return urls
}

// QueueDepth returns the current visibility counters for a queue.
func (s *Store) QueueDepth(queue string) (Depth, error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    q, ok := s.queues[queue]
    if !ok {
        return Depth{}, ErrQueueDoesNotExist
    }
    return classifyQueueMessages(q, s.now()), nil
}

// SendMessage enqueues a message. A nil delaySeconds uses the queue
// DelaySeconds attribute; an explicit value (including 0) overrides it.
func (s *Store) SendMessage(queue, body string, delaySeconds *int64) (*Message, error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    q, ok := s.queues[queue]
    if !ok {
        return nil, ErrQueueDoesNotExist
    }
    delay := int64(q.attributeInt(AttrDelaySeconds, defaultDelaySeconds))
    if delaySeconds != nil {
        delay = *delaySeconds
    }
    if delay < 0 {
        delay = 0
    }
    if delay > 900 {
        delay = 900
    }
    now := s.now()
    message := &Message{
        MessageID:     newRequestID(),
        Body:          body,
        MD5OfBody:     md5Hex(body),
        SentAt:        now,
        AvailableAt:   now.Add(time.Duration(delay) * time.Second),
        ReceiptHandle: newReceiptHandle(),
    }
    q.Messages = append(q.Messages, message)
    return message, nil
}

// ReceiveMessage hands out up to maxMessages currently visible messages and
// starts their visibility window. A nil visibilityTimeout uses the queue
// VisibilityTimeout attribute.
func (s *Store) ReceiveMessage(queue string, maxMessages int, visibilityTimeout *int64) ([]Receipt, error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    q, ok := s.queues[queue]
    if !ok {
        return nil, ErrQueueDoesNotExist
    }
    if maxMessages < 1 {
        maxMessages = 1
    }
    if maxMessages > 10 {
        maxMessages = 10
    }
    timeout := int64(q.attributeInt(AttrVisibilityTimeout, defaultVisibilityTimeout))
    if visibilityTimeout != nil {
        timeout = *visibilityTimeout
    }
    if timeout < 0 {
        timeout = 0
    }
    if timeout > 43200 {
        timeout = 43200
    }
    now := s.now()
    out := make([]Receipt, 0, maxMessages)
    for _, m := range q.Messages {
        if len(out) == maxMessages {
            break
        }
        if !m.visible(now) {
            continue
        }
        m.ReceiveCount++
        if m.ReceiveCount == 1 {
            m.FirstReceiveAt = now
        }
        m.InvisibleUntil = now.Add(time.Duration(timeout) * time.Second)
        m.ReceiptHandle = newReceiptHandle()
        out = append(out, Receipt{
            MessageID:     m.MessageID,
            ReceiptHandle: m.ReceiptHandle,
            MD5OfBody:     m.MD5OfBody,
            Body:          m.Body,
            SenderID:      "AIDACKCEVSQ6C2EXAMPLE",
            SentTimestamp: m.SentAt.UnixMilli(),
            FirstReceive:  m.FirstReceiveAt.UnixMilli(),
            ReceiveCount:  m.ReceiveCount,
        })
    }
    return out, nil
}

// DeleteMessage removes the message whose current receipt handle matches.
// Unknown or stale handles are tolerated, mirroring SQS idempotent deletes.
func (s *Store) DeleteMessage(queue, receiptHandle string) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    q, ok := s.queues[queue]
    if !ok {
        return ErrQueueDoesNotExist
    }
    for i, m := range q.Messages {
        if m.ReceiptHandle == receiptHandle {
            q.Messages = append(q.Messages[:i], q.Messages[i+1:]...)
            return nil
        }
    }
    return nil
}

// GetQueueAttributes resolves the requested attribute names against the live
// queue state. Empty names, or the single name All, yield every attribute the
// queue currently supports. The result is sorted by attribute name: the depth
// and visibility counters are computed from message state, everything else
// from queue configuration.
func (s *Store) GetQueueAttributes(queue string, names []string) ([]Attribute, error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    q, ok := s.queues[queue]
    if !ok {
        return nil, ErrQueueDoesNotExist
    }
    for _, name := range names {
        if !knownQueueAttributes[name] {
            return nil, ErrInvalidAttributeName
        }
    }
    return queueAttributeSnapshot(q, s.now(), names), nil
}

// queueAttributeSnapshot computes the full attribute map for the queue and
// filters it down to the requested names (empty or All means everything).
func queueAttributeSnapshot(q *Queue, now time.Time, names []string) []Attribute {
    depth := classifyQueueMessages(q, now)
    full := map[string]string{
        AttrApproximateNumberOfMessages:           strconv.Itoa(depth.Visible),
        AttrApproximateNumberOfMessagesDelayed:    strconv.Itoa(depth.Delayed),
        AttrApproximateNumberOfMessagesNotVisible: strconv.Itoa(depth.NotVisible),
        AttrCreatedTimestamp:                      strconv.FormatInt(q.CreatedAt.Unix(), 10),
        AttrDelaySeconds:                          strconv.Itoa(q.attributeInt(AttrDelaySeconds, defaultDelaySeconds)),
        AttrLastModifiedTimestamp:                 strconv.FormatInt(q.ModifiedAt.Unix(), 10),
        AttrMaximumMessageSize:                    strconv.Itoa(q.attributeInt(AttrMaximumMessageSize, defaultMaxMessageSize)),
        AttrMessageRetentionPeriod:                strconv.Itoa(q.attributeInt(AttrMessageRetentionPeriod, defaultRetentionPeriod)),
        AttrQueueArn:                              q.ARN,
        AttrReceiveMessageWaitTimeSeconds:         strconv.Itoa(q.attributeInt(AttrReceiveMessageWaitTimeSeconds, defaultReceiveWaitSeconds)),
        AttrVisibilityTimeout:                     strconv.Itoa(q.attributeInt(AttrVisibilityTimeout, defaultVisibilityTimeout)),
    }
    if raw, ok := q.Attributes[AttrRedrivePolicy]; ok && raw != "" {
        full[AttrRedrivePolicy] = raw
    }
    if raw, ok := q.Attributes[AttrRedriveAllowPolicy]; ok && raw != "" {
        full[AttrRedriveAllowPolicy] = raw
    }
    if raw, ok := q.Attributes[AttrSqsManagedSseEnabled]; ok && raw != "" {
        full[AttrSqsManagedSseEnabled] = raw
    }
    if q.FIFO {
        full[AttrFifoQueue] = "true"
        if raw, ok := q.Attributes[AttrContentBasedDeduplication]; ok && raw != "" {
            full[AttrContentBasedDeduplication] = raw
        } else {
            full[AttrContentBasedDeduplication] = "false"
        }
        full[AttrDeduplicationScope] = defaultString(q.Attributes[AttrDeduplicationScope], "queue")
        full[AttrFifoThroughputLimit] = defaultString(q.Attributes[AttrFifoThroughputLimit], "perQueue")
    }
    if len(names) == 0 {
        names = []string{AttrAll}
    }
    wanted := make(map[string]bool, len(names))
    all := false
    for _, name := range names {
        if name == AttrAll {
            all = true
        }
        wanted[name] = true
    }
    out := make([]Attribute, 0, len(full))
    for name, value := range full {
        if all || wanted[name] {
            out = append(out, Attribute{Name: name, Value: value})
        }
    }
    sort.Slice(out, func(i, j int) bool {
        return out[i].Name < out[j].Name
    })
    return out
}

// AttributeSnapshot exposes queueAttributeSnapshot for external tests.
func AttributeSnapshot(q *Queue, now time.Time, names []string) []Attribute {
    return queueAttributeSnapshot(q, now, names)
}

func defaultString(value, fallback string) string {
    if value == "" {
        return fallback
    }
    return value
}

// ---------------------------------------------------------------------------
// HTTP server (SQS query protocol)
// ---------------------------------------------------------------------------

type Server struct {
    Store *Store
}

func NewServer() *Server {
    return &Server{Store: NewStore()}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    if err := r.ParseForm(); err != nil {
        writeSQSError(w, http.StatusBadRequest, errCodeInvalidParam, "Malformed request.")
        return
    }
    switch r.FormValue("Action") {
    case "CreateQueue":
        s.handleCreateQueue(w, r)
    case "GetQueueAttributes":
        s.handleGetQueueAttributes(w, r)
    case "SendMessage":
        s.handleSendMessage(w, r)
    case "ReceiveMessage":
        s.handleReceiveMessage(w, r)
    case "DeleteMessage":
        s.handleDeleteMessage(w, r)
    case "SetQueueAttributes":
        s.handleSetQueueAttributes(w, r)
    case "DeleteQueue":
        s.handleDeleteQueue(w, r)
    case "PurgeQueue":
        s.handlePurgeQueue(w, r)
    case "ListQueues":
        s.handleListQueues(w, r)
    default:
        writeSQSError(w, http.StatusBadRequest, "InvalidAction",
            fmt.Sprintf("The action %s is not supported.", r.FormValue("Action")))
    }
}

// queueNameOrError resolves the QueueUrl parameter to a queue name and checks
// that the queue exists, writing the canonical SQS error otherwise.
func (s *Server) queueNameOrError(w http.ResponseWriter, r *http.Request) (string, bool) {
    rawURL := r.FormValue("QueueUrl")
    if rawURL == "" {
        writeSQSError(w, http.StatusBadRequest, errCodeMissingParam, "The request must contain the parameter QueueUrl.")
        return "", false
    }
    name, err := queueNameFromQueueURL(rawURL)
    if err != nil {
        writeSQSError(w, http.StatusBadRequest, errCodeInvalidParam, "Could not parse QueueUrl.")
        return "", false
    }
    if !s.Store.HasQueue(name) {
        writeSQSError(w, http.StatusBadRequest, errCodeNonExistentQueue, errMsgNonExistentQueue)
        return "", false
    }
    return name, true
}

// handleGetQueueAttributes implements GetQueueAttributes. When AttributeNames
// is omitted (or set to All) every supported attribute is returned;
// requesting an unknown attribute name yields InvalidAttributeName and a
// missing queue yields AWS.SimpleQueueService.NonExistentQueue.
func (s *Server) handleGetQueueAttributes(w http.ResponseWriter, r *http.Request) {
    name, ok := s.queueNameOrError(w, r)
    if !ok {
        return
    }
    names := formList(r.Form, "AttributeName")
    if len(names) == 0 {
        names = formList(r.Form, "AttributeNames")
    }
    for _, requested := range names {
        if !knownQueueAttributes[requested] {
            writeSQSError(w, http.StatusBadRequest, errCodeInvalidAttr, "Unknown Attribute "+requested+".")
            return
        }
    }
    attributes, err := s.Store.GetQueueAttributes(name, names)
    if err != nil {
        writeSQSError(w, http.StatusBadRequest, errCodeNonExistentQueue, errMsgNonExistentQueue)
        return
    }
    writeXML(w, http.StatusOK, getQueueAttributesResponse{
        Result:   getQueueAttributesResult{Attribute: xmlAttributePairs(attributes)},
        Metadata: responseMetadata{RequestID: newRequestID()},
    })
}

func (s *Server) handleCreateQueue(w http.ResponseWriter, r *http.Request) {
    name := r.FormValue("QueueName")
    if name == "" {
        writeSQSError(w, http.StatusBadRequest, errCodeMissingParam, "The request must contain the parameter QueueName.")
        return
    }
    queue := s.Store.GetOrCreateQueue(name, queueURL(r, name), formAttributePairs(r.Form, "Attribute"))
    writeXML(w, http.StatusOK, createQueueResponse{
        Result:   createQueueResult{QueueURL: queue.URL},
        Metadata: responseMetadata{RequestID: newRequestID()},
    })
}

func (s *Server) handleSendMessage(w http.ResponseWriter, r *http.Request) {
    name, ok := s.queueNameOrError(w, r)
    if !ok {
        return
    }
    body, present := formSingle(r, "MessageBody")
    if !present {
        writeSQSError(w, http.StatusBadRequest, errCodeMissingParam, "The request must contain the parameter MessageBody.")
        return
    }
    var delay *int64
    if raw, present := formSingle(r, "DelaySeconds"); present {
        parsed, err := strconv.ParseInt(raw, 10, 64)
        if err != nil || parsed < 0 || parsed > 900 {
            writeSQSError(w, http.StatusBadRequest, errCodeInvalidParam, "Value for parameter DelaySeconds is invalid.")
            return
        }
        delay = &parsed
    }
    message, err := s.Store.SendMessage(name, body, delay)
    if err != nil {
        writeSQSError(w, http.StatusBadRequest, errCodeNonExistentQueue, errMsgNonExistentQueue)
        return
    }
    writeXML(w, http.StatusOK, sendMessageResponse{
        Result:   sendMessageResult{MD5OfMessageBody: message.MD5OfBody, MessageID: message.MessageID},
        Metadata: responseMetadata{RequestID: newRequestID()},
    })
}

func (s *Server) handleReceiveMessage(w http.ResponseWriter, r *http.Request) {
    name, ok := s.queueNameOrError(w, r)
    if !ok {
        return
    }
    maxMessages := 1
    if raw, present := formSingle(r, "MaxNumberOfMessages"); present {
        parsed, err := strconv.Atoi(raw)
        if err != nil || parsed < 1 || parsed > 10 {
            writeSQSError(w, http.StatusBadRequest, errCodeInvalidParam, "Value for parameter MaxNumberOfMessages is invalid.")
            return
        }
        maxMessages = parsed
    }
    var visibilityTimeout *int64
    if raw, present := formSingle(r, "VisibilityTimeout"); present {
        parsed, err := strconv.ParseInt(raw, 10, 64)
        if err != nil || parsed < 0 || parsed > 43200 {
            writeSQSError(w, http.StatusBadRequest, errCodeInvalidParam, "Value for parameter VisibilityTimeout is invalid.")
            return
        }
        visibilityTimeout = &parsed
    }
    // WaitTimeSeconds is accepted for wire compatibility; long polling is
    // emulated as an immediate best-effort delivery.
    receipts, err := s.Store.ReceiveMessage(name, maxMessages, visibilityTimeout)
    if err != nil {
        writeSQSError(w, http.StatusBadRequest, errCodeNonExistentQueue, errMsgNonExistentQueue)
        return
    }
    messages := make([]xmlMessage, 0, len(receipts))
    for _, receipt := range receipts {
        messages = append(messages, xmlMessage{
            MessageID:     receipt.MessageID,
            ReceiptHandle: receipt.ReceiptHandle,
            MD5OfBody:     receipt.MD5OfBody,
            Body:          receipt.Body,
            Attribute: []xmlAttribute{
                {Name: "ApproximateFirstReceiveTimestamp", Value: strconv.FormatInt(receipt.FirstReceive, 10)},
                {Name: "ApproximateReceiveCount", Value: strconv.Itoa(receipt.ReceiveCount)},
                {Name: "SenderId", Value: receipt.SenderID},
                {Name: "SentTimestamp", Value: strconv.FormatInt(receipt.SentTimestamp, 10)},
            },
        })
    }
    writeXML(w, http.StatusOK, receiveMessageResponse{
        Result:   receiveMessageResult{Message: messages},
        Metadata: responseMetadata{RequestID: newRequestID()},
    })
}

func (s *Server) handleDeleteMessage(w http.ResponseWriter, r *http.Request) {
    name, ok := s.queueNameOrError(w, r)
    if !ok {
        return
    }
    s.Store.DeleteMessage(name, r.FormValue("ReceiptHandle"))
    writeXML(w, http.StatusOK, deleteMessageResponse{Metadata: responseMetadata{RequestID: newRequestID()}})
}

func (s *Server) handleSetQueueAttributes(w http.ResponseWriter, r *http.Request) {
    name, ok := s.queueNameOrError(w, r)
    if !ok {
        return
    }
    if err := s.Store.SetQueueAttributes(name, formAttributePairs(r.Form, "Attribute")); err != nil {
        writeSQSError(w, http.StatusBadRequest, errCodeNonExistentQueue, errMsgNonExistentQueue)
        return
    }
    writeXML(w, http.StatusOK, setQueueAttributesResponse{Metadata: responseMetadata{RequestID: newRequestID()}})
}

func (s *Server) handleDeleteQueue(w http.ResponseWriter, r *http.Request) {
    name, ok := s.queueNameOrError(w, r)
    if !ok {
        return
    }
    if err := s.Store.DeleteQueue(name); err != nil {
        writeSQSError(w, http.StatusBadRequest, errCodeNonExistentQueue, errMsgNonExistentQueue)
        return
    }
    writeXML(w, http.StatusOK, deleteQueueResponse{Metadata: responseMetadata{RequestID: newRequestID()}})
}

func (s *Server) handlePurgeQueue(w http.ResponseWriter, r *http.Request) {
    name, ok := s.queueNameOrError(w, r)
    if !ok {
        return
    }
    if err := s.Store.PurgeQueue(name); err != nil {
        writeSQSError(w, http.StatusBadRequest, errCodeNonExistentQueue, errMsgNonExistentQueue)
        return
    }
    writeXML(w, http.StatusOK, purgeQueueResponse{Metadata: responseMetadata{RequestID: newRequestID()}})
}

func (s *Server) handleListQueues(w http.ResponseWriter, r *http.Request) {
    writeXML(w, http.StatusOK, listQueuesResponse{
        Result:   listQueuesResult{QueueURL: s.Store.ListQueues(r.FormValue("QueueNamePrefix"))},
        Metadata: responseMetadata{RequestID: newRequestID()},
    })
}

// ---------------------------------------------------------------------------
// Wire payloads (SQS XML documents)
// ---------------------------------------------------------------------------

type xmlAttribute struct {
    Name  string `xml:"Name"`
    Value string `xml:"Value"`
}

func xmlAttributePairs(attributes []Attribute) []xmlAttribute {
    out := make([]xmlAttribute, 0, len(attributes))
    for _, attribute := range attributes {
        out = append(out, xmlAttribute{Name: attribute.Name, Value: attribute.Value})
    }
    return out
}

type responseMetadata struct {
    RequestID string `xml:"RequestId"`
}

type getQueueAttributesResult struct {
    Attribute []xmlAttribute `xml:"Attribute"`
}

type getQueueAttributesResponse struct {
    XMLName  xml.Name                 `xml:"http://queue.amazonaws.com/doc/2012-11-05/ GetQueueAttributesResponse"`
    Result   getQueueAttributesResult `xml:"GetQueueAttributesResult"`
    Metadata responseMetadata         `xml:"ResponseMetadata"`
}

type createQueueResult struct {
    QueueURL string `xml:"QueueUrl"`
}

type createQueueResponse struct {
    XMLName  xml.Name          `xml:"http://queue.amazonaws.com/doc/2012-11-05/ CreateQueueResponse"`
    Result   createQueueResult `xml:"CreateQueueResult"`
    Metadata responseMetadata  `xml:"ResponseMetadata"`
}

type sendMessageResult struct {
    MD5OfMessageBody string `xml:"MD5OfMessageBody"`
    MessageID        string `xml:"MessageId"`
}

type sendMessageResponse struct {
    XMLName  xml.Name          `xml:"http://queue.amazonaws.com/doc/2012-11-05/ SendMessageResponse"`
    Result   sendMessageResult `xml:"SendMessageResult"`
    Metadata responseMetadata  `xml:"ResponseMetadata"`
}

type xmlMessage struct {
    MessageID     string         `xml:"MessageId"`
    ReceiptHandle string         `xml:"ReceiptHandle"`
    MD5OfBody     string         `xml:"MD5OfBody"`
    Body          string         `xml:"Body"`
    Attribute     []xmlAttribute `xml:"Attribute"`
}

type receiveMessageResult struct {
    Message []xmlMessage `xml:"Message"`
}

type receiveMessageResponse struct {
    XMLName  xml.Name             `xml:"http://queue.amazonaws.com/doc/2012-11-05/ ReceiveMessageResponse"`
    Result   receiveMessageResult `xml:"ReceiveMessageResult"`
    Metadata responseMetadata     `xml:"ResponseMetadata"`
}

type deleteMessageResponse struct {
    XMLName  xml.Name         `xml:"http://queue.amazonaws.com/doc/2012-11-05/ DeleteMessageResponse"`
    Metadata responseMetadata `xml:"ResponseMetadata"`
}

type setQueueAttributesResponse struct {
    XMLName  xml.Name         `xml:"http://queue.amazonaws.com/doc/2012-11-05/ SetQueueAttributesResponse"`
    Metadata responseMetadata `xml:"ResponseMetadata"`
}

type deleteQueueResponse struct {
    XMLName  xml.Name         `xml:"http://queue.amazonaws.com/doc/2012-11-05/ DeleteQueueResponse"`
    Metadata responseMetadata `xml:"ResponseMetadata"`
}

type purgeQueueResponse struct {
    XMLName  xml.Name         `xml:"http://queue.amazonaws.com/doc/2012-11-05/ PurgeQueueResponse"`
    Metadata responseMetadata `xml:"ResponseMetadata"`
}

type listQueuesResult struct {
    QueueURL []string `xml:"QueueUrl"`
}

type listQueuesResponse struct {
    XMLName  xml.Name         `xml:"http://queue.amazonaws.com/doc/2012-11-05/ ListQueuesResponse"`
    Result   listQueuesResult `xml:"ListQueuesResult"`
    Metadata responseMetadata `xml:"ResponseMetadata"`
}

type sqsErrorBody struct {
    Type    string `xml:"Type"`
    Code    string `xml:"Code"`
    Message string `xml:"Message"`
}

type errorResponse struct {
    XMLName  xml.Name         `xml:"http://queue.amazonaws.com/doc/2012-11-05/ ErrorResponse"`
    Error    sqsErrorBody     `xml:"Error"`
    Metadata responseMetadata `xml:"ResponseMetadata"`
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func writeXML(w http.ResponseWriter, status int, payload interface{}) {
    body, err := xml.MarshalIndent(payload, "", "  ")
    if err != nil {
        http.Error(w, "internal error: "+err.Error(), http.StatusInternalServerError)
        return
    }
    w.Header().Set("Content-Type", "text/xml")
    w.WriteHeader(status)
    _, _ = w.Write([]byte(xml.Header))
    _, _ = w.Write(body)
    _, _ = w.Write([]byte("\n"))
}

func writeSQSError(w http.ResponseWriter, status int, code, message string) {
    writeXML(w, status, errorResponse{
        Error:    sqsErrorBody{Type: "Sender", Code: code, Message: message},
        Metadata: responseMetadata{RequestID: newRequestID()},
    })
}

// formSingle returns the first value for key plus whether the key was set.
func formSingle(r *http.Request, key string) (string, bool) {
    values, ok := r.Form[key]
    if !ok || len(values) == 0 {
        return "", false
    }
    return values[0], true
}

// formList reads list-form parameters in both the plain (Name) and the
// indexed (Name.1, Name.2) AWS query-protocol spellings.
func formList(form url.Values, base string) []string {
    var out []string
    if values, ok := form[base]; ok {
        out = append(out, values...)
    }
    for i := 1; i <= 128; i++ {
        values, ok := form[fmt.Sprintf("%s.%d", base, i)]
        if !ok {
            break
        }
        out = append(out, values...)
    }
    return out
}

// formAttributePairs reads Attribute.N.Name / Attribute.N.Value entries.
func formAttributePairs(form url.Values, base string) map[string]string {
    out := make(map[string]string)
    for i := 1; i <= 128; i++ {
        names, ok := form[fmt.Sprintf("%s.%d.Name", base, i)]
        if !ok {
            break
        }
        value := ""
        if values := form[fmt.Sprintf("%s.%d.Value", base, i)]; len(values) > 0 {
            value = values[0]
        }
        if len(names) > 0 && names[0] != "" {
            out[names[0]] = value
        }
    }
    return out
}

func queueURL(r *http.Request, name string) string {
    host := r.Host
    if host == "" {
        host = "localhost:4566"
    }
    return fmt.Sprintf("http://%s/%s/%s", host, defaultAccount, name)
}

func queueNameFromQueueURL(raw string) (string, error) {
    parsed, err := url.Parse(raw)
    if err != nil {
        return "", err
    }
    path := strings.Trim(parsed.Path, "/")
    if path == "" {
        return "", fmt.Errorf("QueueUrl %q does not address a queue", raw)
    }
    parts := strings.Split(path, "/")
    return parts[len(parts)-1], nil
}

func md5Hex(value string) string {
    sum := md5.Sum([]byte(value))
    return hex.EncodeToString(sum[:])
}

func newReceiptHandle() string {
    buf := make([]byte, 24)
    _, _ = rand.Read(buf)
    return base64.StdEncoding.EncodeToString(buf)
}

// newRequestID returns a UUIDv4-shaped request identifier.
func newRequestID() string {
    buf := make([]byte, 16)
    _, _ = rand.Read(buf)
    buf[6] = (buf[6] & 0x0f) | 0x40
    buf[8] = (buf[8] & 0x3f) | 0x80
    hexed := hex.EncodeToString(buf)
    return hexed[0:8] + "-" + hexed[8:12] + "-" + hexed[12:16] + "-" + hexed[16:20] + "-" + hexed[20:32]
}

// ---------------------------------------------------------------------------
// Entrypoint
// ---------------------------------------------------------------------------

func main() {
    port := os.Getenv("FLOCI_PORT")
    if port == "" {
        port = os.Getenv("PORT")
    }
    if port == "" {
        port = "4566"
    }
    http.Handle("/", NewServer())
    log.Printf("floci-go: SQS emulation listening on %s", ":"+port)
    if err := http.ListenAndServe(":"+port, nil); err != nil {
        log.Fatal(err)
    }
}
