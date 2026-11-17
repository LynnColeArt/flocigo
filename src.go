// floci-go / ticket-021: SQS SendMessage.
//
// SendMessage appends the message to the queue's backing file (one JSON
// object per line in <dataDir>/queues/<queue>.jsonl) and answers on the SQS
// query-protocol wire format with MessageId and MD5OfMessageBody (the
// lowercase hex MD5 of the raw message body).
package main

import (
    "crypto/md5"
    "crypto/rand"
    "encoding/base64"
    "encoding/binary"
    "encoding/hex"
    "encoding/json"
    "encoding/xml"
    "fmt"
    "io"
    "log"
    "net/http"
    "net/url"
    "os"
    "path/filepath"
    "sort"
    "strconv"
    "strings"
    "sync"
    "time"
)

const (
    sqsXMLNS    = "http://queue.amazonaws.com/doc/2012-11-05/"
    accountID   = "000000000000"
    maxBodySize = 262144
)

// Attribute is a single SQS message attribute as submitted with SendMessage.
type Attribute struct {
    DataType string `json:"dataType"`
    Value    string `json:"value"`
    Binary   bool   `json:"binary,omitempty"`
}

// Message is one persisted queue-file record (one JSON line).
type Message struct {
    MessageID              string               `json:"messageId"`
    MD5OfMessageBody       string               `json:"md5OfMessageBody"`
    Body                   string               `json:"body"`
    MD5OfMessageAttributes string               `json:"md5OfMessageAttributes,omitempty"`
    MessageAttributes      map[string]Attribute `json:"messageAttributes,omitempty"`
    DelaySeconds           int                  `json:"delaySeconds"`
    MessageGroupID         string               `json:"messageGroupId,omitempty"`
    MessageDeduplicationID string               `json:"messageDeduplicationId,omitempty"`
    SentAt                 time.Time            `json:"sentAt"`
    VisibleAt              time.Time            `json:"visibleAt"`
    ReceiveCount           int                  `json:"receiveCount"`
}

// SQSError describes an SQS wire-level error response.
type SQError = struct{} // placeholder removed
