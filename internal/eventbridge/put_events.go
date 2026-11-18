package eventbridge

// put_events.go — AWSEvents.PutEvents, the ingress of the event bus. Every
// entry is validated, wrapped into the canonical event envelope, matched
// against all enabled rules of the target bus and delivered to the matching
// rules' targets (SQS SendMessage / HTTP POST, see dispatch.go).

import (
    "bytes"
    "crypto/rand"
    "encoding/json"
    "fmt"
    "net/http"
    "regexp"
    "strings"
    "time"
)

const ingressMaxEntries = 10

type PutEventsInput struct {
    Entries []*PutEventsRequestEntry `json:"Entries"`
}

type PutEventsRequestEntry struct {
    Detail       string      `json:"Detail"`
    DetailType   string      `json:"DetailType"`
    EventBusName string      `json:"EventBusName"`
    Resources    []string    `json:"Resources"`
    Source       string      `json:"Source"`
    Time         ingressTime `json:"Time"`
}

type PutEventsOutput struct {
    Entries          []*PutEventsResultEntry `json:"Entries"`
    FailedEntryCount int                     `json:"FailedEntryCount"`
}

type PutEventsResultEntry struct {
    ErrorCode    string `json:"ErrorCode,omitempty"`
    ErrorMessage string `json:"ErrorMessage,omitempty"`
    EventId      string `json:"EventId,omitempty"`
}

// ingressTime accepts the timestamp encodings used by PutEvents entries:
// epoch seconds (possibly fractional) or an ISO-8601 timestamp string.
type ingressTime struct {
    t   time.Time
    set bool
}

func (t *ingressTime) UnmarshalJSON(b []byte) error {
    b = bytes.TrimSpace(b)
    if len(b) == 0 || string(b) == "null" {
        return nil
    }
    t.set = true
    if b[0] == '"' {
        var s string
        if err := json.Unmarshal(b, &s); err != nil {
            return err
        }
        parsed, err := parseIngressTimestamp(s)
        if err != nil {
            return err
        }
        t.t = parsed
        return nil
    }
    var secs float64
    if err := json.Unmarshal(b, &secs); err != nil {
        return err
    }
    whole := float64(int64(secs))
    t.t = time.Unix(int64(secs), int64((secs-whole)*1e9))
    return nil
}

func parseIngressTimestamp(s string) (time.Time, error) {
    for _, layout := range []string{
        time.RFC3339Nano,
        time.RFC3339,
        "2006-01-02T15:04:05.000Z0700",
    } {
        if ts, err := time.Parse(layout, s); err == nil {
            return ts, nil
        }
    }
    return time.Time{}, fmt.Errorf("invalid timestamp %q", s)
}

var ingressSourcePattern = regexp.MustCompile(`^[A-Za-z0-9_.\-]+$`)

// ingressNow can be replaced in tests; PutEvents stamps the event time.
var ingressNow = func() time.Time { return time.Now() }

// PutEvents ingests up to ten entries. Protocol validation runs first
// (required members, entry limit); failures discovered per entry are
// carried in Entries[i].ErrorCode/ErrorMessage.
func (s *Service) PutEvents(in *PutEventsInput) (*PutEventsOutput, error) {
    if in == nil || len(in.Entries) == 0 {
        return nil, newValidationf("Entries is required.")
    }
    if len(in.Entries) > ingressMaxEntries {
        return nil, ebErrorf(http.StatusBadRequest, ebCodeTotalEntriesLimit,
            "A single call to PutEvents must contain at most %d entries.", ingressMaxEntries)
    }
    for i, e := range in.Entries {
        if e == nil {
            return nil, newValidationf("Entries must not contain null values.")
        }
        if e.Source == "" || e.DetailType == "" || e.Detail == "" {
            return nil, newValidationf("Entry %d is missing one of the required members Source, DetailType or Detail.", i+1)
        }
    }
    out := &PutEventsOutput{Entries: make([]*PutEventsResultEntry, 0, len(in.Entries))}
    for _, e := range in.Entries {
        id, apiErr := s.ingestEventEntry(e)
        if apiErr != nil {
            out.Entries = append(out.Entries, &PutEventsResultEntry{
                ErrorCode:    apiErr.Code,
                ErrorMessage: apiErr.Message,
            })
            continue
        }
        out.Entries = append(out.Entries, &PutEventsResultEntry{EventId: id})
    }
    for _, e := range out.Entries {
        if e.ErrorCode != "" {
            out.FailedEntryCount++
        }
    }
    return out, nil
}

// ingestEventEntry validates one entry, matches and delivers it. The event
// id exists from the moment the event enters the bus, even when nothing
// matches.
func (s *Service) ingestEventEntry(e *PutEventsRequestEntry) (string, *ebError) {
    bus := normalizeBusName(e.EventBusName)
    if !s.busExists(bus) {
        return "", newResourceNotFoundf("Event bus %s does not exist.", bus)
    }
    if !ingressSourcePattern.MatchString(e.Source) {
        return "", newInvalidParameterf("Parameter Source is not valid. Reason: only alphanumeric characters, '.', '_' and '-' are allowed.")
    }
    var detail any
    dec := json.NewDecoder(strings.NewReader(e.Detail))
    dec.UseNumber()
    if err := dec.Decode(&detail); err != nil {
        return "", newInvalidParameterf("Parameter Detail is not valid. Reason: Detail must be valid JSON.")
    }
    id := newIngressEventID()
    ts := ingressNow().UTC()
    if e.Time.set {
        ts = e.Time.t
    }
    env := buildIngressEnvelope(id, e, detail, ts)
    envJSON, _ := json.Marshal(env)
    s.matchAndDeliver(bus, env, envJSON)
    return id, nil
}

// matchAndDeliver runs an ingested envelope through every enabled rule of
// the bus and delivers it to each matching rule's targets in order.
func (s *Service) matchAndDeliver(bus string, env map[string]any, envJSON []byte) {
    for _, rule := range s.busRules(bus) {
        if !ruleStateEnabled(rule) {
            continue
        }
        filter, err := compileEventFilter(rule.PatternJSON)
        if err != nil {
            continue // malformed patterns never match
        }
        if !filter.Matches(env) {
            continue
        }
        for _, tgt := range s.targetTable().list(bus, rule.Name) {
            s.deliverIngressTarget(tgt, env, envJSON)
        }
    }
}

// buildIngressEnvelope wraps an entry in the canonical event envelope.
func buildIngressEnvelope(id string, e *PutEventsRequestEntry, detail any, ts time.Time) map[string]any {
    resources := []any{}
    for _, r := range e.Resources {
        resources = append(resources, r)
    }
    return map[string]any{
        "version":     "0",
        "id":          id,
        "detail-type": e.DetailType,
        "source":      e.Source,
        "account":     ingressAccountID,
        "time":        ts.UTC().Format(time.RFC3339Nano),
        "region":      ingressRegion,
        "resources":   resources,
        "detail":      detail,
    }
}

// newIngressEventID returns a version-4 UUID used as the event id.
func newIngressEventID() string {
    b := make([]byte, 16)
    if _, err := rand.Read(b); err != nil {
        now := time.Now().UnixNano()
        for i := range b {
            b[i] = byte(now >> (uint(i%7) * 8))
        }
    }
    b[6] = (b[6] & 0x0f) | 0x40
    b[8] = (b[8] & 0x3f) | 0x80
    return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (s *Service) handlePutEvents(w http.ResponseWriter, r *http.Request) {
    var in PutEventsInput
    if err := decodeEventBridgeRequest(r, &in); err != nil {
        writeEventBridgeError(w, newValidationf("Malformed request body: %v", err))
        return
    }
    // Remember the address this call arrived on so target delivery can
    // reach the SQS surface of the same floci process.
    s.rememberIngressBase(r)
    out, err := s.PutEvents(&in)
    if err != nil {
        writeEventBridgeError(w, err)
        return
    }
    writeEventBridgeJSON(w, out)
}
