// floci-go - EventBridge module.
//
// eventbridge_events.go implements PutEvents (THE ingress) and the
// TestEventPattern dry-run endpoint.
//
// PutEvents flow: entries are schema-validated (whole-request
// ValidationException), valid entries are stamped into the canonical EventBridge
// envelope, matched against every ENABLED pattern rule registered on the
// entry's event bus, and matched rules dispatch the event to their targets
// through the dispatcher: input transformation (Input / InputPath /
// InputTransformer) followed by SQS SendMessage or http(s) POST. Deliveries
// run synchronously so callers observe side effects deterministically right
// after PutEvents; delivery failures are logged and never fail the API call.
package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "net/url"
    "sort"
    "strconv"
    "strings"
    "time"
)

// ebSQSSendMessageHook is the optional in-process delivery hook installed by
// the SQS module (signature: queue ARN, message body, message group id or
// empty string). When it is nil the dispatcher falls back to an HTTP
// SendMessage form POST against ebSQSEndpoint.
var ebSQSSendMessageHook func(queueARN, messageBody, messageGroupID string) error

// ebEnvelope mirrors the JSON envelope EventBridge delivers to targets.
type ebEnvelope struct {
    Version    string          `json:"version"`
    ID         string          `json:"id"`
    DetailType string          `json:"detail-type"`
    Source     string          `json:"source"`
    Account    string          `json:"account"`
    Time       string          `json:"time"`
    Region     string          `json:"region"`
    Resources  []string        `json:"resources"`
    Detail     json.RawMessage `json:"detail"`
}

type ebPutEventsEntry struct {
    EventBusName string   `json:"EventBusName"`
    Source       *string  `json:"Source"`
    DetailType   *string  `json:"DetailType"`
    Detail       *string  `json:"Detail"`
    Resources    []string `json:"Resources"`
    Time         *float64 `json:"Time"`
}

// ---------------------------------------------------------------------------
// PutEvents
// ---------------------------------------------------------------------------

func ebHandlePutEvents(w http.ResponseWriter, r *http.Request) {
    body, err := ebReadBody(r)
    if err != nil {
        ebValidationException(w, "Unable to read request body")
        return
    }
    var req struct {
        Entries []*ebPutEventsEntry `json:"Entries"`
    }
    if err := json.Unmarshal(body, &req); err != nil {
        ebValidationException(w, fmt.Sprintf("Unable to parse request: %v", err))
        return
    }
    if req.Entries == nil {
        ebValidationException(w, "Entries is a required parameter")
        return
    }
    if len(req.Entries) == 0 {
        ebValidationException(w, "Entries must not be empty")
        return
    }
    if len(req.Entries) > 10 {
        ebValidationException(w, "The total number of entries in a PutEvents request must be less than or equal to 10")
        return
    }

    // Schema-level validation: these failures abort the whole request (the
    // same behavior as the AWS client-side JSON schema check).
    for i, entry := range req.Entries {
        if entry == nil {
            ebValidationException(w, fmt.Sprintf("Entries member %d must not be null", i+1))
            return
        }
        if entry.Source == nil || *entry.Source == "" {
            ebValidationException(w, "Source is a required parameter")
            return
        }
        if entry.DetailType == nil || *entry.DetailType == "" {
            ebValidationException(w, "DetailType is a required parameter")
            return
        }
        if entry.Detail == nil || *entry.Detail == "" {
            ebValidationException(w, "Detail is a required parameter")
            return
        }
        if !EventBridgeBusExists(ebBusOrDefault(entry.EventBusName)) {
            ebResourceNotFound(w, fmt.Sprintf("Event bus %s does not exist", ebBusOrDefault(entry.EventBusName)))
            return
        }
    }

    // Per-entry ingress: build the envelope, evaluate it against every rule on
    // the bus, dispatch matched rules' targets.
    results := make([]map[string]interface{}, 0, len(req.Entries))
    failedEntryCount := 0
    for _, entry := range req.Entries {
        detailRaw, detailErr := ebCompactDetail(*entry.Detail)
        if detailErr != nil {
            results = append(results, map[string]interface{}{
                "ErrorCode":    "NonValidJSON",
                "ErrorMessage": fmt.Sprintf("The Detail field of an entry is not valid JSON: %v", detailErr),
            })
            failedEntryCount++
            continue
        }

        eventID := ebNewEventID()
        eventTime := time.Now().UTC()
        if entry.Time != nil && *entry.Time > 0 {
            eventTime = time.Unix(int64(*entry.Time), 0).UTC()
        }
        resources := entry.Resources
        if resources == nil {
            resources = []string{}
        }
        envelope := &ebEnvelope{
            Version:    "0",
            ID:         eventID,
            DetailType: *entry.DetailType,
            Source:     *entry.Source,
            Account:    ebAccountID,
            Time:       ebRFC3339(eventTime),
            Region:     ebRegion,
            Resources:  resources,
            Detail:     detailRaw,
        }
        envelopeJSON, _ := ebMarshalNoEscape(envelope) // cannot fail on a validated envelope
        envelopeMap := map[string]interface{}{}
        _ = json.Unmarshal(envelopeJSON, &envelopeMap)

        bus := ebBusOrDefault(entry.EventBusName)
        for _, snapshot := range EventBridgeRulesOnBus(bus) {
            if snapshot.State == "DISABLED" || snapshot.ScheduleExpression != "" {
                continue // cron/rate rules never fire from PutEvents
            }
            patternJSON := strings.TrimSpace(snapshot.EventPattern)
            if patternJSON == "" {
                continue
            }
            pattern, patternErr := ebParsePattern(patternJSON)
            if patternErr != nil {
                continue // stored patterns are validated at PutRule time
            }
            if !ebPatternMatches(pattern, envelopeMap) {
                continue
            }
            ruleARN := ebEffectiveRuleARN(snapshot, bus, snapshot.Name)
            ebDispatchToRuleTargets(ruleARN, envelopeJSON, envelopeMap)
        }

        results = append(results, map[string]interface{}{"EventId": eventID})
    }

    ebWriteJSON(w, http.StatusOK, map[string]interface{}{
        "Entries":          results,
        "FailedEntryCount": failedEntryCount,
    })
}

// ebCompactDetail validates and compacts the entry Detail payload.
func ebCompactDetail(raw string) (json.RawMessage, error) {
    var buf bytes.Buffer
    if err := json.Compact(&buf, []byte(raw)); err != nil {
        return nil, err
    }
    return json.RawMessage(buf.Bytes()), nil
}

// ebMarshalNoEscape serializes payload without HTML escaping so deliveries
// keep raw < > & characters, matching AWS output.
func ebMarshalNoEscape(payload interface{}) ([]byte, error) {
    var buf bytes.Buffer
    encoder := json.NewEncoder(&buf)
    encoder.SetEscapeHTML(false)
    if err := encoder.Encode(payload); err != nil {
        return nil, err
    }
    encoded := buf.Bytes()
    if n := len(encoded); n > 0 && encoded[n-1] == '\n' {
        encoded = encoded[:n-1]
    }
    return encoded, nil
}

// ---------------------------------------------------------------------------
// Target dispatcher
// ---------------------------------------------------------------------------

// ebDispatchToRuleTargets delivers a matched event to every target on ruleARN.
// Delivery runs synchronously and sequentially so side effects (e.g. SQS
// queue depth) are observable as soon as PutEvents returns; failures only log.
func ebDispatchToRuleTargets(ruleARN string, envelopeJSON []byte, envelopeMap map[string]interface{}) {
    for _, target := range ebTargetStoreSnapshot(ruleARN) {
        payload, err := ebResolveTargetPayload(target, envelopeJSON, envelopeMap)
        if err != nil {
            log.Printf("[eventbridge] rule %s target %s: %v", ruleARN, target.Id, err)
            continue
        }
        if err := ebDeliver(target, payload); err != nil {
            log.Printf("[eventbridge] rule %s target %s (%s): delivery failed: %v", ruleARN, target.Id, target.Arn, err)
            continue
        }
        log.Printf("[eventbridge] rule %s target %s -> %s delivered", ruleARN, target.Id, target.Arn)
    }
}

// ebResolveTargetPayload applies the target input configuration:
// Input (literal), InputPath (JSONPath projection), InputTransformer
// (&lt;key&gt; template substitution) or the untouched envelope.
func ebResolveTargetPayload(target ebTarget, envelopeJSON []byte, envelopeMap map[string]interface{}) (string, error) {
    switch {
    case target.Input != nil:
        return *target.Input, nil
    case target.InputPath != nil:
        value, ok := ebJSONPathLookup(envelopeMap, *target.InputPath)
        if !ok {
            return "", fmt.Errorf("InputPath '%s' did not match the event", *target.InputPath)
        }
        encoded, err := json.Marshal(value)
        if err != nil {
            return "", err
        }
        return string(encoded), nil
    case target.InputTransformer != nil:
        return ebApplyInputTransformer(target.InputTransformer, envelopeMap)
    default:
        return string(envelopeJSON), nil
    }
}

func ebApplyInputTransformer(transformer *ebInputTransformer, envelopeMap map[string]interface{}) (string, error) {
    values := make(map[string]string, len(transformer.InputPathsMap))
    for key, path := range transformer.InputPathsMap {
        value, ok := ebJSONPathLookup(envelopeMap, path)
        if !ok {
            return "", fmt.Errorf("InputPathsMap key '%s' (JSONPath '%s') did not match the event", key, path)
        }
        values[key] = ebTemplateValue(value)
    }
    keys := make([]string, 0, len(values))
    for key := range values {
        keys = append(keys, key)
    }
    sort.Strings(keys)
    pairs := make([]string, 0, 2*len(keys))
    for _, key := range keys {
        pairs = append(pairs, "<"+key+">", values[key])
    }
    return strings.NewReplacer(pairs...).Replace(transformer.InputTemplate), nil
}

// ebTemplateValue renders a template value: strings are inserted raw (callers
// add their own quotes); everything else is inserted as compact JSON.
func ebTemplateValue(value interface{}) string {
    if s, ok := value.(string); ok {
        return s
    }
    encoded, err := json.Marshal(value)
    if err != nil {
        return fmt.Sprintf("%v", value)
    }
    return string(encoded)
}

// ---------------------------------------------------------------------------
// JSONPath subset accepted for InputPath / InputPathsMap:
// $, $.a.b, $['a'], $.a[0], $.a[*] and $.* (wildcards produce arrays)
// ---------------------------------------------------------------------------

type ebJPTokKind int

const (
    ebJPKey ebJPTokKind = iota
    ebJPIndex
    ebJPAll
)

type ebJPTok struct {
    kind  ebJPTokKind
    key   string
    index int
}

func ebParseJSONPath(path string) ([]ebJPTok, bool) {
    p := strings.TrimSpace(path)
    if !strings.HasPrefix(p, "$") {
        return nil, false
    }
    p = p[1:]
    var tokens []ebJPTok
    for p != "" {
        switch {
        case strings.HasPrefix(p, ".."):
            return nil, false // recursive descent unsupported
        case strings.HasPrefix(p, "."):
            p = p[1:]
            if p == "" {
                return nil, false
            }
            if p[0] == '*' {
                tokens = append(tokens, ebJPTok{kind: ebJPAll})
                p = p[1:]
                continue
            }
            end := 0
            for end < len(p) && p[end] != '.' && p[end] != '[' {
                end++
            }
            if end == 0 {
                return nil, false
            }
            tokens = append(tokens, ebJPTok{kind: ebJPKey, key: p[:end]})
            p = p[end:]
        case strings.HasPrefix(p, "["):
            closing := strings.IndexByte(p, ']')
            if closing < 0 {
                return nil, false
            }
            inner := strings.TrimSpace(p[1:closing])
            p = p[closing+1:]
            switch {
            case inner == "*":
                tokens = append(tokens, ebJPTok{kind: ebJPAll})
            case len(inner) >= 2 && (inner[0] == 39 || inner[0] == 34) && inner[len(inner)-1] == inner[0]:
                // 39 and 34 are the ASCII codes of the single and double quote
                tokens = append(tokens, ebJPTok{kind: ebJPKey, key: inner[1 : len(inner)-1]})
            default:
                index, err := strconv.Atoi(inner)
                if err != nil || index < 0 {
                    return nil, false
                }
                tokens = append(tokens, ebJPTok{kind: ebJPIndex, index: index})
            }
        default:
            return nil, false
        }
    }
    return tokens, true
}

func ebJSONPathLookup(root interface{}, path string) (interface{}, bool) {
    tokens, ok := ebParseJSONPath(path)
    if !ok {
        return nil, false
    }
    current := root
    for _, token := range tokens {
        switch token.kind {
        case ebJPKey:
            obj, ok := current.(map[string]interface{})
            if !ok {
                return nil, false
            }
            value, ok := obj[token.key]
            if !ok {
                return nil, false
            }
            current = value
        case ebJPIndex:
            list, ok := current.([]interface{})
            if !ok || token.index >= len(list) {
                return nil, false
            }
            current = list[token.index]
        case ebJPAll:
            var collected []interface{}
            switch value := current.(type) {
            case map[string]interface{}:
                keys := make([]string, 0, len(value))
                for key := range value {
                    keys = append(keys, key)
                }
                sort.Strings(keys)
                for _, key := range keys {
                    collected = append(collected, value[key])
                }
            case []interface{}:
                collected = append(collected, value...)
            default:
                return nil, false
            }
            current = collected
        }
    }
    return current, true
}

// ---------------------------------------------------------------------------
// Delivery
// ---------------------------------------------------------------------------

type ebSQSQueueRef struct {
    ARN     string
    Account string
    Queue   string
}

func ebSQSFromARN(arn string) *ebSQSQueueRef {
    if !strings.HasPrefix(arn, "arn:") {
        return nil
    }
    parts := strings.Split(arn, ":")
    if len(parts) < 6 || parts[2] != "sqs" {
        return nil
    }
    return &ebSQSQueueRef{ARN: arn, Account: parts[4], Queue: parts[5]}
}

// ebDeliver routes the transformed payload to its target: SQS queues via the
// in-process hook (or a loopback SendMessage) and http(s) endpoints via POST.
func ebDeliver(target ebTarget, payload string) error {
    if queue := ebSQSFromARN(target.Arn); queue != nil {
        return ebDeliverSQS(*queue, payload, target)
    }
    if strings.HasPrefix(target.Arn, "http://") || strings.HasPrefix(target.Arn, "https://") {
        return ebDeliverHTTP(target.Arn, payload)
    }
    return fmt.Errorf("unsupported target type (the dispatcher routes sqs queues and http(s) endpoints only): %s", target.Arn)
}

func ebDeliverSQS(queue ebSQSQueueRef, payload string, target ebTarget) error {
    messageGroupID := ""
    if target.SqsParameters != nil {
        messageGroupID = target.SqsParameters.MessageGroupId
    }
    if ebSQSSendMessageHook != nil {
        return ebSQSSendMessageHook(queue.ARN, payload, messageGroupID)
    }
    queueURL := fmt.Sprintf("%s/%s/%s", ebSQSEndpoint, queue.Account, queue.Queue)
    form := url.Values{}
    form.Set("Action", "SendMessage")
    form.Set("Version", "2012-11-05")
    form.Set("QueueUrl", queueURL)
    form.Set("MessageBody", payload)
    if messageGroupID != "" {
        form.Set("MessageGroupId", messageGroupID)
    }
    request, err := http.NewRequest(http.MethodPost, ebSQSEndpoint, strings.NewReader(form.Encode()))
    if err != nil {
        return err
    }
    request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
    response, err := (&http.Client{Timeout: ebHTTPTimeout}).Do(request)
    if err != nil {
        return err
    }
    defer response.Body.Close()
    if response.StatusCode >= 400 {
        return fmt.Errorf("SQS SendMessage returned HTTP %d", response.StatusCode)
    }
    return nil
}

func ebDeliverHTTP(endpoint, payload string) error {
    request, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(payload))
    if err != nil {
        return err
    }
    request.Header.Set("Content-Type", "application/json")
    request.Header.Set("User-Agent", "floci-go eventbridge")
    response, err := (&http.Client{Timeout: ebHTTPTimeout}).Do(request)
    if err != nil {
        return err
    }
    defer response.Body.Close()
    if response.StatusCode >= 400 {
        return fmt.Errorf("http POST returned HTTP %d", response.StatusCode)
    }
    return nil
}

// ---------------------------------------------------------------------------
// TestEventPattern (dry-run validation endpoint)
// ---------------------------------------------------------------------------

func ebHandleTestEventPattern(w http.ResponseWriter, r *http.Request) {
    body, err := ebReadBody(r)
    if err != nil {
        ebValidationException(w, "Unable to read request body")
        return
    }
    var req struct {
        EventPattern *string
        Event        *string
    }
    if err := json.Unmarshal(body, &req); err != nil {
        ebValidationException(w, fmt.Sprintf("Unable to parse request: %v", err))
        return
    }
    if req.EventPattern == nil {
        ebValidationException(w, "EventPattern is a required parameter")
        return
    }
    if req.Event == nil {
        ebValidationException(w, "Event is a required parameter")
        return
    }
    pattern, patternErr := ebParsePatternRequired(*req.EventPattern)
    if patternErr != nil {
        ebInvalidEventPattern(w, patternErr.Error())
        return
    }
    var event interface{}
    if err := json.Unmarshal([]byte(*req.Event), &event); err != nil {
        ebInvalidEventPattern(w, "Event is not valid JSON: "+err.Error())
        return
    }
    eventObj, ok := event.(map[string]interface{})
    if !ok {
        ebInvalidEventPattern(w, "Event must be a JSON object")
        return
    }
    matches := ebPatternMatches(pattern, eventObj)
    ebWriteJSON(w, http.StatusOK, map[string]interface{}{"Matches": matches})
}
