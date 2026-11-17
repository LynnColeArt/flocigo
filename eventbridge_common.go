// floci-go - EventBridge module (continuation of tickets 003 + 004).
//
// eventbridge_common.go holds the configuration, the rule-registry hooks and
// the shared helpers used across this module:
//
//     eventbridge_pattern.go - EventBridge event-pattern validation + matching
//     eventbridge_targets.go - PutTargets / ListTargetsByRule / RemoveTargets
//     eventbridge_events.go  - PutEvents ingress, TestEventPattern, dispatcher
//
// Integration: the bus/rule state itself is owned by the modules created in
// tickets 003/004. Wire it in during bootstrap by reassigning the three hook
// variables below (EventBridgeRuleLookup, EventBridgeRulesOnBus,
// EventBridgeBusExists). The defaults use the local fallback registry at the
// bottom of this file (populated via EBAttachLocalRule) so the endpoints are
// fully functional in isolation, e.g. in unit tests.
package main

import (
    "crypto/rand"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "sort"
    "strings"
    "sync"
    "time"
)

// ---------------------------------------------------------------------------
// Configuration (override during bootstrap to match the deployment)
// ---------------------------------------------------------------------------

var (
    ebRegion    = ebEnvOr("FLOCI_REGION", "us-east-1")
    ebAccountID = ebEnvOr("FLOCI_ACCOUNT_ID", "123456789012")

    // ebSQSEndpoint is used when delivering to SQS targets over the wire. It
    // is only relevant when the SQS module has not installed the in-process
    // hook; see ebSQSSendMessageHook in eventbridge_events.go.
    ebSQSEndpoint = strings.TrimRight(ebEnvOr("FLOCI_ENDPOINT", "http://127.0.0.1:4566"), "/")

    ebHTTPTimeout = 2 * time.Second
)

func ebEnvOr(key, fallback string) string {
    if value := os.Getenv(key); value != "" {
        return value
    }
    return fallback
}

// ---------------------------------------------------------------------------
// Rule-registry hooks (integration point with tickets 003/004)
// ---------------------------------------------------------------------------

// EBRuleSnapshot is the minimal rule view the targets/ingress code needs.
type EBRuleSnapshot struct {
    ARN                string
    Name               string
    EventBusName       string
    State              string // "ENABLED" / "DISABLED"
    ScheduleExpression string // non-empty for cron()/rate() rules
    EventPattern       string // raw pattern JSON, empty when the rule has none
}

// Rule-store hooks. Reassign these during bootstrap, e.g.:
//
//     EventBridgeRuleLookup = ruleStore.Lookup
//     EventBridgeRulesOnBus = ruleStore.RulesOnBus
//     EventBridgeBusExists  = busStore.Exists
var (
    EventBridgeRuleLookup = func(eventBusName, ruleName string) (EBRuleSnapshot, bool) {
        return ebLocalRuleLookup(eventBusName, ruleName)
    }
    EventBridgeRulesOnBus = func(eventBusName string) []EBRuleSnapshot {
        return ebLocalRulesOnBus(eventBusName)
    }
    EventBridgeBusExists = func(eventBusName string) bool {
        return ebLocalBusExists(eventBusName)
    }
)

func ebBusOrDefault(eventBusName string) string {
    if eventBusName == "" {
        return "default"
    }
    return eventBusName
}

// ebRuleARN mirrors the ARN scheme of the rules module (AWS compatible).
func ebRuleARN(eventBusName, ruleName string) string {
    if ebBusOrDefault(eventBusName) == "default" {
        return fmt.Sprintf("arn:aws:events:%s:%s:rule/%s", ebRegion, ebAccountID, ruleName)
    }
    return fmt.Sprintf("arn:aws:events:%s:%s:rule/%s/%s", ebRegion, ebAccountID, eventBusName, ruleName)
}

// ebEffectiveRuleARN prefers the ARN published by the rule store and falls
// back to the canonical reconstruction.
func ebEffectiveRuleARN(snapshot EBRuleSnapshot, eventBusName, ruleName string) string {
    if snapshot.ARN != "" {
        return snapshot.ARN
    }
    return ebRuleARN(eventBusName, ruleName)
}

// ---------------------------------------------------------------------------
// awsjson1.1 helpers (application/x-amz-json-1.1 wire encoding)
// ---------------------------------------------------------------------------

func ebWriteJSON(w http.ResponseWriter, status int, payload interface{}) {
    w.Header().Set("Content-Type", "application/x-amz-json-1.1")
    w.WriteHeader(status)
    _ = json.NewEncoder(w).Encode(payload)
}

func ebFail(w http.ResponseWriter, status int, code, message string) {
    ebWriteJSON(w, status, map[string]string{"__type": code, "message": message})
}

func ebValidationException(w http.ResponseWriter, message string) {
    ebFail(w, http.StatusBadRequest, "ValidationException", message)
}

func ebResourceNotFound(w http.ResponseWriter, message string) {
    ebFail(w, http.StatusBadRequest, "ResourceNotFoundException", message)
}

func ebInvalidEventPattern(w http.ResponseWriter, message string) {
    ebFail(w, http.StatusBadRequest, "InvalidEventPatternException", message)
}

func ebReadBody(r *http.Request) ([]byte, error) {
    return io.ReadAll(io.LimitReader(r.Body, 10<<20))
}

// ---------------------------------------------------------------------------
// Small shared helpers
// ---------------------------------------------------------------------------

// ebNewEventID returns a random v4-style EventBridge event id.
func ebNewEventID() string {
    var b [16]byte
    _, _ = rand.Read(b[:])
    b[6] = (b[6] & 0x0f) | 0x40
    b[8] = (b[8] & 0x3f) | 0x80
    return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// ebRFC3339 formats timestamps the way EventBridge stamps deliveries.
func ebRFC3339(t time.Time) string {
    return t.UTC().Format("2006-01-02T15:04:05.000Z07:00")
}

// ---------------------------------------------------------------------------
// Action registry / router helpers
// ---------------------------------------------------------------------------

// EventBridgeActionHandlers returns the handlers of this module keyed by the
// bare action name; the router prefixes them with the AWSEvents. namespace.
func EventBridgeActionHandlers() map[string]http.HandlerFunc {
    return map[string]http.HandlerFunc{
        "PutTargets":        ebHandlePutTargets,
        "ListTargetsByRule": ebHandleListTargetsByRule,
        "RemoveTargets":     ebHandleRemoveTargets,
        "PutEvents":         ebHandlePutEvents,
        "TestEventPattern":  ebHandleTestEventPattern,
    }
}

// MountEventBridgeActions registers the handlers under /AWSEvents.<Action> on
// a plain mux (convenience for integration points that expose one).
func MountEventBridgeActions(mux *http.ServeMux) {
    for action, handler := range EventBridgeActionHandlers() {
        mux.HandleFunc("/AWSEvents."+action, handler)
    }
}

// ---------------------------------------------------------------------------
// Local fallback rule registry (active while the ticket 003/004 hooks are not
// wired; also used by unit tests)
// ---------------------------------------------------------------------------

var (
    ebLocalMu    sync.RWMutex
    ebLocalBuses = map[string]struct{}{"default": {}}
    ebLocalRules = map[string]map[string]EBRuleSnapshot{} // bus -> rule name -> snapshot
)

// EBAttachLocalRule registers/updates a rule in the fallback registry and
// returns its canonical ARN.
func EBAttachLocalRule(eventBusName, ruleName, eventPattern, scheduleExpression, state string) string {
    bus := ebBusOrDefault(eventBusName)
    snapshot := EBRuleSnapshot{
        ARN:                ebRuleARN(bus, ruleName),
        Name:               ruleName,
        EventBusName:       bus,
        State:              state,
        ScheduleExpression: scheduleExpression,
        EventPattern:       eventPattern,
    }
    if snapshot.State == "" {
        snapshot.State = "ENABLED"
    }
    ebLocalMu.Lock()
    defer ebLocalMu.Unlock()
    ebLocalBuses[bus] = struct{}{}
    byName, ok := ebLocalRules[bus]
    if !ok {
        byName = map[string]EBRuleSnapshot{}
        ebLocalRules[bus] = byName
    }
    byName[ruleName] = snapshot
    return snapshot.ARN
}

func ebLocalRuleLookup(eventBusName, ruleName string) (EBRuleSnapshot, bool) {
    ebLocalMu.RLock()
    defer ebLocalMu.RUnlock()
    byName, ok := ebLocalRules[ebBusOrDefault(eventBusName)]
    if !ok {
        return EBRuleSnapshot{}, false
    }
    snapshot, ok := byName[ruleName]
    return snapshot, ok
}

func ebLocalRulesOnBus(eventBusName string) []EBRuleSnapshot {
    ebLocalMu.RLock()
    defer ebLocalMu.RUnlock()
    byName := ebLocalRules[ebBusOrDefault(eventBusName)]
    snapshots := make([]EBRuleSnapshot, 0, len(byName))
    for name := range byName {
        snapshots = append(snapshots, byName[name])
    }
    sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].Name < snapshots[j].Name })
    return snapshots
}

func ebLocalBusExists(eventBusName string) bool {
    ebLocalMu.RLock()
    defer ebLocalMu.RUnlock()
    _, ok := ebLocalBuses[ebBusOrDefault(eventBusName)]
    return ok
}
