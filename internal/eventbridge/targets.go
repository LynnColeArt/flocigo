package eventbridge

// targets.go — rule target model, per-service target storage backing
// PutTargets / ListTargetsByRule / RemoveTargets, and the PutEvents ingress
// helpers (ticket flocigo/ticket-029).
//
// Target storage is deliberately self-contained so the target operations do
// not depend on ticket-003 store internals. The only shared surface is
// rule/bus lookup, isolated in the adapters below. They assume Service
// exposes a *Store field "store" with GetBus/GetRule/ListRules and that Rule
// carries Name, State ("ENABLED"/"DISABLED") and raw-JSON EventPattern. If
// ticket 003 named anything differently, only these adapters change.

import (
    "encoding/json"
    "fmt"
    "regexp"
    "sort"
    "strings"
    "sync"
)

const (
    // ingressRegion and ingressAccountID are used when synthesizing the
    // event envelope delivered to targets and in error messages.
    ingressRegion    = "us-east-1"
    ingressAccountID = "123456789012"

    // eventBusDefaultName is the bus used when EventBusName is omitted.
    eventBusDefaultName = "default"

    // maxTargetsPerRule mirrors the AWS limit enforced by PutTargets.
    maxTargetsPerRule = 5
)

func (s *Service) lookupRule(bus, name string) *Rule {
    r, ok := s.store.GetRule(bus, name)
    if !ok {
        return nil
    }
    return r
}

// busExists reports whether the named event bus exists; the default bus is
// always considered live.
func (s *Service) busExists(name string) bool {
    if name == eventBusDefaultName {
        return true
    }
    _, ok := s.store.GetBus(name)
    return ok
}

// busRules returns the rules attached to a bus in a stable (name-sorted)
// order so ingress matching is deterministic.
func (s *Service) busRules(bus string) []*Rule {
    rules := s.store.ListRules(bus)
    out := make([]*Rule, len(rules))
    copy(out, rules)
    sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
    return out
}

// ruleStateEnabled reports whether a ticket-003 rule is enabled; only
// enabled rules receive ingested events.
func ruleStateEnabled(r *Rule) bool {
    return strings.EqualFold(strings.TrimSpace(r.State), "ENABLED")
}

// normalizeBusName maps an optional EventBusName (plain name or bus ARN) to
// the store's bus key.
func normalizeBusName(name string) string {
    if strings.TrimSpace(name) == "" {
        return eventBusDefaultName
    }
    if strings.HasPrefix(name, "arn:aws:events:") {
        if i := strings.LastIndex(name, "eventbus/"); i >= 0 {
            return name[i+len("eventbus/"):]
        }
    }
    return name
}

// ruleTargetARN builds a rule ARN for the bus; used in error messages.
func ruleTargetARN(bus, rule string) string {
    if bus == eventBusDefaultName {
        return fmt.Sprintf("arn:aws:events:%s:%s:rule/%s", ingressRegion, ingressAccountID, rule)
    }
    return fmt.Sprintf("arn:aws:events:%s:%s:rule/%s/%s", ingressRegion, ingressAccountID, bus, rule)
}

// Target is an EventBridge rule target (model introduced by ticket 029; if
// ticket 004 already declared it, keep that declaration and drop this one).
type Target struct {
    Arn                    string                  `json:"Arn"`
    BatchParameters        *BatchParameters        `json:"BatchParameters,omitempty"`
    DeadLetterConfig       *DeadLetterConfig       `json:"DeadLetterConfig,omitempty"`
    EcsParameters          *EcsParameters          `json:"EcsParameters,omitempty"`
    HttpParameters         *HttpParameters         `json:"HttpParameters,omitempty"`
    Id                     string                  `json:"Id"`
    Input                  string                  `json:"Input,omitempty"`
    InputPath              string                  `json:"InputPath,omitempty"`
    InputTransformer       *InputTransformer       `json:"InputTransformer,omitempty"`
    KinesisParameters      *KinesisParameters      `json:"KinesisParameters,omitempty"`
    RedshiftDataParameters *RedshiftDataParameters `json:"RedshiftDataParameters,omitempty"`
    RetryPolicy            *RetryPolicy            `json:"RetryPolicy,omitempty"`
    RoleArn                string                  `json:"RoleArn,omitempty"`
    RunCommandParameters   *RunCommandParameters   `json:"RunCommandParameters,omitempty"`
    SqsParameters          *SqsParameters          `json:"SqsParameters,omitempty"`
}

type TargetListEntry struct {
    ErrorCode    string `json:"ErrorCode"`
    ErrorMessage string `json:"ErrorMessage"`
    TargetId     string `json:"TargetId"`
}

type InputTransformer struct {
    InputPathsMap map[string]string `json:"InputPathsMap,omitempty"`
    InputTemplate string            `json:"InputTemplate,omitempty"`
}

type SqsParameters struct {
    MessageGroupId string `json:"MessageGroupId,omitempty"`
}

type KinesisParameters struct {
    PartitionKeyPath string `json:"PartitionKeyPath,omitempty"`
}

type RetryPolicy struct {
    MaximumEventAgeInSeconds int64 `json:"MaximumEventAgeInSeconds,omitempty"`
    MaximumRetryAttempts     int64 `json:"MaximumRetryAttempts,omitempty"`
}

type DeadLetterConfig struct {
    Arn string `json:"Arn,omitempty"`
}

type HttpParameters struct {
    HeaderParameters      map[string]string `json:"HeaderParameters,omitempty"`
    PathParameterValues   []string          `json:"PathParameterValues,omitempty"`
    QueryStringParameters map[string]string `json:"QueryStringParameters,omitempty"`
}

type BatchParameters struct {
    ArrayProperties *BatchArrayProperties `json:"ArrayProperties,omitempty"`
    JobDefinition   string                `json:"JobDefinition,omitempty"`
    JobName         string                `json:"JobName,omitempty"`
    RetryStrategy   *BatchRetryStrategy   `json:"RetryStrategy,omitempty"`
}

type BatchArrayProperties struct {
    Size int64 `json:"Size,omitempty"`
}

type BatchRetryStrategy struct {
    Attempts int64 `json:"Attempts,omitempty"`
}

type EcsParameters struct {
    Group                string                `json:"Group,omitempty"`
    LaunchType           string                `json:"LaunchType,omitempty"`
    NetworkConfiguration *NetworkConfiguration `json:"NetworkConfiguration,omitempty"`
    TaskCount            int64                 `json:"TaskCount,omitempty"`
    TaskDefinitionArn    string                `json:"TaskDefinitionArn,omitempty"`
}

type NetworkConfiguration struct {
    AwsvpcConfiguration *AwsvpcConfiguration `json:"AwsvpcConfiguration,omitempty"`
}

type AwsvpcConfiguration struct {
    AssignPublicIp string   `json:"AssignPublicIp,omitempty"`
    SecurityGroups []string `json:"SecurityGroups,omitempty"`
    Subnets        []string `json:"Subnets,omitempty"`
}

type RunCommandParameters struct {
    RunCommandTargets []*RunCommandTarget `json:"RunCommandTargets,omitempty"`
}

type RunCommandTarget struct {
    Key    string   `json:"Key,omitempty"`
    Values []string `json:"Values,omitempty"`
}

type RedshiftDataParameters struct {
    Database       string `json:"Database,omitempty"`
    SecretArn      string `json:"SecretArn,omitempty"`
    Sql            string `json:"Sql,omitempty"`
    StatementName  string `json:"StatementName,omitempty"`
    WithEvent      bool   `json:"WithEvent,omitempty"`
    WorkgroupName  string `json:"WorkgroupName,omitempty"`
}

// targetTable stores per-rule targets keyed by bus + rule (separated by a
// NUL byte), preserving insertion order (ListTargetsByRule order).
type targetTable struct {
    mu     sync.Mutex
    byRule map[string][]*Target
}

func newTargetTable() *targetTable {
    return &targetTable{byRule: make(map[string][]*Target)}
}

func targetTableKey(bus, rule string) string {
    return bus + "\x00" + rule
}

// upsert inserts a target or replaces an existing one with the same Id in
// place.
func (tt *targetTable) upsert(bus, rule string, t *Target) {
    tt.mu.Lock()
    defer tt.mu.Unlock()
    key := targetTableKey(bus, rule)
    list := tt.byRule[key]
    for i, cur := range list {
        if cur.Id == t.Id {
            list[i] = t
            return
        }
    }
    tt.byRule[key] = append(list, t)
}

// list returns a copy of the rule's targets in insertion order.
func (tt *targetTable) list(bus, rule string) []*Target {
    tt.mu.Lock()
    defer tt.mu.Unlock()
    src := tt.byRule[targetTableKey(bus, rule)]
    if len(src) == 0 {
        return []*Target{}
    }
    out := make([]*Target, len(src))
    copy(out, src)
    return out
}

// remove deletes a target by Id and reports whether it existed.
func (tt *targetTable) remove(bus, rule, id string) bool {
    tt.mu.Lock()
    defer tt.mu.Unlock()
    key := targetTableKey(bus, rule)
    list := tt.byRule[key]
    for i, cur := range list {
        if cur.Id == id {
            tt.byRule[key] = append(list[:i:i], list[i+1:]...)
            return true
        }
    }
    return false
}

// serviceTargetTables keeps one target table per Service instance — for the
// lifetime of the service, exactly like rules in the ticket-003 store.
var serviceTargetTables sync.Map

func (s *Service) targetTable() *targetTable {
    if v, ok := serviceTargetTables.Load(s); ok {
        return v.(*targetTable)
    }
    v, _ := serviceTargetTables.LoadOrStore(s, newTargetTable())
    return v.(*targetTable)
}

var (
    targetIDPattern  = regexp.MustCompile(`^[\.\-_A-Za-z0-9]{1,64}$`)
    inputPathPattern = regexp.MustCompile(`^\$[a-zA-Z0-9_\.\-\[\]"']*$`)
)

// validateTargetEntry applies the per-entry PutTargets checks. It returns
// ("", "") when the target is acceptable, or (errorCode, message) for a
// failed entry.
func validateTargetEntry(t *Target) (string, string) {
    if t == nil {
        return ebCodeInvalidParameter, "Target must not be null."
    }
    if !targetIDPattern.MatchString(t.Id) {
        return ebCodeInvalidParameter, "Target " + t.Id + " is not valid: parameter Id is invalid."
    }
    if !strings.HasPrefix(t.Arn, "arn:") {
        return ebCodeInvalidParameter, "Target " + t.Id + " is not valid: parameter Arn is invalid."
    }
    forms := 0
    if t.Input != "" {
        forms++
    }
    if t.InputPath != "" {
        forms++
    }
    if t.InputTransformer != nil {
        forms++
    }
    if forms > 1 {
        return ebCodeInvalidParameter, "Target " + t.Id + " is not valid: only one of Input, InputPath or InputTransformer may be set."
    }
    if t.Input != "" && !json.Valid([]byte(t.Input)) {
        return ebCodeInvalidParameter, "Target " + t.Id + " is not valid: parameter Input must be valid JSON."
    }
    if t.InputPath != "" && !inputPathPattern.MatchString(t.InputPath) {
        return ebCodeInvalidParameter, "Target " + t.Id + " is not valid: parameter InputPath is invalid."
    }
    if it := t.InputTransformer; it != nil {
        if it.InputTemplate == "" {
            return ebCodeInvalidParameter, "Target " + t.Id + " is not valid: parameter InputTemplate is required."
        }
        for _, p := range it.InputPathsMap {
            if !inputPathPattern.MatchString(p) {
                return ebCodeInvalidParameter, "Target " + t.Id + " is not valid: parameter InputPathsMap is invalid."
            }
        }
        for _, ph := range templatePlaceholders(it.InputTemplate) {
            if _, ok := it.InputPathsMap[ph]; !ok {
                return ebCodeInvalidParameter, "Target " + t.Id + " is not valid: parameter InputTemplate has an unresolvable placeholder <" + ph + ">."
            }
        }
    }
    if k := t.KinesisParameters; k != nil && k.PartitionKeyPath != "" && !inputPathPattern.MatchString(k.PartitionKeyPath) {
        return ebCodeInvalidParameter, "Target " + t.Id + " is not valid: parameter PartitionKeyPath is invalid."
    }
    if rp := t.RetryPolicy; rp != nil {
        if rp.MaximumEventAgeInSeconds != 0 && (rp.MaximumEventAgeInSeconds < 60 || rp.MaximumEventAgeInSeconds > 86400) {
            return ebCodeInvalidParameter, "Target " + t.Id + " is not valid: parameter MaximumEventAgeInSeconds is invalid."
        }
        if rp.MaximumRetryAttempts < 0 || rp.MaximumRetryAttempts > 185 {
            return ebCodeInvalidParameter, "Target " + t.Id + " is not valid: parameter MaximumRetryAttempts is invalid."
        }
    }
    if dlc := t.DeadLetterConfig; dlc != nil && dlc.Arn != "" && !strings.HasPrefix(dlc.Arn, "arn:") {
        return ebCodeInvalidParameter, "Target " + t.Id + " is not valid: parameter DeadLetterConfig.Arn is invalid."
    }
    return "", ""
}

var templatePlaceholderMatcher = regexp.MustCompile(`<([A-Za-z0-9_\-]+)>`)

// templatePlaceholders returns the <placeholder> keys an InputTransformer
// template references.
func templatePlaceholders(template string) []string {
    var out []string
    for _, m := range templatePlaceholderMatcher.FindAllStringSubmatch(template, -1) {
        out = append(out, m[1])
    }
    return out
}
