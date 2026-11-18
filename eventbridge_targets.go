// floci-go - EventBridge module.
//
// eventbridge_targets.go implements the target-management endpoints:
// PutTargets, ListTargetsByRule and RemoveTargets, backed by an in-memory
// target store keyed by rule ARN. Wire shapes follow the AWS events API
// (application/x-amz-json-1.1), including FailedEntries/FailedEntryCount
// semantics and the opaque base64 NextToken used for pagination.
package main

import (
    "encoding/base64"
    "encoding/json"
    "fmt"
    "net/http"
    "strconv"
    "strings"
    "sync"
    "time"
)

// ---------------------------------------------------------------------------
// Target model + store
// ---------------------------------------------------------------------------

type ebInputTransformer struct {
    InputPathsMap map[string]string
    InputTemplate string
}

type ebSqsParameters struct {
    MessageGroupId string
}

type ebDeadLetterConfig struct {
    Arn string
}

type ebRetryPolicy struct {
    MaximumRetryAttempts     int64
    MaximumEventAgeInSeconds int64
}

type ebTarget struct {
    Id               string
    Arn              string
    RoleArn          string
    Input            *string
    InputPath        *string
    InputTransformer *ebInputTransformer
    SqsParameters    *ebSqsParameters
    DeadLetterConfig *ebDeadLetterConfig
    RetryPolicy      ebRetryPolicy
    CreatedAt        time.Time
}

var (
    ebTargetMu     sync.RWMutex
    ebTargetsByARN = map[string][]*ebTarget{} // rule ARN -> targets in insertion order
)

func ebTargetStoreSnapshot(ruleARN string) []ebTarget {
    ebTargetMu.RLock()
    defer ebTargetMu.RUnlock()
    targets := ebTargetsByARN[ruleARN]
    out := make([]ebTarget, 0, len(targets))
    for _, target := range targets {
        out = append(out, *target)
    }
    return out
}

func ebTargetStorePut(ruleARN string, target *ebTarget) {
    ebTargetMu.Lock()
    defer ebTargetMu.Unlock()
    targets := ebTargetsByARN[ruleARN]
    for i, existing := range targets {
        if existing.Id == target.Id {
            targets[i] = target // upsert keeps the original position
            return
        }
    }
    ebTargetsByARN[ruleARN] = append(targets, target)
}

func ebTargetStoreDelete(ruleARN, id string) bool {
    ebTargetMu.Lock()
    defer ebTargetMu.Unlock()
    targets := ebTargetsByARN[ruleARN]
    for i, existing := range targets {
        if existing.Id == id {
            ebTargetsByARN[ruleARN] = append(targets[:i:i], targets[i+1:]...)
            return true
        }
    }
    return false
}

// ---------------------------------------------------------------------------
// Wire shapes (field names match the AWS events API; encoding/json matches
// them case-insensitively so no tags are required)
// ---------------------------------------------------------------------------

type ebFailedEntry struct {
    TargetId     string
    ErrorCode    string
    ErrorMessage string
}

type ebTargetIn struct {
    Id               string
    Arn              string
    RoleArn          *string
    Input            *string
    InputPath        *string
    InputTransformer *ebInputTransformerIn
    SqsParameters    *ebSqsParametersIn
    DeadLetterConfig *ebDeadLetterConfigIn
    RetryPolicy      *ebRetryPolicyIn
}

type ebInputTransformerIn struct {
    InputPathsMap map[string]string
    InputTemplate *string
}

type ebSqsParametersIn struct {
    MessageGroupId *string
}

type ebDeadLetterConfigIn struct {
    Arn *string
}

type ebRetryPolicyIn struct {
    MaximumRetryAttempts     *int64
    MaximumEventAgeInSeconds *int64
}

type ebTargetOut struct {
    Id               string
    Arn              string
    RoleArn          *string
    Input            *string
    InputPath        *string
    InputTransformer *ebInputTransformerOut
    SqsParameters    *ebSqsParametersIn
    DeadLetterConfig *ebDeadLetterConfigIn
    RetryPolicy      *ebRetryPolicyIn
}

type ebInputTransformerOut struct {
    InputPathsMap map[string]string
    InputTemplate string
}

func ebTargetToWire(target *ebTarget) *ebTargetOut {
    out := &ebTargetOut{
        Id:  target.Id,
        Arn: target.Arn,
        RetryPolicy: &ebRetryPolicyIn{
            MaximumRetryAttempts:     ebInt64Ptr(target.RetryPolicy.MaximumRetryAttempts),
            MaximumEventAgeInSeconds: ebInt64Ptr(target.RetryPolicy.MaximumEventAgeInSeconds),
        },
    }
    if target.RoleArn != "" {
        out.RoleArn = ebStrPtr(target.RoleArn)
    }
    out.Input = ebCloneStrPtr(target.Input)
    out.InputPath = ebCloneStrPtr(target.InputPath)
    if target.InputTransformer != nil {
        paths := make(map[string]string, len(target.InputTransformer.InputPathsMap))
        for k, v := range target.InputTransformer.InputPathsMap {
            paths[k] = v
        }
        out.InputTransformer = &ebInputTransformerOut{InputPathsMap: paths, InputTemplate: target.InputTransformer.InputTemplate}
    }
    if target.SqsParameters != nil {
        out.SqsParameters = &ebSqsParametersIn{MessageGroupId: ebStrPtr(target.SqsParameters.MessageGroupId)}
    }
    if target.DeadLetterConfig != nil {
        out.DeadLetterConfig = &ebDeadLetterConfigIn{Arn: ebStrPtr(target.DeadLetterConfig.Arn)}
    }
    return out
}

func ebStrPtr(s string) *string   { return &s }
func ebInt64Ptr(v int64) *int64   { return &v }
func ebCloneStrPtr(p *string) *string {
    if p == nil {
        return nil
    }
    value := *p
    return &value
}

// ---------------------------------------------------------------------------
// PutTargets
// ---------------------------------------------------------------------------

// PutTargets creates or updates targets on an existing rule.
func ebHandlePutTargets(w http.ResponseWriter, r *http.Request) {
    body, err := ebReadBody(r)
    if err != nil {
        ebValidationException(w, "Unable to read request body")
        return
    }
    var req struct {
        Rule         *string       `json:"Rule"`
        EventBusName string        `json:"EventBusName"`
        Targets      []*ebTargetIn
    }
    if err := json.Unmarshal(body, &req); err != nil {
        ebValidationException(w, fmt.Sprintf("Unable to parse request: %v", err))
        return
    }
    if req.Rule == nil || *req.Rule == "" {
        ebValidationException(w, "Rule is a required parameter")
        return
    }
    if len(req.Targets) == 0 {
        ebValidationException(w, "Targets is a required parameter and must not be empty")
        return
    }
    if len(req.Targets) > 100 {
        ebValidationException(w, "Targets member must have length less than or equal to 100")
        return
    }
    bus := ebBusOrDefault(req.EventBusName)
    snapshot, found := EventBridgeRuleLookup(bus, *req.Rule)
    if !found {
        ebResourceNotFound(w, fmt.Sprintf("Rule %s does not exist on EventBusName %s", *req.Rule, bus))
        return
    }
    ruleARN := ebEffectiveRuleARN(snapshot, bus, *req.Rule)

    for _, targetIn := range req.Targets {
        if err := ebValidateTargetIn(targetIn); err != nil {
            ebValidationException(w, err.Error())
            return
        }
    }
    for _, targetIn := range req.Targets {
        ebTargetStorePut(ruleARN, ebNormalizeTarget(targetIn))
    }
    ebWriteJSON(w, http.StatusOK, map[string]interface{}{
        "FailedEntryCount": 0,
        "FailedEntries":    []ebFailedEntry{},
    })
}

func ebValidateTargetIn(target *ebTargetIn) error {
    if target == nil {
        return fmt.Errorf("Targets member must not be null")
    }
    if err := ebValidateTargetId(target.Id); err != nil {
        return err
    }
    if target.Arn == "" {
        return fmt.Errorf(" Arn is a required parameter of target '%s'", target.Id)
    }
    if err := ebValidateTargetArn(target.Arn); err != nil {
        return fmt.Errorf("Invalid Arn on target '%s': %v", target.Id, err)
    }
    if target.Input != nil && target.InputPath != nil {
        return fmt.Errorf("Target '%s': only one of Input, InputPath or InputTransformer may be specified", target.Id)
    }
    if target.Input != nil && target.InputTransformer != nil {
        return fmt.Errorf("Target '%s': only one of Input, InputPath or InputTransformer may be specified", target.Id)
    }
    if target.InputPath != nil && target.InputTransformer != nil {
        return fmt.Errorf("Target '%s': only one of Input, InputPath or InputTransformer may be specified", target.Id)
    }
    if target.InputPath != nil && !strings.HasPrefix(strings.TrimSpace(*target.InputPath), "$") {
        return fmt.Errorf("Target '%s': InputPath must be a JSONPath expression starting with '$'", target.Id)
    }
    if target.InputTransformer != nil {
        if err := ebValidateInputTransformer(target.InputTransformer); err != nil {
            return fmt.Errorf("Target '%s': %v", target.Id, err)
        }
    }
    if target.DeadLetterConfig != nil && (target.DeadLetterConfig.Arn == nil || *target.DeadLetterConfig.Arn == "") {
        return fmt.Errorf("Target '%s': DeadLetterConfig requires an Arn", target.Id)
    }
    if target.RetryPolicy != nil {
        if v := target.RetryPolicy.MaximumRetryAttempts; v != nil && (*v < 0 || *v > 185) {
            return fmt.Errorf("Target '%s': MaximumRetryAttempts must be between 0 and 185", target.Id)
        }
        if v := target.RetryPolicy.MaximumEventAgeInSeconds; v != nil && (*v < 60 || *v > 86400) {
            return fmt.Errorf("Target '%s': MaximumEventAgeInSeconds must be between 60 and 86400", target.Id)
        }
    }
    return nil
}

func ebValidateTargetId(id string) error {
    if id == "" {
        return fmt.Errorf("Id is a required parameter")
    }
    if len(id) > 64 {
        return fmt.Errorf("Id must be 64 characters or fewer")
    }
    for _, c := range id {
        if !(c == '.' || c == '-' || c == '_' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
            return fmt.Errorf("Id contains an invalid character (allowed: a-z, A-Z, 0-9, '.', '-', '_')")
        }
    }
    return nil
}

// ebValidateTargetArn accepts AWS ARNs and raw endpoint URLs; the emulator
// dispatches http(s) targets directly.
func ebValidateTargetArn(arn string) error {
    if strings.HasPrefix(arn, "http://") || strings.HasPrefix(arn, "https://") {
        return nil
    }
    parts := strings.Split(arn, ":")
    if len(parts) < 6 || parts[0] != "arn" {
        return fmt.Errorf("must be an ARN or an http(s) endpoint")
    }
    return nil
}

func ebValidateInputTransformer(transformer *ebInputTransformerIn) error {
    if transformer.InputTemplate == nil || *transformer.InputTemplate == "" {
        return fmt.Errorf("InputTemplate is a required parameter of InputTransformer")
    }
    template := *transformer.InputTemplate
    if len(template) > 8192 {
        return fmt.Errorf("InputTemplate must be 8192 bytes or fewer")
    }
    for _, placeholder := range ebPlaceholders(template) {
        if _, ok := transformer.InputPathsMap[placeholder]; !ok {
            return fmt.Errorf("InputTemplate uses placeholder <%s> that is missing from InputPathsMap", placeholder)
        }
    }
    for key, path := range transformer.InputPathsMap {
        if !strings.HasPrefix(strings.TrimSpace(path), "$") {
            return fmt.Errorf("InputPathsMap value for key '%s' must be a JSONPath expression starting with '$'", key)
        }
    }
    return nil
}

// ebPlaceholders extracts the <key> placeholders of an InputTemplate.
func ebPlaceholders(template string) []string {
    var placeholders []string
    for i := 0; i < len(template); i++ {
        if template[i] != '<' {
            continue
        }
        closing := strings.IndexByte(template[i:], '>')
        if closing < 2 {
            continue
        }
        key := template[i+1 : i+closing]
        if !ebIsPlaceholderKey(key) {
            continue
        }
        placeholders = append(placeholders, key)
        i += closing
    }
    return placeholders
}

func ebIsPlaceholderKey(key string) bool {
    if key == "" {
        return false
    }
    for _, c := range key {
        if !(c == '_' || c == '-' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
            return false
        }
    }
    return true
}

func ebNormalizeTarget(target *ebTargetIn) *ebTarget {
    normalized := &ebTarget{
        Id:        target.Id,
        Arn:       target.Arn,
        CreatedAt: time.Now().UTC(),
        RetryPolicy: ebRetryPolicy{
            MaximumRetryAttempts:     185,
            MaximumEventAgeInSeconds: 86400,
        },
    }
    if target.RoleArn != nil {
        normalized.RoleArn = *target.RoleArn
    }
    if target.Input != nil {
        value := *target.Input
        normalized.Input = &value
    }
    if target.InputPath != nil {
        value := *target.InputPath
        normalized.InputPath = &value
    }
    if target.InputTransformer != nil {
        paths := make(map[string]string, len(target.InputTransformer.InputPathsMap))
        for k, v := range target.InputTransformer.InputPathsMap {
            paths[k] = v
        }
        template := ""
        if target.InputTransformer.InputTemplate != nil {
            template = *target.InputTransformer.InputTemplate
        }
        normalized.InputTransformer = &ebInputTransformer{InputPathsMap: paths, InputTemplate: template}
    }
    if target.SqsParameters != nil && target.SqsParameters.MessageGroupId != nil {
        normalized.SqsParameters = &ebSqsParameters{MessageGroupId: *target.SqsParameters.MessageGroupId}
    }
    if target.DeadLetterConfig != nil && target.DeadLetterConfig.Arn != nil {
        normalized.DeadLetterConfig = &ebDeadLetterConfig{Arn: *target.DeadLetterConfig.Arn}
    }
    if target.RetryPolicy != nil {
        if target.RetryPolicy.MaximumRetryAttempts != nil {
            normalized.RetryPolicy.MaximumRetryAttempts = *target.RetryPolicy.MaximumRetryAttempts
        }
        if target.RetryPolicy.MaximumEventAgeInSeconds != nil {
            normalized.RetryPolicy.MaximumEventAgeInSeconds = *target.RetryPolicy.MaximumEventAgeInSeconds
        }
    }
    return normalized
}

// ---------------------------------------------------------------------------
// ListTargetsByRule
// ---------------------------------------------------------------------------

// ListTargetsByRule returns the targets attached to a rule (paginated).
func ebHandleListTargetsByRule(w http.ResponseWriter, r *http.Request) {
    body, err := ebReadBody(r)
    if err != nil {
        ebValidationException(w, "Unable to read request body")
        return
    }
    var req struct {
        Rule         *string
        EventBusName string
        Limit        *int64
        NextToken    *string
    }
    if err := json.Unmarshal(body, &req); err != nil {
        ebValidationException(w, fmt.Sprintf("Unable to parse request: %v", err))
        return
    }
    if req.Rule == nil || *req.Rule == "" {
        ebValidationException(w, "Rule is a required parameter")
        return
    }
    bus := ebBusOrDefault(req.EventBusName)
    snapshot, found := EventBridgeRuleLookup(bus, *req.Rule)
    if !found {
        ebResourceNotFound(w, fmt.Sprintf("Rule %s does not exist on EventBusName %s", *req.Rule, bus))
        return
    }
    ruleARN := ebEffectiveRuleARN(snapshot, bus, *req.Rule)

    limit := 100
    if req.Limit != nil {
        if *req.Limit < 1 {
            ebValidationException(w, "Limit must be greater than or equal to 1")
            return
        }
        if *req.Limit > 100 {
            ebValidationException(w, "Limit must be less than or equal to 100")
            return
        }
        limit = int(*req.Limit)
    }
    start := 0
    if req.NextToken != nil && strings.TrimSpace(*req.NextToken) != "" {
        offset, tokenErr := ebDecodeTargetsNextToken(*req.NextToken, bus, *req.Rule)
        if tokenErr != nil {
            ebValidationException(w, "Invalid NextToken: "+*req.NextToken)
            return
        }
        start = offset
    }
    stored := ebTargetStoreSnapshot(ruleARN)
    targets := make([]ebTargetOut, 0, len(stored))
    for i := range stored {
        targets = append(targets, *ebTargetToWire(&stored[i]))
    }
    if start > len(targets) {
        start = len(targets)
    }
    end := start + limit
    if end > len(targets) {
        end = len(targets)
    }
    resp := map[string]interface{}{"Targets": targets[start:end]}
    if end < len(targets) {
        resp["NextToken"] = ebEncodeTargetsNextToken(bus, *req.Rule, end)
    }
    ebWriteJSON(w, http.StatusOK, resp)
}

func ebEncodeTargetsNextToken(bus, rule string, offset int) string {
    raw := fmt.Sprintf("%s|%s|%d", bus, rule, offset)
    return base64.StdEncoding.EncodeToString([]byte(raw))
}

func ebDecodeTargetsNextToken(token, bus, rule string) (int, error) {
    raw, err := base64.StdEncoding.DecodeString(token)
    if err != nil {
        return 0, err
    }
    parts := strings.Split(string(raw), "|")
    if len(parts) != 3 || parts[0] != bus || parts[1] != rule {
        return 0, fmt.Errorf("stale NextToken")
    }
    offset, err := strconv.Atoi(parts[2])
    if err != nil || offset < 0 {
        return 0, fmt.Errorf("invalid offset")
    }
    return offset, nil
}

// ---------------------------------------------------------------------------
// RemoveTargets
// ---------------------------------------------------------------------------

// RemoveTargets removes targets from a rule by id. The Force parameter
// (managed rules) is accepted but does not change emulator behavior.
func ebHandleRemoveTargets(w http.ResponseWriter, r *http.Request) {
    body, err := ebReadBody(r)
    if err != nil {
        ebValidationException(w, "Unable to read request body")
        return
    }
    var req struct {
        Rule         *string
        EventBusName string
        Force        bool
        Ids          []string
    }
    if err := json.Unmarshal(body, &req); err != nil {
        ebValidationException(w, fmt.Sprintf("Unable to parse request: %v", err))
        return
    }
    if req.Rule == nil || *req.Rule == "" {
        ebValidationException(w, "Rule is a required parameter")
        return
    }
    if len(req.Ids) == 0 {
        ebValidationException(w, "Ids is a required parameter and must not be empty")
        return
    }
    bus := ebBusOrDefault(req.EventBusName)
    snapshot, found := EventBridgeRuleLookup(bus, *req.Rule)
    if !found {
        ebResourceNotFound(w, fmt.Sprintf("Rule %s does not exist on EventBusName %s", *req.Rule, bus))
        return
    }
    ruleARN := ebEffectiveRuleARN(snapshot, bus, *req.Rule)

    failedEntries := []ebFailedEntry{}
    for _, id := range req.Ids {
        if err := ebValidateTargetId(id); err != nil {
            ebValidationException(w, err.Error())
            return
        }
    }
    for _, id := range req.Ids {
        if !ebTargetStoreDelete(ruleARN, id) {
            failedEntries = append(failedEntries, ebFailedEntry{
                TargetId:     id,
                ErrorCode:    "ErrTargetNotFound",
                ErrorMessage: fmt.Sprintf("Target with id '%s' was not found on rule '%s'", id, *req.Rule),
            })
        }
    }
    ebWriteJSON(w, http.StatusOK, map[string]interface{}{
        "FailedEntryCount": len(failedEntries),
        "FailedEntries":    failedEntries,
    })
}
