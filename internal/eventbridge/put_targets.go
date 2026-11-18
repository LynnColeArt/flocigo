package eventbridge

// put_targets.go — AWSEvents.PutTargets: attach or replace targets on an
// existing rule. Per-entry problems are reported in FailedEntries;
// rule/bus-level problems are top-level API errors.

import (
    "net/http"
)

type PutTargetsInput struct {
    EventBusName string    `json:"EventBusName"`
    Rule         string    `json:"Rule"`
    Targets      []*Target `json:"Targets"`
}

type PutTargetsOutput struct {
    FailedEntries    []*TargetListEntry `json:"FailedEntries"`
    FailedEntryCount int                `json:"FailedEntryCount"`
}

// PutTargets validates and stores targets for a rule. Re-putting an
// existing TargetId replaces the target in place.
func (s *Service) PutTargets(in *PutTargetsInput) (*PutTargetsOutput, error) {
    if in == nil || in.Rule == "" {
        return nil, newValidationf("Rule is required.")
    }
    if len(in.Targets) == 0 {
        return nil, newValidationf("Targets is required.")
    }
    bus := normalizeBusName(in.EventBusName)
    if s.lookupRule(bus, in.Rule) == nil {
        return nil, newResourceNotFoundf("Rule %s not found.", ruleTargetARN(bus, in.Rule))
    }
    tt := s.targetTable()
    existing := tt.list(bus, in.Rule)
    existingIDs := make(map[string]struct{}, len(existing))
    for _, cur := range existing {
        existingIDs[cur.ID] = struct{}{}
    }

    out := &PutTargetsOutput{FailedEntries: []*TargetListEntry{}}
    accepted := make([]*Target, 0, len(in.Targets))
    seen := make(map[string]struct{})
    newIDs := make(map[string]struct{})

    for _, t := range in.Targets {
        var id string
        if t != nil {
            id = t.ID
        }
        if code, msg := validateTargetEntry(t); code != "" {
            out.FailedEntries = append(out.FailedEntries, &TargetListEntry{
                TargetId:     id,
                ErrorCode:    code,
                ErrorMessage: msg,
            })
            continue
        }
        if _, dup := seen[t.ID]; dup {
            continue // a repeated Id within one call is processed once
        }
        seen[t.ID] = struct{}{}
        if _, known := existingIDs[t.ID]; !known {
            newIDs[t.ID] = struct{}{}
        }
        accepted = append(accepted, t)
    }

    if len(existingIDs)+len(newIDs) > maxTargetsPerRule {
        return nil, newLimitExceededf("At most %d targets can be attached to a rule.", maxTargetsPerRule)
    }
    for _, t := range accepted {
        tt.upsert(bus, in.Rule, t)
    }
    out.FailedEntryCount = len(out.FailedEntries)
    return out, nil
}

func (s *Service) handlePutTargets(w http.ResponseWriter, r *http.Request) {
    var in PutTargetsInput
    if err := decodeEventBridgeRequest(r, &in); err != nil {
        writeEventBridgeError(w, newValidationf("Malformed request body: %v", err))
        return
    }
    out, err := s.PutTargets(&in)
    if err != nil {
        writeEventBridgeError(w, err)
        return
    }
    writeEventBridgeJSON(w, out)
}
