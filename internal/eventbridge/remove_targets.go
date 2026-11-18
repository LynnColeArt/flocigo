package eventbridge

// remove_targets.go — AWSEvents.RemoveTargets. Removing an unknown target
// id yields a per-entry failure; unknown rules are top-level errors. Force
// is accepted for compatibility (managed rules are not tracked here).

import (
    "fmt"
    "net/http"
)

type RemoveTargetsInput struct {
    EventBusName string   `json:"EventBusName"`
    Force        bool     `json:"Force"`
    Ids          []string `json:"Ids"`
    Rule         string   `json:"Rule"`
}

type RemoveTargetsOutput struct {
    FailedEntries    []*TargetListEntry `json:"FailedEntries"`
    FailedEntryCount int                `json:"FailedEntryCount"`
}

func (s *Service) RemoveTargets(in *RemoveTargetsInput) (*RemoveTargetsOutput, error) {
    if in == nil || in.Rule == "" {
        return nil, newValidationf("Rule is required.")
    }
    if len(in.Ids) == 0 {
        return nil, newValidationf("Ids is required.")
    }
    bus := normalizeBusName(in.EventBusName)
    if s.lookupRule(bus, in.Rule) == nil {
        return nil, newResourceNotFoundf("Rule %s not found.", ruleTargetARN(bus, in.Rule))
    }
    tt := s.targetTable()
    out := &RemoveTargetsOutput{FailedEntries: []*TargetListEntry{}}
    for _, id := range in.Ids {
        if !tt.remove(bus, in.Rule, id) {
            out.FailedEntries = append(out.FailedEntries, &TargetListEntry{
                TargetId:     id,
                ErrorCode:    ebCodeResourceNotFound,
                ErrorMessage: fmt.Sprintf("Target %s not found.", id),
            })
        }
    }
    out.FailedEntryCount = len(out.FailedEntries)
    return out, nil
}

func (s *Service) handleRemoveTargets(w http.ResponseWriter, r *http.Request) {
    var in RemoveTargetsInput
    if err := decodeEventBridgeRequest(r, &in); err != nil {
        writeEventBridgeError(w, newValidationf("Malformed request body: %v", err))
        return
    }
    out, err := s.RemoveTargets(&in)
    if err != nil {
        writeEventBridgeError(w, err)
        return
    }
    writeEventBridgeJSON(w, out)
}
