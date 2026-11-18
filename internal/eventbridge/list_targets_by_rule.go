package eventbridge

// list_targets_by_rule.go — AWSEvents.ListTargetsByRule with simple integer
// pagination (NextToken is the offset into the rule's target list).

import (
    "net/http"
    "strconv"
)

type ListTargetsByRuleInput struct {
    EventBusName string `json:"EventBusName"`
    Limit        int64  `json:"Limit"`
    NextToken    string `json:"NextToken"`
    Rule         string `json:"Rule"`
}

type ListTargetsByRuleOutput struct {
    NextToken string    `json:"NextToken,omitempty"`
    Targets   []*Target `json:"Targets"`
}

const defaultTargetPageSize = 100

func (s *Service) ListTargetsByRule(in *ListTargetsByRuleInput) (*ListTargetsByRuleOutput, error) {
    if in == nil || in.Rule == "" {
        return nil, newValidationf("Rule is required.")
    }
    bus := normalizeBusName(in.EventBusName)
    if s.lookupRule(bus, in.Rule) == nil {
        return nil, newResourceNotFoundf("Rule %s not found.", ruleTargetARN(bus, in.Rule))
    }
    all := s.targetTable().list(bus, in.Rule)

    offset := 0
    if in.NextToken != "" {
        if n, err := strconv.Atoi(in.NextToken); err == nil && n > 0 {
            offset = n
        }
    }
    limit := int64(defaultTargetPageSize)
    if in.Limit > 0 {
        limit = in.Limit
        if limit > int64(defaultTargetPageSize) {
            limit = int64(defaultTargetPageSize)
        }
    }
    if offset > len(all) {
        offset = len(all)
    }
    end := offset + int(limit)
    if end > len(all) {
        end = len(all)
    }
    page := all[offset:end]

    out := &ListTargetsByRuleOutput{Targets: make([]*Target, 0, len(page))}
    out.Targets = append(out.Targets, page...)
    if end < len(all) {
        out.NextToken = strconv.Itoa(end)
    }
    return out, nil
}

func (s *Service) handleListTargetsByRule(w http.ResponseWriter, r *http.Request) {
    var in ListTargetsByRuleInput
    if err := decodeEventBridgeRequest(r, &in); err != nil {
        writeEventBridgeError(w, newValidationf("Malformed request body: %v", err))
        return
    }
    out, err := s.ListTargetsByRule(&in)
    if err != nil {
        writeEventBridgeError(w, err)
        return
    }
    writeEventBridgeJSON(w, out)
}
