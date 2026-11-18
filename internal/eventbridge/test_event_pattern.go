package eventbridge

// test_event_pattern.go — AWSEvents.TestEventPattern: the dry-run endpoint.
// It validates a pattern (InvalidEventPatternException with a reason on
// failure) and reports whether a sample event matches, without touching any
// rule, target or transport.

import (
    "encoding/json"
    "net/http"
    "strings"
)

type TestEventPatternInput struct {
    Event        string `json:"Event"`
    EventPattern string `json:"EventPattern"`
}

type TestEventPatternOutput struct {
    Event bool `json:"Event"`
}

func (s *Service) TestEventPattern(in *TestEventPatternInput) (*TestEventPatternOutput, error) {
    if in == nil || strings.TrimSpace(in.EventPattern) == "" {
        return nil, newValidationf("EventPattern is required.")
    }
    if strings.TrimSpace(in.Event) == "" {
        return nil, newValidationf("Event is required.")
    }
    filter, err := compileEventFilter(in.EventPattern)
    if err != nil {
        return nil, newInvalidEventPatternf("Event pattern is not valid. Reason: %v", err)
    }
    var event map[string]any
    dec := json.NewDecoder(strings.NewReader(in.Event))
    dec.UseNumber()
    if err := dec.Decode(&event); err != nil {
        return nil, newValidationf("Event must be a valid JSON object.")
    }
    return &TestEventPatternOutput{Event: filter.Matches(event)}, nil
}

func (s *Service) handleTestEventPattern(w http.ResponseWriter, r *http.Request) {
    var in TestEventPatternInput
    if err := decodeEventBridgeRequest(r, &in); err != nil {
        writeEventBridgeError(w, newValidationf("Malformed request body: %v", err))
        return
    }
    out, err := s.TestEventPattern(&in)
    if err != nil {
        writeEventBridgeError(w, err)
        return
    }
    writeEventBridgeJSON(w, out)
}
