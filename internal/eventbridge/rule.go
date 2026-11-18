package eventbridge

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	busNameMaxLen  = 256
	ruleNameMaxLen = 64
	descriptionMax = 512
)

// ruleItem is the ListRules result item shape.
type ruleItem struct {
	Arn                string `json:"Arn"`
	Description        string `json:"Description,omitempty"`
	EventBusName       string `json:"EventBusName,omitempty"`
	EventPattern       string `json:"EventPattern,omitempty"`
	ManagedBy          string `json:"ManagedBy,omitempty"`
	Name               string `json:"Name"`
	RoleArn            string `json:"RoleArn,omitempty"`
	ScheduleExpression string `json:"ScheduleExpression,omitempty"`
	State              string `json:"State"`
}

// opPutRule implements PutRule; an existing rule with the same name on the
// same event bus is updated in place (an omitted State preserves the
// current enabled/disabled state).
func (s *Service) opPutRule(w http.ResponseWriter, ct string, body []byte) {
	var in struct {
		Name               string
		EventBusName       string
		EventPattern       string
		ScheduleExpression string
		State              string
		Description        string
		RoleArn            string
		ManagedBy          string
	}
	if err := decodeWireBody(body, &in); err != nil {
		writeWireError(w, ct, http.StatusBadRequest, "SerializationException", "Failed to parse the request body.")
		return
	}
	busName := in.EventBusName
	if busName == "" {
		busName = defaultBusName
	}
	bus, ok := s.buses[busName]
	if !ok {
		writeWireError(w, ct, http.StatusBadRequest, "ResourceNotFoundException", fmt.Sprintf("Event bus %s does not exist.", busName))
		return
	}
	if !validRuleName(in.Name) {
		writeWireError(w, ct, http.StatusBadRequest, "InvalidParameterException", "Parameter Name is not valid.")
		return
	}
	if in.ScheduleExpression != "" {
		if issue := scheduleExpressionIssue(in.ScheduleExpression); issue != "" {
			writeWireError(w, ct, http.StatusBadRequest, "InvalidParameterException", issue)
			return
		}
	}
	if in.EventPattern != "" {
		if issue := eventPatternIssue(in.EventPattern); issue != "" {
			writeWireError(w, ct, http.StatusBadRequest, "InvalidEventPatternException", "Event pattern is not valid. Reason: "+issue)
			return
		}
	}
	if in.EventPattern == "" && in.ScheduleExpression == "" {
		writeWireError(w, ct, http.StatusBadRequest, "InvalidParameterException", "At least one of EventPattern or ScheduleExpression must be specified.")
		return
	}
	if len(in.Description) > descriptionMax {
		writeWireError(w, ct, http.StatusBadRequest, "InvalidParameterException", "Parameter Description is not valid.")
		return
	}
	state, stateErr := ruleState(in.State)
	if stateErr != "" {
		writeWireError(w, ct, http.StatusBadRequest, "InvalidParameterException", stateErr)
		return
	}
	now := time.Now().Unix()
	rule, ok := bus.Rules[in.Name]
	if !ok {
		rule = &ruleRec{
			Name:         in.Name,
			Arn:          s.ruleARN(busName, in.Name),
			EventBusName: busName,
			State:        state,
			Created:      now,
		}
		bus.Rules[in.Name] = rule
	}
	rule.EventPattern = in.EventPattern
	rule.ScheduleExpression = in.ScheduleExpression
	rule.Description = in.Description
	rule.RoleArn = in.RoleArn
	if in.State != "" {
		rule.State = state
	}
	if in.ManagedBy != "" {
		rule.ManagedBy = in.ManagedBy
	}
	rule.Modified = now
	s.save()
	writeWireJSON(w, ct, struct {
		RuleArn string `json:"RuleArn"`
	}{RuleArn: rule.Arn})
}

// ruleState validates the optional State member; absent means ENABLED.
func ruleState(state string) (string, string) {
	switch state {
	case "":
		return stateEnabled, ""
	case stateEnabled, stateDisabled:
		return state, ""
	default:
		return "", "Parameter State is not valid."
	}
}

// opDescribeRule implements DescribeRule.
func (s *Service) opDescribeRule(w http.ResponseWriter, ct string, body []byte) {
	var in struct {
		Name         string
		EventBusName string
	}
	if err := decodeWireBody(body, &in); err != nil {
		writeWireError(w, ct, http.StatusBadRequest, "SerializationException", "Failed to parse the request body.")
		return
	}
	busName := in.EventBusName
	if busName == "" {
		busName = defaultBusName
	}
	bus, ok := s.buses[busName]
	if !ok {
		writeWireError(w, ct, http.StatusBadRequest, "ResourceNotFoundException", fmt.Sprintf("Event bus %s does not exist.", busName))
		return
	}
	rule, ok := bus.Rules[in.Name]
	if !ok {
		writeWireError(w, ct, http.StatusBadRequest, "ResourceNotFoundException", fmt.Sprintf("Rule %s does not exist.", in.Name))
		return
	}
	writeWireJSON(w, ct, struct {
		Arn                string `json:"Arn"`
		Description        string `json:"Description,omitempty"`
		EventBusName       string `json:"EventBusName,omitempty"`
		EventPattern       string `json:"EventPattern,omitempty"`
		ManagedBy          string `json:"ManagedBy,omitempty"`
		Name               string `json:"Name"`
		RoleArn            string `json:"RoleArn,omitempty"`
		ScheduleExpression string `json:"ScheduleExpression,omitempty"`
		State              string `json:"State"`
	}{
		Arn:                rule.Arn,
		Description:        rule.Description,
		EventBusName:       rule.EventBusName,
		EventPattern:       rule.EventPattern,
		ManagedBy:          rule.ManagedBy,
		Name:               rule.Name,
		RoleArn:            rule.RoleArn,
		ScheduleExpression: rule.ScheduleExpression,
		State:              rule.State,
	})
}

// opListRules implements ListRules with Limit/NextToken paging over rules
// sorted by name.
func (s *Service) opListRules(w http.ResponseWriter, ct string, body []byte) {
	var in struct {
		EventBusName string
		Limit        float64
		NamePrefix   string
		NextToken    string
	}
	if err := decodeWireBody(body, &in); err != nil {
		writeWireError(w, ct, http.StatusBadRequest, "SerializationException", "Failed to parse the request body.")
		return
	}
	busName := in.EventBusName
	if busName == "" {
		busName = defaultBusName
	}
	bus, ok := s.buses[busName]
	if !ok {
		writeWireError(w, ct, http.StatusBadRequest, "ResourceNotFoundException", fmt.Sprintf("Event bus %s does not exist.", busName))
		return
	}
	limit := pageLimit(in.Limit)
	start, err := decodeNextToken(in.NextToken)
	if err != nil {
		writeWireError(w, ct, http.StatusBadRequest, "InvalidNextTokenException", "NextToken is not valid.")
		return
	}
	names := make([]string, 0, len(bus.Rules))
	for name := range bus.Rules {
		if in.NamePrefix == "" || strings.HasPrefix(name, in.NamePrefix) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if start != "" {
		idx := sort.Search(len(names), func(i int) bool { return names[i] > start })
		names = names[idx:]
	}
	end := limit
	if end > len(names) {
		end = len(names)
	}
	resp := struct {
		NextToken string     `json:"NextToken,omitempty"`
		Rules     []ruleItem `json:"Rules"`
	}{Rules: make([]ruleItem, 0, end)}
	for _, name := range names[:end] {
		rule := bus.Rules[name]
		resp.Rules = append(resp.Rules, ruleItem{
			Arn:                rule.Arn,
			Description:        rule.Description,
			EventBusName:       rule.EventBusName,
			EventPattern:       rule.EventPattern,
			ManagedBy:          rule.ManagedBy,
			Name:               rule.Name,
			RoleArn:            rule.RoleArn,
			ScheduleExpression: rule.ScheduleExpression,
			State:              rule.State,
		})
	}
	if len(names) > limit {
		resp.NextToken = encodeNextToken(names[limit-1])
	}
	writeWireJSON(w, ct, resp)
}

// opDeleteRule implements DeleteRule; Force is accepted for wire
// compatibility (this surface stores no targets).
func (s *Service) opDeleteRule(w http.ResponseWriter, ct string, body []byte) {
	var in struct {
		EventBusName string
		Force        bool
		Name         string
	}
	if err := decodeWireBody(body, &in); err != nil {
		writeWireError(w, ct, http.StatusBadRequest, "SerializationException", "Failed to parse the request body.")
		return
	}
	busName := in.EventBusName
	if busName == "" {
		busName = defaultBusName
	}
	bus, ok := s.buses[busName]
	if !ok {
		writeWireError(w, ct, http.StatusBadRequest, "ResourceNotFoundException", fmt.Sprintf("Event bus %s does not exist.", busName))
		return
	}
	if _, ok := bus.Rules[in.Name]; !ok {
		writeWireError(w, ct, http.StatusBadRequest, "ResourceNotFoundException", fmt.Sprintf("Rule %s does not exist.", in.Name))
		return
	}
	delete(bus.Rules, in.Name)
	s.save()
	writeWireJSON(w, ct, struct{}{})
}

// opToggleRule backs EnableRule and DisableRule.
func (s *Service) opToggleRule(w http.ResponseWriter, ct string, body []byte, state string) {
	var in struct {
		EventBusName string
		Name         string
	}
	if err := decodeWireBody(body, &in); err != nil {
		writeWireError(w, ct, http.StatusBadRequest, "SerializationException", "Failed to parse the request body.")
		return
	}
	busName := in.EventBusName
	if busName == "" {
		busName = defaultBusName
	}
	bus, ok := s.buses[busName]
	if !ok {
		writeWireError(w, ct, http.StatusBadRequest, "ResourceNotFoundException", fmt.Sprintf("Event bus %s does not exist.", busName))
		return
	}
	rule, ok := bus.Rules[in.Name]
	if !ok {
		writeWireError(w, ct, http.StatusBadRequest, "ResourceNotFoundException", fmt.Sprintf("Rule %s does not exist.", in.Name))
		return
	}
	rule.State = state
	rule.Modified = time.Now().Unix()
	s.save()
	writeWireJSON(w, ct, struct{}{})
}

// busARN stamps the event bus ARN.
func (s *Service) busARN(busName string) string {
	return fmt.Sprintf("arn:aws:events:%s:%s:event-bus/%s", s.region(), s.account(), busName)
}

// ruleARN stamps the rule ARN. Rules on the default event bus use the
// legacy single-segment form (rule/<name>); rules on other buses carry the
// bus name (rule/<bus>/<rule>).
func (s *Service) ruleARN(busName, ruleName string) string {
	if busName == "" || busName == defaultBusName {
		return fmt.Sprintf("arn:aws:events:%s:%s:rule/%s", s.region(), s.account(), ruleName)
	}
	return fmt.Sprintf("arn:aws:events:%s:%s:rule/%s/%s", s.region(), s.account(), busName, ruleName)
}

func validBusName(name string) bool { return validResourceName(name, busNameMaxLen, true) }

func validRuleName(name string) bool { return validResourceName(name, ruleNameMaxLen, false) }

// validResourceName accepts [A-Za-z0-9._-] (plus "/" for event bus names,
// as used by partner event source buses).
func validResourceName(name string, maxLen int, allowSlash bool) bool {
	if name == "" || len(name) > maxLen {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.', c == '_', c == '-':
		case c == '/' && allowSlash:
		default:
			return false
		}
	}
	return true
}

// scheduleExpressionIssue validates rate()/cron() schedule expressions and
// returns "" for accepted expressions.
func scheduleExpressionIssue(expr string) string {
	expr = strings.TrimSpace(expr)
	switch {
	case strings.HasPrefix(expr, "rate(") && strings.HasSuffix(expr, ")"):
		inner := strings.TrimSpace(expr[len("rate(") : len(expr)-1])
		parts := strings.Fields(inner)
		if len(parts) != 2 {
			return "Parameter ScheduleExpression is not valid."
		}
		n, err := strconv.Atoi(parts[0])
		if err != nil || n <= 0 {
			return "Parameter ScheduleExpression is not valid."
		}
		switch parts[1] {
		case "minute", "hour", "day":
			if n != 1 {
				return "Parameter ScheduleExpression is not valid."
			}
		case "minutes", "hours", "days":
			if n == 1 {
				return "Parameter ScheduleExpression is not valid."
			}
		default:
			return "Parameter ScheduleExpression is not valid."
		}
		return ""
	case strings.HasPrefix(expr, "cron(") && strings.HasSuffix(expr, ")"):
		inner := strings.TrimSpace(expr[len("cron(") : len(expr)-1])
		if n := len(strings.Fields(inner)); n == 6 || n == 7 {
			return ""
		}
		return "Parameter ScheduleExpression is not valid."
	}
	return "Parameter ScheduleExpression is not valid."
}

// eventPatternIssue performs lightweight validation of an event pattern for
// PutRule and returns "" for accepted patterns.
func eventPatternIssue(pattern string) string {
	var root map[string]json.RawMessage
	if err := json.Unmarshal([]byte(pattern), &root); err != nil {
		return "Pattern must be valid JSON."
	}
	if root == nil {
		return "Pattern must be a JSON object."
	}
	for key, value := range root {
		if !eventPatternKeys[key] {
			return fmt.Sprintf("Unrecognized key [%s]", key)
		}
		if key == "detail" {
			var obj map[string]json.RawMessage
			if err := json.Unmarshal(value, &obj); err != nil {
				return fmt.Sprintf("Value for key [%s] must be an object.", key)
			}
			if issue := eventPatternDetailIssue(obj, 0); issue != "" {
				return issue
			}
			continue
		}
		var values []string
		if err := json.Unmarshal(value, &values); err != nil {
			return fmt.Sprintf("Value for key [%s] must be an array of strings.", key)
		}
		if len(values) == 0 {
			return "Value array can not be empty."
		}
		for _, val := range values {
			if val == "" {
				return "Value array can not contain an empty string."
			}
		}
	}
	return ""
}

// eventPatternDetailIssue validates the nested "detail" match object.
func eventPatternDetailIssue(obj map[string]json.RawMessage, depth int) string {
	if depth > 10 {
		return ""
	}
	for _, val := range obj {
		if issue := eventPatternLeafIssue(val, depth); issue != "" {
			return issue
		}
	}
	return ""
}

func eventPatternLeafIssue(raw json.RawMessage, depth int) string {
	if depth > 10 {
		return ""
	}
	var str string
	if err := json.Unmarshal(raw, &str); err == nil {
		if str == "" {
			return "Value can not be empty."
		}
		return ""
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err == nil {
		for _, val := range values {
			if val == "" {
				return "Value can not be empty."
			}
		}
		return ""
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err == nil {
		return eventPatternDetailIssue(obj, depth+1)
	}
	return ""
}

var eventPatternKeys = map[string]bool{
	"account":     true,
	"detail":      true,
	"detail-type": true,
	"id":          true,
	"region":      true,
	"resources":   true,
	"source":      true,
	"time":        true,
}
