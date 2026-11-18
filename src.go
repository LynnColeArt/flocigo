// floci-go: AWS EventBridge control-plane emulation (ticket flocigo/ticket-028).
//
// Implements CreateEventBus, DescribeEventBus, DeleteEventBus, ListEventBuses,
// PutRule, DescribeRule, ListRules, DeleteRule, EnableRule and DisableRule,
// speaking the AWS JSON 1.1 wire protocol (X-Amz-Target: AWSEvents.<Op> with
// com.amazonaws.events# error types).
//
// Persistence uses the DDB JSON 1.0 protocol adapter (ticket 003) over the
// file-backed table store (ticket 004). Buses live in the buses table and
// rule storage is keyed by event-bus name: tables["rules"][bus][ruleName].
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	contentType   = "application/x-amz-json-1.1"
	targetPrefix  = "AWSEvents."
	errorNS       = "com.amazonaws.events#"
	coralNS       = "com.amazon.coral.service#"
	defaultBus    = "default"
	maxPage       = 100
	stateEnabled  = "ENABLED"
	stateDisabled = "DISABLED"
	ddbProtocol   = "1.0"
)

var (
	busNameRE  = regexp.MustCompile(`^[/\.\-_A-Za-z0-9-]+$`)
	ruleNameRE = regexp.MustCompile(`^[\.\-_A-Za-z0-9]+$`)
)

// ---------------------------------------------------------------------------
// DDB JSON 1.0 protocol adapter (ticket flocigo/ticket-003).
// ---------------------------------------------------------------------------

// avToNative converts a DDB JSON 1.0 attribute value into a native value.
func avToNative(av any) any {
	m, ok := av.(map[string]any)
	if !ok || len(m) != 1 {
		return av
	}
	for typ, raw := range m {
		switch typ {
		case "S":
			if s, ok := raw.(string); ok {
				return s
			}
		case "N":
			if s, ok := raw.(string); ok {
				if f, err := strconv.ParseFloat(s, 64); err == nil {
					return f
				}
				return s
			}
		case "BOOL":
			if b, ok := raw.(bool); ok {
				return b
			}
		case "NULL":
			return nil
		case "B":
			if s, ok := raw.(string); ok {
				if b, err := base64.StdEncoding.DecodeString(s); err == nil {
					return b
				}
				return s
			}
		case "L":
			if list, ok := raw.([]any); ok {
				for i, e := range list {
					list[i] = avToNative(e)
				}
				return list
			}
		case "M":
			if m2, ok := raw.(map[string]any); ok {
				for k, e := range m2 {
					m2[k] = avToNative(e)
				}
				return m2
			}
		case "SS", "NS", "BS":
			if list, ok := raw.([]any); ok {
				out := make([]any, 0, len(list))
				for _, e := range list {
					s, ok := e.(string)
					if !ok {
						continue
					}
					switch typ {
					case "SS":
						out = append(out, s)
					case "NS":
						if f, err := strconv.ParseFloat(s, 64); err == nil {
							out = append(out, f)
						} else {
							out = append(out, s)
						}
					case "BS":
						if b, err := base64.StdEncoding.DecodeString(s); err == nil {
							out = append(out, b)
						} else {
							out = append(out, s)
						}
					}
				}
				return out
			}
		}
	}
	return av
}

// nativeToAV converts a native value into a DDB JSON 1.0 attribute value.
func nativeToAV(v any) any {
	switch x := v.(type) {
	case nil:
		return map[string]any{"NULL": true}
	case bool:
		return map[string]any{"BOOL": x}
	case string:
		return map[string]any{"S": x}
	case []byte:
		return map[string]any{"B": base64.StdEncoding.EncodeToString(x)}
	case json.Number:
		return map[string]any{"N": x.String()}
	case float64:
		return map[string]any{"N": ddbNumString(x)}
	case int:
		return map[string]any{"N": strconv.Itoa(x)}
	case int64:
		return map[string]any{"N": strconv.FormatInt(x, 10)}
	case []any:
		list := make([]any, len(x))
		for i, e := range x {
			list[i] = nativeToAV(e)
		}
		return map[string]any{"L": list}
	case map[string]any:
		m := make(map[string]any, len(x))
		for k, e := range x {
			m[k] = nativeToAV(e)
		}
		return map[string]any{"M": m}
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return map[string]any{"NULL": true}
		}
		var n any
		if err := json.Unmarshal(b, &n); err != nil {
			return map[string]any{"NULL": true}
		}
		return nativeToAV(n)
	}
}

// ddbNumString renders a float as its shortest faithful DDB N-number string.
func ddbNumString(f float64) string {
	if f == math.Trunc(f) && math.Abs(f) < 1e15 {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// ---------------------------------------------------------------------------
// Store (ticket flocigo/ticket-004): file-backed tables whose items are
// serialized with the DDB JSON 1.0 protocol adapter above.
// ---------------------------------------------------------------------------

type Store struct {
	path   string
	tables map[string]map[string]any
}

// NewStore loads (or creates) a store backed by the file at path.
func NewStore(path string) (*Store, error) {
	s := &Store{path: path, tables: map[string]map[string]any{}}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, err
	}
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("store: invalid %s: %w", path, err)
	}
	if raw, ok := doc["tables"]; ok {
		if tables, ok := avToNative(raw).(map[string]any); ok {
			for name, rawItems := range tables {
				items, ok := rawItems.(map[string]any)
				if !ok {
					continue
				}
				t := make(map[string]any, len(items))
				for k, v := range items {
					t[k] = v
				}
				s.tables[name] = t
			}
		}
	}
	return s, nil
}

// Table returns the mutable item map of a table, creating it if missing.
func (s *Store) Table(name string) map[string]any {
	t, ok := s.tables[name]
	if !ok {
		t = map[string]any{}
		s.tables[name] = t
	}
	return t
}

// Has reports whether an item exists in a table.
func (s *Store) Has(table, key string) bool {
	t, ok := s.tables[table]
	if !ok {
		return false
	}
	_, ok = t[key]
	return ok
}

// Get returns an item from a table.
func (s *Store) Get(table, key string) (any, bool) {
	t, ok := s.tables[table]
	if !ok {
		return nil, false
	}
	v, ok := t[key]
	return v, ok
}

// Put stores an item in a table.
func (s *Store) Put(table, key string, item any) {
	s.Table(table)[key] = item
}

// Delete removes an item from a table.
func (s *Store) Delete(table, key string) {
	if t, ok := s.tables[table]; ok {
		delete(t, key)
	}
}

// SortedKeys returns the item keys of a table sorted ascending.
func (s *Store) SortedKeys(table string) []string {
	keys := make([]string, 0, len(s.tables[table]))
	for k := range s.tables[table] {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Save atomically writes the store file as a DDB JSON 1.0 document.
func (s *Store) Save() error {
	doc := map[string]any{
		"ddbJSONProtocol": nativeToAV(ddbProtocol),
		"tables":          nativeToAV(s.tables),
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// ---------------------------------------------------------------------------
// EventBridge control plane (ticket flocigo/ticket-028).
// ---------------------------------------------------------------------------

// Service implements the EventBridge control plane over a Store.
type Service struct {
	mu      sync.Mutex
	store   *Store
	region  string
	account string
	now     func() float64
}

// NewService builds the control-plane service, creating the default event bus
// if needed. Region and account come from the environment (AWS_REGION /
// AWS_DEFAULT_REGION and AWS_ACCOUNT_ID).
func NewService(store *Store) *Service {
	s := &Service{
		store:   store,
		region:  firstNonEmpty(os.Getenv("AWS_REGION"), os.Getenv("AWS_DEFAULT_REGION"), "us-east-1"),
		account: firstNonEmpty(os.Getenv("AWS_ACCOUNT_ID"), "123456789012"),
		now:     func() float64 { return float64(time.Now().UnixMilli()) / 1000 },
	}
	s.ensureDefaultBus()
	return s
}

func (s *Service) ensureDefaultBus() {
	if s.store.Has("buses", defaultBus) {
		return
	}
	ts := s.now()
	s.store.Put("buses", defaultBus, map[string]any{
		"name":     defaultBus,
		"arn":      s.busArn(defaultBus),
		"created":  ts,
		"modified": ts,
	})
	_ = s.store.Save()
}

func (s *Service) busArn(name string) string {
	return fmt.Sprintf("arn:aws:events:%s:%s:event-bus/%s", s.region, s.account, name)
}

// ruleArn mirrors AWS: rules on the default event bus have no bus segment.
func (s *Service) ruleArn(bus, name string) string {
	if bus == defaultBus {
		return fmt.Sprintf("arn:aws:events:%s:%s:rule/%s", s.region, s.account, name)
	}
	return fmt.Sprintf("arn:aws:events:%s:%s:rule/%s/%s", s.region, s.account, bus, name)
}

// Response shapes (members emitted in AWS model order).

type createEventBusResponse struct {
	EventBusArn string `json:"EventBusArn"`
}

type eventBus struct {
	Arn              string   `json:"Arn"`
	CreationTime     *float64 `json:"CreationTime,omitempty"`
	Description      string   `json:"Description,omitempty"`
	KmsKeyIdentifier string   `json:"KmsKeyIdentifier,omitempty"`
	LastModifiedTime *float64 `json:"LastModifiedTime,omitempty"`
	Name             string   `json:"Name"`
	Policy           string   `json:"Policy,omitempty"`
}

type listEventBusesResponse struct {
	EventBuses []eventBus `json:"EventBuses"`
	NextToken  string     `json:"NextToken,omitempty"`
}

type putRuleResponse struct {
	RuleArn string `json:"RuleArn"`
}

type rule struct {
	Arn                string `json:"Arn"`
	Description        string `json:"Description,omitempty"`
	EventBusName       string `json:"EventBusName,omitempty"`
	EventPattern       string `json:"EventPattern,omitempty"`
	Name               string `json:"Name"`
	RoleArn            string `json:"RoleArn,omitempty"`
	ScheduleExpression string `json:"ScheduleExpression,omitempty"`
	State              string `json:"State"`
}

type listRulesResponse struct {
	NextToken string `json:"NextToken,omitempty"`
	Rules     []rule `json:"Rules"`
}

// ServeHTTP answers AWS JSON 1.1 requests (X-Amz-Target: AWSEvents.<Op>).
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer recoverInternal(w)

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, coralNS+"SerializationException", "")
		return
	}
	req := map[string]any{}
	if len(bytes.TrimSpace(body)) > 0 {
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.UseNumber()
		if err := dec.Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, coralNS+"SerializationException", "")
			return
		}
	}

	op := strings.TrimPrefix(r.Header.Get("X-Amz-Target"), targetPrefix)
	if i := strings.LastIndexAny(op, ".#"); i >= 0 {
		op = op[i+1:]
	}
	switch op {
	case "CreateEventBus":
		s.createEventBus(w, req)
	case "DescribeEventBus":
		s.describeEventBus(w, req)
	case "DeleteEventBus":
		s.deleteEventBus(w, req)
	case "ListEventBuses":
		s.listEventBuses(w, req)
	case "PutRule":
		s.putRule(w, req)
	case "DescribeRule":
		s.describeRule(w, req)
	case "ListRules":
		s.listRules(w, req)
	case "DeleteRule":
		s.deleteRule(w, req)
	case "EnableRule":
		s.setRuleState(w, req, stateEnabled)
	case "DisableRule":
		s.setRuleState(w, req, stateDisabled)
	default:
		writeError(w, http.StatusBadRequest, coralNS+"UnknownOperationException", "")
	}
}

func (s *Service) createEventBus(w http.ResponseWriter, req map[string]any) {
	name, ok := reqString(req, "Name")
	if !ok || name == "" {
		writeError(w, http.StatusBadRequest, errorNS+"InvalidParameterException", "Parameter Name is required.")
		return
	}
	if len(name) > 256 || !busNameRE.MatchString(name) {
		writeError(w, http.StatusBadRequest, errorNS+"InvalidParameterException", "Parameter Name is not valid.")
		return
	}
	if name == defaultBus || s.store.Has("buses", name) {
		writeError(w, http.StatusBadRequest, errorNS+"ResourceAlreadyExistsException", "The event bus "+name+" already exists.")
		return
	}
	ts := s.now()
	item := map[string]any{
		"name":     name,
		"arn":      s.busArn(name),
		"created":  ts,
		"modified": ts,
	}
	if v, ok := reqString(req, "Description"); ok && v != "" {
		item["description"] = v
	}
	if v, ok := reqString(req, "KmsKeyIdentifier"); ok && v != "" {
		item["kmsKeyIdentifier"] = v
	}
	if v, ok := reqString(req, "EventSourceName"); ok && v != "" {
		item["eventSourceName"] = v
	}
	if tags, ok := req["Tags"]; ok {
		item["tags"] = tags
	}
	s.store.Put("buses", name, item)
	_ = s.store.Save()
	writeJSON(w, createEventBusResponse{EventBusArn: s.busArn(name)})
}

func (s *Service) describeEventBus(w http.ResponseWriter, req map[string]any) {
	name := reqBusNameOrName(req)
	item, ok := s.store.Get("buses", name)
	if !ok {
		errBusNotFound(w, name)
		return
	}
	m, _ := item.(map[string]any)
	writeJSON(w, busShape(m, name))
}

// deleteEventBus refuses to delete the default event bus and removes the
// bus-name entry from the rules table along with the bus itself.
func (s *Service) deleteEventBus(w http.ResponseWriter, req map[string]any) {
	name := reqBusNameOrName(req)
	if name == defaultBus {
		writeError(w, http.StatusBadRequest, errorNS+"ConcurrentModificationException", "Cannot delete the default event bus.")
		return
	}
	if !s.store.Has("buses", name) {
		errBusNotFound(w, name)
		return
	}
	s.store.Delete("buses", name)
	s.store.Delete("rules", name)
	_ = s.store.Save()
	writeJSON(w, struct{}{})
}

func (s *Service) listEventBuses(w http.ResponseWriter, req map[string]any) {
	prefix, _ := reqString(req, "NamePrefix")
	limit := reqLimit(req, "Limit")
	type entry struct {
		name string
		item map[string]any
	}
	var matches []entry
	for _, name := range s.store.SortedKeys("buses") {
		if prefix != "" && !strings.HasPrefix(name, prefix) {
			continue
		}
		raw, _ := s.store.Get("buses", name)
		item, _ := raw.(map[string]any)
		matches = append(matches, entry{name, item})
	}
	offset, end := pageBounds(len(matches), tokenOffset(req), limit)
	shapes := make([]eventBus, 0, end-offset)
	for _, e := range matches[offset:end] {
		shapes = append(shapes, busShape(e.item, e.name))
	}
	resp := listEventBusesResponse{EventBuses: shapes}
	if end < len(matches) {
		resp.NextToken = strconv.Itoa(end)
	}
	writeJSON(w, resp)
}

func (s *Service) putRule(w http.ResponseWriter, req map[string]any) {
	busName := reqBusName(req)
	name, ok := reqString(req, "Name")
	if !ok || name == "" {
		writeError(w, http.StatusBadRequest, errorNS+"InvalidParameterException", "Parameter Name is required.")
		return
	}
	if len(name) > 64 || !ruleNameRE.MatchString(name) {
		writeError(w, http.StatusBadRequest, errorNS+"InvalidParameterException", "Parameter Name is not valid.")
		return
	}
	schedule, okS := reqString(req, "ScheduleExpression")
	pattern, okP := reqString(req, "EventPattern")
	hasSchedule := okS && schedule != ""
	hasPattern := okP && pattern != ""
	if !hasSchedule && !hasPattern {
		writeError(w, http.StatusBadRequest, errorNS+"InvalidParameterException", "Parameter ScheduleExpression and EventPattern are required.")
		return
	}
	if hasSchedule && !validScheduleExpression(schedule) {
		writeError(w, http.StatusBadRequest, errorNS+"InvalidParameterException", "Parameter ScheduleExpression is not valid.")
		return
	}
	if hasPattern && !validEventPattern(pattern) {
		writeError(w, http.StatusBadRequest, errorNS+"InvalidEventPatternException", "Event pattern is not valid. Reason: Invalid JSON.")
		return
	}
	state := ""
	if v, ok := reqString(req, "State"); ok && v != "" {
		if v != stateEnabled && v != stateDisabled {
			writeError(w, http.StatusBadRequest, errorNS+"InvalidParameterException", "Parameter State is not valid.")
			return
		}
		state = v
	}
	if !s.store.Has("buses", busName) {
		errBusNotFound(w, busName)
		return
	}
	ts := s.now()
	raw, _ := s.store.Get("rules", busName)
	rules, _ := raw.(map[string]any)
	if rules == nil {
		rules = map[string]any{}
		s.store.Put("rules", busName, rules)
	}
	item, _ := rules[name].(map[string]any)
	isNew := item == nil
	if isNew {
		item = map[string]any{
			"name":    name,
			"busName": busName,
			"arn":     s.ruleArn(busName, name),
			"created": ts,
			"state":   stateEnabled,
		}
	}
	// PutRule replaces the rule definition, but only overrides State when it
	// is explicitly supplied (the existing state is otherwise preserved).
	if state != "" {
		item["state"] = state
	}
	if hasSchedule {
		item["scheduleExpression"] = schedule
	} else {
		delete(item, "scheduleExpression")
	}
	if hasPattern {
		item["eventPattern"] = compactJSON(pattern)
	} else {
		delete(item, "eventPattern")
	}
	if v, ok := reqString(req, "Description"); ok && v != "" {
		item["description"] = v
	} else {
		delete(item, "description")
	}
	if v, ok := reqString(req, "RoleArn"); ok && v != "" {
		item["roleArn"] = v
	} else {
		delete(item, "roleArn")
	}
	if tags, ok := req["Tags"]; ok && isNew {
		item["tags"] = tags
	}
	item["arn"] = s.ruleArn(busName, name)
	item["modified"] = ts
	rules[name] = item
	_ = s.store.Save()
	writeJSON(w, putRuleResponse{RuleArn: s.ruleArn(busName, name)})
}

func (s *Service) describeRule(w http.ResponseWriter, req map[string]any) {
	busName := reqBusName(req)
	name, _ := reqString(req, "Name")
	if name == "" {
		writeError(w, http.StatusBadRequest, errorNS+"InvalidParameterException", "Parameter Name is required.")
		return
	}
	if !s.store.Has("buses", busName) {
		errBusNotFound(w, busName)
		return
	}
	item, ok := s.ruleItem(busName, name)
	if !ok {
		errRuleNotFound(w, name)
		return
	}
	writeJSON(w, ruleShape(item, busName))
}

func (s *Service) listRules(w http.ResponseWriter, req map[string]any) {
	busName := reqBusName(req)
	if !s.store.Has("buses", busName) {
		errBusNotFound(w, busName)
		return
	}
	prefix, _ := reqString(req, "NamePrefix")
	limit := reqLimit(req, "Limit")
	raw, _ := s.store.Get("rules", busName)
	rules, _ := raw.(map[string]any)
	var matches []string
	for _, name := range sortedKeys(rules) {
		if prefix == "" || strings.HasPrefix(name, prefix) {
			matches = append(matches, name)
		}
	}
	offset, end := pageBounds(len(matches), tokenOffset(req), limit)
	shapes := make([]rule, 0, end-offset)
	for _, name := range matches[offset:end] {
		item, _ := rules[name].(map[string]any)
		shapes = append(shapes, ruleShape(item, busName))
	}
	resp := listRulesResponse{Rules: shapes}
	if end < len(matches) {
		resp.NextToken = strconv.Itoa(end)
	}
	writeJSON(w, resp)
}

func (s *Service) deleteRule(w http.ResponseWriter, req map[string]any) {
	busName := reqBusName(req)
	name, _ := reqString(req, "Name")
	if name == "" {
		writeError(w, http.StatusBadRequest, errorNS+"InvalidParameterException", "Parameter Name is required.")
		return
	}
	if !s.store.Has("buses", busName) {
		errBusNotFound(w, busName)
		return
	}
	rules, ok := s.rulesTable(busName)
	if !ok {
		errRuleNotFound(w, name)
		return
	}
	if _, exists := rules[name]; !exists {
		errRuleNotFound(w, name)
		return
	}
	delete(rules, name)
	_ = s.store.Save()
	writeJSON(w, struct{}{})
}

// setRuleState backs EnableRule and DisableRule.
func (s *Service) setRuleState(w http.ResponseWriter, req map[string]any, state string) {
	busName := reqBusName(req)
	name, _ := reqString(req, "Name")
	if name == "" {
		writeError(w, http.StatusBadRequest, errorNS+"InvalidParameterException", "Parameter Name is required.")
		return
	}
	if !s.store.Has("buses", busName) {
		errBusNotFound(w, busName)
		return
	}
	item, ok := s.ruleItem(busName, name)
	if !ok {
		errRuleNotFound(w, name)
		return
	}
	item["state"] = state
	item["modified"] = s.now()
	_ = s.store.Save()
	writeJSON(w, struct{}{})
}

func (s *Service) rulesTable(busName string) (map[string]any, bool) {
	raw, ok := s.store.Get("rules", busName)
	if !ok {
		return nil, false
	}
	rules, _ := raw.(map[string]any)
	return rules, rules != nil
}

func (s *Service) ruleItem(busName, name string) (map[string]any, bool) {
	rules, ok := s.rulesTable(busName)
	if !ok {
		return nil, false
	}
	item, ok := rules[name].(map[string]any)
	return item, ok
}

func reqString(req map[string]any, key string) (string, bool) {
	v, ok := req[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// reqBusName resolves the target event bus for rule operations.
func reqBusName(req map[string]any) string {
	if v, ok := reqString(req, "EventBusName"); ok && v != "" {
		return v
	}
	return defaultBus
}

// reqBusNameOrName resolves the target event bus for bus-only operations.
func reqBusNameOrName(req map[string]any) string {
	if v, ok := reqString(req, "EventBusName"); ok && v != "" {
		return v
	}
	if v, ok := reqString(req, "Name"); ok && v != "" {
		return v
	}
	return defaultBus
}

// reqLimit parses a paging limit, defaulting and clamping into [1, 100].
func reqLimit(req map[string]any, key string) int {
	v, ok := req[key]
	if !ok {
		return maxPage
	}
	limit := 0
	switch n := v.(type) {
	case json.Number:
		limit, _ = strconv.Atoi(n.String())
	case float64:
		limit = int(n)
	case string:
		limit, _ = strconv.Atoi(n)
	}
	if limit < 1 {
		return maxPage
	}
	if limit > maxPage {
		return maxPage
	}
	return limit
}

// tokenOffset extracts the opaque NextToken page offset.
func tokenOffset(req map[string]any) int {
	token, _ := reqString(req, "NextToken")
	if token == "" {
		return 0
	}
	n, err := strconv.Atoi(token)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// pageBounds clamps a [offset, end) window over n items.
func pageBounds(n, offset, limit int) (int, int) {
	if offset > n {
		offset = n
	}
	end := offset + limit
	if end > n {
		end = n
	}
	return offset, end
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func getStr(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	v, _ := m[key].(string)
	return v
}

func numPtr(v any) *float64 {
	switch n := v.(type) {
	case float64:
		f := n
		return &f
	case json.Number:
		if f, err := n.Float64(); err == nil {
			return &f
		}
	}
	return nil
}

// compactJSON normalizes an event pattern the way AWS stores it (no leading
// or trailing insignificant whitespace).
func compactJSON(s string) string {
	var buf bytes.Buffer
	if err := json.Compact(&buf, []byte(s)); err == nil {
		return buf.String()
	}
	return s
}

func validEventPattern(pattern string) bool {
	var probe any
	return json.Unmarshal([]byte(pattern), &probe) == nil
}

func validScheduleExpression(expr string) bool {
	e := strings.ToLower(expr)
	return strings.HasPrefix(e, "rate(") || strings.HasPrefix(e, "cron(") || strings.HasPrefix(e, "at(")
}

func busShape(item map[string]any, name string) eventBus {
	b := eventBus{Name: name}
	if item != nil {
		b.Arn = getStr(item, "arn")
		b.CreationTime = numPtr(item["created"])
		b.LastModifiedTime = numPtr(item["modified"])
		b.Description = getStr(item, "description")
		b.KmsKeyIdentifier = getStr(item, "kmsKeyIdentifier")
		b.Policy = getStr(item, "policy")
	}
	return b
}

func ruleShape(item map[string]any, busName string) rule {
	r := rule{
		Arn:   getStr(item, "arn"),
		Name:  getStr(item, "name"),
		State: stateEnabled,
	}
	if v := getStr(item, "state"); v != "" {
		r.State = v
	}
	if busName != defaultBus {
		r.EventBusName = busName
	}
	r.Description = getStr(item, "description")
	r.EventPattern = getStr(item, "eventPattern")
	r.RoleArn = getStr(item, "roleArn")
	r.ScheduleExpression = getStr(item, "scheduleExpression")
	return r
}

func writeJSON(w http.ResponseWriter, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		data = []byte("{}")
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func writeError(w http.ResponseWriter, status int, typ, message string) {
	body := map[string]any{"__type": typ}
	if message != "" {
		body["message"] = message
	}
	name := typ
	if i := strings.LastIndexAny(name, "#:"); i >= 0 {
		name = name[i+1:]
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("x-amzn-ErrorType", name)
	w.WriteHeader(status)
	data, _ := json.Marshal(body)
	_, _ = w.Write(data)
}

func errBusNotFound(w http.ResponseWriter, name string) {
	writeError(w, http.StatusBadRequest, errorNS+"ResourceNotFoundException", "Event bus "+name+" does not exist.")
}

func errRuleNotFound(w http.ResponseWriter, name string) {
	writeError(w, http.StatusBadRequest, errorNS+"ResourceNotFoundException", "Rule "+name+" does not exist.")
}

func recoverInternal(w http.ResponseWriter) {
	if rec := recover(); rec != nil {
		log.Printf("floci-go: internal error: %v", rec)
		writeError(w, http.StatusInternalServerError, errorNS+"InternalException", "")
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func main() {
	port := firstNonEmpty(os.Getenv("FLOCI_PORT"), os.Getenv("PORT"), "4566")
	storePath := firstNonEmpty(os.Getenv("FLOCI_STORE"), "store.json")
	flagStore := flag.String("store", "", "path to the floci store file")
	flagPort := flag.String("port", "", "port to serve the EventBridge API on")
	flag.Parse()
	if *flagStore != "" {
		storePath = *flagStore
	}
	explicitPort := *flagPort != "" || os.Getenv("FLOCI_PORT") != "" || os.Getenv("PORT") != ""
	if *flagPort != "" {
		port = *flagPort
	}

	store, err := NewStore(storePath)
	if err != nil {
		log.Fatalf("floci-go: %v", err)
	}
	server := &http.Server{Handler: NewService(store)}

	primary, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("floci-go: cannot listen on port %s: %v", port, err)
	}
	listeners := []net.Listener{primary}
	// Best-effort compat listeners so that a run without explicit port config
	// still covers the common EventBridge emulator ports.
	if !explicitPort {
		for _, alt := range []string{"4566", "4567"} {
			if alt == port {
				continue
			}
			if ln, lerr := net.Listen("tcp", ":"+alt); lerr == nil {
				listeners = append(listeners, ln)
			}
		}
	}
	for _, ln := range listeners {
		log.Printf("floci-go EventBridge API listening on %s", ln.Addr())
		go func(l net.Listener) {
			_ = server.Serve(l)
		}(ln)
	}

	stopped := make(chan struct{})
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		_ = store.Save()
		_ = server.Close()
		close(stopped)
	}()
	<-stopped
}
