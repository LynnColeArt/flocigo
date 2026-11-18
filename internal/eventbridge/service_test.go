package eventbridge

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func callEB(t *testing.T, svc *Service, op string, body string) (int, map[string]any, string) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	r.Header.Set("X-Amz-Target", "AWSEvents."+op)
	r.Header.Set("Content-Type", "application/x-amz-json-1.1")
	w := httptest.NewRecorder()
	svc.ServeHTTP(w, r)
	raw := w.Body.String()
	out := map[string]any{}
	if strings.TrimSpace(raw) != "" {
		if err := json.Unmarshal([]byte(raw), &out); err != nil {
			t.Fatalf("op %s: non-JSON response %q", op, raw)
		}
	}
	return w.Code, out, raw
}

func wantEBError(t *testing.T, svc *Service, op string, body string, wantStatus int, code string) map[string]any {
	t.Helper()
	st, out, raw := callEB(t, svc, op, body)
	if st != wantStatus {
		t.Fatalf("op %s body %s: status = %d, want %d (raw: %s)", op, body, st, wantStatus, raw)
	}
	if got, _ := out["__type"].(string); got != code {
		t.Fatalf("op %s body %s: __type = %v, want %q (raw: %s)", op, body, out["__type"], code, raw)
	}
	return out
}

func sameEBNames(t *testing.T, v any, want ...string) bool {
	t.Helper()
	list, ok := v.([]any)
	if !ok || len(list) != len(want) {
		return false
	}
	for i, item := range list {
		m, ok := item.(map[string]any)
		if !ok || m["Name"] != want[i] {
			return false
		}
	}
	return true
}

// quoteEB JSON-encodes a string so patterns can be embedded in request
// bodies without hand-written escapes.
func quoteEB(v string) string {
	enc, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(enc)
}

func TestEBDefaultEventBus(t *testing.T) {
	svc := NewService("")
	st, out, raw := callEB(t, svc, "DescribeEventBus", "{}")
	if st != http.StatusOK {
		t.Fatalf("DescribeEventBus: status %d (raw %s)", st, raw)
	}
	if out["Name"] != defaultBusName {
		t.Fatalf("Name = %v, want %q", out["Name"], defaultBusName)
	}
	arn, _ := out["Arn"].(string)
	if !strings.Contains(arn, ":event-bus/default") {
		t.Fatalf("Arn = %q", arn)
	}
	if ts, _ := out["CreationTime"].(float64); ts <= 0 {
		t.Fatalf("CreationTime = %v", out["CreationTime"])
	}
	_, out, raw = callEB(t, svc, "ListEventBuses", "{}")
	if !sameEBNames(t, out["EventBuses"], defaultBusName) {
		t.Fatalf("EventBuses = %v (raw %s)", out["EventBuses"], raw)
	}
	wantEBError(t, svc, "DescribeEventBus", `{"Name":"ghost"}`, http.StatusBadRequest, "ResourceNotFoundException")
}

func TestEBEventBusLifecycle(t *testing.T) {
	svc := NewService("")
	wantArn := svc.busARN("orders")
	st, out, raw := callEB(t, svc, "CreateEventBus", `{"Name":"orders"}`)
	if st != http.StatusOK || out["EventBusArn"] != wantArn {
		t.Fatalf("CreateEventBus: status %d out %v (raw %s)", st, out, raw)
	}
	wantEBError(t, svc, "CreateEventBus", `{"Name":"orders"}`, http.StatusBadRequest, "ResourceAlreadyExistsException")
	wantEBError(t, svc, "CreateEventBus", `{"Name":"default"}`, http.StatusBadRequest, "ResourceAlreadyExistsException")
	wantEBError(t, svc, "CreateEventBus", `{"Name":"bad name"}`, http.StatusBadRequest, "InvalidParameterException")
	wantEBError(t, svc, "CreateEventBus", `{}`, http.StatusBadRequest, "InvalidParameterException")

	st, out, raw = callEB(t, svc, "DescribeEventBus", `{"Name":"orders"}`)
	if st != http.StatusOK || out["Name"] != "orders" || out["Arn"] != wantArn {
		t.Fatalf("DescribeEventBus: status %d out %v (raw %s)", st, out, raw)
	}
	if _, ok := out["CreationTime"].(float64); !ok {
		t.Fatalf("missing CreationTime in %v", out)
	}

	st, _, raw = callEB(t, svc, "DeleteEventBus", `{"Name":"orders"}`)
	if st != http.StatusOK {
		t.Fatalf("DeleteEventBus: status %d (raw %s)", st, raw)
	}
	wantEBError(t, svc, "DescribeEventBus", `{"Name":"orders"}`, http.StatusBadRequest, "ResourceNotFoundException")
	wantEBError(t, svc, "DeleteEventBus", `{"Name":"orders"}`, http.StatusBadRequest, "ResourceNotFoundException")
	wantEBError(t, svc, "DeleteEventBus", `{"Name":"default"}`, http.StatusBadRequest, "InvalidOperationException")
	wantEBError(t, svc, "DeleteEventBus", `{"Name":"bad name"}`, http.StatusBadRequest, "InvalidParameterException")
}

func TestEBListEventBusesPaging(t *testing.T) {
	svc := NewService("")
	for _, name := range []string{"b2", "b1", "b3"} {
		st, _, raw := callEB(t, svc, "CreateEventBus", fmt.Sprintf(`{"Name":%q}`, name))
		if st != http.StatusOK {
			t.Fatalf("CreateEventBus %q: status %d (raw %s)", name, st, raw)
		}
	}
	st, out, raw := callEB(t, svc, "ListEventBuses", `{"Limit":2}`)
	if st != http.StatusOK || !sameEBNames(t, out["EventBuses"], "b1", "b2") {
		t.Fatalf("ListEventBuses page 1: status %d buses %v (raw %s)", st, out["EventBuses"], raw)
	}
	token, _ := out["NextToken"].(string)
	if token == "" {
		t.Fatalf("missing NextToken (raw %s)", raw)
	}
	st, out, raw = callEB(t, svc, "ListEventBuses", fmt.Sprintf(`{"Limit":2,"NextToken":%q}`, token))
	if st != http.StatusOK || !sameEBNames(t, out["EventBuses"], "b3", "default") {
		t.Fatalf("ListEventBuses page 2: status %d buses %v (raw %s)", st, out["EventBuses"], raw)
	}
	if _, present := out["NextToken"]; present {
		t.Fatalf("unexpected trailing NextToken (raw %s)", raw)
	}
	st, out, raw = callEB(t, svc, "ListEventBuses", `{"NamePrefix":"b"}`)
	if st != http.StatusOK || !sameEBNames(t, out["EventBuses"], "b1", "b2", "b3") {
		t.Fatalf("ListEventBuses prefix: status %d buses %v (raw %s)", st, out["EventBuses"], raw)
	}
	wantEBError(t, svc, "ListEventBuses", `{"NextToken":"!!!"}`, http.StatusBadRequest, "InvalidNextTokenException")
}

func TestEBRuleLifecycle(t *testing.T) {
	svc := NewService("")

	st, out, raw := callEB(t, svc, "PutRule", `{"Name":"r1","EventPattern":`+quoteEB(`{"source":["com.acme"]}`)+`}`)
	if st != http.StatusOK || out["RuleArn"] != svc.ruleARN(defaultBusName, "r1") {
		t.Fatalf("PutRule r1: status %d out %v (raw %s)", st, out, raw)
	}
	st, _, raw = callEB(t, svc, "CreateEventBus", `{"Name":"orders"}`)
	if st != http.StatusOK {
		t.Fatalf("CreateEventBus orders: status %d (raw %s)", st, raw)
	}

	r2Arn := svc.ruleARN("orders", "r2")
	r2Body := `{"Name":"r2","EventBusName":"orders","State":"DISABLED","Description":"billing","EventPattern":` +
		quoteEB(`{"source":["com.acme.billing"]}`) + `}`
	st, out, raw = callEB(t, svc, "PutRule", r2Body)
	if st != http.StatusOK || out["RuleArn"] != r2Arn {
		t.Fatalf("PutRule r2: status %d out %v (raw %s)", st, out, raw)
	}

	st, out, raw = callEB(t, svc, "DescribeRule", `{"Name":"r2","EventBusName":"orders"}`)
	if st != http.StatusOK {
		t.Fatalf("DescribeRule r2: status %d (raw %s)", st, raw)
	}
	if out["Arn"] != r2Arn || out["Name"] != "r2" || out["EventBusName"] != "orders" ||
		out["State"] != "DISABLED" || out["Description"] != "billing" ||
		out["EventPattern"] != `{"source":["com.acme.billing"]}` {
		t.Fatalf("DescribeRule r2: out %v", out)
	}

	st, _, raw = callEB(t, svc, "DisableRule", `{"Name":"r1"}`)
	if st != http.StatusOK {
		t.Fatalf("DisableRule r1: status %d (raw %s)", st, raw)
	}
	st, out, raw = callEB(t, svc, "DescribeRule", `{"Name":"r1"}`)
	if st != http.StatusOK || out["State"] != "DISABLED" {
		t.Fatalf("DescribeRule r1 after disable: status %d out %v (raw %s)", st, out, raw)
	}
	st, _, raw = callEB(t, svc, "EnableRule", `{"Name":"r1"}`)
	if st != http.StatusOK {
		t.Fatalf("EnableRule r1: status %d (raw %s)", st, raw)
	}
	st, out, raw = callEB(t, svc, "DescribeRule", `{"Name":"r1"}`)
	if st != http.StatusOK || out["State"] != "ENABLED" {
		t.Fatalf("DescribeRule r1 after enable: status %d out %v (raw %s)", st, out, raw)
	}

	st, _, raw = callEB(t, svc, "PutRule", `{"Name":"r1","EventPattern":`+quoteEB(`{"source":["com.acme.new"]}`)+`}`)
	if st != http.StatusOK {
		t.Fatalf("PutRule r1 update: status %d (raw %s)", st, raw)
	}
	st, out, raw = callEB(t, svc, "DescribeRule", `{"Name":"r1"}`)
	if st != http.StatusOK || out["State"] != "ENABLED" || out["EventPattern"] != `{"source":["com.acme.new"]}` {
		t.Fatalf("DescribeRule r1 after re-put: status %d out %v (raw %s)", st, out, raw)
	}

	for _, pair := range [][2]string{
		{"s1", "rate(5 minutes)"},
		{"s2", "rate(1 hour)"},
		{"s3", "cron(0 10 * * ? *)"},
	} {
		body := fmt.Sprintf(`{"Name":%q,"ScheduleExpression":%q}`, pair[0], pair[1])
		st, _, raw = callEB(t, svc, "PutRule", body)
		if st != http.StatusOK {
			t.Fatalf("PutRule %s (schedule %s): status %d (raw %s)", pair[0], pair[1], st, raw)
		}
	}

	for _, expr := range []string{"rate(x)", "rate(5 minute)", "foo(5 minutes)"} {
		body := fmt.Sprintf(`{"Name":"broken","ScheduleExpression":%q}`, expr)
		wantEBError(t, svc, "PutRule", body, http.StatusBadRequest, "InvalidParameterException")
	}

	for _, pattern := range []string{"not json", `{"nope":["x"]}`, `{"source":"x"}`, `{"source":[]}`} {
		body := `{"Name":"broken","EventPattern":` + quoteEB(pattern) + `}`
		wantEBError(t, svc, "PutRule", body, http.StatusBadRequest, "InvalidEventPatternException")
	}

	wantEBError(t, svc, "PutRule", `{"Name":"r9"}`, http.StatusBadRequest, "InvalidParameterException")
	wantEBError(t, svc, "PutRule", `{"Name":"bad name","EventPattern":`+quoteEB(`{"source":["x"]}`)+`}`, http.StatusBadRequest, "InvalidParameterException")
	wantEBError(t, svc, "PutRule", fmt.Sprintf(`{"Name":"%s","EventPattern":%s}`, strings.Repeat("a", 65), quoteEB(`{"source":["x"]}`)), http.StatusBadRequest, "InvalidParameterException")
	wantEBError(t, svc, "PutRule", `{"Name":"r9","State":"PAUSED","EventPattern":`+quoteEB(`{"source":["x"]}`)+`}`, http.StatusBadRequest, "InvalidParameterException")
	wantEBError(t, svc, "PutRule", `{"Name":"r9","EventBusName":"ghost","EventPattern":`+quoteEB(`{"source":["x"]}`)+`}`, http.StatusBadRequest, "ResourceNotFoundException")

	st, _, raw = callEB(t, svc, "DeleteRule", `{"Name":"r2","EventBusName":"orders"}`)
	if st != http.StatusOK {
		t.Fatalf("DeleteRule r2: status %d (raw %s)", st, raw)
	}
	wantEBError(t, svc, "DescribeRule", `{"Name":"r2","EventBusName":"orders"}`, http.StatusBadRequest, "ResourceNotFoundException")
	wantEBError(t, svc, "DeleteRule", `{"Name":"r2","EventBusName":"orders"}`, http.StatusBadRequest, "ResourceNotFoundException")
	wantEBError(t, svc, "DeleteRule", `{"Name":"ghost-rule"}`, http.StatusBadRequest, "ResourceNotFoundException")
	wantEBError(t, svc, "EnableRule", `{"Name":"ghost-rule"}`, http.StatusBadRequest, "ResourceNotFoundException")
	wantEBError(t, svc, "DisableRule", `{"Name":"ghost-rule"}`, http.StatusBadRequest, "ResourceNotFoundException")
	wantEBError(t, svc, "DescribeRule", `{"Name":"ghost-rule"}`, http.StatusBadRequest, "ResourceNotFoundException")
	wantEBError(t, svc, "ListRules", `{"EventBusName":"ghost"}`, http.StatusBadRequest, "ResourceNotFoundException")
}

func TestEBListRulesPaging(t *testing.T) {
	svc := NewService("")
	for _, name := range []string{"beta", "alpha", "gamma"} {
		st, _, raw := callEB(t, svc, "PutRule", `{"Name":`+quoteEB(name)+`,"EventPattern":`+quoteEB(`{"source":["a"]}`)+`}`)
		if st != http.StatusOK {
			t.Fatalf("PutRule %q: status %d (raw %s)", name, st, raw)
		}
	}
	st, out, raw := callEB(t, svc, "ListRules", `{"Limit":2}`)
	if st != http.StatusOK || !sameEBNames(t, out["Rules"], "alpha", "beta") {
		t.Fatalf("ListRules page 1: status %d rules %v (raw %s)", st, out["Rules"], raw)
	}
	items, _ := out["Rules"].([]any)
	first, _ := items[0].(map[string]any)
	if first["Arn"] != svc.ruleARN(defaultBusName, "alpha") || first["State"] != "ENABLED" || first["EventBusName"] != "default" {
		t.Fatalf("rule item %v", first)
	}
	token, _ := out["NextToken"].(string)
	if token == "" {
		t.Fatalf("missing NextToken (raw %s)", raw)
	}
	st, out, raw = callEB(t, svc, "ListRules", fmt.Sprintf(`{"Limit":2,"NextToken":%q}`, token))
	if st != http.StatusOK || !sameEBNames(t, out["Rules"], "gamma") {
		t.Fatalf("ListRules page 2: status %d rules %v (raw %s)", st, out["Rules"], raw)
	}
	if _, present := out["NextToken"]; present {
		t.Fatalf("unexpected trailing NextToken (raw %s)", raw)
	}
	st, out, raw = callEB(t, svc, "ListRules", `{"NamePrefix":"al"}`)
	if st != http.StatusOK || !sameEBNames(t, out["Rules"], "alpha") {
		t.Fatalf("ListRules prefix: status %d rules %v (raw %s)", st, out["Rules"], raw)
	}
}

func TestEBPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "floci.json")
	svc := NewService(path)

	st, _, raw := callEB(t, svc, "CreateEventBus", `{"Name":"persist-bus"}`)
	if st != http.StatusOK {
		t.Fatalf("CreateEventBus: status %d (raw %s)", st, raw)
	}
	st, _, raw = callEB(t, svc, "PutRule", `{"Name":"persist-rule","EventBusName":"persist-bus","Description":"keep","EventPattern":`+quoteEB(`{"source":["p"]}`)+`}`)
	if st != http.StatusOK {
		t.Fatalf("PutRule: status %d (raw %s)", st, raw)
	}
	_, out, raw := callEB(t, svc, "DescribeEventBus", `{"Name":"persist-bus"}`)
	created, _ := out["CreationTime"].(float64)
	if created <= 0 {
		t.Fatalf("CreationTime = %v (raw %s)", out["CreationTime"], raw)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("store file not written: %v", err)
	}

	again := NewService(path)
	st, out, raw = callEB(t, again, "DescribeEventBus", `{"Name":"persist-bus"}`)
	if st != http.StatusOK || out["CreationTime"] != created {
		t.Fatalf("persisted bus: status %d out %v want created %v (raw %s)", st, out, created, raw)
	}
	_, out, raw = callEB(t, again, "ListRules", `{"EventBusName":"persist-bus"}`)
	rules, _ := out["Rules"].([]any)
	if len(rules) != 1 {
		t.Fatalf("persisted rules = %v (raw %s)", out["Rules"], raw)
	}
	item, _ := rules[0].(map[string]any)
	if item["Name"] != "persist-rule" || item["State"] != "ENABLED" || item["Description"] != "keep" {
		t.Fatalf("persisted rule item = %v", item)
	}

	st, _, raw = callEB(t, again, "DeleteEventBus", `{"Name":"persist-bus"}`)
	if st != http.StatusOK {
		t.Fatalf("DeleteEventBus: status %d (raw %s)", st, raw)
	}
	fresh := NewService(path)
	wantEBError(t, fresh, "DescribeEventBus", `{"Name":"persist-bus"}`, http.StatusBadRequest, "ResourceNotFoundException")
}

func TestEBWireProtocolEdges(t *testing.T) {
	svc := NewService("")

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	svc.ServeHTTP(w, r)
	var e map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil || w.Code != http.StatusBadRequest || e["__type"] != "UnsupportedOperationException" {
		t.Fatalf("GET: status %d body %s err %v", w.Code, w.Body.String(), err)
	}

	r = httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	w = httptest.NewRecorder()
	svc.ServeHTTP(w, r)
	if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil || e["__type"] != "MissingActionException" {
		t.Fatalf("missing target: body %s err %v", w.Body.String(), err)
	}

	r = httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	r.Header.Set("X-Amz-Target", "AWSEvents.PutEvents")
	w = httptest.NewRecorder()
	svc.ServeHTTP(w, r)
	if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil || e["__type"] != "UnknownOperationException" {
		t.Fatalf("unknown target: body %s err %v", w.Body.String(), err)
	}

	r = httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	r.Header.Set("X-Amz-Target", "com.amazonaws.eventbridge#DescribeEventBus")
	r.Header.Set("Content-Type", "application/x-amz-json-1.0")
	w = httptest.NewRecorder()
	svc.ServeHTTP(w, r)
	e = map[string]any{}
	if w.Code != http.StatusOK || w.Header().Get("Content-Type") != "application/x-amz-json-1.0" {
		t.Fatalf("protocol echo: status %d ct %q", w.Code, w.Header().Get("Content-Type"))
	}
	if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil || e["Name"] != "default" {
		t.Fatalf("protocol echo body: %s err %v", w.Body.String(), err)
	}
}
