package delivery

import (
    "errors"
    "testing"
)

func parsePolicy(t *testing.T, raw string) FilterPolicy {
    t.Helper()
    p, err := ParseFilterPolicy(raw)
    if err != nil {
        t.Fatalf("ParseFilterPolicy(%s): %v", raw, err)
    }
    return p
}

// attrs builds String-typed attributes from name/value pairs.
func attrs(kv ...string) map[string]MessageAttributeValue {
    m := make(map[string]MessageAttributeValue, len(kv)/2)
    for i := 0; i < len(kv); i += 2 {
        m[kv[i]] = MessageAttributeValue{DataType: "String", StringValue: strValue(kv[i+1])}
    }
    return m
}

func TestFilterPolicyZeroValueMatchesAll(t *testing.T) {
    var p FilterPolicy
    if !p.Empty() || !p.MatchAttributes(nil) {
        t.Fatal("zero FilterPolicy must match every message")
    }
}

func TestParseEmptyPolicy(t *testing.T) {
    p, err := ParseFilterPolicy("")
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    q, err := ParseFilterPolicy("{}")
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if !p.Empty() || !q.Empty() || !q.MatchAttributes(nil) {
        t.Fatal("empty policies must match every message")
    }
}

func TestFilterPolicyExact(t *testing.T) {
    p := parsePolicy(t, `{"store": "example_corp"}`)
    if !p.MatchAttributes(attrs("store", "example_corp")) {
        t.Error("expected match")
    }
    if p.MatchAttributes(attrs("store", "other")) {
        t.Error("unexpected match")
    }
    if p.MatchAttributes(nil) {
        t.Error("missing attribute must not match")
    }
}

func TestFilterPolicyMultipleKeysANDed(t *testing.T) {
    p := parsePolicy(t, `{"store": "example_corp", "event": "order_created"}`)
    if !p.MatchAttributes(attrs("store", "example_corp", "event", "order_created")) {
        t.Error("expected match")
    }
    if p.MatchAttributes(attrs("store", "example_corp")) {
        t.Error("missing second attribute must not match")
    }
    if p.MatchAttributes(attrs("store", "example_corp", "event", "other")) {
        t.Error("second condition must be required")
    }
}

func TestFilterPolicyAnyOfList(t *testing.T) {
    p := parsePolicy(t, `{"event": ["order_created", "order_updated"]}`)
    if !p.MatchAttributes(attrs("event", "order_created")) || !p.MatchAttributes(attrs("event", "order_updated")) {
        t.Error("expected list members to match")
    }
    if p.MatchAttributes(attrs("event", "order_deleted")) {
        t.Error("unexpected match outside list")
    }
}

func TestFilterPolicyExactNumber(t *testing.T) {
    p := parsePolicy(t, `{"count": 5}`)
    num := func(v string) map[string]MessageAttributeValue {
        return map[string]MessageAttributeValue{"count": {DataType: "Number", StringValue: strValue(v)}}
    }
    if !p.MatchAttributes(num("5")) {
        t.Error("expected numeric match")
    }
    if p.MatchAttributes(num("6")) {
        t.Error("unexpected numeric match")
    }
    if p.MatchAttributes(attrs("count", "5")) {
        t.Error("string attribute must not satisfy a numeric condition")
    }
}

func TestFilterPolicyAnythingBut(t *testing.T) {
    p := parsePolicy(t, `{"status": {"anything-but": "error"}}`)
    if !p.MatchAttributes(attrs("status", "ok")) {
        t.Error("expected match")
    }
    if p.MatchAttributes(attrs("status", "error")) {
        t.Error("excluded value must not match")
    }
    if p.MatchAttributes(nil) {
        t.Error("missing attribute must not match an anything-but condition")
    }

    list := parsePolicy(t, `{"status": {"anything-but": ["500", "503"]}}`)
    if !list.MatchAttributes(attrs("status", "200")) {
        t.Error("expected match")
    }
    if list.MatchAttributes(attrs("status", "503")) {
        t.Error("excluded value must not match")
    }
}

func TestFilterPolicyPrefixSuffix(t *testing.T) {
    prefix := parsePolicy(t, `{"url": {"prefix": "https://"}}`)
    if !prefix.MatchAttributes(attrs("url", "https://example.com")) {
        t.Error("expected prefix match")
    }
    if prefix.MatchAttributes(attrs("url", "http://example.com")) {
        t.Error("unexpected prefix match")
    }
    either := parsePolicy(t, `{"url": {"prefix": ["https://", "http://"]}}`)
    if !either.MatchAttributes(attrs("url", "http://example.com")) {
        t.Error("expected prefix list match")
    }
    suffix := parsePolicy(t, `{"event": {"suffix": "_created"}}`)
    if !suffix.MatchAttributes(attrs("event", "order_created")) {
        t.Error("expected suffix match")
    }
    if suffix.MatchAttributes(attrs("event", "order_deleted")) {
        t.Error("unexpected suffix match")
    }
}

func TestFilterPolicyNumeric(t *testing.T) {
    eq := parsePolicy(t, `{"price": {"numeric": ["=", 5]}}`)
    num := func(v string) map[string]MessageAttributeValue {
        return map[string]MessageAttributeValue{"price": {DataType: "Number", StringValue: strValue(v)}}
    }
    if !eq.MatchAttributes(num("5")) {
        t.Error("expected numeric equality")
    }
    if eq.MatchAttributes(num("6")) {
        t.Error("unexpected numeric equality")
    }
    rng := parsePolicy(t, `{"price": {"numeric": [">", 100, "<", 200]}}`)
    if !rng.MatchAttributes(num("150")) {
        t.Error("expected range match")
    }
    if rng.MatchAttributes(num("100")) || rng.MatchAttributes(num("200")) {
        t.Error("range bounds are exclusive")
    }
}

func TestFilterPolicyExists(t *testing.T) {
    has := parsePolicy(t, `{"opt": {"exists": true}}`)
    if !has.MatchAttributes(attrs("opt", "x")) {
        t.Error("existing attribute must match")
    }
    if has.MatchAttributes(nil) {
        t.Error("missing attribute must not match exists:true")
    }
    not := parsePolicy(t, `{"opt": {"exists": false}}`)
    if !not.MatchAttributes(nil) {
        t.Error("missing attribute must match exists:false")
    }
    if not.MatchAttributes(attrs("opt", "x")) {
        t.Error("existing attribute must not match exists:false")
    }
}

func TestFilterPolicyAnythingButWithPrefix(t *testing.T) {
    p := parsePolicy(t, `{"phrase": {"anything-but": {"prefix": "IT"}}}`)
    if p.MatchAttributes(attrs("phrase", "IT support")) {
        t.Error("value with excluded prefix must not match")
    }
    if !p.MatchAttributes(attrs("phrase", "HR support")) {
        t.Error("expected match")
    }
}

func TestFilterPolicyMatchBody(t *testing.T) {
    p := parsePolicy(t, `{"a": 1, "b": {"exists": true}}`)
    if !p.MatchBody(`{"a": 1, "b": "x"}`) {
        t.Error("expected body match")
    }
    if p.MatchBody(`{"a": 1}`) {
        t.Error("missing key must not match")
    }
    if p.MatchBody(`{"a": 2, "b": "x"}`) {
        t.Error("wrong value must not match")
    }
    if !p.MatchBody(`{"a":1,"b":"x"}`) {
        t.Error("compact JSON must behave the same")
    }

    tags := parsePolicy(t, `{"tags": ["red", "green"]}`)
    if !tags.MatchBody(`{"tags": ["red", "blue"]}`) {
        t.Error("expected array member match")
    }
    if tags.MatchBody(`{"tags": ["blue"]}`) {
        t.Error("unexpected array member match")
    }

    counts := parsePolicy(t, `{"count": {"numeric": [">", 1]}}`)
    if !counts.MatchBody(`{"count": 5}`) {
        t.Error("expected numeric body match")
    }
    absent := parsePolicy(t, `{"x": {"exists": false}}`)
    if !absent.MatchBody(`{"y": 1}`) {
        t.Error("exists:false must match when the key is absent")
    }
}

func TestFilterPolicyBodyInvalidJSON(t *testing.T) {
    p := parsePolicy(t, `{"a": 1}`)
    if p.MatchBody("not json") {
        t.Error("invalid body must not match a non-empty policy")
    }
}

func TestParseFilterPolicyInvalid(t *testing.T) {
    cases := []string{
        `[]`,                                            // not an object
        `{"a": true}`,                                   // bare boolean value
        `{"a": []}`,                                     // empty condition list
        `{"a": {"wat": 1}}`,                             // unknown operator
        `{"a": {"exists": "yes"}}`,                     // exists requires boolean
        `{"a": {"numeric": [">", 1, "<", 2, ">=", 3]}}`, // too many pairs
        `{"a": {"numeric": [">", ">"]}}`,               // operand must be number
        `{"a": {"prefix": 1}}`,                          // prefix requires string
    }
    for _, raw := range cases {
        if _, err := ParseFilterPolicy(raw); !errors.Is(err, ErrInvalidFilterPolicy) {
            t.Errorf("ParseFilterPolicy(%s) error = %v, want ErrInvalidFilterPolicy", raw, err)
        }
    }
}
