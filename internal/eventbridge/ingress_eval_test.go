package eventbridge

// ingress_eval_test.go — self-contained checks for the ticket-029 matching,
// validation, storage and input-resolution helpers. They depend on no other
// file of the package beyond the ticket-029 implementation itself.

import (
    "bytes"
    "encoding/json"
    "strings"
    "testing"
)

func ingressMustFilter(t *testing.T, pattern string) *eventFilter {
    t.Helper()
    f, err := compileEventFilter(pattern)
    if err != nil {
        t.Fatalf("compile %q: %v", pattern, err)
    }
    return f
}

func ingressDecodeEvent(t *testing.T, event string) map[string]any {
    t.Helper()
    var m map[string]any
    dec := json.NewDecoder(strings.NewReader(event))
    dec.UseNumber()
    if err := dec.Decode(&m); err != nil {
        t.Fatalf("decode %q: %v", event, err)
    }
    return m
}

func TestIngressFilterMatching(t *testing.T) {
    event := ingressDecodeEvent(t, `{"source":"app.dev","detail":{"state":"running","n":3,"tags":["a","b"]}}`)
    cases := []struct {
        pattern string
        want    bool
    }{
        {`{"source":["app.dev","other"]}`, true},
        {`{"source":"other"}`, false},
        {`{}`, true},
        {`{"detail":{"state":["running"]}}`, true},
        {`{"detail":{"n":[{"numeric":[">",2,"<",10]}]}}`, true},
        {`{"detail":{"n":[{"numeric":[">",10]}]}}`, false},
        {`{"source":[{"anything-but":"app.dev"}]}`, false},
        {`{"source":[{"anything-but":["x","y"]}]}`, true},
        {`{"source":[{"prefix":"app"}]}`, true},
        {`{"source":[{"suffix":"dev"}]}`, true},
        {`{"source":{"exists":true}}`, true},
        {`{"nope":{"exists":false}}`, true},
        {`{"nope":{"exists":true}}`, false},
        {`{"$or":[{"source":"x"},{"source":[{"prefix":"app"}]}]}`, true},
        {`{"detail":{"state":[{"wildcard":"run*"}]}}`, true},
        {`{"detail":{"tags":["c"]}}`, false},
        {`{"detail":{"tags":"a"}}`, false},
    }
    for _, tc := range cases {
        f := ingressMustFilter(t, tc.pattern)
        if got := f.Matches(event); got != tc.want {
            t.Errorf("pattern %s: got %v, want %v", tc.pattern, got, tc.want)
        }
    }
}

func TestIngressFilterValidation(t *testing.T) {
    bad := []string{
        `{"nosuchfield":"x"}`,
        `{"source":{"exists":true,"prefix":"a"}}`,
        `{"source":{"numeric":[">",1,">"]}}`,
        `{"source":{"numeric":[">","x"]}}`,
        `{"source":[]}`,
        `{"detail":{}}`,
        `[]`,
        `{"$or":[]}`,
        `{"$or":["nope"]}`,
    }
    for _, p := range bad {
        if _, err := compileEventFilter(p); err == nil {
            t.Errorf("expected invalid pattern: %s", p)
        }
    }
    if _, err := compileEventFilter(`{"source":{"prefix":"a"}}`); err != nil {
        t.Errorf("expected valid pattern, got: %v", err)
    }
}

func TestIngressTargetTable(t *testing.T) {
    tt := newTargetTable()
    tt.upsert("default", "r", &Target{Id: "t2", Arn: "arn:aws:sqs:us-east-1:123456789012:q"})
    tt.upsert("default", "r", &Target{Id: "t1", Arn: "arn:aws:sqs:us-east-1:123456789012:q2"})
    tt.upsert("default", "r", &Target{Id: "t2", Arn: "arn:aws:sqs:us-east-1:123456789012:q3"})
    got := tt.list("default", "r")
    if len(got) != 2 || got[0].Id != "t2" || got[0].Arn != "arn:aws:sqs:us-east-1:123456789012:q3" || got[1].Id != "t1" {
        t.Fatalf("unexpected list: %+v", got)
    }
    if !tt.remove("default", "r", "t1") || tt.remove("default", "r", "t1") {
        t.Errorf("remove semantics broken")
    }
    if len(tt.list("default", "r")) != 1 {
        t.Errorf("unexpected count after remove")
    }
}

func TestIngressInputResolution(t *testing.T) {
    envJSON := []byte(`{"detail":{"x":"y"},"source":"app.dev"}`)
    var env map[string]any
    if err := json.Unmarshal(envJSON, &env); err != nil {
        t.Fatal(err)
    }
    body, _, ok := resolveIngressInput(&Target{Id: "t", Arn: "arn:aws:sqs:us-east-1:123456789012:q", Input: `{"hello":"world"}`}, env, envJSON)
    if !ok || string(body) != `{"hello":"world"}` {
        t.Errorf("literal Input: %q ok=%v", body, ok)
    }
    body, _, ok = resolveIngressInput(&Target{Id: "t", Arn: "arn:aws:sqs:us-east-1:123456789012:q", InputPath: "$.detail.x"}, env, envJSON)
    if !ok || !bytes.Equal(body, []byte(`"y"`)) {
        t.Errorf("InputPath: %q ok=%v", body, ok)
    }
    if _, ok := resolveIngressInput(&Target{Id: "t", Arn: "arn:aws:sqs:us-east-1:123456789012:q", InputPath: "$.detail.zzz"}, env, envJSON); ok {
        t.Errorf("missing InputPath should not resolve")
    }
    if got := renderIngressTemplate(&InputTransformer{InputPathsMap: map[string]string{"s": "$.source"}, InputTemplate: "hi <s>!"}, env); got != "hi app.dev!" {
        t.Errorf("template: %q", got)
    }
}

func TestIngressPatternAPI(t *testing.T) {
    s := &Service{}
    out, err := s.TestEventPattern(&TestEventPatternInput{
        EventPattern: `{"source":["a.b"]}`,
        Event:        `{"source":"a.b","detail":{}}`,
    })
    if err != nil {
        t.Fatalf("TestEventPattern: %v", err)
    }
    if out == nil || !out.Event {
        t.Errorf("expected match, got %+v", out)
    }
    out, err = s.TestEventPattern(&TestEventPatternInput{EventPattern: `{"bogus":1}`, Event: `{}`})
    if err == nil || out != nil {
        t.Errorf("expected invalid pattern error")
    }
}
