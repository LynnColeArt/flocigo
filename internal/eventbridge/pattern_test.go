package eventbridge

import (
	"encoding/json"
	"strings"
	"testing"
)

func matchJSON(t *testing.T, pattern, event string) bool {
	t.Helper()
	matched, err := MatchJSON([]byte(pattern), []byte(event))
	if err != nil {
		t.Fatalf("MatchJSON(%s, %s) returned error: %v", pattern, event, err)
	}
	return matched
}

func TestExactValueAndTopLevelFields(t *testing.T) {
	const event = `{"source":"aws.ec2","detail-type":"EC2 Instance State-change Notification","account":"123456789012","region":"us-east-1","detail":{"state":"running"}}`
	tests := []struct {
		pattern string
		want    bool
	}{
		{`{}`, true},
		{`{"source":["aws.ec2"]}`, true},
		{`{"source":["aws.sns"]}`, false},
		{`{"source":["aws.sns","aws.ec2"]}`, true},
		{`{"source":["AWS.ec2"]}`, false},
		{`{"source":"aws.ec2"}`, true},
		{`{"source":"aws.sns"}`, false},
		{`{"region":["us-east-1"]}`, true},
		{`{"region":["us-west-2"]}`, false},
		{`{"source":[]}`, false},
		{`{"nonexistent":["x"]}`, false},
		{`{"detail-type":["EC2 Instance State-change Notification"]}`, true},
		{`{"detail":{"state":["running","stopped"]}}`, true},
		{`{"detail":{"state":["stopped"]}}`, false},
		{`{"detail":{"state":"running"}}`, true},
		{`{"detail":{"unknown":["x"]}}`, false},
	}
	for _, test := range tests {
		if got := matchJSON(t, test.pattern, event); got != test.want {
			t.Errorf("pattern %s: match = %v, want %v", test.pattern, got, test.want)
		}
	}
}

func TestNestedDetail(t *testing.T) {
	const pattern = `{"detail":{"instance":{"tags":{"env":["prod"]}}}}`
	for _, event := range []string{
		`{"detail":{"instance":{"tags":{"env":"prod"}}}}`,
		`{"detail":{"instance":{"tags":{"env":"prod","team":"core"}}}}`,
	} {
		if !matchJSON(t, pattern, event) {
			t.Errorf("event %s should match %s", event, pattern)
		}
	}
	for _, event := range []string{
		`{"detail":{"instance":{"tags":{"env":"dev"}}}}`,
		`{"detail":{"instance":"prod"}}`,
		`{"detail":{"instance":{"tags":["prod"]}}}`,
		`{"source":"aws.ec2"}`,
	} {
		if matchJSON(t, pattern, event) {
			t.Errorf("event %s should not match %s", event, pattern)
		}
	}
}

func TestArrayFieldAndSingleElement(t *testing.T) {
	tests := []struct {
		pattern string
		event   string
		want    bool
	}{
		{
			pattern: `{"resources":["arn:aws:ec2:us-east-1:123456789012:instance/i-1"]}`,
			event:   `{"resources":["arn:aws:ec2:us-east-1:123456789012:instance/i-0","arn:aws:ec2:us-east-1:123456789012:instance/i-1"]}`,
			want:    true,
		},
		{
			pattern: `{"resources":["arn:aws:ec2:us-east-1:123456789012:instance/i-9"]}`,
			event:   `{"resources":["arn:aws:ec2:us-east-1:123456789012:instance/i-0","arn:aws:ec2:us-east-1:123456789012:instance/i-1"]}`,
			want:    false,
		},
		{
			pattern: `{"resources":["arn:aws:ec2:us-east-1:123456789012:instance/i-0"]}`,
			event:   `{"resources":"arn:aws:ec2:us-east-1:123456789012:instance/i-0"}`,
			want:    true,
		},
		{
			pattern: `{"detail":{"items":[{"type":["apple"]}]}}`,
			event:   `{"detail":{"items":[{"type":"pear"},{"type":"apple"}]}}`,
			want:    true,
		},
		{
			pattern: `{"detail":{"items":[{"type":["mango"]}]}}`,
			event:   `{"detail":{"items":[{"type":"pear"},{"type":"apple"}]}}`,
			want:    false,
		},
		{
			pattern: `{"detail":{"vals":[{"numeric":[">",2]}]}}`,
			event:   `{"detail":{"vals":[1,3]}}`,
			want:    true,
		},
		{
			pattern: `{"detail":{"vals":[{"numeric":[">",2]}]}}`,
			event:   `{"detail":{"vals":[1,"x"]}}`,
			want:    false,
		},
	}
	for _, test := range tests {
		if got := matchJSON(t, test.pattern, test.event); got != test.want {
			t.Errorf("pattern %s vs event %s: match = %v, want %v", test.pattern, test.event, got, test.want)
		}
	}
}

func TestNumericMatcher(t *testing.T) {
	const event = `{"detail":{"count":5,"price":10.5,"name":"5"}}`
	tests := []struct {
		pattern string
		want    bool
	}{
		{`{"detail":{"count":{"numeric":[">=",1,"<",10]}}}`, true},
		{`{"detail":{"count":{"numeric":[">",0,"<",5]}}}`, false},
		{`{"detail":{"count":{"numeric":["<",5]}}}`, false},
		{`{"detail":{"count":{"numeric":["=",5]}}}`, true},
		{`{"detail":{"count":{"numeric":["=",5.0]}}}`, true},
		{`{"detail":{"count":{"numeric":[">","0"]}}}`, false},
		{`{"detail":{"count":{"numeric":[">",0,"<"]}}}`, false},
		{`{"detail":{"count":{"numeric":"/>="}}}`, false},
		{`{"detail":{"count":{"numeric":"/>="}}}`, false},
		{`{"detail":{"name":{"numeric":[">",0]}}}`, false},
		{`{"detail":{"price":{"numeric":[">",10,"<",11]}}}`, true},
		{`{"detail":{"missing":{"numeric":[">",0]}}}`, false},
	}
	for _, test := range tests {
		if got := matchJSON(t, test.pattern, event); got != test.want {
			t.Errorf("pattern %s: match = %v, want %v", test.pattern, got, test.want)
		}
	}
}

func TestPrefixMatcher(t *testing.T) {
	const event = `{"source":"aws.ec2","detail":{"arn":"arn:aws:ec2:us-east-1:123456789012:instance/i-1"}}`
	tests := []struct {
		pattern string
		want    bool
	}{
		{`{"source":{"prefix":"aws."}}`, true},
		{`{"source":{"prefix":"com."}}`, false},
		{`{"source":{"prefix":1}}`, false},
		{`{"detail":{"arn":{"prefix":"arn:aws:ec2:"}}}`, true},
		{`{"detail":{"arn":{"prefix":"arn:aws:s3:"}}}`, false},
		{`{"detail":{"arn":{"prefix":"arn"}}}`, true},
		{`{"detail":{"missing":{"prefix":"arn"}}}`, false},
	}
	for _, test := range tests {
		if got := matchJSON(t, test.pattern, event); got != test.want {
			t.Errorf("pattern %s: match = %v, want %v", test.pattern, got, test.want)
		}
	}
}

func TestExistsMatcher(t *testing.T) {
	tests := []struct {
		pattern string
		event   string
		want    bool
	}{
		{`{"detail":{"foo":{"exists":true}}}`, `{"detail":{"foo":"bar"}}`, true},
		{`{"detail":{"foo":{"exists":true}}}`, `{"detail":{"bar":"baz"}}`, false},
		{`{"detail":{"foo":{"exists":true}}}`, `{"detail":{"foo":null}}`, true},
		{`{"detail":{"foo":{"exists":false}}}`, `{"detail":{"bar":"baz"}}`, true},
		{`{"detail":{"foo":{"exists":false}}}`, `{"detail":{"foo":"bar"}}`, false},
		{`{"detail":{"foo":{"exists":false}}}`, `{"detail":{"foo":null}}`, false},
	}
	for _, test := range tests {
		if got := matchJSON(t, test.pattern, test.event); got != test.want {
			t.Errorf("pattern %s vs event %s: match = %v, want %v", test.pattern, test.event, got, test.want)
		}
	}
}

func TestNumberEncodingEquivalence(t *testing.T) {
	tests := []struct {
		pattern string
		event   string
		want    bool
	}{
		{`{"detail":{"n":[1]}}`, `{"detail":{"n":1.0}}`, true},
		{`{"detail":{"n":[1.0]}}`, `{"detail":{"n":1}}`, true},
		{`{"detail":{"n":[1000]}}`, `{"detail":{"n":1e3}}`, true},
		{`{"detail":{"n":[1]}}`, `{"detail":{"n":"1"}}`, false},
		{`{"detail":{"n":[true]}}`, `{"detail":{"n":true}}`, true},
		{`{"detail":{"n":[true]}}`, `{"detail":{"n":"true"}}`, false},
		{`{"detail":{"n":[null]}}`, `{"detail":{"n":null}}`, true},
	}
	for _, test := range tests {
		if got := matchJSON(t, test.pattern, test.event); got != test.want {
			t.Errorf("pattern %s vs event %s: match = %v, want %v", test.pattern, test.event, got, test.want)
		}
	}
}

func TestMatchesOnDecodedMaps(t *testing.T) {
	var pattern map[string]any
	decoder := json.NewDecoder(strings.NewReader(`{"detail":{"n":[1]}}`))
	decoder.UseNumber()
	if err := decoder.Decode(&pattern); err != nil {
		t.Fatalf("decode pattern: %v", err)
	}
	var event map[string]any // decoded without UseNumber, so values are float64
	if err := json.Unmarshal([]byte(`{"detail":{"n":1}}`), &event); err != nil {
		t.Fatalf("decode event: %v", err)
	}
	if !Matches(pattern, event) {
		t.Fatal("json.Number pattern value should match a float64 event value")
	}
	if !Matches(nil, map[string]any{"source": "aws.ec2"}) {
		t.Fatal("an empty pattern must match any event")
	}
	if Matches(map[string]any{"source": []any{"aws.ec2"}}, nil) {
		t.Fatal("a nil event must not satisfy a non-empty pattern")
	}
}

func TestMatchJSONErrors(t *testing.T) {
	if _, err := MatchJSON([]byte(`{"detail":`), []byte(`{}`)); err == nil {
		t.Fatal("expected error decoding malformed pattern JSON")
	}
	if _, err := MatchJSON([]byte(`[]`), []byte(`{}`)); err == nil {
		t.Fatal("expected error when the pattern is not a JSON object")
	}
	if _, err := MatchJSON([]byte(`{}`), []byte(`"event"`)); err == nil {
		t.Fatal("expected error when the event is not a JSON object")
	}
}
