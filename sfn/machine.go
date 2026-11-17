package sfn

import (
	"fmt"
	"strings"
	"time"
)

type machine struct {
	name       string
	startAt    string
	states     map[string]*stateDef
	timeoutSec float64
	hasTimeout bool
}

type stateDef struct {
	name                 string
	typ                  string
	next                 string
	end                  bool
	comment              string
	inputPath            *string // nil: absent (default ""), "": null
	resultPath           *string
	outputPath           *string
	params               map[string]any
	hasParams            bool
	result               any
	hasResult            bool
	resultSelector       map[string]any
	resource             string
	timeoutSec           float64
	hasTimeout           bool
	heartbeatSec         float64
	hasHeartbeat         bool
	retry                []*retryRule
	catch                []*catchRule
	waitSeconds          float64
	hasWaitSeconds       bool
	waitSecondsPath      string
	hasWaitSecondsPath   bool
	waitTimestamp        string
	hasWaitTimestamp     bool
	waitTimestampPath    string
	hasWaitTimestampPath bool
	choices              []*choiceNode
	hasDefault           bool
	defaultNext          string
	failError            string
	failCause            string
	branches             []*machine
	iterator             *machine
	itemsPath            *string
	itemSelector         map[string]any
}

type retryRule struct {
	errors      []string
	interval    float64
	maxAttempts int
	backoff     float64
}

type catchRule struct {
	errors     []string
	next       string
	resultPath *string
}

type choiceNode struct {
	variable   string
	hasVar     bool
	op         string
	value      any
	ignoreCase bool
	andNodes   []*choiceNode
	orNodes    []*choiceNode
	notNode    *choiceNode
	next       string
}

var validStateTypes = map[string]bool{
	"Pass": true, "Task": true, "Choice": true, "Wait": true,
	"Succeed": true, "Fail": true, "Parallel": true, "Map": true,
}

func floatField(raw map[string]any, key string) (float64, bool, error) {
	v, ok := raw[key]
	if !ok {
		return 0, false, nil
	}
	f, ok2 := asFloat(v)
	if !ok2 {
		return 0, false, fmt.Errorf("%q must be a number", key)
	}
	return f, true, nil
}

func intField(raw map[string]any, key string) (int64, bool, error) {
	v, ok := raw[key]
	if !ok {
		return 0, false, nil
	}
	n, ok2 := asNumber(v)
	if !ok2 {
		return 0, false, fmt.Errorf("%q must be a number", key)
	}
	i, iok := numberInt64(n)
	if !iok {
		return 0, false, fmt.Errorf("%q must be an integer", key)
	}
	return i, true, nil
}

// pathField parses an optional path field. Absent -> (nil,false,nil);
// JSON null -> pointer to "" (the discard/epty-input marker); otherwise the
// string must be a valid reference path. Note: emty is intentional placeholder.
func pathField(raw map[string]any, key string) (*string, bool, error) {
	v, ok := raw[key]
	if !ok {
		return nil, false, nil
	}
	if v == nil {
		s := ""
		return &s, true, nil
	}
	s, ok := v.(string)
	if !ok {
		return nil, false, fmt.Errorf("%q must be a string or null", key)
	}
	if _, _, err := parsePathBody(s); err != nil {
		return nil, false, fmt.Errorf("%q is not a valid reference path: %q", key, s)
	}
	return &s, true, nil
}

func stringSlice(raw map[string]any, key string) ([]string, error) {
	v, ok := raw[key]
	if !ok {
		return nil, nil
	}
	arr, ok2 := v.([]any)
	if !ok2 {
		return nil, fmt.Errorf("%q must be an array of strings", key)
	}
	out := make([]string, 0, len(arr))
	for _, e := range arr {
		s, ok3 := e.(string)
		if !ok3 {
			return nil, fmt.Errorf("%q must be an array of strings", key)
		}
		out = append(out, s)
	}
	return out, nil
}

func parseMachine(name string, raw map[string]any) (*machine, error) {
	m := &machine{name: name}
	startAt, _ := raw["StartAt"].(string)
	if startAt == "" {
		return nil, fmt.Errorf("machine %q requires a %q", name, "StartAt")
	}
	m.startAt = startAt
	statesRaw, ok := raw["States"].(map[string]any)
	if !ok || len(statesRaw) == 0 {
		return nil, fmt.Errorf("machine %q requires a non-empty %q object", name, "States")
	}
	if f, has, err := floatField(raw, "TimeoutSeconds"); err != nil {
		return nil, err
	} else if has {
		if f <= 0 {
			return nil, fmt.Errorf("machine %q: %q must be greater than zero", name, "TimeoutSeconds")
		}
		m.timeoutSec, m.hasTimeout = f, true
	}
	m.states = make(map[string]*stateDef, len(statesRaw))
	for sname, sraw := range statesRaw {
		sm, ok2 := sraw.(map[string]any)
		if !ok2 {
			return nil, fmt.Errorf("state %q must be a JSON object", sname)
		}
		sd, err := parseState(sname, sm)
		if err != nil {
			return nil, fmt.Errorf("state %q: %v", sname, err)
		}
		m.states[sname] = sd
	}
	if _, ok := m.states[m.startAt]; !ok {
		return nil, fmt.Errorf("%q %q does not reference a state", "StartAt", m.startAt)
	}
	for _, sd := range m.states {
		if sd.next != "" {
			if _, ok := m.states[sd.next]; !ok {
				return nil, fmt.Errorf("state %q: %q %q does not reference a state", sd.name, "Next", sd.next)
			}
		}
		if sd.hasDefault {
			if _, ok := m.states[sd.defaultNext]; !ok {
				return nil, fmt.Errorf("state %q: %q %q does not reference a state", sd.name, "Default", sd.defaultNext)
			}
		}
		for _, cr := range sd.catch {
			if _, ok := m.states[cr.next]; !ok {
				return nil, fmt.Errorf("state %q: Catch %q %q does not reference a state", sd.name, "Next", cr.next)
			}
		}
		for _, cn := range sd.choices {
			if cn.next != "" {
				if _, ok := m.states[cn.next]; !ok {
					return nil, fmt.Errorf("state %q: Choice %q %q does not reference a state", sd.name, "Next", cn.next)
				}
			}
		}
	}
	return m, nil
}

func parseState(name string, raw map[string]any) (*stateDef, error) {
	sd := &stateDef{name: name}
	typ, _ := raw["Type"].(string)
	if !validStateTypes[typ] {
		return nil, fmt.Errorf("invalid state Type %q", typ)
	}
	sd.typ = typ
	if v, ok := raw["End"]; ok {
		b, ok2 := v.(bool)
		if !ok2 {
			return nil, fmt.Errorf("%q must be a boolean", "End")
		}
		sd.end = b
	}
	if v, ok := raw["Next"]; ok {
		s, ok2 := v.(string)
		if !ok2 || s == "" {
			return nil, fmt.Errorf("%q must be a non-empty string", "Next")
		}
		if sd.end {
			return nil, fmt.Errorf("a state cannot declare both %q and %q", "End", "Next")
		}
		sd.next = s
	}
	if p, ok, err := pathField(raw, "InputPath"); err != nil {
		return nil, err
	} else if ok {
		sd.inputPath = p
	}
	if p, ok, err := pathField(raw, "ResultPath"); err != nil {
		return nil, err
	} else if ok {
		sd.resultPath = p
	}
	if p, ok, err := pathField(raw, "OutputPath"); err != nil {
		return nil, err
	} else if ok {
		sd.outputPath = p
	}
	if v, ok := raw["Parameters"]; ok {
		m, ok2 := v.(map[string]any)
		if !ok2 {
			return nil, fmt.Errorf("%q must be a JSON object (payload template)", "Parameters")
		}
		sd.params, sd.hasParams = m, true
	}
	if v, ok := raw["Result"]; ok {
		sd.result, sd.hasResult = v, true
	}
	if v, ok := raw["ResultSelector"]; ok {
		m, ok2 := v.(map[string]any)
		if !ok2 {
			return nil, fmt.Errorf("%q must be a JSON object", "ResultSelector")
		}
		sd.resultSelector = m
	}
	if c, ok := raw["Comment"]; ok {
		sd.comment, _ = c.(string)
	}
	if v, ok := raw["Retry"]; ok {
		arr, ok2 := v.([]any)
		if !ok2 {
			return nil, fmt.Errorf("%q must be an array", "Retry")
		}
		for i, r := range arr {
			rm, ok3 := r.(map[string]any)
			if !ok3 {
				return nil, fmt.Errorf("Retry[%d] must be an object", i)
			}
			rr, err := parseRetryRule(rm)
			if err != nil {
				return nil, fmt.Errorf("Retry[%d]: %v", i, err)
			}
			sd.retry = append(sd.retry, rr)
		}
	}
	if v, ok := raw["Catch"]; ok {
		arr, ok2 := v.([]any)
		if !ok2 {
			return nil, fmt.Errorf("%q must be an array", "Catch")
		}
		for i, r := range arr {
			cm, ok3 := r.(map[string]any)
			if !ok3 {
				return nil, fmt.Errorf("Catch[%d] must be an object", i)
			}
			cr, err := parseCatchRule(cm)
			if err != nil {
				return nil, fmt.Errorf("Catch[%d]: %v", i, err)
			}
			sd.catch = append(sd.catch, cr)
		}
	}
	switch typ {
	case "Task":
		res, _ := raw["Resource"].(string)
		if res == "" {
			return nil, fmt.Errorf("Task states require a non-empty %q", "Resource")
		}
		sd.resource = res
		if t, has, err := floatField(raw, "TimeoutSeconds"); err != nil {
			return nil, err
		} else if has {
			if t <= 0 {
				return nil, fmt.Errorf("%q must be greater than zero", "TimeoutSeconds")
			}
			sd.timeoutSec, sd.hasTimeout = t, true
		}
		if t, has, err := floatField(raw, "HeartbeatSeconds"); err != nil {
			return nil, err
		} else if has {
			if t <= 0 {
				return nil, fmt.Errorf("%q must be greater than zero", "HeartbeatSeconds")
			}
			sd.heartbeatSec, sd.hasHeartbeat = t, true
		}
	case "Choice":
		choices, ok := raw["Choices"].([]any)
		if !ok || len(choices) == 0 {
			return nil, fmt.Errorf("Choice states require a non-empty %q array", "Choices")
		}
		for i, cr := range choices {
			cm, ok2 := cr.(map[string]any)
			if !ok2 {
				return nil, fmt.Errorf("Choices[%d] must be an object", i)
			}
			cn, err := parseChoiceRule(cm, true)
			if err != nil {
				return nil, fmt.Errorf("Choices[%d]: %v", i, err)
			}
			sd.choices = append(sd.choices, cn)
		}
		if v, ok := raw["Default"]; ok {
			s, ok2 := v.(string)
			if !ok2 || s == "" {
				return nil, fmt.Errorf("%q must be a non-empty string", "Default")
			}
			sd.hasDefault, sd.defaultNext = true, s
		}
	case "Wait":
		directives := 0
		if f, has, err := floatField(raw, "Seconds"); err != nil {
			return nil, err
		} else if has {
			if f < 0 {
				return nil, fmt.Errorf("%q must not be negative", "Seconds")
			}
			sd.waitSeconds, sd.hasWaitSeconds = f, true
			directives++
		}
		if v, ok := raw["SecondsPath"]; ok {
			if v == nil {
				return nil, fmt.Errorf("%q must be a reference path string", "SecondsPath")
			}
			s, ok2 := v.(string)
			if !ok2 {
				return nil, fmt.Errorf("%q must be a reference path string", "SecondsPath")
			}
			if _, _, perr := parsePathBody(s); perr != nil {
				return nil, fmt.Errorf("%q %q is not a valid reference path", "SecondsPath", s)
			}
			sd.waitSecondsPath, sd.hasWaitSecondsPath = s, true
			directives++
		}
		if v, ok := raw["Timestamp"]; ok {
			s, ok2 := v.(string)
			if !ok2 {
				return nil, fmt.Errorf("%q must be a string", "Timestamp")
			}
			if _, tok := parseTimestamp(s); !tok {
				return nil, fmt.Errorf("%q %q is not a valid RFC3339 timestamp", "Timestamp", s)
			}
			sd.waitTimestamp, sd.hasWaitTimestamp = s, true
			directives++
		}
		if v, ok := raw["TimestampPath"]; ok {
			if v == nil {
				return nil, fmt.Errorf("%q must be a reference path string", "TimestampPath")
			}
			s, ok2 := v.(string)
			if !ok2 {
				return nil, fmt.Errorf("%q must be a reference path string", "TimestampPath")
			}
			if _, _, perr := parsePathBody(s); perr != nil {
				return nil, fmt.Errorf("%q %q is not a valid reference path", "TimestampPath", s)
			}
			sd.waitTimestampPath, sd.hasWaitTimestampPath = s, true
			directives++
		}
		if directives != 1 {
			return nil, fmt.Errorf("Wait state %q requires exactly one of Seconds, SecondsPath, Timestamp, TimestampPath", name)
		}
	case "Succeed":
		if sd.next != "" || sd.end {
			return nil, fmt.Errorf("Succeed states must not declare %q or %q", "Next", "End")
		}
	case "Fail":
		e, _ := raw["Error"].(string)
		if e == "" {
			return nil, fmt.Errorf("Fail states require a non-empty %q", "Error")
		}
		sd.failError = e
		if c, ok := raw["Cause"]; ok {
			sd.failCause, _ = c.(string)
		}
	case "Parallel":
		branches, ok := raw["Branches"].([]any)
		if !ok || len(branches) == 0 {
			return nil, fmt.Errorf("Parallel states require a non-empty %q array", "Branches")
		}
		for i, b := range branches {
			bm, ok2 := b.(map[string]any)
			if !ok2 {
				return nil, fmt.Errorf("Branches[%d] must be an object", i)
			}
			bmachine, err := parseMachine(fmt.Sprintf("%s.branch%d", name, i), bm)
			if err != nil {
				return nil, err
			}
			sd.branches = append(sd.branches, bmachine)
		}
	case "Map":
		it, ok := raw["Iterator"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("Map states require an %q object", "Iterator")
		}
		im, err := parseMachine(name+".Iterator", it)
		if err != nil {
			return nil, err
		}
		sd.iterator = im
		if p, has, err := pathField(raw, "ItemsPath"); err != nil {
			return nil, err
		} else if has {
			sd.itemsPath = p
		}
		if v, ok := raw["ItemSelector"]; ok {
			m2, ok2 := v.(map[string]any)
			if !ok2 {
				return nil, fmt.Errorf("%q must be a JSON object (payload template)", "ItemSelector")
			}
			sd.itemSelector = m2
		}
		if _, _, err := intField(raw, "MaxConcurrency"); err != nil {
			return nil, err
		}
		// MaxConcurrency is accepted but ignored: iterations run sequentially.
	case "Pass":
		// nothing extra
	}
	switch typ {
	case "Pass", "Task", "Wait", "Parallel", "Map":
		if sd.next == "" && !sd.end {
			return nil, fmt.Errorf("%s state %q must declare %q or %q: true", typ, name, "Next", "End")
		}
	}
	return sd, nil
}

func parseRetryRule(rm map[string]any) (*retryRule, error) {
	r := &retryRule{interval: 1, maxAttempts: 3, backoff: 2}
	errs, err := stringSlice(rm, "ErrorEquals")
	if err != nil {
		return nil, err
	}
	r.errors = errs
	if f, has, ferr := floatField(rm, "IntervalSeconds"); ferr != nil {
		return nil, ferr
	} else if has {
		if f < 0 {
			return nil, fmt.Errorf("%q must not be negative", "IntervalSeconds")
		}
		r.interval = f
	}
	if i, has, ferr := intField(rm, "MaxAttempts"); ferr != nil {
		return nil, ferr
	} else if has {
		if i < 0 {
			return nil, fmt.Errorf("%q must not be negative", "MaxAttempts")
		}
		r.maxAttempts = int(i)
	}
	if f, has, ferr := floatField(rm, "BackoffRate"); ferr != nil {
		return nil, ferr
	} else if has {
		r.backoff = f
	}
	return r, nil
}

func parseCatchRule(cm map[string]any) (*catchRule, error) {
	c := &catchRule{}
	errs, err := stringSlice(cm, "ErrorEquals")
	if err != nil {
		return nil, err
	}
	c.errors = errs
	next, _ := cm["Next"].(string)
	if next == "" {
		return nil, fmt.Errorf("Catch rules require a %q", "Next")
	}
	c.next = next
	if p, has, err := pathField(cm, "ResultPath"); err != nil {
		return nil, err
	} else if has {
		c.resultPath = p
	}
	return c, nil
}

// ---------------------------------------------------------------------------
// Choice evaluation
// ---------------------------------------------------------------------------

var choiceOps = []string{
	"StringEquals", "StringEqualsPath",
	"StringLessThan", "StringLessThanPath",
	"StringLessThanOrEqual", "StringLessThanOrEqualPath",
	"StringGreaterThan", "StringGreaterThanPath",
	"StringGreaterThanOrEqual", "StringGreaterThanOrEqualPath",
	"NumericEquals", "NumericEqualsPath",
	"NumericLessThan", "NumericLessThanPath",
	"NumericGreaterThan", "NumericGreaterThanPath",
	"BooleanEquals", "BooleanEqualsPath",
	"TimestampEquals", "TimestampEqualsPath",
	"TimestampLessThan", "TimestampLessThanPath",
	"TimestampLessThanOrEqual", "TimestampLessThanOrEqualPath",
	"TimestampGreaterThan", "TimestampGreaterThanPath",
	"TimestampGreaterThanOrEqual", "TimestampGreaterThanOrEqualPath",
	"IsPresent", "IsNull", "IsString", "IsNumeric", "IsBoolean", "IsTimestamp",
}

func parseChoiceRule(raw map[string]any, topLevel bool) (*choiceNode, error) {
	n := &choiceNode{}
	if v, ok := raw["Variable"]; ok {
		s, ok2 := v.(string)
		if !ok2 {
			return nil, fmt.Errorf("%q must be a string reference path", "Variable")
		}
		if _, _, err := parsePathBody(s); err != nil {
			return nil, fmt.Errorf("%q %q is not a valid reference path", "Variable", s)
		}
		n.hasVar, n.variable = true, s
	}
	if aRaw, ok := raw["And"]; ok {
		n.op = "And"
		kids, err := parseChoiceChildren(aRaw)
		if err != nil {
			return nil, err
		}
		n.andNodes = kids
	} else if oRaw, ok := raw["Or"]; ok {
		n.op = "Or"
		kids, err := parseChoiceChildren(oRaw)
		if err != nil {
			return nil, err
		}
		n.orNodes = kids
	} else if ntRaw, ok := raw["Not"]; ok {
		n.op = "Not"
		nm, ok2 := ntRaw.(map[string]any)
		if !ok2 {
			return nil, fmt.Errorf("%q must be a choice rule object", "Not")
		}
		kid, err := parseChoiceRule(nm, false)
		if err != nil {
			return nil, err
		}
		n.notNode = kid
	} else {
		found := ""
		for _, op := range choiceOps {
			if v, ok := raw[op]; ok {
				if found != "" {
					return nil, fmt.Errorf("choice rule declares multiple comparison operators")
				}
				found = op
				n.value = v
			}
		}
		if found == "" {
			return nil, fmt.Errorf("choice rule must declare one comparison operator, %q, %q or %q", "And", "Or", "Not")
		}
		if !n.hasVar {
			return nil, fmt.Errorf("choice rule requires a %q", "Variable")
		}
		n.op = found
		if strings.HasSuffix(found, "Path") {
			s, ok := n.value.(string)
			if !ok {
				return nil, fmt.Errorf("%q comparison value must be a string reference path", found)
			}
			if _, _, err := parsePathBody(s); err != nil {
				return nil, fmt.Errorf("%q comparison value %q is not a valid reference path", found, s)
			}
		}
	}
	if ic, ok := raw["IgnoreCase"].(bool); ok {
		n.ignoreCase = ic
	}
	if v, ok := raw["Next"]; ok {
		s, ok2 := v.(string)
		if !ok2 || s == "" {
			return nil, fmt.Errorf("choice rule %q must be a non-empty string", "Next")
		}
		if !topLevel {
			return nil, fmt.Errorf("nested choice rules must not declare %q", "Next")
		}
		n.next = s
	}
	if topLevel && n.next == "" {
		return nil, fmt.Errorf("top-level choice rules require a %q", "Next")
	}
	return n, nil
}

func parseChoiceChildren(v any) ([]*choiceNode, error) {
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("%q/%q must be arrays of choice rules", "And", "Or")
	}
	kids := make([]*choiceNode, 0, len(arr))
	for _, e := range arr {
		em, ok2 := e.(map[string]any)
		if !ok2 {
			return nil, fmt.Errorf("%q/%q entries must be choice rule objects", "And", "Or")
		}
		kid, err := parseChoiceRule(em, false)
		if err != nil {
			return nil, err
		}
		kids = append(kids, kid)
	}
	return kids, nil
}

func boolLit(v any) bool {
	b, _ := v.(bool)
	return b
}

func isNumberValue(v any) bool {
	_, ok := asNumber(v)
	return ok
}

func opIsPathVariant(op string) bool { return strings.HasSuffix(op, "Path") }

func asStringOr(v any) string {
	s, _ := v.(string)
	return s
}

func parseTimestamp(s string) (time.Time, bool) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// evalChoice evaluates one choice rule; unresolved paths make a comparison
// false (except IsPresent, which checks existence).
func evalChoice(n *choiceNode, data any, cx any) bool {
	switch n.op {
	case "And":
		for _, kid := range n.andNodes {
			if !evalChoice(kid, data, cx) {
				return false
			}
		}
		return true
	case "Or":
		for _, kid := range n.orNodes {
			if evalChoice(kid, data, cx) {
				return true
			}
		}
		return false
	case "Not":
		return !evalChoice(n.notNode, data, cx)
	}
	v, present := resolveOptional(data, cx, n.variable)
	want := boolLit(n.value)
	switch n.op {
	case "IsPresent":
		return present == want
	case "IsNull":
		return present && (v == nil) == want
	case "IsString":
		_, ok := v.(string)
		return present && ok == want
	case "IsNumeric":
		return present && isNumberValue(v) == want
	case "IsBoolean":
		_, ok := v.(bool)
		return present && ok == want
	case "IsTimestamp":
		if s, ok := v.(string); ok && present {
			_, parseOK := parseTimestamp(s)
			return parseOK == want
		}
		return false
	}
	if !present {
		return false
	}
	cmp := n.value
	if opIsPathVariant(n.op) {
		lit, litOK := resolveOptional(data, cx, asStringOr(n.value))
		if !litOK {
			return false
		}
		cmp = lit
	}
	return compareChoice(n.op, v, cmp, n.ignoreCase)
}

func compareChoice(op string, actual, want any, fold bool) bool {
	switch {
	case strings.HasPrefix(op, "String"):
		s1, ok1 := actual.(string)
		s2, ok2 := want.(string)
		if !ok1 || !ok2 {
			return false
		}
		if fold {
			s1, s2 = strings.ToLower(s1), strings.ToLower(s2)
		}
		switch op {
		case "StringEquals", "StringEqualsPath":
			return s1 == s2
		case "StringLessThan", "StringLessThanPath":
			return s1 < s2
		case "StringLessThanOrEqual", "StringLessThanOrEqualPath":
			return s1 <= s2
		case "StringGreaterThan", "StringGreaterThanPath":
			return s1 > s2
		case "StringGreaterThanOrEqual", "StringGreaterThanOrEqualPath":
			return s1 >= s2
		}
	case strings.HasPrefix(op, "Numeric"):
		f1, ok1 := asFloat(actual)
		f2, ok2 := asFloat(want)
		if !ok1 || !ok2 {
			return false
		}
		switch op {
		case "NumericEquals", "NumericEqualsPath":
			return f1 == f2
		case "NumericLessThan", "NumericLessThanPath":
			return f1 < f2
		case "NumericGreaterThan", "NumericGreaterThanPath":
			return f1 > f2
		}
	case strings.HasPrefix(op, "Boolean"):
		b1, ok1 := actual.(bool)
		b2, ok2 := want.(bool)
		if !ok1 || !ok2 {
			return false
		}
		return b1 == b2
	case strings.HasPrefix(op, "Timestamp"):
		t1, ok1 := parseTimestamp(asStringOr(actual))
		t2, ok2 := parseTimestamp(asStringOr(want))
		if !ok1 || !ok2 {
			return false
		}
		switch op {
		case "TimestampEquals", "TimestampEqualsPath":
			return t1.Equal(t2)
		case "TimestampLessThan", "TimestampLessThanPath":
			return t1.Before(t2)
		case "TimestampLessThanOrEqual", "TimestampLessThanOrEqualPath":
			return !t1.After(t2)
		case "TimestampGreaterThan", "TimestampGreaterThanPath":
			return t1.After(t2)
		case "TimestampGreaterThanOrEqual", "TimestampGreaterThanOrEqualPath":
			return !t1.Before(t2)
		}
	}
	return false
}
