package delivery

import (
    "encoding/base64"
    "encoding/json"
    "fmt"
    "strconv"
    "strings"
)

// Filter policy scopes (Subscription.FilterPolicyScope).
const (
    FilterScopeAttributes = "MessageAttributes"
    FilterScopeBody       = "MessageBody"
)

// attrValue is one candidate value extracted from a message attribute or a
// message body during policy evaluation.
type attrValue struct {
    Str   string
    Num   float64
    IsNum bool
}

// condition evaluates one filter condition against the values found for a
// single attribute (or body key). present reports whether it exists.
type condition interface {
    matches(vals []attrValue, present bool) bool
}

// conditionList is a disjunction rendered from JSON arrays in filter
// policies; it matches when any child condition matches.
type conditionList []condition

func (l conditionList) matches(vals []attrValue, present bool) bool {
    for _, c := range l {
        if c.matches(vals, present) {
            return true
        }
    }
    return false
}

// FilterPolicy is a parsed SNS subscription filter policy. The zero value
// matches every message. Top-level keys are attribute (or body key) names,
// combined with AND; the condition list of each key is combined with OR.
type FilterPolicy struct {
    rules map[string]conditionList
}

// Empty reports whether the policy accepts every message.
func (f FilterPolicy) Empty() bool { return len(f.rules) == 0 }

// ParseFilterPolicy parses and validates a filter policy from its JSON
// representation. Errors wrap ErrInvalidFilterPolicy.
func ParseFilterPolicy(raw string) (FilterPolicy, error) {
    if strings.TrimSpace(raw) == "" {
        return FilterPolicy{}, nil
    }
    var doc map[string]interface{}
    if err := json.Unmarshal([]byte(raw), &doc); err != nil {
        return FilterPolicy{}, errPolicy(fmt.Sprintf("not a JSON object: %v", err))
    }
    rules := make(map[string]conditionList, len(doc))
    for name, spec := range doc {
        list, err := parseConditionList(spec)
        if err != nil {
            return FilterPolicy{}, err
        }
        rules[name] = list
    }
    return FilterPolicy{rules: rules}, nil
}

// MatchAttributes reports whether the message attributes satisfy the policy.
func (f FilterPolicy) MatchAttributes(attrs map[string]MessageAttributeValue) bool {
    if f.Empty() {
        return true
    }
    for key, list := range f.rules {
        a, present := attrs[key]
        var vals []attrValue
        if present {
            vals = []attrValue{valueFromAttribute(a)}
        }
        if !list.matches(vals, present) {
            return false
        }
    }
    return true
}

// MatchBody reports whether the message body (a JSON document) satisfies the
// policy. A body that is not valid JSON satisfies no non-empty policy.
func (f FilterPolicy) MatchBody(body string) bool {
    if f.Empty() {
        return true
    }
    var doc map[string]interface{}
    if err := json.Unmarshal([]byte(body), &doc); err != nil {
        return false
    }
    for key, list := range f.rules {
        raw, present := doc[key]
        if !list.matches(bodyValues(raw), present) {
            return false
        }
    }
    return true
}

// filterMatches applies the subscription's filter policy using its configured
// scope. Subscriptions without a policy match everything.
func (s Subscription) filterMatches(m Message) bool {
    if s.FilterPolicy.Empty() {
        return true
    }
    switch s.FilterPolicyScope {
    case FilterScopeBody:
        return s.FilterPolicy.MatchBody(m.Body)
    default:
        return s.FilterPolicy.MatchAttributes(m.Attributes)
    }
}

// bodyValues flattens one value from a parsed message body into candidates.
func bodyValues(v interface{}) []attrValue {
    switch t := v.(type) {
    case nil:
        return nil
    case []interface{}:
        var out []attrValue
        for _, e := range t {
            out = append(out, bodyValues(e)...)
        }
        return out
    case string:
        return []attrValue{{Str: t}}
    case bool:
        return []attrValue{{Str: strconv.FormatBool(t)}}
    case float64:
        return []attrValue{{Str: strconv.FormatFloat(t, 'f', -1, 64), Num: t, IsNum: true}}
    default:
        b, err := json.Marshal(t)
        if err != nil {
            return nil
        }
        return []attrValue{{Str: string(b)}}
    }
}

// valueFromAttribute maps an SNS message attribute onto a candidate value.
// Binary attributes surface as their base64 encoding.
func valueFromAttribute(a MessageAttributeValue) attrValue {
    if dataTypePrefix(a.DataType) == "Number" {
        s := ""
        if a.StringValue != nil {
            s = *a.StringValue
        }
        s = strings.TrimSpace(s)
        if n, err := strconv.ParseFloat(s, 64); err == nil {
            return attrValue{Str: s, Num: n, IsNum: true}
        }
        return attrValue{Str: s}
    }
    s := ""
    if a.StringValue != nil {
        s = *a.StringValue
    } else if len(a.BinaryValue) > 0 {
        s = base64.StdEncoding.EncodeToString(a.BinaryValue)
    }
    return attrValue{Str: s}
}

// dataTypePrefix strips custom label suffixes such as "String.custom.foo".
func dataTypePrefix(dt string) string {
    if i := strings.IndexByte(dt, '.'); i >= 0 {
        return dt[:i]
    }
    return dt
}

func parseConditionList(spec interface{}) (conditionList, error) {
    arr, ok := spec.([]interface{})
    if !ok {
        c, err := parseCondition(spec)
        if err != nil {
            return nil, err
        }
        return conditionList{c}, nil
    }
    if len(arr) == 0 {
        return nil, errPolicy("filter condition list must not be empty")
    }
    list := make(conditionList, 0, len(arr))
    for _, e := range arr {
        c, err := parseCondition(e)
        if err != nil {
            return nil, err
        }
        list = append(list, c)
    }
    return list, nil
}

func parseCondition(spec interface{}) (condition, error) {
    switch t := spec.(type) {
    case string:
        return exactCondition{str: t}, nil
    case float64:
        return exactCondition{num: t, isNum: true}, nil
    case []interface{}:
        return parseConditionList(t)
    case map[string]interface{}:
        return parseOperator(t)
    default:
        return nil, errPolicy(fmt.Sprintf("unsupported filter policy value type %T", spec))
    }
}

func parseOperator(m map[string]interface{}) (condition, error) {
    if len(m) != 1 {
        return nil, errPolicy("filter condition object must contain exactly one operator")
    }
    for op, spec := range m {
        switch op {
        case "exists":
            b, ok := spec.(bool)
            if !ok {
                return nil, errPolicy("the exists operator requires a boolean value")
            }
            return existsCondition{mustExist: b}, nil
        case "anything-but":
            c, err := parseCondition(spec)
            if err != nil {
                return nil, err
            }
            if l, ok := c.(conditionList); ok {
                return anythingButCondition{inner: l}, nil
            }
            return anythingButCondition{inner: conditionList{c}}, nil
        case "prefix":
            return parseStringOperator(spec, false)
        case "suffix":
            return parseStringOperator(spec, true)
        case "numeric":
            return parseNumeric(spec)
        default:
            return nil, errPolicy(fmt.Sprintf("unknown filter policy operator %q", op))
        }
    }
    return nil, errPolicy("empty filter condition object") // unreachable
}

// parseStringOperator handles the prefix and suffix operators, which accept a
// single string or a list of strings.
func parseStringOperator(spec interface{}, suffix bool) (condition, error) {
    name := "prefix"
    if suffix {
        name = "suffix"
    }
    one := func(v interface{}) (condition, error) {
        s, ok := v.(string)
        if !ok {
            return nil, errPolicy(fmt.Sprintf("the %q operator requires string values", name))
        }
        if suffix {
            return suffixCondition{suffix: s}, nil
        }
        return prefixCondition{prefix: s}, nil
    }
    if arr, ok := spec.([]interface{}); ok {
        if len(arr) == 0 {
            return nil, errPolicy(fmt.Sprintf("the %q operator requires at least one value", name))
        }
        list := make(conditionList, 0, len(arr))
        for _, e := range arr {
            c, err := one(e)
            if err != nil {
                return nil, err
            }
            list = append(list, c)
        }
        return list, nil
    }
    return one(spec)
}

func parseNumeric(spec interface{}) (condition, error) {
    arr, ok := spec.([]interface{})
    if !ok {
        return nil, errPolicy("the numeric operator requires an array of [operator, number] pairs")
    }
    if len(arr) != 2 && len(arr) != 4 {
        return nil, errPolicy(fmt.Sprintf("the numeric operator requires one or two [operator, number] pairs, got %d elements", len(arr)))
    }
    ops := make([]numericOp, 0, len(arr)/2)
    for i := 0; i < len(arr); i += 2 {
        op, ok := arr[i].(string)
        if !ok {
            return nil, errPolicy("numeric filter operators must be strings")
        }
        switch op {
        case "=", ">", ">=", "<", "<=":
        default:
            return nil, errPolicy(fmt.Sprintf("unknown numeric operator %q", op))
        }
        n, ok := arr[i+1].(float64)
        if !ok {
            return nil, errPolicy("numeric comparison values must be numbers")
        }
        ops = append(ops, numericOp{op: op, value: n})
    }
    return numericCondition{ops: ops}, nil
}

type exactCondition struct {
    str   string
    num   float64
    isNum bool
}

func (c exactCondition) matches(vals []attrValue, _ bool) bool {
    for _, v := range vals {
        if c.isNum {
            if v.IsNum && v.Num == c.num {
                return true
            }
        } else if !v.IsNum && v.Str == c.str {
            return true
        }
    }
    return false
}

// anythingButCondition matches only when the value equals none of the nested
// conditions (supports value lists and nested operators such as prefix).
type anythingButCondition struct {
    inner conditionList
}

func (c anythingButCondition) matches(vals []attrValue, present bool) bool {
    if !present {
        return false
    }
    for _, v := range vals {
        for _, inner := range c.inner {
            if inner.matches([]attrValue{v}, true) {
                return false
            }
        }
    }
    return true
}

type prefixCondition struct {
    prefix string
}

func (c prefixCondition) matches(vals []attrValue, _ bool) bool {
    for _, v := range vals {
        if strings.HasPrefix(v.Str, c.prefix) {
            return true
        }
    }
    return false
}

type suffixCondition struct {
    suffix string
}

func (c suffixCondition) matches(vals []attrValue, _ bool) bool {
    for _, v := range vals {
        if strings.HasSuffix(v.Str, c.suffix) {
            return true
        }
    }
    return false
}

type existsCondition struct {
    mustExist bool
}

func (c existsCondition) matches(_ []attrValue, present bool) bool {
    if c.mustExist {
        return present
    }
    return !present
}

type numericOp struct {
    op    string
    value float64
}

type numericCondition struct {
    ops []numericOp
}

func (c numericCondition) matches(vals []attrValue, _ bool) bool {
    for _, v := range vals {
        if !v.IsNum {
            continue
        }
        ok := true
        for _, o := range c.ops {
            if !satisfiesNumericOp(o.op, v.Num, o.value) {
                ok = false
                break
            }
        }
        if ok {
            return true
        }
    }
    return false
}

func satisfiesNumericOp(op string, v, w float64) bool {
    switch op {
    case "=":
        return v == w
    case ">":
        return v > w
    case ">=":
        return v >= w
    case "<":
        return v < w
    case "<=":
        return v <= w
    }
    return false
}

// errPolicy wraps msg with ErrInvalidFilterPolicy.
func errPolicy(msg string) error {
    return fmt.Errorf("%w: %s", ErrInvalidFilterPolicy, msg)
}
