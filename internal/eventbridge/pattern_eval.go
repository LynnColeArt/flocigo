package eventbridge

// pattern_eval.go — EventBridge event pattern compilation, validation and
// matching, shared by TestEventPattern and the PutEvents ingress.
//
// Implemented semantics:
//   - an empty pattern {} validates and matches every event
//   - top-level keys must be envelope fields (id, region, account, source,
//     time, detail-type, resources, detail, version); "detail" must be an
//     object
//   - bare scalars are exact, case-sensitive matches; arrays are OR lists of
//     alternatives; non-operator objects recurse into the event's fields
//   - operators: anything-but (values and prefix), prefix, suffix, numeric
//     (=, <, <=, >, >= with one or two pairs), exists, wildcard (* and ?
//     escaped with a backslash) and equalIgnoreCase; several operators on
//     one field are AND-ed
//   - $or can appear wherever a field matcher is allowed and ORs its
//     operands; sibling keys still have to match
//   - {"field":{"exists":false}} matches when the field is absent

import (
    "encoding/json"
    "fmt"
    "regexp"
    "strings"
    "sync"
)

const maxFilterDepth = 10

var filterOperators = map[string]bool{
    "anything-but":    true,
    "prefix":          true,
    "suffix":          true,
    "numeric":         true,
    "exists":          true,
    "wildcard":        true,
    "equalIgnoreCase": true,
}

var filterTopLevelFields = map[string]bool{
    "id": true, "region": true, "account": true, "source": true,
    "time": true, "detail-type": true, "resources": true,
    "detail": true, "version": true,
}

// eventFilter is a compiled and validated event pattern.
type eventFilter struct {
    root map[string]any
}

// compileEventFilter parses and validates a raw JSON pattern. The returned
// error text carries the reason for the InvalidEventPatternException.
func compileEventFilter(pattern string) (*eventFilter, error) {
    var root map[string]any
    if err := json.Unmarshal([]byte(pattern), &root); err != nil {
        return nil, fmt.Errorf("pattern is not a valid JSON object")
    }
    if root == nil {
        return nil, fmt.Errorf("pattern must be a JSON object")
    }
    if reason := validateFilterNode(root, true, 1); reason != "" {
        return nil, fmt.Errorf("%s", reason)
    }
    return &eventFilter{root: root}, nil
}

// Matches reports whether a decoded event object satisfies the pattern.
func (f *eventFilter) Matches(event map[string]any) bool {
    if f == nil {
        return false
    }
    return matchFilterNode(f.root, event, true)
}

// --------------------------- validation ---------------------------

func validateFilterNode(v any, top bool, depth int) string {
    switch tv := v.(type) {
    case map[string]any:
        if len(tv) == 0 {
            if top {
                return "" // {} matches all events
            }
            return "match objects must contain at least one field"
        }
        if depth > maxFilterDepth {
            return "pattern nesting is too deep"
        }
        for k, child := range tv {
            if k == "$or" {
                list, ok := child.([]any)
                if !ok || len(list) == 0 {
                    return "the $or operator requires a non-empty array of pattern objects"
                }
                for _, elem := range list {
                    if _, ok := elem.(map[string]any); !ok {
                        return "every $or operand must be a pattern object"
                    }
                    if reason := validateFilterNode(elem, false, depth+1); reason != "" {
                        return reason
                    }
                }
                continue
            }
            if filterOperators[k] {
                if reason := validateFilterOperator(k, child, tv); reason != "" {
                    return reason
                }
                continue
            }
            if top && !filterTopLevelFields[k] {
                return fmt.Sprintf("field '%s' is not a valid top level field", k)
            }
            if reason := validateFilterNode(child, false, depth+1); reason != "" {
                return reason
            }
        }
        return ""
    case []any:
        if len(tv) == 0 {
            return "arrays in an event pattern may not be empty"
        }
        for _, elem := range tv {
            if reason := validateFilterNode(elem, false, depth+1); reason != "" {
                return reason
            }
        }
        return ""
    default:
        return ""
    }
}

func validateFilterOperator(op string, spec any, parent map[string]any) string {
    switch op {
    case "exists":
        if len(parent) > 1 {
            return "the exists operator must be the only operator on its field"
        }
        if _, ok := spec.(bool); !ok {
            return "the exists operator requires a boolean value"
        }
    case "prefix", "suffix", "wildcard", "equalIgnoreCase":
        if _, ok := spec.(string); !ok {
            return fmt.Sprintf("the %s operator requires a string value", op)
        }
    case "numeric":
        list, ok := spec.([]any)
        if !ok || (len(list) != 2 && len(list) != 4) {
            return "the numeric operator requires one or two operator/value pairs"
        }
        for i := 0; i+1 < len(list); i += 2 {
            cmp, isStr := list[i].(string)
            if !isStr || !isNumericComparator(cmp) {
                return "the numeric operator requires a comparison operator (=, <, <=, >, >=)"
            }
            if _, ok := filterNumber(list[i+1]); !ok {
                return "the numeric operator requires a numeric operand"
            }
        }
    case "anything-but":
        switch sv := spec.(type) {
        case map[string]any:
            if reason := validateAnythingButElem(sv); reason != "" {
                return reason
            }
        case []any:
            if len(sv) == 0 {
                return "the anything-but operator requires a non-empty value"
            }
            for _, elem := range sv {
                if em, isMap := elem.(map[string]any); isMap {
                    if reason := validateAnythingButElem(em); reason != "" {
                        return reason
                    }
                }
            }
        }
    }
    return ""
}

func validateAnythingButElem(m map[string]any) string {
    if len(m) != 1 {
        return "an anything-but operand object must contain exactly one prefix"
    }
    if _, ok := m["prefix"].(string); !ok {
        return "an anything-but operand object must contain a prefix"
    }
    return ""
}

func isNumericComparator(cmp string) bool {
    switch cmp {
    case "=", "<", "<=", ">", ">=":
        return true
    }
    return false
}

// ---------------------------- matching ----------------------------

func matchFilterNode(pv any, v any, present bool) bool {
    switch p := pv.(type) {
    case map[string]any:
        return matchFilterObject(p, v, present)
    case []any:
        if !present {
            return false
        }
        for _, alt := range p {
            if matchFilterNode(alt, v, true) {
                return true
            }
        }
        return false
    default:
        if !present {
            return false
        }
        return filterScalarEqual(pv, v)
    }
}

func matchFilterObject(p map[string]any, v any, present bool) bool {
    for k, child := range p {
        if k == "$or" {
            list, _ := child.([]any)
            matched := false
            if present {
                for _, elem := range list {
                    if matchFilterNode(elem, v, true) {
                        matched = true
                        break
                    }
                }
            }
            if !matched {
                return false
            }
            continue
        }
        if filterOperators[k] {
            if !matchFilterOperator(k, child, v, present) {
                return false
            }
            continue
        }
        sub, isMap := v.(map[string]any)
        childV, has := any(nil), false
        if isMap {
            childV, has = sub[k]
        }
        if !has {
            // the path does not exist in the event; only an explicit
            // {"exists":false} matcher can succeed
            if !matchFilterNode(child, nil, false) {
                return false
            }
            continue
        }
        if !matchFilterNode(child, childV, true) {
            return false
        }
    }
    return true
}

func matchFilterOperator(op string, spec any, v any, present bool) bool {
    if op == "exists" {
        want, _ := spec.(bool)
        return present == want
    }
    if !present {
        return false
    }
    switch op {
    case "prefix":
        want, ok1 := spec.(string)
        have, ok2 := v.(string)
        return ok1 && ok2 && strings.HasPrefix(have, want)
    case "suffix":
        want, ok1 := spec.(string)
        have, ok2 := v.(string)
        return ok1 && ok2 && strings.HasSuffix(have, want)
    case "wildcard":
        want, ok1 := spec.(string)
        have, ok2 := v.(string)
        return ok1 && ok2 && matchFilterWildcard(want, have)
    case "equalIgnoreCase":
        want, ok1 := spec.(string)
        have, ok2 := v.(string)
        return ok1 && ok2 && strings.EqualFold(have, want)
    case "anything-but":
        return !matchAnythingButSpec(spec, v)
    case "numeric":
        return matchFilterNumeric(spec, v)
    }
    return false
}

func matchAnythingButSpec(spec any, v any) bool {
    switch sv := spec.(type) {
    case map[string]any:
        if prefix, ok := sv["prefix"].(string); ok {
            have, isStr := v.(string)
            return isStr && strings.HasPrefix(have, prefix)
        }
        return false
    case []any:
        for _, elem := range sv {
            if em, isMap := elem.(map[string]any); isMap {
                if prefix, ok := em["prefix"].(string); ok {
                    have, isStr := v.(string)
                    if isStr && strings.HasPrefix(have, prefix) {
                        return true
                    }
                }
                continue
            }
            if filterScalarEqual(elem, v) {
                return true
            }
        }
        return false
    }
    return filterScalarEqual(spec, v)
}

func matchFilterNumeric(spec any, v any) bool {
    list, ok := spec.([]any)
    if !ok || (len(list) != 2 && len(list) != 4) {
        return false
    }
    val, ok := filterNumber(v)
    if !ok {
        return false
    }
    for i := 0; i+1 < len(list); i += 2 {
        cmp, _ := list[i].(string)
        operand, ok := filterNumber(list[i+1])
        if !ok {
            return false
        }
        pass := false
        switch cmp {
        case "=":
            pass = val == operand
        case "<":
            pass = val < operand
        case "<=":
            pass = val <= operand
        case ">":
            pass = val > operand
        case ">=":
            pass = val >= operand
        }
        if !pass {
            return false
        }
    }
    return true
}

// filterScalarEqual compares a bare pattern value to an event value; JSON
// numbers compare numerically, everything else by structural equality.
func filterScalarEqual(a, b any) bool {
    switch av := a.(type) {
    case string:
        bv, ok := b.(string)
        return ok && av == bv
    case bool:
        bv, ok := b.(bool)
        return ok && av == bv
    case json.Number:
        bf, ok := filterNumber(b)
        af, _ := av.Float64()
        return ok && af == bf
    case float64:
        bf, ok := filterNumber(b)
        return ok && av == bf
    case nil:
        return b == nil
    default:
        return false
    }
}

func filterNumber(v any) (float64, bool) {
    switch tv := v.(type) {
    case json.Number:
        f, err := tv.Float64()
        return f, err == nil
    case float64:
        return tv, true
    case float32:
        return float64(tv), true
    case int:
        return float64(tv), true
    case int64:
        return float64(tv), true
    }
    return 0, false
}

var filterWildcardCache sync.Map // pattern string -> *regexp.Regexp

func matchFilterWildcard(pattern, s string) bool {
    var re *regexp.Regexp
    if v, ok := filterWildcardCache.Load(pattern); ok {
        re = v.(*regexp.Regexp)
    } else {
        r, err := compileFilterWildcard(pattern)
        if err != nil {
            return false
        }
        filterWildcardCache.Store(pattern, r)
        re = r
    }
    return re.MatchString(s)
}

func compileFilterWildcard(pattern string) (*regexp.Regexp, error) {
    var sb strings.Builder
    sb.WriteString("^")
    for i := 0; i < len(pattern); i++ {
        c := pattern[i]
        switch {
        case c == '\\' && i+1 < len(pattern):
            sb.WriteString(regexp.QuoteMeta(string(pattern[i+1])))
            i++
        case c == '*':
            sb.WriteString(".*")
        case c == '?':
            sb.WriteString(".")
        default:
            sb.WriteString(regexp.QuoteMeta(string(c)))
        }
    }
    sb.WriteString("$")
    return regexp.Compile(sb.String())
}
