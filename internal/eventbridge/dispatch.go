package eventbridge

// dispatch.go — delivery of ingested events to rule targets. This mirrors
// ticket-004's target dispatcher for the PutEvents ingress: SQS queue
// targets receive an SQS SendMessage, http(s) endpoints receive an HTTP
// POST carrying the resolved target input. Delivery is synchronous and
// best-effort: endpoint failures never fail the PutEvents call.

import (
    "bytes"
    "encoding/json"
    "encoding/xml"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

var ingressHTTPClient = &http.Client{Timeout: 5 * time.Second}

// ingressBaseURLs records, per Service, the address requests arrive on so
// target delivery can reach the SQS surface of the same floci process.
var ingressBaseURLs sync_Map_placeholder

func init() { ingressBaseURLs = sync.Map{} }

// rememberIngressBase records the request's host for later deliveries.
func (s *Service) rememberIngressBase(r *http.Request) {
    if r == nil || r.Host == "" {
        return
    }
    scheme := "http"
    if r.TLS != nil {
        scheme = "https"
    }
    ingressBaseURLs.Store(s, scheme+"://"+r.Host)
}

// ingressEndpoint resolves the base URL of the local API surface.
func (s *Service) ingressEndpoint() string {
    if v, ok := ingressBaseURLs.Load(s); ok {
        return v.(string)
    }
    if v := strings.TrimSpace(os.Getenv("FLOCI_ENDPOINT")); v != "" {
        return strings.TrimRight(v, "/")
    }
    return "http://127.0.0.1:4566"
}

// deliverIngressTarget resolves the target input for an event and sends it.
func (s *Service) deliverIngressTarget(t *Target, env map[string]any, envJSON []byte) {
    if t == nil || t.Arn == "" {
        return
    }
    body, contentType, ok := resolveIngressInput(t, env, envJSON)
    if !ok {
        return
    }
    switch {
    case strings.HasPrefix(t.Arn, "arn:aws:sqs:"):
        s.deliverIngressToSQS(t, body)
    case strings.HasPrefix(t.Arn, "http://"), strings.HasPrefix(t.Arn, "https://"):
        deliverIngressToHTTP(t.Arn, body, contentType, t.HttpParameters)
    }
}

// resolveIngressInput renders the payload a target receives for an event:
// literal Input, an InputPath projection, a transformed template, or the
// full envelope.
func resolveIngressInput(t *Target, env map[string]any, envJSON []byte) ([]byte, string, bool) {
    switch {
    case t.Input != "":
        return []byte(t.Input), ingressContentType(t.Input), true
    case t.InputPath != "":
        v, ok := evalIngressPath(env, t.InputPath)
        if !ok {
            return nil, "", false
        }
        b, err := json.Marshal(v)
        if err != nil {
            return nil, "", false
        }
        return b, "application/json", true
    case t.InputTransformer != nil:
        out := renderIngressTemplate(t.InputTransformer, env)
        return []byte(out), ingressContentType(out), true
    default:
        return envJSON, "application/json", true
    }
}

func ingressContentType(body string) string {
    if json.Valid([]byte(body)) {
        return "application/json"
    }
    return "text/plain"
}

// renderIngressTemplate substitutes every <placeholder> in the template
// with the value its InputPathsMap entry resolves to in the envelope.
// String values are inserted verbatim; everything else as JSON.
func renderIngressTemplate(it *InputTransformer, env map[string]any) string {
    return templatePlaceholderMatcher.ReplaceAllStringFunc(it.InputTemplate, func(m string) string {
        key := m[1 : len(m)-1]
        path, ok := it.InputPathsMap[key]
        if !ok {
            return m
        }
        v, found := evalIngressPath(env, path)
        if !found {
            return ""
        }
        return ingressTemplateValue(v)
    })
}

func ingressTemplateValue(v any) string {
    switch tv := v.(type) {
    case nil:
        return ""
    case string:
        return tv
    default:
        b, err := json.Marshal(tv)
        if err != nil {
            return ""
        }
        return string(b)
    }
}

// evalIngressPath supports the InputPath subset used by EventBridge: "$",
// dotted field access and numeric array indexing.
func evalIngressPath(node any, path string) (any, bool) {
    if path == "$" || path == "" {
        return node, true
    }
    i := 0
    for i < len(path) {
        switch {
        case strings.HasPrefix(path[i:], "."):
            i++
            start := i
            for i < len(path) && path[i] != '.' && path[i] != '[' {
                i++
            }
            key := path[start:i]
            obj, ok := node.(map[string]any)
            if !ok || key == "" {
                return nil, false
            }
            node, ok = obj[key]
            if !ok {
                return nil, false
            }
        case strings.HasPrefix(path[i:], "["):
            end := strings.IndexByte(path[i:], ']')
            if end < 0 {
                return nil, false
            }
            idx, err := strconv.Atoi(path[i+1 : i+end])
            if err != nil {
                return nil, false
            }
            arr, ok := node.([]any)
            if !ok || idx < 0 || idx >= len(arr) {
                return nil, false
            }
            node = arr[idx]
            i += end + 1
        default:
            return nil, false
        }
    }
    return node, true
}

type getQueueURLResponse struct {
    Result struct {
        QueueURL string `xml:"QueueUrl"`
    } `xml:"GetQueueUrlResult"`
}

func parseSQSQueueARN(arn string) (account, name string, ok bool) {
    parts := strings.Split(arn, ":")
    if len(parts) < 6 || parts[0] != "arn" || parts[2] != "sqs" {
        return "", "", false
    }
    return parts[4], parts[5], true
}

func (s *Service) ingressFallbackQueueURL(arn string) string {
    account, name, ok := parseSQSQueueARN(arn)
    if !ok {
        return ""
    }
    return fmt.Sprintf("%s/%s/%s", s.ingressEndpoint(), account, name)
}

// ingressGetQueueURL resolves a queue URL through the SQS surface so the
// message lands in the queue regardless of the path convention in use.
func (s *Service) ingressGetQueueURL(arn string) (string, bool) {
    account, name, ok := parseSQSQueueARN(arn)
    if !ok {
        return "", false
    }
    form := url.Values{}
    form.Set("Action", "GetQueueUrl")
    form.Set("Version", "2012-11-05")
    form.Set("QueueName", name)
    form.Set("QueueOwnerAWSAccountId", account)
    resp, err := ingressHTTPClient.PostForm(s.ingressEndpoint()+"/", form)
    if err != nil {
        return "", false
    }
    defer resp.Body.Close()
    var parsed getQueueURLResponse
    if err := xml.NewDecoder(resp.Body).Decode(&parsed); err != nil || parsed.Result.QueueURL == "" {
        return "", false
    }
    return parsed.Result.QueueURL, true
}

// deliverIngressToSQS posts an SQS SendMessage whose body is the resolved
// target input.
func (s *Service) deliverIngressToSQS(t *Target, body []byte) {
    queueURL := s.ingressFallbackQueueURL(t.Arn)
    if queueURL == "" {
        return
    }
    if resolved, ok := s.ingressGetQueueURL(t.Arn); ok {
        queueURL = resolved
    }
    form := url.Values{}
    form.Set("Action", "SendMessage")
    form.Set("Version", "2012-11-05")
    form.Set("QueueUrl", queueURL)
    form.Set("MessageBody", string(body))
    if t.SqsParameters != nil && t.SqsParameters.MessageGroupId != "" {
        form.Set("MessageGroupId", t.SqsParameters.MessageGroupId)
    }
    _, _ = ingressHTTPClient.PostForm(queueURL, form)
}

// deliverIngressToHTTP POSTs the resolved input to an http(s) target.
func deliverIngressToHTTP(endpoint string, body []byte, contentType string, params *HttpParameters) {
    if contentType == "" {
        contentType = "application/json"
    }
    req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
    if err != nil {
        return
    }
    req.Header.Set("Content-Type", contentType)
    if params != nil {
        for k, v := range params.HeaderParameters {
            req.Header.Set(k, v)
        }
    }
    resp, err := ingressHTTPClient.Do(req)
    if err != nil {
        return
    }
    defer resp.Body.Close()
    _, _ = io.Copy(io.Discard, resp.Body)
}
