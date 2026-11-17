// Package sns implements an in-memory Amazon Simple Notification Service
// emulator for floci-go, built on the same conventions as the SQS service:
// requests are accepted with either the classic AWS query protocol
// (form-encoded parameters, XML responses) or the newer JSON protocol
// (Content-Type application/x-amz-json-1.0), auto-detected per request,
// mirroring the SQS protocol adapters.
//
// Sugarbit-scoped operations:
//
//	CreateTopic DeleteTopic ListTopics GetTopicAttributes
//	SetTopicAttributes Subscribe Unsubscribe ListSubscriptions
//	ListSubscriptionsByTopic GetSubscriptionAttributes
//	SetSubscriptionAttributes
//
// Plus small extras: ConfirmSubscription and Publish (delivery is
// emulated: Publish only validates and acknowledges).
package sns

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	snsNamespace  = "http://sns.amazonaws.com/doc/2010-03-31/"
	snsRegion     = "us-east-1"
	snsAccount    = "123456789012"
	snsPageSize   = 100
	snsMaxMessage = 262144
	snsMaxSubject = 100
	dq            = `"` // double-quote helper; keeps this file backslash-free
)

var topicNameRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,256}$`)

var snsProtocols = map[string]bool{
	"http": true, "https": true, "email": true, "email-json": true,
	"sms": true, "sqs": true, "application": true, "lambda": true,
}

const snsDefaultDeliveryPolicy = `{"http":{"defaultHealthyRetryPolicy":{"minDelayTarget":20,"maxDelayTarget":20,"numRetries":3,"numMaxDelayRetries":0,"numNoDelayRetries":0,"numMinDelayRetries":0,"backoffFunction":"linear"},"disableSubscriptionOverrides":false}}`

const snsDefaultPolicyTemplate = `{
  "Version": "2012-10-17",
  "Id": "%s/SNSDefaultPolicy",
  "Statement": [
    {
      "Sid": "DefaultStatementID",
      "Effect": "Allow",
      "Principal": {"AWS": "*"},
      "Action": ["sns:GetTopicAttributes", "sns:SetTopicAttributes", "sns:AddPermission", "sns:RemovePermission", "sns:DeleteTopic", "sns:Subscribe", "sns:ListSubscriptionsByTopic", "sns:Publish", "sns:Receive"],
      "Resource": "%s",
      "Condition": {"StringEquals": {"AWS:SourceOwner": "%s"}}
    }
  ]
}`

// Topic is an SNS topic.
type Topic struct {
	Arn            string
	Name           string
	DisplayName    string
	Policy         string
	DeliveryPolicy string
	Tags           map[string]string
	Deleted        int // SubscriptionsDeleted counter
	subs           []*Subscription
}

// Subscription is a subscription attached to a topic.
type Subscription struct {
	Arn               string
	TopicArn          string
	Protocol          string
	Endpoint          string
	Pending           bool
	ConfirmToken      string
	Raw               bool
	FilterPolicy      string
	FilterPolicyScope string
	RedrivePolicy     string
	DeliveryPolicy    string
}

// SNS holds the in-memory SNS service state.
type SNS struct {
	mu        sync.Mutex
	Region    string
	AccountID string
	topics    map[string]*Topic        // keyed by full topic ARN
	subs      map[string]*Subscription // keyed by full subscription ARN
}

// New returns an SNS service with default region and account.
func New() *SNS { return NewSNS(snsRegion, snsAccount) }

// NewSNS returns an SNS service for the given region and account.
func NewSNS(region, account string) *SNS {
	if region == "" {
		region = snsRegion
	}
	if account == "" {
		account = snsAccount
	}
	return &SNS{
		Region:    region,
		AccountID: account,
		topics:    map[string]*Topic{},
		subs:      map[string]*Subscription{},
	}
}

// Reset clears all state.
func (s *SNS) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics = map[string]*Topic{}
	s.subs = map[string]*Subscription{}
}

// APIError is a protocol-level SNS error.
type APIError struct {
	Code    string
	Message string
	Status  int
}

func (e *APIError) Error() string { return e.Code + ": " + e.Message }

// HTTPStatus returns the wire HTTP status for this error.
func (e *APIError) HTTPStatus() int {
	if e == nil || e.Status == 0 {
		return http.StatusBadRequest
	}
	return e.Status
}

func snsErrNew(code, message string) *APIError {
	return &APIError{Code: code, Message: message, Status: http.StatusBadRequest}
}

func invalidParam(message string) *APIError {
	return snsErrNew("InvalidParameterException", message)
}

func notFound(message string) *APIError { return snsErrNew("NotFound", message) }

func invalidAction(action string) *APIError {
	return snsErrNew("InvalidAction", "Could not find operation "+action)
}

func toAPIError(err error) *APIError {
	if err == nil {
		return nil
	}
	if e, ok := err.(*APIError); ok {
		return e
	}
	return snsErrNew("InternalFailure", err.Error())
}

func snsNewUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func snsEncodeToken(s string) string { return base64.URLEncoding.EncodeToString([]byte(s)) }

func snsDecodeToken(t string) string {
	if b, err := base64.URLEncoding.DecodeString(t); err == nil {
		return string(b)
	}
	return t
}

func snsBoolString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func (s *SNS) topicArnFor(name string) string {
	return "arn:aws:sns:" + s.Region + ":" + s.AccountID + ":" + name
}

// findTopic resolves a topic by exact ARN or by the trailing name segment.
func (s *SNS) findTopic(arnOrName string) *Topic {
	if arnOrName == "" {
		return nil
	}
	if t, ok := s.topics[arnOrName]; ok {
		return t
	}
	name := arnOrName
	if i := strings.LastIndex(arnOrName, ":"); i >= 0 && i < len(arnOrName)-1 {
		name = arnOrName[i+1:]
	}
	for _, t := range s.topics {
		if t.Name == name {
			return t
		}
	}
	return nil
}

// CreateTopic creates a topic, or returns the ARN of the existing one.
func (s *SNS) CreateTopic(name string, attrs, tags map[string]string) (string, error) {
	if !topicNameRe.MatchString(name) {
		return "", invalidParam("Invalid parameter: TopicName Reason: Must match pattern [A-Za-z0-9_-]+ and be at most 256 characters")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	arn := s.topicArnFor(name)
	if _, ok := s.topics[arn]; ok {
		return arn, nil
	}
	t := &Topic{
		Arn:    arn,
		Name:   name,
		Tags:   map[string]string{},
		Policy: fmt.Sprintf(snsDefaultPolicyTemplate, arn, arn, s.AccountID),
	}
	for k, v := range tags {
		t.Tags[k] = v
	}
	if attrs != nil {
		if err := s.applyTopicAttrs(t, attrs); err != nil {
			return "", err
		}
	}
	s.topics[arn] = t
	return arn, nil
}

func (s *SNS) applyTopicAttrs(t *Topic, attrs map[string]string) error {
	for k, v := range attrs {
		if err := s.setTopicAttr(t, k, v); err != nil {
			return err
		}
	}
	return nil
}

func (s *SNS) setTopicAttr(t *Topic, name, value string) error {
	switch name {
	case "DisplayName":
		t.DisplayName = value
	case "Policy":
		t.Policy = value
	case "DeliveryPolicy":
		t.DeliveryPolicy = value
	case "TopicArn", "Owner", "SubscriptionsConfirmed", "SubscriptionsPending",
		"SubscriptionsDeleted", "EffectiveDeliveryPolicy":
		return invalidParam(fmt.Sprintf("Could not modify attribute %s. Modification is not allowed.", name))
	default:
		return invalidParam(fmt.Sprintf("Invalid parameter: Attributes Reason: Invalid attribute name %s", name))
	}
	return nil
}

// DeleteTopic removes a topic and all of its subscriptions.
func (s *SNS) DeleteTopic(arn string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.findTopic(arn)
	if t == nil {
		return notFound("Topic does not exist")
	}
	for _, sub := range t.subs {
		delete(s.subs, sub.Arn)
	}
	delete(s.topics, t.Arn)
	return nil
}

// ListTopics returns topic ARNs sorted alphabetically, paginated.
func (s *SNS) ListTopics(nextToken string) ([]map[string]string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	arns := make([]string, 0, len(s.topics))
	for arn := range s.topics {
		arns = append(arns, arn)
	}
	sort.Strings(arns)
	start := 0
	if tok := snsDecodeToken(nextToken); tok != "" {
		for i, arn := range arns {
			if arn == tok {
				start = i + 1
				break
			}
		}
	}
	end := start + snsPageSize
	if end > len(arns) {
		end = len(arns)
	}
	next := ""
	if end < len(arns) && end > start {
		next = snsEncodeToken(arns[end-1])
	}
	out := make([]map[string]string, 0, end-start)
	for _, arn := range arns[start:end] {
		out = append(out, map[string]string{"TopicArn": arn})
	}
	return out, next, nil
}

func (s *SNS) topicAttrs(t *Topic) map[string]string {
	confirmed, pending := 0, 0
	for _, sub := range t.subs {
		if sub.Pending {
			pending++
		} else {
			confirmed++
		}
	}
	eff := t.DeliveryPolicy
	if eff == "" {
		eff = snsDefaultDeliveryPolicy
	}
	m := map[string]string{
		"TopicArn":                t.Arn,
		"Owner":                   s.AccountID,
		"Policy":                  t.Policy,
		"EffectiveDeliveryPolicy": eff,
		"SubscriptionsConfirmed":  strconv.Itoa(confirmed),
		"SubscriptionsPending":    strconv.Itoa(pending),
		"SubscriptionsDeleted":    strconv.Itoa(t.Deleted),
	}
	if t.DisplayName != "" {
		m["DisplayName"] = t.DisplayName
	}
	if t.DeliveryPolicy != "" {
		m["DeliveryPolicy"] = t.DeliveryPolicy
	}
	return m
}

// GetTopicAttributes returns the attribute map for one topic.
func (s *SNS) GetTopicAttributes(arn string) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.findTopic(arn)
	if t == nil {
		return nil, notFound("Topic does not exist")
	}
	return s.topicAttrs(t), nil
}

// SetTopicAttributes sets a single modifiable topic attribute.
func (s *SNS) SetTopicAttributes(arn, name, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.findTopic(arn)
	if t == nil {
		return notFound("Topic does not exist")
	}
	if name == "" {
		return invalidParam("Invalid parameter: AttributeName")
	}
	return s.setTopicAttr(t, name, value)
}

// Subscribe attaches an endpoint to a topic. For protocols that require
// manual confirmation (http, https, email, email-json) the subscription is
// registered as pending and the literal "pending confirmation" wire value
// is returned, unless returnArn forces the real ARN, matching AWS.
func (s *SNS) Subscribe(topicArn, protocol, endpoint string, attrs map[string]string, returnArn bool) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.findTopic(topicArn)
	if t == nil {
		return "", notFound("Topic does not exist")
	}
	if !snsProtocols[protocol] {
		return "", invalidParam("Invalid parameter: Protocol Reason: Protocol " + protocol + " is not supported")
	}
	if endpoint == "" {
		return "", invalidParam("Invalid parameter: Endpoint")
	}
	switch protocol {
	case "email", "email-json":
		if !strings.Contains(endpoint, "@") {
			return "", invalidParam("Invalid parameter: Endpoint Reason: Endpoint must be an email address")
		}
	case "http", "https":
		if !strings.HasPrefix(endpoint, protocol+"://") {
			return "", invalidParam("Invalid parameter: Endpoint Reason: Endpoint must begin with " + protocol + "://")
		}
	}
	sub := &Subscription{
		Arn:          t.Arn + ":" + snsNewUUID(),
		TopicArn:     t.Arn,
		Protocol:     protocol,
		Endpoint:     endpoint,
		ConfirmToken: snsNewUUID(),
	}
	pendingSpecified := false
	userPending := false
	if attrs != nil {
		if v, ok := attrs["PendingConfirmation"]; ok {
			pendingSpecified = true
			userPending = v == "true"
		}
		if err := s.applySubAttrs(sub, attrs); err != nil {
			return "", err
		}
	}
	if pendingSpecified {
		sub.Pending = userPending
	} else {
		switch protocol {
		case "http", "https", "email", "email-json":
			sub.Pending = true
		}
	}
	if sub.FilterPolicy != "" && sub.FilterPolicyScope == "" {
		sub.FilterPolicyScope = "MessageAttributes"
	}
	t.subs = append(t.subs, sub)
	s.subs[sub.Arn] = sub
	if sub.Pending && !returnArn {
		return "pending confirmation", nil
	}
	return sub.Arn, nil
}

func (s *SNS) applySubAttrs(sub *Subscription, attrs map[string]string) error {
	for k, v := range attrs {
		if err := s.setSubAttr(sub, k, v); err != nil {
			return err
		}
	}
	return nil
}

func (s *SNS) setSubAttr(sub *Subscription, name, value string) error {
	switch name {
	case "RawMessageDelivery":
		if value != "true" && value != "false" {
			return invalidParam("Invalid parameter: RawMessageDelivery Reason: Must be true or false")
		}
		sub.Raw = value == "true"
	case "FilterPolicy":
		if value != "" {
			var parsed any
			if err := json.Unmarshal([]byte(value), &parsed); err != nil {
				return invalidParam("Invalid parameter: FilterPolicy Reason: Invalid JSON")
			}
			if _, ok := parsed.(map[string]any); !ok {
				return invalidParam("Invalid parameter: FilterPolicy Reason: Must be a JSON object")
			}
		}
		sub.FilterPolicy = value
	case "FilterPolicyScope":
		if value != "MessageBody" && value != "MessageAttributes" {
			return invalidParam("Invalid parameter: FilterPolicyScope Reason: Must be MessageBody or MessageAttributes")
		}
		sub.FilterPolicyScope = value
	case "RedrivePolicy":
		var parsed map[string]any
		if err := json.Unmarshal([]byte(value), &parsed); err != nil || parsed["deadLetterTargetArn"] == nil {
			return invalidParam("Invalid parameter: RedrivePolicy Reason: deadLetterTargetArn is missing or invalid")
		}
		sub.RedrivePolicy = value
	case "DeliveryPolicy":
		sub.DeliveryPolicy = value
	case "PendingConfirmation":
		if value != "true" && value != "false" {
			return invalidParam("Invalid parameter: PendingConfirmation Reason: Must be true or false")
		}
	case "ConfirmationWasAuthenticated":
		if value != "true" && value != "false" {
			return invalidParam("Invalid parameter: ConfirmationWasAuthenticated Reason: Must be true or false")
		}
	default:
		return invalidParam(fmt.Sprintf("Invalid parameter: Attributes Reason: Unsupported attribute %s", name))
	}
	return nil
}

// Unsubscribe removes a subscription; like AWS this is idempotent.
func (s *SNS) Unsubscribe(arn string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sub := s.subs[arn]; sub != nil {
		s.removeSub(sub)
	}
	return nil
}

func (s *SNS) removeSub(sub *Subscription) {
	delete(s.subs, sub.Arn)
	if t := s.topics[sub.TopicArn]; t != nil {
		for i, x := range t.subs {
			if x == sub {
				t.subs = append(t.subs[:i], t.subs[i+1:]...)
				t.Deleted++
				break
			}
		}
	}
}

// ListSubscriptions lists subscriptions across all topics, paginated.
func (s *SNS) ListSubscriptions(nextToken string) ([]map[string]string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listSubsLocked(nil, nextToken)
}

// ListSubscriptionsByTopic lists subscriptions of one topic, paginated.
func (s *SNS) ListSubscriptionsByTopic(topicArn, nextToken string) ([]map[string]string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.findTopic(topicArn)
	if t == nil {
		return nil, "", notFound("Topic does not exist")
	}
	return s.listSubsLocked(func(sub *Subscription) bool { return sub.TopicArn == t.Arn }, nextToken)
}

func (s *SNS) listSubsLocked(pred func(*Subscription) bool, nextToken string) ([]map[string]string, string, error) {
	all := make([]*Subscription, 0)
	for _, t := range s.topics {
		for _, sub := range t.subs {
			if pred == nil || pred(sub) {
				all = append(all, sub)
			}
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Arn < all[j].Arn })
	start := 0
	if tok := snsDecodeToken(nextToken); tok != "" {
		for i, sub := range all {
			if sub.Arn == tok {
				start = i + 1
				break
			}
		}
	}
	end := start + snsPageSize
	if end > len(all) {
		end = len(all)
	}
	next := ""
	if end < len(all) && end > start {
		next = snsEncodeToken(all[end-1].Arn)
	}
	out := make([]map[string]string, 0, end-start)
	for _, sub := range all[start:end] {
		out = append(out, map[string]string{
			"SubscriptionArn": sub.Arn,
			"Owner":           s.AccountID,
			"Protocol":        sub.Protocol,
			"Endpoint":        sub.Endpoint,
			"TopicArn":        sub.TopicArn,
		})
	}
	return out, next, nil
}

// GetSubscriptionAttributes returns attributes for one subscription.
func (s *SNS) GetSubscriptionAttributes(arn string) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub := s.subs[arn]
	if sub == nil {
		return nil, notFound("Subscription does not exist")
	}
	return s.subAttrs(sub), nil
}

func (s *SNS) subAttrs(sub *Subscription) map[string]string {
	m := map[string]string{
		"SubscriptionArn":              sub.Arn,
		"TopicArn":                     sub.TopicArn,
		"Protocol":                     sub.Protocol,
		"Endpoint":                     sub.Endpoint,
		"Owner":                        s.AccountID,
		"RawMessageDelivery":           snsBoolString(sub.Raw),
		"ConfirmationWasAuthenticated": snsBoolString(!sub.Pending),
	}
	if sub.FilterPolicy != "" {
		m["FilterPolicy"] = sub.FilterPolicy
	}
	scope := sub.FilterPolicyScope
	if scope == "" && sub.FilterPolicy != "" {
		scope = "MessageAttributes"
	}
	if scope != "" {
		m["FilterPolicyScope"] = scope
	}
	if sub.RedrivePolicy != "" {
		m["RedrivePolicy"] = sub.RedrivePolicy
	}
	if sub.DeliveryPolicy != "" {
		m["DeliveryPolicy"] = sub.DeliveryPolicy
	}
	return m
}

// SetSubscriptionAttributes sets a single subscription attribute.
func (s *SNS) SetSubscriptionAttributes(arn, name, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub := s.subs[arn]
	if sub == nil {
		return notFound("Subscription does not exist")
	}
	if name == "" {
		return invalidParam("Invalid parameter: AttributeName")
	}
	return s.setSubAttr(sub, name, value)
}

// ConfirmSubscription confirms a pending subscription by token
// (the raw subscription ARN is also accepted).
func (s *SNS) ConfirmSubscription(topicArn, token string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.findTopic(topicArn)
	if t == nil {
		return "", notFound("Topic does not exist")
	}
	for _, sub := range t.subs {
		if sub.ConfirmToken == token || sub.Arn == token {
			sub.Pending = false
			return sub.Arn, nil
		}
	}
	return "", notFound("Subscription does not exist")
}

// Publish validates a publish request and returns a message id.
// Delivery to subscribers is emulated (no-op).
func (s *SNS) Publish(topicArn, targetArn, message, subject string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var t *Topic
	if topicArn != "" {
		t = s.findTopic(topicArn)
	}
	if t == nil && targetArn != "" {
		if sub := s.subs[targetArn]; sub != nil {
			t = s.topics[sub.TopicArn]
		}
	}
	if t == nil {
		return "", notFound("Topic does not exist")
	}
	if message == "" {
		return "", invalidParam("Invalid parameter: Message Reason: Message may not be empty")
	}
	if len(message) > snsMaxMessage {
		return "", invalidParam("Invalid parameter: Message Reason: Message is too long")
	}
	if len(subject) > snsMaxSubject {
		return "", invalidParam("Invalid parameter: Subject Reason: Subject is too long")
	}
	return snsNewUUID(), nil
}

// snsRequest abstracts one decoded call: query parameters or JSON body.
type snsRequest struct {
	isJSON bool
	q      url.Values
	j      map[string]any
}

func (r *snsRequest) str(name string) string {
	if r.isJSON {
		return snsAnyToString(r.j[name])
	}
	return r.q.Get(name)
}

func (r *snsRequest) boolean(name string) bool {
	if r.isJSON {
		if v, ok := r.j[name].(bool); ok {
			return v
		}
		return snsAnyToString(r.j[name]) == "true"
	}
	b, _ := strconv.ParseBool(r.q.Get(name))
	return b
}

// strMap decodes a map<string,string> parameter (query: name.entry.N.key)
// or its JSON object/list representation.
func (r *snsRequest) strMap(name string) map[string]string {
	out := map[string]string{}
	if r.isJSON {
		raw, ok := r.j[name]
		if !ok || raw == nil {
			return out
		}
		switch v := raw.(type) {
		case map[string]any:
			for k, val := range v {
				out[k] = snsAnyToString(val)
			}
		case []any:
			for _, it := range v {
				if m, ok := it.(map[string]any); ok {
					if k := snsPickString(m, "key"); k != "" {
						out[k] = snsPickString(m, "value")
					}
				}
			}
		}
		return out
	}
	low := snsLowerQuery(r.q)
	prefix := strings.ToLower(name) + ".entry."
	for lk, v := range low {
		if !strings.HasPrefix(lk, prefix) {
			continue
		}
		rest := lk[len(prefix):]
		i := strings.LastIndex(rest, ".")
		if i <= 0 {
			continue
		}
		if rest[i+1:] == "key" {
			out[v] = low[prefix+rest[:i]+".value"]
		}
	}
	return out
}

// strList decodes a list-of-structures parameter with the given fields
// (query: name.member.N.<field> or name.entry.N.<field>, case-insensitive).
func (r *snsRequest) strList(name string, fields ...string) []map[string]string {
	out := []map[string]string{}
	if r.isJSON {
		raw, ok := r.j[name]
		if !ok || raw == nil {
			return out
		}
		if arr, ok := raw.([]any); ok {
			for _, it := range arr {
				if m, ok := it.(map[string]any); ok {
					e := map[string]string{}
					for _, f := range fields {
						e[strings.ToLower(f)] = snsPickString(m, f)
					}
					out = append(out, e)
				}
			}
		}
		return out
	}
	low := snsLowerQuery(r.q)
	items := map[int]map[string]string{}
	base := strings.ToLower(name)
	for lk, v := range low {
		for _, sep := range []string{".member.", ".entry."} {
			prefix := base + sep
			if !strings.HasPrefix(lk, prefix) {
				continue
			}
			rest := lk[len(prefix):]
			i := strings.Index(rest, ".")
			if i <= 0 {
				continue
			}
			n, err := strconv.Atoi(rest[:i])
			if err != nil {
				continue
			}
			field := rest[i+1:]
			for _, f := range fields {
				if field == strings.ToLower(f) {
					if items[n] == nil {
						items[n] = map[string]string{}
					}
					items[n][strings.ToLower(f)] = v
				}
			}
		}
	}
	idxs := make([]int, 0, len(items))
	for n := range items {
		idxs = append(idxs, n)
	}
	sort.Ints(idxs)
	for _, n := range idxs {
		out = append(out, items[n])
	}
	return out
}

func snsLowerQuery(v url.Values) map[string]string {
	out := map[string]string{}
	for k, vs := range v {
		if len(vs) > 0 {
			out[strings.ToLower(k)] = vs[0]
		}
	}
	return out
}

func snsPickString(m map[string]any, field string) string {
	for k, v := range m {
		if strings.EqualFold(k, field) {
			return snsAnyToString(v)
		}
	}
	return ""
}

func snsAnyToString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", t)
	}
}

// dispatch maps an action to its handler and returns a canonical result
// map that both the XML and the JSON adapters render.
func (s *SNS) dispatch(action string, req *snsRequest) (map[string]any, error) {
	switch action {
	case "CreateTopic":
		tags := map[string]string{}
		for _, e := range req.strList("tags", "key", "value") {
			tags[e["key"]] = e["value"]
		}
		arn, err := s.CreateTopic(req.str("Name"), req.strMap("attributes"), tags)
		if err != nil {
			return nil, err
		}
		return map[string]any{"TopicArn": arn}, nil
	case "DeleteTopic":
		if err := s.DeleteTopic(req.str("TopicArn")); err != nil {
			return nil, err
		}
		return map[string]any{}, nil
	case "ListTopics":
		list, next, err := s.ListTopics(req.str("NextToken"))
		if err != nil {
			return nil, err
		}
		out := map[string]any{"Topics": list}
		if next != "" {
			out["NextToken"] = next
		}
		return out, nil
	case "GetTopicAttributes":
		m, err := s.GetTopicAttributes(req.str("TopicArn"))
		if err != nil {
			return nil, err
		}
		return map[string]any{"Attributes": m}, nil
	case "SetTopicAttributes":
		name := req.str("AttributeName")
		if name == "" {
			name = req.str("Attribute.Name")
		}
		value := req.str("AttributeValue")
		if value == "" {
			value = req.str("Attribute.Value")
		}
		if err := s.SetTopicAttributes(req.str("TopicArn"), name, value); err != nil {
			return nil, err
		}
		return map[string]any{}, nil
	case "Subscribe":
		arn, err := s.Subscribe(req.str("TopicArn"), req.str("Protocol"), req.str("Endpoint"),
			req.strMap("attributes"), req.boolean("ReturnSubscriptionArn"))
		if err != nil {
			return nil, err
		}
		return map[string]any{"SubscriptionArn": arn}, nil
	case "Unsubscribe":
		if err := s.Unsubscribe(req.str("SubscriptionArn")); err != nil {
			return nil, err
		}
		return map[string]any{}, nil
	case "ListSubscriptions":
		list, next, err := s.ListSubscriptions(req.str("NextToken"))
		if err != nil {
			return nil, err
		}
		out := map[string]any{"Subscriptions": list}
		if next != "" {
			out["NextToken"] = next
		}
		return out, nil
	case "ListSubscriptionsByTopic":
		list, next, err := s.ListSubscriptionsByTopic(req.str("TopicArn"), req.str("NextToken"))
		if err != nil {
			return nil, err
		}
		out := map[string]any{"Subscriptions": list}
		if next != "" {
			out["NextToken"] = next
		}
		return out, nil
	case "GetSubscriptionAttributes":
		m, err := s.GetSubscriptionAttributes(req.str("SubscriptionArn"))
		if err != nil {
			return nil, err
		}
		return map[string]any{"Attributes": m}, nil
	case "SetSubscriptionAttributes":
		name := req.str("AttributeName")
		if name == "" {
			name = req.str("Attribute.Name")
		}
		value := req.str("AttributeValue")
		if value == "" {
			value = req.str("Attribute.Value")
		}
		if err := s.SetSubscriptionAttributes(req.str("SubscriptionArn"), name, value); err != nil {
			return nil, err
		}
		return map[string]any{}, nil
	case "ConfirmSubscription":
		arn, err := s.ConfirmSubscription(req.str("TopicArn"), req.str("Token"))
		if err != nil {
			return nil, err
		}
		return map[string]any{"SubscriptionArn": arn}, nil
	case "Publish":
		id, err := s.Publish(req.str("TopicArn"), req.str("TargetArn"), req.str("Message"), req.str("Subject"))
		if err != nil {
			return nil, err
		}
		return map[string]any{"MessageId": id}, nil
	}
	return nil, invalidAction(action)
}

// ---- XML (query protocol) adapter ----

type snsXEntry struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}

type snsXTopic struct {
	TopicArn string `xml:"TopicArn"`
}

type snsXSub struct {
	SubscriptionArn string `xml:"SubscriptionArn"`
	Owner           string `xml:"Owner"`
	Protocol        string `xml:"Protocol"`
	Endpoint        string `xml:"Endpoint"`
	TopicArn        string `xml:"TopicArn"`
}

type snsXArnResult struct {
	XMLName         xml.Name
	TopicArn        string `xml:"TopicArn,omitempty"`
	SubscriptionArn string `xml:"SubscriptionArn,omitempty"`
	MessageId       string `xml:"MessageId,omitempty"`
}

type snsXAttrsResult struct {
	XMLName    xml.Name
	Attributes []snsXEntry `xml:"Attributes>entry"`
}

type snsXListTopicsResult struct {
	XMLName   xml.Name
	Topics    []snsXTopic `xml:"Topics>member"`
	NextToken string      `xml:"NextToken,omitempty"`
}

type snsXListSubsResult struct {
	XMLName       xml.Name
	Subscriptions []snsXSub `xml:"Subscriptions>member"`
	NextToken     string    `xml:"NextToken,omitempty"`
}

type snsXEmptyResult struct {
	XMLName xml.Name
}

func snsToXEntries(v any) []snsXEntry {
	m, ok := v.(map[string]string)
	if !ok || m == nil {
		return []snsXEntry{}
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]snsXEntry, 0, len(keys))
	for _, k := range keys {
		out = append(out, snsXEntry{Key: k, Value: m[k]})
	}
	return out
}

func snsToXTopics(v any) []snsXTopic {
	list, ok := v.([]map[string]string)
	if !ok {
		return []snsXTopic{}
	}
	out := make([]snsXTopic, 0, len(list))
	for _, m := range list {
		out = append(out, snsXTopic{TopicArn: m["TopicArn"]})
	}
	return out
}

func snsToXSubs(v any) []snsXSub {
	list, ok := v.([]map[string]string)
	if !ok {
		return []snsXSub{}
	}
	out := make([]snsXSub, 0, len(list))
	for _, m := range list {
		out = append(out, snsXSub{
			SubscriptionArn: m["SubscriptionArn"],
			Owner:           m["Owner"],
			Protocol:        m["Protocol"],
			Endpoint:        m["Endpoint"],
			TopicArn:        m["TopicArn"],
		})
	}
	return out
}

func (s *SNS) xmlBody(action string, data map[string]any, reqID string) []byte {
	var result any = snsXEmptyResult{XMLName: xml.Name{Local: action + "Result"}}
	switch action {
	case "CreateTopic":
		result = snsXArnResult{XMLName: xml.Name{Local: action + "Result"}, TopicArn: snsAnyToString(data["TopicArn"])}
	case "Subscribe", "ConfirmSubscription":
		result = snsXArnResult{XMLName: xml.Name{Local: action + "Result"}, SubscriptionArn: snsAnyToString(data["SubscriptionArn"])}
	case "Publish":
		result = snsXArnResult{XMLName: xml.Name{Local: action + "Result"}, MessageId: snsAnyToString(data["MessageId"])}
	case "ListTopics":
		result = snsXListTopicsResult{
			XMLName:   xml.Name{Local: action + "Result"},
			Topics:    snsToXTopics(data["Topics"]),
			NextToken: snsAnyToString(data["NextToken"]),
		}
	case "GetTopicAttributes", "GetSubscriptionAttributes":
		result = snsXAttrsResult{
			XMLName:    xml.Name{Local: action + "Result"},
			Attributes: snsToXEntries(data["Attributes"]),
		}
	case "ListSubscriptions", "ListSubscriptionsByTopic":
		result = snsXListSubsResult{
			XMLName:       xml.Name{Local: action + "Result"},
			Subscriptions: snsToXSubs(data["Subscriptions"]),
			NextToken:     snsAnyToString(data["NextToken"]),
		}
	}
	inner, _ := xml.Marshal(result)
	var b strings.Builder
	b.WriteString(xml.Header)
	b.WriteString("<" + action + "Response xmlns=" + dq + snsNamespace + dq + ">")
	b.Write(inner)
	b.WriteString("<ResponseMetadata><RequestId>" + reqID + "</RequestId></ResponseMetadata>")
	b.WriteString("</" + action + "Response>")
	return []byte(b.String())
}

type snsXErrorInner struct {
	Type    string `xml:"Type"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

type snsXError struct {
	XMLName   xml.Name      `xml:"ErrorResponse"`
	Xmlns     string        `xml:"xmlns,attr"`
	Error     snsXErrorInner `xml:"Error"`
	RequestId string        `xml:"RequestId"`
}

func snsXMLErrorBody(e *APIError, reqID string) []byte {
	b, _ := xml.Marshal(snsXError{
		Xmlns:     snsNamespace,
		Error:     snsXErrorInner{Type: "Sender", Code: e.Code, Message: e.Message},
		RequestId: reqID,
	})
	return append([]byte(xml.Header), b...)
}

// ---- JSON protocol adapter ----

func snsJSONBody(data map[string]any) []byte {
	if data == nil {
		data = map[string]any{}
	}
	b, _ := json.Marshal(data)
	return b
}

func snsJSONErrorBody(e *APIError) []byte {
	b, _ := json.Marshal(map[string]any{
		"__type":  "com.amazonaws.sns#" + e.Code,
		"code":    e.Code,
		"message": e.Message,
	})
	return b
}

// Handle dispatches one raw request and returns HTTP status, response
// content type and response body. JSON protocol is chosen when the
// content type carries x-amz-json or the body is a JSON object; the
// query/XML protocol is used otherwise, exactly like the SQS adapter.
func (s *SNS) Handle(method string, headers map[string]string, body []byte, rawQuery string) (int, string, []byte) {
	hl := map[string]string{}
	for k, v := range headers {
		hl[strings.ToLower(k)] = v
	}
	ctype := strings.ToLower(hl["content-type"])
	trimmedBody := strings.TrimSpace(string(body))
	isJSON := strings.Contains(ctype, "x-amz-json") || strings.HasPrefix(trimmedBody, "{")
	reqID := snsNewUUID()
	if isJSON {
		const jsonCType = "application/x-amz-json-1.0"
		req := &snsRequest{isJSON: true, j: map[string]any{}}
		if trimmedBody != "" {
			_ = json.Unmarshal([]byte(trimmedBody), &req.j)
		}
		action := snsAnyToString(req.j["Type"])
		if action == "" {
			action = snsAnyToString(req.j["type"])
		}
		if action == "" {
			action = snsAnyToString(req.j["Action"])
		}
		if action == "" {
			action = hl["x-amz-target"]
			if i := strings.LastIndexAny(action, "._"); i >= 0 {
				action = action[i+1:]
			}
		}
		data, err := s.dispatch(action, req)
		if err != nil {
			e := toAPIError(err)
			return e.HTTPStatus(), jsonCType, snsJSONErrorBody(e)
		}
		return http.StatusOK, jsonCType, snsJSONBody(data)
	}
	q := url.Values{}
	if rawQuery != "" {
		if parsed, err := url.ParseQuery(rawQuery); err == nil {
			for k, vs := range parsed {
				for _, v := range vs {
					q.Add(k, v)
				}
			}
		}
	}
	if trimmedBody != "" {
		if parsed, err := url.ParseQuery(trimmedBody); err == nil {
			for k, vs := range parsed {
				for _, v := range vs {
					q.Add(k, v)
				}
			}
		}
	}
	action := q.Get("Action")
	if action == "" {
		e := invalidAction(action)
		return e.HTTPStatus(), "text/xml", snsXMLErrorBody(e, reqID)
	}
	req := &snsRequest{q: q}
	data, err := s.dispatch(action, req)
	if err != nil {
		e := toAPIError(err)
		return e.HTTPStatus(), "text/xml", snsXMLErrorBody(e, reqID)
	}
	return http.StatusOK, "text/xml", s.xmlBody(action, data, reqID)
}

// ServeHTTP makes the service directly mountable as an http.Handler.
func (s *SNS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	headers := map[string]string{}
	for k := range r.Header {
		headers[k] = r.Header.Get(k)
	}
	status, ctype, out := s.Handle(r.Method, headers, body, r.URL.RawQuery)
	w.Header().Set("Content-Type", ctype)
	w.Header().Set("x-amzn-RequestId", snsNewUUID())
	w.WriteHeader(status)
	_, _ = w.Write(out)
}