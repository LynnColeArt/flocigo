package sqs

// Ticket floci-go/ticket-020 — CreateQueue: create the queue file, return the
// QueueUrl. Calling CreateQueue for an existing queue name is idempotent when
// every attribute supplied in the request matches the stored queue, and fails
// with QueueNameExists otherwise.
//
// Queue files are one JSON document per queue under a flat directory, e.g.
//
//	{
//	  "QueueName": "orders",
//	  "Attributes": {"DelaySeconds": "0", "MaximumMessageSize": "262144"},
//	  "QueueArn": "arn:aws:sqs:us-east-1:123456789012:orders",
//	  "CreatedTimestamp": "1720612345",
//	  "LastModifiedTimestamp": "1720612345",
//	  "Messages": []
//	}
//
// HandleCreateQueue renders the SQS query-protocol response/error for the
// action so the wire layer can mount it with:
//
//	case "CreateQueue":
//		sqs.HandleCreateQueue(w, r, queueDir)

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Sentinels wrapped by the concrete error types below.
var (
	ErrQueueNameExists  = errors.New("sqs: queue already exists with different attributes")
	ErrMissingQueueName = errors.New("sqs: the request must contain the parameter QueueName")
	ErrInvalidQueueName = errors.New("sqs: invalid queue name")
)

// QueueNameExistsError is returned when the queue name already exists but a
// supplied attribute differs from the stored queue. Attribute names the first
// differing attribute, mirroring SQS:
//
//	A queue already exists with the same name and a different value for attribute VisibilityTimeout
type QueueNameExistsError struct {
	QueueName string
	Attribute string
}

func (e *QueueNameExistsError) Error() string {
	return "A queue already exists with the same name and a different value for attribute " + e.Attribute
}

// Unwrap lets errors.Is(err, ErrQueueNameExists) succeed.
func (e *QueueNameExistsError) Unwrap() error { return ErrQueueNameExists }

// QueueAttributeError reports an unknown attribute name or an invalid value.
type QueueAttributeError struct {
	Attribute   string
	Value       string
	Reason      string
	NameInvalid bool
}

func (e *QueueAttributeError) Error() string {
	if e.NameInvalid {
		return "Unknown Attribute " + e.Attribute + "."
	}
	if e.Reason != "" {
		return "Invalid value for parameter " + e.Attribute + ". Reason: " + e.Reason
	}
	return "Invalid value for parameter " + e.Attribute + "."
}

// QueueNameInvalidError reports a queue name that violates SQS naming rules.
type QueueNameInvalidError struct {
	QueueName string
}

func (e *QueueNameInvalidError) Error() string {
	return "The address " + e.QueueName + " is not valid."
}

// Unwrap lets errors.Is(err, ErrInvalidQueueName) succeed.
func (e *QueueNameInvalidError) Unwrap() error { return ErrInvalidQueueName }

type attrKind int

const (
	attrInt attrKind = iota
	attrBool
	attrEnum
	attrString
)

type attrSpec struct {
	kind attrKind
	min int64
	max int64
	choices []string
}

// queueAttrSpecs lists every attribute accepted by CreateQueue with its
// validation rules.
var queueAttrSpecs = map[string]attrSpec{
	"DelaySeconds": {kind: attrInt, min: 0, max: 900},
	"MaximumMessageSize": {kind: attrInt, min: 1024, max: 262144},
	"MessageRetentionPeriod": {kind: attrInt, min: 60, max: 1209600},
	"ReceiveMessageWaitTimeSeconds": {kind: attrInt, min: 0, max: 20},
	"VisibilityTimeout": {kind: attrInt, min: 0, max: 43200},
	"KmsDataKeyReusePeriodSeconds": {kind: attrInt, min: 60, max: 86400},
	"ContentBasedDeduplication": {kind: attrBool},
	"FifoQueue": {kind: attrBool},
	"SqsManagedSseEnabled": {kind: attrBool},
	"DeduplicationScope": {kind: attrEnum, choices: []string{"messageGroup", "queue"}},
	"FifoThroughputLimit": {kind: attrEnum, choices: []string{"perMessageGroupId", "perQueue"}},
	"Policy": {kind: attrString},
	"RedrivePolicy": {kind: attrString},
	"RedriveAllowPolicy": {kind: attrString},
	"KmsMasterKeyId": {kind: attrString},
}

// defaultCreateAttributes are the attribute values an SQS queue starts with.
var defaultCreateAttributes = map[string]string{
	"DelaySeconds": "0",
	"MaximumMessageSize": "262144",
	"MessageRetentionPeriod": "345600",
	"ReceiveMessageWaitTimeSeconds": "0",
	"VisibilityTimeout": "30",
}

// sqsDefaultAccountID is the emulated AWS account id embedded in QueueUrls.
const sqsDefaultAccountID = "123456789012"

// createQueueFileData is the on-disk queue file layout.
type createQueueFileData struct {
	QueueName string `json:"QueueName"`
	Attributes map[string]string `json:"Attributes,omitempty"`
	QueueArn string `json:"QueueArn,omitempty"`
	CreatedTimestamp string `json:"CreatedTimestamp,omitempty"`
	LastModifiedTimestamp string `json:"LastModifiedTimestamp,omitempty"`
	Messages []json.RawMessage `json:"Messages"`
}

// CreateQueue creates the queue file for queueName under dir and returns its
// QueueUrl (scheme://host/<account-id>/<name>). attributes holds the
// CreateQueue attribute parameters; supplied attributes are validated and
// merged over the SQS defaults. If the queue already exists, the existing URL
// is returned (idempotent) as long as every supplied attribute matches the
// stored value; otherwise a *QueueNameExistsError naming the first differing
// attribute is returned.
func CreateQueue(dir, queueName, hostBase string, attributes map[string]string) (string, error) {
	name := strings.TrimSpace(queueName)
	if name == "" {
		return "", ErrMissingQueueName
	}
	if len(name) > 80 || !validQueueNameChars(name) {
		return "", &QueueNameInvalidError{QueueName: name}
	}

	provided := make(map[string]string, len(attributes))
	providedNames := make([]string, 0, len(attributes))
	for n := range attributes {
		spec, ok := queueAttrSpecs[n]
		if !ok {
			return "", &QueueAttributeError{Attribute: n, NameInvalid: true}
		}
		v, aerr := normalizeAttrValue(n, spec, attributes[n])
		if aerr != nil {
			return "", aerr
		}
		provided[n] = v
		providedNames = append(providedNames, n)
	}
	sort.Strings(providedNames)

	if dir == "" {
		dir = "sqs"
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("sqs: cannot create queue directory %q: %w", dir, err)
	}
	path := createQueueFilePath(dir, name)

	if qf, err := readCreateQueueFile(path); err == nil && qf != nil {
		for _, n := range providedNames {
			want := provided[n]
			cur, ok := effectiveStoredValue(qf.Attributes, n)
			if ok && !attrEqual(queueAttrSpecs[n], cur, want) {
				return "", &QueueNameExistsError{QueueName: name, Attribute: n}
			}
			if !ok && want != "" {
				return "", &QueueNameExistsError{QueueName: name, Attribute: n}
			}
		}
		return queueURL(hostBase, sqsDefaultAccountID, name), nil
	}

	stored := make(map[string]string, len(defaultCreateAttributes)+len(provided))
	for k, v := range defaultCreateAttributes {
		stored[k] = v
	}
	for k, v := range provided {
		stored[k] = v
	}
	now := strconv.FormatInt(time.Now().Unix(), 10)
	qf := &createQueueFileData{
		QueueName: name,
		Attributes: stored,
		QueueArn: fmt.Sprintf("arn:aws:sqs:%s:%s:%s", sqsRegion(), sqsDefaultAccountID, name),
		CreatedTimestamp: now,
		LastModifiedTimestamp: now,
		Messages: []json.RawMessage{},
	}
	if err := writeCreateQueueFile(path, qf); err != nil {
		return "", fmt.Errorf("sqs: cannot write queue file %q: %w", path, err)
	}
	return queueURL(hostBase, sqsDefaultAccountID, name), nil
}

func createQueueFilePath(dir, name string) string { return filepath.Join(dir, name+".json") }

// readCreateQueueFile loads a queue file. A missing file yields (nil, nil); a
// corrupt file yields an error, which CreateQueue treats as "create it".
func readCreateQueueFile(path string) (*createQueueFileData, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var qf createQueueFileData
	if err := json.Unmarshal(raw, &qf); err != nil {
		return nil, err
	}
	return &qf, nil
}

func writeCreateQueueFile(path string, qf *createQueueFileData) error {
	raw, err := json.MarshalIndent(qf, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

// sqsRegion reads the emulated AWS region from the environment.
func sqsRegion() string {
	if v := os.Getenv("AWS_REGION"); v != "" {
		return v
	}
	if v := os.Getenv("AWS_DEFAULT_REGION"); v != "" {
		return v
	}
	return "us-east-1"
}

func queueURL(hostBase, accountID, name string) string {
	base := strings.TrimSuffix(strings.TrimSpace(hostBase), "/")
	if base == "" {
		base = "http://localhost:4566"
	}
	if accountID != "" {
		return base + "/" + accountID + "/" + name
	}
	return base + "/" + name
}

// validQueueNameChars enforces the SQS queue-name charset (letters, digits,
// hyphens, underscores and periods).
func validQueueNameChars(name string) bool {
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '-', r == '_', r == '.':
		default:
			return false
		}
	}
	return true
}

// normalizeAttrValue validates and canonicalizes one attribute value.
func normalizeAttrValue(name string, spec attrSpec, raw string) (string, *QueueAttributeError) {
	v := strings.TrimSpace(raw)
	switch spec.kind {
	case attrInt:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return "", &QueueAttributeError{Attribute: name, Value: raw, Reason: name + " must be an integer."}
		}
		if n < spec.min || n > spec.max {
			return "", &QueueAttributeError{
				Attribute: name,
				Value: raw,
				Reason: fmt.Sprintf("%s must be an integer from %d to %d %s.", name, spec.min, spec.max, unitForAttr(name)),
			}
		}
		return strconv.FormatInt(n, 10), nil
	case attrBool:
		switch strings.ToLower(v) {
		case "true":
			return "true", nil
		case "false":
			return "false", nil
		}
		return "", &QueueAttributeError{Attribute: name, Value: raw, Reason: name + " must be true or false."}
	case attrEnum:
		for _, c := range spec.choices {
			if v == c {
				return v, nil
			}
		}
		return "", &QueueAttributeError{Attribute: name, Value: raw, Reason: name + " must be one of " + strings.Join(spec.choices, ", ") + "."}
	default:
		return v, nil
	}
}

func unitForAttr(name string) string {
	if name == "MaximumMessageSize" {
		return "bytes"
	}
	return "seconds"
}

// attrEqual compares a stored attribute value against a canonicalized request
// value, numerically/booleanly where applicable.
func attrEqual(spec attrSpec, stored, want string) bool {
	switch spec.kind {
	case attrInt:
		a, aerr := strconv.ParseInt(strings.TrimSpace(stored), 10, 64)
		b, berr := strconv.ParseInt(want, 10, 64)
		if aerr == nil && berr == nil {
			return a == b
		}
	case attrBool:
		return strings.EqualFold(strings.TrimSpace(stored), want)
	}
	return strings.TrimSpace(stored) == want
}

// effectiveStoredValue resolves a queue attribute as it currently reads,
// falling back to SQS defaults (absent booleans read as "false").
func effectiveStoredValue(stored map[string]string, name string) (string, bool) {
	if stored != nil {
		if v, ok := stored[name]; ok {
			return v, true
		}
	}
	if spec, ok := queueAttrSpecs[name]; ok && spec.kind == attrBool {
		return "false", true
	}
	if v, ok := defaultCreateAttributes[name]; ok {
		return v, true
	}
	return "", false
}

// --- wire: SQS query-protocol rendering ---

const sqsXMLNamespace = `http://queue.amazonaws.com/doc/2012-11-05/`

// HandleCreateQueue serves one SQS query-protocol CreateQueue request against
// the queue directory dir. The request body must not have been consumed by
// the caller (the form is parsed from the request).
func HandleCreateQueue(w http.ResponseWriter, r *http.Request, dir string) {
	if err := r.ParseForm(); err != nil {
		writeQueueXMLError(w, http.StatusBadRequest, "Sender", "InvalidQueryParameter", "Could not parse the request.")
		return
	}
	name := r.Form.Get("QueueName")
	attributes := parseQueueAttributeForm(r.Form)
	queueURLStr, err := CreateQueue(dir, name, requestHostBase(r), attributes)
	if err != nil {
		writeCreateQueueError(w, err)
		return
	}
	writeQueueXMLResponse(w, "<CreateQueueResult><QueueUrl>"+xmlTextEscape(queueURLStr)+"</QueueUrl></CreateQueueResult>")
}

// parseQueueAttributeForm reads Attribute.N.Name/Value pairs plus the legacy
// Attribute.entry.N.key/value encoding from an SQS query request form.
func parseQueueAttributeForm(form url.Values) map[string]string {
	names := map[string]string{}
	values := map[string]string{}
	for key, vs := range form {
		if len(vs) == 0 {
			continue
		}
		fields := strings.Split(key, ".")
		if len(fields) < 3 || !strings.EqualFold(fields[0], "Attribute") {
			continue
		}
		idx := fields[1]
		last := fields[len(fields)-1]
		if strings.EqualFold(fields[1], "entry") {
			if len(fields) < 4 {
				continue
			}
			idx = fields[2]
			last = fields[3]
		}
		v := vs[len(vs)-1]
		switch strings.ToLower(last) {
		case "name", "key":
			names[idx] = v
		case "value":
			values[idx] = v
		}
	}
	attributes := make(map[string]string, len(names))
	for idx, n := range names {
		attributes[n] = values[idx]
	}
	return attributes
}

// requestHostBase rebuilds scheme://host for QueueUrl construction.
func requestHostBase(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		scheme = proto
	}
	host := r.Host
	if host == "" {
		host = "localhost:4566"
	}
	return scheme + "://" + host
}

// writeCreateQueueError maps a CreateQueue failure onto the SQS wire error.
func writeCreateQueueError(w http.ResponseWriter, err error) {
	status, errType, code, message := http.StatusInternalServerError, "Receiver", "InternalError", "Internal Error"
	var existsErr *QueueNameExistsError
	var nameErr *QueueNameInvalidError
	var attrErr *QueueAttributeError
	switch {
	case errors.As(err, &existsErr):
		status, errType, code, message = http.StatusBadRequest, "Sender", "QueueNameExists", existsErr.Error()
	case errors.Is(err, ErrMissingQueueName):
		status, errType, code, message = http.StatusBadRequest, "Sender", "MissingParameter", "The request must contain the parameter QueueName."
	case errors.As(err, &nameErr):
		status, errType, code, message = http.StatusBadRequest, "Sender", "InvalidAddress", nameErr.Error()
	case errors.As(err, &attrErr) && attrErr.NameInvalid:
		status, errType, code, message = http.StatusBadRequest, "Sender", "InvalidAttributeName", attrErr.Error()
	case errors.As(err, &attrErr):
		status, errType, code, message = http.StatusBadRequest, "Sender", "InvalidAttributeValue", attrErr.Error()
	}
	writeQueueXMLError(w, status, errType, code, message)
}

func writeQueueXMLResponse(w http.ResponseWriter, result string) {
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `<?xml version="1.0"?><CreateQueueResponse xmlns="%s">%s<ResponseMetadata><RequestId>%s</RequestId></ResponseMetadata></CreateQueueResponse>`, sqsXMLNamespace, result, newQueueRequestID())
}

func writeQueueXMLError(w http.ResponseWriter, status int, errType, code, message string) {
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<?xml version="1.0"?><ErrorResponse xmlns="%s"><Error><Type>%s</Type><Code>%s</Code><Message>%s</Message><Detail/></Error><RequestId>%s</RequestId></ErrorResponse>`, sqsXMLNamespace, errType, code, xmlTextEscape(message), newQueueRequestID())
}

func xmlTextEscape(s string) string {
	return strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&#34;",
		"'", "&#39;",
	).Replace(s)
}

func newQueueRequestID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err == nil {
		return hex.EncodeToString(buf[:])
	}
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}
