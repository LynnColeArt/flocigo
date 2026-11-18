package sqs

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateQueueCreatesQueueFile(t *testing.T) {
	dir := t.TempDir()

	queueURLStr, err := CreateQueue(dir, "orders", "http://localhost:4566", nil)
	if err != nil {
		t.Fatalf("CreateQueue returned error: %v", err)
	}
	if want := "http://localhost:4566/" + sqsDefaultAccountID + "/orders"; queueURLStr != want {
		t.Fatalf("QueueUrl = %q, want %q", queueURLStr, want)
	}

	raw, err := os.ReadFile(filepath.Join(dir, "orders.json"))
	if err != nil {
		t.Fatalf("queue file was not created: %v", err)
	}
	var qf createQueueFileData
	if err := json.Unmarshal(raw, &qf); err != nil {
		t.Fatalf("queue file is not valid JSON: %v", err)
	}
	if qf.QueueName != "orders" {
		t.Errorf("QueueName = %q, want orders", qf.QueueName)
	}
	if !strings.HasPrefix(qf.QueueArn, "arn:aws:sqs:") || !strings.HasSuffix(qf.QueueArn, ":orders") {
		t.Errorf("QueueArn = %q, want arn:aws:sqs:<region>:<account>:orders", qf.QueueArn)
	}
	if qf.Messages == nil {
		t.Error("Messages should be an empty array, not null")
	}
	for name, want := range defaultCreateAttributes {
		if got := qf.Attributes[name]; got != want {
			t.Errorf("attribute %s = %q, want default %q", name, got, want)
		}
	}
}

func TestCreateQueueSuppliedAttributesStored(t *testing.T) {
	dir := t.TempDir()
	if _, err := CreateQueue(dir, "with-attrs", "http://h", map[string]string{
		"DelaySeconds": "10",
		"VisibilityTimeout": "45",
		"ContentBasedDeduplication": "true",
	}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "with-attrs.json"))
	if err != nil {
		t.Fatal(err)
	}
	var qf createQueueFileData
	if err := json.Unmarshal(raw, &qf); err != nil {
		t.Fatal(err)
	}
	if qf.Attributes["DelaySeconds"] != "10" || qf.Attributes["VisibilityTimeout"] != "45" || qf.Attributes["ContentBasedDeduplication"] != "true" {
		t.Errorf("stored attributes = %v", qf.Attributes)
	}
	if qf.Attributes["MaximumMessageSize"] != "262144" {
		t.Errorf("default MaximumMessageSize = %q", qf.Attributes["MaximumMessageSize"])
	}
}

func TestCreateQueueIdempotentWhenAttributesMatch(t *testing.T) {
	dir := t.TempDir()

	first, err := CreateQueue(dir, "jobs", "http://h", map[string]string{"DelaySeconds": "3"})
	if err != nil {
		t.Fatalf("initial CreateQueue: %v", err)
	}

	// Recreate without any attributes: nothing differs, existing URL is returned.
	again, err := CreateQueue(dir, "jobs", "http://h", nil)
	if err != nil {
		t.Fatalf("recreate without attributes: %v", err)
	}
	if again != first {
		t.Errorf("QueueUrl changed: %q != %q", again, first)
	}

	// Recreate with exactly the same attribute value.
	same, err := CreateQueue(dir, "jobs", "http://h", map[string]string{"DelaySeconds": "3"})
	if err != nil {
		t.Fatalf("recreate with same attributes: %v", err)
	}
	if same != first {
		t.Errorf("QueueUrl changed: %q != %q", same, first)
	}
}

func TestCreateQueueQueueNameExists(t *testing.T) {
	dir := t.TempDir()
	if _, err := CreateQueue(dir, "jobs", "http://h", map[string]string{"VisibilityTimeout": "5"}); err != nil {
		t.Fatalf("initial CreateQueue: %v", err)
	}

	_, err := CreateQueue(dir, "jobs", "http://h", map[string]string{"VisibilityTimeout": "7"})
	var existsErr *QueueNameExistsError
	if !errors.As(err, &existsErr) {
		t.Fatalf("want *QueueNameExistsError, got %v", err)
	}
	if !errors.Is(err, ErrQueueNameExists) {
		t.Error("errors.Is(err, ErrQueueNameExists) = false")
	}
	if existsErr.Attribute != "VisibilityTimeout" {
		t.Errorf("Attribute = %q, want VisibilityTimeout", existsErr.Attribute)
	}
	want := "A queue already exists with the same name and a different value for attribute VisibilityTimeout"
	if existsErr.Error() != want {
		t.Errorf("message = %q, want %q", existsErr.Error(), want)
	}
}

func TestCreateQueueValidatesAttributes(t *testing.T) {
	dir := t.TempDir()

	_, err := CreateQueue(dir, "v", "http://h", map[string]string{"Bogus": "1"})
	var attrErr *QueueAttributeError
	if !errors.As(err, &attrErr) || !attrErr.NameInvalid {
		t.Fatalf("want invalid attribute name error, got %v", err)
	}
	if !strings.Contains(attrErr.Error(), "Unknown Attribute Bogus") {
		t.Errorf("message = %q", attrErr.Error())
	}

	_, err = CreateQueue(dir, "v", "http://h", map[string]string{"DelaySeconds": "901"})
	if !errors.As(err, &attrErr) || attrErr.NameInvalid {
		t.Fatalf("want invalid attribute value error, got %v", err)
	}
	if !strings.Contains(attrErr.Error(), "Invalid value for parameter DelaySeconds") {
		t.Errorf("message = %q", attrErr.Error())
	}

	_, err = CreateQueue(dir, "v", "http://h", map[string]string{"VisibilityTimeout": "abc"})
	if !errors.As(err, &attrErr) || attrErr.NameInvalid {
		t.Fatalf("want invalid attribute value error, got %v", err)
	}
}

func TestCreateQueueNameValidation(t *testing.T) {
	dir := t.TempDir()

	if _, err := CreateQueue(dir, "", "", nil); !errors.Is(err, ErrMissingQueueName) {
		t.Fatalf("want ErrMissingQueueName, got %v", err)
	}
	if _, err := CreateQueue(dir, strings.Repeat("a", 81), "", nil); !errors.Is(err, ErrInvalidQueueName) {
		t.Fatalf("want ErrInvalidQueueName for long name, got %v", err)
	}
	if _, err := CreateQueue(dir, "bad name", "", nil); !errors.Is(err, ErrInvalidQueueName) {
		t.Fatalf("want ErrInvalidQueueName for space, got %v", err)
	}
	if _, err := CreateQueue(dir, "a-b_c.d.fifo", "", nil); err != nil {
		t.Fatalf("valid name rejected: %v", err)
	}
}

func TestHandleCreateQueueWire(t *testing.T) {
	dir := t.TempDir()

	post := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Host = "sqs.local:4566"
		rec := httptest.NewRecorder()
		HandleCreateQueue(rec, req, dir)
		return rec
	}

	res := post(`Action=CreateQueue&QueueName=wire&Attribute.1.Name=VisibilityTimeout&Attribute.1.Value=11`)
	if res.Code != http.StatusOK {
		t.Fatalf("code = %d, body = %s", res.Code, res.Body.String())
	}
	if want := "<QueueUrl>http://sqs.local:4566/" + sqsDefaultAccountID + "/wire</QueueUrl>"; !strings.Contains(res.Body.String(), want) {
		t.Errorf("body missing QueueUrl %q:\n%s", want, res.Body.String())
	}

	// Idempotent call with the same attribute value.
	res = post(`Action=CreateQueue&QueueName=wire&Attribute.1.Name=VisibilityTimeout&Attribute.1.Value=11`)
	if res.Code != http.StatusOK {
		t.Fatalf("idempotent code = %d, body = %s", res.Code, res.Body.String())
	}

	// Different attribute value: QueueNameExists, HTTP 400, Sender.
	res = post(`Action=CreateQueue&QueueName=wire&Attribute.1.Name=VisibilityTimeout&Attribute.1.Value=12`)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, body = %s", res.Code, res.Body.String())
	}
	body := res.Body.String()
	for _, want := range []string{"<Type>Sender</Type>", "<Code>QueueNameExists</Code>", "different value for attribute VisibilityTimeout"} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q:\n%s", want, body)
		}
	}

	// Legacy map encoding: Attribute.entry.1.key / .value.
	res = post(`Action=CreateQueue&QueueName=entry&Attribute.entry.1.key=DelaySeconds&Attribute.entry.1.value=7`)
	if res.Code != http.StatusOK {
		t.Fatalf("code = %d, body = %s", res.Code, res.Body.String())
	}
	raw, err := os.ReadFile(filepath.Join(dir, "entry.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"DelaySeconds": "7"`) {
		t.Errorf("DelaySeconds not stored:\n%s", raw)
	}

	// Missing QueueName -> MissingParameter.
	res = post(`Action=CreateQueue`)
	if res.Code != http.StatusBadRequest || !strings.Contains(res.Body.String(), "<Code>MissingParameter</Code>") {
		t.Errorf("code = %d, body = %s", res.Code, res.Body.String())
	}
}
