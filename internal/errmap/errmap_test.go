package errmap

import (
    "errors"
    "fmt"
    "net/http"
    "net/http/httptest"
    "strings"
    "testing"
)

type codedError struct {
    code string
    msg  string
}

func (c codedError) Error() string   { return c.msg }
func (c codedError) ErrorCode() string { return c.code }

func TestS3NoSuchKeyWireShape(t *testing.T) {
    out := S3(New("NoSuchKey", "").With("Key", "photos/cat.png"), "req-1", "host-1")

    want := s3XMLDecl +
        `<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message>` +
        `<Key>photos/cat.png</Key><RequestId>req-1</RequestId><HostId>host-1</HostId></Error>`
    if out.Status != http.StatusNotFound {
        t.Fatalf("status = %d, want 404", out.Status)
    }
    if got := string(out.Body); got != want {
        t.Fatalf("body\n got: %s\nwant: %s", got, want)
    }
    if got := out.Header.Get("Content-Type"); got != "application/xml" {
        t.Errorf("Content-Type = %q, want application/xml", got)
    }
    if got := out.Header.Get("X-Amz-Request-Id"); got != "req-1" {
        t.Errorf("X-Amz-Request-Id = %q", got)
    }
    if got := out.Header.Get("X-Amz-Id-2"); got != "host-1" {
        t.Errorf("X-Amz-Id-2 = %q", got)
    }
}

func TestS3NoSuchBucketContextOrder(t *testing.T) {
    out := S3(New("NoSuchBucket", "").With("BucketName", "old-bucket"), "req-4", "host-4")

    want := s3XMLDecl +
        `<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message>` +
        `<BucketName>old-bucket</BucketName><RequestId>req-4</RequestId><HostId>host-4</HostId></Error>`
    if out.Status != http.StatusNotFound {
        t.Fatalf("status = %d, want 404", out.Status)
    }
    if got := string(out.Body); got != want {
        t.Fatalf("body\n got: %s\nwant: %s", got, want)
    }
}

func TestS3AccessDeniedContextOrder(t *testing.T) {
    e := New("AccessDenied", "")
    e.With("Resource", "arn:aws:s3:::b/k")
    e.With("Key", "k")
    out := S3(e, "r", "h")

    want := s3XMLDecl +
        `<Error><Code>AccessDenied</Code><Message>Access Denied</Message>` +
        `<Key>k</Key><Resource>arn:aws:s3:::b/k</Resource>` +
        `<RequestId>r</RequestId><HostId>h</HostId></Error>`
    if got := string(out.Body); got != want {
        t.Fatalf("body\n got: %s\nwant: %s", got, want)
    }
}

func TestS3XMLEscaping(t *testing.T) {
    out := S3(New("InvalidRequest", `a<b> & c & "d"`), "r", "h")
    if !strings.Contains(string(out.Body), `<Message>a&lt;b&gt; &amp; c &amp; "d"</Message>`) {
        t.Fatalf("body = %s", out.Body)
    }
}

func TestS3UnknownCodeFallsBackToInternalError(t *testing.T) {
    out := S3(New("NotAnS3Error", ""), "r", "h")
    want := s3XMLDecl +
        `<Error><Code>InternalError</Code><Message>We encountered an internal error. Please try again.</Message>` +
        `<RequestId>r</RequestId><HostId>h</HostId></Error>`
    if out.Status != http.StatusInternalServerError {
        t.Fatalf("status = %d, want 500", out.Status)
    }
    if got := string(out.Body); got != want {
        t.Fatalf("body\n got: %s\nwant: %s", got, want)
    }
}

func TestS3ExplicitStatusPassthroughAndOverride(t *testing.T) {
    out := S3(&E{Code: "CustomLimitCode", Message: "calm down", Status: 429}, "r", "h")
    if out.Status != 429 {
        t.Fatalf("status = %d, want 429", out.Status)
    }
    if !strings.Contains(string(out.Body), `<Code>CustomLimitCode</Code><Message>calm down</Message>`) {
        t.Fatalf("body = %s", out.Body)
    }

    out = S3(&E{Code: "NoSuchBucket", Status: http.StatusBadRequest}, "r", "h")
    if out.Status != http.StatusBadRequest {
        t.Fatalf("status = %d, want 400", out.Status)
    }
    if !strings.Contains(string(out.Body), `<Message>The specified bucket does not exist</Message>`) {
        t.Fatalf("body = %s", out.Body)
    }
}

func TestDDBConditionalCheckFailedWireShape(t *testing.T) {
    out := DDB(New("ConditionalCheckFailedException", ""), "req-2")

    shape := DDBShapePrefix + "ConditionalCheckFailedException"
    want := `{"__type":"` + shape + `","message":"The conditional request failed"}`
    if out.Status != http.StatusBadRequest {
        t.Fatalf("status = %d, want 400", out.Status)
    }
    if got := string(out.Body); got != want {
        t.Fatalf("body\n got: %s\nwant: %s", got, want)
    }
    if got := out.Header.Get("X-Amzn-Errortype"); got != shape {
        t.Errorf("X-Amzn-ErrorType = %q, want %q", got, shape)
    }
    if got := out.Header.Get("Content-Type"); got != "application/x-amz-json-1.0" {
        t.Errorf("Content-Type = %q", got)
    }
    if got := out.Header.Get("X-Amzn-Requestid"); got != "req-2" {
        t.Errorf("X-Amzn-Requestid = %q", got)
    }
}

func TestDDBUnknownCodeFallsBack(t *testing.T) {
    out := DDB(New("TotallyBogus", ""), "r")
    want := `{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"We encountered an internal error. Please try again."}`
    if out.Status != http.StatusInternalServerError {
        t.Fatalf("status = %d, want 500", out.Status)
    }
    if got := string(out.Body); got != want {
        t.Fatalf("body\n got: %s\nwant: %s", got, want)
    }
}

func TestDDBJSONEscaping(t *testing.T) {
    out := DDB(New("ValidationException", `2 > 1 & x < y`), "r")
    want := `{"__type":"com.amazonaws.dynamodb.v20120810#ValidationException","message":"2 > 1 & x < y"}`
    if got := string(out.Body); got != want {
        t.Fatalf("body\n got: %s\nwant: %s", got, want)
    }
}

func TestDDBExtraMembers(t *testing.T) {
    e := New("TransactionCanceledException", "")
    e.Extra = []byte(`"CancellationReasons":[{"Code":"None","Message":null}]`)
    out := DDB(e, "r")
    want := `{"__type":"com.amazonaws.dynamodb.v20120810#TransactionCanceledException","message":"Transaction cancelled, please refer cancellation reasons for specific reasons.","CancellationReasons":[{"Code":"None","Message":null}]}`
    if got := string(out.Body); got != want {
        t.Fatalf("body\n got: %s\nwant: %s", got, want)
    }

    e2 := New("TransactionInProgressException", "")
    e2.Extra = []byte(`{"x":1}`)
    out2 := DDB(e2, "r")
    want2 := `{"__type":"com.amazonaws.dynamodb.v20120810#TransactionInProgressException","message":"A transaction with the given ID is already in progress.","x":1}`
    if got := string(out2.Body); got != want2 {
        t.Fatalf("body\n got: %s\nwant: %s", got, want2)
    }
}

func TestDDBPassthroughShapes(t *testing.T) {
    out := DDB(&E{Code: "CustomOpException", Message: "boom", Status: http.StatusTooManyRequests}, "r")
    want := `{"__type":"com.amazonaws.dynamodb.v20120810#CustomOpException","message":"boom"}`
    if out.Status != http.StatusTooManyRequests {
        t.Fatalf("status = %d, want 429", out.Status)
    }
    if got := string(out.Body); got != want {
        t.Fatalf("body\n got: %s\nwant: %s", got, want)
    }

    out = DDB(&E{Code: DDBCoralPrefix + "InvalidSignatureException", Message: "sig", Status: http.StatusBadRequest}, "r")
    want = `{"__type":"com.amazon.coral.service#InvalidSignatureException","message":"sig"}`
    if got := string(out.Body); got != want {
        t.Fatalf("body\n got: %s\nwant: %s", got, want)
    }
}

func TestSQSQueueDoesNotExistWireShape(t *testing.T) {
    out := SQS(New("QueueDoesNotExist", ""), "req-3")

    want := sqsXMLDecl +
        `<ErrorResponse xmlns="` + SQSNamespace + `"><Error><Type>Sender</Type>` +
        `<Code>AWS.SimpleQueueService.NonExistentQueue</Code>` +
        `<Message>The specified queue doesn't exist.</Message><Detail/></Error>` +
        `<RequestId>req-3</RequestId></ErrorResponse>`
    if out.Status != http.StatusBadRequest {
        t.Fatalf("status = %d, want 400", out.Status)
    }
    if got := string(out.Body); got != want {
        t.Fatalf("body\n got: %s\nwant: %s", got, want)
    }
    if got := out.Header.Get("Content-Type"); got != "text/xml" {
        t.Errorf("Content-Type = %q", got)
    }
}

func TestSQSInternalErrorFallback(t *testing.T) {
    out := SQS(New("NotAnSQSError", ""), "r")
    want := sqsXMLDecl +
        `<ErrorResponse xmlns="` + SQSNamespace + `"><Error><Type>Receiver</Type><Code>InternalError</Code>` +
        `<Message>We encountered an internal error. Please try again.</Message><Detail/></Error>` +
        `<RequestId>r</RequestId></ErrorResponse>`
    if out.Status != http.StatusInternalServerError {
        t.Fatalf("status = %d, want 500", out.Status)
    }
    if got := string(out.Body); got != want {
        t.Fatalf("body\n got: %s\nwant: %s", got, want)
    }
}

func TestSQSPassthroughAndEscaping(t *testing.T) {
    out := SQS(&E{Code: "CustomSQSError", Message: "m", Status: http.StatusBadRequest}, "sid")
    want := sqsXMLDecl +
        `<ErrorResponse xmlns="` + SQSNamespace + `"><Error><Type>Sender</Type><Code>CustomSQSError</Code>` +
        `<Message>m</Message><Detail/></Error><RequestId>sid</RequestId></ErrorResponse>`
    if got := string(out.Body); got != want {
        t.Fatalf("body\n got: %s\nwant: %s", got, want)
    }

    out = SQS(&E{Code: "CustomSQSError", Status: http.StatusServiceUnavailable}, "sid")
    if out.Status != http.StatusServiceUnavailable {
        t.Fatalf("status = %d, want 503", out.Status)
    }
    if !strings.Contains(string(out.Body), `<Type>Receiver</Type><Code>CustomSQSError</Code>`) {
        t.Fatalf("body = %s", out.Body)
    }
    if !strings.Contains(string(out.Body), "We encountered an internal error. Please try again.") {
        t.Fatalf("body = %s", out.Body)
    }

    out = SQS(New("InvalidMessageContents", "bad <char> & co"), "r")
    if !strings.Contains(string(out.Body), `<Message>bad &lt;char&gt; &amp; co</Message>`) {
        t.Fatalf("body = %s", out.Body)
    }
}

func TestFromMapping(t *testing.T) {
    if e := From(nil); e.Code != internalCode {
        t.Errorf("From(nil).Code = %q", e.Code)
    }
    plain := From(errors.New("kaboom"))
    if plain.Code != internalCode || plain.Message != "" {
        t.Errorf("From(plain) = %+v", plain)
    }

    base := Wrap(errors.New("boom"), "NoSuchUpload", "")
    if got := From(base); got != base {
        t.Errorf("From should pass *E through unchanged")
    }
    if got := From(fmt.Errorf("complete: %w", base)); got != base {
        t.Errorf("From should unwrap to the same *E")
    }

    ce := codedError{code: "ResourceNotFoundException", msg: "Requested resource not found: Table: t not found"}
    got := From(ce)
    if got.Code != ce.code || got.Message != ce.msg {
        t.Errorf("From(coder) = %q/%q", got.Code, got.Message)
    }
    got = From(fmt.Errorf("outer: %w", ce))
    if got.Code != ce.code || got.Message != ce.msg {
        t.Errorf("From(wrapped coder) = %q/%q", got.Code, got.Message)
    }
}

func TestEErrorString(t *testing.T) {
    cause := errors.New("boom")
    e := Wrap(cause, "NoSuchKey", "no key")
    if !errors.Is(e, cause) {
        t.Error("errors.Is should reach the cause")
    }
    if e.Error() != "NoSuchKey: no key: boom" {
        t.Errorf("Error() = %q", e.Error())
    }
    if New("", "").Error() != internalCode {
        t.Errorf("empty E.Error() = %q", New("", "").Error())
    }
}

func TestCatalogIntegrity(t *testing.T) {
    seen := map[string]bool{}
    for _, d := range catalog {
        if d.Code == "" {
            t.Fatal("catalog entry with empty Code")
        }
        if seen[d.Code] {
            t.Fatalf("duplicate catalog entry for %q", d.Code)
        }
        seen[d.Code] = true

        families := 0
        if d.S3Status != 0 {
            families++
            if d.S3Status < 400 || d.S3Status >= 600 {
                t.Errorf("%s: bad S3 status %d", d.Code, d.S3Status)
            }
            if d.S3Msg == "" {
                t.Errorf("%s: missing S3 message", d.Code)
            }
        }
        if d.DDBStatus != 0 {
            families++
            if d.DDBStatus < 400 || d.DDBStatus >= 600 {
                t.Errorf("%s: bad DDB status %d", d.Code, d.DDBStatus)
            }
            if !strings.Contains(d.DDBShape, "#") {
                t.Errorf("%s: bad DDB shape %q", d.Code, d.DDBShape)
            }
            if d.DDBMsg == "" && d.Code != "SerializationException" {
                t.Errorf("%s: missing DDB message", d.Code)
            }
        }
        if d.SQSStatus != 0 {
            families++
            if d.SQSStatus < 400 || d.SQSStatus >= 600 {
                t.Errorf("%s: bad SQS status %d", d.Code, d.SQSStatus)
            }
            if d.SQSType != Sender && d.SQSType != Receiver {
                t.Errorf("%s: bad SQS type %q", d.Code, d.SQSType)
            }
            if d.SQSMsg == "" {
                t.Errorf("%s: missing SQS message", d.Code)
            }
        }
        if families == 0 {
            t.Fatalf("%s is not registered with any family", d.Code)
        }
    }
}

func TestCatalogFamilyCoverage(t *testing.T) {
    for _, c := range []string{"NoSuchKey", "NoSuchBucket", "NoSuchUpload", "MalformedXML", "PreconditionFailed", "SlowDown", "SignatureDoesNotMatch", "AccessDenied", "InternalError"} {
        d, ok := Lookup(c)
        if !ok {
            t.Fatalf("missing canonical code %q", c)
        }
        if d.S3Status == 0 {
            t.Errorf("%q not exposed by the S3 family", c)
        }
    }
    for _, c := range []string{"ConditionalCheckFailedException", "ValidationException", "ResourceNotFoundException", "ThrottlingException", "ProvisionedThroughputExceededException", "InternalError", "AccessDenied"} {
        d, ok := Lookup(c)
        if !ok {
            t.Fatalf("missing canonical code %q", c)
        }
        if d.DDBStatus == 0 {
            t.Errorf("%q not exposed by the DDB family", c)
        }
    }
    for _, c := range []string{"QueueDoesNotExist", "ReceiptHandleIsInvalid", "InvalidAttributeName", "MessageTooLong", "RequestThrottled", "PurgeQueueInProgress", "AccessDenied", "InternalError"} {
        d, ok := Lookup(c)
        if !ok {
            t.Fatalf("missing canonical code %q", c)
        }
        if d.SQSStatus == 0 {
            t.Errorf("%q not exposed by the SQS family", c)
        }
    }
}

func TestSharedAccessDenied(t *testing.T) {
    d, ok := Lookup("AccessDenied")
    if !ok {
        t.Fatal("missing AccessDenied")
    }
    if d.S3Status != http.StatusForbidden || d.S3Msg != "Access Denied" {
        t.Errorf("S3 row = %+v", d)
    }
    if d.DDBStatus != http.StatusBadRequest || d.DDBShape != DDBShapePrefix+"AccessDeniedException" {
        t.Errorf("DDB row = %+v", d)
    }
    if d.SQSStatus != http.StatusForbidden || d.SQSType != Sender {
        t.Errorf("SQS row = %+v", d)
    }
}

func TestStatusHelpers(t *testing.T) {
    if got := S3Status("NoSuchKey"); got != http.StatusNotFound {
        t.Errorf("S3Status(NoSuchKey) = %d", got)
    }
    if got := S3Status("Bogus"); got != http.StatusInternalServerError {
        t.Errorf("S3Status(Bogus) = %d", got)
    }
    if got := DDBStatus("ConditionalCheckFailedException"); got != http.StatusBadRequest {
        t.Errorf("DDBStatus(ConditionalCheckFailedException) = %d", got)
    }
    if got := DDBStatus("Bogus"); got != http.StatusInternalServerError {
        t.Errorf("DDBStatus(Bogus) = %d", got)
    }
    if got := SQSStatus("QueueDoesNotExist"); got != http.StatusBadRequest {
        t.Errorf("SQSStatus(QueueDoesNotExist) = %d", got)
    }
    if got := SQSStatus("Bogus"); got != http.StatusInternalServerError {
        t.Errorf("SQSStatus(Bogus) = %d", got)
    }
}

func TestOutWrite(t *testing.T) {
    out := SQS(New("RequestThrottled", ""), "req-9")
    rec := httptest.NewRecorder()
    if err := out.Write(rec); err != nil {
        t.Fatal(err)
    }
    if rec.Code != http.StatusForbidden {
        t.Errorf("code = %d, want 403", rec.Code)
    }
    if got := rec.Header().Get("Content-Type"); got != "text/xml" {
        t.Errorf("Content-Type = %q", got)
    }
    if !strings.Contains(rec.Body.String(), `<Code>RequestThrottled</Code>`) {
        t.Errorf("body = %s", rec.Body.String())
    }
}
