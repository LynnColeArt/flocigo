package sigv4

import (
    "crypto/hmac"
    "crypto/sha256"
    "encoding/hex"
    "io"
    "net/http"
    "net/url"
    "strings"
    "testing"
    "time"
)

const (
    testAccessKey = "AKIDEXAMPLE"
    testSecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
    testRegion    = "us-east-1"
    testService   = "s3"
)

// emptySHA256 is the hex SHA-256 of the empty payload, kept local so the
// standalone chain test never depends on package constants.
const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func testCreds() Credentials {
    return Credentials{
        AccessKeyID:     testAccessKey,
        SecretAccessKey: testSecretKey,
        Region:          testRegion,
        Service:         testService,
    }
}

func testTime() time.Time {
    return time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
}

// requestOnWire renders a request the way the http server would present it:
// same method, target, Host and header map.
func requestOnWire(r *http.Request) *http.Request {
    wire := &http.Request{
        Method: r.Method,
        URL:    r.URL,
        Host:   r.Host,
        Header: make(http.Header, len(r.Header)),
        Body:   r.Body,
    }
    for k, vv := range r.Header {
        wire.Header[k] = append([]string(nil), vv...)
    }
    return wire
}

// TestPutBucketRoundTrip is the failing wire case from ticket
// floci-go/ticket-001: a bodyless, content-type-less PUT /bucket signed by
// SignRequest must verify on the wire.
func TestPutBucketRoundTrip(t *testing.T) {
    r, err := http.NewRequest(http.MethodPut, "http://127.0.0.1:9000/mybucket", nil)
    if err != nil {
        t.Fatal(err)
    }
    r.Host = "127.0.0.1:9000"
    if err := SignRequest(r, testCreds(), testTime()); err != nil {
        t.Fatalf("SignRequest: %v", err)
    }
    const wantSigned = "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
    authz := r.Header.Get("Authorization")
    if !strings.Contains(authz, wantSigned) {
        t.Fatalf("unexpected SignedHeaders; want %q within %q", wantSigned, authz)
    }
    if err := Verify(requestOnWire(r), testCreds()); err != nil {
        t.Fatalf("verification of signed PUT /bucket failed on the wire: %v", err)
    }
}

// A Content-Type that carries a value (boto3-style CreateBucket) must be
// signed and must verify; the SignedHeaders list must name content-type.
func TestPutBucketWithContentType(t *testing.T) {
    const cfg = "<CreateBucketConfiguration>" +
        "<LocationConstraint>us-west-2</LocationConstraint>" +
        "</CreateBucketConfiguration>"
    r, err := http.NewRequest(http.MethodPut, "http://s3.example.com/regioned", strings.NewReader(cfg))
    if err != nil {
        t.Fatal(err)
    }
    r.Host = "s3.example.com"
    r.Header.Set("Content-Type", "application/xml")
    if err := SignRequest(r, testCreds(), testTime()); err != nil {
        t.Fatalf("SignRequest: %v", err)
    }
    const wantSigned = "SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date,"
    if !strings.Contains(r.Header.Get("Authorization"), wantSigned) {
        t.Fatalf("Content-Type not signed: %q", r.Header.Get("Authorization"))
    }
    if err := Verify(requestOnWire(r), testCreds()); err != nil {
        t.Fatalf("verification failed: %v", err)
    }
}

// Regression for floci-go/ticket-001: a Content-Type present but empty must
// not enter SignedHeaders, and verification - which used to reconstruct the
// list and disagree - must accept, because it reads SignedHeaders from the
// Authorization header instead of the request header map.
func TestPutBucketEmptyContentType(t *testing.T) {
    r, err := http.NewRequest(http.MethodPut, "http://127.0.0.1:9000/mybucket", nil)
    if err != nil {
        t.Fatal(err)
    }
    r.Host = "127.0.0.1:9000"
    r.Header.Set("Content-Type", "")
    if err := SignRequest(r, testCreds(), testTime()); err != nil {
        t.Fatalf("SignRequest: %v", err)
    }
    const wantSigned = "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
    if !strings.Contains(r.Header.Get("Authorization"), wantSigned) {
        t.Fatalf("empty Content-Type must not be signed: %q", r.Header.Get("Authorization"))
    }
    if err := Verify(requestOnWire(r), testCreds()); err != nil {
        t.Fatalf("verification failed: %v", err)
    }
}

// TestCanonicalRequestWireFormat pins the canonical request bytes to the
// documented SigV4 wire layout that the Floci authority emits: method,
// canonical URI, possibly empty query line, canonical header lines each
// newline terminated, the blank line separating the header block from the
// SignedHeaders line, and the hashed payload last.
func TestCanonicalRequestWireFormat(t *testing.T) {
    r, err := http.NewRequest(http.MethodGet, "https://iam.amazonaws.com/", nil)
    if err != nil {
        t.Fatal(err)
    }
    r.Host = "iam.amazonaws.com"
    r.Header.Set("X-Amz-Date", "20150830T123600Z")
    creq, err := Build(r, emptySHA256, []string{"Host", "X-Amz-Date"})
    if err != nil {
        t.Fatal(err)
    }
    want := "GET\n" +
        "/\n" +
        "\n" +
        "host:iam.amazonaws.com\n" +
        "x-amz-date:20150830T123600Z\n" +
        "\n" +
        "host;x-amz-date\n" +
        emptySHA256
    if creq.Text != want {
        t.Fatalf("canonical request diverges from the wire format:\n got: %q\nwant: %q", creq.Text, want)
    }
}

// TestSignatureIndependentChain recomputes the signature with a standalone
// HMAC-SHA256 chain over a fixed canonical request and compares it with the
// Authorization header SignRequest produced, so drift between the signer
// and the documented key derivation fails loudly.
func TestSignatureIndependentChain(t *testing.T) {
    creds := testCreds()
    creds.Service = "iam"
    r, err := http.NewRequest(http.MethodGet, "https://iam.amazonaws.com/", nil)
    if err != nil {
        t.Fatal(err)
    }
    r.Host = "iam.amazonaws.com"
    if err := SignRequest(r, creds, time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)); err != nil {
        t.Fatalf("SignRequest: %v", err)
    }
    mac := func(key []byte, data string) []byte {
        m := hmac.New(sha256.New, key)
        m.Write([]byte(data))
        return m.Sum(nil)
    }
    sum := sha256.Sum256([]byte("GET\n" +
        "/\n" +
        "\n" +
        "host:iam.amazonaws.com\n" +
        "x-amz-content-sha256:" + emptySHA256 + "\n" +
        "x-amz-date:20150830T123600Z\n" +
        "\n" +
        "host;x-amz-content-sha256;x-amz-date\n" +
        emptySHA256))
    sts := "AWS4-HMAC-SHA256\n" +
        "20150830T123600Z\n" +
        "20150830/us-east-1/iam/aws4_request\n" +
        hex.EncodeToString(sum[:])
    kDate := mac([]byte("AWS4"+testSecretKey), "20150830")
    kRegion := mac(kDate, "us-east-1")
    kService := mac(kRegion, "iam")
    kSigning := mac(kService, "aws4_request")
    want := hex.EncodeToString(mac(kSigning, sts))
    var got string
    for _, field := range strings.Split(strings.TrimPrefix(r.Header.Get("Authorization"), AuthScheme+" "), ",") {
        f := strings.TrimSpace(field)
        if strings.HasPrefix(f, "Signature=") {
            got = strings.TrimPrefix(f, "Signature=")
        }
    }
    if got == "" {
        t.Fatalf("Signature missing in %q", r.Header.Get("Authorization"))
    }
    if got != want {
        t.Fatalf("signature drift: got %s want %s", got, want)
    }
}

// Canonical URI (single encoding, tilde unreserved, empty segments kept)
// and canonical query (re-encoded, valueless params as key=, sorted by name
// then value).
func TestCanonicalQueryAndURIEncoding(t *testing.T) {
    u := &url.URL{Scheme: "https", Host: "s3.example.com", Path: "/buck et/with~key"}
    creq, err := Build(&http.Request{Method: http.MethodGet, URL: u, Header: http.Header{}}, "payload", []string{"host"})
    if err != nil {
        t.Fatal(err)
    }
    if want := "/buck%20et/with~key"; creq.URI != want {
        t.Fatalf("URI: got %q want %q", creq.URI, want)
    }
    q := &url.URL{Scheme: "https", Host: "s3.example.com", Path: "/", RawQuery: "b=2&a=zz&a=0&empty&pct=a%20b"}
    creq2, err := Build(&http.Request{Method: http.MethodGet, URL: q, Header: http.Header{}}, "payload", []string{"host"})
    if err != nil {
        t.Fatal(err)
    }
    if want := "a=0&a=zz&b=2&empty=&pct=a%20b"; creq2.Query != want {
        t.Fatalf("query: got %q want %q", creq2.Query, want)
    }
}

// Verify must reject a wrong secret, an unknown access key, a rewritten
// path and a shifted timestamp.
func TestVerifyRejectsTampering(t *testing.T) {
    mkSigned := func() *http.Request {
        r, err := http.NewRequest(http.MethodPut, "http://127.0.0.1:9000/mybucket", nil)
        if err != nil {
            t.Fatal(err)
        }
        r.Host = "127.0.0.1:9000"
        if err := SignRequest(r, testCreds(), testTime()); err != nil {
            t.Fatal(err)
        }
        return r
    }
    badSecret := testCreds()
    badSecret.SecretAccessKey += "x"
    if err := Verify(requestOnWire(mkSigned()), badSecret); err == nil {
        t.Fatal("expected signature mismatch for wrong secret key")
    }
    badKey := testCreds()
    badKey.AccessKeyID = "AKIDOTHER"
    if err := Verify(requestOnWire(mkSigned()), badKey); err == nil {
        t.Fatal("expected unknown access key error")
    }
    tamperedPath := requestOnWire(mkSigned())
    tamperedPath.URL.Path = "/otherbucket"
    if err := Verify(tamperedPath, testCreds()); err == nil {
        t.Fatal("expected signature mismatch for tampered path")
    }
    tamperedDate := requestOnWire(mkSigned())
    tamperedDate.Header.Set("X-Amz-Date", "20250601T120001Z")
    if err := Verify(tamperedDate, testCreds()); err == nil {
        t.Fatal("expected signature mismatch for tampered timestamp")
    }
}

// A request without an Authorization header is unsigned.
func TestVerifyRequiresAuthorization(t *testing.T) {
    r, err := http.NewRequest(http.MethodPut, "http://127.0.0.1:9000/mybucket", nil)
    if err != nil {
        t.Fatal(err)
    }
    r.Host = "127.0.0.1:9000"
    if err := Verify(requestOnWire(r), testCreds()); err != ErrMissingAuth {
        t.Fatalf("expected ErrMissingAuth, got %v", err)
    }
}

// Signing must not consume the body: the signer buffers and replays it.
func TestSignedRequestStillHasBody(t *testing.T) {
    const body = "<CreateBucketConfiguration/>"
    r, err := http.NewRequest(http.MethodPut, "http://s3.example.com/bucket", strings.NewReader(body))
    if err != nil {
        t.Fatal(err)
    }
    if err := SignRequest(r, testCreds(), testTime()); err != nil {
        t.Fatalf("SignRequest: %v", err)
    }
    data, rerr := io.ReadAll(r.Body)
    if rerr != nil {
        t.Fatalf("reading replayed body: %v", rerr)
    }
    if string(data) != body {
        t.Fatalf("body was consumed by signing: got %q want %q", string(data), body)
    }
}

// A preset UNSIGNED-PAYLOAD X-Amz-Content-Sha256 passes through the payload
// line verbatim when signing and verifying, and the body is never hashed.
func TestUnsignedPayloadPassthrough(t *testing.T) {
    r, err := http.NewRequest(http.MethodPut, "http://s3.example.com/big", nil)
    if err != nil {
        t.Fatal(err)
    }
    r.Host = "s3.example.com"
    r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
    if err := SignRequest(r, testCreds(), testTime()); err != nil {
        t.Fatalf("SignRequest: %v", err)
    }
    const wantSigned = "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
    if !strings.Contains(r.Header.Get("Authorization"), wantSigned) {
        t.Fatalf("unexpected SignedHeaders: %q", r.Header.Get("Authorization"))
    }
    if err := Verify(requestOnWire(r), testCreds()); err != nil {
        t.Fatalf("verify failed: %v", err)
    }
}
