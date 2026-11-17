package s3

// AWS Signature V4 support: header auth and presigned query auth, exposed
// as hooks the server (and the kernel) can compose.

import (
    "bytes"
    "crypto/hmac"
    "crypto/sha256"
    "encoding/hex"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "sort"
    "strconv"
    "strings"
    "time"
)

const sigV4Algorithm = "AWS4-HMAC-SHA256"

const (
    maxPresignTTL = 7 * 24 * time.Hour
    defaultSkew   = 15 * time.Minute
)

// Authenticator is the pre-routing authentication hook. Returning an *Error
// rejects the request with that S3 error document.
type Authenticator interface {
    Authenticate(r *http.Request) error
}

// AuthenticatorFunc adapts plain functions to Authenticator.
type AuthenticatorFunc func(*http.Request) error

func (f AuthenticatorFunc) Authenticate(r *http.Request) error { return f(r) }

// Credentials resolves secret keys for access key IDs. Kernels plug their
// identity store in here.
type Credentials interface {
    Lookup(accessKeyID string) (secretKey string, ok bool)
}

// StaticCredentials serves one fixed identity.
type StaticCredentials struct {
    AccessKeyID     string
    SecretAccessKey string
}

func (c StaticCredentials) Lookup(id string) (string, bool) {
    if id == c.AccessKeyID {
        return c.SecretAccessKey, true
    }
    return "", false
}

// MapCredentials serves identities from a fixed map.
type MapCredentials map[string]string

func (m MapCredentials) Lookup(id string) (string, bool) {
    secret, ok := m[id]
    return secret, ok
}

// V4Authenticator verifies AWS Signature V4: Authorization header signing
// and presigned URLs (X-Amz-Signature query auth).
type V4Authenticator struct {
    // Region, when non-empty, must match the credential scope region ("")
    // accepts any region.
    Region string
    // Service restricts the credential scope service; default "s3".
    Service string
    // Creds is required.
    Creds Credentials
    // Skew bounds clock drift for header auth; default 15m.
    Skew time.Duration
}

// NewV4Authenticator binds credentials and a fixed region check.
func NewV4Authenticator(creds Credentials, region string) *V4Authenticator {
    return &V4Authenticator{Creds: creds, Region: region, Service: "s3"}
}

// Authenticate implements Authenticator, dispatching on request style.
func (a *V4Authenticator) Authenticate(r *http.Request) error {
    if r.URL.Query().Has("X-Amz-Algorithm") {
        return a.verifyQueryAuth(r)
    }
    if r.Header.Get("Authorization") != "" {
        return a.verifyHeaderAuth(r)
    }
    return nil // anonymous: policy enforcement belongs to the caller
}

func (a *V4Authenticator) service() string {
    if a.Service == "" {
        return "s3"
    }
    return a.Service
}

func (a *V4Authenticator) region() string {
    if a.Region == "" {
        return "us-east-1"
    }
    return a.Region
}

func (a *V4Authenticator) skew() time.Duration {
    if a.Skew == 0 {
        return defaultSkew
    }
    return a.Skew
}

// verifyQueryAuth checks presigned (query string) authentication.
func (a *V4Authenticator) verifyQueryAuth(r *http.Request) error {
    q := r.URL.Query()
    bad := func(msg string) *Error {
        return errf("AuthorizationQueryParametersError", http.StatusBadRequest, "%s", msg)
    }
    if q.Get("X-Amz-Algorithm") != sigV4Algorithm {
        return bad(`Unsupported algorithm; only "` + sigV4Algorithm + `" is supported`)
    }
    for _, name := range []string{"X-Amz-Credential", "X-Amz-Signature", "X-Amz-Date", "X-Amz-SignedHeaders", "X-Amz-Expires"} {
        if q.Get(name) == "" {
            return bad(`Query-string authentication requires the X-Amz-Algorithm, X-Amz-Credential, X-Amz-Signature, X-Amz-Date, X-Amz-SignedHeaders, and X-Amz-Expires parameters.`)
        }
    }
    access, date, region, service, ok := parseCredentialScope(q.Get("X-Amz-Credential"))
    if !ok {
        return bad(`Error parsing the X-Amz-Credential parameter; the Credential is mal-formed; expecting "AKIDEXAMPLE/yyyymmdd/region/service/aws4_request".`)
    }
    if service != a.service() {
        return bad(fmt.Sprintf(`Error parsing the X-Amz-Credential parameter; incorrect service "%s". This endpoint belongs to "%s".`, service, a.service()))
    }
    if a.Region != "" && region != a.Region {
        return bad(fmt.Sprintf("Error parsing the X-Amz-Credential parameter; the region '%s' is wrong; expecting '%s'", region, a.Region))
    }
    secret, found := a.Creds.Lookup(access)
    if !found {
        return errf("InvalidAccessKeyId", http.StatusForbidden, "The AWS Access Key Id you provided does not exist in our records.")
    }
    expires, perr := strconv.ParseInt(q.Get("X-Amz-Expires"), 10, 64)
    if perr != nil || expires < 1 {
        return bad("X-Amz-Expires must be a number of seconds")
    }
    if expires > int64(maxPresignTTL/time.Second) {
        return bad("X-Amz-Expires must be less than a week (in seconds); that is, the given X-Amz-Expires must be less than 604800 seconds")
    }
    requestTime, terr := time.Parse("20060102T150405Z", q.Get("X-Amz-Date"))
    if terr != nil {
        return bad(`X-Amz-Date must be in the ISO8601 long format "yyyyMMddThhmmssZ".`)
    }
    if time.Now().After(requestTime.Add(time.Duration(expires) * time.Second)) {
        return errf("AccessDenied", http.StatusForbidden, "Request has expired")
    }
    expected := a.presignSignature(r, secret, date, region, service, q.Get("X-Amz-SignedHeaders"))
    provided := strings.ToLower(q.Get("X-Amz-Signature"))
    if !hmac.Equal([]byte(expected), []byte(provided)) {
        return errSignatureMismatch
    }
    return nil
}

// verifyHeaderAuth checks Authorization-header SigV4 requests.
func (a *V4Authenticator) verifyHeaderAuth(r *http.Request) error {
    auth := strings.TrimSpace(r.Header.Get("Authorization"))
    if !strings.HasPrefix(auth, sigV4Algorithm) {
        return errf("InvalidArgument", http.StatusBadRequest, "Unsupported Authorization Type")
    }
    rest := strings.TrimSpace(strings.TrimPrefix(auth, sigV4Algorithm))
    fields := map[string]string{}
    for _, part := range strings.Split(rest, ",") {
        k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
        if !ok {
            continue
        }
        fields[strings.TrimSpace(k)] = strings.TrimSpace(v)
    }
    credential := fields["Credential"]
    signedNames := fields["SignedHeaders"]
    provided := fields["Signature"]
    if credential == "" || signedNames == "" || provided == "" {
        return errf("InvalidArgument", http.StatusBadRequest, "Authorization header requires the Credential, SignedHeaders and Signature elements.")
    }
    access, date, region, service, ok := parseCredentialScope(credential)
    if !ok {
        return errf("AuthorizationHeaderMalformed", http.StatusBadRequest, `The authorization header credential is malformed; expecting "AKIDEXAMPLE/yyyymmdd/region/service/aws4_request".`)
    }
    if service != a.service() {
        return errf("AuthorizationHeaderMalformed", http.StatusBadRequest, fmt.Sprintf(`The authorization header is malformed; incorrect service "%s"; expecting %q.`, service, a.service()))
    }
    if a.Region != "" && region != a.Region {
        return errf("AuthorizationHeaderMalformed", http.StatusBadRequest, fmt.Sprintf("The authorization header is malformed; the region '%s' is wrong; expecting '%s'", region, a.Region))
    }
    secret, found := a.Creds.Lookup(access)
    if !found {
        return errf("InvalidAccessKeyId", http.StatusForbidden, "The AWS Access Key Id you provided does not exist in our records.")
    }
    amzDate := r.Header.Get("x-amz-date")
    if amzDate == "" {
        return errf("InvalidArgument", http.StatusBadRequest, "Missing required header for this request: x-amz-date")
    }
    requestTime, terr := time.Parse("20060102T150405Z", amzDate)
    if terr != nil {
        return errf("AccessDenied", http.StatusForbidden, "AWS authentication requires a valid Date or x-amz-date header")
    }
    now := time.Now()
    if now.Sub(requestTime) > a.skew() || requestTime.Sub(now) > a.skew() {
        return errf("RequestTimeTooSkewed", http.StatusForbidden, "The difference between the request time and the current time is too large.")
    }
    payload := strings.ToLower(r.Header.Get("x-amz-content-sha256"))
    if payload == "" {
        payload = "UNSIGNED-PAYLOAD"
    }
    switch {
    case payload == "UNSIGNED-PAYLOAD" || strings.HasPrefix(payload, "STREAMING"):
        // Nothing to verify.
    default:
        if digest, derr := hex.DecodeString(payload); derr == nil && len(digest) == sha256.Size {
            body, rerr := io.ReadAll(r.Body)
            if rerr != nil {
                return errf("IncompleteBody", http.StatusBadRequest, "Unable to read request body")
            }
            r.Body = io.NopCloser(bytes.NewReader(body))
            sum := sha256.Sum256(body)
            if hex.EncodeToString(sum[:]) != payload {
                return errf("XAmzContentSHA256Mismatch", http.StatusBadRequest, "The provided 'x-amz-content-sha256' header does not match what was computed.")
            }
        }
    }
    headers, names := canonicalHeaders(r, signedNames)
    canonical := strings.Join([]string{
        r.Method,
        r.URL.EscapedPath(),
        canonicalQuery(r.URL.RawQuery, false),
        headers,
        names,
        payload,
    }, "\n")
    expected := a.sign(amzDate, secret, date, region, service, canonical)
    if !hmac.Equal([]byte(expected), []byte(strings.ToLower(provided))) {
        return errSignatureMismatch
    }
    return nil
}

func parseCredentialScope(credential string) (access, date, region, service string, ok bool) {
    parts := strings.Split(credential, "/")
    if len(parts) != 5 || parts[4] != "aws4_request" {
        return "", "", "", "", false
    }
    return parts[0], parts[1], parts[2], parts[3], true
}

// canonicalHeaders renders the canonical header block for signedNames
// (lowercase, semicolon-separated, sorted by the signer).
func canonicalHeaders(r *http.Request, signedNames string) (block, names string) {
    names = strings.ToLower(signedNames)
    var b strings.Builder
    for _, name := range strings.Split(names, ";") {
        b.WriteString(name)
        b.WriteByte(':')
        b.WriteString(canonicalHeaderValue(r, name))
        b.WriteByte('\n')
    }
    return b.String(), names
}

func canonicalHeaderValue(r *http.Request, name string) string {
    if name == "host" {
        return collapseSpaces(r.Host)
    }
    values := r.Header.Values(name)
    parts := make([]string, 0, len(values))
    for _, v := range values {
        parts = append(parts, collapseSpaces(strings.TrimSpace(v)))
    }
    return strings.Join(parts, ",")
}

func collapseSpaces(s string) string {
    return strings.Join(strings.Fields(s), " ")
}

// canonicalQuery builds the canonical query string: RFC 3986 encoded pairs
// sorted by key then value. In presign mode X-Amz-Signature is excluded.
func canonicalQuery(rawQuery string, presign bool) string {
    values, err := url.ParseQuery(rawQuery)
    if err != nil {
        return rawQuery
    }
    type kv struct{ key, value string }
    var pairs []kv
    for key, vals := range values {
        for _, val := range vals {
            if presign && key == "X-Amz-Signature" {
                continue
            }
            pairs = append(pairs, kv{key, val})
        }
    }
    sort.Slice(pairs, func(i, j int) bool {
        if pairs[i].key != pairs[j].key {
            return pairs[i].key < pairs[j].key
        }
        return pairs[i].value < pairs[j].value
    })
    var b strings.Builder
    for i, p := range pairs {
        if i > 0 {
            b.WriteByte('&')
        }
        b.WriteString(awsURLEncode(p.key))
        b.WriteByte('=')
        b.WriteString(awsURLEncode(p.value))
    }
    return b.String()
}

// awsURLEncode applies AWS canonicalization: RFC 3986 unreserved characters
// stay literal, everything else (space included) is percent-encoded.
func awsURLEncode(s string) string {
    var b strings.Builder
    for i := 0; i < len(s); i++ {
        c := s[i]
        switch {
        case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
            c == '-', c == '_', c == '.', c == '~':
            b.WriteByte(c)
        default:
            fmt.Fprintf(&b, "%%%02X", c)
        }
    }
    return b.String()
}

// signingKey derives the V4 signing key via the standard HMAC chain.
func signingKey(secret, date, region, service string) []byte {
    kDate := hmacSHA256([]byte("AWS4"+secret), date)
    kRegion := hmacSHA256(kDate, region)
    kService := hmacSHA256(kRegion, service)
    return hmacSHA256(kService, "aws4_request")
}

func hmacSHA256(key []byte, data string) []byte {
    mac := hmac.New(sha256.New, key)
    mac.Write([]byte(data))
    return mac.Sum(nil)
}

// presignSignature recomputes a presigned request signature; presigned
// payloads are always UNSIGNED-PAYLOAD.
func (a *V4Authenticator) presignSignature(r *http.Request, secret, date, region, service, signedNames string) string {
    amzDate := r.URL.Query().Get("X-Amz-Date")
    headers, names := canonicalHeaders(r, signedNames)
    canonical := strings.Join([]string{
        r.Method,
        r.URL.EscapedPath(),
        canonicalQuery(r.URL.RawQuery, true),
        headers,
        names,
        "UNSIGNED-PAYLOAD",
    }, "\n")
    return a.sign(amzDate, secret, date, region, service, canonical)
}

// sign derives the signature from a canonical request and credential scope.
func (a *V4Authenticator) sign(amzDate, secret, date, region, service, canonicalRequest string) string {
    sum := sha256.Sum256([]byte(canonicalRequest))
    scope := strings.Join([]string{date, region, service, "aws4_request"}, "/")
    stringToSign := strings.Join([]string{
        sigV4Algorithm,
        amzDate,
        scope,
        hex.EncodeToString(sum[:]),
    }, "\n")
    mac := hmac.New(sha256.New, signingKey(secret, date, region, service))
    mac.Write([]byte(stringToSign))
    return hex.EncodeToString(mac.Sum(nil))
}

// PresignURL produces a presigned URL for target using accessKeyID. It is
// the mirror of presign verification, handy for tests and kernel-driven
// clients.
func (a *V4Authenticator) PresignURL(method, target, accessKeyID string, ttl time.Duration) (*url.URL, error) {
    u, err := url.Parse(target)
    if err != nil {
        return nil, err
    }
    secret, ok := a.Creds.Lookup(accessKeyID)
    if !ok {
        return nil, errf("InvalidAccessKeyId", http.StatusForbidden, "The AWS Access Key Id you provided does not exist in our records.")
    }
    if ttl <= 0 || ttl > maxPresignTTL {
        return nil, errf("InvalidArgument", http.StatusBadRequest, "X-Amz-Expires must be between 1 second and 7 days")
    }
    now := time.Now().UTC()
    amzDate := now.Format("20060102T150405Z")
    credDate := now.Format("20060102")
    q := u.Query()
    q.Set("X-Amz-Algorithm", sigV4Algorithm)
    q.Set("X-Amz-Credential", strings.Join([]string{accessKeyID, credDate, a.region(), a.service(), "aws4_request"}, "/"))
    q.Set("X-Amz-Date", amzDate)
    q.Set("X-Amz-Expires", strconv.FormatInt(int64(ttl/time.Second), 10))
    q.Set("X-Amz-SignedHeaders", "host")
    u.RawQuery = q.Encode()
    request := &http.Request{Method: method, URL: u, Host: u.Host}
    signature := a.presignSignature(request, secret, credDate, a.region(), a.service(), "host")
    q.Set("X-Amz-Signature", signature)
    u.RawQuery = q.Encode()
    return u, nil
}
