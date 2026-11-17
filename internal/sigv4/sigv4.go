// Package sigv4 implements AWS Signature Version 4 request signing and
// verification for floci-go's S3-compatible wire protocol.
//
// Fix for ticket floci-go/ticket-001 (PUT /bucket round-trip signature
// divergence): signing and verification now share a single canonical-request
// builder (Build). The SignedHeaders list is produced exactly once per
// direction:
//
//   - signing: DefaultSignedHeaderNames picks host, a non-empty
//     Content-Type, and every non-empty x-amz-* header.
//   - verification: the list is decoded from the Authorization header and is
//     never reconstructed from the request's header map.
//
// Previously the verify path rebuilt the list from the parsed request and
// used different Content-Type inclusion rules than the canonicalize path:
// the canonical header lines happened to match between the two paths while
// the SignedHeaders line did not, so the canonical requests diverged on
// exactly one line and every content-type-less request (e.g. PUT /bucket)
// failed to verify. Sharing one builder makes that bug class impossible.
package sigv4

import (
    "bytes"
    "crypto/hmac"
    "crypto/sha256"
    "encoding/hex"
    "errors"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "sort"
    "strconv"
    "strings"
    "sync"
    "time"
)

const (
    // AuthScheme is the SigV4 algorithm identifier used in the
    // Authorization header and the string-to-sign.
    AuthScheme = "AWS4-HMAC-SHA256"

    amzTimeFormat = "20060102T150405Z"
    amzDateFormat = "20060102"
    scopeSuffix   = "aws4_request"

    // unsignedDigest is a valid literal X-Amz-Content-Sha256 value clients
    // send when they do not hash the payload; it (and any STREAMING-*)
    // value passes through the hashed-payload line verbatim.
    unsignedDigest = "UNSIGNED-PAYLOAD"

    defaultRegion  = "us-east-1"
    defaultService = "s3"
)

// sha256OfEmpty is the lowercase hex SHA-256 of an empty payload: the
// hashed-payload line of bodyless requests such as CreateBucket
// (PUT /bucket).
const sha256OfEmpty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// Errors returned by Verify.
var (
    ErrMissingAuth         = errors.New("sigv4: missing or malformed Authorization header")
    ErrMalformedCredential = errors.New("sigv4: malformed Credential element in Authorization header")
    ErrUnknownAccessKey    = errors.New("sigv4: unknown access key id")
    ErrMalformedDate       = errors.New("sigv4: malformed or missing request timestamp")
    ErrSignatureMismatch   = errors.New("sigv4: signature mismatch")
)

// Credentials identify the signing identity. Region and Service are
// optional during verification (the credential scope in the Authorization
// header is authoritative); SignRequest defaults them to us-east-1 and s3.
type Credentials struct {
    AccessKeyID     string
    SecretAccessKey string
    Region          string
    Service         string
}

// CanonicalRequest is a SigV4 canonical request plus the fields it was
// composed from. Text is the exact byte string hashed into the
// string-to-sign; HeadersTxt is the canonical header block with every line
// newline terminated, and Signed the semicolon-joined SignedHeaders list.
type CanonicalRequest struct {
    Method     string
    URI        string
    Query      string
    HeadersTxt string
    Signed     string
    Payload    string
    Text       string
}

// cut has strings.Cut semantics, kept self-contained for older toolchains.
func cut(s, sep string) (string, string, bool) {
    if i := strings.Index(s, sep); i >= 0 {
        return s[:i], s[i+len(sep):], true
    }
    return s, "", false
}

// awsURIEncode URI-encodes s with the SigV4 rules: A-Z a-z 0-9 hyphen, dot,
// underscore and tilde survive unescaped, a space never becomes +, and every
// other byte is emitted as %XX with uppercase hex. The path separator slash
// survives when encodeSlash is false.
func awsURIEncode(s string, encodeSlash bool) string {
    const hexUpper = "0123456789ABCDEF"
    var b strings.Builder
    b.Grow(len(s))
    for i := 0; i < len(s); i++ {
        c := s[i]
        switch {
        case 'A' <= c && c <= 'Z', 'a' <= c && c <= 'z', '0' <= c && c <= '9',
            c == '-', c == '.', c == '_', c == '~':
            b.WriteByte(c)
        case c == '/':
            if encodeSlash {
                b.WriteString("%2F")
            } else {
                b.WriteByte('/')
            }
        default:
            b.WriteByte('%')
            b.WriteByte(hexUpper[c>>4])
            b.WriteByte(hexUpper[c&0xF])
        }
    }
    return b.String()
}

// canonicalURI returns the canonical URI: the decoded request path with
// every segment re-encoded exactly once (SigV4/S3 single encoding) while
// empty segments are preserved, so S3 keys containing double slashes keep
// their shape.
func canonicalURI(u *url.URL) string {
    if u == nil {
        return "/"
    }
    p := u.Path
    if p == "" {
        p = "/"
    }
    segs := strings.Split(p, "/")
    for i, seg := range segs {
        segs[i] = awsURIEncode(seg, false)
    }
    return strings.Join(segs, "/")
}

// canonicalQuery rebuilds the query string for the canonical request: keys
// and values are decoded once and re-encoded with the SigV4 rules,
// parameters without a value become key=, and pairs are sorted by encoded
// name, then value. A literal + in the raw query stays a plus sign
// (re-encoded %2B), matching how the signing SDKs spell it.
func canonicalQuery(rawQuery string) string {
    if rawQuery == "" {
        return ""
    }
    type pair struct{ k, v string }
    pairs := make([]pair, 0, 8)
    for _, part := range strings.Split(rawQuery, "&") {
        if part == "" {
            continue
        }
        k, v, _ := cut(part, "=")
        if dk, err := url.PathUnescape(k); err == nil {
            k = dk
        }
        if dv, err := url.PathUnescape(v); err == nil {
            v = dv
        }
        pairs = append(pairs, pair{awsURIEncode(k, true), awsURIEncode(v, true)})
    }
    sort.Slice(pairs, func(i, j int) bool {
        if pairs[i].k != pairs[j].k {
            return pairs[i].k < pairs[j].k
        }
        return pairs[i].v < pairs[j].v
    })
    var b strings.Builder
    for i, p := range pairs {
        if i > 0 {
            b.WriteByte('&')
        }
        b.WriteString(p.k)
        b.WriteByte('=')
        b.WriteString(p.v)
    }
    return b.String()
}

// trimHeaderValue applies the SigV4 trimall rule to a header value: strip
// leading and trailing spaces and collapse interior space runs into one.
func trimHeaderValue(v string) string {
    v = strings.Trim(v, " ")
    for strings.Contains(v, "  ") {
        v = strings.ReplaceAll(v, "  ", " ")
    }
    return v
}

// normalizeHeaderNames lower-cases, trims, sorts and de-duplicates a
// SignedHeaders list. Sorting is a no-op for the sorted lists real clients
// send and keeps the reconstructed block consistent otherwise.
func normalizeHeaderNames(names []string) []string {
    seen := make(map[string]struct{}, len(names))
    out := make([]string, 0, len(names))
    for _, n := range names {
        n = strings.ToLower(strings.TrimSpace(n))
        if n == "" {
            continue
        }
        if _, dup := seen[n]; dup {
            continue
        }
        seen[n] = struct{}{}
        out = append(out, n)
    }
    sort.Strings(out)
    return out
}

// hostValue is the effective Host header of the request, exactly as it
// appears on the wire, including a non-default port.
func hostValue(r *http.Request) string {
    if r.Host != "" {
        return r.Host
    }
    if r.URL != nil {
        return r.URL.Host
    }
    return ""
}

// canonicalHeaderLines renders the name:value canonical header line for
// each signed header. It is the single source of truth consumed by both the
// signing and the verification path, so the header block, the SignedHeaders
// list and their values can never disagree again (ticket
// floci-go/ticket-001).
func canonicalHeaderLines(r *http.Request, names []string) ([]string, error) {
    lines := make([]string, 0, len(names))
    for _, name := range names {
        var vals []string
        if name == "host" {
            vals = []string{hostValue(r)}
        } else {
            vals = r.Header[http.CanonicalHeaderKey(name)]
            if len(vals) == 0 && name == "content-length" && r.ContentLength >= 0 {
                // Content-Length is usually carried in the request struct
                // by signing clients rather than in the header map.
                vals = []string{strconv.FormatInt(r.ContentLength, 10)}
            }
            if len(vals) == 0 {
                return nil, fmt.Errorf("sigv4: signed header %q is missing from the request", name)
            }
            trimmed := make([]string, 0, len(vals))
            for _, v := range vals {
                trimmed = append(trimmed, trimHeaderValue(v))
            }
            vals = trimmed
        }
        lines = append(lines, name+":"+strings.Join(vals, ","))
    }
    return lines, nil
}

// DefaultSignedHeaderNames is the header set signed on outgoing requests:
// host, content-type when it carries a value, and every non-empty x-amz-*
// header, sorted. A Content-Type that is present but empty is not signed -
// treated exactly like an absent Content-Type - so content-type-less verbs
// such as CreateBucket (PUT /bucket) sign host;x-amz-content-sha256;x-amz-date.
func DefaultSignedHeaderNames(r *http.Request) []string {
    names := []string{"host"}
    for key, vals := range r.Header {
        if len(vals) == 0 {
            continue
        }
        name := strings.ToLower(key)
        hasValue := false
        for _, v := range vals {
            if strings.TrimSpace(v) != "" {
                hasValue = true
                break
            }
        }
        if !hasValue {
            continue
        }
        if name == "content-type" || strings.HasPrefix(name, "x-amz-") {
            names = append(names, name)
        }
    }
    sort.Strings(names)
    return names
}

// readAndReplaceBody drains and closes the request body, then installs a
// replayable copy, so hashing a payload does not consume the body for the
// sender or the eventual handler.
func readAndReplaceBody(r *http.Request) ([]byte, error) {
    if r.Body == nil {
        r.Body = http.NoBody
    }
    body, err := io.ReadAll(r.Body)
    closeErr := r.Body.Close()
    r.Body = io.NopCloser(bytes.NewReader(body))
    r.GetBody = func() (io.ReadCloser, error) {
        return io.NopCloser(bytes.NewReader(body)), nil
    }
    if err != nil {
        return nil, err
    }
    if closeErr != nil {
        return nil, closeErr
    }
    return body, nil
}

// signedPayloadHash resolves the hashed-payload value for signing. The
// X-Amz-Content-Sha256 header decides verbatim when it is already set
// (UNSIGNED-PAYLOAD and STREAMING-* values pass through); otherwise the
// body is hashed, buffered back onto the request, and the header is set so
// it joins the signed header set like the S3 SDK clients do.
func signedPayloadHash(r *http.Request) (string, error) {
    if v := strings.TrimSpace(r.Header.Get("X-Amz-Content-Sha256")); v != "" {
        return v, nil
    }
    body, err := readAndReplaceBody(r)
    if err != nil {
        return "", err
    }
    payload := sha256OfEmpty
    if len(body) != 0 {
        sum := sha256.Sum256(body)
        payload = hex.EncodeToString(sum[:])
    }
    r.Header.Set("X-Amz-Content-Sha256", payload)
    if r.ContentLength == 0 && len(body) > 0 {
        r.ContentLength = int64(len(body))
    }
    return payload, nil
}

// verifyPayloadHash resolves the hashed-payload line during verification. A
// declared X-Amz-Content-Sha256 header wins; otherwise the body is hashed
// (and buffered back, so handlers still see the body).
func verifyPayloadHash(r *http.Request) (string, error) {
    if v := strings.TrimSpace(r.Header.Get("X-Amz-Content-Sha256")); v != "" {
        return v, nil
    }
    body, err := readAndReplaceBody(r)
    if err != nil {
        return "", err
    }
    payload := sha256OfEmpty
    if len(body) != 0 {
        sum := sha256.Sum256(body)
        payload = hex.EncodeToString(sum[:])
    }
    return payload, nil
}

// validatePayloadDigest accepts the hash forms S3 clients may declare.
func validatePayloadDigest(v string) error {
    if v == unsignedDigest || strings.HasPrefix(v, "STREAMING-") {
        return nil
    }
    if _, err := hex.DecodeString(v); err != nil || len(v) != sha256.Size*2 {
        return fmt.Errorf("sigv4: malformed X-Amz-Content-Sha256 %q", v)
    }
    return nil
}

// Build forms the SigV4 canonical request. signedHeaderNames is
// authoritative and is never second-guessed: callers pass
// DefaultSignedHeaderNames(r) when signing, and the SignedHeaders list
// decoded from the Authorization header when verifying. Canonical header
// lines and the SignedHeaders list are produced together from the same
// normalized names, which is what makes the sign and verify paths
// byte-identical (ticket floci-go/ticket-001).
func Build(r *http.Request, payloadHash string, signedHeaderNames []string) (*CanonicalRequest, error) {
    if r == nil {
        return nil, errors.New("sigv4: nil request")
    }
    if r.URL == nil {
        return nil, errors.New("sigv4: request without URL")
    }
    names := normalizeHeaderNames(signedHeaderNames)
    if len(names) == 0 {
        return nil, errors.New("sigv4: empty SignedHeaders list")
    }
    lines, err := canonicalHeaderLines(r, names)
    if err != nil {
        return nil, err
    }
    // Every canonical header line is newline terminated; joined onto the
    // rest of the canonical request this yields the blank line that
    // separates the header block from the SignedHeaders line on the wire.
    headersBlock := strings.Join(lines, "\n") + "\n"
    c := &CanonicalRequest{
        Method:     r.Method,
        URI:        canonicalURI(r.URL),
        Query:      canonicalQuery(r.URL.RawQuery),
        HeadersTxt: headersBlock,
        Signed:     strings.Join(names, ";"),
        Payload:    payloadHash,
    }
    c.Text = strings.Join([]string{
        c.Method, c.URI, c.Query, headersBlock, c.Signed, c.Payload,
    }, "\n")
    return c, nil
}

func hmacSHA256(key []byte, s string) []byte {
    m := hmac.New(sha256.New, key)
    m.Write([]byte(s))
    return m.Sum(nil)
}

type signingKeyID struct {
    secret  string
    date    string
    region  string
    service string
}

var signingKeyCache sync.Map

// signingKey derives the SigV4 signing key
// HMAC(HMAC(HMAC(HMAC("AWS4"+secret, date), region), service),
// "aws4_request"), memoized per (secret, date, region, service).
func signingKey(secret, date, region, service string) []byte {
    id := signingKeyID{secret, date, region, service}
    if v, ok := signingKeyCache.Load(id); ok {
        return v.([]byte)
    }
    kSigning := hmacSHA256(
        hmacSHA256(
            hmacSHA256(
                hmacSHA256([]byte("AWS4"+secret), date),
                region),
            service),
        scopeSuffix)
    signingKeyCache.Store(id, kSigning)
    return kSigning
}

// stringToSign assembles the SigV4 string-to-sign: the algorithm
// identifier, the request timestamp, the credential scope and the hex
// SHA-256 of the canonical request, newline separated.
func stringToSign(amzDate, scope, canonicalText string) string {
    sum := sha256.Sum256([]byte(canonicalText))
    return strings.Join([]string{
        AuthScheme,
        amzDate,
        scope,
        hex.EncodeToString(sum[:]),
    }, "\n")
}

type authHeader struct {
    scheme       string
    credential   string
    signedNames  []string
    signatureHex string
}

// parseAuthorization decodes an AWS4-HMAC-SHA256 Authorization header with
// its Credential, SignedHeaders and Signature elements.
func parseAuthorization(raw string) (*authHeader, error) {
    scheme, rest, ok := cut(strings.TrimSpace(raw), " ")
    if !ok {
        return nil, ErrMissingAuth
    }
    h := &authHeader{scheme: scheme, signedNames: []string{}}
    for _, field := range strings.Split(rest, ",") {
        field = strings.TrimSpace(field)
        if field == "" {
            continue
        }
        name, value, ok := cut(field, "=")
        if !ok {
            return nil, ErrMissingAuth
        }
        switch {
        case strings.EqualFold(strings.TrimSpace(name), "Credential"):
            h.credential = value
        case strings.EqualFold(strings.TrimSpace(name), "SignedHeaders"):
            h.signedNames = strings.Split(value, ";")
        case strings.EqualFold(strings.TrimSpace(name), "Signature"):
            h.signatureHex = strings.TrimSpace(value)
        }
    }
    if h.credential == "" || len(h.signedNames) == 0 || h.signatureHex == "" {
        return nil, ErrMissingAuth
    }
    return h, nil
}

// SignRequest signs a request in place with header-based S3 authentication:
// it sets X-Amz-Date, X-Amz-Content-Sha256 and Authorization on r. Region
// and Service default to us-east-1 and s3 when empty in the credentials.
func SignRequest(r *http.Request, creds Credentials, now time.Time) error {
    region, service := creds.Region, creds.Service
    if region == "" {
        region = defaultRegion
    }
    if service == "" {
        service = defaultService
    }
    amzDate := strings.TrimSpace(r.Header.Get("X-Amz-Date"))
    t, perr := time.Parse(amzTimeFormat, amzDate)
    if perr != nil {
        t = now.UTC()
        amzDate = t.Format(amzTimeFormat)
    }
    r.Header.Set("X-Amz-Date", amzDate)
    scopeDate := t.UTC().Format(amzDateFormat)
    payloadHash, err := signedPayloadHash(r)
    if err != nil {
        return err
    }
    creq, err := Build(r, payloadHash, DefaultSignedHeaderNames(r))
    if err != nil {
        return err
    }
    scope := strings.Join([]string{scopeDate, region, service, scopeSuffix}, "/")
    sts := stringToSign(amzDate, scope, creq.Text)
    key := signingKey(creds.SecretAccessKey, scopeDate, region, service)
    r.Header.Set("Authorization", fmt.Sprintf(
        "%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
        AuthScheme, creds.AccessKeyID, scope, creq.Signed,
        hex.EncodeToString(hmacSHA256(key, sts))))
    return nil
}

// Verify validates the header-based SigV4 Authorization header of an
// incoming request. The SignedHeaders list is taken verbatim from the
// Authorization header - never reconstructed from the parsed request - and
// the canonical request is rebuilt by the same Build the signing path uses,
// so any signature produced for the wire (by SignRequest or by any SigV4
// client, e.g. the Floci authority) verifies unchanged. On success the body
// has been left replayable for the handler.
func Verify(r *http.Request, creds Credentials) error {
    h, err := parseAuthorization(r.Header.Get("Authorization"))
    if err != nil {
        return err
    }
    if h.scheme != AuthScheme {
        return fmt.Errorf("sigv4: unsupported signing algorithm %q", h.scheme)
    }
    parts := strings.Split(h.credential, "/")
    if len(parts) != 5 || parts[4] != scopeSuffix {
        return ErrMalformedCredential
    }
    accessKey, scopeDate, scopeRegion, scopeService := parts[0], parts[1], parts[2], parts[3]
    if creds.AccessKeyID != "" && creds.AccessKeyID != accessKey {
        return ErrUnknownAccessKey
    }
    if creds.Region != "" && creds.Region != scopeRegion {
        return fmt.Errorf("sigv4: credential scope region %q does not match %q", scopeRegion, creds.Region)
    }
    if creds.Service != "" && creds.Service != scopeService {
        return fmt.Errorf("sigv4: credential scope service %q does not match %q", scopeService, creds.Service)
    }
    amzDate := strings.TrimSpace(r.Header.Get("X-Amz-Date"))
    if amzDate == "" {
        if date := strings.TrimSpace(r.Header.Get("Date")); date != "" {
            if t, derr := time.Parse(http.TimeFormat, date); derr == nil {
                amzDate = t.UTC().Format(amzTimeFormat)
            }
        }
    }
    if amzDate == "" {
        return ErrMalformedDate
    }
    if !strings.HasPrefix(amzDate, scopeDate) {
        return fmt.Errorf("sigv4: request timestamp %q does not match credential scope date %q", amzDate, scopeDate)
    }
    payloadHash, err := verifyPayloadHash(r)
    if err != nil {
        return err
    }
    if err := validatePayloadDigest(payloadHash); err != nil {
        return err
    }
    creq, err := Build(r, payloadHash, h.signedNames)
    if err != nil {
        return err
    }
    scope := strings.Join([]string{scopeDate, scopeRegion, scopeService, scopeSuffix}, "/")
    sts := stringToSign(amzDate, scope, creq.Text)
    want := hmacSHA256(signingKey(creds.SecretAccessKey, scopeDate, scopeRegion, scopeService), sts)
    got, decErr := hex.DecodeString(h.signatureHex)
    if decErr != nil || len(got) != sha256.Size {
        return ErrSignatureMismatch
    }
    if !hmac.Equal(want, got) {
        return ErrSignatureMismatch
    }
    return nil
}
