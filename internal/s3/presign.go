package s3

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// PresignHooks plug into signed-request handling: credential resolution, a
// replaceable clock for expiry checks, and acceptance/rejection callbacks.
type PresignHooks struct {
	// ResolveSecret maps an access key id to its signing secret. When it
	// reports !ok the request is trusted (emulator behaviour).
	ResolveSecret func(accessKeyID string) (secret string, ok bool)
	// Now overrides the clock used for presign expiry checks.
	Now func() time.Time
	// OnAuthorized fires for accepted signed requests
	// (presigned-v2, presigned-v4, header-v2, header-v4).
	OnAuthorized func(r *http.Request, scheme, accessKey string)
	// OnRejected fires when a signed request fails validation.
	OnRejected func(r *http.Request, err error)
}

func (rt *Router) hooks() *PresignHooks {
	if rt.Presign == nil {
		rt.Presign = &PresignHooks{}
	}
	if rt.Presign.ResolveSecret == nil {
		rt.Presign.ResolveSecret = defaultResolveSecret
	}
	if rt.Presign.Now == nil {
		rt.Presign.Now = time.Now
	}
	return rt.Presign
}

// defaultResolveSecret honors the AWS_*_KEY environment pair plus the common
// emulator default test/test. Unknown access keys are trusted.
func defaultResolveSecret(accessKeyID string) (string, bool) {
	if env := os.Getenv("AWS_ACCESS_KEY_ID"); env != "" && accessKeyID == env {
		return os.Getenv("AWS_SECRET_ACCESS_KEY"), true
	}
	if accessKeyID == "test" {
		return "test", true
	}
	return "", false
}

// authorize validates SigV2/SigV4 signed and presigned requests. Anonymous
// and unverifiable-credential requests are accepted, matching emulator mode;
// expiry of presigned URLs is always enforced.
func (rt *Router) authorize(r *http.Request, bucket string, hostStyle bool) *s3Error {
	q := r.URL.Query()
	hooks := rt.hooks()
	accept := func(scheme, accessKey string) {
		if hooks.OnAuthorized != nil {
			hooks.OnAuthorized(r, scheme, accessKey)
		}
	}
	reject := func(e *s3Error) *s3Error {
		if hooks.OnRejected != nil {
			hooks.OnRejected(r, e)
		}
		return e
	}
	var e *s3Error
	switch {
	case q.Get("X-Amz-Algorithm") != "" || q.Has("X-Amz-Signature"):
		e = rt.authorizePresignV4(r, q, hooks, accept)
	case q.Has("AWSAccessKeyId") || q.Has("Signature"):
		e = rt.authorizePresignV2(r, q, bucket, hostStyle, hooks, accept)
	case strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256"):
		e = rt.authorizeHeaderV4(r, hooks, accept)
	case strings.HasPrefix(r.Header.Get("Authorization"), "AWS "):
		e = rt.authorizeHeaderV2(r, bucket, hostStyle, hooks, accept)
	}
	if e != nil {
		return reject(e)
	}
	return nil
}

// authorizePresignV4 verifies query-string SigV4 (presigned) requests.
func (rt *Router) authorizePresignV4(r *http.Request, q url.Values, hooks *PresignHooks, accept func(string, string)) *s3Error {
	const alg = "AWS4-HMAC-SHA256"
	if q.Get("X-Amz-Algorithm") != alg {
		return s3err(400, "AuthorizationQueryParametersError", "X-Amz-Algorithm only supports %q", alg)
	}
	cred, signature := q.Get("X-Amz-Credential"), q.Get("X-Amz-Signature")
	signedHeaders, amzDate, expiresRaw := q.Get("X-Amz-SignedHeaders"), q.Get("X-Amz-Date"), q.Get("X-Amz-Expires")
	if cred == "" || signature == "" || signedHeaders == "" || amzDate == "" || expiresRaw == "" {
		return s3err(400, "AuthorizationQueryParametersError", "Query-string authentication version 4 requires the X-Amz-Algorithm, X-Amz-Credential, X-Amz-Signature, X-Amz-Date, X-Amz-SignedHeaders, and X-Amz-Expires parameters.")
	}
	accessKey, credDate, region, service, ok := parseCredential(cred)
	if !ok {
		return s3err(400, "AuthorizationQueryParametersError", "Error parsing the X-Amz-Credential parameter; the Credential is mal-formed; expecting %q.", "<YOUR-AKID>/YYYYMMDD/REGION/SERVICE/aws4_request")
	}
	expires, cerr := strconv.Atoi(expiresRaw)
	if cerr != nil {
		return s3err(400, "AuthorizationQueryParametersError", "X-Amz-Expires should be a number")
	}
	if expires < 1 || expires > 604800 {
		return s3err(400, "AuthorizationQueryParametersError", "X-Amz-Expires must be between 1 and 604800 seconds")
	}
	start, derr := time.Parse("20060102T150405Z", amzDate)
	if derr != nil {
		return s3err(403, "AccessDenied", "AWS authentication requires a valid Date or x-amz-date header")
	}
	if hooks.Now().After(start.Add(time.Duration(expires) * time.Second)) {
		return s3err(403, "AccessDenied", "Request has expired")
	}
	secret, known := hooks.ResolveSecret(accessKey)
	if !known {
		accept("presigned-v4", accessKey) // trust unknown credentials
		return nil
	}
	canonicalRequest := presignCanonicalRequest(r, signedHeaders)
	sts := strings.Join([]string{alg, amzDate, credDate + "/" + region + "/" + service + "/aws4_request", sha256Hex(canonicalRequest)}, "\n")
	want := hex.EncodeToString(hmacSHA256(signingKey(secret, credDate, region, service), []byte(sts)))
	if !strings.EqualFold(want, signature) {
		e := s3err(403, "SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided. Check your key and signing method.")
		e.Extra = map[string]string{
			"AWSAccessKeyId":    accessKey,
			"StringToSign":      sts,
			"SignatureProvided": signature,
		}
		return e
	}
	accept("presigned-v4", accessKey)
	return nil
}

// authorizeHeaderV4 verifies SigV4 Authorization headers when the access key
// is known; unknown keys are trusted.
func (rt *Router) authorizeHeaderV4(r *http.Request, hooks *PresignHooks, accept func(string, string)) *s3Error {
	rest := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256"))
	fields := map[string]string{}
	for _, kv := range strings.Split(rest, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(kv), "=")
		if ok {
			fields[strings.TrimSpace(k)] = v
		}
	}
	cred, signedHeaders, signature := fields["Credential"], fields["SignedHeaders"], fields["Signature"]
	if cred == "" || signedHeaders == "" || signature == "" {
		return s3err(400, "InvalidRequest", "Authorization header requires 'Credential', 'SignedHeaders' and 'Signature' parameters.")
	}
	accessKey, date, region, service, ok := parseCredential(cred)
	if !ok {
		return s3err(400, "AuthorizationHeaderMalformed", "The authorization header is malformed; the Credential is mal-formed; expecting %q.", "YYYYMMDD/REGION/SERVICE/aws4_request")
	}
	secret, known := hooks.ResolveSecret(accessKey)
	if !known {
		accept("header-v4", accessKey)
		return nil
	}
	amzDate := strings.TrimSpace(r.Header.Get("x-amz-date"))
	if amzDate == "" {
		// SigV4 requires x-amz-date (or Date); without one we cannot verify.
		accept("header-v4", accessKey)
		return nil
	}
	payload := r.Header.Get("x-amz-content-sha256")
	if payload == "" {
		payload = "UNSIGNED-PAYLOAD"
	}
	canonicalRequest := headerCanonicalRequest(r, signedHeaders, payload)
	sts := strings.Join([]string{"AWS4-HMAC-SHA256", amzDate, date + "/" + region + "/" + service + "/aws4_request", sha256Hex(canonicalRequest)}, "\n")
	want := hex.EncodeToString(hmacSHA256(signingKey(secret, date, region, service), []byte(sts)))
	if !strings.EqualFold(want, signature) {
		e := s3err(403, "SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided. Check your key and signing method.")
		e.Extra = map[string]string{
			"AWSAccessKeyId":    accessKey,
			"StringToSign":      sts,
			"SignatureProvided": signature,
		}
		return e
	}
	accept("header-v4", accessKey)
	return nil
}

// authorizePresignV2 verifies query-string SigV2 presigned requests.
func (rt *Router) authorizePresignV2(r *http.Request, q url.Values, bucket string, hostStyle bool, hooks *PresignHooks, accept func(string, string)) *s3Error {
	accessKey, signature := q.Get("AWSAccessKeyId"), q.Get("Signature")
	if accessKey == "" || signature == "" {
		return s3err(403, "AccessDenied", "Query-string authentication requires the AWSAccessKeyId, Signature and Expires parameters.")
	}
	if expires := q.Get("Expires"); expires != "" {
		deadline, perr := strconv.ParseInt(expires, 10, 64)
		if perr != nil {
			return s3err(403, "AccessDenied", "Invalid date (should be seconds since epoch): %s", expires)
		}
		if hooks.Now().After(time.Unix(deadline, 0)) {
			return s3err(403, "AccessDenied", "Request has expired")
		}
	}
	secret, known := hooks.ResolveSecret(accessKey)
	if !known {
		accept("presigned-v2", accessKey)
		return nil
	}
	sts := v2StringToSign(r, q.Get("Expires"), bucket, hostStyle)
	want := base64.StdEncoding.EncodeToString(hmacSHA1([]byte(secret), []byte(sts)))
	if want != signature {
		e := s3err(403, "SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided. Check your key and signing method.")
		e.Extra = map[string]string{
			"AWSAccessKeyId":    accessKey,
			"StringToSign":      sts,
			"SignatureProvided": signature,
		}
		return e
	}
	accept("presigned-v2", accessKey)
	return nil
}

// authorizeHeaderV2 verifies classic SigV2 Authorization headers when the
// access key is known; unknown keys are trusted.
func (rt *Router) authorizeHeaderV2(r *http.Request, bucket string, hostStyle bool, hooks *PresignHooks, accept func(string, string)) *s3Error {
	accessKey, signature, ok := strings.Cut(strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "AWS ")), ":")
	if !ok || accessKey == "" || signature == "" {
		return s3err(403, "AccessDenied", "Invalid authorization mechanism")
	}
	secret, known := hooks.ResolveSecret(accessKey)
	if !known {
		accept("header-v2", accessKey)
		return nil
	}
	date := strings.TrimSpace(r.Header.Get("x-amz-date"))
	if date == "" {
		date = r.Header.Get("Date")
	}
	sts := v2StringToSign(r, date, bucket, hostStyle)
	want := base64.StdEncoding.EncodeToString(hmacSHA1([]byte(secret), []byte(sts)))
	if want != signature {
		e := s3err(403, "SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided. Check your key and signing method.")
		e.Extra = map[string]string{
			"AWSAccessKeyId":    accessKey,
			"StringToSign":      sts,
			"SignatureProvided": signature,
		}
		return e
	}
	accept("header-v2", accessKey)
	return nil
}

// parseCredential splits AKID/date/region/service/aws4_request.
func parseCredential(cred string) (accessKey, date, region, service string, ok bool) {
	parts := strings.SplitN(cred, "/", 5)
	if len(parts) != 5 || parts[4] != "aws4_request" {
		return "", "", "", "", false
	}
	return parts[0], parts[1], parts[2], parts[3], true
}

// presignCanonicalRequest builds the SigV4 canonical request for a presigned
// URL (UNSIGNED-PAYLOAD unless a concrete content hash was signed).
func presignCanonicalRequest(r *http.Request, signedHeaders string) string {
	q := r.URL.Query()
	q.Del("X-Amz-Signature")
	payload := "UNSIGNED-PAYLOAD"
	if containsToken(signedHeaders, "x-amz-content-sha256") {
		if v := strings.TrimSpace(r.Header.Get("x-amz-content-sha256")); v != "" {
			payload = v
		}
	}
	return r.Method + "\n" + canonicalURI(r) + "\n" + canonicalQueryV4(q) + "\n" + canonicalHeaders(r, signedHeaders) + "\n" + signedHeaders + "\n" + payload
}

// headerCanonicalRequest builds the SigV4 canonical request for header auth.
func headerCanonicalRequest(r *http.Request, signedHeaders, payload string) string {
	return r.Method + "\n" + canonicalURI(r) + "\n" + canonicalQueryV4(r.URL.Query()) + "\n" + canonicalHeaders(r, signedHeaders) + "\n" + signedHeaders + "\n" + payload
}

// canonicalURI is the raw escaped request path as the client signed it.
func canonicalURI(r *http.Request) string {
	uri := r.URL.EscapedPath()
	if uri == "" {
		uri = "/"
	}
	return uri
}

// canonicalHeaders renders the signed header block ("k:v\n" lines).
func canonicalHeaders(r *http.Request, signedHeaders string) string {
	var b strings.Builder
	for _, name := range strings.Split(signedHeaders, ";") {
		name = strings.ToLower(strings.TrimSpace(name))
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(headerValue(r, name))
		b.WriteByte('\n')
	}
	return b.String()
}

func headerValue(r *http.Request, name string) string {
	switch name {
	case "host":
		return r.Host
	case "content-length":
		return strconv.FormatInt(r.ContentLength, 10)
	}
	vals := r.Header.Values(name)
	parts := make([]string, 0, len(vals))
	for _, v := range vals {
		parts = append(parts, collapseSpaces(strings.TrimSpace(v)))
	}
	return strings.Join(parts, ",")
}

func collapseSpaces(v string) string {
	if !strings.Contains(v, "  ") && !strings.Contains(v, "\t") {
		return v
	}
	return strings.Join(strings.Fields(v), " ")
}

func containsToken(list, token string) bool {
	for _, item := range strings.Split(list, ";") {
		if strings.EqualFold(strings.TrimSpace(item), token) {
			return true
		}
	}
	return false
}

// canonicalQueryV4 renders url.Values with SigV4 canonical encoding.
func canonicalQueryV4(q url.Values) string {
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		vals := append([]string(nil), q[k]...)
		sort.Strings(vals)
		for _, v := range vals {
			if b.Len() > 0 {
				b.WriteByte('&')
			}
			b.WriteString(awsURIEncode(k))
			b.WriteByte('=')
			b.WriteString(awsURIEncode(v))
		}
	}
	return b.String()
}

// awsURIEncode percent-encodes everything outside the SigV4 unreserved set.
func awsURIEncode(s string) string {
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '.', c == '_', c == '~':
			b.WriteByte(c)
		case c == ' ':
			b.WriteString("%20")
		default:
			b.WriteByte('%')
			b.WriteByte(hexDigits[c>>4])
			b.WriteByte(hexDigits[c&0x0F])
		}
	}
	return b.String()
}

// v2Subresources are appended to the canonicalized SigV2 resource string.
var v2Subresources = map[string]bool{
	"accelerate": true, "acl": true, "cors": true, "delete": true,
	"lifecycle": true, "location": true, "logging": true, "notification": true,
	"partNumber": true, "policy": true, "requestPayment": true, "tagging": true,
	"torrent": true, "uploadId": true, "uploads": true, "versionId": true,
	"versioning": true, "versions": true, "website": true,
	"response-cache-control": true, "response-content-disposition": true,
	"response-content-encoding": true, "response-content-language": true,
	"response-content-type": true, "response-expires": true,
}

// v2StringToSign assembles the classic SigV2 string-to-sign. dateOrExpires
// is the Expires line for presigned requests or the Date line otherwise.
func v2StringToSign(r *http.Request, dateOrExpires, bucket string, hostStyle bool) string {
	amz := map[string]string{}
	for name, vals := range r.Header {
		ln := strings.ToLower(name)
		if !strings.HasPrefix(ln, "x-amz-") || len(vals) == 0 {
			continue
		}
		value := strings.Join(vals, ",")
		if prev, ok := amz[ln]; ok {
			amz[ln] = prev + "," + value
		} else {
			amz[ln] = value
		}
	}
	names := make([]string, 0, len(amz))
	for k := range amz {
		names = append(names, k)
	}
	sort.Strings(names)
	var headers strings.Builder
	for _, n := range names {
		headers.WriteString(n)
		headers.WriteByte(':')
		headers.WriteString(amz[n])
		headers.WriteByte('\n')
	}
	path := r.URL.EscapedPath()
	resource := path
	if path == "" {
		resource = "/"
	}
	if !hostStyle && bucket != "" {
		resource = "/" + bucket + path
	}
	q := r.URL.Query()
	subs := make([]string, 0, len(q))
	for k := range q {
		if v2Subresources[k] {
			subs = append(subs, k)
		}
	}
	sort.Strings(subs)
	for _, k := range subs {
		resource += "?" + k
		if v := q.Get(k); v != "" {
			resource += "=" + v
		}
	}
	return r.Method + "\n" + r.Header.Get("Content-Md5") + "\n" + r.Header.Get("Content-Type") + "\n" + dateOrExpires + "\n" + headers.String() + resource
}

func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

func signingKey(secret, date, region, service string) []byte {
	k := hmacSHA256([]byte("AWS4"+secret), []byte(date))
	k = hmacSHA256(k, []byte(region))
	k = hmacSHA256(k, []byte(service))
	return hmacSHA256(k, []byte("aws4_request"))
}

func hmacSHA1(key, data []byte) []byte {
	mac := hmac.New(sha1.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
