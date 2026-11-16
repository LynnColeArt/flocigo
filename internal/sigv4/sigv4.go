// Package sigv4 implements AWS Signature V4 request signing and boundary
// verification for the local emulator. Clients sign; the emulator verifies
// against configured static credentials — matching the local authority's
// fixture-key contract. No ambient credential lookup, ever.
package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

var (
	ErrMissingAuth  = errors.New("sigv4: missing authorization")
	ErrMalformed    = errors.New("sigv4: malformed authorization")
	ErrScope        = errors.New("sigv4: credential scope mismatch")
	ErrSignature    = errors.New("sigv4: signature mismatch")
	ErrUnsignedBody = errors.New("sigv4: payload hash missing")
)

const algo = "AWS4-HMAC-SHA256"

func hashHex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func hmacChain(k []byte, parts ...string) []byte {
	for _, p := range parts {
		m := hmac.New(sha256.New, k)
		m.Write([]byte(p))
		k = m.Sum(nil)
	}
	return k
}

// Sign mutates the request: sets X-Amz-Date, X-Amz-Content-Sha256, and the
// Authorization header. Host is taken from req.Host or URL.
func Sign(req *http.Request, access, secret, region, service string, now time.Time) error {
	if req.Host == "" {
		req.Host = req.URL.Host
	}
	stamp := now.UTC().Format("20060102T150405Z")
	date := now.UTC().Format("20060102")
	body := []byte{}
	if req.Body != nil {
		b, e := io.ReadAll(req.Body)
		if e != nil {
			return e
		}
		req.Body.Close()
		body = b
		req.Body = io.NopCloser(strings.NewReader(string(body)))
	}
	if req.Header.Get("Authorization") != "" {
		return errors.New("sigv4: authorization already present")
	}
	payload := hashHex(body)
	req.Header.Set("X-Amz-Date", stamp)
	req.Header.Set("X-Amz-Content-Sha256", payload)
	headers, names := canonicalize(req, payload)
	canonical := strings.Join([]string{req.Method, canonicalURI(req), canonicalQuery(req),
		headers, names, payload}, "\n")
	scope := strings.Join([]string{date, region, service, "aws4_request"}, "/")
	key := hmacChain([]byte("AWS4"+secret), date, region, service, "aws4_request")
	sig := hex.EncodeToString(hmacChain(key, algo+"\n"+stamp+"\n"+scope+"\n"+hashHex([]byte(canonical))))
	req.Header.Set("Authorization", fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		algo, access, scope, names, sig))
	return nil
}

func canonicalURI(req *http.Request) string {
	u := req.URL
	p := u.EscapedPath()
	if p == "" {
		p = "/"
	}
	return p
}

func canonicalQuery(req *http.Request) string {
	q := req.URL.Query()
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		vs := q[k]
		sort.Strings(vs)
		for _, v := range vs {
			parts = append(parts, uriEncode(k)+"="+uriEncode(v))
		}
	}
	return strings.Join(parts, "&")
}

func canonicalize(req *http.Request, payload string) (string, string) {
	seen := map[string]string{"host": strings.ToLower(req.Host),
		"content-type":         strings.ToLower(req.Header.Get("Content-Type")),
		"content-md5":          strings.ToLower(req.Header.Get("Content-Md5")),
		"x-amz-content-sha256": payload,
		"x-amz-date":           req.Header.Get("X-Amz-Date")}
	for k, vs := range req.Header {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "x-amz-") && lk != "x-amz-content-sha256" && lk != "x-amz-date" {
			seen[lk] = strings.ToLower(strings.Join(vs, ","))
		}
	}
	var names []string
	for k := range seen {
		if seen[k] != "" {
			names = append(names, k)
		}
	}
	sort.Strings(names)
	var lines []string
	for _, n := range names {
		lines = append(lines, n+":"+seen[n])
	}
	return strings.Join(lines, "\n"), strings.Join(names, ";")
}

func uriEncode(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '.' || c == '_' || c == '~' {
			b.WriteByte(c)
		} else {
			b.WriteString(fmt.Sprintf("%%%02X", c))
		}
	}
	return b.String()
}

// Verify recomputes the signature of an inbound signed request.
func Verify(req *http.Request, access, secret, region, service string) error {
	auth := req.Header.Get("Authorization")
	if !strings.HasPrefix(auth, algo) {
		return ErrMissingAuth
	}
	var credential, signedHeaders, gotSig string
	for _, part := range strings.Split(auth[len(algo):], ",") {
		part = strings.TrimSpace(part)
		switch {
		case strings.HasPrefix(part, "Credential="):
			credential = strings.TrimPrefix(part, "Credential=")
		case strings.HasPrefix(part, "SignedHeaders="):
			signedHeaders = strings.TrimPrefix(part, "SignedHeaders=")
		case strings.HasPrefix(part, "Signature="):
			gotSig = strings.TrimPrefix(part, "Signature=")
		}
	}
	if credential == "" || signedHeaders == "" || gotSig == "" {
		return ErrMalformed
	}
	scopeParts := strings.Split(credential, "/")
	if len(scopeParts) != 5 || scopeParts[0] != access ||
		scopeParts[2] != region || scopeParts[3] != service {
		return ErrScope
	}
	payload := req.Header.Get("X-Amz-Content-Sha256")
	if payload == "" {
		return ErrUnsignedBody
	}
	if req.Host == "" {
		req.Host = req.URL.Host
	}
	var lines []string
	for _, n := range strings.Split(signedHeaders, ";") {
		var v string
		switch n {
		case "host":
			v = strings.ToLower(req.Host)
		default:
			vals := req.Header.Values(http.CanonicalHeaderKey(n))
			sort.Strings(vals)
			v = strings.ToLower(strings.Join(vals, ","))
		}
		lines = append(lines, n+":"+v)
	}
	canonical := strings.Join([]string{req.Method, canonicalURI(req), canonicalQuery(req),
		strings.Join(lines, "\n"), signedHeaders, payload}, "\n")
	stamp := req.Header.Get("X-Amz-Date")
	if len(stamp) < 8 {
		return ErrMalformed
	}
	date := stamp[:8]
	scope := credential[len(access)+1:]
	key := hmacChain([]byte("AWS4"+secret), date, region, service, "aws4_request")
	want := hex.EncodeToString(hmacChain(key, "AWS4-HMAC-SHA256\n"+stamp+"\n"+scope+"\n"+hashHex([]byte(canonical))))
	if !hmac.Equal([]byte(want), []byte(gotSig)) {
		return ErrSignature
	}
	return nil
}
