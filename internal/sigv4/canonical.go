package sigv4

import (
	"net/http"
	"sort"
	"strings"
)

// optionalSignedHeaders are signed by Sign only when present and non-blank.
// The SigV4 spec mandates host and x-amz-*; content-type and content-md5 are
// the conventional optional additions on the S3 signing path.
var optionalSignedHeaders = map[string]bool{
	"content-type": true,
	"content-md5":  true,
}

// deriveSignedHeaders picks the header names to sign for outgoing requests:
// host (always), every x-amz-* header (per spec), plus the optional headers
// above when present and non-blank. The result is the sorted lowercase set —
// exactly the list CanonicalRequest will render, which is what keeps Sign and
// Verify in lockstep (ticket-001: content-type used to be canonicalized on
// the signing path while missing from the verifier's SignedHeaders list).
func deriveSignedHeaders(r *http.Request) []string {
	signed := map[string]struct{}{"host": {}}
	for name, values := range r.Header {
		lower := strings.ToLower(name)
		switch {
		case strings.HasPrefix(lower, "x-amz-"):
			signed[lower] = struct{}{}
		case optionalSignedHeaders[lower]:
			for _, v := range values {
				if strings.TrimSpace(v) != "" {
					signed[lower] = struct{}{}
					break
				}
			}
		}
	}
	names := make([]string, 0, len(signed))
	for name := range signed {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// canonicalHeaders normalizes the signed header names (lowercase, dedup,
// sorted) and renders the canonical headers blob, already newline-terminated.
func canonicalHeaders(r *http.Request, names []string) ([]string, string) {
	set := make(map[string]struct{}, len(names))
	for _, n := range names {
		n = strings.ToLower(strings.TrimSpace(n))
		if n != "" {
			set[n] = struct{}{}
		}
	}
	list := make([]string, 0, len(set))
	for n := range set {
		list = append(list, n)
	}
	sort.Strings(list)

	var blob strings.Builder
	for _, n := range list {
		blob.WriteString(n)
		blob.WriteByte(':')
		blob.WriteString(headerValues(r, n))
		blob.WriteByte('\n')
	}
	return list, blob.String()
}

// headerValues renders a single canonical header line value: each stored
// value is trimmed and has internal whitespace runs collapsed, and multiple
// values are joined with "," per the SigV4 spec.
func headerValues(r *http.Request, name string) string {
	var raw []string
	if name == "host" {
		if h := hostOf(r); h != "" {
			raw = []string{h}
		}
	} else {
		raw = r.Header.Values(name)
	}
	parts := make([]string, 0, len(raw))
	for _, v := range raw {
		parts = append(parts, collapseSpaces(strings.TrimSpace(v)))
	}
	return strings.Join(parts, ",")
}

// hostOf returns the Host authority exactly as transmitted.
func hostOf(r *http.Request) string {
	if r.Host != "" {
		return r.Host
	}
	return r.URL.Host
}

// collapseSpaces trims leading/trailing whitespace (already trimmed by the
// caller) and turns runs of whitespace into a single space per the spec.
func collapseSpaces(s string) string {
	if s == "" {
		return s
	}
	var b strings.Builder
	space := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == ' ' || c == '\t' {
			space = true
			continue
		}
		if space && b.Len() > 0 {
			b.WriteByte(' ')
		}
		space = false
		b.WriteByte(c)
	}
	return b.String()
}

// canonicalURI returns the S3 canonical URI: the request path exactly as the
// client encoded it (single-encoded, never re-encoded), defaulting to "/".
func canonicalURI(r *http.Request) string {
	p := r.URL.EscapedPath()
	if p == "" {
		p = "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return p
}

// canonicalQueryString encodes the query string per the SigV4 spec: pairs are
// decoded and re-encoded with the canonical URI-encoding, then sorted by
// encoded key, then encoded value, and joined with "&". A value-less key
// renders as "key=".
func canonicalQueryString(r *http.Request) string {
	raw := r.URL.RawQuery
	if raw == "" {
		return ""
	}
	type pair struct{ k, v string }
	var dec []pair
	for _, item := range strings.Split(raw, "&") {
		if item == "" {
			continue
		}
		k, v := item, ""
		if i := strings.IndexByte(item, '='); i >= 0 {
			k, v = item[:i], item[i+1:]
		}
		dec = append(dec, pair{uriDecode(k), uriDecode(v)})
	}
	if len(dec) == 0 {
		return ""
	}
	enc := make([]pair, len(dec))
	for i, p := range dec {
		enc[i] = pair{uriEncode(p.k), uriEncode(p.v)}
	}
	sort.Slice(enc, func(i, j int) bool {
		if enc[i].k != enc[j].k {
			return enc[i].k < enc[j].k
		}
		return enc[i].v < enc[j].v
	})
	var b strings.Builder
	for i, p := range enc {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(p.k)
		b.WriteByte('=')
		b.WriteString(p.v)
	}
	return b.String()
}

// uriEncode applies the SigV4 URI encoding: unreserved characters survive,
// everything else becomes uppercase-hex percent escapes (space = %20).
func uriEncode(s string) string {
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '.' || c == '_' || c == '~' {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hexDigits[c>>4])
		b.WriteByte(hexDigits[c&0xF])
	}
	return b.String()
}

// uriDecode percent-decodes s leniently: malformed escapes are kept verbatim.
func uriDecode(s string) string {
	if !strings.ContainsRune(s, '%') {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			hi, lo := unhex(s[i+1]), unhex(s[i+2])
			if hi >= 0 && lo >= 0 {
				b.WriteByte(byte(hi<<4 | lo))
				i += 2
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func unhex(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	}
	return -1
}

// CanonicalRequest renders the SigV4 canonical request bytes. It is the one
// code path shared by Sign and Verify. signedHeaders is normalized (lowercase,
// deduplicated, sorted) before use. The layout is:
//
//	method \n canonicalURI \n canonicalQuery \n canonicalHeaders \n signedNames \n payloadHash
//
// where canonicalHeaders already ends with \n, producing the required blank
// separator line before the SignedHeaders element.
func CanonicalRequest(r *http.Request, signedHeaders []string, payloadHash string) []byte {
	names, blob := canonicalHeaders(r, signedHeaders)
	var b strings.Builder
	b.WriteString(r.Method)
	b.WriteByte('\n')
	b.WriteString(canonicalURI(r))
	b.WriteByte('\n')
	b.WriteString(canonicalQueryString(r))
	b.WriteByte('\n')
	b.WriteString(blob)
	b.WriteByte('\n')
	b.WriteString(strings.Join(names, ";"))
	b.WriteByte('\n')
	b.WriteString(payloadHash)
	return []byte(b.String())
}
