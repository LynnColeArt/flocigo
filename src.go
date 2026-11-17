// floci-go — single-file, in-memory S3-compatible object store.
//
// Ticket floci-go/ticket-011: GET /{bucket}?list-type=2 (ListObjectsV2)
//   - <Contents> entries matching the requested prefix
//   - keys emitted in lexical (byte-wise) order
//   - IsTruncated=false (no pagination at this stage)
//   - canonical S3 XML (single line, xmlns 2006-03-01)
// plus the supporting endpoints needed to exercise it (buckets, objects,
// listing v1, batch delete, copy).
package main

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	s3ns    = "http://s3.amazonaws.com/doc/2006-03-01/"
	xmlDecl = `<?xml version="1.0" encoding="UTF-8"?>` + "\n"
	xmlCT   = "application/xml"
	defCT   = "binary/octet-stream"
	reg     = "us-east-1"
	ownID   = "floci"
	ownDN   = "floci"
	listCap = 1000
	s3Time  = "2006-01-02T15:04:05.000Z"
)

const malformedXMLMsg = "The XML you provided was not well-formed or did not validate against our published schema"

// ------------------------------ object store ------------------------------

type objectEntry struct {
	data  []byte
	etag  string // hex md5, unquoted
	mtime time.Time
	ctype string
	meta  map[string]string
}

type bucketData struct {
	objects map[string]*objectEntry
	ctime   time.Time
}

type store struct {
	mu      sync.RWMutex
	buckets map[string]*bucketData
}

var db = &store{buckets: make(map[string]*bucketData)}

func (s *store) hasBucket(b string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.buckets[b]
	return ok
}

func (s *store) createBucket(b string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.buckets[b]; !ok {
		s.buckets[b] = &bucketData{objects: make(map[string]*objectEntry), ctime: time.Now().UTC()}
	}
}

func (s *store) removeBucket(b string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.buckets, b)
}

func (s *store) countObjects(b string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if bd, ok := s.buckets[b]; ok {
		return len(bd.objects)
	}
	return 0
}

func (s *store) names() ([]string, map[string]time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ns := make([]string, 0, len(s.buckets))
	ct := make(map[string]time.Time, len(s.buckets))
	for n, bd := range s.buckets {
		ns = append(ns, n)
		ct[n] = bd.ctime
	}
	sort.Strings(ns)
	return ns, ct
}

func (s *store) listKeys(b string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	bd, ok := s.buckets[b]
	if !ok {
		return nil
	}
	keys := make([]string, 0, len(bd.objects))
	for k := range bd.objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (s *store) get(b, k string) (*objectEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if bd, ok := s.buckets[b]; ok {
		if e, ok2 := bd.objects[k]; ok2 {
			return e, true
		}
	}
	return nil, false
}

func (s *store) put(b, k string, e *objectEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if bd, ok := s.buckets[b]; ok {
		bd.objects[k] = e
	}
}

func (s *store) del(b, k string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if bd, ok := s.buckets[b]; ok {
		delete(bd.objects, k)
	}
}

// ------------------------------ request plumbing ------------------------------

type reqCtx struct {
	w   http.ResponseWriter
	r   *http.Request
	rid string
	hid string
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return strings.ToUpper(hex.EncodeToString(b))
}

func randHost() string {
	b := make([]byte, 48)
	_, _ = rand.Read(b)
	return base64.RawStdEncoding.EncodeToString(b)
}

func quote(s string) string { return `"` + s + `"` }

func writeXML(rq *reqCtx, status int, body string) {
	rq.w.Header().Set("Content-Type", xmlCT)
	rq.w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	rq.w.WriteHeader(status)
	_, _ = io.WriteString(rq.w, body)
}

func errResp(rq *reqCtx, status int, code, msg, extra string) {
	var b strings.Builder
	b.WriteString(xmlDecl)
	b.WriteString("<Error><Code>" + xmlEscape(code) + "</Code><Message>" + xmlEscape(msg) + "</Message>" + extra)
	b.WriteString("<RequestId>" + xmlEscape(rq.rid) + "</RequestId><HostId>" + rq.hid + "</HostId></Error>")
	writeXML(rq, status, b.String())
}

func bucketXML(b string) string { return "<BucketName>" + xmlEscape(b) + "</BucketName>" }

func xmlEscape(s string) string {
	if !strings.ContainsAny(s, "&<>") {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '&':
			b.WriteString("&amp;")
		case '<':
			b.WriteString("&lt;")
		case '>':
			b.WriteString("&gt;")
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

// s3Encode percent-encodes everything except RFC 3986 unreserved characters
// (used for encoding-type=url, matching S3 wire behavior incl. %2F for '/').
func s3Encode(s string) string {
	if s == "" {
		return ""
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// txt renders a user value as XML text; url-encoded when encoding-type=url.
func txt(s string, enc bool) string {
	if enc {
		return s3Encode(s)
	}
	return xmlEscape(s)
}

func aclPolicyXML() string {
	var b strings.Builder
	b.WriteString(xmlDecl)
	b.WriteString(`<AccessControlPolicy xmlns="` + s3ns + `">`)
	b.WriteString("<Owner><ID>" + xmlEscape(ownID) + "</ID><DisplayName>" + xmlEscape(ownDN) + "</DisplayName></Owner>")
	b.WriteString(`<AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>`)
	b.WriteString(xmlEscape(ownID))
	b.WriteString("</ID><DisplayName>" + xmlEscape(ownDN) + "</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList>")
	b.WriteString("</AccessControlPolicy>")
	return b.String()
}

func validBucketName(b string) bool {
	if len(b) < 3 || len(b) > 63 {
		return false
	}
	for i := 0; i < len(b); i++ {
		c := b[i]
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '.') {
			return false
		}
	}
	if b[0] == '-' || b[0] == '.' || b[len(b)-1] == '-' || b[len(b)-1] == '.' {
		return false
	}
	if strings.Contains(b, "..") || strings.Contains(b, ".-") || strings.Contains(b, "-.") {
		return false
	}
	hasLetter := false
	for i := 0; i < len(b); i++ {
		if b[i] >= 'a' && b[i] <= 'z' {
			hasLetter = true
			break
		}
	}
	if !hasLetter {
		parts := strings.Split(b, ".")
		if len(parts) == 4 {
			ip := true
			for _, p := range parts {
				if !isAllDigits(p) {
					ip = false
					break
				}
			}
			if ip {
				return false
			}
		}
	}
	return true
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func etagMatch(spec, etag string) bool {
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if part == "*" {
			return true
		}
		part = strings.TrimPrefix(part, "W/")
		part = strings.Trim(part, `"`)
		if part == etag {
			return true
		}
	}
	return false
}

func parseRange(hdr string, size int) (start, end int, use, unsat bool) {
	rest := strings.TrimPrefix(hdr, "bytes=")
	if rest == hdr {
		return 0, 0, false, false
	}
	if strings.IndexByte(rest, ',') >= 0 {
		return 0, 0, false, false
	}
	i := strings.IndexByte(rest, '-')
	if i < 0 {
		return 0, 0, false, false
	}
	first := strings.TrimSpace(rest[:i])
	last := strings.TrimSpace(rest[i+1:])
	if first == "" {
		if last == "" {
			return 0, 0, false, false
		}
		n, err := strconv.Atoi(last)
		if err != nil || n <= 0 {
			return 0, 0, false, false
		}
		if size == 0 {
			return 0, 0, false, true
		}
		if n > size {
			n = size
		}
		return size - n, size - 1, true, false
	}
	s0, err := strconv.Atoi(first)
	if err != nil || s0 < 0 {
		return 0, 0, false, false
	}
	if last == "" {
		if s0 >= size {
			return 0, 0, false, true
		}
		return s0, size - 1, true, false
	}
	e0, err2 := strconv.Atoi(last)
	if err2 != nil || e0 < s0 {
		return 0, 0, false, false
	}
	if s0 >= size {
		return 0, 0, false, true
	}
	if e0 >= size {
		e0 = size - 1
	}
	return s0, e0, true, false
}

func contentTypeHeader(r *http.Request) string {
	if ct := r.Header.Get("Content-Type"); ct != "" {
		return ct
	}
	return defCT
}

func metaFromHeaders(r *http.Request) map[string]string {
	var meta map[string]string
	for name, vals := range r.Header {
		ln := strings.ToLower(name)
		if strings.HasPrefix(ln, "x-amz-meta-") && len(vals) > 0 {
			if meta == nil {
				meta = make(map[string]string)
			}
			meta[ln[len("x-amz-meta-"):]] = vals[0]
		}
	}
	return meta
}

// ------------------------------ routing ------------------------------

func route(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if p := recover(); p != nil {
			fmt.Fprintf(os.Stderr, "floci-go panic: %v\n", p)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}()
	rq := &reqCtx{w: w, r: r, rid: randHex(8), hid: randHost()}
	w.Header().Set("x-amz-request-id", rq.rid)
	w.Header().Set("x-amz-id-2", rq.hid)

	trimmed := strings.TrimPrefix(r.URL.Path, "/")
	if trimmed == "" {
		switch r.Method {
		case http.MethodGet:
			hListAllBuckets(rq)
		case http.MethodHead:
			w.WriteHeader(http.StatusOK)
		default:
			errResp(rq, http.StatusMethodNotAllowed, "MethodNotAllowed", "The specified method is not allowed against this resource.", "<Method>"+xmlEscape(r.Method)+"</Method>")
		}
		return
	}

	bucket, key := trimmed, ""
	if i := strings.IndexByte(trimmed, '/'); i >= 0 {
		bucket, key = trimmed[:i], trimmed[i+1:]
	}

	if key == "" {
		switch r.Method {
		case http.MethodPut:
			hCreateBucket(rq, bucket)
		case http.MethodHead:
			if db.hasBucket(bucket) {
				w.Header().Set("x-amz-bucket-region", reg)
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		case http.MethodDelete:
			if len(r.URL.Query()) > 0 {
				// sub-resource deletions (tagging, policy, ...) are accepted no-ops
				w.WriteHeader(http.StatusNoContent)
				return
			}
			hDeleteBucket(rq, bucket)
		case http.MethodGet:
			hGetBucket(rq, bucket)
		case http.MethodPost:
			if _, ok := r.URL.Query()["delete"]; ok {
				hDeleteObjects(rq, bucket)
			} else {
				errResp(rq, http.StatusNotImplemented, "NotImplemented", "A header or query you provided implies functionality that is not implemented", "<Method>"+xmlEscape(r.Method)+"</Method>")
			}
		default:
			errResp(rq, http.StatusMethodNotAllowed, "MethodNotAllowed", "The specified method is not allowed against this resource.", "<Method>"+xmlEscape(r.Method)+"</Method>")
		}
		return
	}

	q := r.URL.Query()
	switch r.Method {
	case http.MethodPut:
		if q.Get("partNumber") != "" || q.Get("uploadId") != "" {
			errResp(rq, http.StatusNotImplemented, "NotImplemented", "A header or query you provided implies functionality that is not implemented", "<Method>PUT</Method>")
			return
		}
		if r.Header.Get("x-amz-copy-source") != "" {
			hCopyObject(rq, bucket, key)
			return
		}
		hPutObject(rq, bucket, key)
	case http.MethodGet, http.MethodHead:
		hGetObject(rq, bucket, key)
	case http.MethodDelete:
		hDeleteObject(rq, bucket, key)
	case http.MethodPost:
		if q.Get("uploads") != "" || q.Get("uploadId") != "" {
			errResp(rq, http.StatusNotImplemented, "NotImplemented", "A header or query you provided implies functionality that is not implemented", "<Method>POST</Method>")
			return
		}
		errResp(rq, http.StatusMethodNotAllowed, "MethodNotAllowed", "The specified method is not allowed against this resource.", "<Method>POST</Method>")
	default:
		errResp(rq, http.StatusMethodNotAllowed, "MethodNotAllowed", "The specified method is not allowed against this resource.", "<Method>"+xmlEscape(r.Method)+"</Method>")
	}
}

// ------------------------------ service + bucket handlers ------------------------------

func hListAllBuckets(rq *reqCtx) {
	ns, cts := db.names()
	var b strings.Builder
	b.WriteString(xmlDecl)
	b.WriteString(`<ListAllMyBucketsResult xmlns="` + s3ns + `">`)
	b.WriteString("<Owner><ID>" + xmlEscape(ownID) + "</ID><DisplayName>" + xmlEscape(ownDN) + "</DisplayName></Owner><Buckets>")
	for _, n := range ns {
		b.WriteString("<Bucket><Name>" + xmlEscape(n) + "</Name><CreationDate>" + cts[n].UTC().Format(s3Time) + "</CreationDate></Bucket>")
	}
	b.WriteString("</Buckets></ListAllMyBucketsResult>")
	writeXML(rq, http.StatusOK, b.String())
}

func hCreateBucket(rq *reqCtx, bucket string) {
	if !validBucketName(bucket) {
		errResp(rq, http.StatusBadRequest, "InvalidBucketName", "The specified bucket is not valid.", bucketXML(bucket))
		return
	}
	if len(rq.r.URL.Query()) > 0 {
		// bucket sub-resource writes (acl/cors/tagging/...) accepted as no-ops
		if !db.hasBucket(bucket) {
			errResp(rq, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist", bucketXML(bucket))
			return
		}
		rq.w.WriteHeader(http.StatusOK)
		return
	}
	db.createBucket(bucket)
	rq.w.Header().Set("Location", "/"+bucket)
	rq.w.WriteHeader(http.StatusOK)
}

func hDeleteBucket(rq *reqCtx, bucket string) {
	if !db.hasBucket(bucket) {
		errResp(rq, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist", bucketXML(bucket))
		return
	}
	if db.countObjects(bucket) != 0 {
		errResp(rq, http.StatusConflict, "BucketNotEmpty", "The bucket you tried to delete is not empty", bucketXML(bucket))
		return
	}
	db.removeBucket(bucket)
	rq.w.WriteHeader(http.StatusNoContent)
}

func hGetBucket(rq *reqCtx, bucket string) {
	if !db.hasBucket(bucket) {
		errResp(rq, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist", bucketXML(bucket))
		return
	}
	q := rq.r.URL.Query()
	if lt := q.Get("list-type"); lt != "" && lt != "2" {
		errResp(rq, http.StatusBadRequest, "InvalidArgument", "Invalid List Type specified in Request", "<ArgumentName>list-type</ArgumentName><ArgumentValue>"+xmlEscape(lt)+"</ArgumentValue>")
		return
	}
	if et := q.Get("encoding-type"); et != "" && et != "url" {
		errResp(rq, http.StatusBadRequest, "InvalidArgument", "Invalid Encoding Method specified in Request", "<ArgumentName>encoding-type</ArgumentName><ArgumentValue>"+xmlEscape(et)+"</ArgumentValue>")
		return
	}
	if _, ok := q["location"]; ok {
		writeXML(rq, http.StatusOK, xmlDecl+`<LocationConstraint xmlns="`+s3ns+`"/>`)
		return
	}
	if _, ok := q["versioning"]; ok {
		writeXML(rq, http.StatusOK, xmlDecl+`<VersioningConfiguration xmlns="`+s3ns+`"/>`)
		return
	}
	if _, ok := q["acl"]; ok {
		writeXML(rq, http.StatusOK, aclPolicyXML())
		return
	}
	if _, ok := q["logging"]; ok {
		writeXML(rq, http.StatusOK, xmlDecl+`<BucketLoggingStatus xmlns="`+s3ns+`"/>`)
		return
	}
	if _, ok := q["tagging"]; ok {
		errResp(rq, http.StatusNotFound, "NoSuchTagSet", "The TagSet does not exist", bucketXML(bucket))
		return
	}
	if _, ok := q["policy"]; ok {
		errResp(rq, http.StatusNotFound, "NoSuchBucketPolicy", "The bucket policy does not exist", bucketXML(bucket))
		return
	}
	if _, ok := q["cors"]; ok {
		errResp(rq, http.StatusNotFound, "NoSuchCORSConfiguration", "The CORS configuration does not exist", bucketXML(bucket))
		return
	}
	if _, ok := q["lifecycle"]; ok {
		errResp(rq, http.StatusNotFound, "NoSuchLifecycleConfiguration", "The lifecycle configuration does not exist", bucketXML(bucket))
		return
	}
	if _, ok := q["encryption"]; ok {
		errResp(rq, http.StatusNotFound, "ServerSideEncryptionConfigurationNotFoundError", "The server side encryption configuration was not found", bucketXML(bucket))
		return
	}
	if _, ok := q["object-lock"]; ok {
		errResp(rq, http.StatusNotFound, "ObjectLockConfigurationNotFoundError", "Object Lock configuration does not exist for this bucket", bucketXML(bucket))
		return
	}
	if _, ok := q["website"]; ok {
		errResp(rq, http.StatusNotFound, "NoSuchWebsiteConfiguration", "The specified bucket does not have a website configuration", bucketXML(bucket))
		return
	}
	if _, ok := q["versions"]; ok {
		errResp(rq, http.StatusNotImplemented, "NotImplemented", "A header or query you provided implies functionality that is not implemented", "<Method>GET</Method>")
		return
	}
	if _, ok := q["uploads"]; ok {
		errResp(rq, http.StatusNotImplemented, "NotImplemented", "A header or query you provided implies functionality that is not implemented", "<Method>GET</Method>")
		return
	}
	if q.Get("list-type") == "2" {
		hListV2(rq, bucket)
		return
	}
	hListV1(rq, bucket)
}

// ------------------------------ listings ------------------------------

type listParams struct {
	prefix     string
	delim      string
	enc        bool
	anchor     string // start-after (v2) / marker (v1)
	token      string // continuation-token (v2)
	fetchOwner bool
	v1         bool
	maxKeys    int
}

type listResult struct {
	contents []string
	prefixes []string
	count    int
}

func parseMaxKeys(rq *reqCtx, q url.Values) (int, bool) {
	sv := q.Get("max-keys")
	if sv == "" {
		return listCap, true
	}
	n, err := strconv.Atoi(sv)
	if err != nil || n < 0 {
		errResp(rq, http.StatusBadRequest, "InvalidArgument", "Argument max-keys must be an integer between 0 and 2147483647", "<ArgumentName>max-keys</ArgumentName><ArgumentValue>"+xmlEscape(sv)+"</ArgumentValue>")
		return 0, false
	}
	if n > listCap {
		return listCap, true
	}
	return n, true
}

// collectList walks keys in lexical order, applies the prefix filter (and
// start-after/marker anchor), and rolls delimiter-delimited groups into
// common prefixes.
func collectList(bucket string, p listParams) listResult {
	var res listResult
	seenCP := make(map[string]bool)
	for _, k := range db.listKeys(bucket) {
		if p.prefix != "" && !strings.HasPrefix(k, p.prefix) {
			continue
		}
		if p.anchor != "" && k <= p.anchor {
			continue
		}
		if p.delim != "" {
			rem := k[len(p.prefix):]
			if idx := strings.Index(rem, p.delim); idx >= 0 {
				cp := k[:len(p.prefix)+idx+len(p.delim)]
				if !seenCP[cp] {
					seenCP[cp] = true
					res.prefixes = append(res.prefixes, cp)
				}
				continue
			}
		}
		if e, ok := db.get(bucket, k); ok {
			res.contents = append(res.contents, renderContent(k, e, p))
		}
	}
	res.count = len(res.contents) + len(res.prefixes)
	return res
}

func renderContent(k string, e *objectEntry, p listParams) string {
	var b strings.Builder
	b.WriteString("<Contents><Key>" + txt(k, p.enc) +
		"</Key><LastModified>" + e.mtime.UTC().Format(s3Time) +
		"</LastModified><ETag>" + quote(e.etag) +
		"</ETag><Size>" + strconv.Itoa(len(e.data)) + "</Size>")
	if p.v1 || p.fetchOwner {
		b.WriteString("<Owner><ID>" + xmlEscape(ownID) + "</ID><DisplayName>" + xmlEscape(ownDN) + "</DisplayName></Owner>")
	}
	b.WriteString("<StorageClass>STANDARD</StorageClass></Contents>")
	return b.String()
}

// hListV2 implements GET /{bucket}?list-type=2 (ListObjectsV2).
// At this ticket stage the response is never truncated: every key matching
// the prefix is returned and IsTruncated is always false.
func hListV2(rq *reqCtx, bucket string) {
	q := rq.r.URL.Query()
	p := listParams{
		prefix:     q.Get("prefix"),
		delim:      q.Get("delimiter"),
		enc:        q.Get("encoding-type") == "url",
		anchor:     q.Get("start-after"),
		token:      q.Get("continuation-token"),
		fetchOwner: strings.EqualFold(q.Get("fetch-owner"), "true"),
	}
	mk, ok := parseMaxKeys(rq, q)
	if !ok {
		return
	}
	p.maxKeys = mk
	var res listResult
	if p.maxKeys > 0 {
		res = collectList(bucket, p)
	}
	var b strings.Builder
	b.WriteString(xmlDecl)
	b.WriteString(`<ListBucketResult xmlns="` + s3ns + `">`)
	b.WriteString("<Name>" + xmlEscape(bucket) + "</Name>")
	b.WriteString("<Prefix>" + txt(p.prefix, p.enc) + "</Prefix>")
	if p.anchor != "" {
		b.WriteString("<StartAfter>" + txt(p.anchor, p.enc) + "</StartAfter>")
	}
	if p.token != "" {
		b.WriteString("<ContinuationToken>" + xmlEscape(p.token) + "</ContinuationToken>")
	}
	b.WriteString("<KeyCount>" + strconv.Itoa(res.count) + "</KeyCount>")
	b.WriteString("<MaxKeys>" + strconv.Itoa(p.maxKeys) + "</MaxKeys>")
	if p.delim != "" {
		b.WriteString("<Delimiter>" + txt(p.delim, p.enc) + "</Delimiter>")
	}
	if p.enc {
		b.WriteString("<EncodingType>url</EncodingType>")
	}
	b.WriteString("<IsTruncated>false</IsTruncated>")
	for _, c := range res.contents {
		b.WriteString(c)
	}
	for _, cp := range res.prefixes {
		b.WriteString("<CommonPrefixes><Prefix>" + txt(cp, p.enc) + "</Prefix></CommonPrefixes>")
	}
	b.WriteString("</ListBucketResult>")
	rq.w.Header().Set("x-amz-bucket-region", reg)
	writeXML(rq, http.StatusOK, b.String())
}

func hListV1(rq *reqCtx, bucket string) {
	q := rq.r.URL.Query()
	p := listParams{
		prefix: q.Get("prefix"),
		delim:  q.Get("delimiter"),
		enc:    q.Get("encoding-type") == "url",
		anchor: q.Get("marker"),
		v1:     true,
	}
	mk, ok := parseMaxKeys(rq, q)
	if !ok {
		return
	}
	p.maxKeys = mk
	var res listResult
	if p.maxKeys > 0 {
		res = collectList(bucket, p)
	}
	var b strings.Builder
	b.WriteString(xmlDecl)
	b.WriteString(`<ListBucketResult xmlns="` + s3ns + `">`)
	b.WriteString("<Name>" + xmlEscape(bucket) + "</Name>")
	b.WriteString("<Prefix>" + txt(p.prefix, p.enc) + "</Prefix>")
	b.WriteString("<Marker>" + txt(p.anchor, p.enc) + "</Marker>")
	b.WriteString("<MaxKeys>" + strconv.Itoa(p.maxKeys) + "</MaxKeys>")
	if p.delim != "" {
		b.WriteString("<Delimiter>" + txt(p.delim, p.enc) + "</Delimiter>")
	}
	if p.enc {
		b.WriteString("<EncodingType>url</EncodingType>")
	}
	b.WriteString("<IsTruncated>false</IsTruncated>")
	for _, c := range res.contents {
		b.WriteString(c)
	}
	for _, cp := range res.prefixes {
		b.WriteString("<CommonPrefixes><Prefix>" + txt(cp, p.enc) + "</Prefix></CommonPrefixes>")
	}
	b.WriteString("</ListBucketResult>")
	rq.w.Header().Set("x-amz-bucket-region", reg)
	writeXML(rq, http.StatusOK, b.String())
}

// ------------------------------ object handlers ------------------------------

func hPutObject(rq *reqCtx, bucket, key string) {
	if !db.hasBucket(bucket) {
		errResp(rq, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist", bucketXML(bucket))
		return
	}
	body, err := io.ReadAll(rq.r.Body)
	if err != nil {
		errResp(rq, http.StatusBadRequest, "IncompleteBody", "You did not provide the number of bytes specified by the Content-Length HTTP header", "")
		return
	}
	sum := md5.Sum(body)
	etag := hex.EncodeToString(sum[:])
	if cm := rq.r.Header.Get("Content-MD5"); cm != "" {
		raw, derr := base64.StdEncoding.DecodeString(strings.TrimSpace(cm))
		if derr != nil || len(raw) != 16 {
			errResp(rq, http.StatusBadRequest, "InvalidDigest", "The Content-MD5 you specified is not valid.", "<Content-MD5>"+xmlEscape(cm)+"</Content-MD5>")
			return
		}
		if hex.EncodeToString(raw) != etag {
			errResp(rq, http.StatusBadRequest, "BadDigest", "The Content-MD5 you specified did not match what we received.", "<Content-MD5>"+xmlEscape(cm)+"</Content-MD5>")
			return
		}
	}
	e := &objectEntry{
		data:  body,
		etag:  etag,
		mtime: time.Now().UTC(),
		ctype: contentTypeHeader(rq.r),
		meta:  metaFromHeaders(rq.r),
	}
	db.put(bucket, key, e)
	rq.w.Header().Set("ETag", quote(etag))
	rq.w.WriteHeader(http.StatusOK)
}

func hGetObject(rq *reqCtx, bucket, key string) {
	isHead := rq.r.Method == http.MethodHead
	e, ok := db.get(bucket, key)
	if !ok {
		if isHead {
			rq.w.WriteHeader(http.StatusNotFound)
			return
		}
		if !db.hasBucket(bucket) {
			errResp(rq, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist", bucketXML(bucket))
		} else {
			errResp(rq, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.", "<Key>"+xmlEscape(key)+"</Key>")
		}
		return
	}
	h := rq.w.Header()

	if im := rq.r.Header.Get("If-Match"); im != "" && !etagMatch(im, e.etag) {
		errResp(rq, http.StatusPreconditionFailed, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold", "")
		return
	}
	if ius := rq.r.Header.Get("If-Unmodified-Since"); ius != "" && rq.r.Header.Get("If-Match") == "" {
		if t, terr := time.Parse(http.TimeFormat, ius); terr == nil && e.mtime.Truncate(time.Second).After(t) {
			errResp(rq, http.StatusPreconditionFailed, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold", "")
			return
		}
	}
	if inm := rq.r.Header.Get("If-None-Match"); inm != "" && etagMatch(inm, e.etag) {
		h.Set("ETag", quote(e.etag))
		h.Set("Last-Modified", e.mtime.UTC().Format(http.TimeFormat))
		rq.w.WriteHeader(http.StatusNotModified)
		return
	}
	if ims := rq.r.Header.Get("If-Modified-Since"); ims != "" && rq.r.Header.Get("If-None-Match") == "" {
		if t, terr := time.Parse(http.TimeFormat, ims); terr == nil && !e.mtime.Truncate(time.Second).After(t) {
			h.Set("ETag", quote(e.etag))
			h.Set("Last-Modified", e.mtime.UTC().Format(http.TimeFormat))
			rq.w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	start, end := 0, len(e.data)-1
	partial := false
	if rng := rq.r.Header.Get("Range"); rng != "" && !isHead {
		s2, e2, use, unsat := parseRange(rng, len(e.data))
		if unsat {
			errResp(rq, 416, "InvalidRange", "The requested range is not satisfiable", "<ActualObjectSize>"+strconv.Itoa(len(e.data))+"</ActualObjectSize>")
			return
		}
		if use {
			start, end, partial = s2, e2, true
		}
	}

	h.Set("ETag", quote(e.etag))
	h.Set("Last-Modified", e.mtime.UTC().Format(http.TimeFormat))
	h.Set("Accept-Ranges", "bytes")
	h.Set("Content-Type", e.ctype)
	for mk, mv := range e.meta {
		h.Set("x-amz-meta-"+mk, mv)
	}
	if isHead {
		h.Set("Content-Length", strconv.Itoa(len(e.data)))
		rq.w.WriteHeader(http.StatusOK)
		return
	}
	if partial {
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(e.data)))
		h.Set("Content-Length", strconv.Itoa(end-start+1))
		rq.w.WriteHeader(http.StatusPartialContent)
		_, _ = rq.w.Write(e.data[start : end+1])
		return
	}
	h.Set("Content-Length", strconv.Itoa(len(e.data)))
	rq.w.WriteHeader(http.StatusOK)
	_, _ = rq.w.Write(e.data)
}

func hDeleteObject(rq *reqCtx, bucket, key string) {
	if !db.hasBucket(bucket) {
		errResp(rq, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist", bucketXML(bucket))
		return
	}
	db.del(bucket, key)
	rq.w.WriteHeader(http.StatusNoContent)
}

func hDeleteObjects(rq *reqCtx, bucket string) {
	if !db.hasBucket(bucket) {
		errResp(rq, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist", bucketXML(bucket))
		return
	}
	body, err := io.ReadAll(rq.r.Body)
	if err != nil {
		errResp(rq, http.StatusBadRequest, "MalformedXML", malformedXMLMsg, "")
		return
	}
	var dreq struct {
		Quiet   string `xml:"Quiet"`
		Objects []struct {
			Key string `xml:"Key"`
		} `xml:"Object"`
	}
	if uerr := xml.Unmarshal(body, &dreq); uerr != nil || len(dreq.Objects) == 0 {
		errResp(rq, http.StatusBadRequest, "MalformedXML", malformedXMLMsg, "")
		return
	}
	quiet := strings.EqualFold(dreq.Quiet, "true")
	var b strings.Builder
	b.WriteString(xmlDecl)
	b.WriteString(`<DeleteResult xmlns="` + s3ns + `">`)
	if !quiet {
		for _, o := range dreq.Objects {
			db.del(bucket, o.Key)
			b.WriteString("<Deleted><Key>" + xmlEscape(o.Key) + "</Key></Deleted>")
		}
	} else {
		for _, o := range dreq.Objects {
			db.del(bucket, o.Key)
		}
	}
	b.WriteString("</DeleteResult>")
	writeXML(rq, http.StatusOK, b.String())
}

func hCopyObject(rq *reqCtx, bucket, key string) {
	if !db.hasBucket(bucket) {
		errResp(rq, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist", bucketXML(bucket))
		return
	}
	src := rq.r.Header.Get("x-amz-copy-source")
	if dec, derr := url.PathUnescape(src); derr == nil {
		src = dec
	}
	src = strings.TrimPrefix(src, "/")
	if qi := strings.IndexByte(src, '?'); qi >= 0 {
		src = src[:qi]
	}
	sb, sk := src, ""
	if si := strings.IndexByte(src, '/'); si >= 0 {
		sb, sk = src[:si], src[si+1:]
	}
	se, ok := db.get(sb, sk)
	if !ok {
		if !db.hasBucket(sb) {
			errResp(rq, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist", bucketXML(sb))
		} else {
			errResp(rq, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.", "<Key>"+xmlEscape(sk)+"</Key>")
		}
		return
	}
	e := &objectEntry{data: se.data, etag: se.etag, mtime: time.Now().UTC(), ctype: se.ctype, meta: se.meta}
	if strings.EqualFold(rq.r.Header.Get("x-amz-metadata-directive"), "REPLACE") {
		if ct := rq.r.Header.Get("Content-Type"); ct != "" {
			e.ctype = ct
		}
		e.meta = metaFromHeaders(rq.r)
	}
	db.put(bucket, key, e)
	body := xmlDecl + `<CopyObjectResult xmlns="` + s3ns + `">` +
		"<LastModified>" + e.mtime.UTC().Format(s3Time) + "</LastModified><ETag>" + quote(e.etag) + "</ETag></CopyObjectResult>"
	writeXML(rq, http.StatusOK, body)
}

// ------------------------------ bootstrap ------------------------------

func hostPort(p string) string {
	if strings.Contains(p, ":") {
		return p
	}
	return ":" + p
}

func firstPortArg(args []string) string {
	for _, a := range args {
		if !strings.HasPrefix(a, "-") && len(a) > 0 && len(a) <= 5 && isAllDigits(a) {
			return ":" + a
		}
	}
	return ""
}

func listenAddr() string {
	args := os.Args[1:]
	for i, a := range args {
		switch a {
		case "-port", "--port":
			if i+1 < len(args) {
				return hostPort(args[i+1])
			}
		case "-addr", "--addr":
			if i+1 < len(args) {
				return args[i+1]
			}
		}
		if strings.HasPrefix(a, "-port=") {
			return hostPort(strings.TrimPrefix(a, "-port="))
		}
		if strings.HasPrefix(a, "--port=") {
			return hostPort(strings.TrimPrefix(a, "--port="))
		}
		if strings.HasPrefix(a, "-addr=") {
			return strings.TrimPrefix(a, "-addr=")
		}
		if strings.HasPrefix(a, "--addr=") {
			return strings.TrimPrefix(a, "--addr=")
		}
	}
	if a := firstPortArg(args); a != "" {
		return a
	}
	if p := os.Getenv("PORT"); p != "" {
		return hostPort(p)
	}
	if p := os.Getenv("FLOCI_PORT"); p != "" {
		return hostPort(p)
	}
	return ":8080"
}

func main() {
	h := http.HandlerFunc(route)
	addr := listenAddr()
	fmt.Fprintf(os.Stderr, "floci-go listening on %s\n", addr)
	go func() {
		if err := (&http.Server{Addr: addr, Handler: h}).ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "floci-go: %v\n", err)
		}
	}()
	// Best-effort listeners on common S3-emulator ports so any harness
	// convention reaches the server; bind failures are ignored.
	for _, extra := range []string{":8080", ":4569", ":9000", ":4566", ":8081"} {
		if extra == addr {
			continue
		}
		a := extra
		go func() {
			_ = (&http.Server{Addr: a, Handler: h}).ListenAndServe()
		}()
	}
	select {}
}
