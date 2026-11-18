package s3

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// objectLevel dispatches key-scoped operations.
func (c *reqCtx) objectLevel() {
	q := c.r.URL.Query()
	switch c.r.Method {
	case http.MethodGet:
		switch {
		case q.Has("uploadId"):
			c.listPartsOp()
		case q.Has("acl"):
			c.objectACL()
		case q.Has("tagging"):
			c.objectTagging()
		default:
			c.getObject(false)
		}
	case http.MethodHead:
		c.getObject(true)
	case http.MethodPut:
		switch {
		case q.Has("tagging"):
			c.putObjectTagging()
		case q.Has("acl"):
			c.putObjectACL()
		case q.Has("uploadId"):
			if q.Get("partNumber") == "" {
				writeError(c.w, c.r, s3err(400, "InvalidArgument", "The upload ID and part number must be specified together."))
				return
			}
			if c.r.Header.Get("x-amz-copy-source") != "" {
				c.uploadPartCopy()
			} else {
				c.uploadPart()
			}
		case c.r.Header.Get("x-amz-copy-source") != "":
			c.copyObject()
		default:
			c.putObject()
		}
	case http.MethodDelete:
		switch {
		case q.Has("tagging"):
			c.deleteObjectTagging()
		case q.Has("uploadId"):
			c.abortUpload()
		default:
			c.deleteObject()
		}
	case http.MethodPost:
		switch {
		case q.Has("uploads"):
			c.initiateMultipart()
		case q.Has("uploadId"):
			c.completeMultipart()
		default:
			c.default405()
		}
	default:
		c.default405()
	}
}

// putObjectACL acknowledges PutObjectAcl requests.
func (c *reqCtx) putObjectACL() {
	if _, err := c.rt.store().GetObject(c.bucket, c.key); err != nil {
		writeError(c.w, c.r, asS3Error(err))
		return
	}
	_, _ = io.Copy(io.Discard, c.r.Body)
	writeEmpty(c.w, http.StatusOK)
}

// getObject answers GET/HEAD /bucket/key, including ranges, conditionals,
// GetObject part selection and response-header overrides.
func (c *reqCtx) getObject(head bool) {
	q := c.r.URL.Query()
	obj, err := c.rt.store().GetObject(c.bucket, c.key)
	if err != nil {
		writeError(c.w, c.r, asS3Error(err))
		return
	}
	switch code := evalConditionals(c.r, obj); code {
	case http.StatusNotModified:
		h := c.w.Header()
		h.Set("ETag", obj.ETag)
		h.Set("Last-Modified", obj.LastModified.UTC().Format(http.TimeFormat))
		c.w.WriteHeader(http.StatusNotModified)
		return
	case http.StatusPreconditionFailed:
		writeError(c.w, c.r, s3err(412, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold"))
		return
	}

	h := c.w.Header()
	setObjectHeaders(h, obj)
	h.Set("Accept-Ranges", "bytes")
	headerOverrides(h, q)

	body := obj.Data
	status := http.StatusOK
	if pn := q.Get("partNumber"); pn != "" && len(obj.PartSizes) > 0 {
		n, perr := strconv.Atoi(pn)
		if perr != nil || n < 1 || n > len(obj.PartSizes) {
			writeError(c.w, c.r, s3err(416, "InvalidPartNumber", "The requested partnumber is not satisfiable"))
			return
		}
		start := int64(0)
		for i := 0; i < n-1; i++ {
			start += obj.PartSizes[i]
		}
		end := start + obj.PartSizes[n-1] - 1
		body = body[start : end+1]
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, obj.Size()))
		h.Set("x-amz-mp-parts-count", strconv.Itoa(len(obj.PartSizes)))
		status = http.StatusPartialContent
	} else if rg := c.r.Header.Get("Range"); rg != "" {
		start, end, unsat, ok := parseRange(rg, obj.Size())
		if unsat {
			writeError(c.w, c.r, s3err(416, "InvalidRange", "The requested range is not satisfiable"))
			return
		}
		if ok {
			body = body[start : end+1]
			h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, obj.Size()))
			status = http.StatusPartialContent
		}
	}
	h.Set("Content-Length", strconv.Itoa(len(body)))
	c.w.WriteHeader(status)
	if !head {
		_, _ = c.w.Write(body)
	}
}

// evalConditionals applies If-Match/If-None-Match/If-(Un)Modified-Since.
// Returns 0 (proceed), 304 or 412.
func evalConditionals(r *http.Request, obj *Object) int {
	etag := obj.ETag
	mod := obj.LastModified
	if im := r.Header.Get("If-Match"); im != "" {
		if !etagListMatches(im, etag) {
			return http.StatusPreconditionFailed
		}
	} else if ius := r.Header.Get("If-Unmodified-Since"); ius != "" {
		if t, err := http.ParseTime(ius); err == nil && mod.After(t) {
			return http.StatusPreconditionFailed
		}
	}
	if inm := r.Header.Get("If-None-Match"); inm != "" {
		if etagListMatches(inm, etag) {
			return http.StatusNotModified
		}
	} else if ims := r.Header.Get("If-Modified-Since"); ims != "" {
		if t, err := http.ParseTime(ims); err == nil && !mod.After(t) {
			return http.StatusNotModified
		}
	}
	return 0
}

func etagListMatches(list, etag string) bool {
	for _, cand := range strings.Split(list, ",") {
		cand = strings.TrimSpace(cand)
		if cand == "*" || normETag(cand) == normETag(etag) {
			return true
		}
	}
	return false
}

// setObjectHeaders writes the stored metadata of obj as response headers.
func setObjectHeaders(h http.Header, obj *Object) {
	ct := obj.ContentType
	if ct == "" {
		ct = "binary/octet-stream"
	}
	h.Set("Content-Type", ct)
	h.Set("ETag", obj.ETag)
	h.Set("Last-Modified", obj.LastModified.UTC().Format(http.TimeFormat))
	if obj.CacheControl != "" {
		h.Set("Cache-Control", obj.CacheControl)
	}
	if obj.ContentEncoding != "" {
		h.Set("Content-Encoding", obj.ContentEncoding)
	}
	if obj.ContentLanguage != "" {
		h.Set("Content-Language", obj.ContentLanguage)
	}
	if obj.ContentDisposition != "" {
		h.Set("Content-Disposition", obj.ContentDisposition)
	}
	if obj.Expires != "" {
		h.Set("Expires", obj.Expires)
	}
	for k, v := range obj.Metadata {
		if k == "x-amz-website-redirect-location" {
			h.Set("x-amz-website-redirect-location", v)
			continue
		}
		h.Set("X-Amz-Meta-"+k, v)
	}
	if sc := obj.StorageClass; sc != "" && sc != "STANDARD" {
		h.Set("x-amz-storage-class", sc)
	}
}

var responseOverrides = map[string]string{
	"response-content-type":        "Content-Type",
	"response-content-language":    "Content-Language",
	"response-expires":             "Expires",
	"response-cache-control":       "Cache-Control",
	"response-content-disposition": "Content-Disposition",
	"response-content-encoding":    "Content-Encoding",
}

func headerOverrides(h http.Header, q url.Values) {
	for param, header := range responseOverrides {
		if v := q.Get(param); v != "" {
			h.Set(header, v)
		}
	}
}

// parseRange interprets a single-part bytes= range. unsat: syntactically
// valid but empty (416); ok: serve the slice [start,end]; !ok: ignore.
func parseRange(spec string, size int64) (start, end int64, unsat, ok bool) {
	spec = strings.TrimSpace(spec)
	if !strings.HasPrefix(spec, "bytes=") {
		return 0, 0, false, false
	}
	spec = spec[len("bytes="):]
	if strings.Contains(spec, ",") {
		return 0, 0, false, false
	}
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return 0, 0, false, false
	}
	first, second := strings.TrimSpace(spec[:dash]), strings.TrimSpace(spec[dash+1:])
	var s, e int64
	if first == "" {
		n, err := strconv.ParseInt(second, 10, 64)
		if err != nil {
			return 0, 0, false, false
		}
		if n <= 0 || size == 0 {
			return 0, 0, true, true
		}
		s = size - n
		if s < 0 {
			s = 0
		}
		e = size - 1
	} else {
		v, err := strconv.ParseInt(first, 10, 64)
		if err != nil || v < 0 {
			return 0, 0, false, false
		}
		s = v
		if second == "" {
			e = size - 1
		} else {
			v2, err2 := strconv.ParseInt(second, 10, 64)
			if err2 != nil || v2 < s {
				return 0, 0, false, false
			}
			e = v2
		}
		if e >= size {
			e = size - 1
		}
		if s >= size {
			return 0, 0, true, true
		}
	}
	return s, e, false, true
}

// putObject stores the request body as a new object.
func (c *reqCtx) putObject() {
	hdr := c.r.Header
	streaming := streamingRequest(hdr)
	data, err := readBodyData(c.r)
	if err != nil {
		writeError(c.w, c.r, s3err(400, "IncompleteBody", "The request body could not be read"))
		return
	}
	if cmd5 := hdr.Get("Content-Md5"); cmd5 != "" && !streaming {
		want, derr := base64.StdEncoding.DecodeString(cmd5)
		sum := md5.Sum(data)
		if derr != nil || !bytes.Equal(want, sum[:]) {
			writeError(c.w, c.r, s3err(400, "BadDigest", "The Content-MD5 you specified did not match what we received."))
			return
		}
	}
	obj := &Object{
		Key:                c.key,
		Data:               data,
		ContentType:        defaultIfEmpty(hdr.Get("Content-Type"), "binary/octet-stream"),
		ContentEncoding:    stripChunkedEncoding(hdr.Get("Content-Encoding")),
		ContentLanguage:    hdr.Get("Content-Language"),
		ContentDisposition: hdr.Get("Content-Disposition"),
		CacheControl:       hdr.Get("Cache-Control"),
		Expires:            hdr.Get("Expires"),
		StorageClass:       hdr.Get("x-amz-storage-class"),
		Metadata:           collectMetadata(hdr),
		Tags:               parseTagging(hdr.Get("x-amz-tagging")),
	}
	if v := hdr.Get("x-amz-website-redirect-location"); v != "" {
		obj.Metadata["x-amz-website-redirect-location"] = v
	}
	if perr := c.rt.store().PutObject(c.bucket, obj); perr != nil {
		writeError(c.w, c.r, asS3Error(perr))
		return
	}
	c.w.Header().Set("ETag", obj.ETag)
	writeEmpty(c.w, http.StatusOK)
}

// stripChunkedEncoding drops the aws-chunked transfer marker.
func stripChunkedEncoding(raw string) string {
	if !strings.Contains(raw, "aws-chunked") {
		return raw
	}
	kept := make([]string, 0, 2)
	for _, tok := range strings.Split(raw, ",") {
		if tok = strings.TrimSpace(tok); tok != "" && tok != "aws-chunked" {
			kept = append(kept, tok)
		}
	}
	return strings.Join(kept, ",")
}

// deleteObject answers DELETE /bucket/key (idempotent, like S3).
func (c *reqCtx) deleteObject() {
	if _, err := c.rt.store().DeleteObject(c.bucket, c.key); err != nil {
		writeError(c.w, c.r, asS3Error(err))
		return
	}
	writeEmpty(c.w, http.StatusNoContent)
}

// deleteObjectTagging clears the tags of an object.
func (c *reqCtx) deleteObjectTagging() {
	if err := c.rt.store().SetObjectTags(c.bucket, c.key, nil); err != nil {
		writeError(c.w, c.r, asS3Error(err))
		return
	}
	writeEmpty(c.w, http.StatusNoContent)
}

// objectTagging answers GET /bucket/key?tagging.
func (c *reqCtx) objectTagging() {
	obj, err := c.rt.store().GetObject(c.bucket, c.key)
	if err != nil {
		writeError(c.w, c.r, asS3Error(err))
		return
	}
	if len(obj.Tags) == 0 {
		writeError(c.w, c.r, s3err(404, "NoSuchTagSet", "The TagSet does not exist"))
		return
	}
	writeXML(c.w, http.StatusOK, tagListToXML(obj.Tags))
}

// putObjectTagging replaces the tags of an object from an XML body.
func (c *reqCtx) putObjectTagging() {
	body, err := readBodyData(c.r)
	if err != nil || len(body) == 0 {
		writeError(c.w, c.r, s3err(400, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema"))
		return
	}
	var doc xmlTagging
	if uerr := xml.Unmarshal(body, &doc); uerr != nil {
		writeError(c.w, c.r, s3err(400, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema"))
		return
	}
	tags := map[string]string{}
	for _, t := range doc.TagSet {
		tags[t.Key] = t.Value
	}
	if serr := c.rt.store().SetObjectTags(c.bucket, c.key, tags); serr != nil {
		writeError(c.w, c.r, asS3Error(serr))
		return
	}
	writeEmpty(c.w, http.StatusOK)
}

// objectACL answers GET /bucket/key?acl.
func (c *reqCtx) objectACL() {
	if _, err := c.rt.store().GetObject(c.bucket, c.key); err != nil {
		writeError(c.w, c.r, asS3Error(err))
		return
	}
	writeXML(c.w, http.StatusOK, aclDocument())
}

// copyObject implements PUT with x-amz-copy-source.
func (c *reqCtx) copyObject() {
	srcBucket, srcKey, perr := parseCopySource(c.r.Header.Get("x-amz-copy-source"))
	if perr != nil {
		writeError(c.w, c.r, perr)
		return
	}
	src, err := c.rt.store().GetObject(srcBucket, srcKey)
	if err != nil {
		writeError(c.w, c.r, asS3Error(err))
		return
	}
	hdr := c.r.Header
	directive := strings.ToUpper(hdr.Get("x-amz-metadata-directive"))
	if directive == "" {
		directive = "COPY"
	}
	if directive != "COPY" && directive != "REPLACE" {
		writeError(c.w, c.r, s3err(400, "InvalidArgument", "Unknown metadata directive."))
		return
	}
	if srcBucket == c.bucket && srcKey == c.key && directive == "COPY" {
		writeError(c.w, c.r, s3err(400, "InvalidRequest", "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes."))
		return
	}
	obj := &Object{
		Key:                c.key,
		Data:               bytes.Clone(src.Data),
		ETag:               src.ETag,
		ContentType:        src.ContentType,
		ContentEncoding:    src.ContentEncoding,
		ContentLanguage:    src.ContentLanguage,
		ContentDisposition: src.ContentDisposition,
		CacheControl:       src.CacheControl,
		Expires:            src.Expires,
		StorageClass:       src.StorageClass,
		Metadata:           copyTags(src.Metadata),
		Tags:               copyTags(src.Tags),
	}
	if directive == "REPLACE" {
		obj.ContentType = defaultIfEmpty(hdr.Get("Content-Type"), "binary/octet-stream")
		obj.ContentEncoding = stripChunkedEncoding(hdr.Get("Content-Encoding"))
		obj.ContentLanguage = hdr.Get("Content-Language")
		obj.ContentDisposition = hdr.Get("Content-Disposition")
		obj.CacheControl = hdr.Get("Cache-Control")
		obj.Expires = hdr.Get("Expires")
		obj.Metadata = collectMetadata(hdr)
	}
	if sc := hdr.Get("x-amz-storage-class"); sc != "" {
		obj.StorageClass = sc
	}
	if perr := c.rt.store().PutObject(c.bucket, obj); perr != nil {
		writeError(c.w, c.r, asS3Error(perr))
		return
	}
	writeXML(c.w, http.StatusOK, xmlCopyObject{
		ETag:         obj.ETag,
		LastModified: s3Time(obj.LastModified),
	})
}

// parseCopySource splits bucket/key out of an x-amz-copy-source header.
func parseCopySource(raw string) (string, string, *s3Error) {
	if i := strings.IndexByte(raw, '?'); i >= 0 {
		raw = raw[:i]
	}
	if decoded, err := url.PathUnescape(raw); err == nil {
		raw = decoded
	}
	raw = strings.TrimPrefix(raw, "/")
	bucket, key, ok := strings.Cut(raw, "/")
	if !ok || bucket == "" || key == "" {
		return "", "", s3err(400, "InvalidArgument", "Copy Source must mention the source bucket and key: sourcebucket/sourcekey")
	}
	return bucket, key, nil
}

// collectMetadata gathers x-amz-meta-* headers (keys lowercased).
func collectMetadata(h http.Header) map[string]string {
	m := map[string]string{}
	for name, vals := range h {
		ln := strings.ToLower(name)
		if !strings.HasPrefix(ln, "x-amz-meta-") || len(vals) == 0 {
			continue
		}
		m[strings.TrimPrefix(ln, "x-amz-meta-")] = strings.TrimSpace(vals[0])
	}
	return m
}

// parseTagging decodes the x-amz-tagging URL form into a tag map.
func parseTagging(raw string) map[string]string {
	tags := map[string]string{}
	if raw == "" {
		return tags
	}
	for _, pair := range strings.Split(raw, "&") {
		if pair == "" {
			continue
		}
		k, v, _ := strings.Cut(pair, "=")
		key, err1 := url.QueryUnescape(k)
		val, err2 := url.QueryUnescape(v)
		if err1 != nil || err2 != nil || key == "" {
			continue
		}
		tags[key] = val
	}
	return tags
}

// streamingRequest reports whether the body uses sigv4 chunked framing.
func streamingRequest(h http.Header) bool {
	return strings.HasPrefix(h.Get("x-amz-content-sha256"), "STREAMING")
}

// readBodyData reads the request body, decoding aws-chunked when signalled.
func readBodyData(r *http.Request) ([]byte, error) {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	if streamingRequest(r.Header) || strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") {
		b = decodeAWSChunked(b)
	}
	return b, nil
}

// decodeAWSChunked strips the aws-chunked framing (signatures and trailers).
func decodeAWSChunked(body []byte) []byte {
	out := make([]byte, 0, len(body))
	i := 0
	for i < len(body) {
		nl := bytes.IndexByte(body[i:], '\n')
		if nl < 0 {
			break
		}
		line := strings.TrimSpace(string(body[i : i+nl]))
		i += nl + 1
		if idx := strings.IndexByte(line, ';'); idx >= 0 {
			line = line[:idx]
		}
		size, err := strconv.ParseInt(strings.TrimSpace(line), 16, 64)
		if err != nil || size < 0 {
			break
		}
		if size == 0 {
			break // trailing signatures or checksum trailers follow
		}
		if i+int(size) > len(body) {
			out = append(out, body[i:]...)
			break
		}
		out = append(out, body[i:i+int(size)]...)
		i += int(size)
		if i < len(body) && body[i] == '\r' {
			i++
		}
		if i < len(body) && body[i] == '\n' {
			i++
		}
	}
	return out
}

func defaultIfEmpty(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}
