package s3

import (
    "fmt"
    "io"
    "net/http"
    "net/url"
    "strconv"
    "strings"
    "time"
)

// responseHeaderOverrides maps GetObject query overrides onto headers.
var responseHeaderOverrides = [][2]string{
    {"response-content-type", "Content-Type"},
    {"response-content-language", "Content-Language"},
    {"response-expires", "Expires"},
    {"response-cache-control", "Cache-Control"},
    {"response-content-disposition", "Content-Disposition"},
    {"response-content-encoding", "Content-Encoding"},
}

// defaultContentType matches S3's default for PUTs without Content-Type.
func defaultContentType(meta map[string]string) map[string]string {
    if _, ok := meta["content-type"]; !ok {
        meta["content-type"] = "binary/octet-stream"
    }
    return meta
}

func (s *Server) handlePutObject(rc *requestCtx) {
    body, err := io.ReadAll(rc.r.Body)
    if err != nil {
        rc.fail(errInvalidArgument("unable to read request body"))
        return
    }
    meta := defaultContentType(extractMetadata(rc.r))
    info, perr := s.store.PutObject(rc.bucket, rc.key, body, meta)
    if perr != nil {
        rc.fail(perr)
        return
    }
    h := rc.w.Header()
    h.Set("ETag", quoteETag(info.ETag))
    // Echo stored x-amz system headers (x-amz-meta-* go out on GET only).
    for k, v := range info.Metadata {
        if strings.HasPrefix(k, "x-amz-") && !strings.HasPrefix(k, "x-amz-meta-") {
            h.Set(k, v)
        }
    }
    rc.w.WriteHeader(http.StatusOK)
}

// handleCopyObject implements PUT with x-amz-copy-source, including the
// x-amz-metadata-directive and x-amz-copy-source-if-* header semantics.
func (s *Server) handleCopyObject(rc *requestCtx) {
    srcBucket, srcKey := parseCopySource(rc.r.Header.Get("x-amz-copy-source"))
    if srcBucket == "" || srcKey == "" {
        rc.fail(errInvalidArgument("Invalid x-amz-copy-source header"))
        return
    }
    info, data, gerr := s.store.GetObject(srcBucket, srcKey)
    if gerr != nil {
        rc.failCtx(srcBucket, srcKey, gerr)
        return
    }
    if cerr := checkCopyConditions(rc.r, info.ETag, info.LastModified); cerr != nil {
        rc.fail(cerr)
        return
    }
    directive := strings.ToUpper(rc.r.Header.Get("x-amz-metadata-directive"))
    switch directive {
    case "", "COPY", "REPLACE":
    default:
        rc.fail(errInvalidArgument("Unknown metadata directive: %s", directive))
        return
    }
    if directive != "REPLACE" && srcBucket == rc.bucket && srcKey == rc.key {
        rc.fail(errf("InvalidRequest", http.StatusBadRequest,
            "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes."))
        return
    }
    meta := info.Metadata
    if directive == "REPLACE" {
        meta = defaultContentType(extractMetadata(rc.r))
    }
    dst, perr := s.store.PutObject(rc.bucket, rc.key, data, meta)
    if perr != nil {
        rc.fail(perr)
        return
    }
    rc.xml(http.StatusOK, CopyObjectResult{
        Xmlns:        s3Namespace,
        ETag:         quoteETag(dst.ETag),
        LastModified: s3t(dst.LastModified),
    })
}

func (s *Server) handleGetObject(rc *requestCtx, isHead bool) {
    info, data, err := s.store.GetObject(rc.bucket, rc.key)
    if err != nil {
        rc.fail(err)
        return
    }
    h := rc.w.Header()
    h.Set("ETag", quoteETag(info.ETag))
    h.Set("Last-Modified", info.LastModified.UTC().Format(rfc1123GMT))
    h.Set("Accept-Ranges", "bytes")
    for k, v := range info.Metadata {
        h.Set(k, v)
    }
    for _, o := range responseHeaderOverrides {
        if v := rc.r.URL.Query().Get(o[0]); v != "" {
            h.Set(o[1], v)
        }
    }
    if code := conditionalStatus(rc.r, info.ETag, info.LastModified); code != 0 {
        if code == http.StatusNotModified {
            // RFC 7232: 304 responses keep the validators.
            rc.w.WriteHeader(code)
            return
        }
        rc.fail(errPreconditionFailed())
        return
    }
    offset, length := int64(0), int64(len(data))
    status := http.StatusOK
    if rng := rc.r.Header.Get("Range"); rng != "" {
        start, rlen, ok, unsat := parseByteRange(rng, int64(len(data)))
        if unsat {
            rc.fail(errInvalidRange())
            return
        }
        if ok {
            status = http.StatusPartialContent
            offset, length = start, rlen
            h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, len(data)))
        }
    }
    h.Set("Content-Length", strconv.FormatInt(length, 10))
    rc.w.WriteHeader(status)
    if isHead || length == 0 {
        return
    }
    rc.w.Write(data[offset : offset+length])
}

func (s *Server) handleDeleteObject(rc *requestCtx) {
    if !s.store.BucketExists(rc.bucket) {
        rc.fail(errNoSuchBucket())
        return
    }
    if err := s.store.DeleteObject(rc.bucket, rc.key); err != nil {
        rc.fail(err)
        return
    }
    rc.w.WriteHeader(http.StatusNoContent)
}

// conditionalStatus applies If-Match / If-Unmodified-Since / If-None-Match /
// If-Modified-Since for GET and HEAD. Returns 0 to proceed.
func conditionalStatus(r *http.Request, etag string, modified time.Time) int {
    modified = modified.UTC().Truncate(time.Second)
    if v := r.Header.Get("If-Match"); v != "" {
        if !matchETags(v, etag) {
            return http.StatusPreconditionFailed
        }
        return 0
    }
    if v := r.Header.Get("If-Unmodified-Since"); v != "" {
        if t, err := http.ParseTime(v); err == nil && modified.After(t) {
            return http.StatusPreconditionFailed
        }
    }
    if v := r.Header.Get("If-None-Match"); v != "" {
        if matchETags(v, etag) {
            return http.StatusNotModified
        }
        return 0
    }
    if v := r.Header.Get("If-Modified-Since"); v != "" {
        if t, err := http.ParseTime(v); err == nil && !modified.After(t) {
            return http.StatusNotModified
        }
    }
    return 0
}

// matchETags reports whether an If-(None-)Match style header covers etag
// (stored unquoted). Handles lists, W/ weak markers and '*'.
func matchETags(header, etag string) bool {
    if strings.TrimSpace(header) == "*" {
        return true
    }
    for _, part := range strings.Split(header, ",") {
        part = strings.TrimPrefix(strings.TrimSpace(part), "W/")
        if unquoteETag(part) == etag {
            return true
        }
    }
    return false
}

// parseByteRange parses a single "bytes=" range against size. ok=false means
// the header is ignored (malformed or multiple ranges). unsat flags a
// well-formed but unsatisfiable range (HTTP 416).
func parseByteRange(spec string, size int64) (start, length int64, ok, unsat bool) {
    const prefix = "bytes="
    spec = strings.TrimSpace(spec)
    if !strings.HasPrefix(spec, prefix) {
        return 0, 0, false, false
    }
    spec = spec[len(prefix):]
    if strings.Contains(spec, ",") {
        return 0, 0, false, false
    }
    first, last, found := strings.Cut(spec, "-")
    if !found {
        return 0, 0, false, false
    }
    first, last = strings.TrimSpace(first), strings.TrimSpace(last)
    if first == "" {
        n, perr := strconv.ParseInt(last, 10, 64)
        if perr != nil || n < 0 {
            return 0, 0, false, false
        }
        if n == 0 || size == 0 {
            return 0, 0, false, true
        }
        if n > size {
            n = size
        }
        return size - n, n, true, false
    }
    start, perr := strconv.ParseInt(first, 10, 64)
    if perr != nil || start < 0 {
        return 0, 0, false, false
    }
    if start >= size {
        return 0, 0, false, true
    }
    if last == "" {
        return start, size - start, true, false
    }
    end, perr := strconv.ParseInt(last, 10, 64)
    if perr != nil || end < start {
        return 0, 0, false, false
    }
    if end >= size {
        end = size - 1
    }
    return start, end - start + 1, true, false
}

// parseCopySource decodes the x-amz-copy-source header: "/bucket/key",
// "bucket/key", URL-encoded, with an optional ?versionId suffix that is
// ignored (versioning is out of scope for the adapter).
func parseCopySource(v string) (bucket, key string) {
    v = strings.TrimSpace(v)
    if i := strings.IndexByte(v, '?'); i >= 0 {
        v = v[:i]
    }
    if dec, err := url.PathUnescape(v); err == nil {
        v = dec
    }
    v = strings.TrimPrefix(v, "/")
    bucket, key, _ = strings.Cut(v, "/")
    return
}
