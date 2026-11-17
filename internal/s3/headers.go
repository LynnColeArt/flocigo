package s3

import (
    "net/http"
    "strings"
    "time"
)

// preservedHeaders is the set of request headers persisted as object system
// metadata and replayed on GET/HEAD, matching S3's system metadata behavior.
var preservedHeaders = map[string]bool{
    "content-type":                 true,
    "cache-control":                true,
    "content-disposition":          true,
    "content-encoding":             true,
    "content-language":             true,
    "expires":                      true,
    "x-amz-server-side-encryption": true,
}

// extractMetadata collects object metadata from request headers: preserved
// system headers plus user metadata shipped under x-amz-meta-*.
func extractMetadata(r *http.Request) map[string]string {
    meta := map[string]string{}
    for name, values := range r.Header {
        lower := strings.ToLower(name)
        if preservedHeaders[lower] || strings.HasPrefix(lower, "x-amz-meta-") {
            meta[lower] = strings.Join(values, ",")
        }
    }
    return meta
}

// checkCopyConditions evaluates the x-amz-copy-source-if-* preconditions
// used by CopyObject and UploadPartCopy.
func checkCopyConditions(r *http.Request, etag string, modified time.Time) *Error {
    if v := r.Header.Get("x-amz-copy-source-if-match"); v != "" && !matchETags(v, etag) {
        return errPreconditionFailed()
    }
    if v := r.Header.Get("x-amz-copy-source-if-none-match"); v != "" && matchETags(v, etag) {
        return errPreconditionFailed()
    }
    modified = modified.UTC().Truncate(time.Second)
    if v := r.Header.Get("x-amz-copy-source-if-unmodified-since"); v != "" {
        if t, err := http.ParseTime(v); err == nil && modified.After(t) {
            return errPreconditionFailed()
        }
    }
    if v := r.Header.Get("x-amz-copy-source-if-modified-since"); v != "" {
        if t, err := http.ParseTime(v); err == nil && !modified.After(t) {
            return errPreconditionFailed()
        }
    }
    return nil
}
