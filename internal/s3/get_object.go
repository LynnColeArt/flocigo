// get_object.go serves GET and HEAD object requests (GET /{bucket}/{key}),
// returning the stored body plus ETag and Content-Length headers, with the
// NoSuchKey and NoSuchBucket error paths.

package s3

import (
    "net/http"
    "strconv"
)

// handleGetObject serves GET /{bucket}/{key}: the stored body plus ETag,
// Content-Length, Content-Type and Last-Modified headers. A missing bucket
// results in NoSuchBucket and a missing key in NoSuchKey.
func handleGetObject(w http.ResponseWriter, r *http.Request) {
    bucket, key := splitS3Path(r.URL.Path)
    if bucket == "" || key == "" {
        wireNoSuchKey(w, key)
        return
    }
    object, ok := wires.getObject(bucket, key)
    if !ok {
        wireObjectMiss(w, bucket, key)
        return
    }
    wireServeObject(w, r, object)
}

// handleHeadObject mirrors handleGetObject with an empty response body.
func handleHeadObject(w http.ResponseWriter, r *http.Request) {
    bucket, key := splitS3Path(r.URL.Path)
    if bucket == "" || key == "" {
        wireNoSuchKey(w, key)
        return
    }
    object, ok := wires.getObject(bucket, key)
    if !ok {
        wireObjectMiss(w, bucket, key)
        return
    }
    wireServeObject(w, r, object)
}

// handleGet is an alias of handleGetObject.
func handleGet(w http.ResponseWriter, r *http.Request) {
    handleGetObject(w, r)
}

// wireObjectMiss renders NoSuchBucket when the bucket is absent and
// NoSuchKey otherwise.
func wireObjectMiss(w http.ResponseWriter, bucket, key string) {
    if wires.lookupBucket(bucket) == nil {
        wireNoSuchBucket(w, bucket)
        return
    }
    wireNoSuchKey(w, key)
}

// wireServeObject writes the object response headers and body; HEAD requests
// receive the identical headers with no body.
func wireServeObject(w http.ResponseWriter, r *http.Request, object *wireObject) {
    header := w.Header()
    header.Set("ETag", wireQuotedETag(object.ETag))
    contentType := object.ContentType
    if contentType == "" {
        contentType = "binary/octet-stream"
    }
    header.Set("Content-Type", contentType)
    header.Set("Content-Length", strconv.Itoa(len(object.Body)))
    header.Set("Last-Modified", object.LastModified.UTC().Format(http.TimeFormat))
    for name, value := range object.Metadata {
        header.Set(name, value)
    }
    w.WriteHeader(http.StatusOK)
    if r.Method != http.MethodHead {
        _, _ = w.Write(object.Body)
    }
}
