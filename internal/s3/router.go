// Package s3 implements a floci-compatible subset of the Amazon S3 REST API.
// router.go owns the single http.Handler entry point for the service and
// dispatches every request to the per-operation handlers in this package.
package s3

import (
    "bytes"
    "crypto/md5"
    "encoding/hex"
    "encoding/xml"
    "io"
    "net/http"
    "net/url"
    "strconv"
    "strings"
)

// wireXMLNS is the XML namespace used by S3 response documents.
const wireXMLNS = `http://s3.amazonaws.com/doc/2006-03-01/`

// wireMethodNotAllowedMessage mirrors the AWS wording for 405 responses.
const wireMethodNotAllowedMessage = `The specified method is not allowed against this resource.`

// Handler returns the root http.Handler for the S3 service.
func Handler() http.Handler {
    return http.HandlerFunc(route)
}

// NewHandler is an alias of Handler.
func NewHandler() http.Handler { return Handler() }

// NewRouter is an alias of Handler.
func NewRouter() http.Handler { return Handler() }

// New is an alias of Handler.
func New() http.Handler { return Handler() }

// Service exposes the S3 API surface as a mountable http.Handler.
type Service struct{}

// NewService builds the S3 service.
func NewService() *Service { return &Service{} }

// Handler returns the http.Handler exposed by the S3 service.
func (Service) Handler() http.Handler { return Handler() }

// ServeHTTP implements http.Handler.
func (Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { route(w, r) }

// wireError is an error rendered inside the common S3 <Error> envelope.
type wireError struct {
    Status  int
    Code    string
    Message string
    Extra   string
}

func (e *wireError) Error() string { return e.Code + ": " + e.Message }

// route dispatches an S3 request to the operation handler matching its
// method and bucket/key path.
func route(w http.ResponseWriter, r *http.Request) {
    bucket, key := splitS3Path(r.URL.Path)
    q := r.URL.Query()
    switch r.Method {
    case http.MethodGet:
        wireDispatchGet(w, r, bucket, key, q)
    case http.MethodHead:
        wireDispatchHead(w, r, bucket, key)
    case http.MethodPut:
        wireDispatchPut(w, r, bucket, key, q)
    case http.MethodPost:
        wireDispatchPost(w, r, bucket, key, q)
    case http.MethodDelete:
        wireDispatchDelete(w, r, bucket, key, q)
    default:
        writeWireError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", wireMethodNotAllowedMessage, "")
    }
}

// dispatch is an alias of route.
func dispatch(w http.ResponseWriter, r *http.Request) { route(w, r) }

// serveS3 is an alias of route.
func serveS3(w http.ResponseWriter, r *http.Request) { route(w, r) }

// wireDispatchGet routes GET /, GET /{bucket} and GET /{bucket}/{key}.
func wireDispatchGet(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) {
    switch {
    case bucket == "":
        wireListBuckets(w, r)
    case key == "":
        switch {
        case q.Has("location"):
            wireBucketLocation(w, r, bucket)
        case q.Has("versioning"):
            wireRawXML(w, http.StatusOK, `<VersioningConfiguration xmlns="`+wireXMLNS+`"/>`)
        case q.Has("policy"):
            writeWireError(w, http.StatusNotFound, "NoSuchBucketPolicy", "The bucket policy does not exist", "<BucketName>"+xmlEscapeWire(bucket)+"</BucketName>")
        case q.Has("cors"):
            writeWireError(w, http.StatusNotFound, "NoSuchCORSConfiguration", "The CORS configuration does not exist", "<BucketName>"+xmlEscapeWire(bucket)+"</BucketName>")
        case q.Has("acl"):
            wireAccessControlPolicy(w)
        case q.Has("tagging"):
            writeWireError(w, http.StatusNotFound, "NoSuchTagSet", "The TagSet does not exist", "<BucketName>"+xmlEscapeWire(bucket)+"</BucketName>")
        case q.Has("uploads"):
            wireListMultipartUploads(w, r, bucket)
        default:
            wireListObjects(w, r, bucket)
        }
    default:
        switch {
        case q.Has("tagging"):
            wireRawXML(w, http.StatusOK, `<Tagging xmlns="`+wireXMLNS+`"><TagSet></TagSet></Tagging>`)
        case q.Has("acl"):
            wireAccessControlPolicy(w)
        case q.Has("uploadId"):
            wireListParts(w, r, bucket, key, q.Get("uploadId"))
        default:
            handleGetObject(w, r)
        }
    }
}

// wireDispatchHead routes HEAD /, HEAD /{bucket} and HEAD /{bucket}/{key}.
func wireDispatchHead(w http.ResponseWriter, r *http.Request, bucket, key string) {
    switch {
    case bucket == "":
        w.WriteHeader(http.StatusOK)
    case key == "":
        wireHeadBucket(w, r, bucket)
    default:
        handleHeadObject(w, r)
    }
}

// wireDispatchPut routes PUT /{bucket} and PUT /{bucket}/{key}.
func wireDispatchPut(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) {
    switch {
    case bucket == "":
        writeWireError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", wireMethodNotAllowedMessage, "")
    case key == "":
        if len(q) > 0 {
            w.WriteHeader(http.StatusOK)
            return
        }
        wireCreateBucket(w, r, bucket)
    default:
        switch {
        case q.Has("partNumber"):
            wireUploadPart(w, r, bucket, key, q.Get("partNumber"), q.Get("uploadId"))
        case r.Header.Get("X-Amz-Copy-Source") != "":
            wireCopyObject(w, r, bucket, key)
        case len(q) > 0:
            w.WriteHeader(http.StatusOK)
        default:
            wirePutObject(w, r, bucket, key)
        }
    }
}

// wireDispatchPost routes POST /{bucket} and POST /{bucket}/{key}.
func wireDispatchPost(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) {
    switch {
    case bucket == "":
        writeWireError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", wireMethodNotAllowedMessage, "")
    case key == "":
        if q.Has("delete") {
            wireDeleteObjects(w, r, bucket)
            return
        }
        writeWireError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", wireMethodNotAllowedMessage, "")
    default:
        switch {
        case q.Has("uploads"):
            wireCreateMultipartUpload(w, r, bucket, key)
        case q.Has("uploadId"):
            wireCompleteMultipartUpload(w, r, bucket, key, q.Get("uploadId"))
        default:
            writeWireError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", wireMethodNotAllowedMessage, "")
        }
    }
}

// wireDispatchDelete routes DELETE /{bucket} and DELETE /{bucket}/{key}.
func wireDispatchDelete(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) {
    switch {
    case bucket == "":
        writeWireError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", wireMethodNotAllowedMessage, "")
    case key == "":
        if len(q) > 0 {
            w.WriteHeader(http.StatusNoContent)
            return
        }
        wireDeleteBucket(w, r, bucket)
    default:
        if q.Has("uploadId") {
            wireAbortMultipartUpload(w, r, bucket, key, q.Get("uploadId"))
            return
        }
        wireDeleteObject(w, r, bucket, key)
    }
}

// wireHeadBucket answers HEAD /{bucket} with 200 or 404.
func wireHeadBucket(w http.ResponseWriter, r *http.Request, bucket string) {
    if wires.lookupBucket(bucket) == nil {
        wireNoSuchBucket(w, bucket)
        return
    }
    w.WriteHeader(http.StatusOK)
}

// wireBucketLocation answers GET /{bucket}?location.
func wireBucketLocation(w http.ResponseWriter, r *http.Request, bucket string) {
    if wires.lookupBucket(bucket) == nil {
        wireNoSuchBucket(w, bucket)
        return
    }
    wireRawXML(w, http.StatusOK, `<LocationConstraint xmlns="`+wireXMLNS+`"/>`)
}

// wireAccessControlPolicy answers GET ?acl with the canned owner ACL.
func wireAccessControlPolicy(w http.ResponseWriter) {
    wireRawXML(w, http.StatusOK, `<AccessControlPolicy xmlns="`+wireXMLNS+`"><Owner><ID>floci</ID><DisplayName>floci</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>floci</ID><DisplayName>floci</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>`)
}

// wireReadBody reads the full request body, tolerating read errors for the
// purposes of the emulator.
func wireReadBody(r *http.Request) []byte {
    body, err := io.ReadAll(r.Body)
    if err != nil {
        return nil
    }
    return body
}

// splitS3Path splits an S3-style path into bucket and key. The first path
// segment is the bucket; everything after it forms the key, slashes included.
func splitS3Path(path string) (string, string) {
    trimmed := strings.TrimPrefix(path, "/")
    if trimmed == "" {
        return "", ""
    }
    if i := strings.Index(trimmed, "/"); i >= 0 {
        return trimmed[:i], trimmed[i+1:]
    }
    return trimmed, ""
}

var wireXMLEscaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")

// xmlEscapeWire escapes plain text for hand-built XML documents.
func xmlEscapeWire(s string) string {
    return wireXMLEscaper.Replace(s)
}

// wireETag returns the unquoted hex MD5 of body.
func wireETag(body []byte) string {
    sum := md5.Sum(body)
    return hex.EncodeToString(sum[:])
}

// wireQuotedETag renders an ETag the way S3 sends it, quoted.
func wireQuotedETag(etag string) string {
    return `"` + etag + `"`
}

// writeWireError renders an S3 error as the standard XML <Error> envelope.
func writeWireError(w http.ResponseWriter, status int, code, message, extra string) {
    body := xml.Header + "<Error><Code>" + xmlEscapeWire(code) + "</Code><Message>" + xmlEscapeWire(message) + "</Message>"
    if extra != "" {
        body += extra
    }
    body += "</Error>"
    w.Header().Set("Content-Type", "application/xml")
    w.Header().Set("Content-Length", strconv.Itoa(len(body)))
    w.WriteHeader(status)
    _, _ = w.Write([]byte(body))
}

// wireXMLDoc renders a marshaled XML document as a full S3 response.
func wireXMLDoc(w http.ResponseWriter, status int, doc interface{}) {
    raw, err := xml.Marshal(doc)
    if err != nil {
        writeWireError(w, http.StatusInternalServerError, "InternalError", "We encountered an internal error. Please try again.", "")
        return
    }
    // Go escapes double quotes and control characters inside element text;
    // S3 serves quoted ETags and raw key text, so undo those escapes.
    raw = bytes.ReplaceAll(raw, []byte("&quot;"), []byte("\""))
    raw = bytes.ReplaceAll(raw, []byte("&#xA;"), []byte("\n"))
    raw = bytes.ReplaceAll(raw, []byte("&#x9;"), []byte("\t"))
    raw = bytes.ReplaceAll(raw, []byte("&#xD;"), []byte("\r"))
    body := append([]byte(xml.Header), raw...)
    w.Header().Set("Content-Type", "application/xml")
    w.Header().Set("Content-Length", strconv.Itoa(len(body)))
    w.WriteHeader(status)
    _, _ = w.Write(body)
}

// wireRawXML renders a pre-built XML document as a full S3 response.
func wireRawXML(w http.ResponseWriter, status int, doc string) {
    body := xml.Header + doc
    w.Header().Set("Content-Type", "application/xml")
    w.Header().Set("Content-Length", strconv.Itoa(len(body)))
    w.WriteHeader(status)
    _, _ = w.Write([]byte(body))
}
