package s3

import (
    "net/http"
    "strings"
)

// Server is the S3 protocol adapter: it speaks the S3 wire protocol
// (path-style routing, XML documents, presigned auth) and delegates state to
// a Store. Wire the server into any http.Handler mux to serve S3 requests.
type Server struct {
    store     Store
    region    string
    owner     Owner
    auth      Authenticator
    requestID func() string
    // OnRequest runs for every request before authentication and routing;
    // the kernel can use it for policy or observation hooks.
    OnRequest func(r *http.Request)
}

// Option configures a Server.
type Option func(*Server)

// New builds a Server over store. Without an authenticator the server
// accepts anonymous requests (dev mode); plug a V4Authenticator in for
// credential enforcement.
func New(store Store, opts ...Option) *Server {
    s := &Server{
        store:     store,
        region:    "us-east-1",
        owner:     Owner{ID: "floci", DisplayName: "floci"},
        requestID: func() string { return randomHex(8) },
    }
    for _, opt := range opts {
        opt(s)
    }
    return s
}

// WithRegion sets the default bucket location and the SigV4 scope check.
func WithRegion(region string) Option { return func(s *Server) { s.region = region } }

// WithOwner sets the canonical owner identity reported in listings.
func WithOwner(id, display string) Option {
    return func(s *Server) { s.owner = Owner{ID: id, DisplayName: display} }
}

// WithAuthenticator installs the authentication hook.
func WithAuthenticator(a Authenticator) Option { return func(s *Server) { s.auth = a } }

// ServeHTTP implements path-style S3 dispatch over /bucket and /bucket/key.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    rc := &requestCtx{
        server:    s,
        w:         w,
        r:         r,
        requestID: s.requestID(),
        hostID:    s.requestID(),
    }
    w.Header().Set("x-amz-request-id", rc.requestID)
    w.Header().Set("x-amz-id-2", rc.hostID)
    if s.OnRequest != nil {
        s.OnRequest(r)
    }
    if s.auth != nil {
        if err := s.auth.Authenticate(r); err != nil {
            rc.fail(err)
            return
        }
    }
    rc.bucket, rc.key = splitPath(r.URL.Path)
    s.dispatch(rc)
}

// splitPath splits a path-style S3 path into bucket and key.
func splitPath(p string) (bucket, key string) {
    p = strings.TrimPrefix(p, "/")
    if p == "" {
        return "", ""
    }
    if i := strings.IndexByte(p, '/'); i >= 0 {
        return p[:i], p[i+1:]
    }
    return p, ""
}

type requestCtx struct {
    server    *Server
    w         http.ResponseWriter
    r         *http.Request
    requestID string
    hostID    string
    bucket    string
    key       string
    uploadID  string
}

func (rc *requestCtx) fail(err error) {
    rc.server.writeError(rc.w, rc.r, rc.requestID, rc.hostID, err, rc.bucket, rc.key)
}

func (rc *requestCtx) failCtx(bucket, key string, err error) {
    rc.server.writeError(rc.w, rc.r, rc.requestID, rc.hostID, err, bucket, key)
}

func (rc *requestCtx) xml(status int, v any) {
    writeXML(rc.w, status, v)
}

func (s *Server) dispatch(rc *requestCtx) {
    switch {
    case rc.bucket == "":
        if rc.r.Method != http.MethodGet {
            rc.fail(errMethodNotAllowed())
            return
        }
        s.handleListBuckets(rc)
    case rc.key == "":
        s.dispatchBucket(rc)
    default:
        s.dispatchObject(rc)
    }
}

func (s *Server) dispatchBucket(rc *requestCtx) {
    q := rc.r.URL.Query()
    switch rc.r.Method {
    case http.MethodGet:
        switch {
        case q.Has("location"):
            s.handleBucketLocation(rc)
        case q.Has("uploads"):
            s.handleListUploads(rc)
        case q.Get("list-type") == "2":
            s.handleListObjectsV2(rc)
        default:
            s.handleListObjectsV1(rc)
        }
    case http.MethodPut:
        s.handleCreateBucket(rc)
    case http.MethodHead:
        s.handleHeadBucket(rc)
    case http.MethodDelete:
        s.handleDeleteBucket(rc)
    case http.MethodPost:
        if q.Has("delete") {
            s.handleDeleteObjects(rc)
            return
        }
        rc.fail(errMethodNotAllowed())
    default:
        rc.fail(errMethodNotAllowed())
    }
}

func (s *Server) dispatchObject(rc *requestCtx) {
    q := rc.r.URL.Query()
    rc.uploadID = q.Get("uploadId")
    switch rc.r.Method {
    case http.MethodPost:
        switch {
        case q.Has("uploads"):
            s.handleInitiateUpload(rc)
        case rc.uploadID != "":
            s.handleCompleteUpload(rc)
        default:
            rc.fail(errMethodNotAllowed())
        }
    case http.MethodPut:
        if q.Has("partNumber") || rc.uploadID != "" {
            if !q.Has("partNumber") || rc.uploadID == "" {
                rc.fail(errInvalidArgument("Missing partNumber or uploadId"))
                return
            }
            s.handleUploadPart(rc)
            return
        }
        if rc.r.Header.Get("x-amz-copy-source") != "" {
            s.handleCopyObject(rc)
            return
        }
        s.handlePutObject(rc)
    case http.MethodGet:
        if rc.uploadID != "" {
            s.handleListParts(rc)
            return
        }
        s.handleGetObject(rc, false)
    case http.MethodHead:
        s.handleGetObject(rc, true)
    case http.MethodDelete:
        if rc.uploadID != "" {
            s.handleAbortUpload(rc)
            return
        }
        s.handleDeleteObject(rc)
    default:
        rc.fail(errMethodNotAllowed())
    }
}
