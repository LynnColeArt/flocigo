package s3

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"net"
	"net/http"
	"strings"
	"time"
)

// Router serves the S3 HTTP surface over one Store. It understands both
// path-style (http://host/bucket/key) and virtual-hosted-style
// (http://bucket.s3.<host>/key) addressing.
type Router struct {
	// Store backs every bucket and object; lazily created when nil.
	Store *Store
	// Region is reported by GetBucketLocation (default us-east-1).
	Region string
	// Presign plugs in credential resolution and signed-request notification.
	Presign *PresignHooks
}

// Service is an alias kept for uniform service wiring across the emulator.
type Service = Router

// New returns a Router over an optional store.
func New(store ...*Store) *Router { return newRouter(store) }

// NewRouter is a synonym for New.
func NewRouter(store ...*Store) *Router { return newRouter(store) }

// NewHandler is a synonym for New.
func NewHandler(store ...*Store) *Router { return newRouter(store) }

// NewServer is a synonym for New.
func NewServer(store ...*Store) *Router { return newRouter(store) }

// NewService is a synonym for New.
func NewService(store ...*Store) *Router { return newRouter(store) }

func newRouter(store []*Store) *Router {
	rt := &Router{}
	if len(store) > 0 && store[0] != nil {
		rt.Store = store[0]
	}
	return rt
}

// Handler returns rt as an http.Handler.
func (rt *Router) Handler() http.Handler { return rt }

func (rt *Router) store() *Store {
	if rt.Store == nil {
		rt.Store = NewStore()
	}
	return rt.Store
}

func (rt *Router) region() string {
	if rt.Region == "" {
		return DefaultRegion
	}
	return rt.Region
}

// splitTarget resolves the bucket/key pair for a request, honoring
// virtual-host style Host headers before falling back to path style.
func splitTarget(host, path string) (bucket, key string, hostStyle bool) {
	h := host
	if hh, _, err := net.SplitHostPort(h); err == nil {
		h = hh
	}
	h = strings.ToLower(strings.Trim(h, "[]"))
	if i := strings.Index(h, ".s3."); i > 0 && len(h[i+4:]) > 0 {
		return h[:i], strings.TrimPrefix(path, "/"), true
	}
	if rest, ok := strings.CutSuffix(h, ".localhost"); ok && rest != "" && rest != "s3" {
		return rest, strings.TrimPrefix(path, "/"), true
	}
	rest := strings.TrimPrefix(path, "/")
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		return rest[:i], rest[i+1:], false
	}
	return rest, "", false
}

// ServeHTTP implements http.Handler.
func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqID := newRequestID()
	h := w.Header()
	h.Set("Server", "AmazonS3")
	h.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	h.Set("x-amz-request-id", reqID)
	h.Set("x-amz-id-2", newTraceID())

	bucket, _, hostStyle := splitTarget(r.Host, r.URL.Path)
	if e := rt.authorize(r, bucket, hostStyle); e != nil {
		writeError(w, r, e)
		return
	}
	_, key, _ := splitTarget(r.Host, r.URL.Path)
	c := &reqCtx{rt: rt, w: w, r: r, bucket: bucket, key: key, hostStyle: hostStyle}
	c.dispatch()
}

// reqCtx carries one request through the operation dispatch.
type reqCtx struct {
	rt        *Router
	w         http.ResponseWriter
	r         *http.Request
	bucket    string
	key       string
	hostStyle bool
}

func (c *reqCtx) dispatch() {
	switch {
	case c.bucket == "":
		c.rootOperation()
	case c.key == "":
		c.bucketLevel()
	default:
		c.objectLevel()
	}
}

func (c *reqCtx) rootOperation() {
	switch c.r.Method {
	case http.MethodGet:
		c.listBuckets()
	case http.MethodHead:
		writeEmpty(c.w, http.StatusOK)
	default:
		c.default405()
	}
}

func (c *reqCtx) mustBucket() *s3Error {
	if _, ok := c.rt.store().GetBucket(c.bucket); !ok {
		return errNoSuchBucket()
	}
	return nil
}

func (c *reqCtx) default405() {
	writeError(c.w, c.r, s3err(405, "MethodNotAllowed", "The specified method is not allowed against this resource."))
}

func newRequestID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return strings.ToUpper(hex.EncodeToString(b))
}

func newTraceID() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return base64.StdEncoding.EncodeToString(b)
}
