package s3

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"os"
	"strings"
)

// NewStoreAt is a path-arg NewStore, kept for the binary entry point.
func NewStoreAt(root string) *Store { return NewStore() }

// Server is the legacy S3 HTTP surface name used by HEAD/DELETE handlers.
type Server struct {
	store *Store
}

// NewServer returns a Server bound to the store.
func NewServer(store *Store) *Server { return &Server{store: store} }

// Router is the legacy S3 HTTP surface name used by presign handlers.
type Router struct {
	Store      *Store
	Presign    *PresignHooks
	AuthKey    string
	AuthSecret string
}

// region reports the AWS region the store is configured for. us-east-1
// unless overridden by FLOCI_REGION.
func (rt *Router) region() string {
	if v := os.Getenv("FLOCI_REGION"); v != "" {
		return v
	}
	if v := os.Getenv("AWS_REGION"); v != "" {
		return v
	}
	return "us-east-1"
}

// getObject delegates Server-level GET through the reqCtx path.
func (s *Server) getObject(w http.ResponseWriter, r *http.Request) {
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
	bucket, key := parts[0], ""
	if len(parts) > 1 {
		key = parts[1]
	}
	rc := newReqCtx(w, r, s.store, bucket, key, reqID(), reqID())
	rc.getObject(false)
}

// st exposes the Server's store.
func (s *Server) st() *Store { return s.store }

// reqID returns a 16-hex-char request identifier.
func reqID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// Router is the legacy S3 HTTP surface. The live implementation below the
// shim is the reqCtx chain.
func (s *Store) Router() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
		bucket, key := parts[0], ""
		if len(parts) > 1 {
			key = parts[1]
		}
		rc := &reqCtx{r: r, w: w, store: s, bucket: bucket, key: key,
			reqID: reqID(), hostID: reqID(), rt: &Router{Store: s}}
		if key == "" {
			rc.bucketLevel()
		} else {
			rc.objectLevel()
		}
	})
}

// splitTarget splits a Host header into (bucket, key, hostStyle).
func splitTarget(host, path string) (string, string, bool) {
	// Virtual-hosted style: <bucket>.<host>/<key>
	if i := strings.Index(host, "."); i > 0 {
		return host[:i], strings.TrimPrefix(path, "/"), true
	}
	return strings.SplitN(strings.Trim(path, "/"), "/", 2)[0], path, false
}
