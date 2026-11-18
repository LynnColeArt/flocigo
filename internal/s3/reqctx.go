package s3

import "net/http"

// reqCtx carries one S3 request through the routing/handler chain. The
// kernel's Store owns state; reqCtx only wraps request/response plus the
// bucket/key pair the router has already peeled off the URL.
type reqCtx struct {
	r       *http.Request
	w       http.ResponseWriter
	store   *Store
	bucket  string
	rt      *Router
	key     string
	reqID   string
	hostID  string
}

func newReqCtx(w http.ResponseWriter, r *http.Request, store *Store, bucket, key, reqID, hostID string) *reqCtx {
	return &reqCtx{r: r, w: w, store: store, bucket: bucket, key: key, reqID: reqID, hostID: hostID, rt: &Router{Store: store}}
}

// mustBucket verifies the bucket exists; it writes the S3 error to the wire
// and returns the matching *s3Error when missing, or nil when present — so
// callers can `if e := c.mustBucket(); e != nil { ... }`.
func (c *reqCtx) mustBucket() *s3Error {
	if !c.store.BucketExists(c.bucket) {
		writeError(c.w, c.r, s3err(404, "NoSuchBucket", "The specified bucket does not exist"))
		return s3err(404, "NoSuchBucket", "The specified bucket does not exist")
	}
	return nil
}

// default405 answers with 405 MethodNotAllowed for unsupported (method,
// subresource) combinations.
func (c *reqCtx) default405() {
	writeError(c.w, c.r, s3err(405, "MethodNotAllowed", "The specified method is not allowed against this resource."))
}

// rt exposes the store via the legacy rt accessor the bucket subresource
// handlers use.
// rt is the bucket-level accessor (legacy name)
func (c *reqCtx) rtField() *Router {
	return &Router{Store: c.store}
}

// store returns the S3 store the request routes through.
func (rt *Router) store() *Store { return rt.Store }

// BucketExists reports whether the bucket is present.
func (s *Store) BucketExists(name string) bool {
	_, ok := s.GetBucket(name)
	return ok
}
