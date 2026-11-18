package s3

import (
	"net/http"
	"strconv"
)

// headObject implements HEAD /{bucket}/{key}.
//
// The S3 wire contract for HEAD Object is "the GET Object response with the
// body suppressed": same status code and the same headers (ETag,
// Last-Modified, Content-Type, Cache-Control, Content-Disposition,
// Content-Encoding, Accept-Ranges, x-amz-* metadata headers, x-amz-* response
// headers) and never an entity body — including on errors, where HEAD still
// carries the GET error's status and headers but no XML document. Conditional
// requests (If-Match/If-None-Match/If-Modified-Since/If-Unmodified-Since) and
// Range therefore resolve exactly as they do for GET.
//
// This is implemented by replaying the request through the existing object
// GET handler against a recording ResponseWriter and then replaying what was
// captured — status and headers — while discarding the body. Reusing the GET
// path means the GET and HEAD wire formats can never drift and the GET
// handler keeps owning lookup, query overrides, range and conditional logic.
//
// Wire-up: in the object-level ({bucket}/{key}) branch of the s3 router add
//
//	case http.MethodHead:
//		s.headObject(w, r)
//
// The original HEAD request has already passed the router's auth/presign gate
// by the time this runs, so rewriting the replayed request to GET does not
// affect SigV4 or presigned URL validation.
func (s *Server) headObject(w http.ResponseWriter, r *http.Request) {
	// Replay as a GET so any method-sensitive behavior inside the GET path
	// behaves normally; the presign/SigV4 gate for the original HEAD has
	// already run in the router.
	getReq := r.Clone(r.Context())
	getReq.Method = http.MethodGet

	rec := &headObjectRecorder{header: w.Header().Clone()}
	s.getObject(rec, getReq)

	out := w.Header()
	for name, values := range rec.header {
		out[name] = values
	}

	status := rec.status
	if status == 0 {
		status = http.StatusOK
	}
	if status >= 200 && status < 300 {
		// HEAD advertises the size GET would have streamed for this exact
		// request, so Content-Length always matches the object (or the
		// selected range) even if the GET path left the header implicit.
		out.Set("Content-Length", strconv.FormatInt(rec.size, 10))
	}
	w.WriteHeader(status)
}

// headObjectRecorder records the response the object GET handler would have
// produced so headObject can replay it as headers-only.
type headObjectRecorder struct {
	header    http.Header
	status    int
	size      int64
	committed bool
}

func (rec *headObjectRecorder) Header() http.Header { return rec.header }

func (rec *headObjectRecorder) WriteHeader(code int) {
	if rec.committed {
		return
	}
	rec.committed = true
	rec.status = code
}

func (rec *headObjectRecorder) Write(p []byte) (int, error) {
	if !rec.committed {
		rec.WriteHeader(http.StatusOK)
	}
	rec.size += int64(len(p))
	return len(p), nil
}

// Flush implements http.Flusher. GET-streaming handlers may probe for it;
// writes are buffered here, so there is nothing to flush.
func (rec *headObjectRecorder) Flush() {}
