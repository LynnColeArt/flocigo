package s3

import (
	"net/http"
)

// DeleteObject removes the object record for key inside bucket.
//
// The record holds the file bytes together with everything stored beside
// them (user metadata, content type, etag, tags), so removing the record
// drops file and meta in one step. The removal runs under the store lock:
// a concurrent GET or HEAD either sees the full record or none of it.
//
// It reports whether the key existed. Deleting a missing key — or a key in
// a bucket that does not exist — is not an error.
func (st *Store) DeleteObject(bucket, key string) bool {
	st.mu.Lock()
	defer st.mu.Unlock()

	b, ok := st.buckets[bucket]
	if !ok {
		return false
	}
	if _, ok := b.objects[key]; !ok {
		return false
	}
	delete(b.objects, key)
	return true
}

// handleDeleteObject serves DELETE /{bucket}/{key}.
//
// Wire behavior (matches S3 / the floci reference):
//
//   - the object data and meta are removed when the key exists;
//   - the response is always 204 No Content with an empty body — for a
//     missing key and even for an unknown bucket — which keeps DELETE
//     idempotent;
//   - nothing is written to the response body.
//
// Subsequent GET/HEAD on the key fall through to the normal miss path and
// answer 404 NoSuchKey; the bucket itself is left untouched.
func (s *Server) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	_ = r // method is fixed by the router; the path vars carry bucket and key

	s.st.DeleteObject(bucket, key)

	w.WriteHeader(http.StatusNoContent)
}
