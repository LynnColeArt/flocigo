package s3

// create_bucket.go implements the CreateBucket operation (PUT /{bucket}),
// ticket flocigo/ticket-006.
//
// Wire behaviour mirrors S3 and floci:
//
//   - valid name, new bucket    -> 200 OK with a Location header, empty body
//   - valid name, existing name -> idempotent 200 OK: the caller already owns
//     the bucket, so BucketAlreadyOwnedByYou is answered as plain success
//   - invalid name              -> 400 with an InvalidBucketName error doc
//
// Buckets live on disk as directories named after the bucket, directly below
// the store root. The operation is exposed as a standalone http.Handler so it
// can be mounted in the service assembly with a single line:
//
//	s3.MountCreateBucket(mux, root)
//
// The method-aware ServeMux pattern used for the mount only matches a single
// path segment, so PUT /{bucket}/{key} keeps flowing to the object handlers.

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// createBucketNamePattern enforces the structural S3 naming rules: 3-63
// characters of [a-z0-9.-], starting and ending with a letter or digit.
var createBucketNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

// createBucketIPv4Pattern matches names formatted as an IPv4 address, which
// S3 rejects so bucket names cannot be mistaken for host names.
var createBucketIPv4Pattern = regexp.MustCompile(`^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$`)

// createBucketNameValid reports whether bucket satisfies the S3 naming rules.
func createBucketNameValid(bucket string) bool {
	if !createBucketNamePattern.MatchString(bucket) {
		return false
	}
	if strings.Contains(bucket, "..") {
		return false
	}
	if createBucketIPv4Pattern.MatchString(bucket) {
		return false
	}
	if strings.HasPrefix(bucket, "xn--") || strings.HasSuffix(bucket, "-s3alias") {
		return false
	}
	return true
}

// createBucketFault is the wire document returned for failed creates.
type createBucketFault struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}

// createBucketRequestID returns an opaque per-response identifier.
func createBucketRequestID() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "floci"
	}
	return hex.EncodeToString(buf[:])
}

// writeCreateBucketFault writes a canonical S3 error document with the given
// status, code, message and resource.
func writeCreateBucketFault(w http.ResponseWriter, status int, code, message, resource string) {
	body, err := xml.Marshal(createBucketFault{
		Code:      code,
		Message:   message,
		Resource:  resource,
		RequestID: createBucketRequestID(),
	})
	if err != nil {
		http.Error(w, code, status)
		return
	}
	id := createBucketRequestID()
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("x-amz-request-id", id)
	w.WriteHeader(status)
	_, _ = w.Write(append([]byte(xml.Header), body...))
}

// CreateBucketHandler returns an http.Handler implementing PUT /{bucket}
// against the store root.
func CreateBucketHandler(root string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		putBucket(w, r, root)
	})
}

// MountCreateBucket registers the CreateBucket handler on mux with the
// method-aware pattern supported since Go 1.22, capturing exactly the
// single-segment route PUT /{bucket}.
func MountCreateBucket(mux *http.ServeMux, root string) {
	mux.Handle("PUT /{bucket}", CreateBucketHandler(root))
}

// putBucket validates the bucket name, creates the bucket directory on disk
// and answers 200 with a Location header. Re-creating an owned bucket is
// idempotent (BucketAlreadyOwnedByYou is answered as 200), and invalid names
// get a 400 InvalidBucketName document.
func putBucket(w http.ResponseWriter, r *http.Request, root string) {
	if r.Method != http.MethodPut {
		writeCreateBucketFault(w, http.StatusMethodNotAllowed, "MethodNotAllowed",
			"The specified method is not allowed against this resource.", r.URL.Path)
		return
	}
	bucket := strings.Trim(r.URL.Path, "/")
	if bucket == "" || !createBucketNameValid(bucket) {
		writeCreateBucketFault(w, http.StatusBadRequest, "InvalidBucketName",
			"The specified bucket is not valid.", "/"+bucket)
		return
	}
	// os.MkdirAll is a no-op for directories that already exist, which is
	// what makes re-creating a bucket idempotent (BucketAlreadyOwnedByYou).
	if err := os.MkdirAll(filepath.Join(root, bucket), 0o755); err != nil {
		writeCreateBucketFault(w, http.StatusInternalServerError, "InternalError",
			"We encountered an internal error. Please try again.", "/"+bucket)
		return
	}
	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}
