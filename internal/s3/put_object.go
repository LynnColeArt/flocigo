package s3

// Ticket floci-go/ticket-007 — PUT /{bucket}/{key}.
//
// Writes the request body to the on-disk object store and answers with the
// MD5-based ETag in the ETag response header, mirroring Amazon S3 wire
// behaviour: HTTP 200, empty body, ETag set to the quoted lowercase-hex MD5
// of the stored bytes.
//
// The handler is self-contained so routers can wire it for every PUT with a
// non-empty key:
//
//	case http.MethodPut && key != "":
//	    HandlePutObject(w, r)

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const s3XMLNamespace = "http://s3.amazonaws.com/doc/2006-03-01/"

var (
	objectRootMu sync.RWMutex
	objectRoot   string
)

// SetObjectDataRoot pins the directory objects are persisted to. Service
// wiring may call it at startup. When unset, FLOCI_S3_DATA_DIR is checked,
// then FLOCI_DATA_DIR (with /s3 appended), defaulting to .floci/s3.
func SetObjectDataRoot(dir string) {
	objectRootMu.Lock()
	objectRoot = dir
	objectRootMu.Unlock()
}

// ObjectDataRoot returns the directory objects are persisted under.
func ObjectDataRoot() string {
	objectRootMu.RLock()
	pinned := objectRoot
	objectRootMu.RUnlock()
	if pinned != "" {
		return pinned
	}
	if d := strings.TrimSpace(os.Getenv("FLOCI_S3_DATA_DIR")); d != "" {
		return d
	}
	if d := strings.TrimSpace(os.Getenv("FLOCI_DATA_DIR")); d != "" {
		return filepath.Join(d, "s3")
	}
	return filepath.Join(".floci", "s3")
}

// objectFSPath maps (bucket, key) onto a path below root. "." and ".." key
// segments are neutralised so a hostile key cannot escape the store.
func objectFSPath(root, bucket, key string) string {
	segments := strings.Split(key, "/")
	for i, seg := range segments {
		if seg == "." || seg == ".." {
			segments[i] = "_" + seg
		}
	}
	return filepath.Join(root, bucket, filepath.FromSlash(strings.Join(segments, "/")))
}

// PutObjectToDisk streams body to root/bucket/key (temp file plus rename) and
// returns the hex-encoded MD5 of the stored bytes.
func PutObjectToDisk(root, bucket, key string, body io.Reader) (string, error) {
	dst := objectFSPath(root, bucket, key)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".put-")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op once the rename below has succeeded

	digest := md5.New()
	if _, err := io.Copy(io.MultiWriter(tmp, digest), body); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmpName, dst); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// HandlePutObject implements PUT /{bucket}/{key}: writes the request body to
// disk and returns the MD5 ETag header with an empty 200 response. Only call
// it for object requests; a PUT on the bucket root is CreateBucket.
func HandlePutObject(w http.ResponseWriter, r *http.Request) {
	bucket, key, found := splitBucketAndKey(r)
	if !found {
		writePutError(w, r, "InvalidRequest", "Malformed object path.", http.StatusBadRequest)
		return
	}
	if bucket == "" || bucket == "." || bucket == ".." {
		writePutError(w, r, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)
		return
	}
	if key == "" {
		writePutError(w, r, "InvalidRequest", "Missing object key.", http.StatusBadRequest)
		return
	}

	// A PUT carrying x-amz-copy-source is CopyObject, not a body upload.
	if src := r.Header.Get("x-amz-copy-source"); src != "" {
		putCopyObject(w, r, bucket, key, src)
		return
	}

	root := ObjectDataRoot()
	etag, err := PutObjectToDisk(root, bucket, key, r.Body)
	if err != nil {
		writePutError(w, r, "InternalError", "We encountered an internal error. Please try again.", http.StatusInternalServerError)
		return
	}

	// Like S3: reject the upload when Content-MD5 does not match, leaving no
	// object behind.
	if match, malformed := checkContentMD5(r, etag); !match {
		_ = os.Remove(objectFSPath(root, bucket, key))
		if malformed {
			writePutError(w, r, "InvalidDigest", "The Content-MD5 you specified is not valid.", http.StatusBadRequest)
		} else {
			writePutError(w, r, "BadDigest", "The Content-MD5 you specified did not match what we received.", http.StatusBadRequest)
		}
		return
	}

	w.Header().Set("ETag", `"`+etag+`"`)
	w.Header().Set("x-amz-request-id", newPutRequestID())
	w.WriteHeader(http.StatusOK)
}

// splitBucketAndKey extracts (bucket, key) from the request path, decoding
// the escaped form so encoded characters and unicode keys survive.
func splitBucketAndKey(r *http.Request) (bucket, key string, ok bool) {
	escaped := strings.TrimPrefix(r.URL.EscapedPath(), "/")
	slash := strings.IndexByte(escaped, '/')
	if slash < 0 {
		return "", "", false
	}
	var err error
	if bucket, err = url.PathUnescape(escaped[:slash]); err != nil {
		bucket = escaped[:slash]
	}
	if key, err = url.PathUnescape(escaped[slash+1:]); err != nil {
		key = escaped[slash+1:]
	}
	return bucket, key, true
}

// checkContentMD5 verifies the optional Content-MD5 header against the
// received digest (sumHex). malformed reports an unusable header value.
func checkContentMD5(r *http.Request, sumHex string) (match, malformed bool) {
	raw := strings.TrimSpace(r.Header.Get("Content-MD5"))
	if raw == "" {
		return true, false
	}
	given, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return false, true
	}
	return hex.EncodeToString(given) == sumHex, false
}

// newPutRequestID returns a random request id for S3-style responses.
func newPutRequestID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "FLOCIREQUESTID0"
	}
	return strings.ToUpper(hex.EncodeToString(b[:]))
}

// putErrorDocument is the standard S3 <Error/> XML document.
type putErrorDocument struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}

// writePutError emits an S3-style XML error response.
func writePutError(w http.ResponseWriter, r *http.Request, code, message string, status int) {
	rid := newPutRequestID()
	body, err := xml.Marshal(putErrorDocument{
		Code:      code,
		Message:   message,
		Resource:  r.URL.Path,
		RequestID: rid,
	})
	if err != nil {
		body = []byte("<Error><Code>InternalError</Code></Error>")
	}
	h := w.Header()
	h.Set("Content-Type", "application/xml")
	h.Set("x-amz-request-id", rid)
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(body)
	_, _ = w.Write([]byte("\n"))
}

// copyObjectResultDocument is the S3 <CopyObjectResult/> XML document.
type copyObjectResultDocument struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	Xmlns        string   `xml:"xmlns,attr"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

// putCopyObject serves the x-amz-copy-source flavour of PUT (CopyObject).
func putCopyObject(w http.ResponseWriter, r *http.Request, dstBucket, dstKey, copySource string) {
	src := copySource
	if i := strings.IndexByte(src, '?'); i >= 0 {
		src = src[:i] // drop any ?versionId= suffix
	}
	if dec, err := url.PathUnescape(src); err == nil {
		src = dec
	}
	src = strings.TrimPrefix(src, "/")
	slash := strings.IndexByte(src, '/')
	if slash <= 0 || slash == len(src)-1 {
		writePutError(w, r, "InvalidArgument", "Copy Source must mention the source bucket and key: sourcebucket/sourcekey", http.StatusBadRequest)
		return
	}
	srcBucket, srcKey := src[:slash], src[slash+1:]

	data, err := os.ReadFile(objectFSPath(ObjectDataRoot(), srcBucket, srcKey))
	if err != nil {
		writePutError(w, r, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
		return
	}
	etag, err := PutObjectToDisk(ObjectDataRoot(), dstBucket, dstKey, bytes.NewReader(data))
	if err != nil {
		writePutError(w, r, "InternalError", "We encountered an internal error. Please try again.", http.StatusInternalServerError)
		return
	}

	body, err := xml.Marshal(copyObjectResultDocument{
		Xmlns:        s3XMLNamespace,
		LastModified: time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		ETag:         `"` + etag + `"`,
	})
	if err != nil {
		writePutError(w, r, "InternalError", "We encountered an internal error. Please try again.", http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/xml")
	h.Set("x-amz-request-id", newPutRequestID())
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(body)
	_, _ = w.Write([]byte("\n"))
}

// MountPutObject registers HandlePutObject on a pattern-based ServeMux
// (Go 1.22+ routes) so routers can wire the op dynamically.
func MountPutObject(mux *http.ServeMux) {
	mux.HandleFunc("PUT /{bucket}/{key}", HandlePutObject)
}
