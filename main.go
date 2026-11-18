// floci-go - minimal filesystem-backed S3-compatible server.
//
// Ticket floci-go/ticket-010: DELETE /{bucket}/{key} removes the object's
// content file and its metadata sidecar and always responds 204 No Content
// (S3 deletes are idempotent: deleting a missing object still succeeds).
package main

import (
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	metaDirName        = ".floci-meta"
	defaultContentType = "binary/octet-stream"
	s3XMLNS            = "http://s3.amazonaws.com/doc/2006-03-01/"
)

var (
	dataDir = envOr("FLOCI_DATA_DIR", envOr("DATA_DIR", "data"))
	storeMu sync.Mutex
)

func resolveDataRoot() string {
	abs, err := filepath.Abs(dataDir)
	if err != nil {
		return dataDir
	}
	return abs
}

var dataRoot = resolveDataRoot()

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	if err := os.MkdirAll(dataRoot, 0755); err != nil {
		log.Fatalf("floci-go: cannot create data dir %s: %v", dataRoot, err)
	}
	port := envOr("FLOCI_PORT", envOr("PORT", "9000"))
	addr := ":" + port
	log.Printf("floci-go listening on %s (data dir: %s)", addr, dataRoot)
	if err := http.ListenAndServe(addr, http.HandlerFunc(route)); err != nil {
		log.Fatal(err)
	}
}

// route dispatches path-style requests: /, /{bucket}, /{bucket}/{key}.
func route(w http.ResponseWriter, r *http.Request) {
	storeMu.Lock()
	defer storeMu.Unlock()

	w.Header().Set("x-amz-request-id", newRequestID())
	w.Header().Set("Server", "AmazonS3")

	bucket, key := splitPath(r.URL.Path)
	if bucket != "" && !safeBucketName(bucket) {
		writeError(w, http.StatusBadRequest, "InvalidBucketName", "The specified bucket is not valid.", bucket, "")
		return
	}

	switch {
	case bucket == "":
		switch r.Method {
		case http.MethodGet, http.MethodHead:
			listBuckets(w, r)
		default:
			writeMethodNotAllowed(w)
		}
	case key == "":
		switch r.Method {
		case http.MethodPut:
			putBucket(w, r, bucket)
		case http.MethodGet, http.MethodHead:
			getBucket(w, r, bucket)
		case http.MethodDelete:
			deleteBucket(w, bucket)
		case http.MethodPost:
			q := r.URL.Query()
			if _, ok := q["delete"]; ok || q.Get("x-id") == "DeleteObjects" {
				deleteObjects(w, r, bucket)
			} else {
				writeError(w, http.StatusNotImplemented, "NotImplemented", "A header or parameter you provided implies functionality that is not implemented.", bucket, "")
			}
		default:
			writeMethodNotAllowed(w)
		}
	default:
		switch r.Method {
		case http.MethodPut:
			putObject(w, r, bucket, key)
		case http.MethodGet, http.MethodHead:
			getObject(w, r, bucket, key)
		case http.MethodDelete:
			// Ticket floci-go/ticket-010: DELETE /{bucket}/{key}.
			deleteObject(w, bucket, key)
		default:
			writeMethodNotAllowed(w)
		}
	}
}

func splitPath(p string) (bucket, key string) {
	s := strings.TrimPrefix(p, "/")
	if i := strings.Index(s, "/"); i >= 0 {
		return s[:i], s[i+1:]
	}
	return s, ""
}

func safeBucketName(bucket string) bool {
	switch bucket {
	case "", ".", "..":
		return false
	}
	return strings.IndexByte(bucket, 0) < 0
}

// ---- bucket operations ----

func listBuckets(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	entries, err := os.ReadDir(dataRoot)
	if err != nil {
		entries = nil
	}
	buckets := make([]BucketXML, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		ct := time.Now()
		if info, ierr := e.Info(); ierr == nil {
			ct = info.ModTime()
		}
		buckets = append(buckets, BucketXML{Name: e.Name(), CreationDate: iso8601(ct)})
	}
	writeXML(w, http.StatusOK, ListAllMyBucketsResult{
		Xmlns:   s3XMLNS,
		Owner:   Owner{ID: "floci", DisplayName: "floci"},
		Buckets: Buckets{Bucket: buckets},
	})
}

func putBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if isBucketSubresource(r.URL.Query()) {
		// Subresource updates (acl, versioning, cors, ...) are accepted as no-ops.
		if !bucketExists(bucket) {
			notFoundBucket(w, bucket)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	if bucketExists(bucket) {
		writeError(w, http.StatusConflict, "BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded and you already own it.", bucket, "")
		return
	}
	if err := os.MkdirAll(bucketDir(bucket), 0755); err != nil {
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), bucket, "")
		return
	}
	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

func isBucketSubresource(q map[string][]string) bool {
	for k := range q {
		switch k {
		case "versioning", "acl", "cors", "lifecycle", "policy", "tagging",
			"encryption", "website", "logging", "replication", "requestPayment",
			"accelerate", "notification", "object-lock", "publicAccessBlock",
			"ownershipControls", "intelligent-tiering":
			return true
		}
	}
	return false
}

func getBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	q := r.URL.Query()
	if _, ok := q["location"]; ok {
		if !bucketExists(bucket) {
			notFoundBucket(w, bucket)
			return
		}
		writeRawXML(w, xml.Header+`<LocationConstraint xmlns="`+s3XMLNS+`"/>`)
		return
	}
	if _, ok := q["versioning"]; ok {
		if !bucketExists(bucket) {
			notFoundBucket(w, bucket)
			return
		}
		writeRawXML(w, xml.Header+`<VersioningConfiguration xmlns="`+s3XMLNS+`"/>`)
		return
	}
	if !bucketExists(bucket) {
		notFoundBucket(w, bucket)
		return
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	listV2 := q.Get("list-type") == "2"
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	maxKeys := 1000
	if v := q.Get("max-keys"); v != "" {
		if n, aerr := strconv.Atoi(v); aerr == nil && n >= 0 {
			maxKeys = n
		}
	}
	if maxKeys > 1000 {
		maxKeys = 1000
	}
	var marker string
	if listV2 {
		marker = decodeContinuation(q.Get("continuation-token"))
		if marker == "" {
			marker = q.Get("start-after")
		}
	} else {
		marker = q.Get("marker")
	}
	contents, prefixes, truncated := listBucket(bucket, prefix, delimiter, marker, maxKeys)
	res := ListBucketResult{
		Xmlns:          s3XMLNS,
		Name:           bucket,
		Prefix:         prefix,
		Delimiter:      delimiter,
		MaxKeys:        maxKeys,
		IsTruncated:    truncated,
		Contents:       contents,
		CommonPrefixes: prefixes,
	}
	if listV2 {
		res.KeyCount = len(contents) + len(prefixes)
		res.ContinuationToken = q.Get("continuation-token")
		res.StartAfter = q.Get("start-after")
		if truncated {
			last := ""
			if len(contents) > 0 {
				last = contents[len(contents)-1].Key
			}
			if len(prefixes) > 0 && prefixes[len(prefixes)-1].Prefix > last {
				last = prefixes[len(prefixes)-1].Prefix
			}
			res.NextContinuationToken = encodeContinuation(last)
		}
	} else {
		res.Marker = marker
	}
	writeXML(w, http.StatusOK, res)
}

func deleteBucket(w http.ResponseWriter, bucket string) {
	if !bucketExists(bucket) {
		notFoundBucket(w, bucket)
		return
	}
	if bucketHasObjects(bucket) {
		writeError(w, http.StatusConflict, "BucketNotEmpty", "The bucket you tried to delete is not empty", bucket, "")
		return
	}
	_ = os.RemoveAll(bucketDir(bucket))
	w.WriteHeader(http.StatusNoContent)
}

func bucketHasObjects(bucket string) bool {
	found := false
	_ = filepath.WalkDir(bucketDir(bucket), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if d.Name() == metaDirName {
				return fs.SkipDir
			}
			return nil
		}
		found = true
		return nil
	})
	return found
}

// deleteObjects handles POST /{bucket}?delete (multi-object delete).
func deleteObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	if !bucketExists(bucket) {
		notFoundBucket(w, bucket)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		malformedXML(w, bucket)
		return
	}
	var req DeleteObjectsRequest
	if uerr := xml.Unmarshal(body, &req); uerr != nil {
		malformedXML(w, bucket)
		return
	}
	res := DeleteResultXML{Xmlns: s3XMLNS}
	for _, o := range req.Objects {
		removeObjectFS(bucket, o.Key)
		if !req.Quiet {
			res.Deleted = append(res.Deleted, DeletedXML{Key: o.Key})
		}
	}
	writeXML(w, http.StatusOK, res)
}

func malformedXML(w http.ResponseWriter, bucket string) {
	writeError(w, http.StatusBadRequest, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema", bucket, "")
}

// ---- object operations ----

func putObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if !bucketExists(bucket) {
		notFoundBucket(w, bucket)
		return
	}
	objPath, ok := safeObjectPath(bucket, key)
	if !ok {
		writeError(w, http.StatusBadRequest, "InvalidArgument", "Key name contains unsupported path segments.", bucket, key)
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "InvalidRequest", err.Error(), bucket, key)
		return
	}
	etag := md5Hex(data)
	if err := os.MkdirAll(filepath.Dir(objPath), 0755); err != nil {
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), bucket, key)
		return
	}
	if err := os.WriteFile(objPath, data, 0644); err != nil {
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), bucket, key)
		return
	}
	ct := strings.TrimSpace(r.Header.Get("Content-Type"))
	if ct == "" {
		ct = defaultContentType
	}
	saveMeta(bucket, key, objectMeta{
		Key:          key,
		ETag:         etag,
		ContentType:  ct,
		Size:         int64(len(data)),
		LastModified: time.Now().UTC(),
	})
	w.Header().Set("ETag", `"`+etag+`"`)
	w.WriteHeader(http.StatusOK)
}

func getObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if !bucketExists(bucket) {
		notFoundBucket(w, bucket)
		return
	}
	objPath, ok := safeObjectPath(bucket, key)
	if !ok {
		writeError(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.", bucket, key)
		return
	}
	info, err := os.Stat(objPath)
	if err != nil || info.IsDir() {
		writeError(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.", bucket, key)
		return
	}
	ct := defaultContentType
	etag := ""
	if m := readMeta(bucket, key); m != nil {
		if m.ContentType != "" {
			ct = m.ContentType
		}
		etag = m.ETag
	}
	if etag == "" {
		if data, rerr := os.ReadFile(objPath); rerr == nil {
			etag = md5Hex(data)
		}
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("ETag", `"`+etag+`"`)
	w.Header().Set("Last-Modified", info.ModTime().UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
		w.WriteHeader(http.StatusOK)
		return
	}
	data, err := os.ReadFile(objPath)
	if err != nil {
		writeError(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.", bucket, key)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// deleteObject implements DELETE /{bucket}/{key} (ticket floci-go/ticket-010).
// The object's content file and its metadata sidecar are removed. Per S3 and
// floci semantics the delete is idempotent: 204 No Content is returned always,
// whether or not the object, its metadata, or even the bucket exist.
func deleteObject(w http.ResponseWriter, bucket, key string) {
	removeObjectFS(bucket, key)
	w.WriteHeader(http.StatusNoContent)
}

// removeObjectFS removes the metadata sidecar and the content file of an
// object from disk, ignoring absence errors.
func removeObjectFS(bucket, key string) {
	_ = os.Remove(metaPath(bucketDir(bucket), key))
	objPath, ok := safeObjectPath(bucket, key)
	if !ok {
		return
	}
	if err := os.Remove(objPath); err != nil {
		return
	}
	pruneEmptyDirs(bucketDir(bucket), objPath)
}

// ---- listing ----

func listBucket(bucket, prefix, delimiter, marker string, maxKeys int) ([]ContentsXML, []CommonPrefixXML, bool) {
	if maxKeys <= 0 {
		return nil, nil, false
	}
	bDir := bucketDir(bucket)
	var keys []string
	_ = filepath.WalkDir(bDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if d.Name() == metaDirName {
				return fs.SkipDir
			}
			return nil
		}
		rel, rerr := filepath.Rel(bDir, p)
		if rerr != nil {
			return nil
		}
		keys = append(keys, filepath.ToSlash(rel))
		return nil
	})
	sort.Strings(keys)

	filtered := keys
	if marker != "" {
		i := sort.SearchStrings(keys, marker)
		if i < len(keys) && keys[i] == marker {
			i++
		}
		filtered = keys[i:]
	}

	var contents []ContentsXML
	var prefixes []CommonPrefixXML
	seen := make(map[string]bool)
	count := 0
	truncated := false
	for _, k := range filtered {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			continue
		}
		if delimiter != "" {
			rest := strings.TrimPrefix(k, prefix)
			if idx := strings.Index(rest, delimiter); idx >= 0 {
				cp := prefix + rest[:idx+len(delimiter)]
				if seen[cp] {
					continue
				}
				if count >= maxKeys {
					truncated = true
					break
				}
				seen[cp] = true
				prefixes = append(prefixes, CommonPrefixXML{Prefix: cp})
				count++
				continue
			}
		}
		if count >= maxKeys {
			truncated = true
			break
		}
		contents = append(contents, objectToContents(bucket, k))
		count++
	}
	return contents, prefixes, truncated
}

func objectToContents(bucket, key string) ContentsXML {
	var size int64
	var mod time.Time
	var etag string
	objPath, ok := safeObjectPath(bucket, key)
	if ok {
		if info, serr := os.Stat(objPath); serr == nil {
			size = info.Size()
			mod = info.ModTime()
		}
	}
	if m := readMeta(bucket, key); m != nil {
		etag = m.ETag
		if !m.LastModified.IsZero() {
			mod = m.LastModified
		}
	} else if ok {
		if data, rerr := os.ReadFile(objPath); rerr == nil {
			etag = md5Hex(data)
		}
	}
	if mod.IsZero() {
		mod = time.Now()
	}
	return ContentsXML{
		Key:          key,
		LastModified: iso8601(mod),
		ETag:         `"` + etag + `"`,
		Size:         size,
		StorageClass: "STANDARD",
	}
}

// ---- storage helpers ----

func bucketDir(bucket string) string {
	return filepath.Join(dataRoot, bucket)
}

func bucketExists(bucket string) bool {
	info, err := os.Stat(bucketDir(bucket))
	return err == nil && info.IsDir()
}

// safeObjectPath maps (bucket, key) to a content path that stays inside the
// bucket directory and never collides with the metadata directory.
func safeObjectPath(bucket, key string) (string, bool) {
	base := filepath.Clean(bucketDir(bucket))
	p := filepath.Clean(filepath.Join(base, key))
	if !strings.HasPrefix(p, base+string(os.PathSeparator)) {
		return "", false
	}
	rel, err := filepath.Rel(base, p)
	if err != nil {
		return "", false
	}
	for _, seg := range strings.Split(filepath.ToSlash(rel), "/") {
		if seg == metaDirName {
			return "", false
		}
	}
	return p, true
}

// metaPath maps a key to its metadata sidecar file. A SHA-256 of the key keeps
// the sidecar flat and collision-free even for long or slash-laden keys.
func metaPath(bDir, key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(bDir, metaDirName, hex.EncodeToString(sum[:])+.json")
}

func saveMeta(bucket, key string, m objectMeta) {
	p := metaPath(bucketDir(bucket), key)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return
	}
	data, err := json.Marshal(m)
	if err != nil {
		return
	}
	_ = os.WriteFile(p, data, 0644)
}

func readMeta(bucket, key string) *objectMeta {
	data, err := os.ReadFile(metaPath(bucketDir(bucket), key))
	if err != nil {
		return nil
	}
	var m objectMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return nil
	}
	return &m
}

// pruneEmptyDirs removes now-empty directories left behind after an object
// delete, up to but not including the bucket directory.
func pruneEmptyDirs(bDir, objPath string) {
	sep := string(os.PathSeparator)
	dir := filepath.Dir(objPath)
	for dir != bDir && strings.HasPrefix(dir, bDir+sep) {
		if err := os.Remove(dir); err != nil {
			return
		}
		dir = filepath.Dir(dir)
	}
}

func encodeContinuation(lastKey string) string {
	return base64.StdEncoding.EncodeToString([]byte(lastKey))
}

func decodeContinuation(token string) string {
	if b, err := base64.StdEncoding.DecodeString(token); err == nil {
		return string(b)
	}
	return token
}

// ---- response writers ----

func writeXML(w http.ResponseWriter, status int, v any) {
	body, err := xml.Marshal(v)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header + string(body)))
}

func writeRawXML(w http.ResponseWriter, s string) {
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write([]byte(s))
}

func writeError(w http.ResponseWriter, status int, code, message, bucket, key string) {
	rid := w.Header().Get("x-amz-request-id")
	if rid == "" {
		rid = newRequestID()
	}
	writeXML(w, status, ErrorResp{
		Code:       code,
		Message:    message,
		BucketName: bucket,
		Key:        key,
		RequestID:  rid,
	})
}

func notFoundBucket(w http.ResponseWriter, bucket string) {
	writeError(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist", bucket, "")
}

func writeMethodNotAllowed(w http.ResponseWriter) {
	writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "The specified method is not allowed against this resource.", "", "")
}

// ---- misc ----

func md5Hex(b []byte) string {
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:])
}

func newRequestID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%016X", time.Now().UnixNano())
	}
	return strings.ToUpper(hex.EncodeToString(b))
}

func iso8601(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

// ---- shapes ----

type ListAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Owner   Owner    `xml:"Owner"`
	Buckets Buckets  `xml:"Buckets"`
}

type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type Buckets struct {
	Bucket []BucketXML `xml:"Bucket"`
}

type BucketXML struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type ListBucketResult struct {
	XMLName               xml.Name          `xml:"ListBucketResult"`
	Xmlns                 string            `xml:"xmlns,attr"`
	Name                  string            `xml:"Name"`
	Prefix                string            `xml:"Prefix"`
	Marker                string            `xml:"Marker"`
	MaxKeys               int               `xml:"MaxKeys"`
	Delimiter             string            `xml:"Delimiter,omitempty"`
	IsTruncated           bool              `xml:"IsTruncated"`
	KeyCount              int               `xml:"KeyCount,omitempty"`
	ContinuationToken     string            `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string            `xml:"NextContinuationToken,omitempty"`
	StartAfter            string            `xml:"StartAfter,omitempty"`
	Contents              []ContentsXML     `xml:"Contents"`
	CommonPrefixes        []CommonPrefixXML `xml:"CommonPrefixes"`
}

type ContentsXML struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type CommonPrefixXML struct {
	Prefix string `xml:"Prefix"`
}

type DeleteObjectsRequest struct {
	XMLName xml.Name `xml:"Delete"`
	Quiet   bool     `xml:"Quiet"`
	Objects []struct {
		Key string `xml:"Key"`
	} `xml:"Object"`
}

type DeletedXML struct {
	Key string `xml:"Key"`
}

type DeleteResultXML struct {
	XMLName xml.Name     `xml:"DeleteResult"`
	Xmlns   string       `xml:"xmlns,attr"`
	Deleted []DeletedXML `xml:"Deleted"`
}

type ErrorResp struct {
	XMLName    xml.Name `xml:"Error"`
	Code       string   `xml:"Code"`
	Message    string   `xml:"Message"`
	BucketName string   `xml:"BucketName,omitempty"`
	Key        string   `xml:"Key,omitempty"`
	RequestID  string   `xml:"RequestId"`
}

type objectMeta struct {
	Key          string    `json:"key"`
	ETag         string    `json:"etag"`
	ContentType  string    `json:"contentType"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"lastModified"`
}
