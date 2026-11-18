// floci-go: a minimal S3-compatible object storage server.
//
// Ticket-007: implement PUT /{bucket}/{key} — stream the request body to
// disk under <dataDir>/<bucket>/<key>, compute the MD5 digest while writing
// (verifying an optional Content-MD5), and return 200 with the object's
// lowercase-hex MD5 ETag in a quoted ETag response header, matching S3 wire
// format. Missing buckets return 404 NoSuchBucket. The earlier ticket
// surface (list/create/delete buckets, ListObjects v1/v2, GET/HEAD/DELETE
// object) is kept consistent with canonical S3 XML responses.
package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	xmlNS          = "http://s3.amazonaws.com/doc/2006-03-01/"
	defaultPort    = "9000"
	defaultDataDir = "data"
	defaultCT      = "binary/octet-stream"
	iso8601Millis  = "2006-01-02T15:04:05.000Z"
	emptyMD5Hex    = "d41d8cd98f00b204e9800998ecf8427e"
)

// Result kinds for parseByteRange.
const (
	rangeFull = iota
	rangePartial
	rangeUnsat
)

type server struct {
	dataDir string
}

func main() {
	port := pickPort()
	dataDir := pickDataDir()
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		dataDir = filepath.Join(os.TempDir(), "floci-go-data")
		if err2 := os.MkdirAll(dataDir, 0755); err2 != nil {
			fmt.Fprintln(os.Stderr, "floci-go: cannot create data dir:", err2)
			os.Exit(1)
		}
	}
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Fprintln(os.Stderr, "floci-go: cannot listen on port "+port+":", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "floci-go: listening on %s (data dir: %s)\n", ln.Addr().String(), dataDir)
	if err := http.Serve(ln, &server{dataDir: dataDir}); err != nil {
		fmt.Fprintln(os.Stderr, "floci-go:", err)
		os.Exit(1)
	}
}

// pickPort honours a --port flag, then FLOCI_PORT/PORT, then the S3-style default 9000.
func pickPort() string {
	for i := 1; i < len(os.Args); i++ {
		arg := os.Args[i]
		switch {
		case arg == "--port" || arg == "-port" || arg == "-p":
			if i+1 < len(os.Args) {
				return os.Args[i+1]
			}
		case strings.HasPrefix(arg, "--port="):
			return strings.TrimPrefix(arg, "--port=")
		case strings.HasPrefix(arg, "-port="):
			return strings.TrimPrefix(arg, "-port=")
		case i == 1:
			if n, err := strconv.Atoi(arg); err == nil && n > 0 && n < 65536 {
				return arg
			}
		}
	}
	if v := os.Getenv("FLOCI_PORT"); v != "" {
		return v
	}
	if v := os.Getenv("PORT"); v != "" {
		return v
	}
	return defaultPort
}

func pickDataDir() string {
	for _, key := range []string{"FLOCI_DATA", "FLOCI_DATA_DIR", "DATA_DIR"} {
		if v := os.Getenv(key); v != "" {
			return v
		}
	}
	return defaultDataDir
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/")
	if p != "" && strings.HasSuffix(p, "/") {
		trimmed := strings.TrimSuffix(p, "/")
		if !strings.Contains(trimmed, "/") {
			p = trimmed // a trailing slash on the bucket name targets the bucket itself
		}
	}
	if p == "" {
		switch r.Method {
		case http.MethodGet, http.MethodHead:
			s.listBuckets(w, r)
		default:
			s.errXML(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "The specified method is not allowed against this resource.", "/")
		}
		return
	}
	bucket, key, isObject := splitPath(p)
	if !isObject {
		s.bucketRoute(w, r, bucket)
		return
	}
	s.objectRoute(w, r, bucket, key)
}

func splitPath(p string) (string, string, bool) {
	i := strings.IndexByte(p, '/')
	if i < 0 {
		return p, "", false
	}
	return p[:i], p[i+1:], true
}

func validBucket(bucket string) bool {
	if bucket == "" || bucket == "." || bucket == ".." {
		return false
	}
	return !strings.HasPrefix(bucket, ".")
}

func (s *server) bucketRoute(w http.ResponseWriter, r *http.Request, bucket string) {
	if !validBucket(bucket) {
		s.errXML(w, http.StatusBadRequest, "InvalidBucketName", "The specified bucket is not valid.", "/"+bucket)
		return
	}
	switch r.Method {
	case http.MethodPut:
		s.createBucket(w, r, bucket)
	case http.MethodGet, http.MethodHead:
		if !s.bucketExists(bucket) {
			s.errXML(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist", "/"+bucket)
			return
		}
		q := r.URL.Query()
		if _, ok := q["location"]; ok {
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprintf(w, "%s<LocationConstraint xmlns=%q></LocationConstraint>", xml.Header, xmlNS)
			return
		}
		if _, ok := q["versioning"]; ok {
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprintf(w, "%s<VersioningConfiguration xmlns=%q></VersioningConfiguration>", xml.Header, xmlNS)
			return
		}
		s.listObjects(w, r, bucket)
	case http.MethodDelete:
		s.deleteBucket(w, r, bucket)
	default:
		s.errXML(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "The specified method is not allowed against this resource.", "/"+bucket)
	}
}

func (s *server) createBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := os.MkdirAll(filepath.Join(s.dataDir, bucket), 0755); err != nil {
		s.errXML(w, http.StatusInternalServerError, "InternalError", "We encountered an internal error. Please try again.", "/"+bucket)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *server) deleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	bp := filepath.Join(s.dataDir, bucket)
	st, err := os.Stat(bp)
	if err != nil || !st.IsDir() {
		s.errXML(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist", "/"+bucket)
		return
	}
	if entries, rerr := os.ReadDir(bp); rerr == nil && len(entries) > 0 {
		s.errXML(w, http.StatusConflict, "BucketNotEmpty", "The bucket you tried to delete is not empty", "/"+bucket)
		return
	}
	if rerr := os.Remove(bp); rerr != nil {
		s.errXML(w, http.StatusInternalServerError, "InternalError", "We encountered an internal error. Please try again.", "/"+bucket)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) listBuckets(w http.ResponseWriter, r *http.Request) {
	var buckets []xmlBucket
	if entries, err := os.ReadDir(s.dataDir); err == nil {
		for _, e := range entries {
			if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
				continue
			}
			info, ierr := e.Info()
			if ierr != nil {
				continue
			}
			buckets = append(buckets, xmlBucket{
				Name:         e.Name(),
				CreationDate: info.ModTime().UTC().Format(iso8601Millis),
			})
		}
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].Name < buckets[j].Name })
	writeXML(w, http.StatusOK, listAllMyBucketsResult{
		Xmlns:   xmlNS,
		Owner:   xmlOwner{ID: "floci", DisplayName: "floci"},
		Buckets: xmlBuckets{Bucket: buckets},
	})
}

func (s *server) objectRoute(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if !validBucket(bucket) {
		s.errXML(w, http.StatusBadRequest, "InvalidBucketName", "The specified bucket is not valid.", "/"+bucket+"/"+key)
		return
	}
	if r.Method == http.MethodPut && r.Header.Get("x-amz-copy-source") != "" {
		s.copyObject(w, r, bucket, key)
		return
	}
	switch r.Method {
	case http.MethodPut:
		s.putObject(w, r, bucket, key)
	case http.MethodGet, http.MethodHead:
		s.getObject(w, r, bucket, key, r.Method == http.MethodHead)
	case http.MethodDelete:
		s.deleteObject(w, r, bucket, key)
	default:
		s.errXML(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "The specified method is not allowed against this resource.", "/"+bucket+"/"+key)
	}
}

// putObject implements ticket-007: PUT /{bucket}/{key}.
// The body is streamed to <dataDir>/<bucket>/<key> while its MD5 is computed;
// the response carries the quoted lowercase-hex digest in the ETag header.
func (s *server) putObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if !s.bucketExists(bucket) {
		s.errXML(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist", "/"+bucket)
		return
	}
	if key == "" {
		s.createBucket(w, r, bucket)
		return
	}
	resource := "/" + bucket + "/" + key

	var wantSum []byte
	if h := strings.TrimSpace(r.Header.Get("Content-MD5")); h != "" {
		digest, derr := base64.StdEncoding.DecodeString(h)
		if derr != nil || len(digest) != 16 {
			s.errXML(w, http.StatusBadRequest, "InvalidDigest", "The Content-MD5 you specified was invalid.", resource)
			return
		}
		wantSum = digest
	}

	target, ok := s.objectPath(bucket, key)
	if !ok {
		s.errXML(w, http.StatusBadRequest, "InvalidArgument", "Invalid Argument", resource)
		return
	}

	hash := md5.New()
	body := decodeBody(r)
	var copyErr error
	if strings.HasSuffix(key, "/") {
		// A key ending in "/" becomes a folder-marker directory on disk.
		if err := os.MkdirAll(target, 0755); err != nil {
			s.errXML500(w, resource)
			return
		}
		_, copyErr = io.Copy(hash, body)
	} else {
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			s.errXML500(w, resource)
			return
		}
		f, cerr := os.Create(target)
		if cerr != nil {
			s.errXML500(w, resource)
			return
		}
		_, copyErr = io.Copy(io.MultiWriter(f, hash), body)
		if closeErr := f.Close(); copyErr == nil && closeErr != nil {
			copyErr = closeErr
		}
		if copyErr != nil {
			os.Remove(target)
		}
	}
	if copyErr != nil {
		s.errXML500(w, resource)
		return
	}

	sum := hash.Sum(nil)
	if wantSum != nil && !bytes.Equal(wantSum, sum) {
		os.Remove(target)
		s.errXML(w, http.StatusBadRequest, "BadDigest", "The Content-MD5 you specified did not match what we received.", resource)
		return
	}
	w.Header()["ETag"] = []string{"` + `\"" // placeholder
	_ = 0
}
