// floci-go is an in-memory S3 clone (a Go port of floci).
//
// ticket-009: HEAD /{bucket}/{key} -- metadata headers only, no response body.
//
// RFC 9110 9.3.2: a server MUST NOT send a message body in a HEAD response.
// HEAD /{bucket}/{key} answers with exactly the header set that GET on the
// same resource would produce (Content-Length, Content-Type, quoted ETag,
// Last-Modified, Accept-Ranges, any x-amz-meta-* user metadata and
// x-amz-request-id) and suppresses the body -- for success and error
// responses (e.g. 404 NoSuchKey) alike.

package main

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	xmlHeader     = `<?xml version="1.0" encoding="UTF-8"?>` + "\n"
	listNS        = "http://s3.amazonaws.com/doc/2006-03-01/"
	defaultCT     = "binary/octet-stream"
	iso8601Layout = "2006-01-02T15:04:05.000Z07:00"
)

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

// Object is a stored S3 object plus its metadata.
type Object struct {
	Key          string
	Data         []byte
	ContentType  string
	MD5          string // hex digest, without surrounding quotes
	LastModified time.Time
	Metadata     map[string]string // lowercased header names, e.g. x-amz-meta-owner
}

// Bucket holds the objects belonging to one bucket.
type Bucket struct {
	Name         string
	CreationDate time.Time

	mu      sync.Mutex
	objects map[string]*Object
}

func newBucket(name string) *Bucket {
	return &Bucket{Name: name, CreationDate: time.Now().UTC(), objects: map[string]*Object{}}
}

func (b *Bucket) get(key string) *Object {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.objects[key]
}

func (b *Bucket) put(o *Object) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[o.Key] = o
}

func (b *Bucket) del(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.objects, key)
}

func (b *Bucket) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.objects)
}

func (b *Bucket) snapshot() map[string]*Object {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]*Object, len(b.objects))
	for k, v := range b.objects {
		out[k] = v
	}
	return out
}

// Store is the root of the object store.
type Store struct {
	mu      sync.RWMutex
	buckets map[string]*Bucket
}

func newStore() *Store { return &Store{buckets: map[string]*Bucket{}} }

func (s *Store) get(name string) *Bucket {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.buckets[name]
}

func (s *Store) create(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.buckets[name]; !ok {
		s.buckets[name] = newBucket(name)
	}
}

func (s *Store) remove(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.buckets, name)
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

// setRaw stores a header under its exact name. Unlike Header.Set it does not
// canonicalize, so ETag, x-amz-request-id and x-amz-meta-* keep the casing
// S3 (and floci via Node) puts on the wire.
func setRaw(h http.Header, name, value string) {
	h[name] = []string{value}
}

func newRequestID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strings.ToUpper(strconv.FormatInt(time.Now().UnixNano(), 36))
	}
	return strings.ToUpper(hex.EncodeToString(b[:]))
}

// splitPath splits /{bucket}/{key} into its two path-style parts; the key may
// itself contain further "/" characters.
func splitPath(p string) (bucket, key string) {
	p = strings.TrimPrefix(p, "/")
	if i := strings.IndexByte(p, '/'); i >= 0 {
		return p[:i], p[i+1:]
	}
	return p, ""
}

func xmlEsc(s string) string {
	return strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
		"'", "&apos;",
	).Replace(s)
}

func validBucketName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' && c != '.' {
			return false
		}
	}
	alnum := func(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') }
	if !alnum(name[0]) || !alnum(name[len(name)-1]) {
		return false
	}
	return !strings.Contains(name, "..")
}

func metadataOf(h http.Header) map[string]string {
	meta := map[string]string{}
	for name, vals := range h {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-amz-meta-") && len(vals) > 0 {
			meta[lower] = strings.Join(vals, ",")
		}
	}
	return meta
}

func contentTypeOf(r *http.Request) string {
	if ct := r.Header.Get("Content-Type"); ct != "" {
		return ct
	}
	return defaultCT
}

// setObjectHeaders writes the complete S3 object header set shared by GET and
// HEAD responses (this is the heart of ticket-009: HEAD reuses it verbatim
// and then writes no body).
func setObjectHeaders(h http.Header, o *Object) {
	h.Set("Content-Type", o.ContentType)
	h.Set("Content-Length", strconv.Itoa(len(o.Data)))
	setRaw(h, "ETag", `"`+o.MD5+`"`)
	h.Set("Last-Modified", o.LastModified.UTC().Format(http.TimeFormat))
	h.Set("Accept-Ranges", "bytes")
	names := make([]string, 0, len(o.Metadata))
	for k := range o.Metadata {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, name := range names {
		setRaw(h, name, o.Metadata[name])
	}
}

func errorBody(code, message, resource, rid string) string {
	return xmlHeader + "<Error><Code>" + xmlEsc(code) + "</Code><Message>" +
		xmlEsc(message) + "</Message><Resource>" + xmlEsc(resource) +
		"</Resource><RequestId>" + rid + "</RequestId></Error>"
}

// ---------------------------------------------------------------------------
// Server and routing
// ---------------------------------------------------------------------------

type Server struct {
	store *Store
}

func newServer() *Server { return &Server{store: newStore()} }

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rid := newRequestID()
	setRaw(w.Header(), "x-amz-request-id", rid)

	bucket, key := splitPath(r.URL.Path)

	switch r.Method {
	case http.MethodPut:
		if key == "" {
			s.putBucket(w, r, rid, bucket)
		} else {
			s.putObject(w, r, rid, bucket, key)
		}
	case http.MethodGet:
		if key == "" {
			s.getBucket(w, r, rid, bucket)
		} else {
			s.getObject(w, r, rid, bucket, key)
		}
	case http.MethodHead:
		if key == "" {
			s.headBucket(w, r, rid, bucket)
		} else {
			s.headObject(w, r, rid, bucket, key)
		}
	case http.MethodDelete:
		if key == "" {
			s.deleteBucket(w, r, rid, bucket)
		} else {
			s.deleteObject(w, r, rid, bucket, key)
		}
	default:
		s.sendError(w, r, rid, http.StatusMethodNotAllowed, "MethodNotAllowed",
			"The specified method is not allowed against this resource.", r.URL.Path)
	}
}

// ---------------------------------------------------------------------------
// Bucket handlers
// ---------------------------------------------------------------------------

func (s *Server) putBucket(w http.ResponseWriter, r *http.Request, rid, name string) {
	if !validBucketName(name) {
		s.sendError(w, r, rid, http.StatusBadRequest, "InvalidBucketName",
			"The specified bucket is not valid.", "/"+name)
		return
	}
	s.store.create(name)
	w.Header().Set("Location", "/"+name)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) headBucket(w http.ResponseWriter, r *http.Request, rid, name string) {
	if s.store.get(name) == nil {
		s.headersOnlyError(w, rid, http.StatusNotFound, "NoSuchBucket",
			"The specified bucket does not exist", "/"+name)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) deleteBucket(w http.ResponseWriter, r *http.Request, rid, name string) {
	b := s.store.get(name)
	if b == nil {
		s.sendError(w, r, rid, http.StatusNotFound, "NoSuchBucket",
			"The specified bucket does not exist", "/"+name)
		return
	}
	if b.count() > 0 {
		s.sendError(w, r, rid, http.StatusConflict, "BucketNotEmpty",
			"The bucket you tried to delete is not empty", "/"+name)
		return
	}
	s.store.remove(name)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) getBucket(w http.ResponseWriter, r *http.Request, rid, name string) {
	b := s.store.get(name)
	if b == nil {
		s.sendError(w, r, rid, http.StatusNotFound, "NoSuchBucket",
			"The specified bucket does not exist", "/"+name)
		return
	}

	q := r.URL.Query()
	prefix := q.Get("prefix")
	marker := q.Get("marker")
	delimiter := q.Get("delimiter")
	maxKeys := 1000
	if v := q.Get("max-keys"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			maxKeys = n
		}
	}

	objects := b.snapshot()
	keys := make([]string, 0, len(objects))
	for k := range objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	contents := make([]string, 0, len(keys))
	prefixes := make([]string, 0)
	truncated := false
	emitted := 0

	if maxKeys > 0 {
		for _, k := range keys {
			if prefix != "" && !strings.HasPrefix(k, prefix) {
				continue
			}
			if marker != "" && k <= marker {
				continue
			}
			if delimiter != "" {
				if i := strings.Index(k[len(prefix):], delimiter); i >= 0 {
					cp := k[:len(prefix)+i+len(delimiter)]
					if len(prefixes) == 0 || prefixes[len(prefixes)-1] != cp {
						if emitted >= maxKeys {
							truncated = true
							break
						}
						prefixes = append(prefixes, cp)
						emitted++
					}
					continue
				}
			}
			if emitted >= maxKeys {
				truncated = true
				break
			}
			o := objects[k]
			contents = append(contents, "<Contents><Key>"+xmlEsc(k)+"</Key><LastModified>"+
				o.LastModified.UTC().Format(iso8601Layout)+"</LastModified><ETag>&quot;"+
				o.MD5+"&quot;</ETag><Size>"+strconv.Itoa(len(o.Data))+
				"</Size><StorageClass>STANDARD</StorageClass></Contents>")
			emitted++
		}
	}

	var sb strings.Builder
	sb.WriteString(xmlHeader)
	sb.WriteString(`<ListBucketResult xmlns="` + listNS + `">`)
	sb.WriteString("<Name>" + xmlEsc(name) + "</Name>")
	sb.WriteString("<Prefix>" + xmlEsc(prefix) + "</Prefix>")
	sb.WriteString("<Marker>" + xmlEsc(marker) + "</Marker>")
	sb.WriteString("<MaxKeys>" + strconv.Itoa(maxKeys) + "</MaxKeys>")
	if delimiter != "" {
		sb.WriteString("<Delimiter>" + xmlEsc(delimiter) + "</Delimiter>")
	}
	sb.WriteString("<IsTruncated>" + strconv.FormatBool(truncated) + "</IsTruncated>")
	for _, c := range contents {
		sb.WriteString(c)
	}
	for _, cp := range prefixes {
		sb.WriteString("<CommonPrefixes><Prefix>" + xmlEsc(cp) + "</Prefix></CommonPrefixes>")
	}
	sb.WriteString("</ListBucketResult>")

	body := sb.String()
	h := w.Header()
	h.Set("Content-Type", "application/xml")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, body)
}

// ---------------------------------------------------------------------------
// Object handlers
// ---------------------------------------------------------------------------

func (s *Server) putObject(w http.ResponseWriter, r *http.Request, rid, name, key string) {
	b := s.store.get(name)
	if b == nil {
		s.sendError(w, r, rid, http.StatusNotFound, "NoSuchBucket",
			"The specified bucket does not exist", name+"/"+key)
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		s.sendError(w, r, rid, http.StatusBadRequest, "IncompleteBody",
			"The request body was not read successfully.", name+"/"+key)
		return
	}
	sum := md5.Sum(data)
	o := &Object{
		Key:          key,
		Data:         data,
		ContentType:  contentTypeOf(r),
		MD5:          hex.EncodeToString(sum[:]),
		LastModified: time.Now().UTC(),
		Metadata:     metadataOf(r.Header),
	}
	b.put(o)
	setRaw(w.Header(), "ETag", `"`+o.MD5+`"`)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) getObject(w http.ResponseWriter, r *http.Request, rid, name, key string) {
	b := s.store.get(name)
	if b == nil {
		s.sendError(w, r, rid, http.StatusNotFound, "NoSuchBucket",
			"The specified bucket does not exist", name+"/"+key)
		return
	}
	o := b.get(key)
	if o == nil {
		s.sendError(w, r, rid, http.StatusNotFound, "NoSuchKey",
			"The specified key does not exist.", name+"/"+key)
		return
	}
	setObjectHeaders(w.Header(), o)
	w.WriteHeader(http.StatusOK)
	w.Write(o.Data)
}

// headObject implements HEAD /{bucket}/{key} (ticket-009).
//
// The response carries exactly the headers GET would send, and no body at
// all: nothing is written after WriteHeader, so neither success nor error
// responses ever contain payload bytes.
func (s *Server) headObject(w http.ResponseWriter, r *http.Request, rid, name, key string) {
	b := s.store.get(name)
	if b == nil {
		s.headersOnlyError(w, rid, http.StatusNotFound, "NoSuchBucket",
			"The specified bucket does not exist", name+"/"+key)
		return
	}
	o := b.get(key)
	if o == nil {
		s.headersOnlyError(w, rid, http.StatusNotFound, "NoSuchKey",
			"The specified key does not exist.", name+"/"+key)
		return
	}
	setObjectHeaders(w.Header(), o)
	w.WriteHeader(http.StatusOK)
	// Headers only -- no body is ever written for HEAD.
}

func (s *Server) deleteObject(w http.ResponseWriter, r *http.Request, rid, name, key string) {
	b := s.store.get(name)
	if b == nil {
		s.sendError(w, r, rid, http.StatusNotFound, "NoSuchBucket",
			"The specified bucket does not exist", name+"/"+key)
		return
	}
	b.del(key)
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Error rendering
// ---------------------------------------------------------------------------

// sendError writes the XML error document; used by methods that may carry a
// response body.
func (s *Server) sendError(w http.ResponseWriter, r *http.Request, rid string, status int, code, message, resource string) {
	if r.Method == http.MethodHead {
		// Defensive: HEAD responses must not carry a body.
		s.headersOnlyError(w, rid, status, code, message, resource)
		return
	}
	body := errorBody(code, message, resource, rid)
	h := w.Header()
	h.Set("Content-Type", "application/xml")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	io.WriteString(w, body)
}

// headersOnlyError writes an error's headers with no body, as required for
// HEAD. The Content-Type and Content-Length the client would have received
// on GET are still advertised, mirroring floci/Express, which computes
// Content-Length for the suppressed body of a HEAD response.
func (s *Server) headersOnlyError(w http.ResponseWriter, rid string, status int, code, message, resource string) {
	body := errorBody(code, message, resource, rid)
	h := w.Header()
	h.Set("Content-Type", "application/xml")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	// no body
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "4569"
	}
	log.Printf("floci-go listening on :%s", port)
	if err := http.ListenAndServe(":"+port, newServer()); err != nil {
		log.Fatal(err)
	}
}
