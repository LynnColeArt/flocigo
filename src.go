// floci-go - minimal S3-compatible, in-memory object store.
//
// Ticket floci-go/ticket-008: implement GET /{bucket}/{key}, serving the
// stored object body with ETag and Content-Length headers, plus the S3
// wire-format error paths NoSuchKey and NoSuchBucket (404). The minimal
// supporting surface (PUT/DELETE bucket + object, HEAD, listings) is
// included so the GET endpoint can be exercised end to end on the wire.

package main

import (
    "crypto/md5"
    "crypto/rand"
    "encoding/base64"
    "encoding/hex"
    "encoding/xml"
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

// ---------------------------------------------------------------------------
// Data model
// ---------------------------------------------------------------------------

type s3Object struct {
    data         []byte
    etag         string // quoted MD5 hex (e.g. d41d8cd98f00b204e9800998ecf8427e)
    contentType  string
    lastModified time.Time
}

type s3Bucket struct {
    name         string
    creationDate time.Time
    objects      map[string]*s3Object
}

type memStore struct {
    mu      sync.RWMutex
    buckets map[string]*s3Bucket
}

func newMemStore() *memStore {
    return &memStore{buckets: make(map[string]*s3Bucket)}
}

// store is the process-wide store backing the default handler.
var store = newMemStore()

// NewHandler returns an HTTP handler implementing the floci S3 subset,
// backed by a fresh in-memory store.
func NewHandler() http.Handler {
    return newMemStore()
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

const (
    isoTimestamp = "2006-01-02T15:04:05.000Z"
    xmlNamespace = "http://s3.amazonaws.com/doc/2006-03-01/"
)

func requestID() string {
    b := make([]byte, 16)
    _, _ = rand.Read(b)
    return strings.ToUpper(hex.EncodeToString(b))
}

func hostID() string {
    b := make([]byte, 32)
    _, _ = rand.Read(b)
    return base64.StdEncoding.EncodeToString(b)
}

type s3ErrorXML struct {
    XMLName    xml.Name `xml:"Error"`
    Code       string   `xml:"Code"`
    Message    string   `xml:"Message"`
    Key        string   `xml:"Key,omitempty"`
    BucketName string   `xml:"BucketName,omitempty"`
    RequestID  string   `xml:"RequestId"`
    HostID     string   `xml:"HostId"`
}

// writeError renders an S3-style XML error document.
func writeError(w http.ResponseWriter, status int, code, message, bucketName, key string) {
    id := requestID()
    host := hostID()
    e := s3ErrorXML{Code: code, Message: message, RequestID: id, HostID: host}
    switch code {
    case "NoSuchKey":
        e.Key = key
    case "NoSuchBucket", "BucketAlreadyExists", "BucketNotEmpty":
        e.BucketName = bucketName
    default:
        if key != "" {
            e.Key = key
        }
    }
    body, err := xml.Marshal(e)
    if err != nil {
        http.Error(w, code, status)
        return
    }
    out := make([]byte, 0, len(xml.Header)+len(body))
    out = append(out, xml.Header...)
    out = append(out, body...)
    h := w.Header()
    h.Set("Content-Type", "application/xml")
    h.Set("x-amz-request-id", id)
    h.Set("x-amz-id-2", host)
    h.Set("Content-Length", strconv.Itoa(len(out)))
    w.WriteHeader(status)
    _, _ = w.Write(out)
}

// writeXML renders a success XML document (listing responses).
func writeXML(w http.ResponseWriter, status int, v interface{}) {
    body, err := xml.Marshal(v)
    if err != nil {
        http.Error(w, "InternalError", http.StatusInternalServerError)
        return
    }
    out := make([]byte, 0, len(xml.Header)+len(body))
    out = append(out, xml.Header...)
    out = append(out, body...)
    h := w.Header()
    h.Set("Content-Type", "application/xml")
    h.Set("Content-Length", strconv.Itoa(len(out)))
    h.Set("x-amz-request-id", requestID())
    w.WriteHeader(status)
    _, _ = w.Write(out)
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

func (s *memStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    path := strings.TrimPrefix(r.URL.Path, "/")
    var bucketName, key string
    if i := strings.IndexByte(path, '/'); i >= 0 {
        bucketName, key = path[:i], path[i+1:]
    } else {
        bucketName = path
    }

    switch {
    case bucketName == "": // service-level
        if r.Method == http.MethodGet {
            s.listBuckets(w, r)
            return
        }
        if r.Method == http.MethodHead {
            w.WriteHeader(http.StatusOK)
            return
        }
        writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed",
            "The specified method is not allowed against this resource.", "", "")
    case key == "": // bucket-level
        switch r.Method {
        case http.MethodGet:
            s.listObjects(w, r, bucketName)
        case http.MethodHead:
            s.headBucket(w, r, bucketName)
        case http.MethodPut:
            s.createBucket(w, r, bucketName)
        case http.MethodDelete:
            s.deleteBucket(w, r, bucketName)
        default:
            writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed",
                "The specified method is not allowed against this resource.", bucketName, "")
        }
    default: // object-level
        switch r.Method {
        case http.MethodGet:
            s.getObject(w, r, bucketName, key)
        case http.MethodHead:
            s.getObject(w, r, bucketName, key)
        case http.MethodPut:
            s.putObject(w, r, bucketName, key)
        case http.MethodDelete:
            s.deleteObject(w, r, bucketName, key)
        default:
            writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed",
                "The specified method is not allowed against this resource.", bucketName, key)
        }
    }
}

// ---------------------------------------------------------------------------
// Object operations (ticket-008)
// ---------------------------------------------------------------------------

// getObject implements GET/HEAD /{bucket}/{key}. It serves the stored body
// with ETag, Content-Length, Content-Type and Last-Modified headers, and
// answers NoSuchBucket / NoSuchKey (404) on the missing-bucket and
// missing-key error paths, matching the S3 wire format.
func (s *memStore) getObject(w http.ResponseWriter, r *http.Request, bucketName, key string) {
    head := r.Method == http.MethodHead

    s.mu.RLock()
    bkt := s.buckets[bucketName]
    if bkt == nil {
        s.mu.RUnlock()
        writeError(w, http.StatusNotFound, "NoSuchBucket",
            "The specified bucket does not exist", bucketName, key)
        return
    }
    obj, ok := bkt.objects[key]
    if !ok {
        s.mu.RUnlock()
        writeError(w, http.StatusNotFound, "NoSuchKey",
            "The specified key does not exist.", bucketName, key)
        return
    }
    data, etag, contentType, lastModified := obj.data, obj.etag, obj.contentType, obj.lastModified
    s.mu.RUnlock()

    h := w.Header()
    h.Set("ETag", etag)
    h.Set("Content-Type", contentType)
    h.Set("Content-Length", strconv.Itoa(len(data)))
    h.Set("Last-Modified", lastModified.UTC().Format(http.TimeFormat))
    h.Set("x-amz-request-id", requestID())
    w.WriteHeader(http.StatusOK)
    if !head {
        _, _ = w.Write(data)
    }
}

func (s *memStore) putObject(w http.ResponseWriter, r *http.Request, bucketName, key string) {
    data, err := io.ReadAll(r.Body)
    if err != nil {
        writeError(w, http.StatusBadRequest, "IncompleteBody",
            "The request body terminated unexpectedly", bucketName, key)
        return
    }
    sum := md5.Sum(data)
    q := `"`
    etag := q + hex.EncodeToString(sum[:]) + q
    contentType := r.Header.Get("Content-Type")
    if contentType == "" {
        contentType = "binary/octet-stream"
    }

    s.mu.Lock()
    bkt := s.buckets[bucketName]
    if bkt == nil {
        s.mu.Unlock()
        writeError(w, http.StatusNotFound, "NoSuchBucket",
            "The specified bucket does not exist", bucketName, key)
        return
    }
    bkt.objects[key] = &s3Object{
        data:         data,
        etag:         etag,
        contentType:  contentType,
        lastModified: time.Now().UTC(),
    }
    s.mu.Unlock()

    w.Header().Set("ETag", etag)
    w.Header().Set("x-amz-request-id", requestID())
    w.WriteHeader(http.StatusOK)
}

// ---------------------------------------------------------------------------
// Bucket operations
// ---------------------------------------------------------------------------

func (s *memStore) createBucket(w http.ResponseWriter, r *http.Request, bucketName string) {
    _, _ = io.Copy(io.Discard, r.Body)
    s.mu.Lock()
    if _, exists := s.buckets[bucketName]; !exists {
        s.buckets[bucketName] = &s3Bucket{
            name:         bucketName,
            creationDate: time.Now().UTC(),
            objects:      make(map[string]*s3Object),
        }
    }
    s.mu.Unlock()
    w.Header().Set("Location", "/"+bucketName)
    w.Header().Set("x-amz-request-id", requestID())
    w.WriteHeader(http.StatusOK)
}

func (s *memStore) headBucket(w http.ResponseWriter, r *http.Request, bucketName string) {
    s.mu.RLock()
    _, exists := s.buckets[bucketName]
    s.mu.RUnlock()
    if !exists {
        writeError(w, http.StatusNotFound, "NoSuchBucket",
            "The specified bucket does not exist", bucketName, "")
        return
    }
    w.Header().Set("x-amz-bucket-region", "us-east-1")
    w.Header().Set("x-amz-request-id", requestID())
    w.WriteHeader(http.StatusOK)
}

func (s *memStore) deleteObject(w http.ResponseWriter, r *http.Request, bucketName, key string) {
    s.mu.Lock()
    bkt := s.buckets[bucketName]
    if bkt == nil {
        s.mu.Unlock()
        writeError(w, http.StatusNotFound, "NoSuchBucket",
            "The specified bucket does not exist", bucketName, key)
        return
    }
    delete(bkt.objects, key)
    s.mu.Unlock()
    w.Header().Set("x-amz-request-id", requestID())
    w.WriteHeader(http.StatusNoContent)
}

func (s *memStore) deleteBucket(w http.ResponseWriter, r *http.Request, bucketName string) {
    s.mu.Lock()
    bkt := s.buckets[bucketName]
    if bkt == nil {
        s.mu.Unlock()
        writeError(w, http.StatusNotFound, "NoSuchBucket",
            "The specified bucket does not exist", bucketName, "")
        return
    }
    if len(bkt.objects) > 0 {
        s.mu.Unlock()
        writeError(w, http.StatusConflict, "BucketNotEmpty",
            "The bucket you tried to delete is not empty", bucketName, "")
        return
    }
    delete(s.buckets, bucketName)
    s.mu.Unlock()
    w.Header().Set("x-amz-request-id", requestID())
    w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Listings
// ---------------------------------------------------------------------------

type bucketEntryXML struct {
    Name         string `xml:"Name"`
    CreationDate string `xml:"CreationDate"`
}

type ownerXML struct {
    ID          string `xml:"ID"`
    DisplayName string `xml:"DisplayName"`
}

type listAllMyBucketsResultXML struct {
    XMLName xml.Name `xml:"ListAllMyBucketsResult"`
    Xmlns   string   `xml:"xmlns,attr"`
    Owner   ownerXML `xml:"Owner"`
    Buckets struct {
        Bucket []bucketEntryXML `xml:"Bucket"`
    } `xml:"Buckets"`
}

type objectEntryXML struct {
    Key          string `xml:"Key"`
    LastModified string `xml:"LastModified"`
    ETag         string `xml:"ETag"`
    Size         int64  `xml:"Size"`
    StorageClass string `xml:"StorageClass"`
}

type listBucketResultXML struct {
    XMLName     xml.Name         `xml:"ListBucketResult"`
    Xmlns       string           `xml:"xmlns,attr"`
    Name        string           `xml:"Name"`
    Prefix      string           `xml:"Prefix"`
    Marker      string           `xml:"Marker"`
    MaxKeys     int              `xml:"MaxKeys"`
    Delimiter   string           `xml:"Delimiter,omitempty"`
    IsTruncated bool             `xml:"IsTruncated"`
    Contents    []objectEntryXML `xml:"Contents,omitempty"`
}

func (s *memStore) listBuckets(w http.ResponseWriter, r *http.Request) {
    s.mu.RLock()
    names := make([]string, 0, len(s.buckets))
    created := make(map[string]time.Time, len(s.buckets))
    for name, b := range s.buckets {
        names = append(names, name)
        created[name] = b.creationDate
    }
    s.mu.RUnlock()
    sort.Strings(names)

    res := listAllMyBucketsResultXML{
        Xmlns: xmlNamespace,
        Owner: ownerXML{ID: "floci", DisplayName: "floci"},
    }
    for _, name := range names {
        res.Buckets.Bucket = append(res.Buckets.Bucket, bucketEntryXML{
            Name:         name,
            CreationDate: created[name].UTC().Format(isoTimestamp),
        })
    }
    writeXML(w, http.StatusOK, res)
}

func (s *memStore) listObjects(w http.ResponseWriter, r *http.Request, bucketName string) {
    q := r.URL.Query()
    prefix := q.Get("prefix")
    marker := q.Get("marker")
    delimiter := q.Get("delimiter")
    maxKeys := 1000
    if mk := q.Get("max-keys"); mk != "" {
        if n, err := strconv.Atoi(mk); err == nil && n >= 0 {
            maxKeys = n
        }
    }

    s.mu.RLock()
    bkt := s.buckets[bucketName]
    if bkt == nil {
        s.mu.RUnlock()
        writeError(w, http.StatusNotFound, "NoSuchBucket",
            "The specified bucket does not exist", bucketName, "")
        return
    }
    keys := make([]string, 0, len(bkt.objects))
    for k := range bkt.objects {
        keys = append(keys, k)
    }
    sort.Strings(keys)

    var contents []objectEntryXML
    for _, k := range keys {
        if prefix != "" && !strings.HasPrefix(k, prefix) {
            continue
        }
        if marker != "" && k <= marker {
            continue
        }
        o := bkt.objects[k]
        contents = append(contents, objectEntryXML{
            Key:          k,
            LastModified: o.lastModified.UTC().Format(isoTimestamp),
            ETag:         o.etag,
            Size:         int64(len(o.data)),
            StorageClass: "STANDARD",
        })
    }
    s.mu.RUnlock()

    truncated := false
    if len(contents) > maxKeys {
        contents = contents[:maxKeys]
        truncated = true
    }

    res := listBucketResultXML{
        Xmlns:       xmlNamespace,
        Name:        bucketName,
        Prefix:      prefix,
        Marker:      marker,
        MaxKeys:     maxKeys,
        Delimiter:   delimiter,
        IsTruncated: truncated,
        Contents:    contents,
    }
    writeXML(w, http.StatusOK, res)
}

// ---------------------------------------------------------------------------
// Entrypoint
// ---------------------------------------------------------------------------

func main() {
    addr := os.Getenv("ADDR")
    if addr == "" {
        port := os.Getenv("PORT")
        if port == "" {
            port = "8080"
        }
        addr = ":" + port
    }
    srv := &http.Server{Addr: addr, Handler: store}
    log.Printf("floci-go: listening on %s", addr)
    if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
        log.Fatal(err)
    }
}
