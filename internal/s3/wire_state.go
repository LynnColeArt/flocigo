// wire_state.go holds the in-memory bucket/object/multipart state backing
// the S3 wire handlers compiled from router.go.

package s3

import (
    "sort"
    "strings"
    "sync"
    "time"
)

// wireObject is a single stored object.
type wireObject struct {
    Key          string
    Body         []byte
    ContentType  string
    ETag         string
    LastModified time.Time
    Metadata     map[string]string
}

// wirePart is a single uploaded part of a multipart upload.
type wirePart struct {
    Number       int
    Body         []byte
    ETag         string
    LastModified time.Time
}

// wireUpload is an in-progress multipart upload.
type wireUpload struct {
    ID          string
    Bucket      string
    Key         string
    ContentType string
    Parts       map[int]*wirePart
}

// wireBucket is a bucket together with its objects and in-flight uploads.
type wireBucket struct {
    Name    string
    Created time.Time
    Objects map[string]*wireObject
    Uploads map[string]*wireUpload
}

// wireState is the in-memory S3 state used by the wire handlers.
type wireState struct {
    mu      sync.Mutex
    buckets map[string]*wireBucket
}

// wires is the process-wide store backing the wire handlers.
var wires = newWireState()

func newWireState() *wireState {
    return &wireState{buckets: make(map[string]*wireBucket)}
}

// lookupBucket returns the named bucket or nil.
func (s *wireState) lookupBucket(name string) *wireBucket {
    s.mu.Lock()
    defer s.mu.Unlock()
    return s.buckets[name]
}

// createBucket creates the bucket and reports whether it was newly created.
func (s *wireState) createBucket(name string) (*wireBucket, bool) {
    s.mu.Lock()
    defer s.mu.Unlock()
    if bucket, ok := s.buckets[name]; ok {
        return bucket, false
    }
    bucket := &wireBucket{
        Name:    name,
        Created: time.Now().UTC(),
        Objects: make(map[string]*wireObject),
        Uploads: make(map[string]*wireUpload),
    }
    s.buckets[name] = bucket
    return bucket, true
}

// listBuckets returns every bucket ordered by name.
func (s *wireState) listBuckets() []*wireBucket {
    s.mu.Lock()
    defer s.mu.Unlock()
    names := make([]string, 0, len(s.buckets))
    for name := range s.buckets {
        names = append(names, name)
    }
    sort.Strings(names)
    buckets := make([]*wireBucket, 0, len(names))
    for _, name := range names {
        buckets = append(buckets, s.buckets[name])
    }
    return buckets
}

// listObjects returns the objects of a bucket whose keys start with prefix,
// ordered by key.
func (s *wireState) listObjects(bucketName, prefix string) []*wireObject {
    s.mu.Lock()
    defer s.mu.Unlock()
    bucket, ok := s.buckets[bucketName]
    if !ok {
        return nil
    }
    keys := make([]string, 0, len(bucket.Objects))
    for key := range bucket.Objects {
        if strings.HasPrefix(key, prefix) {
            keys = append(keys, key)
        }
    }
    sort.Strings(keys)
    objects := make([]*wireObject, 0, len(keys))
    for _, key := range keys {
        objects = append(objects, bucket.Objects[key])
    }
    return objects
}

// listUploads returns the bucket's in-flight multipart uploads ordered by key.
func (s *wireState) listUploads(bucketName string) []*wireUpload {
    s.mu.Lock()
    defer s.mu.Unlock()
    bucket, ok := s.buckets[bucketName]
    if !ok {
        return nil
    }
    uploads := make([]*wireUpload, 0, len(bucket.Uploads))
    for _, upload := range bucket.Uploads {
        uploads = append(uploads, upload)
    }
    sort.Slice(uploads, func(i, j int) bool {
        if uploads[i].Key != uploads[j].Key {
            return uploads[i].Key < uploads[j].Key
        }
        return uploads[i].ID < uploads[j].ID
    })
    return uploads
}

// putObject stores an object in the bucket and computes its MD5 ETag.
func (s *wireState) putObject(bucket *wireBucket, key string, body []byte, contentType string, metadata map[string]string) *wireObject {
    s.mu.Lock()
    defer s.mu.Unlock()
    object := &wireObject{
        Key:          key,
        Body:         body,
        ContentType:  contentType,
        ETag:         wireETag(body),
        LastModified: time.Now().UTC(),
        Metadata:     metadata,
    }
    bucket.Objects[key] = object
    return object
}

// getObject returns a stored object and whether it exists.
func (s *wireState) getObject(bucketName, key string) (*wireObject, bool) {
    s.mu.Lock()
    defer s.mu.Unlock()
    bucket, ok := s.buckets[bucketName]
    if !ok {
        return nil, false
    }
    object, ok := bucket.Objects[key]
    if !ok {
        return nil, false
    }
    return object, true
}

// deleteObject removes an object and reports whether it existed.
func (s *wireState) deleteObject(bucketName, key string) bool {
    s.mu.Lock()
    defer s.mu.Unlock()
    bucket, ok := s.buckets[bucketName]
    if !ok {
        return false
    }
    if _, ok := bucket.Objects[key]; !ok {
        return false
    }
    delete(bucket.Objects, key)
    return true
}

// deleteBucket removes an empty bucket. It reports whether the bucket
// existed and whether it was empty and therefore removed.
func (s *wireState) deleteBucket(name string) (bool, bool) {
    s.mu.Lock()
    defer s.mu.Unlock()
    bucket, ok := s.buckets[name]
    if !ok {
        return false, false
    }
    if len(bucket.Objects) > 0 || len(bucket.Uploads) > 0 {
        return true, false
    }
    delete(s.buckets, name)
    return true, true
}
