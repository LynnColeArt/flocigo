package s3

// Storage contract between the S3 protocol adapter and the floci kernel, as
// well as a self-contained in-memory implementation used by tests and the
// standalone binary.

import (
    "bytes"
    "crypto/md5"
    "crypto/rand"
    "encoding/hex"
    "errors"
    "sort"
    "strconv"
    "sync"
    "time"
)

const minPartSize = 5 * 1024 * 1024 // S3: every part except the last must exceed 5 MiB

// ObjectInfo describes a stored object.
type ObjectInfo struct {
    Key          string
    Size         int64
    ETag         string // unquoted hex; multipart ETags carry the "-N" suffix
    LastModified time.Time
    Metadata     map[string]string
}

// BucketInfo describes a bucket.
type BucketInfo struct {
    Name         string
    CreationDate time.Time
    Location     string
}

// PartInfo describes an uploaded part.
type PartInfo struct {
    PartNumber   int
    ETag         string
    Size         int64
    LastModified time.Time
}

// UploadSummary describes an in-progress multipart upload.
type UploadSummary struct {
    Key       string
    UploadID  string
    Initiated time.Time
}

// PartSpec names a part (and its ETag) chosen by a CompleteMultipartUpload.
type PartSpec struct {
    PartNumber int
    ETag       string
}

// Backend error sentinels; the server maps them onto S3 error codes.
var (
    ErrNoSuchBucket            = errors.New("bucket does not exist")
    ErrNoSuchKey               = errors.New("key does not exist")
    ErrNoSuchUpload            = errors.New("multipart upload does not exist")
    ErrBucketAlreadyOwnedByYou = errors.New("bucket already owned by the caller")
    ErrBucketAlreadyExists     = errors.New("bucket name already in use")
    ErrBucketNotEmpty          = errors.New("bucket is not empty")
    ErrInvalidPart             = errors.New("part missing or ETag mismatch")
    ErrInvalidPartOrder        = errors.New("parts not ordered by ascending part number")
    ErrEntityTooSmall          = errors.New("non-final parts smaller than the minimum size")
)

// Store is the backend the protocol adapter talks to. The floci kernel
// storage layer implements this contract; MemStore is a reference
// implementation used for tests and standalone runs.
type Store interface {
    ListBuckets() []BucketInfo
    BucketExists(bucket string) bool
    CreateBucket(bucket, location string) error
    DeleteBucket(bucket string) error
    GetBucketLocation(bucket string) string

    GetObject(bucket, key string) (*ObjectInfo, []byte, error)
    PutObject(bucket, key string, data []byte, meta map[string]string) (*ObjectInfo, error)
    DeleteObject(bucket, key string) error
    AllObjects(bucket string) ([]ObjectInfo, error)

    CreateUpload(bucket, key string, meta map[string]string) (uploadID string, err error)
    PutPart(bucket, key, uploadID string, partNumber int, data []byte) (PartInfo, error)
    ListParts(bucket, key, uploadID string) ([]PartInfo, error)
    ListUploads(bucket string) ([]UploadSummary, error)
    CompleteUpload(bucket, key, uploadID string, parts []PartSpec) (*ObjectInfo, error)
    AbortUpload(bucket, key, uploadID string) error
}

// MemStore is a concurrency-safe in-memory Store.
type MemStore struct {
    mu      sync.Mutex
    buckets map[string]*memBucket
    now     func() time.Time
}

type memBucket struct {
    info    BucketInfo
    objects map[string]*memObject
    uploads map[string]*memUpload
}

type memObject struct {
    info ObjectInfo
    data []byte
}

type memUpload struct {
    key       string
    meta      map[string]string
    initiated time.Time
    parts     map[int]*memPart
}

type memPart struct {
    etag string
    data []byte
    at   time.Time
}

// NewMemStore builds an empty in-memory store.
func NewMemStore() *MemStore {
    return &MemStore{
        buckets: map[string]*memBucket{},
        now:     time.Now,
    }
}

func (m *MemStore) ListBuckets() []BucketInfo {
    m.mu.Lock()
    defer m.mu.Unlock()
    out := make([]BucketInfo, 0, len(m.buckets))
    for _, b := range m.buckets {
        out = append(out, b.info)
    }
    sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
    return out
}

func (m *MemStore) BucketExists(bucket string) bool {
    m.mu.Lock()
    defer m.mu.Unlock()
    _, ok := m.buckets[bucket]
    return ok
}

func (m *MemStore) CreateBucket(bucket, location string) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    if _, ok := m.buckets[bucket]; ok {
        return ErrBucketAlreadyOwnedByYou
    }
    if location == "" {
        location = "us-east-1"
    }
    m.buckets[bucket] = &memBucket{
        info:    BucketInfo{Name: bucket, CreationDate: m.now().UTC(), Location: location},
        objects: map[string]*memObject{},
        uploads: map[string]*memUpload{},
    }
    return nil
}

func (m *MemStore) GetBucketLocation(bucket string) string {
    m.mu.Lock()
    defer m.mu.Unlock()
    if b, ok := m.buckets[bucket]; ok {
        return b.info.Location
    }
    return ""
}

func (m *MemStore) DeleteBucket(bucket string) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    b, ok := m.buckets[bucket]
    if !ok {
        return ErrNoSuchBucket
    }
    if len(b.objects) > 0 || len(b.uploads) > 0 {
        return ErrBucketNotEmpty
    }
    delete(m.buckets, bucket)
    return nil
}

func (m *MemStore) GetObject(bucket, key string) (*ObjectInfo, []byte, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    b, ok := m.buckets[bucket]
    if !ok {
        return nil, nil, ErrNoSuchBucket
    }
    o, ok := b.objects[key]
    if !ok {
        return nil, nil, ErrNoSuchKey
    }
    info := o.info
    return &info, o.data, nil
}

func (m *MemStore) PutObject(bucket, key string, data []byte, meta map[string]string) (*ObjectInfo, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    b, ok := m.buckets[bucket]
    if !ok {
        return nil, ErrNoSuchBucket
    }
    sum := md5.Sum(data)
    info := ObjectInfo{
        Key:          key,
        Size:         int64(len(data)),
        ETag:         hex.EncodeToString(sum[:]),
        LastModified: m.now().UTC(),
        Metadata:     cloneMeta(meta),
    }
    b.objects[key] = &memObject{info: info, data: data}
    out := info
    return &out, nil
}

func (m *MemStore) DeleteObject(bucket, key string) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    b, ok := m.buckets[bucket]
    if !ok {
        return ErrNoSuchBucket
    }
    delete(b.objects, key)
    return nil
}

func (m *MemStore) AllObjects(bucket string) ([]ObjectInfo, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    b, ok := m.buckets[bucket]
    if !ok {
        return nil, ErrNoSuchBucket
    }
    keys := make([]string, 0, len(b.objects))
    for key := range b.objects {
        keys = append(keys, key)
    }
    sort.Strings(keys)
    out := make([]ObjectInfo, 0, len(keys))
    for _, key := range keys {
        out = append(out, b.objects[key].info)
    }
    return out, nil
}

func (m *MemStore) CreateUpload(bucket, key string, meta map[string]string) (string, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    b, ok := m.buckets[bucket]
    if !ok {
        return "", ErrNoSuchBucket
    }
    id := randomHex(16)
    b.uploads[id] = &memUpload{
        key:       key,
        meta:      cloneMeta(meta),
        initiated: m.now().UTC(),
        parts:     map[int]*memPart{},
    }
    return id, nil
}

// findUpload requires m.mu to be held by the caller.
func (m *MemStore) findUpload(bucket, key, uploadID string) (*memBucket, *memUpload, error) {
    b, ok := m.buckets[bucket]
    if !ok {
        return nil, nil, ErrNoSuchBucket
    }
    u, ok := b.uploads[uploadID]
    if !ok || u.key != key {
        return nil, nil, ErrNoSuchUpload
    }
    return b, u, nil
}

func (m *MemStore) PutPart(bucket, key, uploadID string, partNumber int, data []byte) (PartInfo, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    _, u, err := m.findUpload(bucket, key, uploadID)
    if err != nil {
        return PartInfo{}, err
    }
    sum := md5.Sum(data)
    at := m.now().UTC()
    u.parts[partNumber] = &memPart{etag: hex.EncodeToString(sum[:]), data: data, at: at}
    return PartInfo{PartNumber: partNumber, ETag: hex.EncodeToString(sum[:]), Size: int64(len(data)), LastModified: at}, nil
}

func (m *MemStore) ListParts(bucket, key, uploadID string) ([]PartInfo, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    _, u, err := m.findUpload(bucket, key, uploadID)
    if err != nil {
        return nil, err
    }
    numbers := make([]int, 0, len(u.parts))
    for n := range u.parts {
        numbers = append(numbers, n)
    }
    sort.Ints(numbers)
    out := make([]PartInfo, 0, len(numbers))
    for _, n := range numbers {
        p := u.parts[n]
        out = append(out, PartInfo{PartNumber: n, ETag: p.etag, Size: int64(len(p.data)), LastModified: p.at})
    }
    return out, nil
}

func (m *MemStore) ListUploads(bucket string) ([]UploadSummary, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    b, ok := m.buckets[bucket]
    if !ok {
        return nil, ErrNoSuchBucket
    }
    type uk struct{ key, id string }
    ids := make([]uk, 0, len(b.uploads))
    for id, u := range b.uploads {
        ids = append(ids, uk{u.key, id})
    }
    sort.Slice(ids, func(i, j int) bool {
        if ids[i].key != ids[j].key {
            return ids[i].key < ids[j].key
        }
        return ids[i].id < ids[j].id
    })
    out := make([]UploadSummary, 0, len(ids))
    for _, pair := range ids {
        u := b.uploads[pair.id]
        out = append(out, UploadSummary{Key: u.key, UploadID: pair.id, Initiated: u.initiated})
    }
    return out, nil
}

func (m *MemStore) CompleteUpload(bucket, key, uploadID string, parts []PartSpec) (*ObjectInfo, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    b, u, err := m.findUpload(bucket, key, uploadID)
    if err != nil {
        return nil, err
    }
    if len(parts) == 0 {
        return nil, ErrInvalidPart
    }
    previous := 0
    var digests, payload bytes.Buffer
    for _, spec := range parts {
        if spec.PartNumber <= previous {
            return nil, ErrInvalidPartOrder
        }
        previous = spec.PartNumber
        part, ok := u.parts[spec.PartNumber]
        if !ok || unquoteETag(spec.ETag) != part.etag {
            return nil, ErrInvalidPart
        }
        raw, derr := hex.DecodeString(part.etag)
        if derr != nil {
            return nil, ErrInvalidPart
        }
        digests.Write(raw)
    }
    for i := 0; i < len(parts)-1; i++ {
        if int64(len(u.parts[parts[i].PartNumber].data)) < minPartSize {
            return nil, ErrEntityTooSmall
        }
    }
    for _, spec := range parts {
        payload.Write(u.parts[spec.PartNumber].data)
    }
    sum := md5.Sum(digests.Bytes())
    info := ObjectInfo{
        Key:          key,
        Size:         int64(payload.Len()),
        ETag:         hex.EncodeToString(sum[:]) + "-" + strconv.Itoa(len(parts)),
        LastModified: m.now().UTC(),
        Metadata:     cloneMeta(u.meta),
    }
    b.objects[key] = &memObject{info: info, data: append([]byte(nil), payload.Bytes()...)}
    delete(b.uploads, uploadID)
    out := info
    return &out, nil
}

func (m *MemStore) AbortUpload(bucket, key, uploadID string) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    b, _, err := m.findUpload(bucket, key, uploadID)
    if err != nil {
        return err
    }
    delete(b.uploads, uploadID)
    return nil
}

func cloneMeta(meta map[string]string) map[string]string {
    out := make(map[string]string, len(meta))
    for k, v := range meta {
        out[k] = v
    }
    return out
}

// randomHex returns n random bytes hex encoded (request IDs, upload IDs).
func randomHex(n int) string {
    b := make([]byte, n)
    if _, err := rand.Read(b); err != nil {
        return hex.EncodeToString(b) + strconv.FormatInt(time.Now().UnixNano(), 16)
    }
    return hex.EncodeToString(b)
}
