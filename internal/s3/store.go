// Package s3 implements Floci's S3 protocol adapter: bucket and object
// storage, path-style and virtual-hosted routing, XML list and multipart
// response construction, x-amz header handling and SigV2/SigV4 presigned
// request hooks.
package s3

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Identity constants for the single emulated account.
const (
	OwnerID       = "floci"
	OwnerDisplay  = "floci"
	DefaultRegion = "us-east-1"

	maxPartNumber = 10000
)

// Object is one stored object body plus the metadata S3 keeps alongside it.
type Object struct {
	Key                string
	Data               []byte
	ETag               string // quoted; ends in "-N" for multipart objects
	LastModified       time.Time
	ContentType        string
	ContentEncoding    string
	ContentLanguage    string
	ContentDisposition string
	CacheControl       string
	Expires            string
	StorageClass       string
	Metadata           map[string]string // x-amz-meta-* values, keys lowercased
	Tags               map[string]string
	PartSizes          []int64 // per-part sizes when the ETag ends in "-N"
}

// Size returns the body length in bytes.
func (o *Object) Size() int64 { return int64(len(o.Data)) }

// Part is one uploaded part of a multipart upload.
type Part struct {
	Number       int
	Data         []byte
	ETag         string // quoted
	LastModified time.Time
}

// MultipartUpload is an in-progress multipart upload.
type MultipartUpload struct {
	UploadID     string
	Bucket       string
	Key          string
	ContentType  string
	StorageClass string
	Metadata     map[string]string
	Initiated    time.Time

	parts map[int]*Part
}

// Bucket is one named bucket holding objects and in-progress uploads.
type Bucket struct {
	Name       string
	Created    time.Time
	Versioning string
	Tags       map[string]string

	objects map[string]*Object
	uploads map[string]*MultipartUpload
}

// PartSpec selects one part of CompleteMultipartUpload.
type PartSpec struct {
	Number int
	ETag   string
}

// UploadOpts carries the init-time metadata of a multipart upload.
type UploadOpts struct {
	ContentType  string
	StorageClass string
	Metadata     map[string]string
}

// ListResult is the outcome of a ListObjects scan.
type ListResult struct {
	Objects    []*Object
	Prefixes   []string
	Truncated  bool
	NextMarker string
}

// UploadList is the outcome of a ListMultipartUploads scan.
type UploadList struct {
	Uploads   []*MultipartUpload
	Prefixes  []string
	Truncated bool
	NextKey   string
	NextID    string
}

// Store is the concurrent in-memory bucket/object database behind the S3
// HTTP surface.
type Store struct {
	mu      sync.RWMutex
	buckets map[string]*Bucket

	// OnObjectPut fires (after the lock is released) following a put, copy
	// or successful multipart completion. Event notification hook.
	OnObjectPut func(bucket, key string)
	// OnObjectDelete fires after every DeleteObject.
	OnObjectDelete func(bucket, key string)
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{buckets: make(map[string]*Bucket)}
}

// CreateBucket validates the name and creates an empty bucket.
func (s *Store) CreateBucket(name string) error {
	if !validBucketName(name) {
		return &s3Error{
			Status:  400,
			Code:    "InvalidBucketName",
			Message: "The specified bucket is not valid.",
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.buckets[name]; exists {
		return &s3Error{
			Status:  409,
			Code:    "BucketAlreadyExists",
			Message: "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.",
		}
	}
	s.buckets[name] = &Bucket{
		Name:    name,
		Created: time.Now().UTC(),
		Tags:    map[string]string{},
		objects: map[string]*Object{},
		uploads: map[string]*MultipartUpload{},
	}
	return nil
}

// GetBucket looks up a bucket by name.
func (s *Store) GetBucket(name string) (*Bucket, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.buckets[name]
	return b, ok
}

// DeleteBucket removes an empty bucket.
func (s *Store) DeleteBucket(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.buckets[name]
	if !ok {
		return errNoSuchBucket()
	}
	if len(b.objects) > 0 || len(b.uploads) > 0 {
		return &s3Error{
			Status:  409,
			Code:    "BucketNotEmpty",
			Message: "The bucket you tried to delete is not empty",
		}
	}
	delete(s.buckets, name)
	return nil
}

// ListBuckets returns every bucket sorted by name.
func (s *Store) ListBuckets() []*Bucket {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Bucket, 0, len(s.buckets))
	for _, b := range s.buckets {
		out = append(out, &Bucket{
			Name:       b.Name,
			Created:    b.Created,
			Versioning: b.Versioning,
			Tags:       copyTags(b.Tags),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// PutObject stores obj in bucket, filling ETag/LastModified when unset.
func (s *Store) PutObject(bucket string, obj *Object) error {
	s.mu.Lock()
	b, ok := s.buckets[bucket]
	if !ok {
		s.mu.Unlock()
		return errNoSuchBucket()
	}
	if obj.ETag == "" {
		obj.ETag = quoteETag(md5sum(obj.Data))
	}
	if obj.LastModified.IsZero() {
		obj.LastModified = time.Now().UTC()
	}
	if obj.Metadata == nil {
		obj.Metadata = map[string]string{}
	}
	if obj.Tags == nil {
		obj.Tags = map[string]string{}
	}
	b.objects[obj.Key] = obj
	hook := s.OnObjectPut
	s.mu.Unlock()
	if hook != nil {
		hook(bucket, obj.Key)
	}
	return nil
}

// GetObject returns the object stored at bucket/key.
func (s *Store) GetObject(bucket, key string) (*Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.buckets[bucket]
	if !ok {
		return nil, errNoSuchBucket()
	}
	o, ok := b.objects[key]
	if !ok {
		return nil, errNoSuchKey(key)
	}
	return o, nil
}

// DeleteObject removes bucket/key. Deleting an absent key is a no-op, as in S3.
func (s *Store) DeleteObject(bucket, key string) (bool, error) {
	s.mu.Lock()
	b, ok := s.buckets[bucket]
	if !ok {
		s.mu.Unlock()
		return false, errNoSuchBucket()
	}
	_, existed := b.objects[key]
	delete(b.objects, key)
	hook := s.OnObjectDelete
	s.mu.Unlock()
	if hook != nil {
		hook(bucket, key)
	}
	return existed, nil
}

// ListObjects scans a bucket with the shared V1/V2 listing rules.
func (s *Store) ListObjects(bucket, prefix, delimiter, marker string, maxKeys int) (*ListResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.buckets[bucket]
	if !ok {
		return nil, errNoSuchBucket()
	}
	return b.scanList(prefix, delimiter, marker, maxKeys), nil
}

// PutObjectData is a convenience wrapper that stores raw bytes as an object.
func (s *Store) PutObjectData(bucket, key, contentType string, data []byte) error {
	return s.PutObject(bucket, &Object{
		Key:         key,
		Data:        data,
		ContentType: defaultIfEmpty(contentType, "binary/octet-stream"),
	})
}

// GetObjectData returns a copy of the raw body stored at bucket/key.
func (s *Store) GetObjectData(bucket, key string) ([]byte, error) {
	o, err := s.GetObject(bucket, key)
	if err != nil {
		return nil, err
	}
	return bytes.Clone(o.Data), nil
}

// SetBucketVersioning records the versioning status of a bucket.
func (s *Store) SetBucketVersioning(name, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.buckets[name]
	if !ok {
		return errNoSuchBucket()
	}
	b.Versioning = status
	return nil
}

// SetBucketTags replaces the tags of a bucket.
func (s *Store) SetBucketTags(name string, tags map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.buckets[name]
	if !ok {
		return errNoSuchBucket()
	}
	if tags == nil {
		tags = map[string]string{}
	}
	b.Tags = tags
	return nil
}

// BucketTags returns a copy of the bucket tags.
func (s *Store) BucketTags(name string) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.buckets[name]
	if !ok {
		return nil, errNoSuchBucket()
	}
	return copyTags(b.Tags), nil
}

// SetObjectTags replaces the tags of an object.
func (s *Store) SetObjectTags(bucket, key string, tags map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.buckets[bucket]
	if !ok {
		return errNoSuchBucket()
	}
	o, ok := b.objects[key]
	if !ok {
		return errNoSuchKey(key)
	}
	if tags == nil {
		tags = map[string]string{}
	}
	o.Tags = tags
	return nil
}

// ObjectTags returns a copy of the object tags.
func (s *Store) ObjectTags(bucket, key string) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.buckets[bucket]
	if !ok {
		return nil, errNoSuchBucket()
	}
	o, ok := b.objects[key]
	if !ok {
		return nil, errNoSuchKey(key)
	}
	return copyTags(o.Tags), nil
}

// CreateUpload starts a multipart upload and returns its descriptor.
func (s *Store) CreateUpload(bucket, key string, opts UploadOpts) (*MultipartUpload, error) {
	s.mu.Lock()
	b, ok := s.buckets[bucket]
	if !ok {
		s.mu.Unlock()
		return nil, errNoSuchBucket()
	}
	u := &MultipartUpload{
		UploadID:     newUploadID(),
		Bucket:       bucket,
		Key:          key,
		ContentType:  opts.ContentType,
		StorageClass: opts.StorageClass,
		Metadata:     copyTags(opts.Metadata),
		Initiated:    time.Now().UTC(),
		parts:        map[int]*Part{},
	}
	b.uploads[u.UploadID] = u
	s.mu.Unlock()
	return cloneUpload(u), nil
}

// GetUpload looks up an in-progress upload.
func (s *Store) GetUpload(bucket, uploadID string) (*MultipartUpload, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.buckets[bucket]
	if !ok {
		return nil, errNoSuchBucket()
	}
	u, ok := b.uploads[uploadID]
	if !ok {
		return nil, errNoSuchUpload(uploadID)
	}
	return cloneUpload(u), nil
}

// PutPart stores (or replaces) one part of an in-progress upload.
func (s *Store) PutPart(bucket, uploadID string, number int, data []byte) (*Part, error) {
	if number < 1 || number > maxPartNumber {
		return nil, &s3Error{
			Status:  400,
			Code:    "InvalidArgument",
			Message: "Part number must be an integer between 1 and 10000, inclusive",
		}
	}
	s.mu.Lock()
	b, ok := s.buckets[bucket]
	if !ok {
		s.mu.Unlock()
		return nil, errNoSuchBucket()
	}
	u, ok := b.uploads[uploadID]
	if !ok {
		s.mu.Unlock()
		return nil, errNoSuchUpload(uploadID)
	}
	p := &Part{
		Number:       number,
		Data:         data,
		ETag:         quoteETag(md5sum(data)),
		LastModified: time.Now().UTC(),
	}
	u.parts[number] = p
	s.mu.Unlock()
	return &Part{Number: p.Number, ETag: p.ETag, LastModified: p.LastModified}, nil
}

// ListParts returns the parts of an upload with number > marker, sorted.
func (s *Store) ListParts(bucket, uploadID string, marker, max int) (*MultipartUpload, []*Part, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.buckets[bucket]
	if !ok {
		return nil, nil, false, errNoSuchBucket()
	}
	u, ok := b.uploads[uploadID]
	if !ok {
		return nil, nil, false, errNoSuchUpload(uploadID)
	}
	clone := cloneUpload(u)
	if max <= 0 {
		return clone, nil, false, nil
	}
	numbers := make([]int, 0, len(u.parts))
	for n := range u.parts {
		numbers = append(numbers, n)
	}
	sort.Ints(numbers)
	var parts []*Part
	truncated := false
	for _, n := range numbers {
		if n <= marker {
			continue
		}
		if len(parts) == max {
			truncated = true
			break
		}
		parts = append(parts, u.parts[n])
	}
	return clone, parts, truncated, nil
}

// AbortUpload cancels an in-progress upload.
func (s *Store) AbortUpload(bucket, uploadID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.buckets[bucket]
	if !ok {
		return errNoSuchBucket()
	}
	if _, ok := b.uploads[uploadID]; !ok {
		return errNoSuchUpload(uploadID)
	}
	delete(b.uploads, uploadID)
	return nil
}

// CompleteUpload assembles the listed parts into a final object.
func (s *Store) CompleteUpload(bucket, uploadID string, spec []PartSpec) (*Object, error) {
	s.mu.Lock()
	b, ok := s.buckets[bucket]
	if !ok {
		s.mu.Unlock()
		return nil, errNoSuchBucket()
	}
	u, ok := b.uploads[uploadID]
	if !ok {
		s.mu.Unlock()
		return nil, errNoSuchUpload(uploadID)
	}
	if len(spec) == 0 {
		s.mu.Unlock()
		return nil, &s3Error{
			Status:  400,
			Code:    "MalformedXML",
			Message: "The XML you provided was not well-formed or did not validate against our published schema",
		}
	}
	prev := 0
	chosen := make([]*Part, 0, len(spec))
	for _, sp := range spec {
		if sp.Number <= prev {
			s.mu.Unlock()
			return nil, &s3Error{
				Status:  400,
				Code:    "InvalidPartOrder",
				Message: "The list of parts was not in ascending order. Parts must be ordered by part number.",
			}
		}
		prev = sp.Number
		p, ok := u.parts[sp.Number]
		if !ok || normETag(sp.ETag) != normETag(p.ETag) {
			s.mu.Unlock()
			return nil, &s3Error{
				Status:  400,
				Code:    "InvalidPart",
				Message: "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.",
			}
		}
		chosen = append(chosen, p)
	}
	total := 0
	for _, p := range chosen {
		total += len(p.Data)
	}
	data := make([]byte, 0, total)
	sizes := make([]int64, 0, len(chosen))
	for _, p := range chosen {
		data = append(data, p.Data...)
		sizes = append(sizes, int64(len(p.Data)))
	}
	obj := &Object{
		Key:          u.Key,
		Data:         data,
		ETag:         multipartETag(chosen),
		LastModified: time.Now().UTC(),
		ContentType:  u.ContentType,
		StorageClass: u.StorageClass,
		Metadata:     copyTags(u.Metadata),
		Tags:         map[string]string{},
		PartSizes:    sizes,
	}
	if obj.ContentType == "" {
		obj.ContentType = "binary/octet-stream"
	}
	delete(b.uploads, uploadID)
	b.objects[u.Key] = obj
	hook := s.OnObjectPut
	s.mu.Unlock()
	if hook != nil {
		hook(bucket, obj.Key)
	}
	return obj, nil
}

// ListUploads scans the in-progress uploads of a bucket.
func (s *Store) ListUploads(bucket, prefix, delimiter, keyMarker, idMarker string, maxUploads int) (*UploadList, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.buckets[bucket]
	if !ok {
		return nil, errNoSuchBucket()
	}
	return b.scanUploads(prefix, delimiter, keyMarker, idMarker, maxUploads), nil
}

// scanList walks the sorted keys of a bucket applying prefix/delimiter/marker.
func (b *Bucket) scanList(prefix, delimiter, marker string, maxKeys int) *ListResult {
	res := &ListResult{}
	if maxKeys <= 0 {
		return res
	}
	keys := make([]string, 0, len(b.objects))
	for k := range b.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	seen := map[string]bool{}
	var last string
	for _, k := range keys {
		if marker != "" && k <= marker {
			continue
		}
		item, isPrefix := k, false
		if delimiter != "" {
			rest := k[len(prefix):]
			if i := strings.Index(rest, delimiter); i >= 0 {
				cp := k[:len(prefix)+i+len(delimiter)]
				if seen[cp] {
					continue
				}
				if marker != "" && cp <= marker {
					seen[cp] = true
					continue
				}
				item, isPrefix = cp, true
			}
		}
		if len(res.Objects)+len(res.Prefixes) == maxKeys {
			res.Truncated = true
			break
		}
		if isPrefix {
			res.Prefixes = append(res.Prefixes, item)
			seen[item] = true
		} else {
			res.Objects = append(res.Objects, b.objects[k])
		}
		last = item
	}
	if res.Truncated {
		res.NextMarker = last
	}
	return res
}

// scanUploads walks the in-progress uploads sorted by key then upload id.
func (b *Bucket) scanUploads(prefix, delimiter, keyMarker, idMarker string, maxUploads int) *UploadList {
	res := &UploadList{}
	if maxUploads <= 0 {
		return res
	}
	uploads := make([]*MultipartUpload, 0, len(b.uploads))
	for _, u := range b.uploads {
		uploads = append(uploads, u)
	}
	sort.Slice(uploads, func(i, j int) bool {
		if uploads[i].Key != uploads[j].Key {
			return uploads[i].Key < uploads[j].Key
		}
		return uploads[i].UploadID < uploads[j].UploadID
	})
	seen := map[string]bool{}
	var lastKey, lastID string
	for _, u := range uploads {
		k := u.Key
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if keyMarker != "" {
			if k < keyMarker {
				continue
			}
			if k == keyMarker && (idMarker == "" || u.UploadID <= idMarker) {
				continue
			}
		}
		item, isPrefix := k, false
		if delimiter != "" {
			rest := k[len(prefix):]
			if i := strings.Index(rest, delimiter); i >= 0 {
				cp := k[:len(prefix)+i+len(delimiter)]
				if seen[cp] {
					continue
				}
				if keyMarker != "" && cp <= keyMarker {
					seen[cp] = true
					continue
				}
				item, isPrefix = cp, true
			}
		}
		if len(res.Uploads)+len(res.Prefixes) == maxUploads {
			res.Truncated = true
			break
		}
		if isPrefix {
			res.Prefixes = append(res.Prefixes, item)
			seen[item] = true
			lastKey, lastID = item, ""
		} else {
			res.Uploads = append(res.Uploads, cloneUpload(u))
			lastKey, lastID = u.Key, u.UploadID
		}
	}
	if res.Truncated {
		res.NextKey, res.NextID = lastKey, lastID
	}
	return res
}

func newUploadID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func md5sum(data []byte) string {
	sum := md5.Sum(data)
	return hex.EncodeToString(sum[:])
}

func md5sumBytes(data []byte) []byte {
	sum := md5.Sum(data)
	return sum[:]
}

func quoteETag(hexSum string) string { return `"` + hexSum + `"` }

func normETag(e string) string { return strings.Trim(strings.TrimSpace(e), `"`) }

// multipartETag mirrors S3: the md5 of the concatenated binary part md5s,
// suffixed with the part count.
func multipartETag(parts []*Part) string {
	h := md5.New()
	for _, p := range parts {
		raw, err := hex.DecodeString(normETag(p.ETag))
		if err != nil || len(raw) != md5.Size {
			raw = md5sumBytes(p.Data)
		}
		h.Write(raw)
	}
	return `"` + hex.EncodeToString(h.Sum(nil)) + `-` + strconv.Itoa(len(parts)) + `"`
}

func copyTags(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneUpload(u *MultipartUpload) *MultipartUpload {
	return &MultipartUpload{
		UploadID:     u.UploadID,
		Bucket:       u.Bucket,
		Key:          u.Key,
		ContentType:  u.ContentType,
		StorageClass: u.StorageClass,
		Metadata:     copyTags(u.Metadata),
		Initiated:    u.Initiated,
	}
}

func validBucketName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '.':
		default:
			return false
		}
	}
	first, last := name[0], name[len(name)-1]
	if first == '-' || first == '.' || last == '-' || last == '.' {
		return false
	}
	if strings.Contains(name, "..") || strings.Contains(name, ".-") || strings.Contains(name, "-.") {
		return false
	}
	if isIPv4Like(name) {
		return false
	}
	return true
}

func isIPv4Like(s string) bool {
	parts := strings.Split(s, ".")
	if len(parts) != 4 {
		return false
	}
	for _, p := range parts {
		if p == "" || len(p) > 3 {
			return false
		}
		for i := 0; i < len(p); i++ {
			if p[i] < '0' || p[i] > '9' {
				return false
			}
		}
	}
	return true
}
