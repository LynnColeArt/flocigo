package s3

import (
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type Store struct {
	root string
	mu   sync.RWMutex
}

func NewStore(root string) *Store { os.MkdirAll(root, 0o755); return &Store{root: root} }

func (s *Store) bucketPath(name string) string { return filepath.Join(s.root, "buckets", name) }

func (s *Store) objectPath(b, k string) string { return filepath.Join(s.bucketPath(b), "objects", k) }

func (s *Store) CreateBucket(name string) error {
	if len(name) < 3 || len(name) > 63 {
		return fmt.Errorf("InvalidBucketName")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, e := os.Stat(s.bucketPath(name)); e == nil {
		return nil
	}
	return os.MkdirAll(filepath.Join(s.bucketPath(name), "objects"), 0o755)
}

func (s *Store) BucketExists(name string) bool {
	st, e := os.Stat(s.bucketPath(name))
	return e == nil && st.IsDir()
}

func (s *Store) DeleteBucket(name string) error {
	objs, e := os.ReadDir(filepath.Join(s.bucketPath(name), "objects"))
	if e != nil {
		return nil
	}
	if len(objs) > 0 {
		return fmt.Errorf("BucketNotEmpty")
	}
	return os.RemoveAll(s.bucketPath(name))
}

func (s *Store) ListBuckets() []string {
	entries, e := os.ReadDir(filepath.Join(s.root, "buckets"))
	if e != nil {
		return nil
	}
	var out []string
	for _, f := range entries {
		if f.IsDir() {
			out = append(out, f.Name())
		}
	}
	sort.Strings(out)
	return out
}

func (s *Store) PutObject(b, k string, body []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op := s.objectPath(b, k)
	os.MkdirAll(filepath.Dir(op), 0o755)
	h := md5.Sum(body)
	if e := os.WriteFile(op, body, 0o644); e != nil {
		return "", e
	}
	return fmt.Sprintf("%x", h), nil
}

func (s *Store) GetObject(b, k string) ([]byte, string, error) {
	op := s.objectPath(b, k)
	if _, e := os.Stat(op); e != nil {
		return nil, "", fmt.Errorf("NoSuchKey")
	}
	body, e := os.ReadFile(op)
	if e != nil {
		return nil, "", e
	}
	h := md5.Sum(body)
	return body, fmt.Sprintf("%x", h), nil
}

func (s *Store) DeleteObject(b, k string) error {
	os.Remove(s.objectPath(b, k))
	return nil
}

type ObjectEntry struct {
	Key, ETag, LastModified string
	Size                    int64
}

func (s *Store) ListObjects(b, prefix string) []ObjectEntry {
	op := filepath.Join(s.bucketPath(b), "objects")
	var out []ObjectEntry
	filepath.Walk(op, func(path string, info os.FileInfo, e error) error {
		if e != nil || info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(op, path)
		k := filepath.ToSlash(rel)
		if strings.HasPrefix(k, prefix) || prefix == "" {
			body, _ := os.ReadFile(path)
			h := md5.Sum(body)
			out = append(out, ObjectEntry{Key: k, Size: int64(len(body)), ETag: fmt.Sprintf("%x", h), LastModified: info.ModTime().UTC().Format("2006-01-02T15:04:05Z")})
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
