package main

import (
    "os"
    "path/filepath"
    "strings"
)

const (
    // s3Namespace is the XML namespace S3 response documents carry.
    s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/"
    // s3TimeFormat is the timestamp format list responses use.
    s3TimeFormat = "2006-01-02T15:04:05.000Z"
    // maxKeysDefault is the page size advertised by list responses.
    maxKeysDefault = 1000
    // The single account floci emulates.
    s3OwnerID   = "floci"
    s3OwnerName = "floci"
)

// Server is the floci-go S3 emulator.
//
// All state lives in the file system: every bucket is a directory named
// after the bucket, directly under Root. Object-level requests are the
// subject of a later ticket, so service and bucket endpoints are live today.
type Server struct {
    // Root is the directory under which bucket directories live.
    Root string
}

// NewServer returns a Server storing its buckets under root.
func NewServer(root string) *Server {
    return &Server{Root: root}
}

// bucketDir is the on-disk directory backing a bucket. Bucket names are
// validated before this is called, so name can never escape Root.
func (s *Server) bucketDir(name string) string {
    return filepath.Join(s.Root, name)
}

// ensureRoot creates the data root when needed, e.g. for tests handing the
// server a fresh directory.
func (s *Server) ensureRoot() error {
    return os.MkdirAll(s.Root, 0o755)
}

// splitRequestPath splits "/bucket" or "/bucket/key..." into its first path
// segment and the remainder.
func splitRequestPath(p string) (bucket, rest string) {
    p = strings.TrimPrefix(p, "/")
    if i := strings.IndexByte(p, '/'); i >= 0 {
        return p[:i], p[i+1:]
    }
    return p, ""
}
