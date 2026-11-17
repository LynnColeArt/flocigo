package main

import (
    "net/http"
    "os"
    "time"
)

// ServeHTTP routes path-style requests:
//
//    /                service requests (GET: ListBuckets)
//    /{bucket}        bucket requests   (PUT: CreateBucket, GET: ListObjects,
//                                       DELETE: DeleteBucket)
//    /{bucket}/{key}  object requests   (later ticket: NotImplemented)
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    if err := s.ensureRoot(); err != nil {
        replyError(w, r, ErrInternalError)
        return
    }

    bucket, rest := splitRequestPath(r.URL.Path)
    switch {
    case bucket == "":
        switch r.Method {
        case http.MethodGet, http.MethodHead:
            s.handleListBuckets(w, r)
        default:
            replyError(w, r, ErrMethodNotAllowed)
        }
    case rest == "":
        switch r.Method {
        case http.MethodPut:
            s.handlePutBucket(w, r)
        case http.MethodGet, http.MethodHead:
            s.handleListObjects(w, r, bucket)
        case http.MethodDelete:
            s.handleDeleteBucket(w, r, bucket)
        default:
            replyError(w, r, ErrMethodNotAllowed)
        }
    default:
        replyError(w, r, ErrNotImplemented)
    }
}

// handleListBuckets implements GET / (ListAllMyBuckets): every bucket
// directory under Root is reported, using its modification time as the
// creation date.
func (s *Server) handleListBuckets(w http.ResponseWriter, r *http.Request) {
    entries, err := os.ReadDir(s.Root)
    if err != nil {
        replyError(w, r, ErrInternalError)
        return
    }

    doc := listAllMyBucketsResultXML{
        Xmlns: s3Namespace,
        Owner: ownerXML{ID: s3OwnerID, DisplayName: s3OwnerName},
    }
    for _, entry := range entries {
        if !entry.IsDir() || !validateBucketName(entry.Name()) {
            continue
        }
        b := bucketXML{Name: entry.Name(), CreationDate: time.Time{}.UTC().Format(s3TimeFormat)}
        if info, err := entry.Info(); err == nil {
            b.CreationDate = info.ModTime().UTC().Format(s3TimeFormat)
        }
        doc.Buckets.Bucket = append(doc.Buckets.Bucket, b)
    }

    writeXML(w, r, http.StatusOK, doc)
}

// handleListObjects implements GET /{bucket} for the moment: the empty
// listing for an existing bucket and NoSuchBucket otherwise. Real object
// storage arrives in a later ticket.
func (s *Server) handleListObjects(w http.ResponseWriter, r *http.Request, bucket string) {
    if info, err := os.Stat(s.bucketDir(bucket)); err != nil || !info.IsDir() {
        replyError(w, r, ErrNoSuchBucket)
        return
    }

    writeXML(w, r, http.StatusOK, listBucketResultXML{
        Xmlns:   s3Namespace,
        Name:    bucket,
        Prefix:  r.URL.Query().Get("prefix"),
        MaxKeys: maxKeysDefault,
    })
}

// handleDeleteBucket implements DELETE /{bucket}: 204 for an empty bucket,
// BucketNotEmpty when objects are inside, NoSuchBucket when it is missing.
func (s *Server) handleDeleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
    dir := s.bucketDir(bucket)
    if info, err := os.Stat(dir); err != nil || !info.IsDir() {
        replyError(w, r, ErrNoSuchBucket)
        return
    }
    entries, err := os.ReadDir(dir)
    if err != nil {
        replyError(w, r, ErrInternalError)
        return
    }
    if len(entries) > 0 {
        replyError(w, r, ErrBucketNotEmpty)
        return
    }
    if err := os.Remove(dir); err != nil {
        replyError(w, r, ErrInternalError)
        return
    }
    setBasicHeaders(w)
    w.WriteHeader(http.StatusNoContent)
}
