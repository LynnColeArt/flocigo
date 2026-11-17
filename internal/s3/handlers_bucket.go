package s3

import (
    "encoding/xml"
    "io"
    "net/http"
    "regexp"
    "strings"
)

var bucketNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

func validBucketName(name string) bool {
    if len(name) < 3 || len(name) > 63 {
        return false
    }
    if !bucketNamePattern.MatchString(name) {
        return false
    }
    return !strings.Contains(name, "..")
}

func (s *Server) handleListBuckets(rc *requestCtx) {
    buckets := s.store.ListBuckets()
    entries := make([]BucketEntry, 0, len(buckets))
    for _, b := range buckets {
        entries = append(entries, BucketEntry{Name: b.Name, CreationDate: s3t(b.CreationDate)})
    }
    rc.xml(http.StatusOK, ListAllMyBucketsResult{
        Xmlns:   s3Namespace,
        Owner:   s.owner,
        Buckets: entries,
    })
}

func (s *Server) handleCreateBucket(rc *requestCtx) {
    if !validBucketName(rc.bucket) {
        rc.fail(errInvalidBucketName())
        return
    }
    body, err := io.ReadAll(io.LimitReader(rc.r.Body, 1<<20))
    if err != nil {
        rc.fail(errInvalidArgument("unable to read request body"))
        return
    }
    location := s.region
    if len(body) > 0 {
        var cfg CreateBucketConfiguration
        if uerr := xml.Unmarshal(body, &cfg); uerr != nil || strings.TrimSpace(cfg.LocationConstraint) == "" {
            rc.fail(errMalformedXML())
            return
        }
        location = strings.TrimSpace(cfg.LocationConstraint)
    }
    if cerr := s.store.CreateBucket(rc.bucket, location); cerr != nil {
        rc.fail(cerr)
        return
    }
    rc.w.Header().Set("Location", "/"+rc.bucket)
    rc.w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteBucket(rc *requestCtx) {
    if err := s.store.DeleteBucket(rc.bucket); err != nil {
        rc.fail(err)
        return
    }
    rc.w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleHeadBucket(rc *requestCtx) {
    if !s.store.BucketExists(rc.bucket) {
        rc.w.WriteHeader(http.StatusNotFound)
        return
    }
    rc.w.WriteHeader(http.StatusOK)
}

func (s *Server) handleBucketLocation(rc *requestCtx) {
    if !s.store.BucketExists(rc.bucket) {
        rc.fail(errNoSuchBucket())
        return
    }
    location := s.store.GetBucketLocation(rc.bucket)
    // S3 answers the us-east-1 default with an empty location constraint.
    if location == "us-east-1" {
        location = ""
    }
    rc.xml(http.StatusOK, LocationConstraint{Xmlns: s3Namespace, Value: location})
}

func (s *Server) handleDeleteObjects(rc *requestCtx) {
    if !s.store.BucketExists(rc.bucket) {
        rc.fail(errNoSuchBucket())
        return
    }
    body, err := io.ReadAll(rc.r.Body)
    if err != nil {
        rc.fail(errInvalidArgument("unable to read request body"))
        return
    }
    var req DeleteRequest
    if uerr := xml.Unmarshal(body, &req); uerr != nil || len(req.Objects) == 0 || len(req.Objects) > 1000 {
        rc.fail(errMalformedXML())
        return
    }
    for _, o := range req.Objects {
        if o.Key == "" {
            rc.fail(errMalformedXML())
            return
        }
    }
    res := DeleteResult{Xmlns: s3Namespace}
    for _, o := range req.Objects {
        if derr := s.store.DeleteObject(rc.bucket, o.Key); derr != nil {
            res.Errors = append(res.Errors, DeleteErr{Key: o.Key, Code: "InternalError", Message: derr.Error()})
            continue
        }
        if !req.Quiet {
            res.Deleted = append(res.Deleted, Deleted{Key: o.Key})
        }
    }
    rc.xml(http.StatusOK, res)
}
