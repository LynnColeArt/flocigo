package s3

import (
    "encoding/xml"
    "io"
    "net/http"
    "strconv"
    "strings"
)

const maxPartNumber = 10000

func parsePartNumber(s string) (int, bool) {
    n, err := strconv.Atoi(s)
    if err != nil || n < 1 || n > maxPartNumber {
        return 0, false
    }
    return n, true
}

func (s *Server) handleInitiateUpload(rc *requestCtx) {
    meta := defaultContentType(extractMetadata(rc.r))
    id, err := s.store.CreateUpload(rc.bucket, rc.key, meta)
    if err != nil {
        rc.fail(err)
        return
    }
    rc.xml(http.StatusOK, InitiateMultipartUploadResult{
        Xmlns:    s3Namespace,
        Bucket:   rc.bucket,
        Key:      rc.key,
        UploadID: id,
    })
}

func (s *Server) handleUploadPart(rc *requestCtx) {
    partNumber, ok := parsePartNumber(rc.r.URL.Query().Get("partNumber"))
    if !ok {
        rc.fail(errInvalidArgument("Part number must be an integer between 1 and %d, inclusive", maxPartNumber))
        return
    }
    if source := rc.r.Header.Get("x-amz-copy-source"); source != "" {
        s.handleUploadPartCopy(rc, partNumber)
        return
    }
    body, err := io.ReadAll(rc.r.Body)
    if err != nil {
        rc.fail(errInvalidArgument("unable to read request body"))
        return
    }
    part, err := s.store.PutPart(rc.bucket, rc.key, rc.uploadID, partNumber, body)
    if err != nil {
        rc.fail(err)
        return
    }
    rc.w.Header().Set("ETag", quoteETag(part.ETag))
    rc.w.WriteHeader(http.StatusOK)
}

// handleUploadPartCopy implements UploadPartCopy: PUT ?partNumber&uploadId
// with an x-amz-copy-source header (plus optional range and preconditions).
func (s *Server) handleUploadPartCopy(rc *requestCtx, partNumber int) {
    srcBucket, srcKey := parseCopySource(rc.r.Header.Get("x-amz-copy-source"))
    if srcBucket == "" || srcKey == "" {
        rc.fail(errInvalidArgument("Invalid x-amz-copy-source header"))
        return
    }
    info, data, err := s.store.GetObject(srcBucket, srcKey)
    if err != nil {
        rc.failCtx(srcBucket, srcKey, err)
        return
    }
    if cerr := checkCopyConditions(rc.r, info.ETag, info.LastModified); cerr != nil {
        rc.fail(cerr)
        return
    }
    if rng := rc.r.Header.Get("x-amz-copy-source-range"); rng != "" {
        start, length, ok, unsat := parseByteRange(rng, int64(len(data)))
        if !ok || unsat {
            rc.fail(errInvalidArgument("The x-amz-copy-source-range value must be of the form bytes=first-last where first and last are the zero-based offsets of the first and last bytes of the copy source range"))
            return
        }
        data = data[start : start+length]
    }
    part, err := s.store.PutPart(rc.bucket, rc.key, rc.uploadID, partNumber, data)
    if err != nil {
        rc.fail(err)
        return
    }
    rc.xml(http.StatusOK, CopyPartResult{
        Xmlns:        s3Namespace,
        ETag:         quoteETag(part.ETag),
        LastModified: s3t(part.LastModified),
    })
}

func (s *Server) handleListParts(rc *requestCtx) {
    q := rc.r.URL.Query()
    maxParts := 1000
    if v := q.Get("max-parts"); v != "" {
        n, err := strconv.Atoi(v)
        if err != nil || n < 0 {
            rc.fail(errInvalidArgument("max-parts must be a non-negative integer"))
            return
        }
        maxParts = n
    }
    marker := 0
    if v := q.Get("part-number-marker"); v != "" {
        n, err := strconv.Atoi(v)
        if err != nil {
            rc.fail(errInvalidArgument("part-number-marker must be an integer"))
            return
        }
        marker = n
    }
    parts, err := s.store.ListParts(rc.bucket, rc.key, rc.uploadID)
    if err != nil {
        rc.fail(err)
        return
    }
    res := ListPartsResult{
        Xmlns:            s3Namespace,
        Bucket:           rc.bucket,
        Key:              rc.key,
        UploadID:         rc.uploadID,
        PartNumberMarker: marker,
        MaxParts:         maxParts,
        Initiator:        s.owner,
        Owner:            s.owner,
        StorageClass:     "STANDARD",
    }
    emitted := 0
    for _, p := range parts {
        if p.PartNumber <= marker {
            continue
        }
        if maxParts == 0 {
            break
        }
        if emitted == maxParts {
            res.IsTruncated = true
            break
        }
        res.Parts = append(res.Parts, PartEntry{
            PartNumber:   p.PartNumber,
            LastModified: s3t(p.LastModified),
            ETag:         quoteETag(p.ETag),
            Size:         p.Size,
        })
        res.NextPartNumberMarker = p.PartNumber
        emitted++
    }
    rc.xml(http.StatusOK, res)
}

func (s *Server) handleCompleteUpload(rc *requestCtx) {
    body, err := io.ReadAll(rc.r.Body)
    if err != nil {
        rc.fail(errInvalidArgument("unable to read request body"))
        return
    }
    var req CompleteMultipartUpload
    if uerr := xml.Unmarshal(body, &req); uerr != nil || len(req.Parts) == 0 {
        rc.fail(errMalformedXML())
        return
    }
    specs := make([]PartSpec, 0, len(req.Parts))
    for _, p := range req.Parts {
        specs = append(specs, PartSpec{PartNumber: p.PartNumber, ETag: p.ETag})
    }
    info, err := s.store.CompleteUpload(rc.bucket, rc.key, rc.uploadID, specs)
    if err != nil {
        rc.fail(err)
        return
    }
    location := "http://" + rc.r.Host + "/" + rc.bucket + "/" + escapePath(rc.key)
    rc.w.Header().Set("ETag", quoteETag(info.ETag))
    rc.xml(http.StatusOK, CompleteMultipartUploadResult{
        Xmlns:    s3Namespace,
        Location: location,
        Bucket:   rc.bucket,
        Key:      rc.key,
        ETag:     quoteETag(info.ETag),
    })
}

func (s *Server) handleAbortUpload(rc *requestCtx) {
    if err := s.store.AbortUpload(rc.bucket, rc.key, rc.uploadID); err != nil {
        rc.fail(err)
        return
    }
    rc.w.WriteHeader(http.StatusNoContent)
}

// handleListUploads implements GET /bucket?uploads (ListMultipartUploads).
func (s *Server) handleListUploads(rc *requestCtx) {
    q := rc.r.URL.Query()
    prefix := q.Get("prefix")
    delimiter := q.Get("delimiter")
    keyMarker := q.Get("key-marker")
    idMarker := q.Get("upload-id-marker")
    maxUploads := 1000
    if v := q.Get("max-uploads"); v != "" {
        n, err := strconv.Atoi(v)
        if err != nil || n < 0 {
            rc.fail(errInvalidArgument("max-uploads must be a non-negative integer"))
            return
        }
        maxUploads = n
    }
    uploads, err := s.store.ListUploads(rc.bucket)
    if err != nil {
        rc.fail(err)
        return
    }
    type entry struct {
        isPrefix bool
        cp       string
        upload   UploadSummary
    }
    var entries []entry
    last := ""
    for _, u := range uploads {
        if prefix != "" && !strings.HasPrefix(u.Key, prefix) {
            continue
        }
        if delimiter != "" {
            rest := u.Key[len(prefix):]
            if idx := strings.Index(rest, delimiter); idx >= 0 {
                cp := u.Key[:len(prefix)+idx+len(delimiter)]
                if cp == last {
                    continue
                }
                last = cp
                if cp <= keyMarker {
                    continue
                }
                entries = append(entries, entry{isPrefix: true, cp: cp})
                continue
            }
        }
        if keyMarker != "" {
            if u.Key < keyMarker {
                continue
            }
            if u.Key == keyMarker && (idMarker == "" || u.UploadID <= idMarker) {
                continue
            }
        }
        entries = append(entries, entry{upload: u})
    }
    res := ListMultipartUploadsResult{
        Xmlns:          s3Namespace,
        Bucket:         rc.bucket,
        KeyMarker:      keyMarker,
        UploadIDMarker: idMarker,
        Prefix:         prefix,
        MaxUploads:     maxUploads,
    }
    if delimiter != "" {
        res.Delimiter = delimiter
    }
    emitted := 0
    for _, e := range entries {
        if maxUploads == 0 {
            break
        }
        if emitted == maxUploads {
            res.IsTruncated = true
            break
        }
        if e.isPrefix {
            res.CommonPrefixes = append(res.CommonPrefixes, CommonPrefix{Prefix: e.cp})
            res.NextKeyMarker, res.NextUploadIDMarker = e.cp, ""
        } else {
            res.Uploads = append(res.Uploads, UploadEntry{
                Key:          e.upload.Key,
                UploadID:     e.upload.UploadID,
                Initiator:    s.owner,
                Owner:        s.owner,
                StorageClass: "STANDARD",
                Initiated:    s3t(e.upload.Initiated),
            })
            res.NextKeyMarker, res.NextUploadIDMarker = e.upload.Key, e.upload.UploadID
        }
        emitted++
    }
    rc.xml(http.StatusOK, res)
}
