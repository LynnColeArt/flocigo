package s3

import (
	"encoding/xml"
	"net/http"
	"strconv"
)

// initiateMultipart answers POST /bucket/key?uploads.
func (c *reqCtx) initiateMultipart() {
	if e := c.mustBucket(); e != nil {
		writeError(c.w, c.r, e)
		return
	}
	hdr := c.r.Header
	u, err := c.rt.store().CreateUpload(c.bucket, c.key, UploadOpts{
		ContentType:  defaultIfEmpty(hdr.Get("Content-Type"), "binary/octet-stream"),
		StorageClass: hdr.Get("x-amz-storage-class"),
		Metadata:     collectMetadata(hdr),
	})
	if err != nil {
		writeError(c.w, c.r, asS3Error(err))
		return
	}
	writeXML(c.w, http.StatusOK, xmlInitiateMultipart{
		Bucket:   c.bucket,
		Key:      c.key,
		UploadID: u.UploadID,
	})
}

// partNumberOf extracts and validates the partNumber query parameter.
func (c *reqCtx) partNumberOf() (int, *s3Error) {
	n, err := strconv.Atoi(c.r.URL.Query().Get("partNumber"))
	if err != nil || n < 1 || n > maxPartNumber {
		return 0, s3err(400, "InvalidArgument", "Part number must be an integer between 1 and 10000, inclusive")
	}
	return n, nil
}

// uploadPart answers PUT /bucket/key?partNumber&uploadId.
func (c *reqCtx) uploadPart() {
	number, perr := c.partNumberOf()
	if perr != nil {
		writeError(c.w, c.r, perr)
		return
	}
	data, err := readBodyData(c.r)
	if err != nil {
		writeError(c.w, c.r, s3err(400, "IncompleteBody", "The request body could not be read"))
		return
	}
	part, err := c.rt.store().PutPart(c.bucket, c.r.URL.Query().Get("uploadId"), number, data)
	if err != nil {
		writeError(c.w, c.r, asS3Error(err))
		return
	}
	c.w.Header().Set("ETag", part.ETag)
	writeEmpty(c.w, http.StatusOK)
}

// uploadPartCopy answers PUT /bucket/key?partNumber&uploadId with a copy source.
func (c *reqCtx) uploadPartCopy() {
	number, perr := c.partNumberOf()
	if perr != nil {
		writeError(c.w, c.r, perr)
		return
	}
	srcBucket, srcKey, cerr := parseCopySource(c.r.Header.Get("x-amz-copy-source"))
	if cerr != nil {
		writeError(c.w, c.r, cerr)
		return
	}
	src, err := c.rt.store().GetObject(srcBucket, srcKey)
	if err != nil {
		writeError(c.w, c.r, asS3Error(err))
		return
	}
	data := src.Data
	if rg := c.r.Header.Get("x-amz-copy-source-range"); rg != "" {
		start, end, unsat, ok := parseRange(rg, src.Size())
		if !ok || unsat {
			writeError(c.w, c.r, s3err(400, "InvalidRange", "The requested range is not satisfiable"))
			return
		}
		data = src.Data[start : end+1]
	}
	part, err := c.rt.store().PutPart(c.bucket, c.r.URL.Query().Get("uploadId"), number, data)
	if err != nil {
		writeError(c.w, c.r, asS3Error(err))
		return
	}
	writeXML(c.w, http.StatusOK, xmlCopyPart{
		ETag:         part.ETag,
		LastModified: s3Time(part.LastModified),
	})
}

// abortUpload answers DELETE /bucket/key?uploadId.
func (c *reqCtx) abortUpload() {
	if err := c.rt.store().AbortUpload(c.bucket, c.r.URL.Query().Get("uploadId")); err != nil {
		writeError(c.w, c.r, asS3Error(err))
		return
	}
	writeEmpty(c.w, http.StatusNoContent)
}

// completeMultipart answers POST /bucket/key?uploadId.
func (c *reqCtx) completeMultipart() {
	if e := c.mustBucket(); e != nil {
		writeError(c.w, c.r, e)
		return
	}
	body, err := readBodyData(c.r)
	if err != nil {
		writeError(c.w, c.r, s3err(400, "InvalidRequest", "could not read request body"))
		return
	}
	var req xmlCompleteRequest
	if len(body) == 0 || xml.Unmarshal(body, &req) != nil || len(req.Parts) == 0 {
		writeError(c.w, c.r, s3err(400, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema"))
		return
	}
	spec := make([]PartSpec, 0, len(req.Parts))
	for _, p := range req.Parts {
		spec = append(spec, PartSpec{Number: p.PartNumber, ETag: p.ETag})
	}
	obj, err := c.rt.store().CompleteUpload(c.bucket, c.r.URL.Query().Get("uploadId"), spec)
	if err != nil {
		writeError(c.w, c.r, asS3Error(err))
		return
	}
	writeXML(c.w, http.StatusOK, xmlCompleteMultipart{
		Location: "http://" + c.r.Host + "/" + c.bucket + "/" + c.key,
		Bucket:   c.bucket,
		Key:      c.key,
		ETag:     obj.ETag,
	})
}

// listPartsOp answers GET /bucket/key?uploadId.
func (c *reqCtx) listPartsOp() {
	q := c.r.URL.Query()
	uploadID := q.Get("uploadId")
	if _, err := c.rt.store().GetUpload(c.bucket, uploadID); err != nil {
		writeError(c.w, c.r, asS3Error(err))
		return
	}
	maxParts := 1000
	if raw := q.Get("max-parts"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			writeError(c.w, c.r, s3err(400, "InvalidArgument", "Argument max-parts must be an integer between 0 and 2147483647"))
			return
		}
		maxParts = n
	}
	marker := 0
	if raw := q.Get("part-number-marker"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			writeError(c.w, c.r, s3err(400, "InvalidArgument", "Argument part-number-marker must be an integer between 0 and 2147483647"))
			return
		}
		marker = n
	}
	upload, parts, truncated, err := c.rt.store().ListParts(c.bucket, uploadID, marker, maxParts)
	if err != nil {
		writeError(c.w, c.r, asS3Error(err))
		return
	}
	doc := xmlListParts{
		Xmlns:            s3XMLNS,
		Bucket:           c.bucket,
		Key:              upload.Key,
		UploadID:         uploadID,
		PartNumberMarker: marker,
		MaxParts:         maxParts,
		IsTruncated:      truncated,
		Initiator:        xmlFlociOwner(),
		Owner:            xmlFlociOwner(),
		StorageClass:     storageClassOrDefault(upload.StorageClass),
	}
	for _, p := range parts {
		doc.Part = append(doc.Part, xmlPart{
			PartNumber:   p.Number,
			LastModified: s3Time(p.LastModified),
			ETag:         p.ETag,
			Size:         int64(len(p.Data)),
		})
	}
	if truncated && len(parts) > 0 {
		doc.NextPartNumberMarker = parts[len(parts)-1].Number
	}
	writeXML(c.w, http.StatusOK, doc)
}
