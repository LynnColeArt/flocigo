package s3

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// bucketLevel dispatches bucket-scoped operations.
func (c *reqCtx) bucketLevel() {
	q := c.r.URL.Query()
	switch c.r.Method {
	case http.MethodGet:
		switch {
		case q.Has("uploads"):
			c.listMultipartUploads()
		case q.Has("location"):
			c.bucketLocation()
		case q.Has("versioning"):
			c.bucketVersioning()
		case q.Has("tagging"):
			c.bucketTagging()
		case q.Has("acl"):
			c.bucketACL()
		case q.Has("cors"):
			c.subresourceMissing("NoSuchCORSConfiguration", "The CORS configuration does not exist")
		case q.Has("lifecycle"):
			c.subresourceMissing("NoSuchLifecycleConfiguration", "The lifecycle configuration does not exist")
		case q.Has("website"):
			c.subresourceMissing("NoSuchWebsiteConfiguration", "The specified bucket does not have a website configuration")
		case q.Has("policy"):
			c.subresourceMissing("NoSuchBucketPolicy", "The bucket policy does not exist")
		case q.Has("encryption"):
			c.subresourceMissing("ServerSideEncryptionConfigurationNotFoundError", "The server side encryption configuration was not found")
		default:
			if q.Get("list-type") == "2" {
				c.listObjectsV2()
			} else {
				c.listObjectsV1()
			}
		}
	case http.MethodHead:
		c.headBucket()
	case http.MethodPut:
		c.putBucket(q)
	case http.MethodDelete:
		c.deleteBucket(q)
	case http.MethodPost:
		if q.Has("delete") {
			c.deleteObjects()
			return
		}
		c.default405()
	default:
		c.default405()
	}
}

// listBuckets answers GET / with a ListAllMyBucketsResult document.
func (c *reqCtx) listBuckets() {
	doc := xmlListBuckets{Xmlns: s3XMLNS, Owner: xmlFlociOwner()}
	for _, b := range c.rt.store().ListBuckets() {
		doc.Buckets = append(doc.Buckets, xmlBucket{Name: b.Name, CreationDate: s3Time(b.Created)})
	}
	writeXML(c.w, http.StatusOK, doc)
}

// createBucket answers PUT /bucket.
func (c *reqCtx) createBucket() {
	_, _ = io.Copy(io.Discard, c.r.Body)
	if err := c.rt.store().CreateBucket(c.bucket); err != nil {
		writeError(c.w, c.r, asS3Error(err))
		return
	}
	c.w.Header().Set("Location", "/"+c.bucket)
	writeEmpty(c.w, http.StatusOK)
}

// subresourceWrite reports whether the query targets a configuration
// subresource the emulator acknowledges without storing.
func subresourceWrite(q url.Values) bool {
	for _, sub := range []string{"acl", "cors", "website", "policy", "lifecycle", "logging", "replication", "requestPayment", "accelerate", "notification", "analytics", "inventory", "metrics", "ownershipControls", "publicAccessBlock", "intelligent-tiering", "object-lock", "encryption"} {
		if q.Has(sub) {
			return true
		}
	}
	return false
}

// putBucket handles PUT /bucket (create) and subresource writes.
func (c *reqCtx) putBucket(q url.Values) {
	switch {
	case subresourceWrite(q):
		if e := c.mustBucket(); e != nil {
			writeError(c.w, c.r, e)
			return
		}
		_, _ = io.Copy(io.Discard, c.r.Body)
		writeEmpty(c.w, http.StatusOK)
	case q.Has("versioning"):
		if e := c.mustBucket(); e != nil {
			writeError(c.w, c.r, e)
			return
		}
		body, _ := io.ReadAll(c.r.Body)
		var doc xmlVersioning
		status := ""
		if len(body) > 0 {
			if err := xml.Unmarshal(body, &doc); err != nil {
				writeError(c.w, c.r, s3err(400, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema"))
				return
			}
			status = doc.Status
		}
		if err := c.rt.store().SetBucketVersioning(c.bucket, status); err != nil {
			writeError(c.w, c.r, asS3Error(err))
			return
		}
		writeEmpty(c.w, http.StatusOK)
	case q.Has("tagging"):
		if e := c.mustBucket(); e != nil {
			writeError(c.w, c.r, e)
			return
		}
		body, err := readBodyData(c.r)
		if err != nil || len(body) == 0 {
			writeError(c.w, c.r, s3err(400, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema"))
			return
		}
		var doc xmlTagging
		if uerr := xml.Unmarshal(body, &doc); uerr != nil {
			writeError(c.w, c.r, s3err(400, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema"))
			return
		}
		tags := map[string]string{}
		for _, t := range doc.TagSet {
			tags[t.Key] = t.Value
		}
		if serr := c.rt.store().SetBucketTags(c.bucket, tags); serr != nil {
			writeError(c.w, c.r, asS3Error(serr))
			return
		}
		writeEmpty(c.w, http.StatusOK)
	default:
		c.createBucket()
	}
}

// deleteBucket handles DELETE /bucket and its subresources.
func (c *reqCtx) deleteBucket(q url.Values) {
	if q.Has("tagging") {
		if e := c.mustBucket(); e != nil {
			writeError(c.w, c.r, e)
			return
		}
		if err := c.rt.store().SetBucketTags(c.bucket, map[string]string{}); err != nil {
			writeError(c.w, c.r, asS3Error(err))
			return
		}
		writeEmpty(c.w, http.StatusNoContent)
		return
	}
	if subresourceWrite(q) {
		if e := c.mustBucket(); e != nil {
			writeError(c.w, c.r, e)
			return
		}
		writeEmpty(c.w, http.StatusNoContent)
		return
	}
	if err := c.rt.store().DeleteBucket(c.bucket); err != nil {
		writeError(c.w, c.r, asS3Error(err))
		return
	}
	writeEmpty(c.w, http.StatusNoContent)
}

// headBucket answers HEAD /bucket.
func (c *reqCtx) headBucket() {
	if _, ok := c.rt.store().GetBucket(c.bucket); !ok {
		writeError(c.w, c.r, errNoSuchBucket())
		return
	}
	c.w.Header().Set("x-amz-bucket-region", c.rt.region())
	writeEmpty(c.w, http.StatusOK)
}

func (c *reqCtx) subresourceMissing(code, message string) {
	if e := c.mustBucket(); e != nil {
		writeError(c.w, c.r, e)
		return
	}
	writeError(c.w, c.r, s3err(404, code, message))
}

// bucketLocation answers GET /bucket?location.
func (c *reqCtx) bucketLocation() {
	if e := c.mustBucket(); e != nil {
		writeError(c.w, c.r, e)
		return
	}
	doc := xmlLocationConstraint{Xmlns: s3XMLNS}
	if region := c.rt.region(); region != "us-east-1" {
		doc.Value = region
	}
	writeXML(c.w, http.StatusOK, doc)
}

// bucketVersioning answers GET /bucket?versioning.
func (c *reqCtx) bucketVersioning() {
	b, ok := c.rt.store().GetBucket(c.bucket)
	if !ok {
		writeError(c.w, c.r, errNoSuchBucket())
		return
	}
	writeXML(c.w, http.StatusOK, xmlVersioning{Xmlns: s3XMLNS, Status: b.Versioning})
}

// bucketTagging answers GET /bucket?tagging.
func (c *reqCtx) bucketTagging() {
	tags, err := c.rt.store().BucketTags(c.bucket)
	if err != nil {
		writeError(c.w, c.r, asS3Error(err))
		return
	}
	if len(tags) == 0 {
		writeError(c.w, c.r, s3err(404, "NoSuchTagSet", "The TagSet does not exist"))
		return
	}
	writeXML(c.w, http.StatusOK, tagListToXML(tags))
}

// bucketACL answers GET /bucket?acl with the canned emulated policy.
func (c *reqCtx) bucketACL() {
	if e := c.mustBucket(); e != nil {
		writeError(c.w, c.r, e)
		return
	}
	writeXML(c.w, http.StatusOK, aclDocument())
}

// listLimit parses a count-style query parameter with the S3 echo cap.
func listLimit(q url.Values, name string, def int) (int, *s3Error) {
	raw := q.Get(name)
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, s3err(400, "InvalidArgument", "Argument %s must be an integer between 0 and 2147483647", name)
	}
	if n > 1000 {
		n = 1000
	}
	return n, nil
}

// listEncoding consults the encoding-type parameter.
func listEncoding(q url.Values) string {
	if strings.EqualFold(q.Get("encoding-type"), "url") {
		return "url"
	}
	return ""
}

func encodeForList(enc, key string) string {
	if enc == "url" {
		return encodeS3Key(key)
	}
	return key
}

func storageClassOrDefault(sc string) string {
	if sc == "" {
		return "STANDARD"
	}
	return sc
}

// listObjectsV1 answers GET /bucket for the classic (V1) listing API.
func (c *reqCtx) listObjectsV1() {
	q := c.r.URL.Query()
	if e := c.mustBucket(); e != nil {
		writeError(c.w, c.r, e)
		return
	}
	prefix, delimiter := q.Get("prefix"), q.Get("delimiter")
	maxKeys, e := listLimit(q, "max-keys", 1000)
	if e != nil {
		writeError(c.w, c.r, e)
		return
	}
	res, err := c.rt.store().ListObjects(c.bucket, prefix, delimiter, q.Get("marker"), maxKeys)
	if err != nil {
		writeError(c.w, c.r, asS3Error(err))
		return
	}
	enc := listEncoding(q)
	doc := xmlListObjectsV1{
		Xmlns:        s3XMLNS,
		Name:         c.bucket,
		Prefix:       prefix,
		Marker:       q.Get("marker"),
		MaxKeys:      maxKeys,
		Delimiter:    delimiter,
		EncodingType: enc,
		IsTruncated:  res.Truncated,
	}
	if res.Truncated {
		doc.NextMarker = res.NextMarker
	}
	owner := xmlFlociOwner()
	for _, o := range res.Objects {
		doc.Contents = append(doc.Contents, xmlObjectEntry{
			Key:          encodeForList(enc, o.Key),
			LastModified: s3Time(o.LastModified),
			ETag:         o.ETag,
			Size:         o.Size(),
			StorageClass: storageClassOrDefault(o.StorageClass),
			Owner:        &owner,
		})
	}
	for _, p := range res.Prefixes {
		doc.CommonPrefixes = append(doc.CommonPrefixes, xmlCommonPrefix{Prefix: encodeForList(enc, p)})
	}
	writeXML(c.w, http.StatusOK, doc)
}

// listObjectsV2 answers GET /bucket?list-type=2.
func (c *reqCtx) listObjectsV2() {
	q := c.r.URL.Query()
	if e := c.mustBucket(); e != nil {
		writeError(c.w, c.r, e)
		return
	}
	prefix, delimiter := q.Get("prefix"), q.Get("delimiter")
	maxKeys, e := listLimit(q, "max-keys", 1000)
	if e != nil {
		writeError(c.w, c.r, e)
		return
	}
	token := q.Get("continuation-token")
	startAfter := q.Get("start-after")
	marker := token
	if marker == "" {
		marker = startAfter
	}
	res, err := c.rt.store().ListObjects(c.bucket, prefix, delimiter, marker, maxKeys)
	if err != nil {
		writeError(c.w, c.r, asS3Error(err))
		return
	}
	enc := listEncoding(q)
	doc := xmlListObjectsV2{
		Xmlns:        s3XMLNS,
		Name:         c.bucket,
		Prefix:       prefix,
		KeyCount:     len(res.Objects) + len(res.Prefixes),
		MaxKeys:      maxKeys,
		Delimiter:    delimiter,
		EncodingType: enc,
		IsTruncated:  res.Truncated,
	}
	if token != "" {
		doc.ContinuationToken = token
	} else if startAfter != "" {
		doc.StartAfter = startAfter
	}
	if res.Truncated {
		doc.NextContinuationToken = res.NextMarker
	}
	var owner *xmlOwner
	if q.Get("fetch-owner") == "true" {
		o := xmlFlociOwner()
		owner = &o
	}
	for _, o := range res.Objects {
		doc.Contents = append(doc.Contents, xmlObjectEntry{
			Key:          encodeForList(enc, o.Key),
			LastModified: s3Time(o.LastModified),
			ETag:         o.ETag,
			Size:         o.Size(),
			StorageClass: storageClassOrDefault(o.StorageClass),
			Owner:        owner,
		})
	}
	for _, p := range res.Prefixes {
		doc.CommonPrefixes = append(doc.CommonPrefixes, xmlCommonPrefix{Prefix: encodeForList(enc, p)})
	}
	writeXML(c.w, http.StatusOK, doc)
}

// deleteObjects implements POST /bucket?delete.
func (c *reqCtx) deleteObjects() {
	if e := c.mustBucket(); e != nil {
		writeError(c.w, c.r, e)
		return
	}
	body, err := readBodyData(c.r)
	malformed := s3err(400, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema")
	if err != nil {
		writeError(c.w, c.r, malformed)
		return
	}
	var req xmlDeleteRequest
	if len(body) == 0 || xml.Unmarshal(body, &req) != nil || len(req.Objects) == 0 || len(req.Objects) > 1000 {
		writeError(c.w, c.r, malformed)
		return
	}
	doc := xmlDeleteResult{Xmlns: s3XMLNS}
	for _, o := range req.Objects {
		if _, derr := c.rt.store().DeleteObject(c.bucket, o.Key); derr != nil {
			doc.Errors = append(doc.Errors, xmlDeleteError{Key: o.Key, Message: derr.Error()})
			continue
		}
		if !req.Quiet {
			doc.Deleted = append(doc.Deleted, xmlDeletedObject{Key: o.Key})
		}
	}
	writeXML(c.w, http.StatusOK, doc)
}

// listMultipartUploads answers GET /bucket?uploads.
func (c *reqCtx) listMultipartUploads() {
	if e := c.mustBucket(); e != nil {
		writeError(c.w, c.r, e)
		return
	}
	q := c.r.URL.Query()
	maxUploads, e := listLimit(q, "max-uploads", 1000)
	if e != nil {
		writeError(c.w, c.r, e)
		return
	}
	prefix, delimiter := q.Get("prefix"), q.Get("delimiter")
	res, err := c.rt.store().ListUploads(c.bucket, prefix, delimiter, q.Get("key-marker"), q.Get("upload-id-marker"), maxUploads)
	if err != nil {
		writeError(c.w, c.r, asS3Error(err))
		return
	}
	enc := listEncoding(q)
	doc := xmlListUploads{
		Xmlns:              s3XMLNS,
		Bucket:             c.bucket,
		KeyMarker:          q.Get("key-marker"),
		UploadIDMarker:     q.Get("upload-id-marker"),
		NextKeyMarker:      res.NextKey,
		NextUploadIDMarker: res.NextID,
		Delimiter:          delimiter,
		Prefix:             prefix,
		MaxUploads:         maxUploads,
		IsTruncated:        res.Truncated,
	}
	for _, u := range res.Uploads {
		doc.Upload = append(doc.Upload, xmlUpload{
			Key:          encodeForList(enc, u.Key),
			UploadID:     u.UploadID,
			Initiator:    xmlFlociOwner(),
			Owner:        xmlFlociOwner(),
			StorageClass: storageClassOrDefault(u.StorageClass),
			Initiated:    s3Time(u.Initiated),
		})
	}
	for _, p := range res.Prefixes {
		doc.CommonPrefixes = append(doc.CommonPrefixes, xmlCommonPrefix{Prefix: encodeForList(enc, p)})
	}
	writeXML(c.w, http.StatusOK, doc)
}
