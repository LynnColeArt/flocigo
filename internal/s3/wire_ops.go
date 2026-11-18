// wire_ops.go implements the remaining operations served by router.go:
// create bucket, put/copy/delete object, delete bucket, delete objects and
// the two list endpoints.

package s3

import (
    "encoding/xml"
    "io"
    "net/http"
    "net/url"
    "strconv"
    "strings"
)

const (
    wireNoSuchBucketMessage = "The specified bucket does not exist"
    wireNoSuchKeyMessage    = "The specified key does not exist."
)

// wireNoSuchBucket renders the 404 NoSuchBucket error for bucket.
func wireNoSuchBucket(w http.ResponseWriter, bucket string) {
    writeWireError(w, http.StatusNotFound, "NoSuchBucket", wireNoSuchBucketMessage, "<BucketName>"+xmlEscapeWire(bucket)+"</BucketName>")
}

// wireNoSuchKey renders the 404 NoSuchKey error for key.
func wireNoSuchKey(w http.ResponseWriter, key string) {
    writeWireError(w, http.StatusNotFound, "NoSuchKey", wireNoSuchKeyMessage, "<Key>"+xmlEscapeWire(key)+"</Key>")
}

// wireValidBucketName applies the S3 bucket naming rules used by the
// emulator: 3-63 characters of lowercase letters, digits, dots and dashes,
// starting and ending with a letter or digit.
func wireValidBucketName(name string) bool {
    if len(name) < 3 || len(name) > 63 {
        return false
    }
    alnum := func(c byte) bool {
        return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
    }
    for i := 0; i < len(name); i++ {
        c := name[i]
        if !alnum(c) && c != '.' && c != '-' {
            return false
        }
    }
    if !alnum(name[0]) || !alnum(name[len(name)-1]) {
        return false
    }
    return !strings.Contains(name, "..")
}

// wireCreateBucket handles PUT /{bucket}.
func wireCreateBucket(w http.ResponseWriter, r *http.Request, bucket string) {
    if !wireValidBucketName(bucket) {
        writeWireError(w, http.StatusBadRequest, "InvalidBucketName", "The specified bucket is not valid.", "<BucketName>"+xmlEscapeWire(bucket)+"</BucketName>")
        return
    }
    if _, created := wires.createBucket(bucket); !created {
        writeWireError(w, http.StatusConflict, "BucketAlreadyExists", "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.", "<BucketName>"+xmlEscapeWire(bucket)+"</BucketName>")
        return
    }
    _, _ = io.Copy(io.Discard, r.Body)
    w.Header().Set("Location", "/"+bucket)
    w.Header().Set("Content-Length", "0")
    w.WriteHeader(http.StatusOK)
}

// wirePutObject handles PUT /{bucket}/{key}.
func wirePutObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
    bucketState := wires.lookupBucket(bucket)
    if bucketState == nil {
        wireNoSuchBucket(w, bucket)
        return
    }
    body := wireReadBody(r)
    metadata := make(map[string]string)
    for name, values := range r.Header {
        lower := strings.ToLower(name)
        if strings.HasPrefix(lower, "x-amz-meta-") && len(values) > 0 {
            metadata[http.CanonicalHeaderKey(lower)] = values[0]
        }
    }
    object := wires.putObject(bucketState, key, body, r.Header.Get("Content-Type"), metadata)
    w.Header().Set("ETag", wireQuotedETag(object.ETag))
    w.Header().Set("Content-Length", "0")
    w.WriteHeader(http.StatusOK)
}

// wireCopyObject handles PUT /{bucket}/{key} with an x-amz-copy-source header.
func wireCopyObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
    bucketState := wires.lookupBucket(bucket)
    if bucketState == nil {
        wireNoSuchBucket(w, bucket)
        return
    }
    source := r.Header.Get("X-Amz-Copy-Source")
    source = strings.TrimPrefix(source, "/")
    if i := strings.Index(source, "?"); i >= 0 {
        source = source[:i]
    }
    if decoded, err := url.PathUnescape(source); err == nil {
        source = decoded
    }
    sourceBucket, sourceKey, ok := strings.Cut(source, "/")
    if !ok || sourceBucket == "" || sourceKey == "" {
        writeWireError(w, http.StatusBadRequest, "InvalidArgument", "Copy Source must mention the source bucket and key: sourcebucket/sourcekey", "<ArgumentName>x-amz-copy-source</ArgumentName>")
        return
    }
    sourceObject, found := wires.getObject(sourceBucket, sourceKey)
    if !found {
        wireObjectMiss(w, sourceBucket, sourceKey)
        return
    }
    object := wires.putObject(bucketState, key, sourceObject.Body, sourceObject.ContentType, sourceObject.Metadata)
    doc := &wireCopyObjectResult{
        XMLName:      xml.Name{Local: "CopyObjectResult"},
        Xmlns:        wireXMLNS,
        LastModified: object.LastModified.UTC().Format("2006-01-02T15:04:05.000Z"),
        ETag:         wireQuotedETag(object.ETag),
    }
    wireXMLDoc(w, http.StatusOK, doc)
}

// wireDeleteObject handles DELETE /{bucket}/{key}. S3 answers 204 even for
// keys that do not exist.
func wireDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
    if wires.lookupBucket(bucket) == nil {
        wireNoSuchBucket(w, bucket)
        return
    }
    _, _ = io.Copy(io.Discard, r.Body)
    _ = wires.deleteObject(bucket, key)
    w.WriteHeader(http.StatusNoContent)
}

// wireDeleteBucket handles DELETE /{bucket}.
func wireDeleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
    _, _ = io.Copy(io.Discard, r.Body)
    found, empty := wires.deleteBucket(bucket)
    if !found {
        wireNoSuchBucket(w, bucket)
        return
    }
    if !empty {
        writeWireError(w, http.StatusConflict, "BucketNotEmpty", "The bucket you tried to delete is not empty", "<BucketName>"+xmlEscapeWire(bucket)+"</BucketName>")
        return
    }
    w.WriteHeader(http.StatusNoContent)
}

type wireOwner struct {
    ID          string `xml:"ID"`
    DisplayName string `xml:"DisplayName"`
}

type wireBucketListEntry struct {
    Name         string `xml:"Name"`
    CreationDate string `xml:"CreationDate"`
}

type wireBucketList struct {
    Bucket []*wireBucketListEntry `xml:"Bucket"`
}

type wireListAllMyBucketsResult struct {
    XMLName xml.Name       `xml:"ListAllMyBucketsResult"`
    Xmlns   string         `xml:"xmlns,attr"`
    Owner   wireOwner      `xml:"Owner"`
    Buckets wireBucketList `xml:"Buckets"`
}

// wireListBuckets handles GET /.
func wireListBuckets(w http.ResponseWriter, r *http.Request) {
    buckets := wires.listBuckets()
    result := &wireListAllMyBucketsResult{
        XMLName: xml.Name{Local: "ListAllMyBucketsResult"},
        Xmlns:   wireXMLNS,
        Owner:   wireOwner{ID: "floci", DisplayName: "floci"},
    }
    for _, bucket := range buckets {
        result.Buckets.Bucket = append(result.Buckets.Bucket, &wireBucketListEntry{
            Name:         bucket.Name,
            CreationDate: bucket.Created.UTC().Format("2006-01-02T15:04:05.000Z"),
        })
    }
    wireXMLDoc(w, http.StatusOK, result)
}

type wireListEntry struct {
    Key          string `xml:"Key"`
    LastModified string `xml:"LastModified"`
    ETag         string `xml:"ETag"`
    Size         int    `xml:"Size"`
    StorageClass string `xml:"StorageClass"`
}

type wireCommonPrefix struct {
    Prefix string `xml:"Prefix"`
}

type wireListBucketResult struct {
    XMLName        xml.Name            `xml:"ListBucketResult"`
    Xmlns          string              `xml:"xmlns,attr"`
    Name           string              `xml:"Name"`
    Prefix         string              `xml:"Prefix"`
    Marker         string              `xml:"Marker"`
    MaxKeys        int                 `xml:"MaxKeys"`
    Delimiter      string              `xml:"Delimiter"`
    IsTruncated    bool                `xml:"IsTruncated"`
    KeyCount       int                 `xml:"KeyCount,omitempty"`
    Contents       []*wireListEntry    `xml:"Contents"`
    CommonPrefixes []*wireCommonPrefix `xml:"CommonPrefixes"`
}

// wireListObjects handles GET /{bucket} (ListObjects v1 and v2).
func wireListObjects(w http.ResponseWriter, r *http.Request, bucket string) {
    if wires.lookupBucket(bucket) == nil {
        wireNoSuchBucket(w, bucket)
        return
    }
    q := r.URL.Query()
    v2 := q.Get("list-type") == "2"
    prefix := q.Get("prefix")
    marker := q.Get("marker")
    if v2 {
        if startAfter := q.Get("start-after"); startAfter != "" && startAfter > marker {
            marker = startAfter
        }
        if token := q.Get("continuation-token"); token != "" {
            marker = token
        }
    }
    maxKeys := 1000
    if raw := q.Get("max-keys"); raw != "" {
        parsed, err := strconv.Atoi(raw)
        if err != nil || parsed < 0 || parsed > 2147483647 {
            writeWireError(w, http.StatusBadRequest, "InvalidArgument", "Argument max-keys must be an integer between 0 and 2147483647", "<ArgumentName>max-keys</ArgumentName><ArgumentValue>"+xmlEscapeWire(raw)+"</ArgumentValue>")
            return
        }
        maxKeys = parsed
    }
    delimiter := q.Get("delimiter")

    objects := wires.listObjects(bucket, prefix)
    doc := &wireListBucketResult{
        XMLName:   xml.Name{Local: "ListBucketResult"},
        Xmlns:     wireXMLNS,
        Name:      bucket,
        Prefix:    prefix,
        MaxKeys:   maxKeys,
        Delimiter: delimiter,
    }
    if !v2 {
        doc.Marker = marker
    }
    var contents []*wireListEntry
    var commons []*wireCommonPrefix
    seen := make(map[string]bool)
    count := 0
    for _, object := range objects {
        if object.Key <= marker {
            continue
        }
        rest := strings.TrimPrefix(object.Key, prefix)
        if delimiter != "" {
            if i := strings.Index(rest, delimiter); i >= 0 {
                common := prefix + rest[:i+len(delimiter)]
                if seen[common] {
                    continue
                }
                if count >= maxKeys {
                    doc.IsTruncated = true
                    break
                }
                seen[common] = true
                commons = append(commons, &wireCommonPrefix{Prefix: common})
                count++
                continue
            }
        }
        if count >= maxKeys {
            doc.IsTruncated = true
            break
        }
        contents = append(contents, &wireListEntry{
            Key:          object.Key,
            LastModified: object.LastModified.UTC().Format("2006-01-02T15:04:05.000Z"),
            ETag:         wireQuotedETag(object.ETag),
            Size:         len(object.Body),
            StorageClass: "STANDARD",
        })
        count++
    }
    doc.Contents = contents
    doc.CommonPrefixes = commons
    if v2 {
        doc.KeyCount = count
    }
    wireXMLDoc(w, http.StatusOK, doc)
}

type wireDeleteObjectRequest struct {
    Key string `xml:"Key"`
}

type wireDeleteRequest struct {
    XMLName xml.Name                  `xml:"Delete"`
    Quiet   bool                      `xml:"Quiet"`
    Objects []wireDeleteObjectRequest `xml:"Object"`
}

type wireDeleteEntry struct {
    Key string `xml:"Key"`
}

type wireDeleteErrorEntry struct {
    Key     string `xml:"Key"`
    Code    string `xml:"Code"`
    Message string `xml:"Message"`
}

type wireDeleteResult struct {
    XMLName xml.Name                `xml:"DeleteResult"`
    Xmlns   string                  `xml:"xmlns,attr"`
    Deleted []*wireDeleteEntry      `xml:"Deleted"`
    Errors  []*wireDeleteErrorEntry `xml:"Error"`
}

// wireDeleteObjects handles POST /{bucket}?delete (DeleteObjects).
func wireDeleteObjects(w http.ResponseWriter, r *http.Request, bucket string) {
    if wires.lookupBucket(bucket) == nil {
        wireNoSuchBucket(w, bucket)
        return
    }
    body := wireReadBody(r)
    var request wireDeleteRequest
    if err := xml.Unmarshal(body, &request); err != nil || len(request.Objects) == 0 {
        writeWireError(w, http.StatusBadRequest, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema", "")
        return
    }
    result := &wireDeleteResult{
        XMLName: xml.Name{Local: "DeleteResult"},
        Xmlns:   wireXMLNS,
    }
    for _, object := range request.Objects {
        wires.deleteObject(bucket, object.Key)
        if !request.Quiet {
            result.Deleted = append(result.Deleted, &wireDeleteEntry{Key: object.Key})
        }
    }
    wireXMLDoc(w, http.StatusOK, result)
}

type wireCopyObjectResult struct {
    XMLName      xml.Name `xml:"CopyObjectResult"`
    Xmlns        string   `xml:"xmlns,attr"`
    LastModified string   `xml:"LastModified"`
    ETag         string   `xml:"ETag"`
}
