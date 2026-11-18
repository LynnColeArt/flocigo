// wire_multipart.go implements the multipart upload endpoints served by
// router.go: initiate, upload part, list parts, complete and abort.

package s3

import (
    "crypto/md5"
    "crypto/rand"
    "encoding/hex"
    "encoding/xml"
    "io"
    "net/http"
    "sort"
    "strconv"
    "time"
)

// wireNewUploadID returns a random multipart upload identifier.
func wireNewUploadID() string {
    buf := make([]byte, 16)
    _, _ = rand.Read(buf)
    return hex.EncodeToString(buf)
}

// wireNoSuchUpload renders the 404 NoSuchUpload error.
func wireNoSuchUpload(w http.ResponseWriter, uploadID string) {
    writeWireError(w, http.StatusNotFound, "NoSuchUpload", "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.", "<UploadId>"+xmlEscapeWire(uploadID)+"</UploadId>")
}

type wireInitiateMultipartUploadResult struct {
    XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
    Xmlns    string   `xml:"xmlns,attr"`
    Bucket   string   `xml:"Bucket"`
    Key      string   `xml:"Key"`
    UploadId string   `xml:"UploadId"`
}

// wireCreateMultipartUpload handles POST /{bucket}/{key}?uploads.
func wireCreateMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
    bucketState := wires.lookupBucket(bucket)
    if bucketState == nil {
        wireNoSuchBucket(w, bucket)
        return
    }
    _, _ = io.Copy(io.Discard, r.Body)
    uploadID := wireNewUploadID()
    wires.mu.Lock()
    bucketState.Uploads[uploadID] = &wireUpload{
        ID:          uploadID,
        Bucket:      bucket,
        Key:         key,
        ContentType: r.Header.Get("Content-Type"),
        Parts:       make(map[int]*wirePart),
    }
    wires.mu.Unlock()
    doc := &wireInitiateMultipartUploadResult{
        XMLName:  xml.Name{Local: "InitiateMultipartUploadResult"},
        Xmlns:    wireXMLNS,
        Bucket:   bucket,
        Key:      key,
        UploadId: uploadID,
    }
    wireXMLDoc(w, http.StatusOK, doc)
}

// wireUploadPart handles PUT /{bucket}/{key}?partNumber=N&uploadId=ID.
func wireUploadPart(w http.ResponseWriter, r *http.Request, bucket, key, partNumber, uploadID string) {
    _ = key
    bucketState := wires.lookupBucket(bucket)
    if bucketState == nil {
        wireNoSuchBucket(w, bucket)
        return
    }
    number, err := strconv.Atoi(partNumber)
    if err != nil || number < 1 || number > 10000 {
        writeWireError(w, http.StatusBadRequest, "InvalidArgument", "Part number must be an integer between 1 and 10000, inclusive", "<ArgumentName>partNumber</ArgumentName><ArgumentValue>"+xmlEscapeWire(partNumber)+"</ArgumentValue>")
        return
    }
    body := wireReadBody(r)
    etag := wireETag(body)
    wires.mu.Lock()
    upload, ok := bucketState.Uploads[uploadID]
    if !ok {
        wires.mu.Unlock()
        wireNoSuchUpload(w, uploadID)
        return
    }
    upload.Parts[number] = &wirePart{Number: number, Body: body, ETag: etag, LastModified: time.Now().UTC()}
    wires.mu.Unlock()
    w.Header().Set("ETag", wireQuotedETag(etag))
    w.Header().Set("Content-Length", "0")
    w.WriteHeader(http.StatusOK)
}

type wireCompleteMultipartUploadResult struct {
    XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
    Xmlns    string   `xml:"xmlns,attr"`
    Location string   `xml:"Location"`
    Bucket   string   `xml:"Bucket"`
    Key      string   `xml:"Key"`
    ETag     string   `xml:"ETag"`
}

// wireCompleteMultipartUpload handles POST /{bucket}/{key}?uploadId=ID by
// concatenating the uploaded parts in order and storing the final object.
func wireCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string) {
    bucketState := wires.lookupBucket(bucket)
    if bucketState == nil {
        wireNoSuchBucket(w, bucket)
        return
    }
    _, _ = io.Copy(io.Discard, r.Body)
    wires.mu.Lock()
    upload, ok := bucketState.Uploads[uploadID]
    if !ok {
        wires.mu.Unlock()
        wireNoSuchUpload(w, uploadID)
        return
    }
    numbers := make([]int, 0, len(upload.Parts))
    for number := range upload.Parts {
        numbers = append(numbers, number)
    }
    if len(numbers) == 0 {
        wires.mu.Unlock()
        writeWireError(w, http.StatusBadRequest, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema", "")
        return
    }
    sort.Ints(numbers)
    var data []byte
    var etags string
    for _, number := range numbers {
        part := upload.Parts[number]
        data = append(data, part.Body...)
        etags += part.ETag
    }
    digest := md5.Sum([]byte(etags))
    etag := hex.EncodeToString(digest[:]) + "-" + strconv.Itoa(len(numbers))
    bucketState.Objects[key] = &wireObject{
        Key:          key,
        Body:         data,
        ContentType:  upload.ContentType,
        ETag:         etag,
        LastModified: time.Now().UTC(),
    }
    delete(bucketState.Uploads, uploadID)
    wires.mu.Unlock()
    doc := &wireCompleteMultipartUploadResult{
        XMLName:  xml.Name{Local: "CompleteMultipartUploadResult"},
        Xmlns:    wireXMLNS,
        Location: "http://" + r.Host + "/" + bucket + "/" + key,
        Bucket:   bucket,
        Key:      key,
        ETag:     wireQuotedETag(etag),
    }
    wireXMLDoc(w, http.StatusOK, doc)
}

// wireAbortMultipartUpload handles DELETE /{bucket}/{key}?uploadId=ID.
func wireAbortMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string) {
    _ = key
    bucketState := wires.lookupBucket(bucket)
    if bucketState == nil {
        wireNoSuchBucket(w, bucket)
        return
    }
    _, _ = io.Copy(io.Discard, r.Body)
    wires.mu.Lock()
    _, ok := bucketState.Uploads[uploadID]
    if ok {
        delete(bucketState.Uploads, uploadID)
    }
    wires.mu.Unlock()
    if !ok {
        wireNoSuchUpload(w, uploadID)
        return
    }
    w.WriteHeader(http.StatusNoContent)
}

type wireListPartsEntry struct {
    PartNumber   int    `xml:"PartNumber"`
    LastModified string `xml:"LastModified"`
    ETag         string `xml:"ETag"`
    Size         int    `xml:"Size"`
}

type wireListPartsResult struct {
    XMLName      xml.Name          `xml:"ListPartsResult"`
    Xmlns        string            `xml:"xmlns,attr"`
    Bucket       string            `xml:"Bucket"`
    Key          string            `xml:"Key"`
    UploadId     string            `xml:"UploadId"`
    StorageClass string            `xml:"StorageClass"`
    Parts        []*wireListPartsEntry `xml:"Part"`
}

// wireListParts handles GET /{bucket}/{key}?uploadId=ID.
func wireListParts(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string) {
    _ = r
    bucketState := wires.lookupBucket(bucket)
    if bucketState == nil {
        wireNoSuchBucket(w, bucket)
        return
    }
    wires.mu.Lock()
    upload, ok := bucketState.Uploads[uploadID]
    wires.mu.Unlock()
    if !ok {
        wireNoSuchUpload(w, uploadID)
        return
    }
    numbers := make([]int, 0, len(upload.Parts))
    for number := range upload.Parts {
        numbers = append(numbers, number)
    }
    sort.Ints(numbers)
    doc := &wireListPartsResult{
        XMLName:      xml.Name{Local: "ListPartsResult"},
        Xmlns:        wireXMLNS,
        Bucket:       bucket,
        Key:          key,
        UploadId:     uploadID,
        StorageClass: "STANDARD",
    }
    for _, number := range numbers {
        part := upload.Parts[number]
        doc.Parts = append(doc.Parts, &wireListPartsEntry{
            PartNumber:   part.Number,
            LastModified: part.LastModified.UTC().Format("2006-01-02T15:04:05.000Z"),
            ETag:         wireQuotedETag(part.ETag),
            Size:         len(part.Body),
        })
    }
    wireXMLDoc(w, http.StatusOK, doc)
}

type wireUploadEntry struct {
    Key      string `xml:"Key"`
    UploadId string `xml:"UploadId"`
}

type wireListMultipartUploadsResult struct {
    XMLName xml.Name           `xml:"ListMultipartUploadsResult"`
    Xmlns   string             `xml:"xmlns,attr"`
    Bucket  string             `xml:"Bucket"`
    Uploads []*wireUploadEntry `xml:"Upload"`
}

// wireListMultipartUploads handles GET /{bucket}?uploads.
func wireListMultipartUploads(w http.ResponseWriter, r *http.Request, bucket string) {
    _ = r
    if wires.lookupBucket(bucket) == nil {
        wireNoSuchBucket(w, bucket)
        return
    }
    uploads := wires.listUploads(bucket)
    doc := &wireListMultipartUploadsResult{
        XMLName: xml.Name{Local: "ListMultipartUploadsResult"},
        Xmlns:   wireXMLNS,
        Bucket:  bucket,
    }
    for _, upload := range uploads {
        doc.Uploads = append(doc.Uploads, &wireUploadEntry{Key: upload.Key, UploadId: upload.ID})
    }
    wireXMLDoc(w, http.StatusOK, doc)
}
