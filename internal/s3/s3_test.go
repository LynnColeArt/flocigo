package s3

import (
    "bytes"
    "crypto/md5"
    "crypto/sha256"
    "encoding/hex"
    "encoding/xml"
    "fmt"
    "io"
    "net/http"
    "net/http/httptest"
    "net/url"
    "strconv"
    "strings"
    "testing"
    "time"
)

const (
    testAccess = "AKIDEXAMPLE"
    testSecret = "wJalrXUtnFEMI"
    testRegion = "us-east-1"
)

func newTestServer(t *testing.T, opts ...Option) *httptest.Server {
    t.Helper()
    ts := httptest.NewServer(New(NewMemStore(), opts...))
    t.Cleanup(ts.Close)
    return ts
}

func newTestServerStore(t *testing.T, store Store, opts ...Option) *httptest.Server {
    t.Helper()
    ts := httptest.NewServer(New(store, opts...))
    t.Cleanup(ts.Close)
    return ts
}

func send(t *testing.T, ts *httptest.Server, method, path string, body []byte, header map[string]string) (*http.Response, []byte) {
    t.Helper()
    return sendURL(t, ts, ts.URL+path, method, body, header)
}

func sendURL(t *testing.T, ts *httptest.Server, rawurl, method string, body []byte, header map[string]string) (*http.Response, []byte) {
    t.Helper()
    var reader io.Reader
    if body != nil {
        reader = bytes.NewReader(body)
    }
    req, err := http.NewRequest(method, rawurl, reader)
    if err != nil {
        t.Fatalf("%s %s: %v", method, rawurl, err)
    }
    for k, v := range header {
        req.Header.Set(k, v)
    }
    resp, err := ts.Client().Do(req)
    if err != nil {
        t.Fatalf("%s %s: %v", method, rawurl, err)
    }
    defer resp.Body.Close()
    got, err := io.ReadAll(resp.Body)
    if err != nil {
        t.Fatalf("%s %s: read body: %v", method, rawurl, err)
    }
    return resp, got
}

func requireStatus(t *testing.T, resp *http.Response, want int) {
    t.Helper()
    if resp.StatusCode != want {
        t.Fatalf("status = %d, want %d", resp.StatusCode, want)
    }
}

func parseXML(t *testing.T, body []byte, v any) {
    t.Helper()
    if err := xml.Unmarshal(body, v); err != nil {
        t.Fatalf("unmarshal %q: %v", body, err)
    }
}

func requireError(t *testing.T, body []byte, code string) errorXML {
    t.Helper()
    var doc errorXML
    parseXML(t, body, &doc)
    if doc.Code != code {
        t.Fatalf("error code = %q (%q), want %q", doc.Code, doc.Message, code)
    }
    return doc
}

func md5Hex(b []byte) string {
    sum := md5.Sum(b)
    return hex.EncodeToString(sum[:])
}

func multipartETag(parts ...string) string {
    h := md5.New()
    for _, p := range parts {
        raw, _ := hex.DecodeString(p)
        h.Write(raw)
    }
    return hex.EncodeToString(h.Sum(nil)) + "-" + strconv.Itoa(len(parts))
}

func TestBucketLifecycle(t *testing.T) {
    ts := newTestServer(t)

    resp, _ := send(t, ts, http.MethodPut, "/photos", nil, nil)
    requireStatus(t, resp, http.StatusOK)
    if loc := resp.Header.Get("Location"); loc != "/photos" {
        t.Errorf("Location = %q", loc)
    }

    resp, body := send(t, ts, http.MethodPut, "/photos", nil, nil)
    requireStatus(t, resp, http.StatusConflict)
    if doc := requireError(t, body, "BucketAlreadyOwnedByYou"); doc.Bucket != "photos" {
        t.Errorf("BucketName = %q", doc.Bucket)
    }

    resp, body = send(t, ts, http.MethodPut, "/Bad_Bucket!", nil, nil)
    requireStatus(t, resp, http.StatusBadRequest)
    requireError(t, body, "InvalidBucketName")

    resp, body = send(t, ts, http.MethodGet, "/photos?location", nil, nil)
    requireStatus(t, resp, http.StatusOK)
    var constraint LocationConstraint
    parseXML(t, body, &constraint)
    if constraint.Value != "" { // us-east-1 answers with an empty constraint
        t.Errorf("LocationConstraint = %q", constraint.Value)
    }

    resp, _ = send(t, ts, http.MethodHead, "/photos", nil, nil)
    requireStatus(t, resp, http.StatusOK)
    resp, _ = send(t, ts, http.MethodHead, "/absent", nil, nil)
    requireStatus(t, resp, http.StatusNotFound)

    resp, _ = send(t, ts, http.MethodPut, "/photos/keeper", []byte("x"), nil)
    requireStatus(t, resp, http.StatusOK)
    resp, body = send(t, ts, http.MethodDelete, "/photos", nil, nil)
    requireStatus(t, resp, http.StatusConflict)
    requireError(t, body, "BucketNotEmpty")

    resp, _ = send(t, ts, http.MethodDelete, "/photos/keeper", nil, nil)
    requireStatus(t, resp, http.StatusNoContent)
    resp, _ = send(t, ts, http.MethodDelete, "/photos", nil, nil)
    requireStatus(t, resp, http.StatusNoContent)
    resp, _ = send(t, ts, http.MethodHead, "/photos", nil, nil)
    requireStatus(t, resp, http.StatusNotFound)
}

func TestObjectLifecycle(t *testing.T) {
    ts := newTestServer(t)

    data := []byte("hello floci")
    putHeaders := map[string]string{
        "Content-Type":                 "image/png",
        "X-Amz-Meta-Tag":               "vacation",
        "X-Amz-Server-Side-Encryption": "AES256",
    }
    resp, _ := send(t, ts, http.MethodPut, "/objects/pic.png", data, putHeaders)
    requireStatus(t, resp, http.StatusOK)
    if etag := resp.Header.Get("ETag"); etag != `"`+md5Hex(data)+`"` {
        t.Errorf("ETag = %q", etag)
    }

    resp, got := send(t, ts, http.MethodGet, "/objects/pic.png", nil, nil)
    requireStatus(t, resp, http.StatusOK)
    if !bytes.Equal(got, data) {
        t.Errorf("body = %q", got)
    }
    if ct := resp.Header.Get("Content-Type"); ct != "image/png" {
        t.Errorf("Content-Type = %q", ct)
    }
    if meta := resp.Header.Get("X-Amz-Meta-Tag"); meta != "vacation" {
        t.Errorf("X-Amz-Meta-Tag = %q", meta)
    }
    if sse := resp.Header.Get("X-Amz-Server-Side-Encryption"); sse != "AES256" {
        t.Errorf("x-amz-server-side-encryption = %q", sse)
    }
    if resp.Header.Get("Last-Modified") == "" {
        t.Error("missing Last-Modified")
    }
    if accept := resp.Header.Get("Accept-Ranges"); accept != "bytes" {
        t.Errorf("Accept-Ranges = %q", accept)
    }

    resp, _ = send(t, ts, http.MethodHead, "/objects/pic.png", nil, nil)
    requireStatus(t, resp, http.StatusOK)
    if cl := resp.Header.Get("Content-Length"); cl != strconv.Itoa(len(data)) {
        t.Errorf("HEAD Content-Length = %q", cl)
    }

    resp, _ = send(t, ts, http.MethodPut, "/objects/raw", []byte{0, 1, 2}, nil)
    requireStatus(t, resp, http.StatusOK)
    resp, _ = send(t, ts, http.MethodGet, "/objects/raw", nil, nil)
    if ct := resp.Header.Get("Content-Type"); ct != "binary/octet-stream" {
        t.Errorf("default Content-Type = %q", ct)
    }

    resp, body := send(t, ts, http.MethodGet, "/objects/ghost", nil, nil)
    requireStatus(t, resp, http.StatusNotFound)
    doc := requireError(t, body, "NoSuchKey")
    if doc.Key != "ghost" {
        t.Errorf("error Key = %q", doc.Key)
    }
    if doc.RequestID == "" || doc.RequestID != resp.Header.Get("x-amz-request-id") {
        t.Errorf("RequestId %q vs header %q", doc.RequestID, resp.Header.Get("x-amz-request-id"))
    }

    resp, body = send(t, ts, http.MethodGet, "/nowhere/x", nil, nil)
    requireStatus(t, resp, http.StatusNotFound)
    if bucket := requireError(t, body, "NoSuchBucket").Bucket; bucket != "nowhere" {
        t.Errorf("BucketName = %q", bucket)
    }

    resp, _ = send(t, ts, http.MethodPost, "/objects", nil, nil)
    requireStatus(t, resp, http.StatusMethodNotAllowed)

    resp, _ = send(t, ts, http.MethodGet, "/objects/pic.png", nil, map[string]string{"If-None-Match": `"` + md5Hex(data) + `"`})
    requireStatus(t, resp, http.StatusNotModified)

    resp, body = send(t, ts, http.MethodGet, "/objects/pic.png", nil, map[string]string{"If-Match": `"stale"`})
    requireStatus(t, resp, http.StatusPreconditionFailed)
    requireError(t, body, "PreconditionFailed")

    resp, got = send(t, ts, http.MethodGet, "/objects/pic.png", nil, map[string]string{"Range": "bytes=6-9"})
    requireStatus(t, resp, http.StatusPartialContent)
    if string(got) != "floc" {
        t.Errorf("range body = %q", got)
    }
    if cr := resp.Header.Get("Content-Range"); cr != "bytes 6-9/11" {
        t.Errorf("Content-Range = %q", cr)
    }
    resp, got = send(t, ts, http.MethodGet, "/objects/pic.png", nil, map[string]string{"Range": "bytes=-5"})
    requireStatus(t, resp, http.StatusPartialContent)
    if string(got) != "floci" {
        t.Errorf("suffix body = %q", got)
    }
    resp, body = send(t, ts, http.MethodGet, "/objects/pic.png", nil, map[string]string{"Range": "bytes=50-60"})
    requireStatus(t, resp, http.StatusRequestedRangeNotSatisfiable)
    requireError(t, body, "InvalidRange")

    resp, _ = send(t, ts, http.MethodDelete, "/objects/pic.png", nil, nil)
    requireStatus(t, resp, http.StatusNoContent)
    resp, _ = send(t, ts, http.MethodGet, "/objects/pic.png", nil, nil)
    requireStatus(t, resp, http.StatusNotFound)
}

func TestListBuckets(t *testing.T) {
    store := NewMemStore()
    if err := store.CreateBucket("alpha", ""); err != nil {
        t.Fatal(err)
    }
    if err := store.CreateBucket("beta", "eu-west-1"); err != nil {
        t.Fatal(err)
    }
    ts := newTestServerStore(t, store)

    resp, body := send(t, ts, http.MethodGet, "/", nil, nil)
    requireStatus(t, resp, http.StatusOK)
    if ct := resp.Header.Get("Content-Type"); ct != "application/xml" {
        t.Errorf("Content-Type = %q", ct)
    }
    var doc ListAllMyBucketsResult
    parseXML(t, body, &doc)
    if doc.Xmlns != s3Namespace {
        t.Errorf("xmlns = %q", doc.Xmlns)
    }
    if doc.Owner.ID != "floci" || doc.Owner.DisplayName != "floci" {
        t.Errorf("owner = %+v", doc.Owner)
    }
    if len(doc.Buckets) != 2 || doc.Buckets[0].Name != "alpha" || doc.Buckets[1].Name != "beta" {
        t.Fatalf("buckets = %+v", doc.Buckets)
    }
    if doc.Buckets[0].CreationDate.IsZero() {
        t.Error("missing CreationDate")
    }
}

func TestListObjectsV2(t *testing.T) {
    store := NewMemStore()
    if err := store.CreateBucket("bkt", ""); err != nil {
        t.Fatal(err)
    }
    keys := []string{"a", "a/1", "a/2", "a/b/3", "b", "c", "sp ace"}
    for _, k := range keys {
        if _, err := store.PutObject("bkt", k, []byte(k), nil); err != nil {
            t.Fatal(err)
        }
    }
    ts := newTestServerStore(t, store)

    resp, body := send(t, ts, http.MethodGet, "/bkt?list-type=2&prefix=a&delimiter=/", nil, nil)
    requireStatus(t, resp, http.StatusOK)
    var doc ListBucketResultV2
    parseXML(t, body, &doc)
    if doc.Name != "bkt" || doc.Prefix != "a" || doc.KeyCount != 3 || doc.MaxKeys != 1000 || doc.IsTruncated {
        t.Errorf("list = %+v", doc)
    }
    if len(doc.Contents) != 1 || doc.Contents[0].Key != "a" || doc.Contents[0].Size != 1 {
        t.Errorf("contents = %+v", doc.Contents)
    }
    if doc.Contents[0].ETag != `"`+md5Hex([]byte("a"))+`"` {
        t.Errorf("etag = %q", doc.Contents[0].ETag)
    }
    var prefixes []string
    for _, cp := range doc.CommonPrefixes {
        prefixes = append(prefixes, cp.Prefix)
    }
    if strings.Join(prefixes, ",") != "a/,a/b/" {
        t.Errorf("prefixes = %v", prefixes)
    }

    resp, body = send(t, ts, http.MethodGet, "/bkt?list-type=2&delimiter=/&max-keys=2", nil, nil)
    parseXML(t, body, &doc)
    if doc.KeyCount != 2 || !doc.IsTruncated || doc.NextContinuationToken == "" {
        t.Fatalf("page1 = %+v", doc)
    }
    if len(doc.Contents) != 1 || doc.Contents[0].Key != "a" || len(doc.CommonPrefixes) != 1 || doc.CommonPrefixes[0].Prefix != "a/" {
        t.Errorf("page1 items = %+v / %+v", doc.Contents, doc.CommonPrefixes)
    }
    token1 := doc.NextContinuationToken

    resp, body = send(t, ts, http.MethodGet, "/bkt?list-type=2&delimiter=/&max-keys=2&continuation-token="+url.QueryEscape(token1), nil, nil)
    parseXML(t, body, &doc)
    if doc.ContinuationToken != token1 {
        t.Errorf("echo token = %q", doc.ContinuationToken)
    }
    if !doc.IsTruncated || doc.KeyCount != 2 || len(doc.Contents) != 1 || doc.Contents[0].Key != "b" || len(doc.CommonPrefixes) != 1 || doc.CommonPrefixes[0].Prefix != "a/b/" {
        t.Fatalf("page2 = %+v / %+v", doc.Contents, doc.CommonPrefixes)
    }
    token2 := doc.NextContinuationToken
    if token2 == token1 {
        t.Error("token did not advance")
    }

    resp, body = send(t, ts, http.MethodGet, "/bkt?list-type=2&delimiter=/&max-keys=2&continuation-token="+url.QueryEscape(token2), nil, nil)
    parseXML(t, body, &doc)
    if doc.IsTruncated || doc.KeyCount != 2 || len(doc.Contents) != 2 || doc.Contents[0].Key != "c" || doc.Contents[1].Key != "sp ace" {
        t.Errorf("page3 = %+v", doc.Contents)
    }

    resp, body = send(t, ts, http.MethodGet, "/bkt?list-type=2&start-after=a/b/3", nil, nil)
    parseXML(t, body, &doc)
    if doc.StartAfter != "a/b/3" || doc.KeyCount != 3 || doc.IsTruncated {
        t.Errorf("start-after doc = %+v", doc)
    }
    if len(doc.Contents) != 3 || doc.Contents[0].Key != "b" {
        t.Errorf("start-after contents = %+v", doc.Contents)
    }

    resp, body = send(t, ts, http.MethodGet, "/bkt?list-type=2&fetch-owner=true&prefix=a&max-keys=1", nil, nil)
    parseXML(t, body, &doc)
    if len(doc.Contents) != 1 || doc.Contents[0].Owner == nil || doc.Contents[0].Owner.ID != "floci" {
        t.Errorf("fetch-owner = %+v", doc.Contents)
    }
}

func TestListObjectsV1(t *testing.T) {
    store := NewMemStore()
    if err := store.CreateBucket("bkt", ""); err != nil {
        t.Fatal(err)
    }
    for _, k := range []string{"a", "a/1", "a/b/2", "b", "x y"} {
        if _, err := store.PutObject("bkt", k, []byte(k), nil); err != nil {
            t.Fatal(err)
        }
    }
    ts := newTestServerStore(t, store)

    resp, body := send(t, ts, http.MethodGet, "/bkt", nil, nil)
    requireStatus(t, resp, http.StatusOK)
    var doc ListBucketResult
    parseXML(t, body, &doc)
    if doc.Xmlns != s3Namespace || doc.Name != "bkt" || doc.MaxKeys != 1000 || doc.IsTruncated || doc.Marker != "" {
        t.Errorf("v1 = %+v", doc)
    }
    if len(doc.Contents) != 5 {
        t.Fatalf("contents = %d", len(doc.Contents))
    }
    last := doc.Contents[4]
    if last.Key != "x y" || last.Size != 3 || last.Owner == nil || last.StorageClass != "STANDARD" {
        t.Errorf("last entry = %+v", last)
    }
    if last.ETag != `"`+md5Hex([]byte("x y"))+`"` {
        t.Errorf("ETag = %q", last.ETag)
    }

    resp, body = send(t, ts, http.MethodGet, "/bkt?delimiter=/&marker=a/b/2", nil, nil)
    parseXML(t, body, &doc)
    if doc.Marker != "a/b/2" || doc.Delimiter != "/" || doc.IsTruncated || doc.NextMarker != "" {
        t.Errorf("marker doc = %+v", doc)
    }
    if len(doc.CommonPrefixes) != 0 || len(doc.Contents) != 2 || doc.Contents[0].Key != "b" || doc.Contents[1].Key != "x y" {
        t.Errorf("after marker: %+v / %+v", doc.Contents, doc.CommonPrefixes)
    }

    resp, body = send(t, ts, http.MethodGet, "/bkt?delimiter=/&max-keys=2", nil, nil)
    parseXML(t, body, &doc)
    if !doc.IsTruncated || doc.NextMarker != "a/b/" || len(doc.Contents) != 0 || len(doc.CommonPrefixes) != 2 {
        t.Fatalf("truncated page = %+v", doc)
    }
    resp, body = send(t, ts, http.MethodGet, "/bkt?delimiter=/&max-keys=2&marker="+url.QueryEscape("a/b/"), nil, nil)
    parseXML(t, body, &doc)
    if len(doc.Contents) != 2 || doc.Contents[0].Key != "b" || doc.Contents[1].Key != "x y" || doc.IsTruncated {
        t.Errorf("resume page = %+v", doc.Contents)
    }

    resp, body = send(t, ts, http.MethodGet, "/bkt?encoding-type=url", nil, nil)
    parseXML(t, body, &doc)
    if doc.EncodingType != "url" {
        t.Errorf("EncodingType = %q", doc.EncodingType)
    }
    if doc.Contents[4].Key != "x+y" {
        t.Errorf("encoded key = %q", doc.Contents[4].Key)
    }
}

func TestBucketRegionOverride(t *testing.T) {
    ts := newTestServer(t, WithRegion("eu-west-1"))
    resp, _ := send(t, ts, http.MethodPut, "/eub", nil, nil)
    requireStatus(t, resp, http.StatusOK)
    resp, body := send(t, ts, http.MethodGet, "/eub?location", nil, nil)
    requireStatus(t, resp, http.StatusOK)
    var loc LocationConstraint
    parseXML(t, body, &loc)
    if loc.Value != "eu-west-1" {
        t.Errorf("location = %q", loc.Value)
    }
}

func TestCopyObject(t *testing.T) {
    ts := newTestServer(t)
    send(t, ts, http.MethodPut, "/copy", nil, nil)
    src := []byte("source data")
    resp, _ := send(t, ts, http.MethodPut, "/copy/a.txt", src, map[string]string{
        "Content-Type": "text/plain",
        "X-Amz-Meta-A": "1",
    })
    srcETag := resp.Header.Get("ETag")

    resp, body := send(t, ts, http.MethodPut, "/copy/b.txt", nil, map[string]string{"X-Amz-Copy-Source": "/copy/a.txt"})
    requireStatus(t, resp, http.StatusOK)
    var copyResult CopyObjectResult
    parseXML(t, body, &copyResult)
    if copyResult.ETag != srcETag {
        t.Errorf("copy etag = %q, want %q", copyResult.ETag, srcETag)
    }
    if copyResult.LastModified.IsZero() {
        t.Error("missing LastModified")
    }
    resp, _ = send(t, ts, http.MethodGet, "/copy/b.txt", nil, nil)
    if ct := resp.Header.Get("Content-Type"); ct != "text/plain" {
        t.Errorf("copied Content-Type = %q", ct)
    }
    if meta := resp.Header.Get("X-Amz-Meta-A"); meta != "1" {
        t.Errorf("copied meta = %q", meta)
    }

    resp, _ = send(t, ts, http.MethodPut, "/copy/c.txt", nil, map[string]string{
        "X-Amz-Copy-Source":        "copy/a.txt",
        "X-Amz-Metadata-Directive": "REPLACE",
        "Content-Type":             "application/json",
        "X-Amz-Meta-B":             "2",
    })
    requireStatus(t, resp, http.StatusOK)
    resp, _ = send(t, ts, http.MethodGet, "/copy/c.txt", nil, nil)
    if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
        t.Errorf("replaced Content-Type = %q", ct)
    }
    if v := resp.Header.Get("X-Amz-Meta-A"); v != "" {
        t.Errorf("stale meta survived: %q", v)
    }
    if v := resp.Header.Get("X-Amz-Meta-B"); v != "2" {
        t.Errorf("new meta = %q", v)
    }

    resp, body = send(t, ts, http.MethodPut, "/copy/a.txt", nil, map[string]string{"X-Amz-Copy-Source": "/copy/a.txt"})
    requireStatus(t, resp, http.StatusBadRequest)
    requireError(t, body, "InvalidRequest")

    resp, body = send(t, ts, http.MethodPut, "/copy/d.txt", nil, map[string]string{
        "X-Amz-Copy-Source":               "/copy/a.txt",
        "X-Amz-Copy-Source-If-None-Match": srcETag,
    })
    requireStatus(t, resp, http.StatusPreconditionFailed)
    requireError(t, body, "PreconditionFailed")

    resp, body = send(t, ts, http.MethodPut, "/copy/e.txt", nil, map[string]string{"X-Amz-Copy-Source": "/copy/ghost"})
    requireStatus(t, resp, http.StatusNotFound)
    requireError(t, body, "NoSuchKey")
}

func TestMultipartLifecycle(t *testing.T) {
    ts := newTestServer(t)
    send(t, ts, http.MethodPut, "/mps", nil, nil)

    part1 := bytes.Repeat([]byte("A"), minPartSize)
    part2 := []byte("tail")
    etag1, etag2 := md5Hex(part1), md5Hex(part2)

    resp, body := send(t, ts, http.MethodPost, "/mps/big?uploads", nil, nil)
    requireStatus(t, resp, http.StatusOK)
    var initiated InitiateMultipartUploadResult
    parseXML(t, body, &initiated)
    if initiated.Bucket != "mps" || initiated.Key != "big" || initiated.UploadID == "" {
        t.Fatalf("initiated = %+v", initiated)
    }
    uploadID := initiated.UploadID

    resp, body = send(t, ts, http.MethodPut, "/mps/big?partNumber=10001&uploadId="+uploadID, part2, nil)
    requireStatus(t, resp, http.StatusBadRequest)
    requireError(t, body, "InvalidArgument")

    resp, _ = send(t, ts, http.MethodPut, "/mps/big?partNumber=1&uploadId="+uploadID, part1, nil)
    requireStatus(t, resp, http.StatusOK)
    resp, _ = send(t, ts, http.MethodPut, "/mps/big?partNumber=2&uploadId="+uploadID, part2, nil)
    requireStatus(t, resp, http.StatusOK)

    resp, body = send(t, ts, http.MethodPut, "/mps/big?partNumber=3&uploadId=nope", part2, nil)
    requireStatus(t, resp, http.StatusNotFound)
    requireError(t, body, "NoSuchUpload")

    resp, body = send(t, ts, http.MethodGet, "/mps/big?uploadId="+uploadID, nil, nil)
    requireStatus(t, resp, http.StatusOK)
    var parts ListPartsResult
    parseXML(t, body, &parts)
    if parts.UploadID != uploadID || len(parts.Parts) != 2 || parts.MaxParts != 1000 || parts.IsTruncated {
        t.Fatalf("parts = %+v", parts)
    }
    if parts.Parts[0].PartNumber != 1 || parts.Parts[0].Size != int64(len(part1)) || parts.Parts[0].ETag != `"`+etag1+`"` {
        t.Errorf("part1 = %+v", parts.Parts[0])
    }

    resp, body = send(t, ts, http.MethodPost, "/mps/big?uploadId="+uploadID, []byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"bad"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>"`+etag2+`"</ETag></Part></CompleteMultipartUpload>`), nil)
    requireStatus(t, resp, http.StatusBadRequest)
    requireError(t, body, "InvalidPart")

    resp, body = send(t, ts, http.MethodPost, "/mps/big?uploadId="+uploadID, []byte(`<CompleteMultipartUpload><Part><PartNumber>2</PartNumber><ETag>"`+etag2+`"</ETag></Part><Part><PartNumber>1</PartNumber><ETag>"`+etag1+`"</ETag></Part></CompleteMultipartUpload>`), nil)
    requireStatus(t, resp, http.StatusBadRequest)
    requireError(t, body, "InvalidPartOrder")

    completeBody := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"` + etag1 + `"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>"` + etag2 + `"</ETag></Part></CompleteMultipartUpload>`
    resp, body = send(t, ts, http.MethodPost, "/mps/big?uploadId="+uploadID, []byte(completeBody), nil)
    requireStatus(t, resp, http.StatusOK)
    var done CompleteMultipartUploadResult
    parseXML(t, body, &done)
    wantETag := `"` + multipartETag(etag1, etag2) + `"`
    if done.ETag != wantETag {
        t.Errorf("complete ETag = %q, want %q", done.ETag, wantETag)
    }
    if done.Bucket != "mps" || done.Key != "big" || !strings.Contains(done.Location, "/mps/big") {
        t.Errorf("complete result = %+v", done)
    }
    if resp.Header.Get("ETag") != wantETag {
        t.Errorf("ETag header = %q", resp.Header.Get("ETag"))
    }

    resp, got := send(t, ts, http.MethodGet, "/mps/big", nil, nil)
    requireStatus(t, resp, http.StatusOK)
    want := append(append([]byte{}, part1...), part2...)
    if !bytes.Equal(got, want) {
        t.Errorf("assembled size = %d", len(got))
    }
    if resp.Header.Get("ETag") != wantETag {
        t.Errorf("object ETag = %q", resp.Header.Get("ETag"))
    }

    resp, body = send(t, ts, http.MethodGet, "/mps/big?uploadId="+uploadID, nil, nil)
    requireStatus(t, resp, http.StatusNotFound)
    requireError(t, body, "NoSuchUpload")

    resp, body = send(t, ts, http.MethodPost, "/mps/other?uploads", nil, nil)
    parseXML(t, body, &initiated)
    second := initiated.UploadID
    resp, _ = send(t, ts, http.MethodDelete, "/mps/other?uploadId="+second, nil, nil)
    requireStatus(t, resp, http.StatusNoContent)
    resp, body = send(t, ts, http.MethodGet, "/mps/other?uploadId="+second, nil, nil)
    requireStatus(t, resp, http.StatusNotFound)
    requireError(t, body, "NoSuchUpload")

    resp, body = send(t, ts, http.MethodPost, "/mps/small?uploads", nil, nil)
    parseXML(t, body, &initiated)
    small := initiated.UploadID
    send(t, ts, http.MethodPut, "/mps/small?partNumber=1&uploadId="+small, []byte("s1"), nil)
    send(t, ts, http.MethodPut, "/mps/small?partNumber=2&uploadId="+small, []byte("s2"), nil)
    smallBody := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"` + md5Hex([]byte("s1")) + `"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>"` + md5Hex([]byte("s2")) + `"</ETag></Part></CompleteMultipartUpload>`
    resp, body = send(t, ts, http.MethodPost, "/mps/small?uploadId="+small, []byte(smallBody), nil)
    requireStatus(t, resp, http.StatusBadRequest)
    requireError(t, body, "EntityTooSmall")
}

func TestUploadPartCopy(t *testing.T) {
    ts := newTestServer(t)
    send(t, ts, http.MethodPut, "/mpc", nil, nil)
    source := bytes.Repeat([]byte("xzy"), 64) // 192 bytes
    send(t, ts, http.MethodPut, "/mpc/src.bin", source, nil)

    resp, body := send(t, ts, http.MethodPost, "/mpc/dst?uploads", nil, nil)
    var initiated InitiateMultipartUploadResult
    parseXML(t, body, &initiated)
    uploadID := initiated.UploadID

    headers := map[string]string{
        "X-Amz-Copy-Source":       "/mpc/src.bin",
        "X-Amz-Copy-Source-Range": "bytes=0-63",
    }
    resp, body = send(t, ts, http.MethodPut, "/mpc/dst?partNumber=1&uploadId="+uploadID, nil, headers)
    requireStatus(t, resp, http.StatusOK)
    var copyPart CopyPartResult
    parseXML(t, body, &copyPart)
    if copyPart.ETag != `"`+md5Hex(source[:64])+`"` {
        t.Errorf("copy-part ETag = %q", copyPart.ETag)
    }
    if copyPart.LastModified.IsZero() {
        t.Error("missing LastModified")
    }

    headers["X-Amz-Copy-Source-Range"] = "bytes=64-191"
    resp, body = send(t, ts, http.MethodPut, "/mpc/dst?partNumber=2&uploadId="+uploadID, nil, headers)
    requireStatus(t, resp, http.StatusOK)

    headers["X-Amz-Copy-Source-Range"] = "bytes=5-3"
    resp, body = send(t, ts, http.MethodPut, "/mpc/dst?partNumber=3&uploadId="+uploadID, nil, headers)
    requireStatus(t, resp, http.StatusBadRequest)
    requireError(t, body, "InvalidArgument")

    compBody := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"` + md5Hex(source[:64]) + `"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>"` + md5Hex(source[64:]) + `"</ETag></Part></CompleteMultipartUpload>`
    resp, body = send(t, ts, http.MethodPost, "/mpc/dst?uploadId="+uploadID, []byte(compBody), nil)
    requireStatus(t, resp, http.StatusOK)
    resp, got := send(t, ts, http.MethodGet, "/mpc/dst", nil, nil)
    requireStatus(t, resp, http.StatusOK)
    if !bytes.Equal(got, source) {
        t.Errorf("copy result mismatch: %d bytes", len(got))
    }
    if resp.Header.Get("ETag") != `"`+multipartETag(md5Hex(source[:64]), md5Hex(source[64:]))+`"` {
        t.Errorf("unexpected final ETag %q", resp.Header.Get("ETag"))
    }
}

func TestDeleteObjects(t *testing.T) {
    ts := newTestServer(t)
    send(t, ts, http.MethodPut, "/batch", nil, nil)
    for _, k := range []string{"k1", "k2", "k3"} {
        send(t, ts, http.MethodPut, "/batch/"+k, []byte(k), nil)
    }

    resp, body := send(t, ts, http.MethodPost, "/batch?delete", []byte(`<Delete><Object><Key>k1</Key></Object><Object><Key>k2</Key></Object></Delete>`), nil)
    requireStatus(t, resp, http.StatusOK)
    var del DeleteResult
    parseXML(t, body, &del)
    if len(del.Deleted) != 2 || del.Deleted[0].Key != "k1" || del.Deleted[1].Key != "k2" {
        t.Fatalf("deleted = %+v", del.Deleted)
    }
    resp, _ = send(t, ts, http.MethodGet, "/batch/k1", nil, nil)
    requireStatus(t, resp, http.StatusNotFound)

    resp, body = send(t, ts, http.MethodPost, "/batch?delete", []byte(`<Delete><Quiet>true</Quiet><Object><Key>k3</Key></Object></Delete>`), nil)
    requireStatus(t, resp, http.StatusOK)
    parseXML(t, body, &del)
    if len(del.Deleted) != 0 {
        t.Errorf("quiet deleted = %+v", del.Deleted)
    }

    resp, body = send(t, ts, http.MethodPost, "/batch?delete", []byte("not xml"), nil)
    requireStatus(t, resp, http.StatusBadRequest)
    requireError(t, body, "MalformedXML")
}

func TestListMultipartUploads(t *testing.T) {
    ts := newTestServer(t)
    send(t, ts, http.MethodPut, "/ups", nil, nil)
    for _, key := range []string{"a/one", "a/two", "z"} {
        resp, body := send(t, ts, http.MethodPost, "/ups/"+key+"?uploads", nil, nil)
        requireStatus(t, resp, http.StatusOK)
        var ini InitiateMultipartUploadResult
        parseXML(t, body, &ini)
        if ini.Key != key {
            t.Errorf("upload key = %q, want %q", ini.Key, key)
        }
    }

    resp, body := send(t, ts, http.MethodGet, "/ups?uploads&delimiter=/", nil, nil)
    requireStatus(t, resp, http.StatusOK)
    var listing ListMultipartUploadsResult
    parseXML(t, body, &listing)
    if listing.Bucket != "ups" || listing.MaxUploads != 1000 || listing.IsTruncated || listing.Delimiter != "/" {
        t.Errorf("listing = %+v", listing)
    }
    if len(listing.CommonPrefixes) != 1 || listing.CommonPrefixes[0].Prefix != "a/" {
        t.Errorf("prefixes = %+v", listing.CommonPrefixes)
    }
    if len(listing.Uploads) != 1 || listing.Uploads[0].Key != "z" || listing.Uploads[0].UploadID == "" {
        t.Errorf("uploads = %+v", listing.Uploads)
    }

    resp, body = send(t, ts, http.MethodGet, "/ups?uploads&key-marker=a/", nil, nil)
    parseXML(t, body, &listing)
    if len(listing.Uploads) != 1 || listing.Uploads[0].Key != "z" || len(listing.CommonPrefixes) != 0 {
        t.Errorf("resumed listing = %+v / %+v", listing.Uploads, listing.CommonPrefixes)
    }
}

// presign builds a presigned URL independently of the server's request
// plumbing, mirroring the SigV4 presign spec.
func presign(t *testing.T, method, base, path, access, secret, region string, signed time.Time, expires int64) string {
    t.Helper()
    u, err := url.Parse(base)
    if err != nil {
        t.Fatal(err)
    }
    host := u.Host
    date := signed.UTC().Format("20060102")
    datetime := signed.UTC().Format("20060102T150405Z")
    scope := access + "/" + date + "/" + region + "/s3/aws4_request"
    query := strings.Join([]string{
        "X-Amz-Algorithm=AWS4-HMAC-SHA256",
        "X-Amz-Credential=" + awsURLEncode(scope),
        "X-Amz-Date=" + datetime,
        "X-Amz-Expires=" + strconv.FormatInt(expires, 10),
        "X-Amz-SignedHeaders=host",
    }, "&")
    canonical := strings.Join([]string{
        method,
        path,
        query,
        "host:" + host,
        "host",
        "UNSIGNED-PAYLOAD",
    }, "\n")
    signer := &V4Authenticator{}
    sig := signer.sign(datetime, secret, date, region, "s3", canonical)
    return base + path + "?" + query + "&X-Amz-Signature=" + sig
}

func TestPresignedRequests(t *testing.T) {
    store := NewMemStore()
    if err := store.CreateBucket("secure", ""); err != nil {
        t.Fatal(err)
    }
    doc := []byte("classified payload")
    if _, err := store.PutObject("secure", "file.txt", doc, nil); err != nil {
        t.Fatal(err)
    }
    auth := NewV4Authenticator(StaticCredentials{AccessKeyID: testAccess, SecretAccessKey: testSecret}, testRegion)
    ts := httptest.NewServer(New(store, WithAuthenticator(auth)))
    t.Cleanup(ts.Close)

    signed := presign(t, http.MethodGet, ts.URL, "/secure/file.txt", testAccess, testSecret, testRegion, time.Now().UTC(), 900)
    resp, got := sendURL(t, ts, signed, http.MethodGet, nil, nil)
    requireStatus(t, resp, http.StatusOK)
    if !bytes.Equal(got, doc) {
        t.Errorf("presigned body = %q", got)
    }

    putURL := presign(t, http.MethodPut, ts.URL, "/secure/uploaded.txt", testAccess, testSecret, testRegion, time.Now().UTC(), 900)
    payload := []byte("via presign")
    resp, _ = sendURL(t, ts, putURL, http.MethodPut, payload, nil)
    requireStatus(t, resp, http.StatusOK)

    tampered := signed[:len(signed)-4] + "0000"
    resp, body := sendURL(t, ts, tampered, http.MethodGet, nil, nil)
    requireStatus(t, resp, http.StatusForbidden)
    requireError(t, body, "SignatureDoesNotMatch")

    expired := presign(t, http.MethodGet, ts.URL, "/secure/file.txt", testAccess, testSecret, testRegion, time.Now().Add(-2*time.Hour).UTC(), 600)
    resp, body = sendURL(t, ts, expired, http.MethodGet, nil, nil)
    requireStatus(t, resp, http.StatusForbidden)
    if msg := requireError(t, body, "AccessDenied").Message; msg != "Request has expired" {
        t.Errorf("expired message = %q", msg)
    }

    huge := presign(t, http.MethodGet, ts.URL, "/secure/file.txt", testAccess, testSecret, testRegion, time.Now().UTC(), 604801)
    resp, body = sendURL(t, ts, huge, http.MethodGet, nil, nil)
    requireStatus(t, resp, http.StatusBadRequest)
    requireError(t, body, "AuthorizationQueryParametersError")

    foreign := presign(t, http.MethodGet, ts.URL, "/secure/file.txt", "AKIDNOBODY", testSecret, testRegion, time.Now().UTC(), 900)
    resp, body = sendURL(t, ts, foreign, http.MethodGet, nil, nil)
    requireStatus(t, resp, http.StatusForbidden)
    requireError(t, body, "InvalidAccessKeyId")

    other := presign(t, http.MethodGet, ts.URL, "/secure/file.txt", testAccess, testSecret, "eu-west-1", time.Now().UTC(), 900)
    resp, body = sendURL(t, ts, other, http.MethodGet, nil, nil)
    requireStatus(t, resp, http.StatusBadRequest)
    requireError(t, body, "AuthorizationQueryParametersError")

    resp, body = sendURL(t, ts, ts.URL+"/secure/file.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256", http.MethodGet, nil, nil)
    requireStatus(t, resp, http.StatusBadRequest)
    requireError(t, body, "AuthorizationQueryParametersError")
}

func TestSignedHeaderAuth(t *testing.T) {
    store := NewMemStore()
    if err := store.CreateBucket("hs", ""); err != nil {
        t.Fatal(err)
    }
    if _, err := store.PutObject("hs", "k", []byte("v"), nil); err != nil {
        t.Fatal(err)
    }
    auth := NewV4Authenticator(StaticCredentials{AccessKeyID: testAccess, SecretAccessKey: testSecret}, testRegion)
    ts := httptest.NewServer(New(store, WithAuthenticator(auth)))
    t.Cleanup(ts.Close)

    request := func(tamper bool) *http.Request {
        req, err := http.NewRequest(http.MethodGet, ts.URL+"/hs/k", nil)
        if err != nil {
            t.Fatal(err)
        }
        now := time.Now().UTC()
        date := now.Format("20060102")
        datetime := now.Format("20060102T150405Z")
        emptySHA := sha256.Sum256(nil)
        payload := hex.EncodeToString(emptySHA[:])
        canonical := strings.Join([]string{
            http.MethodGet,
            "/hs/k",
            "",
            "host:" + req.URL.Host,
            "host",
            payload,
        }, "\n")
        signer := &V4Authenticator{}
        sig := signer.sign(datetime, testSecret, date, testRegion, "s3", canonical)
        if tamper {
            sig = sig[:60] + "0000"
        }
        scope := testAccess + "/" + date + "/" + testRegion + "/s3/aws4_request"
        req.Header.Set("x-amz-date", datetime)
        req.Header.Set("x-amz-content-sha256", payload)
        req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+scope+", SignedHeaders=host, Signature="+sig)
        return req
    }

    resp, err := ts.Client().Do(request(false))
    if err != nil {
        t.Fatal(err)
    }
    good, _ := io.ReadAll(resp.Body)
    resp.Body.Close()
    requireStatus(t, resp, http.StatusOK)
    if string(good) != "v" {
        t.Errorf("body = %q", good)
    }

    resp, err = ts.Client().Do(request(true))
    if err != nil {
        t.Fatal(err)
    }
    bad, _ := io.ReadAll(resp.Body)
    resp.Body.Close()
    requireStatus(t, resp, http.StatusForbidden)
    requireError(t, bad, "SignatureDoesNotMatch")
}
