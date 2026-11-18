package s3

import (
	"bytes"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func newServer(t *testing.T) (*Router, string) {
	t.Helper()
	rt := New()
	ts := httptest.NewServer(rt)
	t.Cleanup(ts.Close)
	return rt, ts.URL
}

func call(t *testing.T, method, rawurl string, headers map[string]string, body []byte) (int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, rawurl, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build %s %s: %v", method, rawurl, err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return doReq(t, req)
}

func doReq(t *testing.T, req *http.Request) (int, http.Header, []byte) {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", req.Method, req.URL, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp.StatusCode, resp.Header, data
}

func TestSplitTargetRouting(t *testing.T) {
	cases := []struct {
		host, path string
		bucket, key string
		hostStyle  bool
	}{
		{"s3.amazonaws.com", "/bkt/key", "bkt", "key", false},
		{"s3.amazonaws.com", "/bkt", "bkt", "", false},
		{"s3.us-west-2.amazonaws.com", "/a/b/c", "a", "b/c", false},
		{"bkt.s3.amazonaws.com", "/key", "bkt", "key", true},
		{"bkt.s3.us-west-2.amazonaws.com", "/key", "bkt", "key", true},
		{"bkt.s3.localhost", "/key", "bkt", "key", true},
		{"s3.localhost", "/bkt/key", "bkt", "key", false},
		{"bkt.localhost", "/key", "bkt", "key", true},
		{"localhost", "/bkt/key", "bkt", "key", false},
		{"localhost:4566", "/bkt", "bkt", "", false},
		{"bkt.localhost:4566", "/k", "bkt", "k", true},
	}
	for i, tc := range cases {
		b, k, hs := splitTarget(tc.host, tc.path)
		if b != tc.bucket || k != tc.key || hs != tc.hostStyle {
			t.Errorf("case %d (%s, %s): got (%q, %q, %v), want (%q, %q, %v)",
				i, tc.host, tc.path, b, k, hs, tc.bucket, tc.key, tc.hostStyle)
		}
	}
}

func TestVirtualHostRouting(t *testing.T) {
	_, base := newServer(t)
	code, _, _ := call(t, http.MethodPut, base+"/hosted", nil, nil)
	if code != http.StatusOK {
		t.Fatalf("create bucket: %d", code)
	}
	code, _, _ = call(t, http.MethodPut, base+"/hosted/obj", nil, []byte("hv"))
	if code != http.StatusOK {
		t.Fatalf("put: %d", code)
	}
	req, err := http.NewRequest(http.MethodGet, base+"/obj", nil)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	req.Host = "hosted.s3.localhost"
	code, _, body := doReq(t, req)
	if code != http.StatusOK || string(body) != "hv" {
		t.Fatalf("virtual-host GET: %d %s", code, body)
	}
}

func TestBucketLifecycle(t *testing.T) {
	_, base := newServer(t)
	code, _, _ := call(t, http.MethodPut, base+"/life", nil, nil)
	if code != http.StatusOK {
		t.Fatalf("create: %d", code)
	}
	code, _, body := call(t, http.MethodPut, base+"/life", nil, nil)
	if code != http.StatusConflict || !strings.Contains(string(body), "BucketAlreadyExists") {
		t.Fatalf("recreate: %d %s", code, body)
	}
	code, _, body = call(t, http.MethodGet, base+"/", nil, nil)
	if code != http.StatusOK || !strings.Contains(string(body), "<Name>life</Name>") {
		t.Fatalf("list buckets: %d %s", code, body)
	}
	code, _, _ = call(t, http.MethodPut, base+"/life/obj", nil, []byte("x"))
	if code != http.StatusOK {
		t.Fatalf("put obj: %d", code)
	}
	code, _, body = call(t, http.MethodDelete, base+"/life", nil, nil)
	if code != http.StatusConflict || !strings.Contains(string(body), "BucketNotEmpty") {
		t.Fatalf("delete non-empty: %d %s", code, body)
	}
	code, _, _ = call(t, http.MethodDelete, base+"/life/obj", nil, nil)
	if code != http.StatusNoContent {
		t.Fatalf("delete obj: %d", code)
	}
	code, _, _ = call(t, http.MethodDelete, base+"/life", nil, nil)
	if code != http.StatusNoContent {
		t.Fatalf("delete bucket: %d", code)
	}
	code, _, _ = call(t, http.MethodHead, base+"/life", nil, nil)
	if code != http.StatusNotFound {
		t.Fatalf("head missing bucket: %d", code)
	}
}

func TestObjectMetadataRoundTrip(t *testing.T) {
	_, base := newServer(t)
	call(t, http.MethodPut, base+"/m1", nil, nil)
	hdrs := map[string]string{
		"Content-Type":        "text/plain",
		"X-Amz-Meta-Color":    "blue",
		"X-Amz-Storage-Class": "REDUCED_REDUNDANCY",
		"Cache-Control":       "max-age=60",
	}
	code, _, _ := call(t, http.MethodPut, base+"/m1/obj", hdrs, []byte("payload"))
	if code != http.StatusOK {
		t.Fatalf("put: %d", code)
	}
	code, h, body := call(t, http.MethodGet, base+"/m1/obj", nil, nil)
	if code != http.StatusOK || string(body) != "payload" {
		t.Fatalf("get: %d %s", code, body)
	}
	if got := h.Get("Content-Type"); got != "text/plain" {
		t.Fatalf("content type: %s", got)
	}
	if got := h.Get("X-Amz-Meta-Color"); got != "blue" {
		t.Fatalf("meta: %s", got)
	}
	if got := h.Get("X-Amz-Storage-Class"); got != "REDUCED_REDUNDANCY" {
		t.Fatalf("storage class: %s", got)
	}
	if got := h.Get("Cache-Control"); got != "max-age=60" {
		t.Fatalf("cache control: %s", got)
	}
	if got := h.Get("ETag"); !strings.HasPrefix(got, `"`) {
		t.Fatalf("etag: %s", got)
	}
	call(t, http.MethodPut, base+"/m1/plain", nil, []byte("d"))
	_, h2, _ := call(t, http.MethodHead, base+"/m1/plain", nil, nil)
	if h2.Get("Content-Type") != "binary/octet-stream" {
		t.Fatalf("default content type: %s", h2.Get("Content-Type"))
	}
	code, _, _ = call(t, http.MethodDelete, base+"/m1/obj", nil, nil)
	if code != http.StatusNoContent {
		t.Fatalf("delete: %d", code)
	}
	code, _, body = call(t, http.MethodGet, base+"/m1/obj", nil, nil)
	if code != http.StatusNotFound || !strings.Contains(string(body), "NoSuchKey") {
		t.Fatalf("get after delete: %d %s", code, body)
	}
}

func TestGetObjectRange(t *testing.T) {
	_, base := newServer(t)
	call(t, http.MethodPut, base+"/rg", nil, nil)
	call(t, http.MethodPut, base+"/rg/nums", nil, []byte("0123456789"))
	code, h, body := call(t, http.MethodGet, base+"/rg/nums", map[string]string{"Range": "bytes=2-5"}, nil)
	if code != http.StatusPartialContent || string(body) != "2345" || h.Get("Content-Range") != "bytes 2-5/10" {
		t.Fatalf("range 2-5: %d %s %s", code, body, h.Get("Content-Range"))
	}
	_, _, body = call(t, http.MethodGet, base+"/rg/nums", map[string]string{"Range": "bytes=-3"}, nil)
	if string(body) != "789" {
		t.Fatalf("suffix range: %s", body)
	}
	_, _, body = call(t, http.MethodGet, base+"/rg/nums", map[string]string{"Range": "bytes=7-"}, nil)
	if string(body) != "789" {
		t.Fatalf("open range: %s", body)
	}
	code, _, body = call(t, http.MethodGet, base+"/rg/nums", map[string]string{"Range": "bytes=10-"}, nil)
	if code != http.StatusRequestedRangeNotSatisfiable || !strings.Contains(string(body), "InvalidRange") {
		t.Fatalf("unsatisfiable: %d %s", code, body)
	}
}

func TestConditionalGet(t *testing.T) {
	_, base := newServer(t)
	call(t, http.MethodPut, base+"/cd", nil, nil)
	call(t, http.MethodPut, base+"/cd/k", nil, []byte("d"))
	code, h, _ := call(t, http.MethodGet, base+"/cd/k", nil, nil)
	if code != http.StatusOK {
		t.Fatalf("get: %d", code)
	}
	etag := h.Get("ETag")
	code, _, _ = call(t, http.MethodGet, base+"/cd/k", map[string]string{"If-None-Match": etag}, nil)
	if code != http.StatusNotModified {
		t.Fatalf("if-none-match: %d", code)
	}
	code, _, _ = call(t, http.MethodGet, base+"/cd/k", map[string]string{"If-None-Match": `"beef"`}, nil)
	if code != http.StatusOK {
		t.Fatalf("if-none-match other: %d", code)
	}
	code, _, _ = call(t, http.MethodGet, base+"/cd/k", map[string]string{"If-Match": `"beef"`}, nil)
	if code != http.StatusPreconditionFailed {
		t.Fatalf("if-match: %d", code)
	}
}

type listPage struct {
	KeyCount    int      `xml:"KeyCount"`
	MaxKeys     int      `xml:"MaxKeys"`
	IsTruncated bool     `xml:"IsTruncated"`
	NextToken   string   `xml:"NextContinuationToken"`
	Keys        []string `xml:"Contents>Key"`
	Prefixes    []string `xml:"CommonPrefixes>Prefix"`
}

func TestListObjectsV2Pagination(t *testing.T) {
	_, base := newServer(t)
	call(t, http.MethodPut, base+"/l2", nil, nil)
	for _, k := range []string{"a.txt", "b/1.txt", "b/2.txt", "c/3.txt", "d.txt"} {
		if code, _, _ := call(t, http.MethodPut, base+"/l2/"+k, nil, []byte("x")); code != http.StatusOK {
			t.Fatalf("put %s: %d", k, code)
		}
	}
	code, _, body := call(t, http.MethodGet, base+"/l2?list-type=2&delimiter=/&max-keys=2", nil, nil)
	if code != http.StatusOK {
		t.Fatalf("page1: %d", code)
	}
	var p1 listPage
	if err := xml.Unmarshal(body, &p1); err != nil {
		t.Fatalf("page1 xml: %v", err)
	}
	if len(p1.Keys) != 1 || p1.Keys[0] != "a.txt" || len(p1.Prefixes) != 1 || p1.Prefixes[0] != "b/" {
		t.Fatalf("page1 keys/prefixes: %+v", p1)
	}
	if !p1.IsTruncated || p1.KeyCount != 2 || p1.NextToken == "" {
		t.Fatalf("page1 truncation: %+v", p1)
	}
	page2 := base + "/l2?list-type=2&delimiter=/&max-keys=2&continuation-token=" + url.QueryEscape(p1.NextToken)
	code, _, body = call(t, http.MethodGet, page2, nil, nil)
	if code != http.StatusOK {
		t.Fatalf("page2: %d", code)
	}
	var p2 listPage
	if err := xml.Unmarshal(body, &p2); err != nil {
		t.Fatalf("page2 xml: %v", err)
	}
	if len(p2.Keys) != 1 || p2.Keys[0] != "d.txt" || len(p2.Prefixes) != 1 || p2.Prefixes[0] != "c/" {
		t.Fatalf("page2 keys/prefixes: %+v", p2)
	}
	if p2.IsTruncated || p2.KeyCount != 2 {
		t.Fatalf("page2 truncation: %+v", p2)
	}
}

func TestListObjectsV1MarkerPagination(t *testing.T) {
	_, base := newServer(t)
	call(t, http.MethodPut, base+"/l1", nil, nil)
	for _, k := range []string{"k01", "k02", "k03", "k04", "k05"} {
		call(t, http.MethodPut, base+"/l1/"+k, nil, []byte(k))
	}
	var v1 struct {
		Keys        []string `xml:"Contents>Key"`
		NextMarker  string   `xml:"NextMarker"`
		IsTruncated bool     `xml:"IsTruncated"`
	}
	code, _, body := call(t, http.MethodGet, base+"/l1?max-keys=2", nil, nil)
	if code != http.StatusOK || xml.Unmarshal(body, &v1) != nil {
		t.Fatalf("page1: %d", code)
	}
	if len(v1.Keys) != 2 || v1.Keys[1] != "k02" || v1.NextMarker != "k02" || !v1.IsTruncated {
		t.Fatalf("page1: %+v", v1)
	}
	code, _, body = call(t, http.MethodGet, base+"/l1?max-keys=2&marker=k02", nil, nil)
	if code != http.StatusOK || xml.Unmarshal(body, &v1) != nil {
		t.Fatalf("page2: %d", code)
	}
	if len(v1.Keys) != 2 || v1.Keys[0] != "k03" || v1.Keys[1] != "k04" || v1.NextMarker != "k04" {
		t.Fatalf("page2: %+v", v1)
	}
	code, _, body = call(t, http.MethodGet, base+"/l1?max-keys=2&marker=k04", nil, nil)
	if code != http.StatusOK || xml.Unmarshal(body, &v1) != nil {
		t.Fatalf("page3: %d", code)
	}
	if len(v1.Keys) != 1 || v1.Keys[0] != "k05" || v1.IsTruncated {
		t.Fatalf("page3: %+v", v1)
	}
}

func TestDeleteObjects(t *testing.T) {
	_, base := newServer(t)
	call(t, http.MethodPut, base+"/md", nil, nil)
	for _, k := range []string{"a", "b", "c"} {
		call(t, http.MethodPut, base+"/md/"+k, nil, []byte(k))
	}
	reqBody := `<Delete><Object><Key>a</Key></Object><Object><Key>b</Key></Object></Delete>`
	code, _, body := call(t, http.MethodPost, base+"/md?delete",
		map[string]string{"Content-Type": "application/xml"}, []byte(reqBody))
	if code != http.StatusOK ||
		!strings.Contains(string(body), "<Deleted><Key>a</Key></Deleted>") ||
		!strings.Contains(string(body), "<Key>b</Key>") {
		t.Fatalf("multi-delete: %d %s", code, body)
	}
	quiet := `<Delete><Quiet>true</Quiet><Object><Key>c</Key></Object></Delete>`
	code, _, body = call(t, http.MethodPost, base+"/md?delete", nil, []byte(quiet))
	if code != http.StatusOK || strings.Contains(string(body), "<Deleted>") {
		t.Fatalf("quiet delete: %d %s", code, body)
	}
	code, _, body = call(t, http.MethodPost, base+"/md?delete", nil, nil)
	if code != http.StatusBadRequest || !strings.Contains(string(body), "MalformedXML") {
		t.Fatalf("empty delete body: %d %s", code, body)
	}
}

func TestCopyObject(t *testing.T) {
	_, base := newServer(t)
	call(t, http.MethodPut, base+"/cp", nil, nil)
	call(t, http.MethodPut, base+"/cp/src",
		map[string]string{"Content-Type": "text/csv", "X-Amz-Meta-Via": "origin"},
		[]byte("csv-data"))
	code, h, _ := call(t, http.MethodGet, base+"/cp/src", nil, nil)
	srcETag := h.Get("ETag")
	if code != http.StatusOK || srcETag == "" {
		t.Fatalf("src: %d", code)
	}
	code, _, body := call(t, http.MethodPut, base+"/cp/dst",
		map[string]string{"x-amz-copy-source": "/cp/src"}, nil)
	if code != http.StatusOK || !strings.Contains(string(body), "<CopyObjectResult") {
		t.Fatalf("copy: %d %s", code, body)
	}
	var res struct {
		ETag string `xml:"ETag"`
	}
	if err := xml.Unmarshal(body, &res); err != nil || res.ETag != srcETag {
		t.Fatalf("copy result: %v %s", err, res.ETag)
	}
	code, h, body = call(t, http.MethodGet, base+"/cp/dst", nil, nil)
	if code != http.StatusOK || string(body) != "csv-data" ||
		h.Get("Content-Type") != "text/csv" || h.Get("X-Amz-Meta-Via") != "origin" {
		t.Fatalf("copied object: %d %s %s", code, body, h.Get("Content-Type"))
	}
	code, _, _ = call(t, http.MethodPut, base+"/cp/dst2",
		map[string]string{"x-amz-copy-source": "/cp/src", "x-amz-metadata-directive": "REPLACE",
			"Content-Type": "application/json"}, nil)
	if code != http.StatusOK {
		t.Fatalf("replace copy: %d", code)
	}
	_, h2, _ := call(t, http.MethodGet, base+"/cp/dst2", nil, nil)
	if h2.Get("Content-Type") != "application/json" || h2.Get("X-Amz-Meta-Via") != "" {
		t.Fatalf("replace directive: %s %s", h2.Get("Content-Type"), h2.Get("X-Amz-Meta-Via"))
	}
	code, _, body = call(t, http.MethodPut, base+"/cp/src",
		map[string]string{"x-amz-copy-source": "cp/src"}, nil)
	if code != http.StatusBadRequest || !strings.Contains(string(body), "InvalidRequest") {
		t.Fatalf("self copy: %d %s", code, body)
	}
}

func TestMultipartLifecycle(t *testing.T) {
	_, base := newServer(t)
	call(t, http.MethodPut, base+"/mp", nil, nil)
	code, _, body := call(t, http.MethodPost, base+"/mp/big?uploads", nil, nil)
	if code != http.StatusOK {
		t.Fatalf("init: %d %s", code, body)
	}
	var initDoc struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal(body, &initDoc); err != nil || initDoc.UploadID == "" {
		t.Fatalf("init xml: %v %s", err, body)
	}
	q1 := url.Values{"partNumber": {"1"}, "uploadId": {initDoc.UploadID}}
	code, h1, _ := call(t, http.MethodPut, base+"/mp/big?"+q1.Encode(), nil, []byte("AAAA"))
	if code != http.StatusOK {
		t.Fatalf("part1: %d", code)
	}
	etag1 := h1.Get("ETag")
	q2 := url.Values{"partNumber": {"2"}, "uploadId": {initDoc.UploadID}}
	code, h2, _ := call(t, http.MethodPut, base+"/mp/big?"+q2.Encode(), nil, []byte("BBBBBBBBBB"))
	if code != http.StatusOK {
		t.Fatalf("part2: %d", code)
	}
	etag2 := h2.Get("ETag")

	code, _, body = call(t, http.MethodGet, base+"/mp/big?uploadId="+initDoc.UploadID, nil, nil)
	if code != http.StatusOK {
		t.Fatalf("list parts: %d", code)
	}
	var partsDoc struct {
		Part []struct {
			Number int    `xml:"PartNumber"`
			Size   int64  `xml:"Size"`
			ETag   string `xml:"ETag"`
		} `xml:"Part"`
		IsTruncated bool `xml:"IsTruncated"`
	}
	if err := xml.Unmarshal(body, &partsDoc); err != nil {
		t.Fatalf("list parts xml: %v", err)
	}
	if len(partsDoc.Part) != 2 || partsDoc.Part[0].Number != 1 || partsDoc.Part[0].Size != 4 ||
		partsDoc.Part[1].Number != 2 || partsDoc.Part[1].Size != 10 || partsDoc.IsTruncated {
		t.Fatalf("parts: %+v", partsDoc)
	}

	badBody := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"deadbeef"</ETag></Part></CompleteMultipartUpload>`
	code, _, body = call(t, http.MethodPost, base+"/mp/big?uploadId="+initDoc.UploadID,
		map[string]string{"Content-Type": "application/xml"}, []byte(badBody))
	if code != http.StatusBadRequest || !strings.Contains(string(body), "InvalidPart") {
		t.Fatalf("bad complete: %d %s", code, body)
	}

	goodBody := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + etag1 +
		`</ETag></Part><Part><PartNumber>2</PartNumber><ETag>` + etag2 + `</ETag></Part></CompleteMultipartUpload>`
	code, _, body = call(t, http.MethodPost, base+"/mp/big?uploadId="+initDoc.UploadID,
		map[string]string{"Content-Type": "application/xml"}, []byte(goodBody))
	if code != http.StatusOK {
		t.Fatalf("complete: %d %s", code, body)
	}
	var comp struct {
		ETag string `xml:"ETag"`
	}
	if err := xml.Unmarshal(body, &comp); err != nil || !strings.HasSuffix(comp.ETag, `-2"`) {
		t.Fatalf("complete xml: %v %s", err, comp.ETag)
	}
	fcode, fh, fbody := call(t, http.MethodGet, base+"/mp/big", nil, nil)
	if fcode != http.StatusOK || string(fbody) != "AAAABBBBBBBBBB" || fh.Get("ETag") != comp.ETag {
		t.Fatalf("assembled object: %d %s %s", fcode, fbody, fh.Get("ETag"))
	}
	hcode, hh, _ := call(t, http.MethodHead, base+"/mp/big", nil, nil)
	if hcode != http.StatusOK || hh.Get("Content-Length") != "14" {
		t.Fatalf("head: %d %s", hcode, hh.Get("Content-Length"))
	}
}

func TestMultipartAbortAndListUploads(t *testing.T) {
	_, base := newServer(t)
	call(t, http.MethodPut, base+"/au", nil, nil)
	var ids []string
	for _, key := range []string{"a", "b"} {
		code, _, body := call(t, http.MethodPost, base+"/au/"+key+"?uploads", nil, nil)
		if code != http.StatusOK {
			t.Fatalf("init %s: %d", key, code)
		}
		var doc struct {
			UploadID string `xml:"UploadId"`
		}
		if err := xml.Unmarshal(body, &doc); err != nil || doc.UploadID == "" {
			t.Fatalf("init xml %s: %v %s", key, err, body)
		}
		ids = append(ids, doc.UploadID)
	}
	type uploadsDoc struct {
		Upload []struct {
			Key      string `xml:"Key"`
			UploadID string `xml:"UploadId"`
		} `xml:"Upload"`
	}
	code, _, body := call(t, http.MethodGet, base+"/au?uploads", nil, nil)
	if code != http.StatusOK {
		t.Fatalf("list uploads: %d", code)
	}
	var before uploadsDoc
	if err := xml.Unmarshal(body, &before); err != nil || len(before.Upload) != 2 {
		t.Fatalf("uploads before aborts: %v %s", err, body)
	}
	code, _, _ = call(t, http.MethodDelete, base+"/au/a?uploadId="+ids[0], nil, nil)
	if code != http.StatusNoContent {
		t.Fatalf("abort a: %d", code)
	}
	code, _, _ = call(t, http.MethodDelete, base+"/au/a?uploadId="+ids[0], nil, nil)
	if code != http.StatusNotFound {
		t.Fatalf("re-abort a: %d", code)
	}
	code, _, _ = call(t, http.MethodDelete, base+"/au/b?uploadId="+ids[1], nil, nil)
	if code != http.StatusNoContent {
		t.Fatalf("abort b: %d", code)
	}
	code, _, _ = call(t, http.MethodPost, base+"/au/b?uploadId="+ids[1], nil,
		[]byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"x"</ETag></Part></CompleteMultipartUpload>`))
	if code != http.StatusNotFound {
		t.Fatalf("complete after abort: %d", code)
	}
	code, _, body = call(t, http.MethodGet, base+"/au?uploads", nil, nil)
	if code != http.StatusOK {
		t.Fatalf("uploads after aborts: %d", code)
	}
	var after uploadsDoc
	if err := xml.Unmarshal(body, &after); err != nil || len(after.Upload) != 0 {
		t.Fatalf("uploads after aborts: %v %s", err, body)
	}
}

// presignV4 signs rawurl the same way the AWS SDKs presign SigV4 requests.
func presignV4(t *testing.T, method, rawurl, ak, sk string, issued time.Time, expires int, tamper bool) string {
	t.Helper()
	u, err := url.Parse(rawurl)
	if err != nil {
		t.Fatalf("parse %s: %v", rawurl, err)
	}
	amzDate := issued.UTC().Format("20060102T150405Z")
	scopeDate := issued.UTC().Format("20060102")
	cred := ak + "/" + scopeDate + "/us-east-1/s3/aws4_request"
	q := u.Query()
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", cred)
	q.Set("X-Amz-Date", amzDate)
	q.Set("X-Amz-Expires", strconv.Itoa(expires))
	q.Set("X-Amz-SignedHeaders", "host")
	canonicalRequest := method + "\n" + u.EscapedPath() + "\n" + canonicalQueryV4(q) +
		"\nhost:" + u.Host + "\n\nhost\nUNSIGNED-PAYLOAD"
	sts := strings.Join([]string{"AWS4-HMAC-SHA256", amzDate,
		scopeDate + "/us-east-1/s3/aws4_request", sha256Hex(canonicalRequest)}, "\n")
	sig := hex.EncodeToString(hmacSHA256(signingKey(sk, scopeDate, "us-east-1", "s3"), []byte(sts)))
	if tamper {
		sig = "0" + sig[1:]
	}
	q.Set("X-Amz-Signature", sig)
	u.RawQuery = q.Encode()
	return u.String()
}

func TestPresignedV4GetAndPut(t *testing.T) {
	_, base := newServer(t)
	call(t, http.MethodPut, base+"/ps", nil, nil)
	call(t, http.MethodPut, base+"/ps/data", nil, []byte("presigned"))
	u := presignV4(t, http.MethodGet, base+"/ps/data", "test", "test", time.Now(), 300, false)
	code, _, body := call(t, http.MethodGet, u, nil, nil)
	if code != http.StatusOK || string(body) != "presigned" {
		t.Fatalf("presigned GET: %d %s", code, body)
	}
	bad := presignV4(t, http.MethodGet, base+"/ps/data", "test", "test", time.Now(), 300, true)
	code, _, body = call(t, http.MethodGet, bad, nil, nil)
	if code != http.StatusForbidden || !strings.Contains(string(body), "SignatureDoesNotMatch") {
		t.Fatalf("tampered: %d %s", code, body)
	}
	expired := presignV4(t, http.MethodGet, base+"/ps/data", "test", "test", time.Now().Add(-2*time.Hour), 60, false)
	code, _, body = call(t, http.MethodGet, expired, nil, nil)
	if code != http.StatusForbidden || !strings.Contains(string(body), "Request has expired") {
		t.Fatalf("expired: %d %s", code, body)
	}
	// Unknown access keys are trusted, like the rest of the emulator.
	trusted := presignV4(t, http.MethodGet, base+"/ps/data", "other", "whatever", time.Now(), 300, false)
	code, _, _ = call(t, http.MethodGet, trusted, nil, nil)
	if code != http.StatusOK {
		t.Fatalf("trusted presign: %d", code)
	}
	pu := presignV4(t, http.MethodPut, base+"/ps/uploaded", "test", "test", time.Now(), 300, false)
	code, _, _ = call(t, http.MethodPut, pu, nil, []byte("via-presign"))
	if code != http.StatusOK {
		t.Fatalf("presigned PUT: %d", code)
	}
	code, _, body = call(t, http.MethodGet, base+"/ps/uploaded", nil, nil)
	if code != http.StatusOK || string(body) != "via-presign" {
		t.Fatalf("presigned upload readback: %d %s", code, body)
	}
}

func TestStreamingChunkedPut(t *testing.T) {
	_, base := newServer(t)
	call(t, http.MethodPut, base+"/sk", nil, nil)
	framed := "8;chunk-signature=0102\r\n12345678\r\n0;chunk-signature=0304\r\n"
	hdrs := map[string]string{
		"X-Amz-Content-Sha256":         "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
		"X-Amz-Decoded-Content-Length": "8",
	}
	code, _, _ := call(t, http.MethodPut, base+"/sk/chunked", hdrs, []byte(framed))
	if code != http.StatusOK {
		t.Fatalf("streaming put: %d", code)
	}
	code, _, body := call(t, http.MethodGet, base+"/sk/chunked", nil, nil)
	if code != http.StatusOK || string(body) != "12345678" {
		t.Fatalf("chunked readback: %d %s", code, body)
	}
}

func TestBucketSubresources(t *testing.T) {
	_, base := newServer(t)
	call(t, http.MethodPut, base+"/sub", nil, nil)
	code, _, body := call(t, http.MethodGet, base+"/sub?location", nil, nil)
	if code != http.StatusOK || !strings.Contains(string(body), "LocationConstraint") {
		t.Fatalf("location: %d %s", code, body)
	}
	code, _, _ = call(t, http.MethodPut, base+"/sub?versioning", nil,
		[]byte("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"))
	if code != http.StatusOK {
		t.Fatalf("put versioning: %d", code)
	}
	code, _, body = call(t, http.MethodGet, base+"/sub?versioning", nil, nil)
	if code != http.StatusOK || !strings.Contains(string(body), "<Status>Enabled</Status>") {
		t.Fatalf("get versioning: %d %s", code, body)
	}
	code, _, body = call(t, http.MethodGet, base+"/sub?cors", nil, nil)
	if code != http.StatusNotFound || !strings.Contains(string(body), "NoSuchCORSConfiguration") {
		t.Fatalf("cors: %d %s", code, body)
	}
	code, _, body = call(t, http.MethodGet, base+"/sub?acl", nil, nil)
	if code != http.StatusOK || !strings.Contains(string(body), "FULL_CONTROL") {
		t.Fatalf("acl: %d %s", code, body)
	}
	code, _, body = call(t, http.MethodGet, base+"/absent", nil, nil)
	if code != http.StatusNotFound || !strings.Contains(string(body), "NoSuchBucket") {
		t.Fatalf("absent bucket: %d %s", code, body)
	}
}

func TestObjectTagging(t *testing.T) {
	_, base := newServer(t)
	call(t, http.MethodPut, base+"/tg", nil, nil)
	code, _, _ := call(t, http.MethodPut, base+"/tg/k",
		map[string]string{"x-amz-tagging": "env=dev&team=core"}, []byte("x"))
	if code != http.StatusOK {
		t.Fatalf("put: %d", code)
	}
	code, _, body := call(t, http.MethodGet, base+"/tg/k?tagging", nil, nil)
	if code != http.StatusOK ||
		!strings.Contains(string(body), "<Key>env</Key><Value>dev</Value>") ||
		!strings.Contains(string(body), "<Key>team</Key><Value>core</Value>") {
		t.Fatalf("get tagging: %d %s", code, body)
	}
	code, _, _ = call(t, http.MethodPut, base+"/tg/k?tagging", nil,
		[]byte(`<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>`))
	if code != http.StatusOK {
		t.Fatalf("put tagging: %d", code)
	}
	code, _, body = call(t, http.MethodGet, base+"/tg/k?tagging", nil, nil)
	if code != http.StatusOK || !strings.Contains(string(body), "<Value>prod</Value>") {
		t.Fatalf("replaced tagging: %d %s", code, body)
	}
	code, _, _ = call(t, http.MethodDelete, base+"/tg/k?tagging", nil, nil)
	if code != http.StatusNoContent {
		t.Fatalf("delete tagging: %d", code)
	}
	code, _, body = call(t, http.MethodGet, base+"/tg/k?tagging", nil, nil)
	if code != http.StatusNotFound || !strings.Contains(string(body), "NoSuchTagSet") {
		t.Fatalf("missing tagging: %d %s", code, body)
	}
}

func TestListObjectsEncodingTypeURL(t *testing.T) {
	_, base := newServer(t)
	call(t, http.MethodPut, base+"/eu", nil, nil)
	code, _, _ := call(t, http.MethodPut, base+"/eu/sp%20ace", nil, []byte("x"))
	if code != http.StatusOK {
		t.Fatalf("put: %d", code)
	}
	code, _, body := call(t, http.MethodGet, base+"/eu?encoding-type=url", nil, nil)
	if code != http.StatusOK ||
		!strings.Contains(string(body), "<EncodingType>url</EncodingType>") ||
		!strings.Contains(string(body), "sp%20ace") {
		t.Fatalf("encoded list: %d %s", code, body)
	}
}

func TestErrorXMLShape(t *testing.T) {
	_, base := newServer(t)
	call(t, http.MethodPut, base+"/e1", nil, nil)
	code, h, body := call(t, http.MethodGet, base+"/e1/missing", nil, nil)
	if code != http.StatusNotFound {
		t.Fatalf("status: %d", code)
	}
	s := string(body)
	if !strings.HasPrefix(s, "<?xml") ||
		!strings.Contains(s, "<Code>NoSuchKey</Code>") ||
		!strings.Contains(s, "<RequestId>") {
		t.Fatalf("error xml: %s", s)
	}
	if h.Get("Content-Type") != "application/xml" {
		t.Fatalf("content type: %s", h.Get("Content-Type"))
	}
}
