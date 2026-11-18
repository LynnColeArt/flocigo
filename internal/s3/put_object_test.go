package s3

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func ticket007MD5Hex(t *testing.T, s string) string {
	t.Helper()
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

// ticket007Root points the on-disk object store at a fresh temp dir.
func ticket007Root(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	t.Setenv("FLOCI_S3_DATA_DIR", root)
	return root
}

func TestTicket007PutObjectPersistsBodyWithMD5ETag(t *testing.T) {
	root := ticket007Root(t)
	body := "hello floci-go"

	etag, err := PutObjectToDisk(root, "books", "chapters/01.txt", strings.NewReader(body))
	if err != nil {
		t.Fatalf("PutObjectToDisk: %v", err)
	}
	if want := ticket007MD5Hex(t, body); etag != want {
		t.Fatalf("etag = %q, want %q", etag, want)
	}
	data, err := os.ReadFile(filepath.Join(root, "books", "chapters", "01.txt"))
	if err != nil {
		t.Fatalf("object not written to disk: %v", err)
	}
	if string(data) != body {
		t.Fatalf("stored body = %q, want %q", data, body)
	}
}

func TestTicket007PutObjectOverwritesExistingKey(t *testing.T) {
	root := ticket007Root(t)
	first, second := "first body", "second body"

	if _, err := PutObjectToDisk(root, "bkt", "k", strings.NewReader(first)); err != nil {
		t.Fatalf("first put: %v", err)
	}
	etag, err := PutObjectToDisk(root, "bkt", "k", strings.NewReader(second))
	if err != nil {
		t.Fatalf("second put: %v", err)
	}
	if want := ticket007MD5Hex(t, second); etag != want {
		t.Fatalf("etag = %q, want %q", etag, want)
	}
	data, err := os.ReadFile(filepath.Join(root, "bkt", "k"))
	if err != nil || string(data) != second {
		t.Fatalf("stored body = %q (%v), want %q", data, err, second)
	}
	entries, err := os.ReadDir(filepath.Join(root, "bkt"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("bucket dir entries = %d (%v), want 1", len(entries), err)
	}
}

func TestTicket007HandlePutObjectReturnsQuotedETagHeader(t *testing.T) {
	root := ticket007Root(t)
	body := "the payload"

	req := httptest.NewRequest(http.MethodPut, "/bucket-a/nested/key.txt", strings.NewReader(body))
	rec := httptest.NewRecorder()
	HandlePutObject(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	wantETag := `"` + ticket007MD5Hex(t, body) + `"`
	if got := rec.Header().Get("ETag"); got != wantETag {
		t.Fatalf("ETag header = %q, want %q", got, wantETag)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("response body = %q, want empty", rec.Body.String())
	}
	data, err := os.ReadFile(filepath.Join(root, "bucket-a", "nested", "key.txt"))
	if err != nil || string(data) != body {
		t.Fatalf("object not persisted correctly: %v %q", err, data)
	}
}

func TestTicket007HandlePutObjectRejectsBadDigest(t *testing.T) {
	root := ticket007Root(t)
	wrongMD5 := md5.Sum([]byte("something else"))

	req := httptest.NewRequest(http.MethodPut, "/bkt/protected.txt", strings.NewReader("actual body"))
	req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(wrongMD5[:]))
	rec := httptest.NewRecorder()
	HandlePutObject(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "<Code>BadDigest</Code>") {
		t.Fatalf("body = %q, want BadDigest error", rec.Body.String())
	}
	if _, err := os.Stat(filepath.Join(root, "bkt", "protected.txt")); !os.IsNotExist(err) {
		t.Fatalf("failed upload should not persist an object (stat err = %v)", err)
	}
}

func TestTicket007HandlePutObjectRejectsMalformedDigest(t *testing.T) {
	root := ticket007Root(t)

	req := httptest.NewRequest(http.MethodPut, "/bkt/x.bin", strings.NewReader("body"))
	req.Header.Set("Content-MD5", "not-base64!!")
	rec := httptest.NewRecorder()
	HandlePutObject(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "<Code>InvalidDigest</Code>") {
		t.Fatalf("body = %q, want InvalidDigest error", rec.Body.String())
	}
	if _, err := os.Stat(filepath.Join(root, "bkt", "x.bin")); !os.IsNotExist(err) {
		t.Fatalf("failed upload should not persist an object (stat err = %v)", err)
	}
}

func TestTicket007HandlePutObjectDecodesEscapedKeys(t *testing.T) {
	root := ticket007Root(t)
	body := "spacey"

	req := httptest.NewRequest(http.MethodPut, "/bkt/na%C3%AFve%20name.txt", strings.NewReader(body))
	rec := httptest.NewRecorder()
	HandlePutObject(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	data, err := os.ReadFile(filepath.Join(root, "bkt", "naïve name.txt"))
	if err != nil || string(data) != body {
		t.Fatalf("escaped key not stored under decoded name: %v %q", err, data)
	}
}

func TestTicket007HandlePutObjectCopySourceCopiesFromDisk(t *testing.T) {
	root := ticket007Root(t)
	body := "copied payload"

	if _, err := PutObjectToDisk(root, "src-bkt", "orig/key.dat", strings.NewReader(body)); err != nil {
		t.Fatalf("seed source object: %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/dst-bkt/copied/key.dat", nil)
	req.Header.Set("x-amz-copy-source", "src-bkt/orig/key.dat")
	rec := httptest.NewRecorder()
	HandlePutObject(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "xml") {
		t.Fatalf("Content-Type = %q, want xml", ct)
	}
	wantETag := `"` + ticket007MD5Hex(t, body) + `"`
	if !strings.Contains(rec.Body.String(), "<CopyObjectResult") || !strings.Contains(rec.Body.String(), wantETag) {
		t.Fatalf("body = %q, want CopyObjectResult with ETag %s", rec.Body.String(), wantETag)
	}
	data, err := os.ReadFile(filepath.Join(root, "dst-bkt", "copied", "key.dat"))
	if err != nil || string(data) != body {
		t.Fatalf("copy target not persisted: %v %q", err, data)
	}
}
