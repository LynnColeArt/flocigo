package s3

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPutBucketNameValidity(t *testing.T) {
	valid := []string{
		"abc",
		"abc-123",
		"my.test-bucket",
		"0-0",
		strings.Repeat("a", 63),
	}
	for _, name := range valid {
		if !createBucketNameValid(name) {
			t.Errorf("createBucketNameValid(%q) = false, want true", name)
		}
	}
	invalid := []string{
		"",
		"ab",
		"-abc",
		"abc-",
		"a..b",
		"a_b",
		"Abc",
		"192.168.1.1",
		"xn--abc",
		"aaa-s3alias",
		strings.Repeat("a", 64),
		"a/b",
		"a b",
	}
	for _, name := range invalid {
		if createBucketNameValid(name) {
			t.Errorf("createBucketNameValid(%q) = true, want false", name)
		}
	}
}

func TestPutBucketCreatesOnDiskAndReturnsLocation(t *testing.T) {
	root := t.TempDir()
	srv := CreateBucketHandler(root)

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/fresh-bucket", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "/fresh-bucket" {
		t.Errorf("Location = %q, want /fresh-bucket", loc)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want empty", rec.Body.String())
	}
	if info, err := os.Stat(filepath.Join(root, "fresh-bucket")); err != nil || !info.IsDir() {
		t.Fatalf("bucket directory missing under root: %v", err)
	}
}

func TestPutBucketIdempotentWhenAlreadyOwned(t *testing.T) {
	root := t.TempDir()
	srv := CreateBucketHandler(root)

	for i := 1; i <= 2; i++ {
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/own-bucket", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("PUT #%d status = %d, want 200 (idempotent BucketAlreadyOwnedByYou)", i, rec.Code)
		}
		if loc := rec.Header().Get("Location"); loc != "/own-bucket" {
			t.Fatalf("PUT #%d Location = %q, want /own-bucket", i, loc)
		}
	}
}

func TestPutBucketRejectsInvalidNames(t *testing.T) {
	root := t.TempDir()
	srv := CreateBucketHandler(root)

	for _, name := range []string{"ab", "UPPER", "bad_name", "a..b", "1.2.3.4"} {
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/"+name, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("PUT %q status = %d, want 400", name, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "<Code>InvalidBucketName</Code>") {
			t.Fatalf("PUT %q body = %q, want InvalidBucketName document", name, rec.Body.String())
		}
	}

	if _, err := os.Stat(filepath.Join(root, "UPPER")); !os.IsNotExist(err) {
		t.Fatalf("invalid bucket name must not be created on disk")
	}
}
