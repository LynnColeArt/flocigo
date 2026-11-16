package sigv4

import (
	"bytes"
	"errors"
	"net/http/httptest"
	"testing"
	"time"
)

const testRegion = "us-east-1"

func TestSignVerifyRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		method, path, body string
	}{
		{"PUT", "/bucket", ""},
		{"PUT", "/bucket/with_key/and%2Bplus", "hello world payload"},
		{"GET", "/bucket/obj.bin", ""},
		{"DELETE", "/bucket/obj", ""},
	} {
		req := httptest.NewRequest(tc.method, "http://127.0.0.1:9000"+tc.path, bytes.NewReader([]byte(tc.body)))
		req.Host = "127.0.0.1:9000"
		req.Header.Set("X-Amz-Target", "s3.PutObject")
		now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
		if e := Sign(req, "k", "s", testRegion, "s3", now); e != nil {
			t.Fatal(e)
		}
		if e := Verify(req, "k", "s", testRegion, "s3"); e != nil {
			t.Fatalf("%s %s: %v", tc.method, tc.path, e)
		}
	}
}

func TestTamperRejected(t *testing.T) {
	req := httptest.NewRequest("PUT", "http://127.0.0.1:9000/b/k", bytes.NewReader([]byte("original")))
	req.Host = "127.0.0.1:9000"
	if e := Sign(req, "k", "s", testRegion, "s3", time.Now().UTC()); e != nil {
		t.Fatal(e)
	}
	// tamper the body after signing: signature must fail
	req.Body = httptest.NewRequest("PUT", "http://x", bytes.NewReader([]byte("tampered"))).Body
	req.Header.Set("X-Amz-Content-Sha256", hashHex([]byte("tampered")))
	if e := Verify(req, "k", "s", testRegion, "s3"); !errors.Is(e, ErrSignature) {
		t.Fatalf("want ErrSignature, got %v", e)
	}
	// tamper the auth header: wrong key
	req2 := httptest.NewRequest("GET", "http://127.0.0.1:9000/b", nil)
	req2.Host = "127.0.0.1:9000"
	if e := Sign(req2, "k", "s", testRegion, "s3", time.Now().UTC()); e != nil {
		t.Fatal(e)
	}
	if e := Verify(req2, "wrong", "s", testRegion, "s3"); !errors.Is(e, ErrScope) {
		t.Fatalf("want ErrScope, got %v", e)
	}
}

func TestMissingAuth(t *testing.T) {
	req := httptest.NewRequest("GET", "http://127.0.0.1:9000/b", nil)
	req.Host = "127.0.0.1:9000"
	if e := Verify(req, "k", "s", testRegion, "s3"); !errors.Is(e, ErrMissingAuth) {
		t.Fatalf("got %v", e)
	}
}
