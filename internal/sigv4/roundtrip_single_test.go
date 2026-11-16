package sigv4

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestSignVerifyIsolated(t *testing.T) {
	req := httptest.NewRequest("PUT", "http://127.0.0.1:9000/bucket", nil)
	req.Host = "127.0.0.1:9000"
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	e1 := Sign(req, "key", "secret", "us-east-1", "s3", now)
	if e1 != nil {
		t.Fatal("Sign:", e1)
	}
	auth := req.Header.Get("Authorization")
	t.Log("auth:", auth)
	payload := req.Header.Get("X-Amz-Content-Sha256")
	t.Log("payload:", payload[:16])
	h, n := canonicalize(req, payload)
	t.Log("sign canon headers:", h)
	t.Log("sign canon names:", n)
	t.Log("sign canonical:", canonicalURI(req), "+", canonicalQuery(req), "+", payload[:12])
	e2 := Verify(req, "key", "secret", "us-east-1", "s3")
	if e2 != nil {
		t.Fatal("Verify:", e2)
	}
	t.Log("ROUNDTRIP OK")
}
