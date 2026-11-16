package sigv4

import (
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestDebugCanonical(t *testing.T) {
	req := httptest.NewRequest("PUT", "http://127.0.0.1:9000/bucket", nil)
	req.Host = "127.0.0.1:9000"
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	if e := Sign(req, "k", "s", testRegion, "s3", now); e != nil {
		t.Fatal(e)
	}
	auth := req.Header.Get("Authorization")
	i := strings.Index(auth, "SignedHeaders=")
	j := strings.Index(auth, "Signature=")
	names := auth[i+len("SignedHeaders=") : j-2]
	t.Log("signed names:", names)
	ctx, cq := canonicalURI(req), canonicalQuery(req)
	t.Log("uri:", ctx, "query:", cq, "payload:", hashHex(nil))
	var lines []string
	for _, n := range strings.Split(names, ";") {
		var v string
		switch n {
		case "host":
			v = strings.ToLower(req.Host)
		default:
			vals := req.Header.Values(http.CanonicalHeaderKey(n))
			sort.Strings(vals)
			v = strings.ToLower(strings.Join(vals, ","))
		}
		lines = append(lines, n+":"+v)
	}
	t.Log("verify lines:", strings.Join(lines, "\n"))
}
