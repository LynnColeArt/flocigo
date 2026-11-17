package s3

import (
	"encoding/hex"
	"crypto/rand"
	"net/http"
	"strings"

	"github.com/LynnColeArt/flocigo/internal/errmap"
)

// requestID returns a short request identifier the way S3 headers carry it.
func requestID() string { b := make([]byte, 8); rand.Read(b); return hex.EncodeToString(b) }

func (s *Store) Router() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := requestID()
		fail := func(e error) {
			errmap.S3(e, reqID, reqID).Write(w)
		}
		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
		bucket, key := parts[0], ""
		if len(parts) > 1 {
			key = parts[1]
		}
		if key == "" {
			switch r.Method {
			case "PUT":
				if e := s.CreateBucket(bucket); e != nil {
					fail(errmap.New("InvalidBucketName",""))
					return
				}
				w.Header().Set("Location", "/"+bucket)
				w.WriteHeader(200)
			case "DELETE":
				if e := s.DeleteBucket(bucket); e != nil {
					if e.Error() == "BucketNotEmpty" {
						fail(errmap.New("BucketNotEmpty",""))
						return
					}
					fail(errmap.New("NoSuchBucket",""))
					return
				}
				w.WriteHeader(204)
			case "GET":
				objs := s.ListObjects(bucket, "")
				w.Header().Set("Content-Type", "application/xml")
				w.Write([]byte("<ListBucketResult>" + itoa(len(objs)) + "</ListBucketResult>"))
			case "HEAD":
				if !s.BucketExists(bucket) {
					fail(errmap.New("NoSuchBucket",""))
					return
				}
				w.WriteHeader(200)
			default:
				fail(errmap.New("NotImplemented",""))
			}
			return
		}
		switch r.Method {
		case "PUT":
			body := make([]byte, 4096)
			n, _ := r.Body.Read(body)
			etag, e := s.PutObject(bucket, key, body[:n])
			if e != nil { fail(errmap.New("NoSuchBucket","")); return }
			w.Header().Set("ETag", etag)
			w.WriteHeader(200)
		case "GET":
			body, etag, e := s.GetObject(bucket, key)
			if e != nil { fail(errmap.New("NoSuchKey","")); return }
			w.Header().Set("ETag", etag)
			w.Write(body)
		case "HEAD":
			_, etag, e := s.GetObject(bucket, key)
			if e != nil { fail(errmap.New("NoSuchKey","")); return }
			w.Header().Set("ETag", etag)
		case "DELETE":
			s.DeleteObject(bucket, key)
			w.WriteHeader(204)
		default:
			fail(errmap.New("NotImplemented",""))
		}
	})
}

func itoa(n int) string { if n == 0 { return "0" }; s := ""; for n > 0 { s = string(rune('0'+n%10)) + s; n /= 10 }; return s }
