package s3

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	"github.com/LynnColeArt/flocigo/internal/errmap"
)

func (s *Store) Router() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
		bucket, key := parts[0], ""
		if len(parts) > 1 {
			key = parts[1]
		}
		if key == "" {
			switch r.Method {
			case "PUT":
				if e := s.CreateBucket(bucket); e != nil {
					errmap.S3XML(w, errmap.InvalidBucket)
					return
				}
				w.Header().Set("Location", "/"+bucket)
				w.WriteHeader(200)
			case "DELETE":
				if e := s.DeleteBucket(bucket); e != nil {
					if e.Error() == "BucketNotEmpty" {
						errmap.S3XML(w, errmap.BucketNotEmpty)
						return
					}
					errmap.S3XML(w, errmap.NoSuchBucket)
					return
				}
				w.WriteHeader(204)
			case "GET":
				objs := s.ListObjects(bucket, r.URL.Query().Get("prefix"))
				w.Header().Set("Content-Type", "application/xml")
				w.Write([]byte("<ListBucketResult>" + fmt.Sprint(len(objs)) + "</ListBucketResult>"))
			case "HEAD":
				if !s.BucketExists(bucket) {
					errmap.S3XML(w, errmap.NoSuchBucket)
					return
				}
				w.WriteHeader(200)
			default:
				errmap.S3XML(w, errmap.NotImplemented(r.Method))
			}
			return
		}
		switch r.Method {
		case "PUT":
			body := make([]byte, 4096)
			n, _ := r.Body.Read(body)
			etag, e := s.PutObject(bucket, key, body[:n])
			if e != nil {
				errmap.S3XML(w, errmap.NotImplemented(e.Error()))
				return
			}
			w.Header().Set("ETag", etag)
			w.WriteHeader(200)
		case "GET":
			body, etag, e := s.GetObject(bucket, key)
			if e != nil {
				errmap.S3XML(w, errmap.NoSuchKey)
				return
			}
			w.Header().Set("ETag", etag)
			w.Write(body)
		case "HEAD":
			_, etag, e := s.GetObject(bucket, key)
			if e != nil {
				errmap.S3XML(w, errmap.NoSuchKey)
				return
			}
			w.Header().Set("ETag", etag)
		case "DELETE":
			s.DeleteObject(bucket, key)
			w.WriteHeader(204)
		default:
			errmap.S3XML(w, errmap.NotImplemented(r.Method))
		}
	})
}

func toXML(s string) string { return xmlEscape(s) }

func xmlEscape(s string) string {
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">&", "&gt;")
	return s
}

var _ = xml.Marshal
var _ = fmt.Sprint
