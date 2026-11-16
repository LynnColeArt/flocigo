package s3

import (
	"net/http/httptest"
	"testing"
)

func TestS3Kernel(t *testing.T) {
	store := NewStore(t.TempDir())
	srv := httptest.NewServer(store.Router())
	defer srv.Close()

	// CreateBucket
	store.CreateBucket("test-bucket")
	if !store.BucketExists("test-bucket") {
		t.Fatal("create bucket")
	}
	// PutObject
	etag, e := store.PutObject("test-bucket", "dir/file.txt", []byte("hello world"))
	if e != nil || etag == "" {
		t.Fatal("put object", e)
	}
	// GetObject
	body, gotETag, e := store.GetObject("test-bucket", "dir/file.txt")
	if e != nil || string(body) != "hello world" || gotETag != etag {
		t.Fatal("get object", e)
	}
	// ListObjects
	objs := store.ListObjects("test-bucket", "")
	if len(objs) != 1 || objs[0].Key != "dir/file.txt" {
		t.Fatal("list objects", len(objs))
	}
	// DeleteObject
	if e := store.DeleteObject("test-bucket", "dir/file.txt"); e != nil {
		t.Fatal("delete object", e)
	}
	if _, _, e := store.GetObject("test-bucket", "dir/file.txt"); e == nil {
		t.Fatal("object should be gone")
	}
	// Router smoke

}
