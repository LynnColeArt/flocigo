package s3

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Minimal stand-in store; it exercises the reflective bucket lookup and the
// field fallbacks (no StorageClass field, unquoted ETag, empty payload).
type listV2TestObject struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
	Data         []byte
}

type listV2TestBucket struct {
	Objects map[string]*listV2TestObject
}

type listV2TestStore struct {
	buckets map[string]*listV2TestBucket
}

func newListV2TestStore() *listV2TestStore {
	return &listV2TestStore{buckets: map[string]*listV2TestBucket{
		"bucket": {
			Objects: map[string]*listV2TestObject{
				"a b":   {Key: "a b", Data: []byte("sp")},
				"a.txt": {Key: "a.txt", Size: 1, ETag: `"a1"`, LastModified: time.Date(2024, 1, 2, 3, 4, 5, 0, time UTC)},
				"a/b/1": {Key: "a/b/1", Data: []byte("one")},
				"a/b/2": {Key: "a/b/2"},
				"ab":    {Key: "ab", Data: []byte("ab")},
				"b.txt": {Key: "b.txt", ETag: "b2"},
				"c/x":   {Key: "c/x", Data: []byte("xxx")},
			},
		},
	}}
}

func listV2TestGet(t *testing.T, store *listV2TestStore, target string, wantStatus int) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	HandleListObjectsV2(store, rec, req)
	if rec.Code != wantStatus {
		t.Fatalf("GET %s: status = %d, want %d; body:\n%s", target, rec.Code, wantStatus, rec.Body.String())
	}
	return rec.Body.String()
}

func listV2AssertContains(t *testing.T, body string, wants []string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q:\n%s", want, body)
		}
	}
}

func TestListObjectsV2WireDocument(t *testing.T) {
	store := newListV2TestStore()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/bucket?list-type=2", nil)
	HandleListObjectsV2(store, rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body:\n%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/xml" {
		t.Fatalf("Content-Type = %q, want application/xml", got)
	}
	body := rec.Body.String()
	listV2AssertContains(t, body, []string{
		`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`,
		`<Name>bucket</Name>`,
		`<Prefix></Prefix>`,
		`<KeyCount>7</KeyCount>`,
		`<MaxKeys>1000</MaxKeys>`,
		`<IsTruncated>false</IsTruncated>`,
		`<Key>a.txt</Key>`,
		`<ETag>"a1"</ETag>`,
		`<ETag>"b2"</ETag>`,
		`<ETag>"`,
		`<Size>1</Size>`,
		`<Size>3</Size>`,
		`<LastModified>2024-01-02T03:04:05.000Z</LastModified>`,
		`<LastModified>1970-01-01T00:00:00.000Z</LastModified>`,
		`<StorageClass>STANDARD</StorageClass>`,
		`</ListBucketResult>`,
	})
	// Lexical (bytewise) key order.
	pos := -1
	for _, k := range []string{"a b", "a.txt", "a/b/1", "a/b/2", "ab", "b.txt", "c/x"} {
		i := strings.Index(body, "<Key>"+k+"</Key>")
		if i < 0 {
			t.Fatalf("missing <Key>%s</Key> in:\n%s", k, body)
		}
		if i <= pos {
			t.Fatalf("keys not in lexical order around %s:\n%s", k, body)
		}
		pos = i
	}
}

func TestListObjectsV2Delimiter(t *testing.T) {
	body := listV2TestGet(t, newListV2TestStore(), "/bucket?list-type=2&delimiter=/", http.StatusOK)
	listV2AssertContains(t, body, []string{
		`<KeyCount>6</KeyCount>`,
		`<Delimiter>/</Delimiter>`,
		`<Key>a b</Key>`,
		`<Key>a.txt</Key>`,
		`<Key>ab</Key>`,
		`<Key>b.txt</Key>`,
		`<CommonPrefixes><Prefix>a/</Prefix></CommonPrefixes>`,
		`<CommonPrefixes><Prefix>c/</Prefix></CommonPrefixes>`,
	})
	if strings.Contains(body, "a/b/1") {
		t.Fatalf("folded keys leaked into Contents:\n%s", body)
	}
	if got := strings.Count(body, "<CommonPrefixes>"); got != 2 {
		t.Fatalf("CommonPrefixes = %d, want 2:\n%s", got, body)
	}
}

func TestListObjectsV2Prefix(t *testing.T) {
	body := listV2TestGet(t, newListV2TestStore(), "/bucket?list-type=2&prefix=a&delimiter=/", http.StatusOK)
	listV2AssertContains(t, body, []string{
		`<Prefix>a</Prefix>`,
		`<KeyCount>4</KeyCount>`,
		`<Key>a b</Key>`,
		`<Key>a.txt</Key>`,
		`<Key>ab</Key>`,
		`<CommonPrefixes><Prefix>a/</Prefix></CommonPrefixes>`,
	})
	if strings.Contains(body, "b.txt") || strings.Contains(body, "c/x") {
		t.Fatalf("prefix filter leaked:\n%s", body)
	}
}

func TestListObjectsV2EncodingAndOwner(t *testing.T) {
	body := listV2TestGet(t, newListV2TestStore(), "/bucket?list-type=2&prefix=a&encoding-type=url&fetch-owner=true", http.StatusOK)
	listV2AssertContains(t, body, []string{
		`<KeyCount>5</KeyCount>`,
		`<EncodingType>url</EncodingType>`,
		`<Key>a%20b</Key>`,
		`<Key>a.txt</Key>`,
		`<Key>a/b/1</Key>`,
		`<Owner><ID>floci</ID><DisplayName>floci</DisplayName></Owner>`,
	})
	if got := strings.Count(body, "<Owner>"); got != 3 {
		t.Fatalf("Owner blocks = %d, want 3:\n%s", got, body)
	}
}

func TestListObjectsV2StartAfter(t *testing.T) {
	body := listV2TestGet(t, newListV2TestStore(), "/bucket?list-type=2&start-after=a.txt", http.StatusOK)
	listV2AssertContains(t, body, []string{
		`<StartAfter>a.txt</StartAfter>`,
		`<KeyCount>5</KeyCount>`,
		`<Key>ab</Key>`,
		`<Key>c/x</Key>`,
	})
	if strings.Contains(body, "<Key>a b</Key>") || strings.Contains(body, "<Key>a.txt</Key>") {
		t.Fatalf("start-after not applied:\n%s", body)
	}
}

func TestListObjectsV2MaxKeysAndErrors(t *testing.T) {
	body := listV2TestGet(t, newListV2TestStore(), "/bucket?list-type=2&max-keys=0", http.StatusOK)
	listV2AssertContains(t, body, []string{
		`<KeyCount>0</KeyCount>`,
		`<IsTruncated>false</IsTruncated>`,
	})
	if got := strings.Count(body, "<Contents>"); got != 0 {
		t.Fatalf("Contents = %d, want 0:\n%s", got, body)
	}

	body = listV2TestGet(t, newListV2TestStore(), "/bucket?list-type=2&max-keys=nope", http.StatusBadRequest)
	listV2AssertContains(t, body, []string{
		`<Code>InvalidArgument</Code>`,
	})

	body = listV2TestGet(t, newListV2TestStore(), "/missing-bucket?list-type=2", http.StatusNotFound)
	listV2AssertContains(t, body, []string{
		`<Code>NoSuchBucket</Code>`,
		`<Message>The specified bucket does not exist</Message>`,
		`<Resource>/missing-bucket</Resource>`,
	})
}
