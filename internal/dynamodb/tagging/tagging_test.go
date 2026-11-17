package tagging

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
)

const (
	testTableArn = "arn:aws:dynamodb:us-east-1:111122223333:table/Music"
	testIndexArn = "arn:aws:dynamodb:us-east-1:111122223333:table/Music/index/ByArtist"
)

func newFixture() (*MapResolver, *Store) {
	res := NewMapResolver()
	res.AddTable("Music")
	res.AddIndex("Music", "ByArtist")
	return res, NewStore(res)
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %v: %v", value, err)
	}
	return body
}

func makeTags(n int) []Tag {
	tags := make([]Tag, 0, n)
	for i := 0; i < n; i++ {
		tags = append(tags, Tag{Key: fmt.Sprintf("k%03d", i), Value: "v"})
	}
	return tags
}

func TestParseARNAcceptsVariants(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{testTableArn, testTableArn},
		{testIndexArn, testIndexArn},
		{"arn:aws-us-gov:dynamodb:us-gov-west-1:111122223333:table/T", "arn:aws-us-gov:dynamodb:us-gov-west-1:111122223333:table/T"},
		{"arn:AWS:dynamodb:eu-west-1:000000000000:table/T", "arn:aws:dynamodb:eu-west-1:000000000000:table/T"},
	}
	for _, tc := range cases {
		a, err := ParseARN(tc.raw)
		if err != nil {
			t.Fatalf("ParseARN(%q) = %v, want nil", tc.raw, err)
		}
		if a.String() != tc.want {
			t.Fatalf("ParseARN(%q).String() = %q, want %q", tc.raw, a.String(), tc.want)
		}
	}
	if a, _ := ParseARN(testTableArn); a.Kind != KindTable || a.Index != "" {
		t.Fatalf("table ARN parsed as %+v", a)
	}
	if a, _ := ParseARN(testIndexArn); a.Kind != KindIndex || a.Table != "Music" || a.Index != "ByArtist" {
		t.Fatalf("index ARN parsed as %+v", a)
	}
}

func TestMapResolver(t *testing.T) {
	r := NewMapResolver()
	if r.Exists("T", "") {
		t.Fatal("unknown table reported as existing")
	}
	r.AddTable("T")
	if !r.Exists("T", "") {
		t.Fatal("table should exist")
	}
	if r.Exists("T", "I") {
		t.Fatal("unknown index reported as existing")
	}
	r.AddIndex("T", "I")
	if !r.Exists("T", "I") {
		t.Fatal("index should exist")
	}
	r.RemoveIndex("T", "I")
	if r.Exists("T", "I") {
		t.Fatal("removed index should not exist")
	}
	r.RemoveTable("T")
	if r.Exists("T", "") || r.Exists("T", "I") {
		t.Fatal("removed table should not exist")
	}
}

func TestTagAndListTableTags(t *testing.T) {
	_, store := newFixture()

	tags := []Tag{{Key: "env", Value: "dev"}, {Key: "app", Value: "playlist"}}
	if err := store.TagResource(testTableArn, tags); err != nil {
		t.Fatalf("TagResource(table) = %v, want nil", err)
	}
	got, err := store.ListTagsOfResource(testTableArn)
	if err != nil {
		t.Fatalf("ListTagsOfResource(table) = %v, want nil", err)
	}
	want := []Tag{{Key: "app", Value: "playlist"}, {Key: "env", Value: "dev"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListTagsOfResource(table) = %+v, want %+v", got, want)
	}
}

func TestListUntaggedResourceIsEmptySlice(t *testing.T) {
	_, store := newFixture()
	got, err := store.ListTagsOfResource(testTableArn)
	if err != nil {
		t.Fatalf("ListTagsOfResource = %v, want nil", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("got %#v, want an empty non-nil slice", got)
	}
}

func TestTagAndListIndexTagsIsolatedFromTable(t *testing.T) {
	_, store := newFixture()

	if err := store.TagResource(testIndexArn, []Tag{{Key: "tier", Value: "gold"}}); err != nil {
		t.Fatalf("TagResource(index) = %v, want nil", err)
	}
	got, err := store.ListTagsOfResource(testIndexArn)
	if err != nil {
		t.Fatalf("ListTagsOfResource(index) = %v, want nil", err)
	}
	if len(got) != 1 || got[0] != (Tag{Key: "tier", Value: "gold"}) {
		t.Fatalf("ListTagsOfResource(index) = %+v, want the index tag", got)
	}
	tableTags, err := store.ListTagsOfResource(testTableArn)
	if err != nil {
		t.Fatalf("ListTagsOfResource(table) = %v, want nil", err)
	}
	if len(tableTags) != 0 {
		t.Fatalf("index tags leaked into table metadata: %+v", tableTags)
	}
}

func TestTagResourceOverwritesExistingKeys(t *testing.T) {
	_, store := newFixture()

	if err := store.TagResource(testTableArn, []Tag{{Key: "env", Value: "dev"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.TagResource(testTableArn, []Tag{{Key: "env", Value: "prod"}, {Key: "team", Value: "core"}}); err != nil {
		t.Fatal(err)
	}
	got, err := store.ListTagsOfResource(testTableArn)
	if err != nil {
		t.Fatal(err)
	}
	want := []Tag{{Key: "env", Value: "prod"}, {Key: "team", Value: "core"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestUntagResource(t *testing.T) {
	_, store := newFixture()

	if err := store.TagResource(testTableArn, []Tag{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}, {Key: "c", Value: "3"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.UntagResource(testTableArn, []string{"a", "c"}); err != nil {
		t.Fatalf("UntagResource = %v, want nil", err)
	}
	got, err := store.ListTagsOfResource(testTableArn)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Key != "b" {
		t.Fatalf("after untag got %+v, want only key b", got)
	}
	// Untagging keys that are not present is a no-op.
	if err := store.UntagResource(testTableArn, []string{"missing-key"}); err != nil {
		t.Fatalf("UntagResource(unknown key) = %v, want nil", err)
	}
}

func TestResourceNotFoundPaths(t *testing.T) {
	_, store := newFixture()

	missingTable := "arn:aws:dynamodb:us-east-1:111122223333:table/Ghost"
	missingIndex := "arn:aws:dynamodb:us-east-1:111122223333:table/Music/index/GhostIndex"

	ops := []struct {
		name string
		run  func(arn string) error
	}{
		{"TagResource", func(arn string) error { return store.TagResource(arn, []Tag{{Key: "k", Value: "v"}}) }},
		{"UntagResource", func(arn string) error { return store.UntagResource(arn, []string{"k"}) }},
		{"ListTagsOfResource", func(arn string) error { _, err := store.ListTagsOfResource(arn); return err }},
	}
	for _, op := range ops {
		for _, arn := range []string{missingTable, missingIndex} {
			err := op.run(arn)
			if !IsResourceNotFound(err) {
				t.Fatalf("%s(%q) = %v, want ResourceNotFoundException", op.name, arn, err)
			}
			if !errors.Is(err, ErrResourceNotFound) {
				t.Fatalf("%s(%q): errors.Is(ErrResourceNotFound) = false for %v", op.name, arn, err)
			}
		}
	}
}

func TestInvalidArnPaths(t *testing.T) {
	_, store := newFixture()

	badArns := []string{
		"",
		"   ",
		"not-an-arn",
		"https://dynamodb.us-east-1.amazonaws.com/table/Music",
		"arn:aws:s3:::bucket/table/Music",
		"arn:aws:dynamodb:us-east-1:111122223333:backup/abc",
		"arn:aws:dynamodb::111122223333:table/Music",
		"arn:aws:dynamodb:us-east-1:1234:table/Music",
		"arn:aws:dynamodb:us-east-1:111122223333:table/",
		"arn:aws:dynamodb:us-east-1:111122223333:table/Music/index/",
		"arn:aws:dynamodb:us-east-1:111122223333:table/Music/stream/2020",
	}
	for _, arn := range badArns {
		if err := store.TagResource(arn, []Tag{{Key: "k", Value: "v"}}); !IsInvalidArn(err) {
			t.Fatalf("TagResource(%q) = %v, want InvalidArn", arn, err)
		}
		if err := store.UntagResource(arn, []string{"k"}); !IsInvalidArn(err) {
			t.Fatalf("UntagResource(%q) = %v, want InvalidArn", arn, err)
		}
		if _, err := store.ListTagsOfResource(arn); !IsInvalidArn(err) {
			t.Fatalf("ListTagsOfResource(%q) = %v, want InvalidArn", arn, err)
		}
	}
	if _, err := ParseARN("junk"); !errors.Is(err, ErrInvalidArn) {
		t.Fatalf("ParseARN(junk): errors.Is(ErrInvalidArn) = false for %v", err)
	}
}

func TestTagValidation(t *testing.T) {
	_, store := newFixture()

	cases := []struct {
		name string
		tags []Tag
	}{
		{"empty", nil},
		{"too many tags", makeTags(51)},
		{"empty key", []Tag{{Key: "", Value: "v"}}},
		{"reserved aws: key", []Tag{{Key: "aws:owner", Value: "x"}}},
		{"duplicate keys", []Tag{{Key: "k", Value: "1"}, {Key: "k", Value: "2"}}},
		{"key too long", []Tag{{Key: strings.Repeat("k", 129), Value: "v"}}},
		{"value too long", []Tag{{Key: "k", Value: strings.Repeat("v", 257)}}},
	}
	for _, tc := range cases {
		if err := store.TagResource(testTableArn, tc.tags); !IsValidation(err) {
			t.Fatalf("%s: TagResource = %v, want ValidationException", tc.name, err)
		}
	}
	if err := store.UntagResource(testTableArn, nil); !IsValidation(err) {
		t.Fatalf("UntagResource(nil keys) = %v, want ValidationException", err)
	}
	if err := store.UntagResource(testTableArn, []string{""}); !IsValidation(err) {
		t.Fatalf("UntagResource(empty key) = %v, want ValidationException", err)
	}
}

func TestAttachTagsOnCreate(t *testing.T) {
	res := NewMapResolver()
	store := NewStore(res)

	arn := "arn:aws:dynamodb:ap-south-1:111122223333:table/Fresh"
	if err := store.AttachTagsOnCreate(arn, []Tag{{Key: "boot", Value: "yes"}}); err != nil {
		t.Fatalf("AttachTagsOnCreate = %v, want nil", err)
	}

	// The engine registers the table, then the create-time tags are listable.
	res.AddTable("Fresh")
	got, err := store.ListTagsOfResource(arn)
	if err != nil {
		t.Fatalf("ListTagsOfResource = %v, want nil", err)
	}
	if len(got) != 1 || got[0].Value != "yes" {
		t.Fatalf("got %+v, want the create-time tag", got)
	}

	if err := store.AttachTagsOnCreate(arn, nil); err != nil {
		t.Fatalf("AttachTagsOnCreate(no tags) = %v, want nil", err)
	}
	if err := store.AttachTagsOnCreate(arn, []Tag{{Key: "aws:bad", Value: "x"}}); !IsValidation(err) {
		t.Fatalf("AttachTagsOnCreate(reserved key) = %v, want ValidationException", err)
	}
	if err := store.AttachTagsOnCreate("bogus", []Tag{{Key: "k", Value: "v"}}); !IsInvalidArn(err) {
		t.Fatalf("AttachTagsOnCreate(bogus arn) = %v, want InvalidArn", err)
	}
}

func TestDropResourceClearsTableAndIndexTags(t *testing.T) {
	res, store := newFixture()

	otherArn := "arn:aws:dynamodb:us-east-1:111122223333:table/Keep"
	res.AddTable("Keep")
	if err := store.AttachTagsOnCreate(otherArn, []Tag{{Key: "keep", Value: "me"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.AttachTagsOnCreate(testTableArn, []Tag{{Key: "t", Value: "1"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.AttachTagsOnCreate(testIndexArn, []Tag{{Key: "i", Value: "1"}}); err != nil {
		t.Fatal(err)
	}

	res.RemoveTable("Music")
	store.DropResource(testTableArn)

	// Recreate the same table and index: no previous tags may resurface.
	res.AddTable("Music")
	res.AddIndex("Music", "ByArtist")
	tableTags, err := store.ListTagsOfResource(testTableArn)
	if err != nil {
		t.Fatal(err)
	}
	if len(tableTags) != 0 {
		t.Fatalf("table tags survived DropResource: %+v", tableTags)
	}
	indexTags, err := store.ListTagsOfResource(testIndexArn)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexTags) != 0 {
		t.Fatalf("index tags survived DropResource: %+v", indexTags)
	}
	kept, err := store.ListTagsOfResource(otherArn)
	if err != nil {
		t.Fatal(err)
	}
	if len(kept) != 1 || kept[0].Value != "me" {
		t.Fatalf("unrelated table lost its tags: %+v", kept)
	}
	store.DropResource("junk-arn") // must be a safe no-op
}

func TestConcurrentTagAccess(t *testing.T) {
	_, store := newFixture()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = store.TagResource(testTableArn, []Tag{{Key: "n", Value: "v"}})
				_, _ = store.ListTagsOfResource(testTableArn)
				_ = store.UntagResource(testTableArn, []string{"gone"})
			}
		}()
	}
	wg.Wait()
}

func TestHandlerActionsTable(t *testing.T) {
	_, store := newFixture()
	h := &Handler{Store: store}
	actions := h.Actions()
	for _, target := range []string{
		"DynamoDB_20120810.TagResource",
		"DynamoDB_20120810.UntagResource",
		"DynamoDB_20120810.ListTagsOfResource",
	} {
		if _, ok := actions[target]; !ok {
			t.Fatalf("missing action %q", target)
		}
	}
	if _, ok := actions["DynamoDB_20120810.DescribeTable"]; ok {
		t.Fatal("unexpected DescribeTable action")
	}
}

func TestHandlerWireFormat(t *testing.T) {
	_, store := newFixture()
	h := &Handler{Store: store}

	res, err := h.TagResourceAction(mustJSON(t, map[string]any{
		"ResourceArn": testTableArn,
		"Tags":        []Tag{{Key: "env", Value: "dev"}},
	}))
	if err != nil {
		t.Fatalf("TagResourceAction = %v, want nil", err)
	}
	if out, mErr := json.Marshal(res); mErr != nil || string(out) != "{}" {
		t.Fatalf("TagResourceAction response = %s (marshal err %v), want empty object", out, mErr)
	}

	res, err = h.ListTagsOfResourceAction(mustJSON(t, map[string]string{"ResourceArn": testTableArn}))
	if err != nil {
		t.Fatalf("ListTagsOfResourceAction = %v, want nil", err)
	}
	if out, mErr := json.Marshal(res); mErr != nil || string(out) != `{"Tags":[{"Key":"env","Value":"dev"}]}` {
		t.Fatalf("ListTagsOfResource response = %s (marshal err %v)", out, mErr)
	}

	res, err = h.ListTagsOfResourceAction(mustJSON(t, map[string]string{"ResourceArn": testIndexArn}))
	if err != nil {
		t.Fatalf("ListTagsOfResourceAction(index) = %v, want nil", err)
	}
	if out, _ := json.Marshal(res); string(out) != `{"Tags":[]}` {
		t.Fatalf("empty tag set response = %s, want empty JSON Tags array", out)
	}

	if _, err = h.UntagResourceAction(mustJSON(t, map[string]any{"ResourceArn": testTableArn, "TagKeys": []string{"env"}})); err != nil {
		t.Fatalf("UntagResourceAction = %v, want nil", err)
	}
	res, err = h.ListTagsOfResourceAction(mustJSON(t, map[string]string{"ResourceArn": testTableArn}))
	if err != nil {
		t.Fatal(err)
	}
	if out, _ := json.Marshal(res); string(out) != `{"Tags":[]}` {
		t.Fatalf("after untag response = %s, want empty JSON Tags array", out)
	}
}

func TestHandlerErrorPaths(t *testing.T) {
	res := NewMapResolver() // no resources exist: everything is not-found
	store := NewStore(res)
	h := &Handler{Store: store}

	tagReq := mustJSON(t, map[string]any{"ResourceArn": testTableArn, "Tags": []Tag{{Key: "k", Value: "v"}}})
	if _, err := h.TagResourceAction(tagReq); !IsResourceNotFound(err) {
		t.Fatalf("TagResourceAction = %v, want ResourceNotFoundException", err)
	}
	if _, err := h.UntagResourceAction(mustJSON(t, map[string]any{"ResourceArn": testTableArn, "TagKeys": []string{"k"}})); !IsResourceNotFound(err) {
		t.Fatalf("UntagResourceAction = %v, want ResourceNotFoundException", err)
	}
	if _, err := h.ListTagsOfResourceAction(mustJSON(t, map[string]string{"ResourceArn": testTableArn})); !IsResourceNotFound(err) {
		t.Fatalf("ListTagsOfResourceAction = %v, want ResourceNotFoundException", err)
	}
	if _, err := h.ListTagsOfResourceAction(mustJSON(t, map[string]string{"ResourceArn": "bogus"})); !IsInvalidArn(err) {
		t.Fatalf("ListTagsOfResourceAction(bogus arn) = %v, want InvalidArn", err)
	}

	_, err := h.ListTagsOfResourceAction(mustJSON(t, map[string]string{"ResourceArn": testTableArn}))
	wire, encErr := EncodeError(err)
	if encErr != nil {
		t.Fatal(encErr)
	}
	var decoded APIError
	if uErr := json.Unmarshal(wire, &decoded); uErr != nil {
		t.Fatal(uErr)
	}
	if decoded.Type != ErrorTypePrefix+CodeResourceNotFound {
		t.Fatalf("wire type = %q, want %q", decoded.Type, ErrorTypePrefix+CodeResourceNotFound)
	}
	if StatusCode(err) != 400 {
		t.Fatalf("StatusCode = %d, want 400", StatusCode(err))
	}
}

func TestHandlerMalformedPayload(t *testing.T) {
	_, store := newFixture()
	h := &Handler{Store: store}
	if _, err := h.TagResourceAction([]byte("{not json")); !IsValidation(err) {
		t.Fatalf("TagResourceAction(malformed) = %v, want ValidationException", err)
	}
	if _, err := h.UntagResourceAction([]byte("[1,2,3")); !IsValidation(err) {
		t.Fatalf("UntagResourceAction(malformed) = %v, want ValidationException", err)
	}
}
