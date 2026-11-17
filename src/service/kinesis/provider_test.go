package kinesis

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const (
	maxHashKeyText       = "340282366920938463463374607431768211455"
	midHashKeyText       = "170141183460469231731687303715884105728"
	midMinus1HashKeyText = "170141183460469231731687303715884105727"
)

// call posts a raw x-amz-json 1.0 request to the provider and returns the
// HTTP status plus the decoded response body.
func call(t *testing.T, p *Provider, op, payload string) (int, map[string]interface{}) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
	req.Header.Set("X-Amz-Target", "Kinesis_20131202."+op)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	out := map[string]interface{}{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

// getIterator performs a successful GetShardIterator and returns the token.
func getIterator(t *testing.T, p *Provider, payload string) string {
	t.Helper()
	code, body := call(t, p, "GetShardIterator", payload)
	if code != http.StatusOK {
		t.Fatalf("GetShardIterator: %d %v", code, body)
	}
	iter, ok := body["ShardIterator"].(string)
	if !ok || iter == "" {
		t.Fatalf("ShardIterator missing: %v", body)
	}
	return iter
}

// getRecordsBody calls GetRecords, optionally with extra request fields.
func getRecordsBody(t *testing.T, p *Provider, iter, extra string) map[string]interface{} {
	t.Helper()
	_, body := call(t, p, "GetRecords", fmt.Sprintf(`{"ShardIterator":%q%s}`, iter, extra))
	return body
}

// recordsFrom extracts the decoded Records array of a response.
func recordsFrom(t *testing.T, body map[string]interface{}) []map[string]interface{} {
	t.Helper()
	raw, ok := body["Records"].([]interface{})
	if !ok {
		t.Fatalf("Records missing: %v", body)
	}
	out := make([]map[string]interface{}, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]interface{})
		if !ok {
			t.Fatalf("record is not an object: %v", item)
		}
		out = append(out, m)
	}
	return out
}

// nextIterator returns the NextShardIterator field of a GetRecords
// response: (value, true) when set, ("", false) when null.
func nextIterator(t *testing.T, body map[string]interface{}) (string, bool) {
	t.Helper()
	v, ok := body["NextShardIterator"]
	if !ok || v == nil {
		return "", false
	}
	s, ok := v.(string)
	if !ok {
		t.Fatalf("NextShardIterator is not a string: %v", body)
	}
	return s, true
}

// iteratorReq builds a GetShardIterator payload.
func iteratorReq(stream, shard, typ, extra string) string {
	return fmt.Sprintf(`{"StreamName":%q,"ShardId":%q,"ShardIteratorType":%q%s}`, stream, shard, typ, extra)
}

// timestampReq builds an AT_TIMESTAMP payload against the "ts" stream.
func timestampReq(shard string, ts time.Time) string {
	return fmt.Sprintf(`{"StreamName":"ts","ShardId":%q,"ShardIteratorType":"AT_TIMESTAMP","Timestamp":%d}`, shard, ts.Unix())
}

// assertShard verifies id, hash key range and sequence bookkeeping.
func assertShard(t *testing.T, sh map[string]interface{}, id, start, end string) {
	t.Helper()
	if sh["ShardId"] != id {
		t.Fatalf("ShardId = %v, want %s (shard %#v)", sh["ShardId"], id, sh)
	}
	hk, ok := sh["HashKeyRange"].(map[string]interface{})
	if !ok {
		t.Fatalf("HashKeyRange missing: %#v", sh)
	}
	if hk["StartingHashKey"] != start || hk["EndingHashKey"] != end {
		t.Fatalf("HashKeyRange = %v, want [%s, %s]", hk, start, end)
	}
	snr, ok := sh["SequenceNumberRange"].(map[string]interface{})
	if !ok {
		t.Fatalf("SequenceNumberRange missing: %#v", sh)
	}
	if starting, _ := snr["StartingSequenceNumber"].(string); starting == "" {
		t.Fatalf("StartingSequenceNumber missing: %#v", snr)
	}
}

// fakeClock is a deterministic stand-in for Provider.Now.
type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

func TestStreamLifecycle(t *testing.T) {
	p := NewProvider()

	if code, body := call(t, p, "CreateStream", `{"StreamName":"alpha","ShardCount":2}`); code != http.StatusOK || len(body) != 0 {
		t.Fatalf("CreateStream: %d %v", code, body)
	}

	code, body := call(t, p, "DescribeStream", `{"StreamName":"alpha"}`)
	if code != http.StatusOK {
		t.Fatalf("DescribeStream: %d %v", code, body)
	}
	desc, ok := body["StreamDescription"].(map[string]interface{})
	if !ok {
		t.Fatalf("StreamDescription missing: %v", body)
	}
	if desc["StreamName"] != "alpha" || desc["StreamStatus"] != "ACTIVE" {
		t.Fatalf("stream description: %v", desc)
	}
	if want := "arn:aws:kinesis:us-east-1:000000000000:stream/alpha"; desc["StreamARN"] != want {
		t.Fatalf("StreamARN = %v, want %s", desc["StreamARN"], want)
	}
	if desc["EncryptionType"] != "NONE" || desc["RetentionPeriodHours"] != float64(24) {
		t.Fatalf("stream defaults: %v", desc)
	}
	shards, ok := desc["Shards"].([]interface{})
	if !ok || len(shards) != 2 || desc["HasMoreShards"] != false {
		t.Fatalf("shards: %#v", desc)
	}
	assertShard(t, shards[0].(map[string]interface{}), "shardId-000000000000", "0", midMinus1HashKeyText)
	assertShard(t, shards[1].(map[string]interface{}), "shardId-000000000001", midHashKeyText, maxHashKeyText)

	if code, body := call(t, p, "CreateStream", `{"StreamName":"alpha"}`); code != http.StatusBadRequest || body["__type"] != "ResourceInUseException" {
		t.Fatalf("duplicate create: %d %v", code, body)
	}

	call(t, p, "CreateStream", `{"StreamName":"beta"}`)
	code, body = call(t, p, "DescribeStreamSummary", `{"StreamName":"beta"}`)
	if code != http.StatusOK {
		t.Fatalf("DescribeStreamSummary: %d %v", code, body)
	}
	sum, ok := body["StreamDescriptionSummary"].(map[string]interface{})
	if !ok {
		t.Fatalf("StreamDescriptionSummary missing: %v", body)
	}
	if sum["OpenShardCount"] != float64(1) || sum["StreamStatus"] != "ACTIVE" || sum["ConsumerCount"] != float64(0) {
		t.Fatalf("summary: %v", sum)
	}

	_, body = call(t, p, "ListStreams", `{}`)
	names, ok := body["StreamNames"].([]interface{})
	if !ok || len(names) != 2 || names[0] != "alpha" || names[1] != "beta" || body["HasMoreStreams"] != false {
		t.Fatalf("ListStreams: %v", body)
	}

	_, body = call(t, p, "ListStreams", `{"Limit":1}`)
	names, ok = body["StreamNames"].([]interface{})
	if !ok || len(names) != 1 || names[0] != "alpha" || body["HasMoreStreams"] != true {
		t.Fatalf("ListStreams limit: %v", body)
	}

	_, body = call(t, p, "ListStreams", `{"ExclusiveStartStreamName":"alpha"}`)
	names, ok = body["StreamNames"].([]interface{})
	if !ok || len(names) != 1 || names[0] != "beta" {
		t.Fatalf("ListStreams exclusive start: %v", body)
	}

	if code, body := call(t, p, "DescribeStream", `{"StreamName":"ghost"}`); code != http.StatusBadRequest || body["__type"] != "ResourceNotFoundException" {
		t.Fatalf("describe ghost: %d %v", code, body)
	}

	call(t, p, "DeleteStream", `{"StreamName":"alpha"}`)
	_, body = call(t, p, "ListStreams", `{}`)
	names, ok = body["StreamNames"].([]interface{})
	if !ok || len(names) != 1 || names[0] != "beta" {
		t.Fatalf("ListStreams after delete: %v", body)
	}
	if code, body := call(t, p, "DeleteStream", `{"StreamName":"alpha"}`); code != http.StatusBadRequest || body["__type"] != "ResourceNotFoundException" {
		t.Fatalf("delete ghost: %d %v", code, body)
	}
}

func TestMD5PartitionKeyRouting(t *testing.T) {
	p := NewProvider()
	if code, body := call(t, p, "CreateStream", `{"StreamName":"route","ShardCount":2}`); code != http.StatusOK || len(body) != 0 {
		t.Fatalf("create: %d %v", code, body)
	}

	// md5("foo") = acbd18db4cc2f85cedef654fccc4a4d8 -> upper half of the
	// 128 bit key space, so it must land on shard 1.
	code, body := call(t, p, "PutRecord", `{"StreamName":"route","Data":"Zm9vLWRhdGE=","PartitionKey":"foo"}`)
	if code != http.StatusOK {
		t.Fatalf("put foo: %d %v", code, body)
	}
	if body["ShardId"] != "shardId-000000000001" {
		t.Fatalf("md5(foo) must route to shard 1: %v", body)
	}
	seq, ok := body["SequenceNumber"].(string)
	if !ok || seq == "" {
		t.Fatalf("SequenceNumber missing: %v", body)
	}

	// md5("a") = 0cc175b9c0f1b6a831c399e269772661 -> lower half, shard 0.
	code, body = call(t, p, "PutRecord", `{"StreamName":"route","Data":"YQ==","PartitionKey":"a"}`)
	if code != http.StatusOK {
		t.Fatalf("put a: %d %v", code, body)
	}
	if body["ShardId"] != "shardId-000000000000" {
		t.Fatalf("md5(a) must route to shard 0: %v", body)
	}

	// explicit hash keys pin the routing decision
	code, body = call(t, p, "PutRecord", `{"StreamName":"route","Data":"eg==","PartitionKey":"ignored","ExplicitHashKey":"0"}`)
	if code != http.StatusOK {
		t.Fatalf("put explicit 0: %d %v", code, body)
	}
	if body["ShardId"] != "shardId-000000000000" {
		t.Fatalf("explicit hash key 0 must route to shard 0: %v", body)
	}
	code, body = call(t, p, "PutRecord", fmt.Sprintf(`{"StreamName":"route","Data":"eg==","PartitionKey":"ignored","ExplicitHashKey":%q}`, midHashKeyText))
	if code != http.StatusOK {
		t.Fatalf("put explicit mid: %d %v", code, body)
	}
	if body["ShardId"] != "shardId-000000000001" {
		t.Fatalf("explicit hash key 2^127 must route to shard 1: %v", body)
	}

	// shard 1 holds the foo record first, readable through an iterator
	iter := getIterator(t, p, iteratorReq("route", "shardId-000000000001", "TRIM_HORIZON", ""))
	got := recordsFrom(t, getRecordsBody(t, p, iter, ""))
	if len(got) != 2 {
		t.Fatalf("shard 1 contents: %v", got)
	}
	if got[0]["PartitionKey"] != "foo" || got[0]["Data"] != "Zm9vLWRhdGE=" || got[0]["SequenceNumber"] != seq {
		t.Fatalf("first shard 1 record: %v", got[0])
	}

	if code, body := call(t, p, "PutRecord", fmt.Sprintf(`{"StreamName":"route","Data":"YQ==","PartitionKey":"%s"}`, strings.Repeat("k", 257))); code != http.StatusBadRequest || body["__type"] != "InvalidArgumentException" {
		t.Fatalf("partition key too long: %d %v", code, body)
	}
	if code, body := call(t, p, "PutRecord", `{"StreamName":"route","PartitionKey":"k"}`); code != http.StatusBadRequest || body["__type"] != "InvalidArgumentException" {
		t.Fatalf("data required: %d %v", code, body)
	}
	if code, body := call(t, p, "PutRecord", `{"StreamName":"ghost","Data":"YQ==","PartitionKey":"k"}`); code != http.StatusBadRequest || body["__type"] != "ResourceNotFoundException" {
		t.Fatalf("ghost stream: %d %v", code, body)
	}
}

func TestShardIteratorsAndPaging(t *testing.T) {
	p := NewProvider()
	if code, body := call(t, p, "CreateStream", `{"StreamName":"it"}`); code != http.StatusOK || len(body) != 0 {
		t.Fatalf("create: %d %v", code, body)
	}

	var seqs [3]string
	for i, word := range []string{"one", "two", "three"} {
		payload := fmt.Sprintf(`{"StreamName":"it","Data":%q,"PartitionKey":"p"}`, base64.StdEncoding.EncodeToString([]byte(word)))
		code, body := call(t, p, "PutRecord", payload)
		seq, gotSeq := body["SequenceNumber"].(string)
		if code != http.StatusOK || !gotSeq || seq == "" {
			t.Fatalf("put %s: %d %v", word, code, body)
		}
		seqs[i] = seq
	}
	if !(seqs[0] < seqs[1] && seqs[1] < seqs[2]) {
		t.Fatalf("sequence numbers must increase: %v", seqs)
	}

	// TRIM_HORIZON with Limit paging
	iter := getIterator(t, p, iteratorReq("it", "shardId-000000000000", "TRIM_HORIZON", ""))
	body := getRecordsBody(t, p, iter, `,"Limit":2`)
	recs := recordsFrom(t, body)
	if len(recs) != 2 || recs[0]["Data"] != "b25l" || recs[1]["Data"] != "dHdv" {
		t.Fatalf("page 1: %v", body)
	}
	if _, isNum := body["MillisBehindLatest"].(float64); !isNum {
		t.Fatalf("MillisBehindLatest missing: %v", body)
	}
	next, hasNext := nextIterator(t, body)
	if !hasNext || next == "" {
		t.Fatalf("page 1 next iterator: %v", body)
	}

	body = getRecordsBody(t, p, next, "")
	recs = recordsFrom(t, body)
	if len(recs) != 1 || recs[0]["Data"] != "dGhyZWU=" {
		t.Fatalf("page 2: %v", body)
	}
	next, hasNext = nextIterator(t, body)
	if !hasNext || next == "" {
		t.Fatalf("open shard keeps yielding iterators: %v", body)
	}

	body = getRecordsBody(t, p, next, "")
	recs = recordsFrom(t, body)
	if len(recs) != 0 {
		t.Fatalf("page 3 (drained): %v", body)
	}

	// LATEST only sees records added after iterator creation
	latest := getIterator(t, p, iteratorReq("it", "shardId-000000000000", "LATEST", ""))
	body = getRecordsBody(t, p, latest, "")
	recs = recordsFrom(t, body)
	if len(recs) != 0 {
		t.Fatalf("LATEST before new data: %v", body)
	}
	call(t, p, "PutRecord", `{"StreamName":"it","Data":"bGF0ZXN0","PartitionKey":"p"}`)
	body = getRecordsBody(t, p, latest, "")
	recs = recordsFrom(t, body)
	if len(recs) != 1 || recs[0]["Data"] != "bGF0ZXN0" {
		t.Fatalf("LATEST after new data: %v", body)
	}

	// AT_SEQUENCE_NUMBER is inclusive, AFTER_SEQUENCE_NUMBER exclusive
	at := getIterator(t, p, iteratorReq("it", "shardId-000000000000", "AT_SEQUENCE_NUMBER", fmt.Sprintf(`,"StartingSequenceNumber":%q`, seqs[1])))
	body = getRecordsBody(t, p, at, "")
	recs = recordsFrom(t, body)
	if len(recs) != 3 || recs[0]["Data"] != "dHdv" {
		t.Fatalf("AT_SEQUENCE_NUMBER: %v", body)
	}

	after := getIterator(t, p, iteratorReq("it", "shardId-000000000000", "AFTER_SEQUENCE_NUMBER", fmt.Sprintf(`,"StartingSequenceNumber":%q`, seqs[1])))
	body = getRecordsBody(t, p, after, "")
	recs = recordsFrom(t, body)
	if len(recs) != 2 || recs[0]["Data"] != "dGhyZWU=" {
		t.Fatalf("AFTER_SEQUENCE_NUMBER: %v", body)
	}

	// error paths
	if code, body := call(t, p, "GetShardIterator", iteratorReq("it", "shardId-000000000000", "YESTERDAY", "")); code != http.StatusBadRequest || body["__type"] != "InvalidArgumentException" {
		t.Fatalf("unknown iterator type: %d %v", code, body)
	}
	if code, body := call(t, p, "GetShardIterator", iteratorReq("it", "shardId-000000000000", "AFTER_SEQUENCE_NUMBER", "")); code != http.StatusBadRequest || body["__type"] != "InvalidArgumentException" {
		t.Fatalf("missing StartingSequenceNumber: %d %v", code, body)
	}
	if code, body := call(t, p, "GetShardIterator", iteratorReq("ghost", "shardId-000000000000", "TRIM_HORIZON", "")); code != http.StatusBadRequest || body["__type"] != "ResourceNotFoundException" {
		t.Fatalf("ghost stream: %d %v", code, body)
	}
	if code, body := call(t, p, "GetShardIterator", iteratorReq("it", "shardId-999999999999", "TRIM_HORIZON", "")); code != http.StatusBadRequest || body["__type"] != "ResourceNotFoundException" {
		t.Fatalf("ghost shard: %d %v", code, body)
	}
	if code, body := call(t, p, "GetRecords", `{"ShardIterator":"garbage"}`); code != http.StatusBadRequest || body["__type"] != "ExpiredIteratorException" {
		t.Fatalf("garbage iterator: %d %v", code, body)
	}
}

func TestAtTimestampIterator(t *testing.T) {
	t0 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: t0}
	p := NewProvider()
	p.Now = clock.Now

	if code, body := call(t, p, "CreateStream", `{"StreamName":"ts"}`); code != http.StatusOK || len(body) != 0 {
		t.Fatalf("create: %d %v", code, body)
	}
	call(t, p, "PutRecord", `{"StreamName":"ts","Data":"ZTE=","PartitionKey":"k"}`) // e1 at t0
	clock.Advance(2 * time.Second)
	call(t, p, "PutRecord", `{"StreamName":"ts","Data":"ZTI=","PartitionKey":"k"}`) // e2 at t0+2s

	// between the two records: only the newer one is visible
	iter := getIterator(t, p, timestampReq("shardId-000000000000", t0.Add(time.Second)))
	body := getRecordsBody(t, p, iter, "")
	recs := recordsFrom(t, body)
	if len(recs) != 1 || recs[0]["Data"] != "ZTI=" {
		t.Fatalf("iterator at t0+1s: %v", body)
	}
	if recs[0]["ApproximateArrivalTimestamp"] != float64(1577836802) {
		t.Fatalf("approximate arrival timestamp: %v", recs[0])
	}

	// at t0 both records are visible (AT_TIMESTAMP is inclusive)
	iter = getIterator(t, p, timestampReq("shardId-000000000000", t0))
	body = getRecordsBody(t, p, iter, "")
	recs = recordsFrom(t, body)
	if len(recs) != 2 || recs[0]["Data"] != "ZTE=" {
		t.Fatalf("iterator at t0: %v", body)
	}

	// an iterator positioned in the future waits for matching records
	iter = getIterator(t, p, timestampReq("shardId-000000000000", t0.Add(10*time.Second)))
	body = getRecordsBody(t, p, iter, "")
	recs = recordsFrom(t, body)
	if len(recs) != 0 {
		t.Fatalf("iterator in the future: %v", body)
	}
	next, hasNext := nextIterator(t, body)
	if !hasNext || next == "" {
		t.Fatalf("future iterator next: %v", body)
	}
	clock.Advance(11 * time.Second)
	call(t, p, "PutRecord", `{"StreamName":"ts","Data":"ZTM=","PartitionKey":"k"}`) // e3 at t0+13s
	body = getRecordsBody(t, p, next, "")
	recs = recordsFrom(t, body)
	if len(recs) != 1 || recs[0]["Data"] != "ZTM=" {
		t.Fatalf("future iterator after arrival: %v", body)
	}

	if code, body := call(t, p, "GetShardIterator", `{"StreamName":"ts","ShardId":"shardId-000000000000","ShardIteratorType":"AT_TIMESTAMP"}`); code != http.StatusBadRequest || body["__type"] != "InvalidArgumentException" {
		t.Fatalf("missing timestamp: %d %v", code, body)
	}
}

func TestSplitAndMergeShards(t *testing.T) {
	p := NewProvider()
	if code, body := call(t, p, "CreateStream", `{"StreamName":"reshard"}`); code != http.StatusOK || len(body) != 0 {
		t.Fatalf("create: %d %v", code, body)
	}
	call(t, p, "PutRecord", `{"StreamName":"reshard","Data":"cGFyZW50","PartitionKey":"x"}`)

	if code, body := call(t, p, "SplitShard", `{"StreamName":"reshard","ShardToSplit":"shardId-000000000000","NewStartingHashKey":"0"}`); code != http.StatusBadRequest || body["__type"] != "InvalidArgumentException" {
		t.Fatalf("split at range start: %d %v", code, body)
	}
	if code, body := call(t, p, "SplitShard", `{"StreamName":"reshard","ShardToSplit":"shardId-123456789012","NewStartingHashKey":"100"}`); code != http.StatusBadRequest || body["__type"] != "ResourceNotFoundException" {
		t.Fatalf("split ghost shard: %d %v", code, body)
	}

	if code, body := call(t, p, "SplitShard", fmt.Sprintf(`{"StreamName":"reshard","ShardToSplit":"shardId-000000000000","NewStartingHashKey":%q}`, midHashKeyText)); code != http.StatusOK || len(body) != 0 {
		t.Fatalf("split: %d %v", code, body)
	}

	_, body := call(t, p, "DescribeStream", `{"StreamName":"reshard","Limit":10}`)
	desc := body["StreamDescription"].(map[string]interface{})
	shards := desc["Shards"].([]interface{})
	if len(shards) != 3 || desc["HasMoreShards"] != false {
		t.Fatalf("shards after split: %#v", desc)
	}
	parent := shards[0].(map[string]interface{})
	if parent["ShardId"] != "shardId-000000000000" {
		t.Fatalf("parent id: %v", parent)
	}
	seqRange := parent["SequenceNumberRange"].(map[string]interface{})
	if ending, _ := seqRange["EndingSequenceNumber"].(string); ending == "" {
		t.Fatalf("split parent must be closed: %v", parent)
	}
	left := shards[1].(map[string]interface{})
	assertShard(t, left, "shardId-000000000001", "0", midMinus1HashKeyText)
	if left["ParentShardId"] != "shardId-000000000000" {
		t.Fatalf("left parent link: %v", left)
	}
	right := shards[2].(map[string]interface{})
	assertShard(t, right, "shardId-000000000002", midHashKeyText, maxHashKeyText)
	if right["ParentShardId"] != "shardId-000000000000" {
		t.Fatalf("right parent link: %v", right)
	}

	// parent records remain readable; once drained the next iterator is null
	iter := getIterator(t, p, iteratorReq("reshard", "shardId-000000000000", "TRIM_HORIZON", ""))
	body = getRecordsBody(t, p, iter, "")
	recs := recordsFrom(t, body)
	if len(recs) != 1 || recs[0]["Data"] != "cGFyZW50" {
		t.Fatalf("parent records: %v", body)
	}
	if _, has := nextIterator(t, body); has {
		t.Fatalf("exhausted closed shard must hand back a null next iterator: %v", body)
	}

	// new data lands in the children
	_, body = call(t, p, "PutRecord", `{"StreamName":"reshard","Data":"bGVmdA==","PartitionKey":"a"}`)
	if body["ShardId"] != "shardId-000000000001" {
		t.Fatalf("routing into left child: %v", body)
	}
	_, body = call(t, p, "PutRecord", `{"StreamName":"reshard","Data":"cmlnaHQ=","PartitionKey":"foo"}`)
	if body["ShardId"] != "shardId-000000000002" {
		t.Fatalf("routing into right child: %v", body)
	}

	// merging the two siblings recreates a full range shard
	if code, body := call(t, p, "MergeShards", `{"StreamName":"reshard","ShardToSplit":"shardId-000000000001","AdjacentShardToMerge":"shardId-000000000002"}`); code != http.StatusOK || len(body) != 0 {
		t.Fatalf("merge: %d %v", code, body)
	}
	_, body = call(t, p, "DescribeStream", `{"StreamName":"reshard","Limit":10}`)
	desc = body["StreamDescription"].(map[string]interface{})
	shards = desc["Shards"].([]interface{})
	if len(shards) != 4 {
		t.Fatalf("shards after merge: %#v", desc)
	}
	merged := shards[3].(map[string]interface{})
	assertShard(t, merged, "shardId-000000000003", "0", maxHashKeyText)
	if merged["ParentShardId"] != "shardId-000000000001" || merged["AdjacentParentShardId"] != "shardId-000000000002" {
		t.Fatalf("merge parent links: %v", merged)
	}
	_, body = call(t, p, "PutRecord", `{"StreamName":"reshard","Data":"bWVyZ2Vk","PartitionKey":"foo"}`)
	if body["ShardId"] != "shardId-000000000003" {
		t.Fatalf("routing after merge: %v", body)
	}

	_, body = call(t, p, "ListShards", `{"StreamName":"reshard"}`)
	allShards, gotShards := body["Shards"].([]interface{})
	if !gotShards || len(allShards) != 4 {
		t.Fatalf("ListShards: %v", body)
	}

	// DescribeStream shard pagination
	_, body = call(t, p, "DescribeStream", `{"StreamName":"reshard","Limit":2}`)
	desc = body["StreamDescription"].(map[string]interface{})
	if desc["HasMoreShards"] != true {
		t.Fatalf("describe paging: %#v", desc)
	}
	_, body = call(t, p, "DescribeStream", `{"StreamName":"reshard","Limit":2,"ExclusiveStartShardId":"shardId-000000000001"}`)
	desc = body["StreamDescription"].(map[string]interface{})
	shards = desc["Shards"].([]interface{})
	if desc["HasMoreShards"] != false || len(shards) != 2 ||
		shards[0].(map[string]interface{})["ShardId"] != "shardId-000000000002" ||
		shards[1].(map[string]interface{})["ShardId"] != "shardId-000000000003" {
		t.Fatalf("describe exclusive start: %#v", desc)
	}

	if code, body := call(t, p, "MergeShards", `{"StreamName":"reshard","ShardToSplit":"shardId-000000000000","AdjacentShardToMerge":"shardId-000000000003"}`); code != http.StatusBadRequest || body["__type"] != "InvalidArgumentException" {
		t.Fatalf("merging closed shards: %d %v", code, body)
	}
}

func TestPutRecordsBatch(t *testing.T) {
	p := NewProvider()
	if code, body := call(t, p, "CreateStream", `{"StreamName":"batch"}`); code != http.StatusOK || len(body) != 0 {
		t.Fatalf("create: %d %v", code, body)
	}

	payload := `{"StreamName":"batch","Records":[` +
		`{"Data":"cg==","PartitionKey":"a"},` +
		`{"Data":"cw==","PartitionKey":"a"},` +
		`{"Data":"dA==","PartitionKey":"foo"}]}`
	code, body := call(t, p, "PutRecords", payload)
	if code != http.StatusOK {
		t.Fatalf("PutRecords: %d %v", code, body)
	}
	if body["FailedRecordCount"] != float64(0) {
		t.Fatalf("FailedRecordCount: %v", body)
	}
	results := recordsFrom(t, body)
	if len(results) != 3 {
		t.Fatalf("results: %v", body)
	}
	distinct := map[interface{}]bool{}
	for _, entry := range results {
		if entry["SequenceNumber"] == "" || entry["ShardId"] == "" {
			t.Fatalf("result entry: %v", entry)
		}
		distinct[entry["ShardId"]] = true
	}
	if len(distinct) != 2 {
		t.Fatalf("records should span two shards: %v", distinct)
	}
}

func TestIteratorAfterStreamDelete(t *testing.T) {
	p := NewProvider()
	call(t, p, "CreateStream", `{"StreamName":"gone"}`)
	call(t, p, "PutRecord", `{"StreamName":"gone","Data":"ZA==","PartitionKey":"k"}`)

	iter := getIterator(t, p, iteratorReq("gone", "shardId-000000000000", "TRIM_HORIZON", ""))
	call(t, p, "DeleteStream", `{"StreamName":"gone"}`)

	if code, body := call(t, p, "GetRecords", fmt.Sprintf(`{"ShardIterator":%q}`, iter)); code != http.StatusBadRequest || body["__type"] != "ExpiredIteratorException" {
		t.Fatalf("stale iterator: %d %v", code, body)
	}
}

func TestJSONProtocol(t *testing.T) {
	p := NewProvider()

	if code, body := call(t, p, "BogusOperation", `{}`); code != http.StatusBadRequest || body["__type"] != "UnknownOperationException" {
		t.Fatalf("unknown op: %d %v", code, body)
	}

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing X-Amz-Target: %d %s", rec.Code, rec.Body.String())
	}

	if code, body := call(t, p, "CreateStream", `{"StreamName":`); code != http.StatusBadRequest || body["__type"] != "SerializationException" {
		t.Fatalf("malformed body: %d %v", code, body)
	}

	req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"StreamName":"hdr"}`))
	req.Header.Set("X-Amz-Target", "Kinesis_20131202.CreateStream")
	rec = httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/x-amz-json-1.0" {
		t.Fatalf("Content-Type: %q", ct)
	}
	if rec.Header().Get("x-amzn-RequestId") == "" {
		t.Fatal("x-amzn-RequestId header missing")
	}

	if code, body := call(t, p, "DescribeStream", `{"StreamName":"hdr2"}`); code != http.StatusBadRequest {
		t.Fatalf("describe hdr2: %d %v", code, body)
	} else if msg, _ := body["message"].(string); !strings.Contains(msg, "hdr2") {
		t.Fatalf("error message should mention the stream: %v", body)
	}
}
