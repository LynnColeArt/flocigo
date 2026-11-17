package kinesis

import (
    "bytes"
    "crypto/md5"
    "crypto/sha256"
    "encoding/base64"
    "encoding/json"
    "fmt"
    "math/big"
    "net/http"
    "net/http/httptest"
    "net/url"
    "strings"
    "testing"
    "time"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

type prOut struct {
    ShardId        string `json:"ShardId"`
    SequenceNumber string `json:"SequenceNumber"`
    EncryptionType string `json:"EncryptionType,omitempty"`
}

type grOut struct {
    Records []struct {
        SequenceNumber              string  `json:"SequenceNumber"`
        ApproximateArrivalTimestamp float64 `json:"ApproximateArrivalTimestamp"`
        Data                        []byte  `json:"Data"`
        PartitionKey                string  `json:"PartitionKey"`
    } `json:"Records"`
    NextShardIterator  string `json:"NextShardIterator"`
    MillisBehindLatest int64  `json:"MillisBehindLatest"`
}

func doReq(t *testing.T, svc *Service, headers map[string]string, body []byte) *httptest.ResponseRecorder {
    t.Helper()
    r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
    for k, v := range headers {
        r.Header.Set(k, v)
    }
    w := httptest.NewRecorder()
    svc.ServeHTTP(w, r)
    return w
}

func postJSON(t *testing.T, svc *Service, op string, req any) (int, []byte) {
    t.Helper()
    raw, err := json.Marshal(req)
    if err != nil {
        t.Fatalf("marshal request: %v", err)
    }
    w := doReq(t, svc, map[string]string{
        "X-Amz-Target": "Kinesis_20131202." + op,
        "Content-Type": "application/x-amz-json-1.1",
    }, raw)
    return w.Code, w.Body.Bytes()
}

func mustDecode(t *testing.T, body []byte, v any) {
    t.Helper()
    if err := json.Unmarshal(body, v); err != nil {
        t.Fatalf("invalid response %q: %v", string(body), err)
    }
}

func errorCodeOf(t *testing.T, body []byte) string {
    t.Helper()
    var e struct {
        Type    string `json:"__type"`
        Message string `json:"message"`
    }
    mustDecode(t, body, &e)
    return e.Type
}

func createStream(t *testing.T, svc *Service, name string, shards int) {
    t.Helper()
    code, body := postJSON(t, svc, "CreateStream", map[string]any{"StreamName": name, "ShardCount": shards})
    if code != http.StatusOK {
        t.Fatalf("CreateStream %s: status %d body %s", name, code, body)
    }
}

func getIterator(t *testing.T, svc *Service, streamName, shardID, typ string) string {
    t.Helper()
    return iteratorWith(t, svc, map[string]any{
        "StreamName":        streamName,
        "ShardId":           shardID,
        "ShardIteratorType": typ,
    })
}

func iteratorWith(t *testing.T, svc *Service, req map[string]any) string {
    t.Helper()
    code, body := postJSON(t, svc, "GetShardIterator", req)
    if code != http.StatusOK {
        t.Fatalf("GetShardIterator: status %d body %s", code, body)
    }
    var out struct {
        ShardIterator string `json:"ShardIterator"`
    }
    mustDecode(t, body, &out)
    if out.ShardIterator == "" {
        t.Fatal("empty shard iterator")
    }
    return out.ShardIterator
}

func putRecord(t *testing.T, svc *Service, streamName, partitionKey string, data []byte) prOut {
    t.Helper()
    code, body := postJSON(t, svc, "PutRecord", map[string]any{
        "StreamName":   streamName,
        "PartitionKey": partitionKey,
        "Data":         base64.StdEncoding.EncodeToString(data),
    })
    if code != http.StatusOK {
        t.Fatalf("PutRecord: status %d body %s", code, body)
    }
    var out prOut
    mustDecode(t, body, &out)
    if out.ShardId == "" || out.SequenceNumber == "" {
        t.Fatalf("PutRecord missing output fields: %s", body)
    }
    return out
}

func getRecordsReq(t *testing.T, svc *Service, iterator string, limit int) (int, []byte, grOut) {
    t.Helper()
    req := map[string]any{"ShardIterator": iterator}
    if limit > 0 {
        req["Limit"] = limit
    }
    code, body := postJSON(t, svc, "GetRecords", req)
    var out grOut
    mustDecode(t, body, &out)
    return code, body, out
}

func expectedShardID(t *testing.T, svc *Service, streamName, partitionKey string) string {
    t.Helper()
    svc.mu.RLock()
    defer svc.mu.RUnlock()
    st, ok := svc.streams[streamName]
    if !ok {
        t.Fatalf("stream %q not found", streamName)
    }
    return st.shards[routeIndex(st, partitionKey, "")].id
}

// ---------------------------------------------------------------------------
// JSON protocol: PutRecord / GetRecords
// ---------------------------------------------------------------------------

func TestGetRecordsAndPutRecordJSON(t *testing.T) {
    svc := NewService()
    createStream(t, svc, "app", 3)

    code, body := postJSON(t, svc, "CreateStream", map[string]any{"StreamName": "app"})
    if code != http.StatusBadRequest || errorCodeOf(t, body) != "ResourceInUseException" {
        t.Fatalf("duplicate create: status %d body %s", code, body)
    }

    var desc struct {
        StreamDescription struct {
            StreamName   string `json:"StreamName"`
            StreamARN    string `json:"StreamARN"`
            StreamStatus string `json:"StreamStatus"`
            Shards       []struct {
                ShardId      string `json:"ShardId"`
                HashKeyRange struct {
                    StartingHashKey string `json:"StartingHashKey"`
                    EndingHashKey   string `json:"EndingHashKey"`
                } `json:"HashKeyRange"`
            } `json:"Shards"`
            HasMoreShards bool `json:"HasMoreShards"`
        } `json:"StreamDescription"`
    }
    code, body = postJSON(t, svc, "DescribeStream", map[string]any{"StreamName": "app"})
    if code != http.StatusOK {
        t.Fatalf("DescribeStream: status %d body %s", code, body)
    }
    mustDecode(t, body, &desc)
    d := desc.StreamDescription
    if d.StreamName != "app" || d.StreamStatus != "ACTIVE" || len(d.Shards) != 3 {
        t.Fatalf("bad description: %+v", d)
    }
    if !strings.Contains(d.StreamARN, "kinesis") || !strings.Contains(d.StreamARN, "stream/app") {
        t.Errorf("bad ARN: %s", d.StreamARN)
    }
    start0, endLast := hashRange(0, 3)
    if d.Shards[0].HashKeyRange.StartingHashKey != start0.Text(10) {
        t.Errorf("shard0 start: got %s want %s", d.Shards[0].HashKeyRange.StartingHashKey, start0.Text(10))
    }
    if d.Shards[2].HashKeyRange.EndingHashKey != endLast.Text(10) {
        t.Errorf("last shard end mismatch")
    }

    rec := putRecord(t, svc, "app", "alpha-key", []byte("alpha-payload"))
    wantShard := expectedShardID(t, svc, "app", "alpha-key")
    if rec.ShardId != wantShard {
        t.Fatalf("routed %s, want %s", rec.ShardId, wantShard)
    }
    if len(rec.SequenceNumber) != seqWidth {
        t.Errorf("sequence format: %q", rec.SequenceNumber)
    }

    it := getIterator(t, svc, "app", rec.ShardId, "TRIM_HORIZON")
    code, _, out := getRecordsReq(t, svc, it, 0)
    if code != http.StatusOK {
        t.Fatalf("GetRecords status %d", code)
    }
    if len(out.Records) != 1 {
        t.Fatalf("expected 1 record, got %d", len(out.Records))
    }
    r0 := out.Records[0]
    if string(r0.Data) != "alpha-payload" {
        t.Errorf("base64 data mismatch: %q", string(r0.Data))
    }
    if r0.PartitionKey != "alpha-key" {
        t.Errorf("partition key mismatch: %q", r0.PartitionKey)
    }
    if r0.SequenceNumber != rec.SequenceNumber {
        t.Errorf("sequence mismatch")
    }
    if r0.ApproximateArrivalTimestamp <= 0 {
        t.Errorf("missing arrival timestamp")
    }
    if out.NextShardIterator == "" {
        t.Errorf("missing NextShardIterator")
    }
    if out.MillisBehindLatest < 0 {
        t.Errorf("negative MillisBehindLatest")
    }
}

func TestGetRecordsPaginationAndContinuation(t *testing.T) {
    svc := NewService()
    createStream(t, svc, "one", 1)

    want := make([]string, 0, 5)
    for i := 0; i < 5; i++ {
        data := fmt.Sprintf("payload-%d", i)
        putRecord(t, svc, "one", "pk", []byte(data))
        want = append(want, data)
    }

    it := getIterator(t, svc, "one", "shardId-000000000000", "TRIM_HORIZON")
    got := make([]string, 0, 5)
    var lastSeqs []string
    for len(got) < len(want) {
        _, _, out := getRecordsReq(t, svc, it, 2)
        if out.NextShardIterator == "" {
            t.Fatal("NextShardIterator missing while stream is open")
        }
        for _, r := range out.Records {
            got = append(got, string(r.Data))
            lastSeqs = append(lastSeqs, r.SequenceNumber)
        }
        it = out.NextShardIterator
    }
    if len(got) != len(want) {
        t.Fatalf("got %d records, want %d", len(got), len(want))
    }
    for i := range want {
        if got[i] != want[i] {
            t.Errorf("record %d: got %q want %q", i, got[i], want[i])
        }
    }
    for i := 1; i < len(lastSeqs); i++ {
        if seqBig(lastSeqs[i]).Cmp(seqBig(lastSeqs[i-1])) <= 0 {
            t.Errorf("sequence numbers not increasing: %s then %s", lastSeqs[i-1], lastSeqs[i])
        }
    }

    _, _, out := getRecordsReq(t, svc, it, 2)
    if len(out.Records) != 0 {
        t.Errorf("expected exhausted read, got %d records", len(out.Records))
    }
    if out.NextShardIterator == "" {
        t.Fatalf("exhausted read must still return NextShardIterator")
    }
    it = out.NextShardIterator
    putRecord(t, svc, "one", "pk2", []byte("late"))
    _, _, out = getRecordsReq(t, svc, it, 2)
    if len(out.Records) != 1 || string(out.Records[0].Data) != "late" {
        t.Fatalf("continuation after idle: %+v", out.Records)
    }

    _, body := postJSON(t, svc, "GetRecords", map[string]any{"ShardIterator": it, "Limit": 10001})
    if errorCodeOf(t, body) != "InvalidArgumentException" {
        t.Fatalf("Limit>10000 must fail, got %s", body)
    }
}

func TestIteratorPositions(t *testing.T) {
    svc := NewService()
    createStream(t, svc, "pos", 1)
    shard := "shardId-000000000000"

    putRecord(t, svc, "pos", "a", []byte("old-1"))
    putRecord(t, svc, "pos", "b", []byte("old-2"))

    _, _, trim := getRecordsReq(t, svc, getIterator(t, svc, "pos", shard, "TRIM_HORIZON"), 0)
    if len(trim.Records) != 2 {
        t.Fatalf("TRIM_HORIZON: %d records", len(trim.Records))
    }
    firstSeq := trim.Records[0].SequenceNumber

    latest := getIterator(t, svc, "pos", shard, "LATEST")
    putRecord(t, svc, "pos", "c", []byte("new-1"))
    _, _, outLatest := getRecordsReq(t, svc, latest, 0)
    if len(outLatest.Records) != 1 || string(outLatest.Records[0].Data) != "new-1" {
        t.Fatalf("LATEST: %+v", outLatest.Records)
    }

    atReq := map[string]any{
        "StreamName":              "pos",
        "ShardId":                 shard,
        "ShardIteratorType":       "AT_SEQUENCE_NUMBER",
        "StartingSequenceNumber":  firstSeq,
    }
    _, _, outAt := getRecordsReq(t, svc, iteratorWith(t, svc, atReq), 0)
    if len(outAt.Records) != 2 {
        t.Fatalf("AT_SEQUENCE_NUMBER: %d records", len(outAt.Records))
    }
    atReq["ShardIteratorType"] = "AFTER_SEQUENCE_NUMBER"
    _, _, outAfter := getRecordsReq(t, svc, iteratorWith(t, svc, atReq), 0)
    if len(outAfter.Records) != 1 || string(outAfter.Records[0].Data) != "old-2" {
        t.Fatalf("AFTER_SEQUENCE_NUMBER: %+v", outAfter.Records)
    }

    tsReq := map[string]any{
        "StreamName":        "pos",
        "ShardId":           shard,
        "ShardIteratorType": "AT_TIMESTAMP",
        "Timestamp":         float64(time.Now().Add(-time.Hour).Unix()),
    }
    _, _, outTs := getRecordsReq(t, svc, iteratorWith(t, svc, tsReq), 0)
    if len(outTs.Records) != 3 {
        t.Fatalf("AT_TIMESTAMP: %d records", len(outTs.Records))
    }

    _, body := postJSON(t, svc, "GetShardIterator", map[string]any{
        "StreamName": "pos", "ShardId": shard, "ShardIteratorType": "NOPE",
    })
    if errorCodeOf(t, body) != "InvalidArgumentException" {
        t.Fatalf("invalid iterator type: %s", body)
    }
}

func TestExpiredIterator(t *testing.T) {
    svc := NewService()
    createStream(t, svc, "gone", 1)
    it := getIterator(t, svc, "gone", "shardId-000000000000", "TRIM_HORIZON")
    code, body := postJSON(t, svc, "DeleteStream", map[string]any{"StreamName": "gone"})
    if code != http.StatusOK {
        t.Fatalf("DeleteStream: %s", body)
    }
    code, body = postJSON(t, svc, "GetRecords", map[string]any{"ShardIterator": it})
    if code != http.StatusBadRequest || errorCodeOf(t, body) != "ExpiredIteratorException" {
        t.Fatalf("after delete: status %d body %s", code, body)
    }
    code, body = postJSON(t, svc, "GetRecords", map[string]any{"ShardIterator": "not!!!a!!!token"})
    if code != http.StatusBadRequest || errorCodeOf(t, body) != "ExpiredIteratorException" {
        t.Fatalf("bogus token: status %d body %s", code, body)
    }
}

// ---------------------------------------------------------------------------
// Deterministic partition-key routing
// ---------------------------------------------------------------------------

func TestDeterministicShardRouting(t *testing.T) {
    svc := NewService()
    createStream(t, svc, "route", 4)

    for _, key := range []string{"a", "b", "zzz", "kinesis", "partition-key-1234", "user-42"} {
        sum := md5.Sum([]byte(key))
        h := new(big.Int).SetBytes(sum[:])
        want := -1
        for i := 0; i < 4; i++ {
            s, e := hashRange(i, 4)
            if h.Cmp(s) >= 0 && h.Cmp(e) <= 0 {
                want = i
                break
            }
        }
        if want < 0 {
            t.Fatalf("hash outside all ranges for %q", key)
        }
        first := putRecord(t, svc, "route", key, []byte("d1"))
        second := putRecord(t, svc, "route", key, []byte("d2"))
        if first.ShardId != second.ShardId {
            t.Errorf("key %q inconsistent: %s vs %s", key, first.ShardId, second.ShardId)
        }
        if expected := fmt.Sprintf("shardId-%012d", want); first.ShardId != expected {
            t.Errorf("key %q: got %s want %s", key, first.ShardId, expected)
        }
    }

    var out prOut
    code, body := postJSON(t, svc, "PutRecord", map[string]any{
        "StreamName":      "route",
        "PartitionKey":    "ignored-key",
        "ExplicitHashKey": "0",
        "Data":            base64.StdEncoding.EncodeToString([]byte("explicit")),
    })
    if code != http.StatusOK {
        t.Fatalf("explicit hash key: status %d body %s", code, body)
    }
    mustDecode(t, body, &out)
    if out.ShardId != "shardId-000000000000" {
        t.Errorf("ExplicitHashKey 0 must land on shard 0, got %s", out.ShardId)
    }
}

// ---------------------------------------------------------------------------
// PutRecords (batch 500)
// ---------------------------------------------------------------------------

func TestPutRecordsBatch(t *testing.T) {
    svc := NewService()
    createStream(t, svc, "batch", 2)

    var records []map[string]any
    for i := 0; i < 5; i++ {
        records = append(records, map[string]any{
            "PartitionKey": fmt.Sprintf("pk-%d", i),
            "Data":         base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("rec-%d", i))),
        })
    }
    code, body := postJSON(t, svc, "PutRecords", map[string]any{"StreamName": "batch", "Records": records})
    if code != http.StatusOK {
        t.Fatalf("PutRecords status %d body %s", code, body)
    }
    var out struct {
        FailedRecordCount int `json:"FailedRecordCount"`
        Records           []struct {
            SequenceNumber string `json:"SequenceNumber"`
            ShardId        string `json:"ShardId"`
            ErrorCode      string `json:"ErrorCode"`
            ErrorMessage   string `json:"ErrorMessage"`
        } `json:"Records"`
    }
    mustDecode(t, body, &out)
    if out.FailedRecordCount != 0 {
        t.Errorf("unexpected failures: %+v", out.Records)
    }
    if len(out.Records) != 5 {
        t.Fatalf("got %d result entries", len(out.Records))
    }
    for i, r := range out.Records {
        if r.ErrorCode != "" || r.ShardId == "" || r.SequenceNumber == "" {
            t.Errorf("bad entry %d: %+v", i, r)
        }
        if expected := expectedShardID(t, svc, "batch", fmt.Sprintf("pk-%d", i)); r.ShardId != expected {
            t.Errorf("entry %d routed to %s, want %s", i, r.ShardId, expected)
        }
    }

    mixed := []map[string]any{
        {"PartitionKey": "ok", "Data": base64.StdEncoding.EncodeToString([]byte("good"))},
        {"PartitionKey": strings.Repeat("x", 300), "Data": base64.StdEncoding.EncodeToString([]byte("bad"))},
    }
    code, body = postJSON(t, svc, "PutRecords", map[string]any{"StreamName": "batch", "Records": mixed})
    if code != http.StatusOK {
        t.Fatalf("partial failure status %d", code)
    }
    mustDecode(t, body, &out)
    if out.FailedRecordCount != 1 {
        t.Errorf("FailedRecordCount = %d, want 1", out.FailedRecordCount)
    }
    if out.Records[0].ErrorCode != "" {
        t.Errorf("good record failed: %+v", out.Records[0])
    }
    if out.Records[1].ErrorCode == "" {
        t.Errorf("bad record did not fail: %+v", out.Records[1])
    }

    var bulk []map[string]any
    for i := 0; i < 501; i++ {
        bulk = append(bulk, map[string]any{
            "PartitionKey": "k",
            "Data":         base64.StdEncoding.EncodeToString([]byte("d")),
        })
    }
    code, body = postJSON(t, svc, "PutRecords", map[string]any{"StreamName": "batch", "Records": bulk})
    if code != http.StatusBadRequest || errorCodeOf(t, body) != "LimitExceededException" {
        t.Fatalf("over-limit batch: status %d body %s", code, body)
    }
}

func TestPutRecordValidation(t *testing.T) {
    svc := NewService()
    createStream(t, svc, "v", 1)

    _, body := postJSON(t, svc, "PutRecord", map[string]any{"StreamName": "v", "Data": "eA=="})
    if errorCodeOf(t, body) != "InvalidArgumentException" {
        t.Fatalf("missing partition key: %s", body)
    }
    _, body = postJSON(t, svc, "PutRecord", map[string]any{
        "StreamName": "v", "PartitionKey": strings.Repeat("k", 257), "Data": "eA==",
    })
    if errorCodeOf(t, body) != "InvalidArgumentException" {
        t.Fatalf("oversized partition key: %s", body)
    }
    _, body = postJSON(t, svc, "PutRecord", map[string]any{
        "StreamName": "v", "PartitionKey": "k",
        "Data": base64.StdEncoding.EncodeToString(make([]byte, maxRecordDataBytes+1)),
    })
    if errorCodeOf(t, body) != "InvalidArgumentException" {
        t.Fatalf("oversized data: %s", body)
    }
    _, body = postJSON(t, svc, "PutRecord", map[string]any{
        "StreamName": "does-not-exist", "PartitionKey": "k", "Data": "eA==",
    })
    if errorCodeOf(t, body) != "ResourceNotFoundException" {
        t.Fatalf("missing stream: %s", body)
    }
}

// ---------------------------------------------------------------------------
// Event-stream wire variant (the SDK gotcha)
// ---------------------------------------------------------------------------

func TestEventStreamWirePutRecord(t *testing.T) {
    svc := NewService()
    createStream(t, svc, "es", 2)

    frame := encodeESMessage([]esHeader{
        esStr(":message-type", "request"),
        esStr(":event-type", "PutRecord"),
        esStr("stream-name", "es"),
        esStr("partition-key", "pk-es"),
    }, []byte("evt-data"))
    w := doReq(t, svc, map[string]string{"Content-Type": "application/vnd.amazon.eventstream"}, frame)
    if w.Code != http.StatusOK {
        t.Fatalf("event-stream PutRecord status %d body %q", w.Code, w.Body.String())
    }
    if ct := w.Header().Get("Content-Type"); ct != "application/vnd.amazon.eventstream" {
        t.Fatalf("response content-type %q", ct)
    }
    msgs, err := decodeEventStream(w.Body.Bytes())
    if err != nil {
        t.Fatalf("decode response: %v", err)
    }
    resp := msgs[len(msgs)-1]
    if v, _ := resp.str(":event-type"); v != "PutRecord" {
        t.Fatalf("unexpected :event-type %q", v)
    }
    var out map[string]any
    if err := json.Unmarshal(resp.Payload, &out); err != nil {
        t.Fatalf("payload not json: %s", resp.Payload)
    }
    shardID, _ := out["ShardId"].(string)
    if shardID == "" {
        t.Fatalf("missing ShardId: %v", out)
    }
    if want := expectedShardID(t, svc, "es", "pk-es"); shardID != want {
        t.Fatalf("routing mismatch: %s vs %s", shardID, want)
    }

    _, _, gr := getRecordsReq(t, svc, getIterator(t, svc, "es", shardID, "TRIM_HORIZON"), 0)
    if len(gr.Records) != 1 {
        t.Fatalf("records: %d", len(gr.Records))
    }
    if string(gr.Records[0].Data) != "evt-data" {
        t.Errorf("data mismatch: %q", string(gr.Records[0].Data))
    }
    if gr.Records[0].PartitionKey != "pk-es" {
        t.Errorf("partition key mismatch: %q", gr.Records[0].PartitionKey)
    }
}

func TestEventStreamPutRecordsAndJSONPayload(t *testing.T) {
    svc := NewService()
    createStream(t, svc, "esb", 1)

    arrayPayload, _ := json.Marshal([]map[string]any{
        {"PartitionKey": "k1", "Data": base64.StdEncoding.EncodeToString([]byte("r1"))},
        {"PartitionKey": "k2", "Data": base64.StdEncoding.EncodeToString([]byte("r2"))},
    })
    frame := encodeESMessage([]esHeader{
        esStr(":event-type", "PutRecords"),
        esStr("stream-name", "esb"),
    }, arrayPayload)
    w := doReq(t, svc, map[string]string{"Content-Type": "application/vnd.amazon.eventstream"}, frame)
    if w.Code != http.StatusOK {
        t.Fatalf("event-stream PutRecords status %d body %q", w.Code, w.Body.String())
    }
    msgs, err := decodeEventStream(w.Body.Bytes())
    if err != nil {
        t.Fatalf("decode response: %v", err)
    }
    var batchOut struct {
        FailedRecordCount int `json:"FailedRecordCount"`
        Records           []struct {
            SequenceNumber string `json:"SequenceNumber"`
            ShardId        string `json:"ShardId"`
        } `json:"Records"`
    }
    if err := json.Unmarshal(msgs[len(msgs)-1].Payload, &batchOut); err != nil {
        t.Fatalf("bad payload: %s", msgs[len(msgs)-1].Payload)
    }
    if batchOut.FailedRecordCount != 0 || len(batchOut.Records) != 2 {
        t.Fatalf("bad batch result: %+v", batchOut)
    }

    objPayload, _ := json.Marshal(map[string]any{
        "StreamName":   "esb",
        "PartitionKey": "kj",
        "Data":         base64.StdEncoding.EncodeToString([]byte("json-data")),
    })
    frame = encodeESMessage([]esHeader{esStr(":event-type", "PutRecord")}, objPayload)
    w = doReq(t, svc, map[string]string{}, frame)
    if w.Code != http.StatusOK {
        t.Fatalf("JSON-payload PutRecord status %d body %q", w.Code, w.Body.String())
    }
    msgs, err = decodeEventStream(w.Body.Bytes())
    if err != nil {
        t.Fatalf("decode response: %v", err)
    }
    var po prOut
    if err := json.Unmarshal(msgs[len(msgs)-1].Payload, &po); err != nil {
        t.Fatalf("bad payload: %s", msgs[len(msgs)-1].Payload)
    }
    if po.ShardId == "" || po.SequenceNumber == "" {
        t.Fatal("missing PutRecord output")
    }

    _, _, gr := getRecordsReq(t, svc, getIterator(t, svc, "esb", "shardId-000000000000", "TRIM_HORIZON"), 0)
    found := false
    for _, r := range gr.Records {
        if string(r.Data) == "json-data" {
            found = true
        }
    }
    if !found {
        t.Fatal("json payload record missing")
    }
}

func TestEventStreamMissingPartitionKeyException(t *testing.T) {
    svc := NewService()
    createStream(t, svc, "esx", 1)
    frame := encodeESMessage([]esHeader{
        esStr(":event-type", "PutRecord"),
        esStr("stream-name", "esx"),
    }, []byte("no-pk-here"))
    w := doReq(t, svc, map[string]string{"Content-Type": "application/vnd.amazon.eventstream"}, frame)
    if w.Code != http.StatusBadRequest {
        t.Fatalf("status %d body %q", w.Code, w.Body.String())
    }
    msgs, err := decodeEventStream(w.Body.Bytes())
    if err != nil {
        t.Fatalf("decode response: %v", err)
    }
    resp := msgs[len(msgs)-1]
    mt, _ := resp.str(":message-type")
    et, _ := resp.str(":exception-type")
    if mt != "exception" || et != "InvalidArgumentException" {
        t.Fatalf("headers: %+v", resp.Headers)
    }
}

// ---------------------------------------------------------------------------
// SigV4
// ---------------------------------------------------------------------------

func signedHeaders(t *testing.T, access, secret string, body []byte) map[string]string {
    t.Helper()
    region, service := "us-east-1", "kinesis"
    amzDate := time.Now().UTC().Format("20060102T150405Z")
    date := amzDate[:8]
    payloadHash := fmt.Sprintf("%x", sha256.Sum256(body))
    signedList := "host;x-amz-date"
    canonical := strings.Join([]string{
        http.MethodPost,
        "/",
        "",
        "host:example.com\nx-amz-date:" + amzDate + "\n",
        signedList,
        payloadHash,
    }, "\n")
    sts := strings.Join([]string{
        "AWS4-HMAC-SHA256",
        amzDate,
        date + "/" + region + "/" + service + "/aws4_request",
        fmt.Sprintf("%x", sha256.Sum256([]byte(canonical))),
    }, "\n")
    sig := sigv4Signature(secret, date, region, service, sts)
    return map[string]string{
        "X-Amz-Date": amzDate,
        "Authorization": fmt.Sprintf(
            "AWS4-HMAC-SHA256 Credential=%s/%s/%s/%s/aws4_request, SignedHeaders=%s, Signature=%s",
            access, date, region, service, signedList, sig),
    }
}

func TestSigV4Enforcement(t *testing.T) {
    svc := NewService(WithEnforceSigV4(true), WithCredentials("test", "test"), WithRegion("us-east-1"))
    payload, _ := json.Marshal(map[string]any{"StreamName": "sv", "ShardCount": 1})
    base := map[string]string{
        "X-Amz-Target": "Kinesis_20131202.CreateStream",
        "Content-Type": "application/x-amz-json-1.1",
    }

    w := doReq(t, svc, base, payload)
    if w.Code != http.StatusForbidden {
        t.Fatalf("unsigned request: status %d body %s", w.Code, w.Body.String())
    }

    signed := signedHeaders(t, "test", "test", payload)
    for k, v := range base {
        signed[k] = v
    }
    w = doReq(t, svc, signed, payload)
    if w.Code != http.StatusOK {
        t.Fatalf("signed request: status %d body %s", w.Code, w.Body.String())
    }

    bad := signedHeaders(t, "test", "not-the-secret", payload)
    for k, v := range base {
        bad[k] = v
    }
    w = doReq(t, svc, bad, payload)
    if w.Code != http.StatusForbidden {
        t.Fatalf("bad signature: status %d", w.Code)
    }

    w = doReq(t, NewService(), base, payload)
    if w.Code != http.StatusOK {
        t.Fatalf("lenient default must accept unsigned: status %d body %s", w.Code, w.Body.String())
    }
}

func TestSigV4PresignedQuery(t *testing.T) {
    svc := NewService(WithEnforceSigV4(true), WithCredentials("test", "test"), WithRegion("us-east-1"))
    date := "20250101"
    amzDate := "20250101T000000Z"
    q := url.Values{}
    q.Set("Action", "PutRecord")
    q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
    q.Set("X-Amz-Credential", "test/20250101/us-east-1/kinesis/aws4_request")
    q.Set("X-Amz-Date", amzDate)
    q.Set("X-Amz-Expires", "30")
    q.Set("X-Amz-SignedHeaders", "host")
    canonical := strings.Join([]string{
        http.MethodPost,
        "/",
        canonicalQueryValues(q),
        "host:example.com\n",
        "host",
        "UNSIGNED-PAYLOAD",
    }, "\n")
    sts := strings.Join([]string{
        "AWS4-HMAC-SHA256",
        amzDate,
        date + "/us-east-1/kinesis/aws4_request",
        fmt.Sprintf("%x", sha256.Sum256([]byte(canonical))),
    }, "\n")
    q.Set("X-Amz-Signature", sigv4Signature("test", date, "us-east-1", "kinesis", sts))
    body, _ := json.Marshal(map[string]any{"StreamName": "pv", "ShardCount": 1})
    r := httptest.NewRequest(http.MethodPost, "/?"+q.Encode(), bytes.NewReader(body))
    r.Header.Set("X-Amz-Target", "Kinesis_20131202.CreateStream")
    w := httptest.NewRecorder()
    svc.ServeHTTP(w, r)
    if w.Code != http.StatusOK {
        t.Fatalf("presigned status %d body %s", w.Code, w.Body.String())
    }
}
