// Package kinesis is the in-memory Amazon Kinesis Data Streams emulation
// backend for floci-go.
//
// Wire formats:
//
//   - Standard Kinesis JSON: POST with "X-Amz-Target:
//     Kinesis_20131202.<Operation>" and "Content-Type:
//     application/x-amz-json-1.1". Record Data fields are base64 both ways.
//
//   - Amazon Event Stream framing (the interop gotcha): some AWS SDKs marshal
//     unary Kinesis calls as event-stream messages (Content-Type
//     application/vnd.amazon.eventstream, possibly SigV4-signed). The server
//     detects the framing by content type, by the
//     "X-Amzn-Serialize-Event-Stream" header, or by a body whose first bytes
//     validate as an event-stream prelude, recovers the operation from the
//     ":event-type"/":message-type" headers (falling back to X-Amz-Target or
//     JSON shape inference), reads parameters from extra headers (stream-name,
//     partition-key, ...) or from a JSON payload, and answers with an
//     event-stream message carrying the JSON result (plus per-field headers
//     such as "shard-id"/"sequence-number").
//
// SigV4: when created with WithEnforceSigV4(true), header-signed and
// presigned requests are verified (the payload hash covers the exact wire
// body, event-stream framed or not); by default any or no credentials are
// accepted, matching LocalStack-style leniency.
package kinesis

import (
    "bytes"
    "context"
    "crypto/hmac"
    "crypto/md5"
    "crypto/sha256"
    "encoding/base64"
    "encoding/binary"
    "encoding/hex"
    "encoding/json"
    "errors"
    "fmt"
    "hash/crc32"
    "io"
    "math/big"
    "net/http"
    "net/url"
    "sort"
    "strconv"
    "strings"
    "sync"
    "time"
)

const (
    contentTypeJSON            = "application/x-amz-json-1.1"
    contentTypeEventStream     = "application/vnd.amazon.eventstream"
    serializeEventStreamHeader = "X-Amzn-Serialize-Event-Stream"

    // targetPrefix is informational; X-Amz-Target values are parsed generically
    // (everything after the last "." is treated as the operation name).
    targetPrefix = "Kinesis_20131202."

    maxRecordsPerPut     = 500
    maxTotalPutBytes     = 5 * 1024 * 1024
    maxRecordDataBytes   = 1024 * 1024
    maxPartitionKeyBytes = 256
    maxGetRecordsLimit   = 10000
    defaultShardCount    = 4
    seqWidth             = 56

    encryptionNone = "NONE"
    iterMagic      = "floci-kinesis-iterator-v1"
)

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

type apiError struct {
    Code    string
    Message string
    Status  int
}

func (e *apiError) Error() string { return e.Code + ": " + e.Message }

func invalidArgument(format string, args ...any) *apiError {
    return &apiError{Code: "InvalidArgumentException", Message: fmt.Sprintf(format, args...), Status: http.StatusBadRequest}
}

func resourceNotFound(format string, args ...any) *apiError {
    return &apiError{Code: "ResourceNotFoundException", Message: fmt.Sprintf(format, args...), Status: http.StatusBadRequest}
}

func resourceInUse(format string, args ...any) *apiError {
    return &apiError{Code: "ResourceInUseException", Message: fmt.Sprintf(format, args...), Status: http.StatusBadRequest}
}

func limitExceeded(format string, args ...any) *apiError {
    return &apiError{Code: "LimitExceededException", Message: fmt.Sprintf(format, args...), Status: http.StatusBadRequest}
}

func expiredIterator(format string, args ...any) *apiError {
    return &apiError{Code: "ExpiredIteratorException", Message: fmt.Sprintf(format, args...), Status: http.StatusBadRequest}
}

// ---------------------------------------------------------------------------
// Service state
// ---------------------------------------------------------------------------

type record struct {
    seq     string
    data    []byte
    pk      string
    arrived time.Time
}

type shard struct {
    id      string
    records []record
    seq     int64
}

type stream struct {
    name   string
    arn    string
    status string
    shards []*shard
}

type Service struct {
    mu           sync.RWMutex
    streams      map[string]*stream
    region       string
    account      string
    accessKey    string
    secretKey    string
    enforceSigV4 bool
    now          func() time.Time
}

var _ http.Handler = (*Service)(nil)

type Option func(*Service)

func WithRegion(region string) Option {
    return func(s *Service) {
        if region != "" {
            s.region = region
        }
    }
}

func WithAccountID(account string) Option {
    return func(s *Service) {
        if account != "" {
            s.account = account
        }
    }
}

func WithCredentials(accessKey, secretKey string) Option {
    return func(s *Service) {
        if accessKey != "" {
            s.accessKey = accessKey
        }
        if secretKey != "" {
            s.secretKey = secretKey
        }
    }
}

func WithEnforceSigV4(enforce bool) Option {
    return func(s *Service) { s.enforceSigV4 = enforce }
}

func WithClock(clock func() time.Time) Option {
    return func(s *Service) {
        if clock != nil {
            s.now = clock
        }
    }
}

func NewService(opts ...Option) *Service {
    svc := &Service{
        streams:   make(map[string]*stream),
        region:    "us-east-1",
        account:   "000000000000",
        accessKey: "test",
        secretKey: "test",
        now:       func() time.Time { return time.Now().UTC() },
    }
    for _, opt := range opts {
        opt(svc)
    }
    return svc
}

// ---------------------------------------------------------------------------
// Deterministic partition-key -> shard routing (MD5 key space, like AWS)
// ---------------------------------------------------------------------------

var hashModulus = new(big.Int).Lsh(big.NewInt(1), 128) // 2^128

func md5Key(s string) *big.Int {
    sum := md5.Sum([]byte(s))
    return new(big.Int).SetBytes(sum[:])
}

func hashRange(index, total int) (*big.Int, *big.Int) {
    if total <= 0 {
        return big.NewInt(0), new(big.Int).Sub(hashModulus, big.NewInt(1))
    }
    step := new(big.Int).Div(hashModulus, big.NewInt(int64(total)))
    start := new(big.Int).Mul(step, big.NewInt(int64(index)))
    var end *big.Int
    if index == total-1 {
        end = new(big.Int).Sub(hashModulus, big.NewInt(1))
    } else {
        end = new(big.Int).Sub(new(big.Int).Mul(step, big.NewInt(int64(index+1))), big.NewInt(1))
    }
    return start, end
}

// routeHash: ExplicitHashKey (numeric or, leniently, hashed) overrides the
// partition-key MD5, exactly as Kinesis documents.
func routeHash(partitionKey, explicitHashKey string) *big.Int {
    if explicitHashKey != "" {
        if v, ok := new(big.Int).SetString(explicitHashKey, 10); ok {
            return v
        }
        return md5Key(explicitHashKey)
    }
    return md5Key(partitionKey)
}

func (s *stream) shardIndexFor(h *big.Int) int {
    total := len(s.shards)
    for i := 0; i < total; i++ {
        start, end := hashRange(i, total)
        if h.Cmp(start) >= 0 && h.Cmp(end) <= 0 {
            return i
        }
    }
    return total - 1
}

func routeIndex(s *stream, partitionKey, explicitHashKey string) int {
    return s.shardIndexFor(routeHash(partitionKey, explicitHashKey))
}

func (s *stream) findShard(id string) *shard {
    for _, sh := range s.shards {
        if sh.id == id {
            return sh
        }
    }
    return nil
}

func formatSeq(shardIndex int, n int64) string {
    off := new(big.Int).Mul(big.NewInt(int64(shardIndex)), new(big.Int).Exp(big.NewInt(10), big.NewInt(40), nil))
    v := new(big.Int).Add(off, big.NewInt(n))
    txt := v.Text(10)
    for len(txt) < seqWidth {
        txt = "0" + txt
    }
    return txt
}

func seqBig(s string) *big.Int {
    if v, ok := new(big.Int).SetString(strings.TrimSpace(s), 10); ok {
        return v
    }
    return big.NewInt(0)
}

// ---------------------------------------------------------------------------
// Shard iterator tokens
// ---------------------------------------------------------------------------

type iteratorToken struct {
    Magic  string  `json:"m"`
    Stream string  `json:"s"`
    Shard  string  `json:"h"`
    Kind   string  `json:"k"` // trim | latest | at | after | ts | index
    Seq    string  `json:"q,omitempty"`
    Index  int64   `json:"i,omitempty"`
    Ts     float64 `json:"t,omitempty"`
}

func encodeIterator(t iteratorToken) string {
    t.Magic = iterMagic
    raw, _ := json.Marshal(t)
    return base64.StdEncoding.EncodeToString(raw)
}

func decodeIterator(tok string) (iteratorToken, error) {
    raw, err := base64.StdEncoding.DecodeString(tok)
    if err != nil {
        return iteratorToken{}, errors.New("malformed shard iterator")
    }
    var t iteratorToken
    if err := json.Unmarshal(raw, &t); err != nil || t.Magic != iterMagic {
        return iteratorToken{}, errors.New("malformed shard iterator")
    }
    return t, nil
}

func truncate(s string, n int) string {
    if len(s) <= n {
        return s
    }
    return s[:n] + "..."
}

// ---------------------------------------------------------------------------
// Wire API structs (Kinesis JSON encoding)
// ---------------------------------------------------------------------------

type StreamModeDetails struct {
    StreamMode string `json:"StreamMode,omitempty"`
}

type CreateStreamInput struct {
    StreamName        string             `json:"StreamName,omitempty"`
    StreamARN         string             `json:"StreamARN,omitempty"`
    ShardCount        int                `json:"ShardCount,omitempty"`
    StreamModeDetails *StreamModeDetails `json:"StreamModeDetails,omitempty"`
}

type DeleteStreamInput struct {
    StreamName              string `json:"StreamName,omitempty"`
    StreamARN               string `json:"StreamARN,omitempty"`
    EnforceConsumerDeletion bool   `json:"EnforceConsumerDeletion,omitempty"`
}

type DescribeStreamInput struct {
    StreamName            string `json:"StreamName,omitempty"`
    StreamARN             string `json:"StreamARN,omitempty"`
    ExclusiveStartShardId string `json:"ExclusiveStartShardId,omitempty"`
    Limit                 int    `json:"Limit,omitempty"`
}

type HashKeyRangeView struct {
    StartingHashKey string `json:"StartingHashKey"`
    EndingHashKey   string `json:"EndingHashKey"`
}

type SequenceNumberRangeView struct {
    StartingSequenceNumber string `json:"StartingSequenceNumber"`
    EndingSequenceNumber   string `json:"EndingSequenceNumber,omitempty"`
}

type ShardView struct {
    ShardId              string                 `json:"ShardId"`
    ParentShardId        string                 `json:"ParentShardId,omitempty"`
    AdjacentParentShardId string                `json:"AdjacentParentShardId,omitempty"`
    HashKeyRange         HashKeyRangeView       `json:"HashKeyRange"`
    SequenceNumberRange  SequenceNumberRangeView `json:"SequenceNumberRange"`
}

type StreamDescription struct {
    StreamName        string             `json:"StreamName"`
    StreamARN         string             `json:"StreamARN"`
    StreamStatus      string             `json:"StreamStatus"`
    StreamModeDetails *StreamModeDetails `json:"StreamModeDetails,omitempty"`
    Shards            []ShardView        `json:"Shards"`
    HasMoreShards     bool               `json:"HasMoreShards"`
}

type DescribeStreamOutput struct {
    StreamDescription StreamDescription `json:"StreamDescription"`
}

type ListShardsInput struct {
    StreamName string `json:"StreamName,omitempty"`
    StreamARN  string `json:"StreamARN,omitempty"`
}

type ListShardsOutput struct {
    Shards []ShardView `json:"Shards"`
}

type ListStreamsInput struct {
    ExclusiveStartStreamName string `json:"ExclusiveStartStreamName,omitempty"`
    Limit                    int    `json:"Limit,omitempty"`
    NextToken                string `json:"NextToken,omitempty"`
}

type ListStreamsOutput struct {
    StreamNames    []string `json:"StreamNames"`
    HasMoreStreams bool     `json:"HasMoreStreams"`
}

type GetShardIteratorInput struct {
    StreamName             string   `json:"StreamName,omitempty"`
    StreamARN              string   `json:"StreamARN,omitempty"`
    ShardId                string   `json:"ShardId,omitempty"`
    ShardIteratorType      string   `json:"ShardIteratorType,omitempty"`
    StartingSequenceNumber string   `json:"StartingSequenceNumber,omitempty"`
    Timestamp              *float64 `json:"Timestamp,omitempty"`
}

type GetShardIteratorOutput struct {
    ShardIterator string `json:"ShardIterator"`
}

type GetRecordsInput struct {
    ShardIterator string `json:"ShardIterator"`
    Limit         int    `json:"Limit,omitempty"`
    StreamARN     string `json:"StreamARN,omitempty"`
}

type RecordView struct {
    SequenceNumber              string  `json:"SequenceNumber"`
    ApproximateArrivalTimestamp float64 `json:"ApproximateArrivalTimestamp"`
    Data                        []byte  `json:"Data"` // base64 on the wire
    PartitionKey                string  `json:"PartitionKey"`
}

type GetRecordsOutput struct {
    Records           []RecordView `json:"Records"`
    NextShardIterator string       `json:"NextShardIterator"`
    MillisBehindLatest int64       `json:"MillisBehindLatest"`
}

type PutRecordInput struct {
    StreamName                string `json:"StreamName,omitempty"`
    StreamARN                 string `json:"StreamARN,omitempty"`
    Data                      []byte `json:"Data,omitempty"`
    PartitionKey              string `json:"PartitionKey,omitempty"`
    ExplicitHashKey           string `json:"ExplicitHashKey,omitempty"`
    SequenceNumberForOrdering string `json:"SequenceNumberForOrdering,omitempty"`
}

type PutRecordOutput struct {
    ShardId        string `json:"ShardId"`
    SequenceNumber string `json:"SequenceNumber"`
    EncryptionType string `json:"EncryptionType,omitempty"`
}

type PutRecordsRequestEntry struct {
    Data            []byte `json:"Data,omitempty"`
    ExplicitHashKey string `json:"ExplicitHashKey,omitempty"`
    PartitionKey    string `json:"PartitionKey,omitempty"`
}

type PutRecordsInput struct {
    StreamName string                    `json:"StreamName,omitempty"`
    StreamARN  string                    `json:"StreamARN,omitempty"`
    Records    []PutRecordsRequestEntry  `json:"Records"`
}

type PutRecordsResultEntry struct {
    SequenceNumber string `json:"SequenceNumber,omitempty"`
    ShardId        string `json:"ShardId,omitempty"`
    ErrorCode      string `json:"ErrorCode,omitempty"`
    ErrorMessage   string `json:"ErrorMessage,omitempty"`
}

type PutRecordsOutput struct {
    Records           []PutRecordsResultEntry `json:"Records"`
    FailedRecordCount int                     `json:"FailedRecordCount"`
}

func streamNameFrom(name, arn string) string {
    if name != "" {
        return name
    }
    if arn != "" {
        if i := strings.LastIndex(arn, "/"); i >= 0 {
            return arn[i+1:]
        }
        return arn
    }
    return ""
}

// resolveLocked expects svc.mu held (read or write).
func (svc *Service) resolveLocked(name, arn string) (*stream, *apiError) {
    name = streamNameFrom(name, arn)
    if name == "" {
        return nil, invalidArgument("StreamName or StreamARN is required.")
    }
    st, ok := svc.streams[name]
    if !ok {
        return nil, resourceNotFound("Stream %s under account %s not found.", name, svc.account)
    }
    if st.status != "ACTIVE" {
        return nil, resourceInUse("Stream %s is not ACTIVE.", name)
    }
    return st, nil
}

func (s *stream) shardViews() []ShardView {
    out := make([]ShardView, 0, len(s.shards))
    for i, sh := range s.shards {
        start, end := hashRange(i, len(s.shards))
        out = append(out, ShardView{
            ShardId: sh.id,
            HashKeyRange: HashKeyRangeView{
                StartingHashKey: start.Text(10),
                EndingHashKey:   end.Text(10),
            },
            SequenceNumberRange: SequenceNumberRangeView{StartingSequenceNumber: formatSeq(i, 0)},
        })
    }
    return out
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

func (svc *Service) opCreateStream(in CreateStreamInput) (any, *apiError) {
    name := streamNameFrom(in.StreamName, in.StreamARN)
    if name == "" {
        return nil, invalidArgument("StreamName or StreamARN is required.")
    }
    n := in.ShardCount
    if n <= 0 {
        n = defaultShardCount
    }
    if n > 100000 {
        return nil, limitExceeded("Requested shard count %d exceeds the limit.", n)
    }
    svc.mu.Lock()
    defer svc.mu.Unlock()
    if _, exists := svc.streams[name]; exists {
        return nil, resourceInUse("Stream %s already exists.", name)
    }
    st := &stream{
        name:   name,
        arn:    fmt.Sprintf("arn:aws:kinesis:%s:%s:stream/%s", svc.region, svc.account, name),
        status: "ACTIVE",
        shards: make([]*shard, n),
    }
    for i := 0; i < n; i++ {
        st.shards[i] = &shard{id: fmt.Sprintf("shardId-%012d", i)}
    }
    svc.streams[name] = st
    return struct{}{}, nil
}

func (svc *Service) opDeleteStream(in DeleteStreamInput) (any, *apiError) {
    svc.mu.Lock()
    defer svc.mu.Unlock()
    st, err := svc.resolveLocked(in.StreamName, in.StreamARN)
    if err != nil {
        return nil, err
    }
    delete(svc.streams, st.name)
    return struct{}{}, nil
}

func (svc *Service) opDescribeStream(in DescribeStreamInput) (any, *apiError) {
    svc.mu.RLock()
    defer svc.mu.RUnlock()
    st, err := svc.resolveLocked(in.StreamName, in.StreamARN)
    if err != nil {
        return nil, err
    }
    desc := StreamDescription{
        StreamName:        st.name,
        StreamARN:         st.arn,
        StreamStatus:      st.status,
        StreamModeDetails: &StreamModeDetails{StreamMode: "PROVISIONED"},
        Shards:            st.shardViews(),
        HasMoreShards:     false,
    }
    if in.ExclusiveStartShardId != "" {
        filtered := make([]ShardView, 0, len(desc.Shards))
        seen := false
        for _, s := range desc.Shards {
            if seen {
                filtered = append(filtered, s)
            }
            if s.ShardId == in.ExclusiveStartShardId {
                seen = true
            }
        }
        desc.Shards = filtered
    }
    return DescribeStreamOutput{StreamDescription: desc}, nil
}

func (svc *Service) opListShards(in ListShardsInput) (any, *apiError) {
    svc.mu.RLock()
    defer svc.mu.RUnlock()
    st, err := svc.resolveLocked(in.StreamName, in.StreamARN)
    if err != nil {
        return nil, err
    }
    return ListShardsOutput{Shards: st.shardViews()}, nil
}

func (svc *Service) opListStreams(in ListStreamsInput) (any, *apiError) {
    svc.mu.RLock()
    defer svc.mu.RUnlock()
    names := make([]string, 0, len(svc.streams))
    for name := range svc.streams {
        names = append(names, name)
    }
    sort.Strings(names)
    if in.ExclusiveStartStreamName != "" {
        i := sort.SearchStrings(names, in.ExclusiveStartStreamName)
        if i < len(names) && names[i] == in.ExclusiveStartStreamName {
            i++
        }
        names = names[i:]
    }
    hasMore := false
    if in.Limit > 0 && in.Limit < len(names) {
        names = names[:in.Limit]
        hasMore = true
    }
    return ListStreamsOutput{StreamNames: names, HasMoreStreams: hasMore}, nil
}

func (svc *Service) opGetShardIterator(in GetShardIteratorInput) (any, *apiError) {
    if in.ShardId == "" {
        return nil, invalidArgument("ShardId is required.")
    }
    if in.ShardIteratorType == "" {
        return nil, invalidArgument("ShardIteratorType is required.")
    }
    svc.mu.RLock()
    defer svc.mu.RUnlock()
    st, err := svc.resolveLocked(in.StreamName, in.StreamARN)
    if err != nil {
        return nil, err
    }
    sh := st.findShard(in.ShardId)
    if sh == nil {
        return nil, resourceNotFound("Shard %s in stream %s under account %s not found.", in.ShardId, st.name, svc.account)
    }
    var tok iteratorToken
    switch in.ShardIteratorType {
    case "TRIM_HORIZON":
        tok = iteratorToken{Kind: "trim"}
    case "LATEST":
        tok = iteratorToken{Kind: "index", Index: int64(len(sh.records))}
    case "AT_SEQUENCE_NUMBER":
        if in.StartingSequenceNumber == "" {
            return nil, invalidArgument("StartingSequenceNumber is required when ShardIteratorType is AT_SEQUENCE_NUMBER.")
        }
        tok = iteratorToken{Kind: "at", Seq: in.StartingSequenceNumber}
    case "AFTER_SEQUENCE_NUMBER":
        if in.StartingSequenceNumber == "" {
            return nil, invalidArgument("StartingSequenceNumber is required when ShardIteratorType is AFTER_SEQUENCE_NUMBER.")
        }
        tok = iteratorToken{Kind: "after", Seq: in.StartingSequenceNumber}
    case "AT_TIMESTAMP":
        if in.Timestamp == nil {
            return nil, invalidArgument("Timestamp is required when ShardIteratorType is AT_TIMESTAMP.")
        }
        tok = iteratorToken{Kind: "ts", Ts: *in.Timestamp}
    default:
        return nil, invalidArgument("Invalid ShardIteratorType %q.", in.ShardIteratorType)
    }
    tok.Stream = st.name
    tok.Shard = sh.id
    return GetShardIteratorOutput{ShardIterator: encodeIterator(tok)}, nil
}

func iterateStart(sh *shard, tok iteratorToken) (int, *apiError) {
    switch tok.Kind {
    case "trim", "":
        return 0, nil
    case "index":
        if tok.Index < 0 {
            return 0, nil
        }
        if tok.Index > int64(len(sh.records)) {
            return len(sh.records), nil
        }
        return int(tok.Index), nil
    case "at", "after":
        cutoff := seqBig(tok.Seq)
        for i := range sh.records {
            c := seqBig(sh.records[i].seq).Cmp(cutoff)
            if (tok.Kind == "at" && c >= 0) || (tok.Kind == "after" && c > 0) {
                return i, nil
            }
        }
        return len(sh.records), nil
    case "ts":
        after := time.UnixMilli(int64(tok.Ts * 1000)).UTC()
        for i := range sh.records {
            if !sh.records[i].arrived.Before(after) {
                return i, nil
            }
        }
        return len(sh.records), nil
    default:
        return 0, expiredIterator("ShardIterator is no longer valid.")
    }
}

func (svc *Service) opGetRecords(in GetRecordsInput) (any, *apiError) {
    if in.ShardIterator == "" {
        return nil, invalidArgument("ShardIterator is required.")
    }
    tok, terr := decodeIterator(in.ShardIterator)
    if terr != nil {
        return nil, expiredIterator("ShardIterator %s is invalid or has expired.", truncate(in.ShardIterator, 32))
    }
    limit := in.Limit
    if limit <= 0 {
        limit = maxGetRecordsLimit
    }
    if limit > maxGetRecordsLimit {
        return nil, invalidArgument("Limit must be between 1 and %d.", maxGetRecordsLimit)
    }
    svc.mu.RLock()
    defer svc.mu.RUnlock()
    st := svc.streams[tok.Stream]
    if st == nil {
        return nil, expiredIterator("ShardIterator expired: stream %s no longer exists.", tok.Stream)
    }
    sh := st.findShard(tok.Shard)
    if sh == nil {
        return nil, expiredIterator("ShardIterator expired: shard %s no longer exists.", tok.Shard)
    }
    start, serr := iterateStart(sh, tok)
    if serr != nil {
        return nil, serr
    }
    end := start + limit
    if end > len(sh.records) {
        end = len(sh.records)
    }
    out := GetRecordsOutput{
        Records: make([]RecordView, 0, end-start),
        NextShardIterator: encodeIterator(iteratorToken{
            Stream: st.name,
            Shard:  sh.id,
            Kind:   "index",
            Index:  int64(end),
        }),
    }
    for _, r := range sh.records[start:end] {
        out.Records = append(out.Records, RecordView{
            SequenceNumber:              r.seq,
            ApproximateArrivalTimestamp: float64(r.arrived.UnixMilli()) / 1000.0,
            Data:                        append([]byte(nil), r.data...),
            PartitionKey:                r.pk,
        })
    }
    if n := len(sh.records); n > 0 {
        if mb := svc.now().Sub(sh.records[n-1].arrived).Milliseconds(); mb > 0 {
            out.MillisBehindLatest = mb
        }
    }
    return out, nil
}

func validateRecord(partitionKey string, data []byte) *apiError {
    if partitionKey == "" {
        return invalidArgument("PartitionKey is required.")
    }
    if len(partitionKey) > maxPartitionKeyBytes {
        return invalidArgument("PartitionKey length (%d bytes) exceeds the limit (%d bytes).", len(partitionKey), maxPartitionKeyBytes)
    }
    if len(data) > maxRecordDataBytes {
        return invalidArgument("Data size (%d bytes) exceeds the limit (%d bytes).", len(data), maxRecordDataBytes)
    }
    return nil
}

func (svc *Service) opPutRecord(in PutRecordInput) (any, *apiError) {
    if err := validateRecord(in.PartitionKey, in.Data); err != nil {
        return nil, err
    }
    svc.mu.Lock()
    defer svc.mu.Unlock()
    st, err := svc.resolveLocked(in.StreamName, in.StreamARN)
    if err != nil {
        return nil, err
    }
    idx := routeIndex(st, in.PartitionKey, in.ExplicitHashKey)
    sh := st.shards[idx]
    sh.seq++
    seq := formatSeq(idx, sh.seq)
    sh.records = append(sh.records, record{
        seq:     seq,
        data:    append([]byte(nil), in.Data...),
        pk:      in.PartitionKey,
        arrived: svc.now().UTC(),
    })
    return PutRecordOutput{ShardId: sh.id, SequenceNumber: seq, EncryptionType: encryptionNone}, nil
}

func (svc *Service) opPutRecords(in PutRecordsInput) (any, *apiError) {
    if len(in.Records) > maxRecordsPerPut {
        return nil, limitExceeded("Records size exceeds %d limit.", maxRecordsPerPut)
    }
    total := 0
    for _, e := range in.Records {
        total += len(e.Data)
    }
    if total > maxTotalPutBytes {
        return nil, limitExceeded("Records size exceeds %d bytes limit.", maxTotalPutBytes)
    }
    svc.mu.Lock()
    defer svc.mu.Unlock()
    st, err := svc.resolveLocked(in.StreamName, in.StreamARN)
    if err != nil {
        return nil, err
    }
    out := PutRecordsOutput{Records: make([]PutRecordsResultEntry, 0, len(in.Records))}
    for _, e := range in.Records {
        entry := PutRecordsResultEntry{}
        switch {
        case e.PartitionKey == "":
            entry.ErrorCode = "InvalidArgumentException"
            entry.ErrorMessage = "PartitionKey is required."
        case len(e.PartitionKey) > maxPartitionKeyBytes:
            entry.ErrorCode = "InvalidArgumentException"
            entry.ErrorMessage = fmt.Sprintf("PartitionKey length (%d bytes) exceeds the limit (%d bytes).", len(e.PartitionKey), maxPartitionKeyBytes)
        case len(e.Data) > maxRecordDataBytes:
            entry.ErrorCode = "InvalidArgumentException"
            entry.ErrorMessage = fmt.Sprintf("Data size (%d bytes) exceeds the limit (%d bytes).", len(e.Data), maxRecordDataBytes)
        default:
            idx := routeIndex(st, e.PartitionKey, e.ExplicitHashKey)
            sh := st.shards[idx]
            sh.seq++
            entry.SequenceNumber = formatSeq(idx, sh.seq)
            entry.ShardId = sh.id
            sh.records = append(sh.records, record{
                seq:     entry.SequenceNumber,
                data:    append([]byte(nil), e.Data...),
                pk:      e.PartitionKey,
                arrived: svc.now().UTC(),
            })
        }
        if entry.ErrorCode != "" {
            out.FailedRecordCount++
        }
        out.Records = append(out.Records, entry)
    }
    return out, nil
}

// call is the single dispatch point shared by the JSON and event-stream wire
// paths. body is the operation's JSON request encoding.
func (svc *Service) call(_ context.Context, op string, body []byte) (any, *apiError) {
    decode := func(v any) *apiError {
        if len(bytes.TrimSpace(body)) == 0 {
            return nil
        }
        if err := json.Unmarshal(body, v); err != nil {
            return &apiError{Code: "SerializationException", Message: "failed to parse request body: " + err.Error(), Status: http.StatusBadRequest}
        }
        return nil
    }
    switch op {
    case "CreateStream":
        var in CreateStreamInput
        if e := decode(&in); e != nil {
            return nil, e
        }
        return svc.opCreateStream(in)
    case "DeleteStream":
        var in DeleteStreamInput
        if e := decode(&in); e != nil {
            return nil, e
        }
        return svc.opDeleteStream(in)
    case "DescribeStream":
        var in DescribeStreamInput
        if e := decode(&in); e != nil {
            return nil, e
        }
        return svc.opDescribeStream(in)
    case "ListShards":
        var in ListShardsInput
        if e := decode(&in); e != nil {
            return nil, e
        }
        return svc.opListShards(in)
    case "ListStreams":
        var in ListStreamsInput
        if e := decode(&in); e != nil {
            return nil, e
        }
        return svc.opListStreams(in)
    case "GetShardIterator":
        var in GetShardIteratorInput
        if e := decode(&in); e != nil {
            return nil, e
        }
        return svc.opGetShardIterator(in)
    case "GetRecords":
        var in GetRecordsInput
        if e := decode(&in); e != nil {
            return nil, e
        }
        return svc.opGetRecords(in)
    case "PutRecord":
        var in PutRecordInput
        if e := decode(&in); e != nil {
            return nil, e
        }
        return svc.opPutRecord(in)
    case "PutRecords":
        var in PutRecordsInput
        if e := decode(&in); e != nil {
            return nil, e
        }
        return svc.opPutRecords(in)
    default:
        return nil, &apiError{Code: "UnsupportedOperationException", Message: fmt.Sprintf("unknown or unsupported operation %q", op), Status: http.StatusBadRequest}
    }
}

// ---------------------------------------------------------------------------
// Amazon Event Stream codec
// ---------------------------------------------------------------------------

const (
    esTypeTrue      = 0x00
    esTypeFalse     = 0x01
    esTypeByte      = 0x02
    esTypeShort     = 0x03
    esTypeInteger   = 0x04
    esTypeLong      = 0x05
    esTypeByteArray = 0x06
    esTypeString    = 0x07
    esTypeTimestamp = 0x08
    esTypeUUID      = 0x09
)

type esHeader struct {
    Name string
    Type byte
    Raw  []byte
}

type esMessage struct {
    Headers []esHeader
    Payload []byte
}

func esStr(name, value string) esHeader {
    return esHeader{Name: name, Type: esTypeString, Raw: []byte(value)}
}

func esLong(name string, v int64) esHeader {
    raw := make([]byte, 8)
    binary.BigEndian.PutUint64(raw, uint64(v))
    return esHeader{Name: name, Type: esTypeLong, Raw: raw}
}

// str returns the last matching header so that merged frames behave like a
// single frame with later values overriding earlier ones.
func (m *esMessage) str(name string) (string, bool) {
    for i := len(m.Headers) - 1; i >= 0; i-- {
        h := m.Headers[i]
        if h.Name == name && h.Type == esTypeString {
            return string(h.Raw), true
        }
    }
    return "", false
}

func (m *esMessage) anyStr(names ...string) (string, bool) {
    for _, n := range names {
        if v, ok := m.str(n); ok {
            return v, true
        }
    }
    return "", false
}

func (m *esMessage) streamAlias() (name, arn string) {
    name, _ = m.anyStr("stream-name", "StreamName", "stream")
    arn, _ = m.anyStr("stream-arn", "StreamARN")
    return name, arn
}

func parseESHeaders(seg []byte) ([]esHeader, error) {
    var out []esHeader
    pos := 0
    for pos < len(seg) {
        nameLen := int(seg[pos])
        pos++
        if pos+nameLen+1 > len(seg) {
            return nil, errors.New("truncated event-stream header name")
        }
        name := string(seg[pos : pos+nameLen])
        pos += nameLen
        vType := seg[pos]
        pos++
        var n int
        switch vType {
        case esTypeTrue, esTypeFalse:
            n = 0
        case esTypeByte:
            n = 1
        case esTypeShort:
            n = 2
        case esTypeInteger:
            n = 4
        case esTypeLong, esTypeTimestamp:
            n = 8
        case esTypeUUID:
            n = 16
        case esTypeByteArray, esTypeString:
            if pos+2 > len(seg) {
                return nil, errors.New("truncated event-stream header length")
            }
            n = int(binary.BigEndian.Uint16(seg[pos : pos+2]))
            pos += 2
        default:
            return nil, fmt.Errorf("unsupported event-stream header type %d for %q", vType, name)
        }
        if pos+n > len(seg) {
            return nil, errors.New("truncated event-stream header value")
        }
        out = append(out, esHeader{Name: name, Type: vType, Raw: append([]byte(nil), seg[pos:pos+n]...)})
        pos += n
    }
    return out, nil
}

func decodeEventStream(b []byte) ([]esMessage, error) {
    var out []esMessage
    off := 0
    for off < len(b) {
        if len(b)-off < 12 {
            return nil, fmt.Errorf("truncated event-stream message at offset %d", off)
        }
        total := int(binary.BigEndian.Uint32(b[off : off+4]))
        headLen := int(binary.BigEndian.Uint32(b[off+4 : off+8]))
        if total < 12 || headLen > total-12 || off+total > len(b) {
            return nil, fmt.Errorf("invalid event-stream prelude at offset %d", off)
        }
        // Lenient on CRCs: accept frames from producers that leave them blank.
        if binary.BigEndian.Uint32(b[off+8:off+12]) != crc32.ChecksumIEEE(b[off:off+8]) {
            _ = off
        }
        headers, err := parseESHeaders(b[off+12 : off+12+headLen])
        if err != nil {
            return nil, err
        }
        out = append(out, esMessage{
            Headers: headers,
            Payload: append([]byte(nil), b[off+12+headLen:off+total-4]...),
        })
        off += total
    }
    if len(out) == 0 {
        return nil, errors.New("empty event-stream body")
    }
    return out, nil
}

func encodeESMessage(headers []esHeader, payload []byte) []byte {
    hb := make([]byte, 0, 64)
    for _, h := range headers {
        hb = append(hb, byte(len(h.Name)))
        hb = append(hb, h.Name...)
        hb = append(hb, h.Type)
        switch h.Type {
        case esTypeByteArray, esTypeString:
            var l [2]byte
            binary.BigEndian.PutUint16(l[:], uint16(len(h.Raw)))
            hb = append(hb, l[:]...)
        }
        hb = append(hb, h.Raw...)
    }
    total := 12 + len(hb) + len(payload) + 4
    var pre [8]byte
    binary.BigEndian.PutUint32(pre[0:4], uint32(total))
    binary.BigEndian.PutUint32(pre[4:8], uint32(len(hb)))
    buf := make([]byte, 0, total)
    buf = append(buf, pre[:]...)
    var pc [4]byte
    binary.BigEndian.PutUint32(pc[:], crc32.ChecksumIEEE(pre[:]))
    buf = append(buf, pc[:]...)
    buf = append(buf, hb...)
    buf = append(buf, payload...)
    var mc [4]byte
    binary.BigEndian.PutUint32(mc[:], crc32.ChecksumIEEE(buf))
    buf = append(buf, mc[:]...)
    return buf
}

func payloadIsJSONObject(b []byte) bool {
    var m map[string]json.RawMessage
    if err := json.Unmarshal(bytes.TrimSpace(b), &m); err != nil {
        return false
    }
    return m != nil
}

func injectStreamInfo(payload []byte, name, arn string) []byte {
    var m map[string]json.RawMessage
    if err := json.Unmarshal(payload, &m); err != nil || m == nil {
        return payload
    }
    if name != "" {
        if _, ok := m["StreamName"]; !ok {
            m["StreamName"], _ = json.Marshal(name)
        }
    }
    if arn != "" {
        if _, ok := m["StreamARN"]; !ok {
            m["StreamARN"], _ = json.Marshal(arn)
        }
    }
    out, _ := json.Marshal(m)
    return out
}

func esError(w http.ResponseWriter, e *apiError) {
    payload, _ := json.Marshal(map[string]string{"message": e.Message})
    headers := []esHeader{
        esStr(":message-type", "exception"),
        esStr(":exception-type", e.Code),
    }
    w.Header().Set("Content-Type", contentTypeEventStream)
    w.WriteHeader(e.Status)
    _, _ = w.Write(encodeESMessage(headers, payload))
}

// serveEventStream handles SDKs that marshal unary PutRecord/PutRecords (and
// other JSON operations) onto the event-stream wire format.
func (svc *Service) serveEventStream(w http.ResponseWriter, op string, body []byte) {
    msgs, derr := decodeEventStream(body)
    if derr != nil {
        esError(w, invalidArgument("malformed event-stream request: %v", derr))
        return
    }
    m := &esMessage{}
    var payload []byte
    for _, frame := range msgs {
        m.Headers = append(m.Headers, frame.Headers...)
        payload = append(payload, frame.Payload...)
    }
    m.Payload = payload
    if op == "" {
        op, _ = m.str(":event-type")
    }
    if op == "" {
        op = inferOperation(payload)
    }
    streamName, streamARN := m.streamAlias()
    reqBody := payload
    isJSON := payloadIsJSONObject(payload)
    var probe map[string]json.RawMessage
    if isJSON {
        _ = json.Unmarshal(payload, &probe)
    }
    _, hasData := probe["Data"]
    _, hasRecords := probe["Records"]
    pk, hasPK := m.anyStr("partition-key", "PartitionKey")
    ehk, _ := m.anyStr("explicit-hash-key", "ExplicitHashKey")
    switch op {
    case "PutRecord":
        switch {
        case isJSON && hasData:
            reqBody = injectStreamInfo(payload, streamName, streamARN)
        case hasPK || ehk != "":
            in := PutRecordInput{
                StreamName:      streamName,
                StreamARN:       streamARN,
                PartitionKey:    pk,
                ExplicitHashKey: ehk,
                Data:            payload,
            }
            if len(in.Data) == 0 {
                if d, ok := m.anyStr("data", "Data"); ok {
                    if dec, err := base64.StdEncoding.DecodeString(d); err == nil {
                        in.Data = dec
                    }
                }
            }
            reqBody, _ = json.Marshal(in)
        default:
            if isJSON {
                reqBody = injectStreamInfo(payload, streamName, streamARN)
            } else {
                // Raw bytes with no partition key: surface the real problem.
                in := PutRecordInput{StreamName: streamName, StreamARN: streamARN, Data: payload}
                reqBody, _ = json.Marshal(in)
            }
        }
    case "PutRecords":
        switch {
        case isJSON && hasRecords:
            reqBody = injectStreamInfo(payload, streamName, streamARN)
        case !isJSON:
            var arr []PutRecordsRequestEntry
            if err := json.Unmarshal(payload, &arr); err == nil && len(arr) > 0 {
                in := PutRecordsInput{StreamName: streamName, StreamARN: streamARN, Records: arr}
                reqBody, _ = json.Marshal(in)
            }
        }
    default:
        if op == "" {
            esError(w, invalidArgument("missing operation: provide :event-type or X-Amz-Target"))
            return
        }
        if !isJSON {
            esError(w, &apiError{
                Code:    "SerializationException",
                Message: fmt.Sprintf("operation %s cannot consume a raw event-stream payload; provide a JSON request body", op),
                Status:  http.StatusBadRequest,
            })
            return
        }
        reqBody = injectStreamInfo(payload, streamName, streamARN)
    }
    out, aerr := svc.call(context.Background(), op, reqBody)
    if aerr != nil {
        esError(w, aerr)
        return
    }
    jsonOut, _ := json.Marshal(out)
    headers := []esHeader{
        esStr(":message-type", "event"),
        esStr(":event-type", op),
        esStr(":content-type", "application/json"),
    }
    switch o := out.(type) {
    case PutRecordOutput:
        headers = append(headers, esStr("shard-id", o.ShardId), esStr("sequence-number", o.SequenceNumber))
    case PutRecordsOutput:
        headers = append(headers, esLong("failed-record-count", int64(o.FailedRecordCount)))
    case GetRecordsOutput:
        headers = append(headers, esStr("next-shard-iterator", o.NextShardIterator), esLong("millis-behind-latest", o.MillisBehindLatest))
    }
    w.Header().Set("Content-Type", contentTypeEventStream)
    w.WriteHeader(http.StatusOK)
    _, _ = w.Write(encodeESMessage(headers, jsonOut))
}

// ---------------------------------------------------------------------------
// HTTP surface
// ---------------------------------------------------------------------------

func opFromTarget(target string) string {
    if target == "" {
        return ""
    }
    if i := strings.LastIndex(target, "."); i >= 0 {
        return target[i+1:]
    }
    return target
}

// inferOperation recovers the operation from the request body shape when a
// client omits X-Amz-Target.
func inferOperation(body []byte) string {
    var m map[string]json.RawMessage
    if err := json.Unmarshal(bytes.TrimSpace(body), &m); err != nil || m == nil {
        return ""
    }
    has := func(k string) bool {
        _, ok := m[k]
        return ok
    }
    switch {
    case has("ShardIterator"):
        return "GetRecords"
    case has("ShardIteratorType"):
        return "GetShardIterator"
    case has("Records"):
        return "PutRecords"
    case has("Data"):
        return "PutRecord"
    case has("ExclusiveStartShardId"):
        return "DescribeStream"
    case has("StartingSequenceNumber"), has("ShardId"):
        return "GetShardIterator"
    case has("ExclusiveStartStreamName"):
        return "ListStreams"
    case has("ShardCount"):
        return "CreateStream"
    case has("EnforceConsumerDeletion"):
        return "DeleteStream"
    case has("StreamName"), has("StreamARN"):
        return "DescribeStream"
    }
    return ""
}

func looksEventStream(b []byte) bool {
    if len(b) < 16 || b[0] == '{' {
        return false
    }
    total := int(binary.BigEndian.Uint32(b[0:4]))
    headLen := int(binary.BigEndian.Uint32(b[4:8]))
    return total >= 12 && total <= len(b) && headLen > 0 && headLen <= total-12
}

func requestID() string {
    return fmt.Sprintf("%016x", uint64(time.Now().UnixNano()))
}

func writeJSONErr(w http.ResponseWriter, e *apiError) {
    w.Header().Set("Content-Type", contentTypeJSON)
    w.Header().Set("x-amzn-ErrorType", e.Code+":")
    w.WriteHeader(e.Status)
    _ = json.NewEncoder(w).Encode(struct {
        Type    string `json:"__type"`
        Message string `json:"message"`
    }{Type: e.Code, Message: e.Message})
}

func (svc *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    body, err := io.ReadAll(io.LimitReader(r.Body, 128<<20))
    if err != nil {
        writeJSONErr(w, &apiError{Code: "SerializationException", Message: err.Error(), Status: http.StatusBadRequest})
        return
    }
    if svc.enforceSigV4 {
        if verr := verifySigV4(r, body, svc.accessKey, svc.secretKey); verr != nil {
            writeJSONErr(w, &apiError{Code: "InvalidSignatureException", Message: verr.Error(), Status: http.StatusForbidden})
            return
        }
    }
    op := opFromTarget(r.Header.Get("X-Amz-Target"))
    switch {
    case looksEventStream(body),
        strings.Contains(r.Header.Get("Content-Type"), "eventstream") && !payloadIsJSONObject(body),
        r.Header.Get(serializeEventStreamHeader) != "" && !payloadIsJSONObject(body):
        svc.serveEventStream(w, op, body)
    default:
        if op == "" {
            op = inferOperation(body)
        }
        out, aerr := svc.call(r.Context(), op, body)
        if aerr != nil {
            writeJSONErr(w, aerr)
            return
        }
        w.Header().Set("Content-Type", contentTypeJSON)
        w.Header().Set("x-amzn-RequestId", requestID())
        _ = json.NewEncoder(w).Encode(out)
    }
}

// ---------------------------------------------------------------------------
// SigV4 (covers AWS4-HMAC-SHA256 header signing and presigned query auth;
// the payload hash covers the exact wire body, event-stream framed or not)
// ---------------------------------------------------------------------------

func hmacSHA256(key, data []byte) []byte {
    h := hmac.New(sha256.New, key)
    h.Write(data)
    return h.Sum(nil)
}

func sigv4Signature(secretKey, scopeDate, region, service, stringToSign string) string {
    kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(scopeDate))
    kRegion := hmacSHA256(kDate, []byte(region))
    kService := hmacSHA256(kRegion, []byte(service))
    kSigning := hmacSHA256(kService, []byte("aws4_request"))
    return hex.EncodeToString(hmacSHA256(kSigning, []byte(stringToSign)))
}

func awsURIEncode(s string) string {
    return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}

func canonicalQueryValues(q url.Values) string {
    keys := make([]string, 0, len(q))
    for k := range q {
        keys = append(keys, k)
    }
    sort.Strings(keys)
    parts := make([]string, 0, len(keys))
    for _, k := range keys {
        vals := append([]string(nil), q[k]...)
        sort.Strings(vals)
        for _, v := range vals {
            parts = append(parts, awsURIEncode(k)+"="+awsURIEncode(v))
        }
    }
    return strings.Join(parts, "&")
}

func canonicalURI(r *http.Request) string {
    p := r.URL.EscapedPath()
    if p == "" {
        p = "/"
    }
    return p
}

func canonicalHeaders(r *http.Request, signedHeaders string) (string, bool) {
    var sb strings.Builder
    for _, name := range strings.Split(signedHeaders, ";") {
        key := strings.ToLower(name)
        var vals []string
        switch key {
        case "host":
            vals = []string{r.Host}
        case "content-length":
            vals = []string{strconv.FormatInt(r.ContentLength, 10)}
        default:
            vals, ok := r.Header[http.CanonicalHeaderKey(key)]
            if !ok || len(vals) == 0 {
                return "", false
            }
            trimmed := make([]string, 0, len(vals))
            for _, v := range vals {
                trimmed = append(trimmed, strings.Join(strings.Fields(v), " "))
            }
            sb.WriteString(key)
            sb.WriteString(":")
            sb.WriteString(strings.Join(trimmed, ","))
            sb.WriteString("\n")
            continue
        }
        sb.WriteString(key)
        sb.WriteString(":")
        sb.WriteString(vals[0])
        sb.WriteString("\n")
    }
    return sb.String(), true
}

func verifySigV4(r *http.Request, body []byte, accessKey, secretKey string) error {
    q := r.URL.Query()
    auth := strings.TrimSpace(r.Header.Get("Authorization"))
    if auth == "" {
        if q.Get("X-Amz-Signature") != "" {
            return verifyPresignedSigV4(r, q, accessKey, secretKey)
        }
        return errors.New("request is not signed: missing Authorization header")
    }
    const scheme = "AWS4-HMAC-SHA256"
    if !strings.HasPrefix(auth, scheme) {
        return fmt.Errorf("unsupported signing scheme %q", strings.SplitN(auth, " ", 2)[0])
    }
    fields := map[string]string{}
    for _, part := range strings.Split(strings.TrimSpace(strings.TrimPrefix(auth, scheme)), ",") {
        kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
        if len(kv) == 2 {
            fields[kv[0]] = kv[1]
        }
    }
    credential, signedHeaders, signature := fields["Credential"], fields["SignedHeaders"], fields["Signature"]
    if credential == "" || signedHeaders == "" || signature == "" {
        return errors.New("malformed Authorization header")
    }
    scope := strings.Split(credential, "/")
    if len(scope) != 5 {
        return fmt.Errorf("malformed Credential %q", credential)
    }
    if scope[0] != accessKey {
        return fmt.Errorf("unrecognized AccessKeyId %q", scope[0])
    }
    scopeDate, region, service := scope[1], scope[2], scope[3]
    amzDate := r.Header.Get("X-Amz-Date")
    if amzDate == "" {
        if d := r.Header.Get("Date"); d != "" {
            if t, perr := http.ParseTime(d); perr == nil {
                amzDate = t.UTC().Format("20060102T150405Z")
            }
        }
    }
    if amzDate == "" {
        amzDate = scopeDate + "T000000Z"
    }
    payloadHash := r.Header.Get("X-Amz-Content-Sha256")
    if payloadHash == "" || strings.HasPrefix(payloadHash, "STREAMING-") {
        sum := sha256.Sum256(body)
        payloadHash = hex.EncodeToString(sum[:])
    }
    wireHeaders, ok := canonicalHeaders(r, signedHeaders)
    if !ok {
        return fmt.Errorf("missing one of the signed headers %q", signedHeaders)
    }
    canonicalReq := strings.Join([]string{
        r.Method,
        canonicalURI(r),
        canonicalQueryValues(q),
        wireHeaders,
        signedHeaders,
        payloadHash,
    }, "\n")
    sum2 := sha256.Sum256([]byte(canonicalReq))
    sts := strings.Join([]string{
        "AWS4-HMAC-SHA256",
        amzDate,
        strings.Join([]string{scopeDate, region, service, "aws4_request"}, "/"),
        hex.EncodeToString(sum2[:]),
    }, "\n")
    want := sigv4Signature(secretKey, scopeDate, region, service, sts)
    if !strings.EqualFold(want, signature) {
        return fmt.Errorf("signature mismatch: expected %s got %s", want, signature)
    }
    return nil
}

func verifyPresignedSigV4(r *http.Request, q url.Values, accessKey, secretKey string) error {
    signature := q.Get("X-Amz-Signature")
    scope := strings.Split(q.Get("X-Amz-Credential"), "/")
    if len(scope) != 5 {
        return fmt.Errorf("malformed X-Amz-Credential %q", q.Get("X-Amz-Credential"))
    }
    if scope[0] != accessKey {
        return fmt.Errorf("unrecognized AccessKeyId %q", scope[0])
    }
    scopeDate, region, service := scope[1], scope[2], scope[3]
    amzDate := q.Get("X-Amz-Date")
    if amzDate == "" {
        amzDate = scopeDate + "T000000Z"
    }
    filtered := url.Values{}
    for k, vs := range q {
        if k == "X-Amz-Signature" {
            continue
        }
        filtered[k] = vs
    }
    signedHeaders := q.Get("X-Amz-SignedHeaders")
    if signedHeaders == "" {
        signedHeaders = "host"
    }
    canonicalReq := strings.Join([]string{
        r.Method,
        canonicalURI(r),
        canonicalQueryValues(filtered),
        "host:" + r.Host + "\n",
        signedHeaders,
        "UNSIGNED-PAYLOAD",
    }, "\n")
    sum := sha256.Sum256([]byte(canonicalReq))
    sts := strings.Join([]string{
        "AWS4-HMAC-SHA256",
        amzDate,
        strings.Join([]string{scopeDate, region, service, "aws4_request"}, "/"),
        hex.EncodeToString(sum[:]),
    }, "\n")
    want := sigv4Signature(secretKey, scopeDate, region, service, sts)
    if !strings.EqualFold(want, signature) {
        return errors.New("signature mismatch")
    }
    return nil
}
