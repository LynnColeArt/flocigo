package kinesis

import (
    "crypto/md5"
    "encoding/base64"
    "encoding/json"
    "fmt"
    "math/big"
    "sort"
    "strconv"
    "sync"
    "time"
)

const (
    recordMax     = 1 << 20 // 1 MiB single-record payload cap
    putRecordsMax = 500     // PutRecords batch ceiling
    shardMax      = 1000    // largest shard count per stream
)

// Record is one data record stored in a shard.
type Record struct {
    Num          uint64 // numeric sequence number; defines per-shard order
    Seq          string // sequence number as sent over the wire
    Data         []byte // decoded payload (the wire carries base64)
    PartitionKey string
    Arrival      time.Time
}

// Shard is a fixed shard of a stream.
type Shard struct {
    ID        string // "shardId-000000000000"
    StartHash *big.Int
    EndHash   *big.Int
    StartSeq  string
    seq       uint64 // last sequence number handed out
    Records   []Record
}

// Stream is an in-memory Kinesis stream.
type Stream struct {
    Name           string
    ARN            string
    Status         string
    Mode           string
    Created        time.Time
    RetentionHours int
    Shards         []*Shard
    width          *big.Int // hash-key bucket width used for shard mapping

    mu sync.RWMutex // guards seq counters and Records slices
}

// Store holds every stream of the emulated account.
type Store struct {
    mu      sync.RWMutex
    streams map[string]*Stream
}

func newStore() *Store { return &Store{streams: map[string]*Stream{}} }

func (st *Store) get(name string) *Stream {
    st.mu.RLock()
    defer st.mu.RUnlock()
    return st.streams[name]
}

func (st *Store) create(name, arn string, shardCount int, mode string) (*Stream, bool) {
    st.mu.Lock()
    defer st.mu.Unlock()
    if _, ok := st.streams[name]; ok {
        return nil, false
    }
    s := newStream(name, arn, shardCount, mode)
    st.streams[name] = s
    return s, true
}

func (st *Store) delete(name string) bool {
    st.mu.Lock()
    defer st.mu.Unlock()
    if _, ok := st.streams[name]; !ok {
        return false
    }
    delete(st.streams, name)
    return true
}

// list returns all streams sorted by name.
func (st *Store) list() []*Stream {
    st.mu.RLock()
    defer st.mu.RUnlock()
    names := make([]string, 0, len(st.streams))
    for n := range st.streams {
        names = append(names, n)
    }
    sort.Strings(names)
    out := make([]*Stream, 0, len(names))
    for _, n := range names {
        out = append(out, st.streams[n])
    }
    return out
}

func newStream(name, arn string, shardCount int, mode string) *Stream {
    shards, width := makeShards(shardCount)
    return &Stream{
        Name:           name,
        ARN:            arn,
        Status:         "ACTIVE",
        Mode:           mode,
        Created:        time.Now().UTC(),
        RetentionHours: 24,
        Shards:         shards,
        width:          width,
    }
}

var hashTotal = new(big.Int).Lsh(big.NewInt(1), 128) // 2^128

// makeShards partitions [0, 2^128-1] into shardCount contiguous hash ranges
// (the last shard absorbs the remainder) and returns the shards plus the
// bucket width used by shardForHash.
func makeShards(n int) ([]*Shard, *big.Int) {
    if n < 1 {
        n = 1
    }
    w := new(big.Int).Div(
        new(big.Int).Add(hashTotal, big.NewInt(int64(n-1))),
        big.NewInt(int64(n)),
    ) // ceil(2^128 / n)
    shards := make([]*Shard, n)
    start := big.NewInt(0)
    for i := 0; i < n; i++ {
        end := new(big.Int).Sub(new(big.Int).Add(start, w), big.NewInt(1))
        if i == n-1 {
            end = new(big.Int).Sub(hashTotal, big.NewInt(1)) // 2^128-1
        }
        shards[i] = &Shard{
            ID:        fmt.Sprintf("shardId-%012d", i),
            StartHash: new(big.Int).Set(start),
            EndHash:   end,
            StartSeq:  "1",
            seq:       1,
        }
        start = new(big.Int).Add(start, w)
    }
    return shards, w
}

// hashPartitionKey mirrors Kinesis: the MD5 of the partition key, interpreted
// as a 128-bit unsigned integer, decides the shard.
func hashPartitionKey(pk string) *big.Int {
    sum := md5.Sum([]byte(pk))
    return new(big.Int).SetBytes(sum[:])
}

// parseHashKey validates an optional ExplicitHashKey (decimal, 0 <= x < 2^128).
func parseHashKey(s string) (*big.Int, *apiError) {
    if s == "" {
        return nil, nil
    }
    v, ok := new(big.Int).SetString(s, 10)
    if !ok || v.Sign() < 0 || v.Cmp(hashTotal) >= 0 {
        return nil, invalidArgument("ExplicitHashKey must be a decimal number between 0 and %s.", new(big.Int).Sub(hashTotal, big.NewInt(1)).String())
    }
    return v, nil
}

// Shard returns the shard with the given id, or nil.
func (s *Stream) Shard(id string) *Shard {
    for _, sh := range s.Shards {
        if sh.ID == id {
            return sh
        }
    }
    return nil
}

// appendItem is one prepared record waiting for a shard and sequence number.
type appendItem struct {
    Data []byte
    PK   string
    Hash *big.Int
}

type appendResult struct {
    Shard *Shard
    Seq   string
}

// appendBatch maps every record to its shard (md5 of the partition key, or
// the explicit hash key override) and appends them atomically, so a batch
// keeps its ordering inside each shard and sequence numbers strictly grow.
func (s *Stream) appendBatch(items []appendItem) []appendResult {
    s.mu.Lock()
    defer s.mu.Unlock()
    out := make([]appendResult, len(items))
    now := time.Now().UTC()
    for i, it := range items {
        sh := s.shardForHash(it.Hash)
        sh.seq++
        seq := strconv.FormatUint(sh.seq, 10)
        sh.Records = append(sh.Records, Record{
            Num:          sh.seq,
            Seq:          seq,
            Data:         it.Data,
            PartitionKey: it.PK,
            Arrival:      now,
        })
        out[i] = appendResult{Shard: sh, Seq: seq}
    }
    return out
}

// shardForHash picks the shard whose hash-key range contains h. Callers must
// hold the stream lock.
func (s *Stream) shardForHash(h *big.Int) *Shard {
    idx := new(big.Int).Div(h, s.width).Int64()
    if idx >= int64(len(s.Shards)) {
        idx = int64(len(s.Shards) - 1)
    }
    if idx < 0 {
        idx = 0
    }
    return s.Shards[idx]
}

// read returns up to limit records starting at index excl.
func (s *Stream) read(sh *Shard, excl, limit int) []Record {
    s.mu.RLock()
    defer s.mu.RUnlock()
    if excl < 0 || excl >= len(sh.Records) || limit <= 0 {
        return nil
    }
    end := len(sh.Records)
    if excl+limit < end {
        end = excl + limit
    }
    out := make([]Record, end-excl)
    copy(out, sh.Records[excl:end])
    return out
}

func (s *Stream) count(sh *Shard) int {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return len(sh.Records)
}

// lowerBound returns the first index whose sequence number is >= seq (where
// an AT_SEQUENCE_NUMBER iterator starts). With exclusive set and an exact
// match, it returns the following index (AFTER_SEQUENCE_NUMBER).
func (s *Stream) lowerBound(sh *Shard, seq uint64, exclusive bool) int {
    s.mu.RLock()
    defer s.mu.RUnlock()
    i := sort.Search(len(sh.Records), func(k int) bool { return sh.Records[k].Num >= seq })
    if exclusive && i < len(sh.Records) && sh.Records[i].Num == seq {
        return i + 1
    }
    return i
}

// firstAtOrAfter returns the first record index whose arrival time is >= ts.
func (s *Stream) firstAtOrAfter(sh *Shard, ts time.Time) int {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return sort.Search(len(sh.Records), func(k int) bool { return !sh.Records[k].Arrival.Before(ts) })
}

// millisBehind reports how far the shard's newest record lags behind now.
func (s *Stream) millisBehind(sh *Shard) int64 {
    s.mu.RLock()
    defer s.mu.RUnlock()
    if n := len(sh.Records); n > 0 {
        return time.Since(sh.Records[n-1].Arrival).Milliseconds()
    }
    return 0
}

// decodeData accepts the base64 alphabets SDKs emit (std/raw/URL variants).
func decodeData(s string) ([]byte, *apiError) {
    for _, e := range []*base64.Encoding{
        base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding,
    } {
        if b, err := e.DecodeString(s); err == nil {
            return b, nil
        }
    }
    return nil, invalidArgument("Data must be valid base64.")
}

// iterToken is the shard-iterator state, base64-encoded as a ShardIterator.
// E is the exclusive lower bound: the index of the next record to read.
type iterToken struct {
    V int    `json:"v"`
    S string `json:"s"`
    H string `json:"h"`
    E int64  `json:"e"`
}

func encodeIterator(t iterToken) string {
    t.V = 1
    b, _ := json.Marshal(t)
    return base64.StdEncoding.EncodeToString(b)
}

func decodeIterator(tok string) (iterToken, bool) {
    var raw []byte
    var err error
    for _, e := range []*base64.Encoding{
        base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding,
    } {
        if raw, err = e.DecodeString(tok); err == nil {
            break
        }
    }
    if err != nil {
        return iterToken{}, false
    }
    var it iterToken
    if json.Unmarshal(raw, &it) != nil || it.S == "" || it.H == "" || it.E < 0 {
        return iterToken{}, false
    }
    return it, true
}

// NewStore opens a Kinesis store under dataDir.
func NewStore(dataDir string) *Store { return newStore() }

// validStreamName enforces the documented character set and length.
func validStreamName(name string) bool {
	if name == "" || len(name) > 128 {
		return false
	}
	for _, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '_', c == '.', c == '-':
		default:
			return false
		}
	}
	return true
}

func invalidArgument(format string, args ...any) *apiError {
	return errInvalid(fmt.Sprintf(format, args...))
}
func resourceInUse(format string, args ...any) *apiError {
	return errInUse(fmt.Sprintf(format, args...))
}
