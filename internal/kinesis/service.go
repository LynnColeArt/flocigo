package kinesis

// service.go — one-file Kinesis HTTP service. The Store owns all state; this
// file dispatches the awsjson1.0 operations and renders errors on the wire
// the way the Kinesis protocol does: HTTP 200 + {"__type","message"} for a
// fault, 404 with a not-recognized-target for anything unhandled.

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"strings"
)

const (
	regionDefault    = "us-east-1"
	accountDefault   = "000000000000"
	wireContentType  = "application/x-amz-json-1.1"
	targetPrefix     = "Kinesis_20131202."
	maxBodyBytes     = 1 << 20
	defaultShardMax  = 1000
)

// Service registers the AWS Kinesis /20131202/ HTTP surface over a Store.
type Service struct {
	Store      *Store
	Region     string
	AccountID  string
	mu         sync.Mutex
}

// NewService creates a Service using a data-dir for persistent storage.
func NewService(dataDir string) *Service {
	if dataDir == "" {
		dataDir = os.Getenv("FLOCI_DATA_DIR")
	}
	return &Service{Store: NewStore(dataDir), Region: regionDefault, AccountID: accountDefault}
}

func (s *Service) streamARN(name string) string {
	return fmt.Sprintf("arn:aws:kinesis:%s:%s:stream/%s", s.Region, s.AccountID, name)
}

type apiError struct {
	Code       string
	HTTPStatus int
	Message    string
}

func errNotFound(msg string) *apiError    { return &apiError{Code: "ResourceNotFoundException", HTTPStatus: http.StatusBadRequest, Message: msg} }
func errInUse(msg string) *apiError       { return &apiError{Code: "ResourceInUseException", HTTPStatus: http.StatusBadRequest, Message: msg} }
func errInvalid(msg string) *apiError     { return &apiError{Code: "InvalidArgumentException", HTTPStatus: http.StatusBadRequest, Message: msg} }
func errLimit(msg string) *apiError       { return &apiError{Code: "LimitExceededException", HTTPStatus: http.StatusBadRequest, Message: msg} }
func errExpired(msg string) *apiError     { return &apiError{Code: "ExpiredIteratorException", HTTPStatus: http.StatusBadRequest, Message: msg} }
func errInternal(msg string) *apiError    { return &apiError{Code: "InternalFailure", HTTPStatus: http.StatusBadRequest, Message: msg} }

func writeError(w http.ResponseWriter, id string, e *apiError) {
	b, err := json.Marshal(map[string]string{
		"__type":  "com.amazonaws.kinesis.v20131202#" + e.Code,
		"message": e.Message,
	})
	if err != nil {
		b = []byte("{\"__type\":\"InternalFailure\"}")
	}
	w.Header().Set("Content-Type", wireContentType)
	w.Header().Set("x-amzn-RequestId", id)
	w.WriteHeader(e.HTTPStatus)
	_, _ = w.Write(b)
}

func writeResult(w http.ResponseWriter, id string, body any) {
	w.Header().Set("Content-Type", wireContentType)
	w.Header().Set("x-amzn-RequestId", id)
	w.WriteHeader(http.StatusOK)
	if body != nil {
		_ = json.NewEncoder(w).Encode(body)
	}
}

// ServeHTTP dispatches on X-Amz-Target.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := newRequestID()
	target := r.Header.Get("X-Amz-Target")
	if i := strings.IndexByte(target, '.'); i >= 0 && strings.HasPrefix(target, targetPrefix) {
		target = target[i+1:]
	}
	if target == "" {
		writeError(w, id, errNotFound("The request is missing X-Amz-Target."))
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		writeError(w, id, errInternal("Unable to read request body."))
		return
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		body = []byte("{}")
	}
	var req map[string]any
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, id, errInvalid("Malformed JSON request body."))
		return
	}

	s.mu.Lock()
	out, aerr := s.invoke(target, req)
	s.mu.Unlock()
	if aerr != nil {
		writeError(w, id, aerr)
		return
	}
	writeResult(w, id, out)
}

// invoke dispatches a target name to its handler.
func (s *Service) invoke(target string, req map[string]any) (any, *apiError) {
	switch target {
	case "CreateStream":                 return s.createStream(req)
	case "DeleteStream":                 return s.deleteStream(req)
	case "ListStreams":                  return s.listStreams(req)
	case "DescribeStream":               return s.describeStream(req)
	case "DescribeStreamSummary":        return s.describeStreamSummary(req)
	case "ListShards":                   return s.listShards(req)
	case "GetShardIterator":             return s.getShardIterator(req)
	case "GetRecords":                   return s.getRecords(req)
	case "PutRecord":                    return s.putRecord(req)
	case "PutRecords":                   return s.putRecords(req)
	case "SplitShard":                   return s.splitShard(req)
	case "MergeShards":                  return s.mergeShards(req)
	case "UpdateShardCount":             return s.updateShardCount(req)
	case "IncreaseStreamRetentionPeriod": return s.retention(req, +1)
	case "DecreaseStreamRetentionPeriod": return s.retention(req, -1)
	default:
		return nil, errNotFound(fmt.Sprintf("Unknown target: %s", target))
	}
}

func newRequestID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// req returns the request field as a string.
func reqStr(req map[string]any, k string) string {
	v, _ := req[k].(string)
	return v
}

func reqInt(req map[string]any, k string) int {
	f, _ := req[k].(float64)
	return int(f)
}

// Stream wire shapes.
type streamModeDetails struct{ StreamMode string `json:"StreamMode,omitempty"` }
type hashKeyRange struct{ Starting int64 `json:"StartingHashKey"`; Ending int64 `json:"EndingHashKey"` }
type seqRange struct{ Starting string `json:"StartingSequenceNumber"`; Ending string `json:"EndingSequenceNumber"` }
type shardWire struct {
	ShardID string `json:"ShardId"`
	ParentShardID string `json:"ParentShardId,omitempty"`
	AdjacentParentShardID string `json:"AdjacentParentShardId,omitempty"`
	SequenceNumberRange *seqRange `json:"SequenceNumberRange"`
	HashKeyRange *hashKeyRange `json:"HashKeyRange"`
}
type streamWire struct {
	StreamName string `json:"StreamName"`
	StreamARN  string `json:"StreamARN"`
	StreamStatus string `json:"StreamStatus"`
	StreamModeDetails *streamModeDetails `json:"StreamModeDetails"`
	Shards []*shardWire `json:"Shards"`
	HasMoreShards bool `json:"HasMoreShards"`
	RetentionPeriodHours int `json:"RetentionPeriodHours"`
}

func streamToWire(s *Service, st *Stream) *streamWire {
	out := &streamWire{
		StreamName: st.Name,
		StreamARN:  s.streamARN(st.Name),
		StreamStatus: "ACTIVE",
		StreamModeDetails: &streamModeDetails{StreamMode: st.Mode},
		RetentionPeriodHours: st.RetentionHours,
		HasMoreShards: false,
	}
	for _, sh := range st.Shards {
		hk := &hashKeyRange{}
		if sh.StartHash != nil {
			hk.Starting = sh.StartHash.Int64()
		}
		if sh.EndHash != nil {
			hk.Ending = sh.EndHash.Int64()
		}
		out.Shards = append(out.Shards, &shardWire{
			ShardID: sh.ID,
			SequenceNumberRange: &seqRange{Starting: sh.StartSeq},
			HashKeyRange: hk,
		})
	}
	return out
}

func (s *Service) createStream(req map[string]any) (any, *apiError) {
	name := reqStr(req, "StreamName")
	if !validStreamName(name) {
		return nil, errInvalid("StreamName must match [a-zA-Z0-9_.-]+ and be 1..128 chars.")
	}
	shards := reqInt(req, "ShardCount")
	if shards < 1 { shards = 1 }
	if shards > defaultShardMax {
		return nil, errInvalid(fmt.Sprintf("ShardCount %d exceeds limit %d.", shards, defaultShardMax))
	}
	if _, ok := s.Store.create(name, s.streamARN(name), shards, "PROVISIONED"); !ok {
		return nil, errInUse(fmt.Sprintf("Stream %s already exists.", name))
	}
	return nil, nil
}

func (s *Service) deleteStream(req map[string]any) (any, *apiError) {
	name := reqStr(req, "StreamName")
	if !s.Store.delete(name) {
		return nil, errNotFound(fmt.Sprintf("Stream %s not found.", name))
	}
	return nil, nil
}

func (s *Service) listStreams(req map[string]any) (any, *apiError) {
	outs := s.Store.list()
	names := make([]string, 0, len(outs))
	for _, st := range outs { names = append(names, st.Name) }
	return map[string]any{"StreamNames": names, "HasMoreStreams": false}, nil
}

func (s *Service) describeStream(req map[string]any) (any, *apiError) {
	name := reqStr(req, "StreamName")
	st := s.Store.get(name)
	if st == nil {
		return nil, errNotFound(fmt.Sprintf("Stream %s not found.", name))
	}
	body := streamToWire(s, st)
	return map[string]any{"StreamDescription": body}, nil
}

func (s *Service) describeStreamSummary(req map[string]any) (any, *apiError) {
	name := reqStr(req, "StreamName")
	st := s.Store.get(name)
	if st == nil {
		return nil, errNotFound(fmt.Sprintf("Stream %s not found.", name))
	}
	return map[string]any{"StreamDescriptionSummary": streamToWire(s, st)}, nil
}

func (s *Service) listShards(req map[string]any) (any, *apiError) {
	name := reqStr(req, "StreamName")
	st := s.Store.get(name)
	if st == nil {
		return nil, errNotFound(fmt.Sprintf("Stream %s not found.", name))
	}
	shards := make([]any, 0, len(st.Shards))
		for _, sh := range st.Shards {
		hk := &hashKeyRange{}
		if sh.StartHash != nil { hk.Starting = sh.StartHash.Int64() }
		if sh.EndHash != nil { hk.Ending = sh.EndHash.Int64() }
		shards = append(shards, &shardWire{
			ShardID: sh.ID,
			SequenceNumberRange: &seqRange{Starting: sh.StartSeq},
			HashKeyRange: hk,
		})
	}
	return map[string]any{"Shards": shards, "NextToken": ""}, nil
}

// getShardIterator mints an iterator the caller may feed to GetRecords.
func (s *Service) getShardIterator(req map[string]any) (any, *apiError) {
	name := reqStr(req, "StreamName")
	shard := reqStr(req, "ShardId")
	st := s.Store.get(name)
	if st == nil {
		return nil, errNotFound(fmt.Sprintf("Stream %s not found.", name))
	}
	var target *Shard
	for _, sh := range st.Shards {
		if sh.ID == shard { target = sh; break }
	}
	if target == nil {
		return nil, errInvalid(fmt.Sprintf("Shard %s not in stream %s.", shard, name))
	}
	typ := reqStr(req, "ShardIteratorType")
	seq := reqStr(req, "StartingSequenceNumber")
	e := int64(0)
	switch typ {
	case "", "LATEST", "TRIM_HORIZON":
		e = 0
		if typ == "LATEST" {
			e = int64(len(target.Records))
		}
	case "AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER":
		for i, r := range target.Records {
			if r.Seq == seq { e = int64(i); if typ == "AFTER_SEQUENCE_NUMBER" { e++ }; break }
		}
	default:
		return nil, errInvalid(fmt.Sprintf("Unknown ShardIteratorType %s.", typ))
	}
	it := encodeIterator(iterToken{V: 1, S: name, H: shard, E: e})
	return map[string]any{"ShardIterator": it}, nil
}

// getRecords returns records from a shard iterator.
func (s *Service) getRecords(req map[string]any) (any, *apiError) {
	tok := reqStr(req, "ShardIterator")
	limit := reqInt(req, "Limit")
	it, ok := decodeIterator(tok)
	if !ok {
		return nil, errExpired("The provided shard iterator is not valid.")
	}
	st := s.Store.get(it.S)
	if st == nil {
		return nil, errExpired("The stream for this iterator no longer exists.")
	}
	var target *Shard
	for _, sh := range st.Shards {
		if sh.ID == it.H { target = sh; break }
	}
	if target == nil {
		return nil, errExpired("The shard for this iterator no longer exists.")
	}
	max := int64(1000)
	if max <= 0 || max > 1000 { max = 1000 }
	if limit > 0 && int64(limit) < max { max = int64(limit) }
	out := make([]any, 0, 8)
	i := it.E
	for ; i < int64(len(target.Records)) && int64(len(out)) < max; i++ {
		r := target.Records[i]
		out = append(out, map[string]any{
			"SequenceNumber": r.Seq,
			"Data":           r.Data,
			"PartitionKey":   r.PartitionKey,
			"ApproximateArrivalTimestamp": 0.0,
		})
	}
	if i >= int64(len(target.Records)) {
		i = int64(len(target.Records))
	}
	next := encodeIterator(iterToken{V: 1, S: it.S, H: it.H, E: i})
	return map[string]any{
		"ShardIterator":     next,
		"Records":           out,
		"MillisBehindLatest": int64(0),
	}, nil
}

func (s *Service) putRecord(req map[string]any) (any, *apiError) {
	name := reqStr(req, "StreamName")
	st := s.Store.get(name)
	if st == nil {
		return nil, errNotFound(fmt.Sprintf("Stream %s not found.", name))
	}
	data := reqStr(req, "Data")
	pk := reqStr(req, "PartitionKey")
	// Choose target shard by partition-key hash mod shard count.
	var target *Shard
	var best uint32
	for i, sh := range st.Shards {
		h := shardHash(pk) + uint32(i)
		if target == nil || h < best { target, best = sh, h }
	}
	target.seq++
	seq := fmt.Sprintf("%020d", target.seq)
	target.Records = append(target.Records, Record{Seq: seq, PartitionKey: pk, Data: []byte(data)})
	return map[string]any{"ShardId": target.ID, "SequenceNumber": seq}, nil
}

func (s *Service) putRecords(req map[string]any) (any, *apiError) {
	name := reqStr(req, "StreamName")
	st := s.Store.get(name)
	if st == nil {
		return nil, errNotFound(fmt.Sprintf("Stream %s not found.", name))
	}
	entries, _ := req["Records"].([]any)
	res := make([]any, 0, len(entries))
	code := 0
	for _, e := range entries {
		rec, _ := e.(map[string]any)
		data := reqStr(rec, "Data")
		pk := reqStr(rec, "PartitionKey")
		var target *Shard
		var best uint32
		for i, sh := range st.Shards {
			h := shardHash(pk) + uint32(i)
			if target == nil || h < best { target, best = sh, h }
		}
		target.seq++
	seq := fmt.Sprintf("%020d", target.seq)
	target.Records = append(target.Records, Record{Seq: seq, PartitionKey: pk, Data: []byte(data)})
		res = append(res, map[string]any{"ShardId": target.ID, "SequenceNumber": seq, "ErrorCode": ""})
	}
	if code > 0 {
		return nil, errInternal("some records failed")
	}
	return map[string]any{"Records": res, "FailedRecordCount": 0}, nil
}

func shardHash(k string) uint32 {
	h := uint32(2166136261)
	for _, c := range k {
		h *= 16777619
		h ^= uint32(c)
	}
	return h
}

func (s *Service) splitShard(req map[string]any) (any, *apiError) {
	return map[string]any{"ShardId": reqStr(req, "ShardId")}, nil
}

func (s *Service) mergeShards(req map[string]any) (any, *apiError) {
	return map[string]any{}, nil
}

func (s *Service) updateShardCount(req map[string]any) (any, *apiError) {
	name := reqStr(req, "StreamName")
	st := s.Store.get(name)
	if st == nil {
		return nil, errNotFound(fmt.Sprintf("Stream %s not found.", name))
	}
	return map[string]any{"CurrentShardCount": len(st.Shards)}, nil
}

func (s *Service) retention(req map[string]any, dir int) (any, *apiError) {
	name := reqStr(req, "StreamName")
	st := s.Store.get(name)
	if st == nil {
		return nil, errNotFound(fmt.Sprintf("Stream %s not found.", name))
	}
	h := reqInt(req, "RetentionPeriodHours")
	if h == 0 { h = 24 }
	st.RetentionHours = h
	return nil, nil
}
