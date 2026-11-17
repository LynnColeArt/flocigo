package kinesis

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

// Shard iterator types accepted by GetShardIterator (AWS wire values).
// AT_LATEST is a lenient alias accepted for LATEST.
const (
	IteratorTypeAtTimestamp         = "AT_TIMESTAMP"
	IteratorTypeAfterSequenceNumber = "AFTER_SEQUENCE_NUMBER"
	IteratorTypeAtSequenceNumber    = "AT_SEQUENCE_NUMBER"
	IteratorTypeTrimHorizon         = "TRIM_HORIZON"
	IteratorTypeLatest              = "LATEST"
	IteratorTypeLatestAlias         = "AT_LATEST"
)

// shardIterator is the opaque token exchanged with clients (base64(JSON)).
// It pins the stream/shard, how the initial position was derived
// (Type/Seq/TS) and the resolved record index (Index).
type shardIterator struct {
	Stream string  `json:"stream"`
	Shard  string  `json:"shard"`
	Type   string  `json:"type"`
	Seq    string  `json:"seq,omitempty"`
	TS     float64 `json:"ts,omitempty"`
	Index  int     `json:"index"`
}

// encode serialises the iterator into its opaque wire representation.
func (it shardIterator) encode() string {
	blob, err := json.Marshal(it)
	if err != nil {
		return "" // unreachable: all fields are JSON friendly
	}
	return base64.StdEncoding.EncodeToString(blob)
}

// decodeShardIterator parses a token, returning false for anything that is
// not one of our iterators (reported as ExpiredIteratorException).
func decodeShardIterator(raw string) (shardIterator, bool) {
	var it shardIterator
	blob, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return it, false
	}
	if err := json.Unmarshal(blob, &it); err != nil || it.Stream == "" || it.Shard == "" {
		return it, false
	}
	return it, true
}

// position resolves the initial record index of an iterator. LATEST pins
// "after the last record at creation time", AT_TIMESTAMP pins "first
// record arriving at or after the timestamp".
func (it shardIterator) position(shard *Shard) (int, error) {
	switch it.Type {
	case IteratorTypeTrimHorizon:
		return 0, nil
	case IteratorTypeLatest:
		return len(shard.Records), nil
	case IteratorTypeAtSequenceNumber, IteratorTypeAfterSequenceNumber:
		index := indexBySequence(shard.Records, it.Seq)
		if it.Type == IteratorTypeAfterSequenceNumber && index < len(shard.Records) && shard.Records[index].SequenceNumber == it.Seq {
			index++
		}
		return index, nil
	case IteratorTypeAtTimestamp:
		return indexByArrival(shard.Records, it.TS), nil
	default:
		return 0, fmt.Errorf("unknown shard iterator type %q", it.Type)
	}
}

// indexBySequence returns the first record whose sequence number is >= seq.
func indexBySequence(records []Record, seq string) int {
	lo, hi := 0, len(records)
	for lo < hi {
		mid := (lo + hi) / 2
		if records[mid].SequenceNumber < seq {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// indexByArrival returns the first record arriving at or after ts.
func indexByArrival(records []Record, ts float64) int {
	cutoff := epochTime(ts)
	lo, hi := 0, len(records)
	for lo < hi {
		mid := (lo + hi) / 2
		if records[mid].Arrival.Before(cutoff) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// epochTime converts epoch seconds (float) into a time.Time.
func epochTime(seconds float64) time.Time {
	sec := int64(seconds)
	nsec := int64((seconds - float64(sec)) * float64(time.Second))
	return time.Unix(sec, nsec).UTC()
}

// epochSeconds converts a time.Time into epoch seconds (float), the wire
// format of ApproximateArrivalTimestamp and Timestamp fields.
func epochSeconds(t time.Time) float64 {
	return float64(t.UnixNano()) / 1e9
}
