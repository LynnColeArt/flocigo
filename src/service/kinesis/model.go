package kinesis

import (
	"fmt"
	"math/big"
	"sort"
	"time"
)

// Stream lifecycle states. The emulator keeps every stream ACTIVE from
// the moment CreateStream returns (no artificial transition delay).
const (
	StreamStatusCreating = "CREATING"
	StreamStatusActive   = "ACTIVE"
	StreamStatusUpdating = "UPDATING"
	StreamStatusDeleting = "DELETING"
)

// Defaults matching the real service.
const (
	EncryptionTypeNone    = "NONE"
	DefaultRetentionHours = 24
)

// Stream capacity modes.
const (
	StreamModeProvisioned = "PROVISIONED"
	StreamModeOnDemand    = "ON_DEMAND"
)

// Record is a single data record as stored inside a shard.
type Record struct {
	PartitionKey    string
	ExplicitHashKey string
	Data            []byte
	SequenceNumber  string
	Arrival         time.Time
}

// Shard is one shard together with all records it ever received. Records
// on closed shards (split/merge parents) remain readable.
type Shard struct {
	ShardID               string
	ParentShardID         string
	AdjacentParentShardID string
	StartingHashKey       *big.Int
	EndingHashKey         *big.Int
	StartingSeq           string
	EndingSeq             string // non-empty => shard is closed
	Records               []Record
}

// Closed reports whether the shard no longer receives new records.
func (s *Shard) Closed() bool { return s.EndingSeq != "" }

// Stream is a named stream with its complete shard map.
type Stream struct {
	Name         string
	Region       string
	AccountID    string
	Status       string
	Mode         string
	CreatedAt    time.Time
	Shards       []*Shard
	nextShardNum uint64
	seqCounter   uint64
}

// ARN returns the stream's canonical ARN.
func (s *Stream) ARN() string {
	return fmt.Sprintf("arn:aws:kinesis:%s:%s:stream/%s", s.Region, s.AccountID, s.Name)
}

// shard looks up a shard by id.
func (s *Stream) shard(id string) *Shard {
	for _, sh := range s.Shards {
		if sh.ShardID == id {
			return sh
		}
	}
	return nil
}

// openShards returns the shards that still receive records.
func (s *Stream) openShards() []*Shard {
	var out []*Shard
	for _, sh := range s.Shards {
		if !sh.Closed() {
			out = append(out, sh)
		}
	}
	return out
}

// sortedShards returns all shards ordered by id; ids are zero padded, so
// lexicographic order equals creation (numeric) order.
func (s *Stream) sortedShards() []*Shard {
	out := append([]*Shard(nil), s.Shards...)
	sort.Slice(out, func(i, j int) bool { return out[i].ShardID < out[j].ShardID })
	return out
}

// nextShardID mints the next sequential shard id ("shardId-000000000000").
func (s *Stream) nextShardID() string {
	id := fmt.Sprintf("shardId-%012d", s.nextShardNum)
	s.nextShardNum++
	return id
}

// nextSeqNumber returns the next stream wide sequence number. Sequence
// numbers are fixed width decimals, so lexicographic order equals numeric
// order; they are unique and comparable across shards of one stream.
func (s *Stream) nextSeqNumber() string {
	s.seqCounter++
	return fmt.Sprintf("%056d", s.seqCounter)
}

// --- wire views ---------------------------------------------------------

// StreamModeDetails carries the stream capacity mode on the wire.
type StreamModeDetails struct {
	StreamMode string `json:"StreamMode"`
}

// EnhancedMetrics is the monitoring block of DescribeStream responses.
type EnhancedMetrics struct {
	ShardLevelMetrics []string `json:"ShardLevelMetrics"`
}

// HashKeyRange is a shard hash key range (decimal strings).
type HashKeyRange struct {
	StartingHashKey string `json:"StartingHashKey"`
	EndingHashKey   string `json:"EndingHashKey"`
}

// SequenceNumberRange is a shard sequence number range.
type SequenceNumberRange struct {
	StartingSequenceNumber string `json:"StartingSequenceNumber"`
	EndingSequenceNumber   string `json:"EndingSequenceNumber,omitempty"`
}

// ShardView is the wire representation of a shard.
type ShardView struct {
	ShardId               string              `json:"ShardId"`
	ParentShardId         string              `json:"ParentShardId,omitempty"`
	AdjacentParentShardId string              `json:"AdjacentParentShardId,omitempty"`
	HashKeyRange          HashKeyRange        `json:"HashKeyRange"`
	SequenceNumberRange   SequenceNumberRange `json:"SequenceNumberRange"`
}

// view renders the shard for DescribeStream / ListShards responses.
func (s *Shard) view() ShardView {
	return ShardView{
		ShardId:               s.ShardID,
		ParentShardId:         s.ParentShardID,
		AdjacentParentShardId: s.AdjacentParentShardID,
		HashKeyRange: HashKeyRange{
			StartingHashKey: s.StartingHashKey.Text(10),
			EndingHashKey:   s.EndingHashKey.Text(10),
		},
		SequenceNumberRange: SequenceNumberRange{
			StartingSequenceNumber: s.StartingSeq,
			EndingSequenceNumber:   s.EndingSeq,
		},
	}
}
