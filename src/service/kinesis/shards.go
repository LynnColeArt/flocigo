package kinesis

import (
	"crypto/md5"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// maxHashKey is 2^128-1, the largest valid hash key value (partition key
// hashes are 128 bit MD5 digests).
var maxHashKey = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))

// partitionKeyHash maps a partition key onto its 128 bit MD5 hash key,
// exactly like the real service determines the target shard.
func partitionKeyHash(key string) *big.Int {
	sum := md5.Sum([]byte(key))
	return new(big.Int).SetBytes(sum[:])
}

// parseHashKey interprets an explicit hash key: numeric values inside the
// 128 bit range are used verbatim; everything else is MD5 hashed like a
// partition key (kinesalite compatible).
func parseHashKey(value string) *big.Int {
	if v, ok := new(big.Int).SetString(strings.TrimSpace(value), 10); ok &&
		v.Sign() >= 0 && v.Cmp(maxHashKey) <= 0 {
		return v
	}
	return partitionKeyHash(value)
}

// divideHashKeyRange splits the key space [0, 2^128-1] into n contiguous
// equally sized ranges.
func divideHashKeyRange(n int) [][2]*big.Int {
	total := new(big.Int).Lsh(big.NewInt(1), 128)
	width := new(big.Int).Div(total, big.NewInt(int64(n)))
	ranges := make([][2]*big.Int, 0, n)
	start := big.NewInt(0)
	for i := 0; i < n; i++ {
		end := new(big.Int).Sub(new(big.Int).Add(start, width), big.NewInt(1))
		if i == n-1 {
			end = new(big.Int).Set(maxHashKey)
		}
		ranges = append(ranges, [2]*big.Int{new(big.Int).Set(start), end})
		start = new(big.Int).Add(start, width)
	}
	return ranges
}

// newStream builds a stream with shardCount equally sized shards.
func newStream(name, region, account, mode string, shardCount int, now time.Time) *Stream {
	if shardCount < 1 {
		shardCount = 1
	}
	s := &Stream{
		Name:      name,
		Region:    region,
		AccountID: account,
		Status:    StreamStatusActive,
		Mode:      mode,
		CreatedAt: now,
	}
	for _, r := range divideHashKeyRange(shardCount) {
		s.Shards = append(s.Shards, &Shard{
			ShardID:         s.nextShardID(),
			StartingHashKey: r[0],
			EndingHashKey:   r[1],
			StartingSeq:     s.nextSeqNumber(),
		})
	}
	return s
}

// shardForKey returns the open shard owning the given hash key. The last
// open shard is a safe fallback so records are never dropped.
func (s *Stream) shardForKey(key *big.Int) *Shard {
	open := s.openShards()
	if len(open) == 0 {
		return nil
	}
	for _, sh := range open {
		if key.Cmp(sh.StartingHashKey) >= 0 && key.Cmp(sh.EndingHashKey) <= 0 {
			return sh
		}
	}
	return open[len(open)-1]
}

// SplitShard closes shardID and creates two children that divide its hash
// key range at newStartingHashKey (which becomes the right child's
// starting hash key), like the real SplitShard operation.
func (s *Stream) SplitShard(shardID, newStartingHashKey string) *apiError {
	sh := s.shard(shardID)
	if sh == nil {
		return shardNotFound(s, shardID)
	}
	if sh.Closed() {
		return &apiError{
			Code:    "InvalidArgumentException",
			Message: fmt.Sprintf("Shard %s in stream %s under account %s has already been split or merged and cannot be split again.", shardID, s.Name, s.AccountID),
		}
	}
	n, ok := new(big.Int).SetString(strings.TrimSpace(newStartingHashKey), 10)
	if !ok || n.Cmp(sh.StartingHashKey) <= 0 || n.Cmp(sh.EndingHashKey) > 0 {
		return &apiError{
			Code:    "InvalidArgumentException",
			Message: fmt.Sprintf("NewStartingHashKey %s is outside the hash key range of shard %s (%s..%s).", newStartingHashKey, shardID, sh.StartingHashKey.Text(10), sh.EndingHashKey.Text(10)),
		}
	}

	parent := sh
	parent.EndingSeq = s.nextSeqNumber()
	left := &Shard{
		ShardID:         s.nextShardID(),
		ParentShardID:   parent.ShardID,
		StartingHashKey: parent.StartingHashKey,
		EndingHashKey:   new(big.Int).Sub(n, big.NewInt(1)),
		StartingSeq:     s.nextSeqNumber(),
	}
	right := &Shard{
		ShardID:         s.nextShardID(),
		ParentShardID:   parent.ShardID,
		StartingHashKey: n,
		EndingHashKey:   parent.EndingHashKey,
		StartingSeq:     s.nextSeqNumber(),
	}
	s.Shards = append(s.Shards, left, right)
	return nil
}

// MergeShards closes two adjacent shards and opens a single child owning
// their combined hash key range.
func (s *Stream) MergeShards(shardA, shardB string) *apiError {
	a, b := s.shard(shardA), s.shard(shardB)
	if a == nil {
		return shardNotFound(s, shardA)
	}
	if b == nil {
		return shardNotFound(s, shardB)
	}
	if a.Closed() || b.Closed() {
		return &apiError{
			Code:    "InvalidArgumentException",
			Message: fmt.Sprintf("Shards %s and %s in stream %s under account %s are closed and cannot be merged.", shardA, shardB, s.Name, s.AccountID),
		}
	}
	var left, right *Shard
	one := big.NewInt(1)
	switch {
	case new(big.Int).Add(a.EndingHashKey, one).Cmp(b.StartingHashKey) == 0:
		left, right = a, b
	case new(big.Int).Add(b.EndingHashKey, one).Cmp(a.StartingHashKey) == 0:
		left, right = b, a
	default:
		return &apiError{
			Code:    "InvalidArgumentException",
			Message: fmt.Sprintf("Shards %s and %s in stream %s under account %s are not directly adjacent, they do not own contiguous hash key ranges.", shardA, shardB, s.Name, s.AccountID),
		}
	}
	left.EndingSeq = s.nextSeqNumber()
	right.EndingSeq = s.nextSeqNumber()
	s.Shards = append(s.Shards, &Shard{
		ShardID:               s.nextShardID(),
		ParentShardID:         left.ShardID,
		AdjacentParentShardID: right.ShardID,
		StartingHashKey:       left.StartingHashKey,
		EndingHashKey:         right.EndingHashKey,
		StartingSeq:           s.nextSeqNumber(),
	})
	return nil
}

// shardNotFound is the ResourceNotFoundException for a missing shard.
func shardNotFound(s *Stream, shardID string) *apiError {
	return &apiError{
		Code:    "ResourceNotFoundException",
		Message: fmt.Sprintf("Could not find shard %s in stream %s under account %s.", shardID, s.Name, s.AccountID),
	}
}
