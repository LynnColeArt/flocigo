package kinesis

import (
	"math/big"
	"sort"
)

// ===========================================================================
// Wire request/response types
// ===========================================================================

type CreateStreamInput struct {
	StreamName        string             `json:"StreamName"`
	ShardCount        int                `json:"ShardCount"`
	StreamModeDetails *StreamModeDetails `json:"StreamModeDetails"`
}

type CreateStreamOutput struct{}

type DeleteStreamInput struct {
	StreamName              string `json:"StreamName"`
	EnforceConsumerDeletion bool   `json:"EnforceConsumerDeletion"`
}

type DeleteStreamOutput struct{}

type ListStreamsInput struct {
	Limit                    int    `json:"Limit"`
	ExclusiveStartStreamName string `json:"ExclusiveStartStreamName"`
	NextToken                string `json:"NextToken"`
}

type ListStreamsOutput struct {
	StreamNames    []string `json:"StreamNames"`
	HasMoreStreams bool     `json:"HasMoreStreams"`
	NextToken      string   `json:"NextToken,omitempty"`
}

type DescribeStreamInput struct {
	StreamName            string `json:"StreamName"`
	Limit                 int    `json:"Limit"`
	ExclusiveStartShardId string `json:"ExclusiveStartShardId"`
}

type StreamDescription struct {
	StreamName              string             `json:"StreamName"`
	StreamARN               string             `json:"StreamARN"`
	StreamStatus            string             `json:"StreamStatus"`
	StreamModeDetails       *StreamModeDetails `json:"StreamModeDetails"`
	RetentionPeriodHours    int                `json:"RetentionPeriodHours"`
	StreamCreationTimestamp float64            `json:"StreamCreationTimestamp"`
	Shards                  []ShardView        `json:"Shards"`
	HasMoreShards           bool               `json:"HasMoreShards"`
	EnhancedMonitoring      []EnhancedMetrics  `json:"EnhancedMonitoring"`
	EncryptionType          string             `json:"EncryptionType"`
}

type DescribeStreamOutput struct {
	StreamDescription StreamDescription `json:"StreamDescription"`
}

type DescribeStreamSummaryInput struct {
	StreamName string `json:"StreamName"`
}

type StreamDescriptionSummary struct {
	StreamName              string             `json:"StreamName"`
	StreamARN               string             `json:"StreamARN"`
	StreamStatus            string             `json:"StreamStatus"`
	StreamModeDetails       *StreamModeDetails `json:"StreamModeDetails"`
	RetentionPeriodHours    int                `json:"RetentionPeriodHours"`
	StreamCreationTimestamp float64            `json:"StreamCreationTimestamp"`
	EnhancedMonitoring      []EnhancedMetrics  `json:"EnhancedMonitoring"`
	EncryptionType          string             `json:"EncryptionType"`
	OpenShardCount          int                `json:"OpenShardCount"`
	ConsumerCount           int                `json:"ConsumerCount"`
}

type DescribeStreamSummaryOutput struct {
	StreamDescriptionSummary StreamDescriptionSummary `json:"StreamDescriptionSummary"`
}

type ListShardsInput struct {
	StreamName              string   `json:"StreamName"`
	NextToken               string   `json:"NextToken"`
	ExclusiveStartShardId   string   `json:"ExclusiveStartShardId"`
	MaxResults              int      `json:"MaxResults"`
	StreamCreationTimestamp *float64 `json:"StreamCreationTimestamp"`
}

type ListShardsOutput struct {
	Shards    []ShardView `json:"Shards"`
	NextToken string      `json:"NextToken,omitempty"`
}

type GetShardIteratorInput struct {
	StreamName             string   `json:"StreamName"`
	ShardId                string   `json:"ShardId"`
	ShardIteratorType      string   `json:"ShardIteratorType"`
	StartingSequenceNumber string   `json:"StartingSequenceNumber"`
	Timestamp              *float64 `json:"Timestamp"`
}

type GetShardIteratorOutput struct {
	ShardIterator string `json:"ShardIterator"`
}

type GetRecordsInput struct {
	ShardIterator string `json:"ShardIterator"`
	Limit         int    `json:"Limit"`
}

// RecordView is the wire form of a stored record; Data is base64 encoded.
type RecordView struct {
	SequenceNumber              string  `json:"SequenceNumber"`
	Data                        []byte  `json:"Data"`
	PartitionKey                string  `json:"PartitionKey"`
	ApproximateArrivalTimestamp float64 `json:"ApproximateArrivalTimestamp"`
}

type GetRecordsOutput struct {
	Records            []RecordView `json:"Records"`
	NextShardIterator  *string      `json:"NextShardIterator"`
	MillisBehindLatest int64        `json:"MillisBehindLatest"`
}

type PutRecordInput struct {
	StreamName                string `json:"StreamName"`
	Data                      []byte `json:"Data"`
	PartitionKey              string `json:"PartitionKey"`
	ExplicitHashKey           string `json:"ExplicitHashKey"`
	SequenceNumberForOrdering string `json:"SequenceNumberForOrdering"`
}

type PutRecordOutput struct {
	ShardId        string `json:"ShardId"`
	SequenceNumber string `json:"SequenceNumber"`
	EncryptionType string `json:"EncryptionType"`
}

type PutRecordsInput struct {
	StreamName string                   `json:"StreamName"`
	Records    []PutRecordsRequestEntry `json:"Records"`
}

type PutRecordsRequestEntry struct {
	Data            []byte `json:"Data"`
	PartitionKey    string `json:"PartitionKey"`
	ExplicitHashKey string `json:"ExplicitHashKey"`
}

type PutRecordsResultEntry struct {
	SequenceNumber string `json:"SequenceNumber,omitempty"`
	ShardId        string `json:"ShardId,omitempty"`
	ErrorCode      string `json:"ErrorCode,omitempty"`
	ErrorMessage   string `json:"ErrorMessage,omitempty"`
}

type PutRecordsOutput struct {
	FailedRecordCount int                     `json:"FailedRecordCount"`
	Records           []PutRecordsResultEntry `json:"Records"`
}

type SplitShardInput struct {
	StreamName         string `json:"StreamName"`
	ShardToSplit       string `json:"ShardToSplit"`
	NewStartingHashKey string `json:"NewStartingHashKey"`
}

type MergeShardsInput struct {
	StreamName           string `json:"StreamName"`
	ShardToSplit         string `json:"ShardToSplit"`
	AdjacentShardToMerge string `json:"AdjacentShardToMerge"`
}

type ReshardOutput struct{}

// ===========================================================================
// Stream lifecycle
// ===========================================================================

// opCreateStream handles CreateStream; streams become ACTIVE immediately.
func (p *Provider) opCreateStream(body []byte) (interface{}, *apiError) {
	var in CreateStreamInput
	if e := decode(body, &in); e != nil {
		return nil, e
	}
	if in.StreamName == "" {
		return nil, argRequired("StreamName")
	}
	if len(in.StreamName) > 128 {
		return nil, newErr("InvalidArgumentException", "StreamName must be at most 128 characters long.")
	}

	mode := StreamModeProvisioned
	if in.StreamModeDetails != nil && in.StreamModeDetails.StreamMode != "" {
		mode = in.StreamModeDetails.StreamMode
		if mode != StreamModeProvisioned && mode != StreamModeOnDemand {
			return nil, newErr("InvalidArgumentException", "Unknown StreamMode %s.", mode)
		}
	}
	shardCount := in.ShardCount
	if shardCount == 0 {
		shardCount = 1
	}
	if shardCount < 0 {
		return nil, newErr("InvalidArgumentException", "ShardCount must be a non-negative integer.")
	}
	if mode == StreamModeOnDemand {
		shardCount = 1 // on-demand streams report a single open shard
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.streams[in.StreamName]; exists {
		return nil, newErr("ResourceInUseException", "Stream %s under account %s that you tried to create already exists.", in.StreamName, p.AccountID)
	}
	if len(p.streams) >= 100 {
		return nil, newErr("LimitExceededException", "Reached the maximum number of active streams (100).")
	}
	s := newStream(in.StreamName, p.Region, p.AccountID, mode, shardCount, p.Now())
	p.streams[s.Name] = s
	p.order = append(p.order, s.Name)
	return &CreateStreamOutput{}, nil
}

// opDeleteStream handles DeleteStream; deletion is immediate.
func (p *Provider) opDeleteStream(body []byte) (interface{}, *apiError) {
	var in DeleteStreamInput
	if e := decode(body, &in); e != nil {
		return nil, e
	}
	if in.StreamName == "" {
		return nil, argRequired("StreamName")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.getStream(in.StreamName) == nil {
		return nil, p.notFound(in.StreamName)
	}
	delete(p.streams, in.StreamName)
	return &DeleteStreamOutput{}, nil
}

// opListStreams handles ListStreams with creation-order paging.
func (p *Provider) opListStreams(body []byte) (interface{}, *apiError) {
	var in ListStreamsInput
	if e := decode(body, &in); e != nil {
		return nil, e
	}
	if in.Limit < 0 {
		return nil, newErr("InvalidArgumentException", "Limit must not be negative.")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	exclusive := in.ExclusiveStartStreamName
	if in.NextToken != "" {
		exclusive = in.NextToken
	}
	limit := in.Limit
	if limit <= 0 {
		limit = 10000
	}

	started := exclusive == ""
	var selected []string
	for _, name := range p.orderList() {
		if !started {
			if name == exclusive {
				started = true
			}
			continue
		}
		selected = append(selected, name)
	}
	if selected == nil {
		selected = []string{}
	}
	hasMore := false
	if len(selected) > limit {
		selected = selected[:limit]
		hasMore = true
	}
	out := &ListStreamsOutput{StreamNames: selected, HasMoreStreams: hasMore}
	if hasMore {
		out.NextToken = selected[len(selected)-1]
	}
	return out, nil
}

// ===========================================================================
// Describe / List shards
// ===========================================================================

// shardPage selects a page of shards for DescribeStream / ListShards.
func shardPage(s *Stream, limit int, exclusiveStartShardID string) ([]ShardView, bool) {
	if limit <= 0 {
		limit = 100
	}
	all := s.sortedShards()
	start := 0
	if exclusiveStartShardID != "" {
		start = sort.Search(len(all), func(i int) bool { return all[i].ShardID > exclusiveStartShardID })
	}
	end := start + limit
	hasMore := end < len(all)
	if end > len(all) {
		end = len(all)
	}
	out := make([]ShardView, 0, end-start)
	for _, sh := range all[start:end] {
		out = append(out, sh.view())
	}
	return out, hasMore
}

// streamDescription builds the stream level part of describe responses.
func streamDescription(s *Stream) StreamDescription {
	return StreamDescription{
		StreamName:              s.Name,
		StreamARN:               s.ARN(),
		StreamStatus:            s.Status,
		StreamModeDetails:       &StreamModeDetails{StreamMode: s.Mode},
		RetentionPeriodHours:    DefaultRetentionHours,
		StreamCreationTimestamp: epochSeconds(s.CreatedAt),
		EnhancedMonitoring:      []EnhancedMetrics{{ShardLevelMetrics: []string{}}},
		EncryptionType:          EncryptionTypeNone,
	}
}

// opDescribeStream handles DescribeStream with shard pagination.
func (p *Provider) opDescribeStream(body []byte) (interface{}, *apiError) {
	var in DescribeStreamInput
	if e := decode(body, &in); e != nil {
		return nil, e
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.getStream(in.StreamName)
	if s == nil {
		return nil, p.notFound(in.StreamName)
	}
	shards, hasMore := shardPage(s, in.Limit, in.ExclusiveStartShardId)
	desc := streamDescription(s)
	desc.Shards = shards
	desc.HasMoreShards = hasMore
	return &DescribeStreamOutput{StreamDescription: desc}, nil
}

// opDescribeStreamSummary handles DescribeStreamSummary.
func (p *Provider) opDescribeStreamSummary(body []byte) (interface{}, *apiError) {
	var in DescribeStreamSummaryInput
	if e := decode(body, &in); e != nil {
		return nil, e
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.getStream(in.StreamName)
	if s == nil {
		return nil, p.notFound(in.StreamName)
	}
	return &DescribeStreamSummaryOutput{StreamDescriptionSummary: StreamDescriptionSummary{
		StreamName:              s.Name,
		StreamARN:               s.ARN(),
		StreamStatus:            s.Status,
		StreamModeDetails:       &StreamModeDetails{StreamMode: s.Mode},
		RetentionPeriodHours:    DefaultRetentionHours,
		StreamCreationTimestamp: epochSeconds(s.CreatedAt),
		EnhancedMonitoring:      []EnhancedMetrics{{ShardLevelMetrics: []string{}}},
		EncryptionType:          EncryptionTypeNone,
		OpenShardCount:          len(s.openShards()),
		ConsumerCount:           0,
	}}, nil
}

// opListShards handles ListShards.
func (p *Provider) opListShards(body []byte) (interface{}, *apiError) {
	var in ListShardsInput
	if e := decode(body, &in); e != nil {
		return nil, e
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.getStream(in.StreamName)
	if s == nil {
		return nil, p.notFound(in.StreamName)
	}
	exclusive := in.ExclusiveStartShardId
	if in.NextToken != "" {
		exclusive = in.NextToken
	}
	shards, hasMore := shardPage(s, in.MaxResults, exclusive)
	out := &ListShardsOutput{Shards: shards}
	if hasMore && len(shards) > 0 {
		out.NextToken = shards[len(shards)-1].ShardId
	}
	return out, nil
}

// ===========================================================================
// Shard iterators
// ===========================================================================

// opGetShardIterator handles GetShardIterator for all iterator types.
func (p *Provider) opGetShardIterator(body []byte) (interface{}, *apiError) {
	var in GetShardIteratorInput
	if e := decode(body, &in); e != nil {
		return nil, e
	}
	if in.StreamName == "" {
		return nil, argRequired("StreamName")
	}
	if in.ShardId == "" {
		return nil, argRequired("ShardId")
	}
	if in.ShardIteratorType == "" {
		return nil, argRequired("ShardIteratorType")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.getStream(in.StreamName)
	if s == nil {
		return nil, p.notFound(in.StreamName)
	}
	sh := s.shard(in.ShardId)
	if sh == nil {
		return nil, shardNotFound(s, in.ShardId)
	}

	iterType := in.ShardIteratorType
	if iterType == IteratorTypeLatestAlias {
		iterType = IteratorTypeLatest
	}

	it := shardIterator{Stream: s.Name, Shard: sh.ShardID, Type: iterType}
	switch iterType {
	case IteratorTypeAtSequenceNumber, IteratorTypeAfterSequenceNumber:
		if in.StartingSequenceNumber == "" {
			return nil, newErr("InvalidArgumentException", "StartingSequenceNumber is required when ShardIteratorType is %s.", iterType)
		}
		if _, ok := new(big.Int).SetString(in.StartingSequenceNumber, 10); !ok {
			return nil, newErr("InvalidArgumentException", "StartingSequenceNumber %s is not a valid sequence number.", in.StartingSequenceNumber)
		}
		it.Seq = in.StartingSequenceNumber
	case IteratorTypeAtTimestamp:
		if in.Timestamp == nil {
			return nil, newErr("InvalidArgumentException", "Timestamp is required when ShardIteratorType is AT_TIMESTAMP.")
		}
		it.TS = *in.Timestamp
	case IteratorTypeTrimHorizon, IteratorTypeLatest:
		// no additional input required
	default:
		return nil, newErr("InvalidArgumentException", "Unknown ShardIteratorType %s. Valid values are LATEST, TRIM_HORIZON, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER and AT_TIMESTAMP.", in.ShardIteratorType)
	}

	index, err := it.position(sh)
	if err != nil {
		return nil, newErr("InvalidArgumentException", "invalid shard iterator: %v", err)
	}
	it.Index = index
	return &GetShardIteratorOutput{ShardIterator: it.encode()}, nil
}

// opGetRecords handles GetRecords, including iterator continuation and the
// null NextShardIterator of exhausted closed shards.
func (p *Provider) opGetRecords(body []byte) (interface{}, *apiError) {
	var in GetRecordsInput
	if e := decode(body, &in); e != nil {
		return nil, e
	}
	if in.ShardIterator == "" {
		return nil, argRequired("ShardIterator")
	}
	if in.Limit > 10000 {
		return nil, newErr("InvalidArgumentException", "Limit must not exceed 10,000.")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	it, ok := decodeShardIterator(in.ShardIterator)
	if !ok {
		return nil, expiredIterator()
	}
	s := p.getStream(it.Stream)
	if s == nil {
		return nil, expiredIterator()
	}
	sh := s.shard(it.Shard)
	if sh == nil {
		return nil, expiredIterator()
	}

	start := it.Index
	if start < 0 {
		start = 0
	}
	if start > len(sh.Records) {
		start = len(sh.Records)
	}
	limit := in.Limit
	if limit <= 0 {
		limit = 10000
	}
	end := start + limit
	if end > len(sh.Records) {
		end = len(sh.Records)
	}

	out := &GetRecordsOutput{Records: make([]RecordView, 0, end-start)}
	for _, rec := range sh.Records[start:end] {
		out.Records = append(out.Records, RecordView{
			SequenceNumber:              rec.SequenceNumber,
			Data:                        rec.Data,
			PartitionKey:                rec.PartitionKey,
			ApproximateArrivalTimestamp: epochSeconds(rec.Arrival),
		})
	}
	served := end - start

	if sh.Closed() && start+served >= len(sh.Records) {
		out.NextShardIterator = nil
	} else {
		next := it
		next.Index = start + served
		token := next.encode()
		out.NextShardIterator = &token
	}

	var behind int64
	if n := len(sh.Records); n > 0 {
		if d := p.Now().Sub(sh.Records[n-1].Arrival); d > 0 {
			behind = d.Milliseconds()
		}
	}
	out.MillisBehindLatest = behind
	return out, nil
}

// ===========================================================================
// Records
// ===========================================================================

// appendRecord computes the routing hash key, resolves the owning open
// shard and appends the record with the next sequence number. Callers must
// hold p.mu.
func (p *Provider) appendRecord(s *Stream, partitionKey, explicitHashKey string, data []byte) (*Shard, string, *apiError) {
	key := partitionKeyHash(partitionKey)
	if explicitHashKey != "" {
		key = parseHashKey(explicitHashKey)
	}
	sh := s.shardForKey(key)
	if sh == nil {
		return nil, "", newErr("InvalidArgumentException", "Stream %s under account %s has no open shards.", s.Name, s.AccountID)
	}
	seq := s.nextSeqNumber()
	sh.Records = append(sh.Records, Record{
		PartitionKey:    partitionKey,
		ExplicitHashKey: explicitHashKey,
		Data:            append([]byte(nil), data...),
		SequenceNumber:  seq,
		Arrival:         p.Now(),
	})
	return sh, seq, nil
}

// opPutRecord handles PutRecord.
func (p *Provider) opPutRecord(body []byte) (interface{}, *apiError) {
	var in PutRecordInput
	if e := decode(body, &in); e != nil {
		return nil, e
	}
	if in.StreamName == "" {
		return nil, argRequired("StreamName")
	}
	if len(in.Data) == 0 {
		return nil, argRequired("Data")
	}
	if in.PartitionKey == "" {
		return nil, argRequired("PartitionKey")
	}
	if len(in.PartitionKey) > 256 {
		return nil, newErr("InvalidArgumentException", "PartitionKey must be at most 256 characters long.")
	}
	if len(in.ExplicitHashKey) > 256 {
		return nil, newErr("InvalidArgumentException", "ExplicitHashKey must be at most 256 characters long.")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.getStream(in.StreamName)
	if s == nil {
		return nil, p.notFound(in.StreamName)
	}
	sh, seq, aerr := p.appendRecord(s, in.PartitionKey, in.ExplicitHashKey, in.Data)
	if aerr != nil {
		return nil, aerr
	}
	return &PutRecordOutput{
		ShardId:        sh.ShardID,
		SequenceNumber: seq,
		EncryptionType: EncryptionTypeNone,
	}, nil
}

// opPutRecords handles PutRecords (single request level validation, like
// the real service).
func (p *Provider) opPutRecords(body []byte) (interface{}, *apiError) {
	var in PutRecordsInput
	if e := decode(body, &in); e != nil {
		return nil, e
	}
	if in.StreamName == "" {
		return nil, argRequired("StreamName")
	}
	if len(in.Records) == 0 {
		return nil, argRequired("Records")
	}
	for i, entry := range in.Records {
		if len(entry.Data) == 0 {
			return nil, newErr("InvalidArgumentException", "Records[%d].Data is required.", i)
		}
		if entry.PartitionKey == "" {
			return nil, newErr("InvalidArgumentException", "Records[%d].PartitionKey is required.", i)
		}
		if len(entry.PartitionKey) > 256 {
			return nil, newErr("InvalidArgumentException", "Records[%d].PartitionKey must be at most 256 characters long.", i)
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.getStream(in.StreamName)
	if s == nil {
		return nil, p.notFound(in.StreamName)
	}
	out := &PutRecordsOutput{Records: make([]PutRecordsResultEntry, 0, len(in.Records))}
	for _, entry := range in.Records {
		sh, seq, aerr := p.appendRecord(s, entry.PartitionKey, entry.ExplicitHashKey, entry.Data)
		if aerr != nil {
			out.FailedRecordCount++
			out.Records = append(out.Records, PutRecordsResultEntry{
				ErrorCode:    "InternalFailure",
				ErrorMessage: aerr.Message,
			})
			continue
		}
		out.Records = append(out.Records, PutRecordsResultEntry{
			SequenceNumber: seq,
			ShardId:        sh.ShardID,
		})
	}
	return out, nil
}

// ===========================================================================
// Re-sharding
// ===========================================================================

// opSplitShard handles SplitShard.
func (p *Provider) opSplitShard(body []byte) (interface{}, *apiError) {
	var in SplitShardInput
	if e := decode(body, &in); e != nil {
		return nil, e
	}
	if in.StreamName == "" {
		return nil, argRequired("StreamName")
	}
	if in.ShardToSplit == "" {
		return nil, argRequired("ShardToSplit")
	}
	if in.NewStartingHashKey == "" {
		return nil, argRequired("NewStartingHashKey")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.getStream(in.StreamName)
	if s == nil {
		return nil, p.notFound(in.StreamName)
	}
	if e := s.SplitShard(in.ShardToSplit, in.NewStartingHashKey); e != nil {
		return nil, e
	}
	return &ReshardOutput{}, nil
}

// opMergeShards handles MergeShards.
func (p *Provider) opMergeShards(body []byte) (interface{}, *apiError) {
	var in MergeShardsInput
	if e := decode(body, &in); e != nil {
		return nil, e
	}
	if in.StreamName == "" {
		return nil, argRequired("StreamName")
	}
	if in.ShardToSplit == "" {
		return nil, argRequired("ShardToSplit")
	}
	if in.AdjacentShardToMerge == "" {
		return nil, argRequired("AdjacentShardToMerge")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.getStream(in.StreamName)
	if s == nil {
		return nil, p.notFound(in.StreamName)
	}
	if e := s.MergeShards(in.ShardToSplit, in.AdjacentShardToMerge); e != nil {
		return nil, e
	}
	return &ReshardOutput{}, nil
}
