// Package s3vectors implements the S3 Vectors service: vector buckets,
// vector indexes and vector data (PutVectors, GetVectors, ListVectors,
// DeleteVectors, SearchVectors) on top of an in-memory store.
//
// It is a Go port of Floci's S3VectorsService and keeps the same wire style:
// every operation is a plain JSON payload (awsJson) dispatched by the
// X-Amz-Target header. The DDB handler multiplexes that JSON endpoint, so
// SearchVectors and friends arrive there first; route them into the vector
// service with:
//
//    if s3vectors.IsS3VectorsTarget(target) {
//        s3vectors.Default().ServeJSON(w, target, body)
//        return
//    }
//
// Target dispatch is namespace agnostic: it matches the operation name after
// the last dot, case-insensitively ("S3Vectors.PutVectors",
// "AmazonS3Vectors.SearchVectors", "SearchVectors", ...). "QueryVectors" is
// accepted as an alias for SearchVectors. The service is also an
// http.Handler, so it can be mounted directly (POST /<Operation> or via the
// X-Amz-Target header).
//
// Errors use the awsJson envelope (a __type code in the body plus the
// x-amzn-ErrorType header): 400 ValidationException, 404
// ResourceNotFoundException, 409 ConflictException and 400
// UnknownOperationException.
//
// All math is dot-product based float32 arithmetic with no external
// dependency. For the cosine metric SearchVectors reports the distance as
// 1 minus the cosine similarity; the euclidean metric reports the euclidean
// distance. Lower means more similar. Results are sorted by distance (ties
// broken by key) and truncated to topK (default 10).
package s3vectors

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultRegion and DefaultAccountID are used when composing ARNs. Both
	// fields on Service are public and can be overridden before use.
	DefaultRegion    = "us-east-1"
	DefaultAccountID = "123456789012"

	// MaxDimension is the largest supported vector dimension.
	MaxDimension = 4096

	// DefaultTopK is used when SearchVectors omits topK.
	DefaultTopK = 10

	dataTypeFloat32   = "float32"
	distanceCosine    = "cosine"
	distanceEuclidean = "euclidean"

	maxSegments = 32
)

// APIError is rendered as an awsJson error response.
type APIError struct {
	Status  int
	Code    string
	Message string
}

// Error implements the error interface.
func (e *APIError) Error() string { return e.Code + ": " + e.Message }

func validationErr(format string, args ...any) *APIError {
	return &APIError{
		Status:  http.StatusBadRequest,
		Code:    "ValidationException",
		Message: fmt.Sprintf(format, args...),
	}
}

func notFoundErr(format string, args ...any) *APIError {
	return &APIError{
		Status:  http.StatusNotFound,
		Code:    "ResourceNotFoundException",
		Message: fmt.Sprintf(format, args...),
	}
}

func conflictErr(format string, args ...any) *APIError {
	return &APIError{
		Status:  http.StatusConflict,
		Code:    "ConflictException",
		Message: fmt.Sprintf(format, args...),
	}
}

// EncryptionConfiguration describes server side encryption of a bucket.
type EncryptionConfiguration struct {
	SSEAlgorithm string `json:"sseAlgorithm,omitempty"`
	KMSKeyARN    string `json:"kmsKeyArn,omitempty"`
}

// VectorBucket is a container for vector indexes.
type VectorBucket struct {
	Name         string
	ARN          string
	CreationTime int64 // unix seconds
	Encryption   *EncryptionConfiguration
	Tags         map[string]string
	Indexes      map[string]*VectorIndex
}

// VectorIndex stores vectors of one dimension with one distance metric.
type VectorIndex struct {
	Name                      string
	Bucket                    string
	ARN                       string
	CreationTime              int64
	DataType                  string
	Dimension                 int
	DistanceMetric            string
	NonfilterableMetadataKeys []string
	Vectors                   map[string]*StoredVector
}

// StoredVector is a single vector record.
type StoredVector struct {
	Key      string
	Data     []float32
	Metadata map[string]any
}

// Service stores all S3 Vectors resources in memory.
type Service struct {
	mu        sync.RWMutex
	buckets   map[string]*VectorBucket
	Region    string
	AccountID string
}

// NewService returns a ready-to-use S3 Vectors service.
func NewService() *Service {
	return &Service{
		buckets:   map[string]*VectorBucket{},
		Region:    DefaultRegion,
		AccountID: DefaultAccountID,
	}
}

var (
	defaultService     *Service
	defaultServiceOnce sync.Once
)

// Default returns a process-wide instance, handy for the DDB handler wiring.
func Default() *Service {
	defaultServiceOnce.Do(func() { defaultService = NewService() })
	return defaultService
}

func (s *Service) bucketARN(name string) string {
	return "arn:aws:s3vectors:" + s.Region + ":" + s.AccountID + ":bucket/" + name
}

func (s *Service) indexARN(bucket, index string) string {
	return "arn:aws:s3vectors:" + s.Region + ":" + s.AccountID + ":bucket/" + bucket + "/index/" + index
}

// lookupBucket and lookupIndex expect the caller to hold s.mu.
func (s *Service) lookupBucket(name string) (*VectorBucket, *APIError) {
	b, ok := s.buckets[name]
	if !ok {
		return nil, notFoundErr("The specified vector bucket does not exist: %s", name)
	}
	return b, nil
}

func (s *Service) lookupIndex(bucketName, indexName string) (*VectorIndex, *APIError) {
	b, aerr := s.lookupBucket(bucketName)
	if aerr != nil {
		return nil, aerr
	}
	idx, ok := b.Indexes[indexName]
	if !ok {
		return nil, notFoundErr("The specified vector index does not exist: %s", indexName)
	}
	return idx, nil
}

var resourceNameRe = regexp.MustCompile("^[a-zA-Z0-9][a-zA-Z0-9._-]{1,61}[a-zA-Z0-9]$")

func validateResourceName(kind, name string) *APIError {
	if name == "" {
		return validationErr("%s name must not be empty.", kind)
	}
	if len(name) < 3 || len(name) > 63 || !resourceNameRe.MatchString(name) {
		return validationErr("The %s name %q is not valid. Names must be 3 to 63 characters of letters, digits, dots, underscores or hyphens.", kind, name)
	}
	return nil
}

func validateEncryption(e *EncryptionConfiguration) *APIError {
	switch e.SSEAlgorithm {
	case "", "AES256", "aws:kms":
		return nil
	default:
		return validationErr("The server side encryption algorithm %q is not supported.", e.SSEAlgorithm)
	}
}

// decodeBody parses a JSON payload, keeping number literals intact so that
// metadata round-trips byte-for-byte.
func decodeBody(body []byte, v any) *APIError {
	if len(bytes.TrimSpace(body)) == 0 {
		body = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return validationErr("The request body is not valid JSON: %v", err)
	}
	return nil
}

func encodeToken(name string) string {
	return base64.URLEncoding.EncodeToString([]byte("v1:" + name))
}

func decodeToken(token string) string {
	if raw, err := base64.URLEncoding.DecodeString(token); err == nil {
		if s := string(raw); strings.HasPrefix(s, "v1:") {
			return s[3:]
		}
	}
	return token
}

func fnv32(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// DataPayload carries a float32 vector as {"float32": [...]}. A bare array
// is accepted on input for robustness.
type DataPayload struct {
	Float32 []float32 `json:"float32"`
}

// UnmarshalJSON accepts both {"float32": [...]} and the bare array form.
func (d *DataPayload) UnmarshalJSON(b []byte) error {
	trimmed := bytes.TrimSpace(b)
	if bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	var obj struct {
		Float32 []float32 `json:"float32"`
	}
	if err := json.Unmarshal(trimmed, &obj); err == nil && obj.Float32 != nil {
		d.Float32 = obj.Float32
		return nil
	}
	var arr []float32
	if err := json.Unmarshal(trimmed, &arr); err == nil {
		d.Float32 = arr
		return nil
	}
	return validationErr("Vector data must be an object with a float32 array.")
}

// MarshalJSON always emits {"float32": [...]}.
func (d DataPayload) MarshalJSON() ([]byte, error) {
	if d.Float32 == nil {
		return []byte(`{"float32":[]}`), nil
	}
	type wire struct {
		Float32 []float32 `json:"float32"`
	}
	return json.Marshal(wire{Float32: d.Float32})
}

// Dot returns the dot product of two float32 vectors, accumulated in float64.
func Dot(a, b []float32) float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	sum := 0.0
	for i := 0; i < n; i++ {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}

// Norm returns the euclidean norm of v.
func Norm(v []float32) float64 { return math.Sqrt(Dot(v, v)) }

// CosineSimilarity computes cos(a, b) via float32 dot products. Zero vectors
// score 0. The result is clamped to [-1, 1] against float drift.
func CosineSimilarity(a, b []float32) float64 {
	na, nb := Norm(a), Norm(b)
	if na == 0 || nb == 0 {
		return 0
	}
	sim := Dot(a, b) / (na * nb)
	if sim > 1 {
		sim = 1
	} else if sim < -1 {
		sim = -1
	}
	return sim
}

// EuclideanDistance returns the L2 distance between a and b.
func EuclideanDistance(a, b []float32) float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	sum := 0.0
	for i := 0; i < n; i++ {
		diff := float64(a[i]) - float64(b[i])
		sum += diff * diff
	}
	return math.Sqrt(sum)
}

// distance scores query against stored using the index metric; lower is
// closer. Cosine is reported as 1 minus the cosine similarity.
func distance(metric string, query, stored []float32) float64 {
	if metric == distanceEuclidean {
		return EuclideanDistance(query, stored)
	}
	return 1 - CosineSimilarity(query, stored)
}

func finiteFloat32(vals []float32) bool {
	for _, f := range vals {
		if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Vector buckets
// ---------------------------------------------------------------------------

type CreateVectorBucketInput struct {
	VectorBucketName        string                   `json:"vectorBucketName"`
	EncryptionConfiguration *EncryptionConfiguration `json:"encryptionConfiguration"`
	Tags                    map[string]string        `json:"tags"`
}

type CreateVectorBucketOutput struct {
	VectorBucketARN string `json:"vectorBucketArn"`
}

// CreateVectorBucket creates a new vector bucket.
func (s *Service) CreateVectorBucket(in *CreateVectorBucketInput) (*CreateVectorBucketOutput, *APIError) {
	if aerr := validateResourceName("vector bucket", in.VectorBucketName); aerr != nil {
		return nil, aerr
	}
	if in.EncryptionConfiguration != nil {
		if aerr := validateEncryption(in.EncryptionConfiguration); aerr != nil {
			return nil, aerr
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.buckets[in.VectorBucketName]; exists {
		return nil, conflictErr("A vector bucket with the name %s already exists.", in.VectorBucketName)
	}
	bucket := &VectorBucket{
		Name:         in.VectorBucketName,
		ARN:          s.bucketARN(in.VectorBucketName),
		CreationTime: time.Now().Unix(),
		Encryption:   in.EncryptionConfiguration,
		Tags:         in.Tags,
		Indexes:      map[string]*VectorIndex{},
	}
	s.buckets[bucket.Name] = bucket
	return &CreateVectorBucketOutput{VectorBucketARN: bucket.ARN}, nil
}

type DeleteVectorBucketInput struct {
	VectorBucketName string `json:"vectorBucketName"`
}

type DeleteVectorBucketOutput struct{}

// DeleteVectorBucket deletes an empty vector bucket.
func (s *Service) DeleteVectorBucket(in *DeleteVectorBucketInput) (*DeleteVectorBucketOutput, *APIError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	bucket, aerr := s.lookupBucket(in.VectorBucketName)
	if aerr != nil {
		return nil, aerr
	}
	if len(bucket.Indexes) > 0 {
		return nil, conflictErr("The vector bucket %s still contains vector indexes. Delete them before deleting the bucket.", in.VectorBucketName)
	}
	delete(s.buckets, in.VectorBucketName)
	return &DeleteVectorBucketOutput{}, nil
}

type GetVectorBucketInput struct {
	VectorBucketName string `json:"vectorBucketName"`
}

// VectorBucketProfile is the API view of a vector bucket.
type VectorBucketProfile struct {
	CreationTime            int64                    `json:"creationTime"`
	EncryptionConfiguration *EncryptionConfiguration `json:"encryptionConfiguration,omitempty"`
	VectorBucketARN         string                   `json:"vectorBucketArn"`
	VectorBucketName        string                   `json:"vectorBucketName"`
}

type GetVectorBucketOutput struct {
	VectorBucket *VectorBucketProfile `json:"vectorBucket"`
}

// GetVectorBucket returns one vector bucket.
func (s *Service) GetVectorBucket(in *GetVectorBucketInput) (*GetVectorBucketOutput, *APIError) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	bucket, aerr := s.lookupBucket(in.VectorBucketName)
	if aerr != nil {
		return nil, aerr
	}
	return &GetVectorBucketOutput{VectorBucket: bucketProfile(bucket)}, nil
}

func bucketProfile(bucket *VectorBucket) *VectorBucketProfile {
	return &VectorBucketProfile{
		CreationTime:            bucket.CreationTime,
		EncryptionConfiguration: bucket.Encryption,
		VectorBucketARN:         bucket.ARN,
		VectorBucketName:        bucket.Name,
	}
}

type ListVectorBucketsInput struct {
	MaxResults *int   `json:"maxResults"`
	NextToken  string `json:"nextToken"`
	Prefix     string `json:"prefix"`
}

// VectorBucketSummary is a list item of ListVectorBuckets.
type VectorBucketSummary struct {
	CreationTime     int64  `json:"creationTime"`
	Region           string `json:"region"`
	VectorBucketARN  string `json:"vectorBucketArn"`
	VectorBucketName string `json:"vectorBucketName"`
}

type ListVectorBucketsOutput struct {
	NextToken     string                `json:"nextToken,omitempty"`
	VectorBuckets []VectorBucketSummary `json:"vectorBuckets"`
}

// ListVectorBuckets returns buckets ordered by name, honouring prefix and
// nextToken. Without maxResults everything is returned.
func (s *Service) ListVectorBuckets(in *ListVectorBucketsInput) (*ListVectorBucketsOutput, *APIError) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.buckets))
	for name := range s.buckets {
		names = append(names, name)
	}
	sort.Strings(names)
	var after string
	if in.NextToken != "" {
		after = decodeToken(in.NextToken)
	}
	out := &ListVectorBucketsOutput{VectorBuckets: []VectorBucketSummary{}}
	for _, name := range names {
		if in.Prefix != "" && !strings.HasPrefix(name, in.Prefix) {
			continue
		}
		if after != "" && name <= after {
			continue
		}
		if in.MaxResults != nil && *in.MaxResults > 0 && len(out.VectorBuckets) >= *in.MaxResults {
			out.NextToken = encodeToken(name)
			break
		}
		bucket := s.buckets[name]
		out.VectorBuckets = append(out.VectorBuckets, VectorBucketSummary{
			CreationTime:     bucket.CreationTime,
			Region:           s.Region,
			VectorBucketARN:  bucket.ARN,
			VectorBucketName: bucket.Name,
		})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Vector indexes
// ---------------------------------------------------------------------------

type CreateVectorIndexInput struct {
	DataType                  string   `json:"dataType"`
	Dimension                 int      `json:"dimension"`
	DistanceMetric            string   `json:"distanceMetric"`
	IndexName                 string   `json:"indexName"`
	NonfilterableMetadataKeys []string `json:"nonfilterableMetadataKeys"`
	VectorBucketName          string   `json:"vectorBucketName"`
}

type CreateVectorIndexOutput struct {
	IndexARN string `json:"indexArn"`
}

// CreateVectorIndex creates a vector index inside an existing bucket.
func (s *Service) CreateVectorIndex(in *CreateVectorIndexInput) (*CreateVectorIndexOutput, *APIError) {
	if aerr := validateResourceName("vector index", in.IndexName); aerr != nil {
		return nil, aerr
	}
	dataType := in.DataType
	if dataType == "" {
		dataType = dataTypeFloat32
	}
	if dataType != dataTypeFloat32 {
		return nil, validationErr("The data type %q is not supported; only float32 is supported.", in.DataType)
	}
	if in.Dimension < 1 || in.Dimension > MaxDimension {
		return nil, validationErr("The dimension %d is not valid. The dimension must be between 1 and %d.", in.Dimension, MaxDimension)
	}
	metric := in.DistanceMetric
	if metric == "" {
		metric = distanceCosine
	}
	if metric != distanceCosine && metric != distanceEuclidean {
		return nil, validationErr("The distance metric %q is not supported. Supported metrics: cosine, euclidean.", in.DistanceMetric)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	bucket, aerr := s.lookupBucket(in.VectorBucketName)
	if aerr != nil {
		return nil, aerr
	}
	if _, exists := bucket.Indexes[in.IndexName]; exists {
		return nil, conflictErr("A vector index with the name %s already exists in the vector bucket %s.", in.IndexName, in.VectorBucketName)
	}
	idx := &VectorIndex{
		Name:                      in.IndexName,
		Bucket:                    in.VectorBucketName,
		ARN:                       s.indexARN(in.VectorBucketName, in.IndexName),
		CreationTime:              time.Now().Unix(),
		DataType:                  dataType,
		Dimension:                 in.Dimension,
		DistanceMetric:            metric,
		NonfilterableMetadataKeys: in.NonfilterableMetadataKeys,
		Vectors:                   map[string]*StoredVector{},
	}
	bucket.Indexes[idx.Name] = idx
	return &CreateVectorIndexOutput{IndexARN: idx.ARN}, nil
}

type DeleteVectorIndexInput struct {
	IndexName        string `json:"indexName"`
	VectorBucketName string `json:"vectorBucketName"`
}

type DeleteVectorIndexOutput struct{}

// DeleteVectorIndex deletes a vector index and all of its vectors.
func (s *Service) DeleteVectorIndex(in *DeleteVectorIndexInput) (*DeleteVectorIndexOutput, *APIError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	bucket, aerr := s.lookupBucket(in.VectorBucketName)
	if aerr != nil {
		return nil, aerr
	}
	if _, exists := bucket.Indexes[in.IndexName]; !exists {
		return nil, notFoundErr("The specified vector index does not exist: %s", in.IndexName)
	}
	delete(bucket.Indexes, in.IndexName)
	return &DeleteVectorIndexOutput{}, nil
}

type GetVectorIndexInput struct {
	IndexName        string `json:"indexName"`
	VectorBucketName string `json:"vectorBucketName"`
}

// VectorIndexProfile is the API view of a vector index.
type VectorIndexProfile struct {
	CreationTime              int64    `json:"creationTime"`
	DataType                  string   `json:"dataType"`
	Dimension                 int      `json:"dimension"`
	DistanceMetric            string   `json:"distanceMetric"`
	IndexARN                  string   `json:"indexArn"`
	IndexName                 string   `json:"indexName"`
	NonfilterableMetadataKeys []string `json:"nonfilterableMetadataKeys,omitempty"`
}

type GetVectorIndexOutput struct {
	VectorIndex *VectorIndexProfile `json:"index"`
}

// GetVectorIndex returns one vector index.
func (s *Service) GetVectorIndex(in *GetVectorIndexInput) (*GetVectorIndexOutput, *APIError) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	idx, aerr := s.lookupIndex(in.VectorBucketName, in.IndexName)
	if aerr != nil {
		return nil, aerr
	}
	return &GetVectorIndexOutput{VectorIndex: indexProfile(idx)}, nil
}

func indexProfile(idx *VectorIndex) *VectorIndexProfile {
	return &VectorIndexProfile{
		CreationTime:              idx.CreationTime,
		DataType:                  idx.DataType,
		Dimension:                 idx.Dimension,
		DistanceMetric:            idx.DistanceMetric,
		IndexARN:                  idx.ARN,
		IndexName:                 idx.Name,
		NonfilterableMetadataKeys: idx.NonfilterableMetadataKeys,
	}
}

type ListVectorIndexesInput struct {
	MaxResults       *int   `json:"maxResults"`
	NextToken        string `json:"nextToken"`
	VectorBucketName string `json:"vectorBucketName"`
}

type ListVectorIndexesOutput struct {
	NextToken     string               `json:"nextToken,omitempty"`
	VectorIndexes []VectorIndexProfile `json:"vectorIndexes"`
}

// ListVectorIndexes returns the indexes of a bucket ordered by name.
func (s *Service) ListVectorIndexes(in *ListVectorIndexesInput) (*ListVectorIndexesOutput, *APIError) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	bucket, aerr := s.lookupBucket(in.VectorBucketName)
	if aerr != nil {
		return nil, aerr
	}
	names := make([]string, 0, len(bucket.Indexes))
	for name := range bucket.Indexes {
		names = append(names, name)
	}
	sort.Strings(names)
	var after string
	if in.NextToken != "" {
		after = decodeToken(in.NextToken)
	}
	out := &ListVectorIndexesOutput{VectorIndexes: []VectorIndexProfile{}}
	for _, name := range names {
		if after != "" && name <= after {
			continue
		}
		if in.MaxResults != nil && *in.MaxResults > 0 && len(out.VectorIndexes) >= *in.MaxResults {
			out.NextToken = encodeToken(name)
			break
		}
		out.VectorIndexes = append(out.VectorIndexes, *indexProfile(bucket.Indexes[name]))
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Vector records
// ---------------------------------------------------------------------------

// VectorRecord is a vector in responses: the key plus optional data/metadata.
type VectorRecord struct {
	Key      string         `json:"key"`
	Data     *DataPayload   `json:"data,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type PutVectorsInput struct {
	IndexName        string           `json:"indexName"`
	VectorBucketName string           `json:"vectorBucketName"`
	Vectors          []PutVectorInput `json:"vectors"`
}

type PutVectorInput struct {
	Key      string         `json:"key"`
	Data     *DataPayload   `json:"data"`
	Metadata map[string]any `json:"metadata"`
}

type PutVectorsOutput struct{}

// PutVectors stores or replaces vectors inside an index. Every vector must
// match the index dimension and contain finite float32 values.
func (s *Service) PutVectors(in *PutVectorsInput) (*PutVectorsOutput, *APIError) {
	if len(in.Vectors) == 0 {
		return nil, validationErr("At least one vector is required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	idx, aerr := s.lookupIndex(in.VectorBucketName, in.IndexName)
	if aerr != nil {
		return nil, aerr
	}
	for i := range in.Vectors {
		v := &in.Vectors[i]
		if v.Key == "" {
			return nil, validationErr("The vector key must not be empty.")
		}
		if v.Data == nil || len(v.Data.Float32) == 0 {
			return nil, validationErr("The vector %q must provide float32 data.", v.Key)
		}
		if !finiteFloat32(v.Data.Float32) {
			return nil, validationErr("The vector %q must not contain NaN or infinite values.", v.Key)
		}
		if len(v.Data.Float32) != idx.Dimension {
			return nil, validationErr("The vector %q has dimension %d, but the index dimension is %d.", v.Key, len(v.Data.Float32), idx.Dimension)
		}
	}
	for i := range in.Vectors {
		v := &in.Vectors[i]
		idx.Vectors[v.Key] = &StoredVector{
			Key:      v.Key,
			Data:     append([]float32(nil), v.Data.Float32...),
			Metadata: v.Metadata,
		}
	}
	return &PutVectorsOutput{}, nil
}

type GetVectorsInput struct {
	IndexName        string   `json:"indexName"`
	VectorBucketName string   `json:"vectorBucketName"`
	Keys             []string `json:"keys"`
	ReturnData       *bool    `json:"returnData"`
	ReturnMetadata   *bool    `json:"returnMetadata"`
}

type GetVectorsOutput struct {
	Vectors []VectorRecord `json:"vectors"`
}

// GetVectors returns vectors in the order the keys were requested; unknown
// keys are skipped. returnMetadata defaults to true, returnData to false.
func (s *Service) GetVectors(in *GetVectorsInput) (*GetVectorsOutput, *APIError) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	idx, aerr := s.lookupIndex(in.VectorBucketName, in.IndexName)
	if aerr != nil {
		return nil, aerr
	}
	returnData := in.ReturnData != nil && *in.ReturnData
	returnMetadata := in.ReturnMetadata == nil || *in.ReturnMetadata
	out := &GetVectorsOutput{Vectors: []VectorRecord{}}
	for _, key := range in.Keys {
		stored, ok := idx.Vectors[key]
		if !ok {
			continue
		}
		out.Vectors = append(out.Vectors, vectorRecord(stored, returnData, returnMetadata))
	}
	return out, nil
}

func vectorRecord(stored *StoredVector, returnData, returnMetadata bool) VectorRecord {
	rec := VectorRecord{Key: stored.Key}
	if returnData {
		rec.Data = &DataPayload{Float32: stored.Data}
	}
	if returnMetadata {
		rec.Metadata = stored.Metadata
	}
	return rec
}

type ListVectorsInput struct {
	IndexName        string `json:"indexName"`
	VectorBucketName string `json:"vectorBucketName"`
	MaxResults       *int   `json:"maxResults"`
	NextToken        string `json:"nextToken"`
	ReturnData       *bool  `json:"returnData"`
	ReturnMetadata   *bool  `json:"returnMetadata"`
	SegmentCount     *int   `json:"segmentCount"`
	SegmentIndex     *int   `json:"segmentIndex"`
}

type ListVectorsOutput struct {
	NextToken string         `json:"nextToken,omitempty"`
	Vectors   []VectorRecord `json:"vectors"`
}

// ListVectors lists the vectors of an index in key order with token and
// segment scan support. Without maxResults everything is returned.
func (s *Service) ListVectors(in *ListVectorsInput) (*ListVectorsOutput, *APIError) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	idx, aerr := s.lookupIndex(in.VectorBucketName, in.IndexName)
	if aerr != nil {
		return nil, aerr
	}
	if in.SegmentCount != nil {
		if *in.SegmentCount < 1 || *in.SegmentCount > maxSegments {
			return nil, validationErr("segmentCount must be between 1 and %d.", maxSegments)
		}
		if in.SegmentIndex == nil || *in.SegmentIndex < 0 || *in.SegmentIndex >= *in.SegmentCount {
			return nil, validationErr("segmentIndex must be between 0 and segmentCount - 1.")
		}
	}
	returnData := in.ReturnData != nil && *in.ReturnData
	returnMetadata := in.ReturnMetadata != nil && *in.ReturnMetadata
	keys := sortedVectorKeys(idx)
	var after string
	if in.NextToken != "" {
		after = decodeToken(in.NextToken)
	}
	out := &ListVectorsOutput{Vectors: []VectorRecord{}}
	for _, key := range keys {
		if in.SegmentCount != nil && fnv32(key)%uint32(*in.SegmentCount) != uint32(*in.SegmentIndex) {
			continue
		}
		if after != "" && key <= after {
			continue
		}
		if in.MaxResults != nil && *in.MaxResults > 0 && len(out.Vectors) >= *in.MaxResults {
			out.NextToken = encodeToken(key)
			break
		}
		out.Vectors = append(out.Vectors, vectorRecord(idx.Vectors[key], returnData, returnMetadata))
	}
	return out, nil
}

func sortedVectorKeys(idx *VectorIndex) []string {
	keys := make([]string, 0, len(idx.Vectors))
	for key := range idx.Vectors {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

type DeleteVectorsInput struct {
	IndexName        string   `json:"indexName"`
	VectorBucketName string   `json:"vectorBucketName"`
	Keys             []string `json:"keys"`
}

type DeleteVectorsOutput struct{}

// DeleteVectors removes the given keys; unknown keys are ignored.
func (s *Service) DeleteVectors(in *DeleteVectorsInput) (*DeleteVectorsOutput, *APIError) {
	if len(in.Keys) == 0 {
		return nil, validationErr("At least one vector key is required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	idx, aerr := s.lookupIndex(in.VectorBucketName, in.IndexName)
	if aerr != nil {
		return nil, aerr
	}
	for _, key := range in.Keys {
		delete(idx.Vectors, key)
	}
	return &DeleteVectorsOutput{}, nil
}

// ---------------------------------------------------------------------------
// SearchVectors (QueryVectors)
// ---------------------------------------------------------------------------

type SearchVectorsInput struct {
	IndexName        string          `json:"indexName"`
	VectorBucketName string          `json:"vectorBucketName"`
	TopK             *int            `json:"topK"`
	QueryVector      *DataPayload    `json:"queryVector"`
	Filter           json.RawMessage `json:"filter"`
	ReturnDistance   *bool           `json:"returnDistance"`
	ReturnMetadata   *bool           `json:"returnMetadata"`
}

type SearchVectorsOutput struct {
	Distances *[]float64     `json:"distances,omitempty"`
	Vectors   []VectorRecord `json:"vectors"`
}

// SearchVectors (the Floci name of QueryVectors) is a brute force nearest
// neighbour query: every vector of the index is scored with the index
// distance metric using float32 dot products, sorted ascending and truncated
// to topK. An optional openSearch-style metadata filter narrows candidates.
func (s *Service) SearchVectors(in *SearchVectorsInput) (*SearchVectorsOutput, *APIError) {
	if in.QueryVector == nil || len(in.QueryVector.Float32) == 0 {
		return nil, validationErr("A queryVector with float32 data is required.")
	}
	if !finiteFloat32(in.QueryVector.Float32) {
		return nil, validationErr("The query vector must not contain NaN or infinite values.")
	}
	topK := DefaultTopK
	if in.TopK != nil {
		if *in.TopK < 1 {
			return nil, validationErr("topK must be greater than 0.")
		}
		topK = *in.TopK
	}
	var filter any
	if len(bytes.TrimSpace(in.Filter)) > 0 {
		if err := json.Unmarshal(in.Filter, &filter); err != nil {
			return nil, validationErr("The metadata filter is not valid: %v", err)
		}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	idx, aerr := s.lookupIndex(in.VectorBucketName, in.IndexName)
	if aerr != nil {
		return nil, aerr
	}
	if len(in.QueryVector.Float32) != idx.Dimension {
		return nil, validationErr("The query vector dimension %d does not match the index dimension %d.", len(in.QueryVector.Float32), idx.Dimension)
	}
	query := in.QueryVector.Float32
	type scored struct {
		dist float64
		vec  *StoredVector
	}
	results := make([]scored, 0, len(idx.Vectors))
	for _, stored := range idx.Vectors {
		if filter != nil && !matchFilter(filter, stored.Metadata) {
			continue
		}
		results = append(results, scored{dist: distance(idx.DistanceMetric, query, stored.Data), vec: stored})
	}
	sort.SliceStable(results, func(i, j int) bool {
		if results[i].dist != results[j].dist {
			return results[i].dist < results[j].dist
		}
		return results[i].vec.Key < results[j].vec.Key
	})
	if len(results) > topK {
		results = results[:topK]
	}
	returnDistance := in.ReturnDistance == nil || *in.ReturnDistance
	returnMetadata := in.ReturnMetadata == nil || *in.ReturnMetadata
	out := &SearchVectorsOutput{Vectors: make([]VectorRecord, 0, len(results))}
	if returnDistance {
		distances := make([]float64, 0, len(results))
		out.Distances = &distances
	}
	for _, r := range results {
		if returnDistance {
			*out.Distances = append(*out.Distances, r.dist)
		}
		out.Vectors = append(out.Vectors, vectorRecord(r.vec, false, returnMetadata))
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Metadata filter (small opensearch-style grammar)
// ---------------------------------------------------------------------------

// matchFilter evaluates a filter node against vector metadata. Supported
// clauses: match_all, match, term, terms, range, exists, prefix, bool and the
// bare {"field": value} shorthand. An empty or non-object node matches all.
func matchFilter(node any, meta map[string]any) bool {
	clauses, ok := node.(map[string]any)
	if !ok || len(clauses) == 0 {
		return true
	}
	for key, val := range clauses {
		switch key {
		case "match_all":
		case "term", "match":
			terms, _ := val.(map[string]any)
			for field, want := range terms {
				got, present := meta[field]
				if !present || !valueEquals(got, want) {
					return false
				}
			}
		case "terms":
			terms, _ := val.(map[string]any)
			for field, want := range terms {
				got, present := meta[field]
				if !present || !valueIn(got, want) {
					return false
				}
			}
		case "exists":
			spec, _ := val.(map[string]any)
			field, _ := spec["field"].(string)
			if _, present := meta[field]; !present {
				return false
			}
		case "prefix":
			prefixes, _ := val.(map[string]any)
			for field, want := range prefixes {
				prefix, _ := want.(string)
				got, _ := meta[field].(string)
				if !strings.HasPrefix(got, prefix) {
					return false
				}
			}
		case "range":
			ranges, _ := val.(map[string]any)
			for field, bounds := range ranges {
				got, present := meta[field]
				if !present {
					return false
				}
				ops, _ := bounds.(map[string]any)
				for op, bound := range ops {
					if !compareRange(got, op, bound) {
						return false
					}
				}
			}
		case "bool":
			spec, _ := val.(map[string]any)
			if !matchBool(spec, meta) {
				return false
			}
		default:
			got, present := meta[key]
			if !present || !valueEquals(got, val) {
				return false
			}
		}
	}
	return true
}

func matchBool(spec map[string]any, meta map[string]any) bool {
	if len(spec) == 0 {
		return true
	}
	if must, ok := spec["must"]; ok {
		for _, clause := range toClauseList(must) {
			if !matchFilter(clause, meta) {
				return false
			}
		}
	}
	if flt, ok := spec["filter"]; ok {
		for _, clause := range toClauseList(flt) {
			if !matchFilter(clause, meta) {
				return false
			}
		}
	}
	if should, ok := spec["should"]; ok {
		clauses := toClauseList(should)
		matched := false
		for _, clause := range clauses {
			if matchFilter(clause, meta) {
				matched = true
				break
			}
		}
		if len(clauses) > 0 && !matched {
			return false
		}
	}
	if mustNot, ok := spec["must_not"]; ok {
		for _, clause := range toClauseList(mustNot) {
			if matchFilter(clause, meta) {
				return false
			}
		}
	}
	return true
}

func toClauseList(v any) []any {
	if list, ok := v.([]any); ok {
		return list
	}
	if v == nil {
		return nil
	}
	return []any{v}
}

func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}

func valueEquals(a, b any) bool {
	if af, ok := asFloat(a); ok {
		bf, ok2 := asFloat(b)
		return ok2 && af == bf
	}
	switch av := a.(type) {
	case string:
		bs, ok := b.(string)
		return ok && av == bs
	case bool:
		bb, ok := b.(bool)
		return ok && av == bb
	}
	return false
}

func valueIn(got, want any) bool {
	list, ok := want.([]any)
	if !ok {
		return valueEquals(got, want)
	}
	for _, item := range list {
		if valueEquals(got, item) {
			return true
		}
	}
	return false
}

func compareRange(got any, op string, bound any) bool {
	gf, gok := asFloat(got)
	bf, bok := asFloat(bound)
	if gok && bok {
		switch op {
		case "gt":
			return gf > bf
		case "gte":
			return gf >= bf
		case "lt":
			return gf < bf
		case "lte":
			return gf <= bf
		}
		return false
	}
	gs, g1 := got.(string)
	bs, b1 := bound.(string)
	if !g1 || !b1 {
		return false
	}
	switch op {
	case "gt":
		return gs > bs
	case "gte":
		return gs >= bs
	case "lt":
		return gs < bs
	case "lte":
		return gs <= bs
	}
	return false
}

// ---------------------------------------------------------------------------
// awsJson dispatch (DDB handler integration)
// ---------------------------------------------------------------------------

func knownOperation(op string) bool {
	switch op {
	case "createvectorbucket", "deletevectorbucket", "getvectorbucket", "listvectorbuckets",
		"createvectorindex", "deletevectorindex", "getvectorindex", "listvectorindexes",
		"putvectors", "getvectors", "listvectors", "deletevectors",
		"searchvectors", "queryvectors":
		return true
	}
	return false
}

// normalizeTarget maps an X-Amz-Target value to the served operation name by
// looking at everything after the last dot, case-insensitively.
func normalizeTarget(target string) (string, bool) {
	op := strings.TrimSpace(target)
	if i := strings.LastIndex(op, "."); i >= 0 {
		op = op[i+1:]
	}
	op = strings.ToLower(op)
	if !knownOperation(op) {
		return "", false
	}
	return op, true
}

// IsS3VectorsTarget reports whether an X-Amz-Target value belongs to the
// S3 Vectors service. Use it in the DDB handler to route into this service:
//
//    if s3vectors.IsS3VectorsTarget(target) {
//        s3vectors.Default().ServeJSON(w, target, body)
//        return
//    }
func IsS3VectorsTarget(target string) bool {
	_, ok := normalizeTarget(target)
	return ok
}

// EmptyOutput renders as an empty JSON object for bodyless responses.
type EmptyOutput struct{}

// ServeJSON handles one decoded awsJson operation. target is the X-Amz-Target
// value and body the raw JSON payload of the request.
func (s *Service) ServeJSON(w http.ResponseWriter, target string, body []byte) {
	op, ok := normalizeTarget(target)
	if !ok {
		writeAPIError(w, &APIError{
			Status:  http.StatusBadRequest,
			Code:    "UnknownOperationException",
			Message: "Unknown S3 Vectors operation: " + target,
		})
		return
	}
	var out any
	var aerr *APIError
	switch op {
	case "createvectorbucket":
		in := &CreateVectorBucketInput{}
		if aerr = decodeBody(body, in); aerr == nil {
			out, aerr = s.CreateVectorBucket(in)
		}
	case "deletevectorbucket":
		in := &DeleteVectorBucketInput{}
		if aerr = decodeBody(body, in); aerr == nil {
			out, aerr = s.DeleteVectorBucket(in)
		}
	case "getvectorbucket":
		in := &GetVectorBucketInput{}
		if aerr = decodeBody(body, in); aerr == nil {
			out, aerr = s.GetVectorBucket(in)
		}
	case "listvectorbuckets":
		in := &ListVectorBucketsInput{}
		if aerr = decodeBody(body, in); aerr == nil {
			out, aerr = s.ListVectorBuckets(in)
		}
	case "createvectorindex":
		in := &CreateVectorIndexInput{}
		if aerr = decodeBody(body, in); aerr == nil {
			out, aerr = s.CreateVectorIndex(in)
		}
	case "deletevectorindex":
		in := &DeleteVectorIndexInput{}
		if aerr = decodeBody(body, in); aerr == nil {
			out, aerr = s.DeleteVectorIndex(in)
		}
	case "getvectorindex":
		in := &GetVectorIndexInput{}
		if aerr = decodeBody(body, in); aerr == nil {
			out, aerr = s.GetVectorIndex(in)
		}
	case "listvectorindexes":
		in := &ListVectorIndexesInput{}
		if aerr = decodeBody(body, in); aerr == nil {
			out, aerr = s.ListVectorIndexes(in)
		}
	case "putvectors":
		in := &PutVectorsInput{}
		if aerr = decodeBody(body, in); aerr == nil {
			out, aerr = s.PutVectors(in)
		}
	case "getvectors":
		in := &GetVectorsInput{}
		if aerr = decodeBody(body, in); aerr == nil {
			out, aerr = s.GetVectors(in)
		}
	case "listvectors":
		in := &ListVectorsInput{}
		if aerr = decodeBody(body, in); aerr == nil {
			out, aerr = s.ListVectors(in)
		}
	case "deletevectors":
		in := &DeleteVectorsInput{}
		if aerr = decodeBody(body, in); aerr == nil {
			out, aerr = s.DeleteVectors(in)
		}
	case "searchvectors", "queryvectors":
		in := &SearchVectorsInput{}
		if aerr = decodeBody(body, in); aerr == nil {
			out, aerr = s.SearchVectors(in)
		}
	default:
		aerr = &APIError{
			Status:  http.StatusBadRequest,
			Code:    "UnknownOperationException",
			Message: "Unknown S3 Vectors operation: " + target,
		}
	}
	if aerr != nil {
		writeAPIError(w, aerr)
		return
	}
	if out == nil {
		out = &EmptyOutput{}
	}
	writeJSON(w, http.StatusOK, out)
}

// ServeHTTP makes the service a plain http.Handler: POST with an X-Amz-Target
// header, or POST /<Operation> (the last path segment decides).
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeAPIError(w, validationErr("Failed to read the request body: %v", err))
		return
	}
	target := r.Header.Get("X-Amz-Target")
	if target == "" {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		target = parts[len(parts)-1]
	}
	s.ServeJSON(w, target, body)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	payload := []byte("{}")
	if v != nil {
		if b, err := json.Marshal(v); err == nil {
			payload = b
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-amzn-RequestId", newRequestID())
	w.WriteHeader(status)
	w.Write(payload)
}

func writeAPIError(w http.ResponseWriter, aerr *APIError) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-amzn-ErrorType", aerr.Code)
	w.Header().Set("x-amzn-RequestId", newRequestID())
	w.WriteHeader(aerr.Status)
	payload, _ := json.Marshal(map[string]string{"__type": aerr.Code, "message": aerr.Message})
	w.Write(payload)
}

func newRequestID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "00000000-0000-4000-8000-000000000000"
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	h := hex.EncodeToString(raw[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}