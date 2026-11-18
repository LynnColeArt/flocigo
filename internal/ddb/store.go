// Package ddb implements a DynamoDB emulator surface. Table definitions (with
// their pk/sk primary-key schema) are persisted in a JSON store file under the
// floci data directory, and the operations of this package are served over the
// Amazon JSON 1.0 protocol (X-Amz-Target: DynamoDB_20120810.*).
package ddb

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Table status. Freshly created tables are reported as ACTIVE.
const StatusActive = "ACTIVE"

// Key schema roles.
const (
	KeyTypeHash  = "HASH"
	KeyTypeRange = "RANGE"
)

// Billing modes.
const (
	BillingModeProvisioned   = "PROVISIONED"
	BillingModePayPerRequest = "PAY_PER_REQUEST"
)

// ErrResourceInUse is returned when a table name is already present in the
// store; the wire layer maps it to ResourceInUseException.
var ErrResourceInUse = errors.New("ddb: table already exists")

// KeySchemaElement is one HASH/RANGE component of a table or index key.
type KeySchemaElement struct {
	AttributeName string `json:"AttributeName"`
	KeyType       string `json:"KeyType"`
}

// AttributeDefinition declares the scalar type of a key attribute (S, N or B).
type AttributeDefinition struct {
	AttributeName string `json:"AttributeName"`
	AttributeType string `json:"AttributeType"`
}

// IndexRecord is the persisted form of a GSI/LSI definition.
type IndexRecord struct {
	Name               string             `json:"name"`
	ARN                string             `json:"arn"`
	KeySchema          []KeySchemaElement `json:"keySchema"`
	ProjectionType     string             `json:"projectionType,omitempty"`
	NonKeyAttributes   []string           `json:"nonKeyAttributes,omitempty"`
	ReadCapacityUnits  int64              `json:"readCapacityUnits,omitempty"`
	WriteCapacityUnits int64              `json:"writeCapacityUnits,omitempty"`
}

// StoredTable is the persisted record of one table inside the store file. PK
// and SK hold the pk (HASH) and sk (RANGE) attribute names extracted from the
// key schema so later operations can address items without re-parsing KeySchema.
type StoredTable struct {
	Name                      string                     `json:"name"`
	Status                    string                     `json:"status"`
	CreationDateTime          float64                    `json:"creationDateTime"`
	TableID                   string                     `json:"tableId"`
	ARN                       string                     `json:"arn"`
	Region                    string                     `json:"region"`
	AccountID                 string                     `json:"accountId"`
	PK                        string                     `json:"pk"`
	SK                        string                     `json:"sk,omitempty"`
	KeySchema                 []KeySchemaElement         `json:"keySchema"`
	AttributeDefinitions      []AttributeDefinition      `json:"attributeDefinitions"`
	BillingMode               string                     `json:"billingMode"`
	ReadCapacityUnits         int64                      `json:"readCapacityUnits,omitempty"`
	WriteCapacityUnits        int64                      `json:"writeCapacityUnits,omitempty"`
	GlobalSecondaryIndexes    []IndexRecord              `json:"gsis,omitempty"`
	LocalSecondaryIndexes     []IndexRecord              `json:"lsis,omitempty"`
	TableClass                string                     `json:"tableClass,omitempty"`
	DeletionProtectionEnabled bool                       `json:"deletionProtectionEnabled"`
	Tags                      map[string]string          `json:"tags,omitempty"`
	Items                     map[string]json.RawMessage `json:"items"`
}

// HashKey returns the pk (HASH) attribute name of the table.
func (t *StoredTable) HashKey() string { return t.PK }

// RangeKey returns the sk (RANGE) attribute name and whether the table has one.
func (t *StoredTable) RangeKey() (string, bool) {
	if t.SK == "" {
		return "", false
	}
	return t.SK, true
}

// ItemKey composes the store-file key for an item from its pk/sk values.
func (t *StoredTable) ItemKey(pk, sk string) string {
	if _, has := t.RangeKey(); !has {
		return pk
	}
	return pk + "#" + sk
}

type tableStoreFile struct {
	Version int                     `json:"version"`
	Tables  map[string]*StoredTable `json:"tables"`
}

// TableStore owns the tables.json store file under <dataDir>/ddb/. Lookups
// and mutations are serialized through a mutex and every mutation flushes the
// file so table definitions survive emulator restarts.
type TableStore struct {
	mu     sync.Mutex
	path   string
	tables map[string]*StoredTable
}

// DefaultDataDir resolves the emulator data directory: $FLOCI_DATA_DIR or ".floci".
func DefaultDataDir() string {
	if dir := os.Getenv("FLOCI_DATA_DIR"); dir != "" {
		return dir
	}
	return ".floci"
}

// OpenTableStore loads (or creates) the store file under dataDir/ddb/. An
// empty dataDir falls back to DefaultDataDir.
func OpenTableStore(dataDir string) (*TableStore, error) {
	if dataDir == "" {
		dataDir = DefaultDataDir()
	}
	path := filepath.Join(dataDir, "ddb", "tables.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("ddb: create store directory: %w", err)
	}
	s := &TableStore{path: path, tables: map[string]*StoredTable{}}
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return s, nil
		}
		return nil, fmt.Errorf("ddb: read store file: %w", err)
	}
	if len(bytes.TrimSpace(raw)) > 0 {
		var file tableStoreFile
		if err := json.Unmarshal(raw, &file); err != nil {
			return nil, fmt.Errorf("ddb: parse %s: %w", path, err)
		}
		for _, t := range file.Tables {
			if t == nil || t.Name == "" {
				continue
			}
			if t.Items == nil {
				t.Items = map[string]json.RawMessage{}
			}
			if t.Status == "" {
				t.Status = StatusActive
			}
			s.tables[t.Name] = t
		}
	}
	return s, nil
}

// Path returns the location of the underlying store file.
func (s *TableStore) Path() string { return s.path }

// CreateTable records a new table and persists it to the store file. When the
// name is already present it returns ErrResourceInUse and leaves the store
// untouched.
func (s *TableStore) CreateTable(t *StoredTable) (*StoredTable, error) {
	if t == nil || t.Name == "" {
		return nil, errors.New("ddb: table name is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tables == nil {
		s.tables = map[string]*StoredTable{}
	}
	if _, exists := s.tables[t.Name]; exists {
		return nil, ErrResourceInUse
	}
	stored := *t
	stored.Status = StatusActive
	if stored.Items == nil {
		stored.Items = map[string]json.RawMessage{}
	}
	s.tables[t.Name] = &stored
	if err := s.flushLocked(); err != nil {
		delete(s.tables, t.Name)
		return nil, err
	}
	out := stored
	return &out, nil
}

// GetTable returns a snapshot of the stored table by exact name.
func (s *TableStore) GetTable(name string) (StoredTable, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tables[name]
	if !ok {
		return StoredTable{}, false
	}
	return *t, true
}

// ListTables returns every stored table ordered by name.
func (s *TableStore) ListTables() []StoredTable {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.tables))
	for name := range s.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]StoredTable, 0, len(names))
	for _, name := range names {
		out = append(out, *s.tables[name])
	}
	return out
}

// flushLocked atomically rewrites the store file; the caller holds s.mu.
func (s *TableStore) flushLocked() error {
	file := tableStoreFile{Version: 1, Tables: s.tables}
	raw, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return fmt.Errorf("ddb: write store file: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("ddb: replace store file: %w", err)
	}
	return nil
}

// ddbNow returns the current UTC time as an epoch-seconds float (ms precision).
func ddbNow() float64 {
	return float64(time.Now().UTC().UnixMilli()) / 1000
}

// ddbRandHex returns n random bytes hex encoded.
func ddbRandHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}

// ddbTableID generates a UUID-shaped table id.
func ddbTableID() string {
	return fmt.Sprintf("%s-%s-%s-%s-%s", ddbRandHex(4), ddbRandHex(2), ddbRandHex(2), ddbRandHex(2), ddbRandHex(6))
}
