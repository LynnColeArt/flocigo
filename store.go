package main

import (
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
    "sync"
)

// Table is the durable record of a single DynamoDB table. It carries the
// pk/sk key schema plus everything needed to render TableDescription
// responses, and is persisted to the store file.
type Table struct {
    TableName            string                `json:"tableName"`
    TableID              string                `json:"tableId"`
    TableArn             string                `json:"tableArn"`
    TableStatus          string                `json:"tableStatus"`
    CreationDateTime     float64               `json:"creationDateTime"`
    AttributeDefinitions []AttributeDefinition `json:"attributeDefinitions"`
    KeySchema            []KeySchemaElement    `json:"keySchema"`
    PartitionKey         string                `json:"partitionKey"`
    PartitionKeyType     string                `json:"partitionKeyType"`
    SortKey              string                `json:"sortKey,omitempty"`
    SortKeyType          string                `json:"sortKeyType,omitempty"`
    BillingMode          string                `json:"billingMode"`
    ReadCapacityUnits    int64                 `json:"readCapacityUnits"`
    WriteCapacityUnits   int64                 `json:"writeCapacityUnits"`
    ItemCount            int64                 `json:"itemCount"`
    TableSizeBytes       int64                 `json:"tableSizeBytes"`
}

// Store holds table metadata, persisted as JSON in a single store file. The
// file is loaded at startup and rewritten atomically on every mutation, so
// tables survive restarts and re-creating an existing table keeps failing
// with ResourceInUseException across process restarts.
type Store struct {
    mu     sync.Mutex
    path   string
    Tables map[string]*Table `json:"tables"`
}

// OpenStore loads (or initializes) the store file at path.
func OpenStore(path string) (*Store, error) {
    if path == "" {
        path = defaultStorePath
    }
    s := &Store{path: path}
    if data, err := os.ReadFile(path); err == nil {
        if len(data) > 0 {
            if err := json.Unmarshal(data, s); err != nil {
                return nil, fmt.Errorf("store file %s is not valid JSON: %w", path, err)
            }
        }
    } else if !os.IsNotExist(err) {
        return nil, fmt.Errorf("cannot read store file %s: %w", path, err)
    }
    if s.Tables == nil {
        s.Tables = make(map[string]*Table)
    }
    if err := s.save(); err != nil {
        return nil, fmt.Errorf("cannot write store file %s: %w", path, err)
    }
    return s, nil
}

// save atomically rewrites the store file (write to temp, then rename).
// Callers must hold s.mu.
func (s *Store) save() error {
    if dir := filepath.Dir(s.path); dir != "" && dir != "." {
        if err := os.MkdirAll(dir, 0o755); err != nil {
            return err
        }
    }
    data, err := json.MarshalIndent(s, "", "  ")
    if err != nil {
        return err
    }
    data = append(data, '\n')
    tmp := s.path + ".tmp"
    if err := os.WriteFile(tmp, data, 0o644); err != nil {
        return err
    }
    return os.Rename(tmp, s.path)
}

// CreateTable inserts a new table record. If a table with the same
// (case-sensitive) name already exists, exists=true is returned and the store
// is left untouched so the caller responds with ResourceInUseException.
func (s *Store) CreateTable(t *Table) (bool, error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    if _, ok := s.Tables[t.TableName]; ok {
        return true, nil
    }
    s.Tables[t.TableName] = t
    if err := s.save(); err != nil {
        delete(s.Tables, t.TableName)
        return false, fmt.Errorf("persist table %q: %w", t.TableName, err)
    }
    return false, nil
}

// GetTable looks a table up by exact (case-sensitive) name.
func (s *Store) GetTable(name string) (*Table, bool) {
    s.mu.Lock()
    defer s.mu.Unlock()
    t, ok := s.Tables[name]
    return t, ok
}
