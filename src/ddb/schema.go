package ddb

import "fmt"

const (
    maxHashKeyBytes      = 2048
    maxRangeKeyBytes     = 1024
    maxItemBytes         = 400 * 1024
    batchGetMaxItems     = 100
    batchGetMaxItemsPerTable = 100
    batchWriteMaxRequests    = 25
)

// batchGetMaxTotalBytes mirrors the 16 MB limit a single BatchGetItem reply
// may carry. It is a variable so tests can shrink it.
var batchGetMaxTotalBytes = 16 * 1024 * 1024

// TableSchema describes the key schema of a table.
type TableSchema struct {
    Name     string
    HashKey  string
    RangeKey string            // "" when the table has no sort key
    KeyTypes map[string]string // key attribute -> "S" | "N" | "B"
}

// Store is the storage surface required by the batch handlers. It maps onto
// the existing floci-go storage engine; adapters only need these four calls.
type Store interface {
    Schema(table string) (TableSchema, error)
    GetItem(table string, key map[string]AttrVal, consistent bool) (map[string]AttrVal, bool, error)
    PutItem(table string, item map[string]AttrVal) error
    DeleteItem(table string, key map[string]AttrVal) error
}

// Handler adapts an operation for the HTTP router: Invoke returns the
// response object to marshal, or an APIError which the router should render
// with APIError.WriteTo (HTTP status plus X-Amzn-Errortype header).
type Handler interface {
    Target() string
    Invoke(body []byte) (interface{}, *APIError)
}

// NewBatchHandlers wires the two batch operations against a store. The map
// keys are the full X-Amz-Target names served over the DynamoDB JSON API.
func NewBatchHandlers(store Store) map[string]Handler {
    return map[string]Handler{
        "DynamoDB_20120810.BatchGetItem":  NewBatchGetItemHandler(store),
        "DynamoDB_20120810.BatchWriteItem": NewBatchWriteItemHandler(store),
    }
}

func keyTypeMismatch(name, want, actual string) *APIError {
    return validation(fmt.Sprintf("One or more parameter values were invalid: Type mismatch for key %s expected: %s actual: %s", name, want, actual))
}

func keyTooLarge(max int) *APIError {
    return validation(fmt.Sprintf("One or more parameter values are not valid: The size of a key attribute must be less than or equal to %d bytes", max))
}

// validateKeyShape validates m against the table key schema and returns a
// normalized copy containing exactly the key attributes.
func validateKeyShape(schema TableSchema, m map[string]AttrVal) (map[string]AttrVal, *APIError) {
    if _, ok := m[schema.HashKey]; !ok {
        return nil, validation("The provided key element does not match the schema")
    }
    if schema.RangeKey != "" {
        if _, ok := m[schema.RangeKey]; !ok {
            return nil, validation("The provided key element does not match the schema")
        }
    }
    extras := len(m) - 1
    if schema.RangeKey != "" {
        extras--
    }
    if extras > 0 {
        return nil, validation("The provided key element does not match the schema")
    }
    if v := m[schema.HashKey]; v.Type != schema.KeyTypes[schema.HashKey] {
        return nil, keyTypeMismatch(schema.HashKey, schema.KeyTypes[schema.HashKey], v.Type)
    }
    if attrValSize(m[schema.HashKey]) > maxHashKeyBytes {
        return nil, keyTooLarge(maxHashKeyBytes)
    }
    if schema.RangeKey != "" {
        if v := m[schema.RangeKey]; v.Type != schema.KeyTypes[schema.RangeKey] {
            return nil, keyTypeMismatch(schema.RangeKey, schema.KeyTypes[schema.RangeKey], v.Type)
        }
        if attrValSize(m[schema.RangeKey]) > maxRangeKeyBytes {
            return nil, keyTooLarge(maxRangeKeyBytes)
        }
    }
    out := map[string]AttrVal{schema.HashKey: copyAttr(m[schema.HashKey])}
    if schema.RangeKey != "" {
        out[schema.RangeKey] = copyAttr(m[schema.RangeKey])
    }
    return out, nil
}

// validatePutItem validates an item for a BatchWriteItem PutRequest.
func validatePutItem(schema TableSchema, item map[string]AttrVal) (map[string]AttrVal, *APIError) {
    if len(item) == 0 {
        return nil, validation("One or more parameter values are not valid: The Item in PutRequest cannot be empty")
    }
    hv, ok := item[schema.HashKey]
    if !ok {
        return nil, validation(fmt.Sprintf("One or more parameter values were invalid: Missing the key %s in the item", schema.HashKey))
    }
    if hv.Type != schema.KeyTypes[schema.HashKey] {
        return nil, keyTypeMismatch(schema.HashKey, schema.KeyTypes[schema.HashKey], hv.Type)
    }
    if attrValSize(hv) > maxHashKeyBytes {
        return nil, keyTooLarge(maxHashKeyBytes)
    }
    if schema.RangeKey != "" {
        rk, ok := item[schema.RangeKey]
        if !ok {
            return nil, validation(fmt.Sprintf("One or more parameter values were invalid: Missing the key %s in the item", schema.RangeKey))
        }
        if rk.Type != schema.KeyTypes[schema.RangeKey] {
            return nil, keyTypeMismatch(schema.RangeKey, schema.KeyTypes[schema.RangeKey], rk.Type)
        }
        if attrValSize(rk) > maxRangeKeyBytes {
            return nil, keyTooLarge(maxRangeKeyBytes)
        }
    }
    if itemSize(item) > maxItemBytes {
        return nil, validation("Item size has exceeded the maximum allowed size")
    }
    return copyItem(item), nil
}

// primaryKeySubset extracts only the key attributes of an item.
func primaryKeySubset(schema TableSchema, item map[string]AttrVal) map[string]AttrVal {
    out := map[string]AttrVal{schema.HashKey: item[schema.HashKey]}
    if schema.RangeKey != "" {
        out[schema.RangeKey] = item[schema.RangeKey]
    }
    return out
}
