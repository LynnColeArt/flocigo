package ddb

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// PutItemInput holds the parameters of a DynamoDB PutItem request
// (ticket floci-go/ticket-013).
//
// Attribute values are kept exactly as they arrive on the wire (AWS JSON
// protocol attribute maps such as {"S":"value"}) so the emulator can
// validate, store and echo them without lossy conversions. Struct tags use
// the member names of the DynamoDB JSON protocol (PascalCase on the wire).
type PutItemInput struct {
    Item                        map[string]any `json:"Item"`
    TableName                   string         `json:"TableName"`
    ReturnConsumedCapacity      string         `json:"ReturnConsumedCapacity,omitempty"`
    ReturnItemCollectionMetrics string         `json:"ReturnItemCollectionMetrics,omitempty"`
    ReturnValues                string         `json:"ReturnValues,omitempty"`
    ConditionExpression         string         `json:"ConditionExpression,omitempty"`
    Expected                    map[string]any `json:"Expected,omitempty"`
    ExpressionAttributeNames    map[string]any `json:"ExpressionAttributeNames,omitempty"`
    ExpressionAttributeValues   map[string]any `json:"ExpressionAttributeValues,omitempty"`
}

// PutItemOutput holds the response of a DynamoDB PutItem request.
type PutItemOutput struct {
    Attributes       map[string]any    `json:"Attributes,omitempty"`
    ConsumedCapacity *ConsumedCapacity `json:"ConsumedCapacity,omitempty"`
}

// Write capacity accounting: one unit covers up to 1 KiB of item size and a
// single item may never exceed 400 KiB.
const (
    putItemKB      = 1024
    putItemMaxKB   = 400
    putItemMaxSize = putItemMaxKB * putItemKB
)

// PutItem stores the item in the requested table, replacing any item that
// already shares its primary key, and reports the consumed write capacity
// when ReturnConsumedCapacity is TOTAL or INDEXES.
//
// Validation mirrors the real service: protocol level null checks, attribute
// value shape checks, table resolution, primary key checks and the 400 KiB
// item limit. Failures use the canonical DynamoDB messages so the shared
// error catalog maps them to ValidationException / ResourceNotFoundException.
// Conditions (ConditionExpression, Expected) are accepted but not evaluated
// by this emulator.
func (s *Service) PutItem(input *PutItemInput) (*PutItemOutput, error) {
    if input == nil {
        input = &PutItemInput{}
    }
    if input.Item == nil {
        return nil, fmt.Errorf("1 validation error detected: Value null at 'item' failed to satisfy constraint: Member must not be null")
    }
    if input.TableName == "" {
        return nil, fmt.Errorf("1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null")
    }
    switch input.ReturnConsumedCapacity {
    case "", "NONE", "TOTAL", "INDEXES":
    default:
        return nil, fmt.Errorf("1 validation error detected: Value '%s' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]", input.ReturnConsumedCapacity)
    }
    switch input.ReturnValues {
    case "", "NONE", "ALL_OLD":
    default:
        return nil, fmt.Errorf("One or more parameter values were invalid: Invalid ReturnValues: Must be `NONE` or `ALL_OLD`")
    }
    if err := putItemValidateItem(input.Item); err != nil {
        return nil, err
    }

    table := s.Store.Tables[input.TableName]
    if table == nil {
        return nil, fmt.Errorf("Requested resource not found: Table: %s not found", input.TableName)
    }

    if err := putItemValidateKey(table, input.Item); err != nil {
        return nil, err
    }
    if size := putItemSize(input.Item); size > putItemMaxSize {
        return nil, fmt.Errorf("Item size has exceeded the maximum allowed size")
    }

    key := putItemKeyString(table, input.Item)
    old, existed := table.Items[key]
    units := putItemWriteUnits(putItemSize(input.Item))
    if existed && old != nil {
        // A replace behaves as delete + put: the service charges based on
        // the larger of the previous and the new item size.
        if oldUnits := putItemWriteUnits(putItemSize(old)); oldUnits > units {
            units = oldUnits
        }
    }
    if table.Items == nil {
        table.Items = make(map[string]map[string]any)
    }
    table.Items[key] = input.Item

    output := &PutItemOutput{}
    if input.ReturnValues == "ALL_OLD" && existed && old != nil {
        output.Attributes = old
    }
    if rc := input.ReturnConsumedCapacity; rc == "TOTAL" || rc == "INDEXES" {
        output.ConsumedCapacity = &ConsumedCapacity{
            TableName:     input.TableName,
            CapacityUnits: float64(units),
        }
    }
    return output, nil
}

// putItemValidateItem validates every attribute value the way the real
// service does while deserializing the request body: each value must be an
// attribute map with exactly one recognized datatype tag, numbers must be
// parseable, and nested documents recurse.
func putItemValidateItem(item map[string]any) error {
    for _, raw := range item {
        av, ok := raw.(map[string]any)
        if !ok {
            return fmt.Errorf("Supplied AttributeValue is empty, must contain exactly one of the supported datatypes")
        }
        if err := putItemValidateValue(av); err != nil {
            return err
        }
    }
    return nil
}

func putItemValidateValue(av map[string]any) error {
    if len(av) == 0 {
        return fmt.Errorf("Supplied AttributeValue is empty, must contain exactly one of the supported datatypes")
    }
    if len(av) > 1 {
        return fmt.Errorf("Supplied AttributeValue has more than one datatype populated, must be exactly one")
    }
    for tag, value := range av {
        switch tag {
        case "S", "B", "SS", "NS", "BS", "BOOL", "NULL":
            // Kept exactly as received on the wire.
        case "N":
            str, _ := value.(string)
            if !putItemValidNumber(str) {
                return fmt.Errorf("The parameter cannot be converted to a numeric value")
            }
        case "M":
            doc, _ := value.(map[string]any)
            for _, v := range doc {
                nested, ok := v.(map[string]any)
                if !ok {
                    return fmt.Errorf("Supplied AttributeValue is empty, must contain exactly one of the supported datatypes")
                }
                if err := putItemValidateValue(nested); err != nil {
                    return err
                }
            }
        case "L":
            list, _ := value.([]any)
            for _, v := range list {
                nested, ok := v.(map[string]any)
                if !ok {
                    return fmt.Errorf("Supplied AttributeValue is empty, must contain exactly one of the supported datatypes")
                }
                if err := putItemValidateValue(nested); err != nil {
                    return err
                }
            }
        default:
            return fmt.Errorf("Supplied AttributeValue is empty, must contain exactly one of the supported datatypes")
        }
    }
    return nil
}

// putItemValidateKey checks the primary key attributes of the item against
// the table schema: presence, scalar type match with the attribute
// definitions and non-empty key values.
func putItemValidateKey(table *Table, item map[string]any) error {
    declared := make(map[string]string, len(table.AttributeDefinitions))
    for _, ad := range table.AttributeDefinitions {
        declared[ad.AttributeName] = ad.AttributeType
    }
    for _, ks := range table.KeySchema {
        name := ks.AttributeName
        want := declared[name]
        raw, present := item[name]
        if !present || raw == nil {
            return fmt.Errorf("One or more parameter values were invalid: Missing the key %s in the item", name)
        }
        av, isMap := raw.(map[string]any)
        if !isMap || len(av) != 1 {
            return fmt.Errorf("One or more parameter values were invalid: Type mismatch for key %s expected: %s actual: null indexName=null", name, want)
        }
        for got, value := range av {
            if want != "" && got != want {
                return fmt.Errorf("One or more parameter values were invalid: Type mismatch for key %s expected: %s actual: %s indexName=null", name, want, got)
            }
            switch got {
            case "S", "N", "B":
                str, isStr := value.(string)
                if !isStr || str == "" {
                    return fmt.Errorf("One or more parameter values are not valid: The AttributeValue for a key attribute cannot contain an empty string value. Key: %s", name)
                }
            default:
                return fmt.Errorf("One or more parameter values were invalid: Type mismatch for key %s expected: %s actual: %s indexName=null", name, want, got)
            }
        }
    }
    return nil
}

// putItemKeyString renders the canonical storage key for an item: the JSON
// encoding of the schema key attributes (sorted, deterministic), matching the
// canonical form the rest of the package uses for primary keys.
func putItemKeyString(table *Table, item map[string]any) string {
    if len(table.KeySchema) == 0 {
        encoded, _ := json.Marshal(item)
        return string(encoded)
    }
    key := make(map[string]any, len(table.KeySchema))
    for _, ks := range table.KeySchema {
        key[ks.AttributeName] = item[ks.AttributeName]
    }
    encoded, _ := json.Marshal(key)
    return string(encoded)
}

// putItemSize approximates the documented item size formula: attribute name
// plus attribute value size in bytes, recursing through maps, lists and sets.
func putItemSize(item map[string]any) int {
    size := 0
    for name, raw := range item {
        size += len(name) + putItemValueSize(raw)
    }
    return size
}

func putItemValueSize(raw any) int {
    av, ok := raw.(map[string]any)
    if !ok {
        return 0
    }
    for tag, value := range av {
        switch tag {
        case "S", "N":
            str, _ := value.(string)
            return len(str)
        case "B":
            str, _ := value.(string)
            if blob, err := base64.StdEncoding.DecodeString(str); err == nil {
                return len(blob)
            }
            return len(str)
        case "BOOL", "NULL":
            return 1
        case "SS", "NS":
            list, _ := value.([]any)
            n := 0
            for _, e := range list {
                if str, ok := e.(string); ok {
                    n += len(str)
                }
            }
            return n
        case "BS":
            list, _ := value.([]any)
            n := 0
            for _, e := range list {
                str, _ := e.(string)
                if blob, err := base64.StdEncoding.DecodeString(str); err == nil {
                    n += len(blob)
                } else {
                    n += len(str)
                }
            }
            return n
        case "M":
            doc, _ := value.(map[string]any)
            n := 0
            for name, e := range doc {
                n += len(name) + putItemValueSize(e)
            }
            return n
        case "L":
            list, _ := value.([]any)
            n := 0
            for _, e := range list {
                n += putItemValueSize(e)
            }
            return n
        }
    }
    return 0
}

// putItemWriteUnits rounds an item size up to write capacity units (1 KiB
// granularity, minimum one unit).
func putItemWriteUnits(size int) int {
    units := (size + putItemKB - 1) / putItemKB
    if units < 1 {
        units = 1
    }
    return units
}

// putItemValidNumber accepts the number forms the real service parses:
// optional sign, digits with optional fractional and exponential parts.
func putItemValidNumber(s string) bool {
    if s == "" {
        return false
    }
    lower := strings.ToLower(s)
    if strings.Contains(lower, "inf") || strings.Contains(lower, "nan") {
        return false
    }
    if _, err := strconv.ParseFloat(s, 64); err == nil {
        return true
    }
    // Accept decimal forms the Go parser rejects (e.g. leading zeros).
    mantissa := s
    exponent := ""
    if i := strings.IndexAny(s, "eE"); i >= 0 {
        mantissa = s[:i]
        exponent = s[i+1:]
    }
    mantissa = strings.TrimPrefix(strings.TrimPrefix(mantissa, "+"), "-")
    if mantissa == "" {
        return false
    }
    digits, dots := 0, 0
    for i := 0; i < len(mantissa); i++ {
        switch c := mantissa[i]; {
        case c >= '0' && c <= '9':
            digits++
        case c == '.':
            dots++
        default:
            return false
        }
    }
    if digits == 0 || dots > 1 {
        return false
    }
    if exponent != "" {
        if exponent[0] == '+' || exponent[0] == '-' {
            exponent = exponent[1:]
        }
        if exponent == "" {
            return false
        }
        for i := 0; i < len(exponent); i++ {
            if exponent[i] < '0' || exponent[i] > '9' {
                return false
            }
        }
    }
    return true
}
