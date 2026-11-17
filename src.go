// floci-go — an in-memory DynamoDB-compatible local emulator that speaks
// the AWS JSON 1.0 wire protocol.
//
// Ticket floci-go/ticket-013: implement the PutItem action — validate the
// request (ValidationException on violation), store the item in the table
// (overwrite semantics on primary-key collision) and return ConsumedCapacity
// when ReturnConsumedCapacity is TOTAL or INDEXES.
package main

import (
    "crypto/rand"
    "encoding/base64"
    "encoding/hex"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "log"
    "math/big"
    "net/http"
    "os"
    "regexp"
    "strconv"
    "strings"
    "sync"
    "time"
)

// ===========================================================================
// Errors — DynamoDB protocol error shape and helpers
// ===========================================================================

// dynamoErrPrefix is the namespace DynamoDB uses in the __type field of
// service errors.
const dynamoErrPrefix = "com.amazonaws.dynamodb.v20120810#"

// APIError is a DynamoDB protocol error. Type carries the full __type
// identifier, e.g. "com.amazonaws.dynamodb.v20120810#ValidationException".
type APIError struct {
    Type    string
    Message string
    Status  int
}

func (e *APIError) Error() string {
    return strings.TrimPrefix(e.Type, dynamoErrPrefix) + ": " + e.Message
}

// MarshalJSON renders the AWS JSON 1.0 error body.
func (e *APIError) MarshalJSON() ([]byte, error) {
    body := map[string]any{"__type": e.Type}
    if e.Message != "" {
        body["message"] = e.Message
    }
    return json.Marshal(body)
}

func validationException(format string, args ...any) *APIError {
    return &APIError{
        Type:    dynamoErrPrefix + "ValidationException",
        Message: fmt.Sprintf(format, args...),
        Status:  http.StatusBadRequest,
    }
}

func resourceNotFoundException(tableName string) *APIError {
    return &APIError{
        Type:    dynamoErrPrefix + "ResourceNotFoundException",
        Message: "Requested resource not found: Table: " + tableName + " not found",
        Status:  http.StatusBadRequest,
    }
}

// validateEnum enforces an optional enum parameter (empty means "not
// provided"), mirroring the smithy enum constraint message.
func validateEnum(field, value string, allowed ...string) *APIError {
    if value == "" {
        return nil
    }
    for _, a := range allowed {
        if value == a {
            return nil
        }
    }
    return validationException(
        "1 validation error detected: Value '%s' at '%s' failed to satisfy constraint: Member must satisfy enum value set: [%s]",
        value, field, strings.Join(allowed, ", "))
}

// ===========================================================================
// AttributeValue — the DynamoDB attribute value wire model
// ===========================================================================

// AttributeValue is a single DynamoDB attribute value. Exactly one member is
// set; a value with no member (or more than one) is rejected during request
// validation with ValidationException.
type AttributeValue struct {
    B    []byte                     // B    — binary (base64 on the wire)
    BOOL *bool                      // BOOL — boolean
    BS   [][]byte                   // BS   — binary set
    L    []*AttributeValue          // L    — list
    M    map[string]*AttributeValue // M    — map
    N    *string                    // N    — number (decimal string)
    NULL *bool                      // NULL — null
    S    *string                    // S    — string
    NS   []string                   // NS   — number set
    SS   []string                   // SS   — string set
}

var (
    errNoDataType    = errors.New("Supplied AttributeValue is empty, must contain exactly one of the supported datatypes")
    errMultiDataType = errors.New("Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes")
)

// attrValueError marks errors that DynamoDB reports as ValidationException
// (attribute value structure) rather than transport-level
// SerializationException.
type attrValueError struct{ err error }

func (e *attrValueError) Error() string { return e.err.Error() }

func attrErr(err error) error { return &attrValueError{err: err} }

// convErr mirrors DynamoDB's "convertible value" error family.
func convErr(kind string) error {
    return fmt.Errorf("The parameter cannot be converted to a %s value", kind)
}

// conversionKind maps an AttributeValue type key to its conversion name.
func conversionKind(t string) string {
    switch t {
    case "N", "NS":
        return "numeric"
    case "B", "BS":
        return "binary"
    case "BOOL", "NULL":
        return "boolean"
    case "L":
        return "list"
    case "M":
        return "map"
    default:
        return "text"
    }
}

// MarshalJSON emits the single-key wire form used by the DynamoDB JSON
// protocol, e.g. {"S":"x"} or {"N":"12"}.
func (v *AttributeValue) MarshalJSON() ([]byte, error) {
    if v == nil {
        return nil, errNoDataType
    }
    switch {
    case v.S != nil:
        return json.Marshal(map[string]any{"S": *v.S})
    case v.N != nil:
        return json.Marshal(map[string]any{"N": *v.N})
    case v.B != nil:
        return json.Marshal(map[string]any{"B": base64.StdEncoding.EncodeToString(v.B)})
    case v.BOOL != nil:
        return json.Marshal(map[string]any{"BOOL": *v.BOOL})
    case v.NULL != nil:
        return json.Marshal(map[string]any{"NULL": *v.NULL})
    case v.SS != nil:
        return json.Marshal(map[string]any{"SS": v.SS})
    case v.NS != nil:
        return json.Marshal(map[string]any{"NS": v.NS})
    case v.BS != nil:
        enc := make([]string, len(v.BS))
        for i, b := range v.BS {
            enc[i] = base64.StdEncoding.EncodeToString(b)
        }
        return json.Marshal(map[string]any{"BS": enc})
    case v.L != nil:
        return json.Marshal(map[string]any{"L": v.L})
    case v.M != nil:
        return json.Marshal(map[string]any{"M": v.M})
    default:
        return nil, errNoDataType
    }
}

// UnmarshalJSON parses the single-key wire form. Structural violations (no
// datatype key, several datatype keys, wrong inner types, bad base64) return
// errors that surface as ValidationException.
func (v *AttributeValue) UnmarshalJSON(data []byte) error {
    if v == nil {
        return attrErr(errors.New("AttributeValue cannot be unmarshaled into nil"))
    }
    var raw map[string]json.RawMessage
    if err := json.Unmarshal(data, &raw); err != nil {
        return err
    }
    *v = AttributeValue{}
    if len(raw) == 0 {
        return attrErr(errNoDataType)
    }
    if len(raw) > 1 {
        return attrErr(errMultiDataType)
    }
    for t, payload := range raw {
        if string(payload) == "null" {
            return attrErr(convErr(conversionKind(t)))
        }
        switch t {
        case "S":
            v.S = new(string)
            if err := json.Unmarshal(payload, v.S); err != nil {
                return attrErr(convErr("text"))
            }
        case "N":
            v.N = new(string)
            if err := json.Unmarshal(payload, v.N); err != nil {
                return attrErr(convErr("numeric"))
            }
        case "B":
            var s string
            if err := json.Unmarshal(payload, &s); err != nil {
                return attrErr(convErr("binary"))
            }
            b, err := base64.StdEncoding.DecodeString(s)
            if err != nil {
                return attrErr(convErr("binary"))
            }
            if b == nil {
                b = []byte{}
            }
            v.B = b
        case "BOOL":
            v.BOOL = new(bool)
            if err := json.Unmarshal(payload, v.BOOL); err != nil {
                return attrErr(convErr("boolean"))
            }
        case "NULL":
            v.NULL = new(bool)
            if err := json.Unmarshal(payload, v.NULL); err != nil {
                return attrErr(convErr("boolean"))
            }
        case "SS":
            if err := json.Unmarshal(payload, &v.SS); err != nil {
                return attrErr(convErr("text"))
            }
        case "NS":
            if err := json.Unmarshal(payload, &v.NS); err != nil {
                return attrErr(convErr("numeric"))
            }
        case "BS":
            var enc []string
            if err := json.Unmarshal(payload, &enc); err != nil {
                return attrErr(convErr("binary"))
            }
            v.BS = make([][]byte, 0, len(enc))
            for _, e := range enc {
                b, err := base64.StdEncoding.DecodeString(e)
                if err != nil {
                    return attrErr(convErr("binary"))
                }
                if b == nil {
                    b = []byte{}
                }
                v.BS = append(v.BS, b)
            }
        case "L":
            if err := json.Unmarshal(payload, &v.L); err != nil {
                return attrErr(convErr("list"))
            }
        case "M":
            if err := json.Unmarshal(payload, &v.M); err != nil {
                return attrErr(convErr("map"))
            }
        default:
            return attrErr(errNoDataType)
        }
    }
    return nil
}

// isSet reports whether any datatype member is populated.
func (v *AttributeValue) isSet() bool {
    return v.S != nil || v.N != nil || v.B != nil || v.BOOL != nil ||
        v.NULL != nil || v.SS != nil || v.NS != nil || v.BS != nil ||
        v.L != nil || v.M != nil
}

// ===========================================================================
// Number validation
// ===========================================================================

// numberPattern accepts an optional minus, decimal digits with an optional
// fraction and optional scientific exponent — DynamoDB's "N" grammar minus
// spellings the real service rejects (leading '+', trailing dot, etc.).
var numberPattern = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

// DynamoDB numbers support 38 digits of precision and a decimal exponent in
// roughly [-130, +125].
const (
    maxSignificantDigits = 38
    minDecimalExponent   = -130
    maxDecimalExponent   = 125
)

// validNumber reports whether s is a well-formed DynamoDB number within the
// supported precision and exponent range.
func validNumber(s string) bool {
    if !numberPattern.MatchString(s) {
        return false
    }
    mant := s
    exp := 0
    if i := strings.IndexAny(s, "eE"); i >= 0 {
        var err error
        mant = s[:i]
        exp, err = strconv.Atoi(s[i+1:])
        if err != nil {
            return false
        }
    }
    dot := strings.IndexByte(mant, '.')
    if dot < 0 {
        dot = len(mant)
    }
    digits := strings.TrimLeft(strings.ReplaceAll(mant, ".", ""), "0")
    if digits == "" {
        return true // zero is always representable
    }
    if len(digits) > maxSignificantDigits {
        return false
    }
    fs := strings.IndexFunc(mant, func(r rune) bool { return r >= '1' && r <= '9' })
    var e10 int
    if fs < dot {
        e10 = dot - 1 - fs
    } else {
        e10 = dot - fs
    }
    e10 += exp
    if e10 > maxDecimalExponent || e10 < minDecimalExponent {
        return false
    }
    _, ok := new(big.Rat).SetString(s)
    return ok
}

// ===========================================================================
// Validation of attribute values
// ===========================================================================

// maxNestingDepth is DynamoDB's limit on list/map nesting.
const maxNestingDepth = 32

// validateAttr enforces DynamoDB's structural rules on an attribute value and
// recurses into collections.
func validateAttr(v *AttributeValue) *APIError {
    return validateAttrDepth(v, 1)
}

func validateAttrDepth(v *AttributeValue, depth int) *APIError {
    if v == nil || !v.isSet() {
        return validationException("%s", errNoDataType.Error())
    }
    if depth > maxNestingDepth {
        return validationException("Item depth exceeds the maximum allowed depth")
    }
    switch {
    case v.N != nil:
        if !validNumber(*v.N) {
            return validationException("One or more parameter values were invalid: Number is invalid or not exactly representable")
        }
    case v.SS != nil:
        if len(v.SS) == 0 {
            return validationException("One or more parameter values were invalid: An string set may not be empty")
        }
    case v.NS != nil:
        if len(v.NS) == 0 {
            return validationException("One or more parameter values were invalid: An number set may not be empty")
        }
        for _, n := range v.NS {
            if !validNumber(n) {
                return validationException("One or more parameter values were invalid: Number is invalid or not exactly representable")
            }
        }
    case v.BS != nil:
        if len(v.BS) == 0 {
            return validationException("One or more parameter values were invalid: An binary set may not be empty")
        }
    case v.L != nil:
        for _, el := range v.L {
            if err := validateAttrDepth(el, depth+1); err != nil {
                return err
            }
        }
    case v.M != nil:
        for _, el := range v.M {
            if err := validateAttrDepth(el, depth+1); err != nil {
                return err
            }
        }
    }
    return nil
}

// ===========================================================================
// Storage — tables and items
// ===========================================================================

// KeySchemaElement is one HASH or RANGE attribute of a table's primary key.
type KeySchemaElement struct {
    AttributeName string
    KeyType       string // "HASH" or "RANGE"
}

// Table is one DynamoDB table: schema definitions plus stored items. All
// methods are safe for concurrent use.
type Table struct {
    Name          string
    AttributeDefs map[string]string // attribute name -> "S" | "N" | "B"
    KeySchema     []KeySchemaElement

    mu        sync.Mutex
    items     map[string]map[string]*AttributeValue // canonical key -> item
    createdAt time.Time
}

// ItemCount reports the number of items currently stored in the table.
func (t *Table) ItemCount() int {
    t.mu.Lock()
    defer t.mu.Unlock()
    return len(t.items)
}

// Store is the in-memory registry of tables backing every action. Safe for
// concurrent use.
type Store struct {
    mu     sync.RWMutex
    tables map[string]*Table
}

// NewStore returns an empty store.
func NewStore() *Store {
    return &Store{tables: make(map[string]*Table)}
}

// GetTable returns the named table, or false if it does not exist.
func (st *Store) GetTable(name string) (*Table, bool) {
    st.mu.RLock()
    defer st.mu.RUnlock()
    t, ok := st.tables[name]
    return t, ok
}

// CreateTable registers a table definition. The table-management actions from
// earlier tickets use this backing store; tests use it to create fixtures.
func (st *Store) CreateTable(name string, attributeDefs map[string]string, keySchema []KeySchemaElement) (*Table, *APIError) {
    if name == "" {
        return nil, validationException("1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null")
    }
    if len(keySchema) == 0 || keySchema[0].KeyType != "HASH" {
        return nil, validationException("One or more parameter values were invalid: No Hash Key has been defined for the table")
    }
    st.mu.Lock()
    defer st.mu.Unlock()
    if _, exists := st.tables[name]; exists {
        return nil, &APIError{
            Type:    dynamoErrPrefix + "ResourceInUseException",
            Message: "Cannot create preexisting table",
            Status:  http.StatusBadRequest,
        }
    }
    defs := make(map[string]string, len(attributeDefs))
    for k, v := range attributeDefs {
        defs[k] = v
    }
    schema := make([]KeySchemaElement, len(keySchema))
    copy(schema, keySchema)
    t := &Table{
        Name:          name,
        AttributeDefs: defs,
        KeySchema:     schema,
        items:         make(map[string]map[string]*AttributeValue),
        createdAt:     time.Now().UTC(),
    }
    st.tables[name] = t
    return t, nil
}

// DeleteTable removes and returns a table by name.
func (st *Store) DeleteTable(name string) (*Table, bool) {
    st.mu.Lock()
    defer st.mu.Unlock()
    t, ok := st.tables[name]
    if ok {
        delete(st.tables, name)
    }
    return t, ok
}

// Lookup returns a deep copy of the stored item with the given primary key
// attributes, or false if absent. Keys must be schema-valid.
func (st *Store) Lookup(tableName string, key map[string]*AttributeValue) (map[string]*AttributeValue, bool) {
    table, ok := st.GetTable(tableName)
    if !ok {
        return nil, false
    }
    enc, err := table.keyString(key)
    if err != nil {
        return nil, false
    }
    table.mu.Lock()
    defer table.mu.Unlock()
    item, ok := table.items[enc]
    if !ok {
        return nil, false
    }
    return deepCopyItem(item), true
}

// keyString derives the internal canonical primary-key string for an item.
// Numbers are normalized through big.Rat so that numerically equal values
// ("1", "1.0", "1e2") map to the same item. The item's keys must already
// have been schema-validated.
func (t *Table) keyString(item map[string]*AttributeValue) (string, *APIError) {
    parts := make([]string, 0, len(t.KeySchema))
    for _, ks := range t.KeySchema {
        v := item[ks.AttributeName]
        if v == nil {
            return "", validationException("The provided key element does not match the schema")
        }
        switch {
        case v.S != nil:
            parts = append(parts, "S:"+strconv.Quote(*v.S))
        case v.N != nil:
            r, ok := new(big.Rat).SetString(*v.N)
            if !ok {
                return "", validationException("One or more parameter values were invalid: Number is invalid or not exactly representable")
            }
            parts = append(parts, "N:"+r.RatString())
        case v.B != nil:
            parts = append(parts, "B:"+hex.EncodeToString(v.B))
        default:
            return "", validationException("The provided key element does not match the schema")
        }
    }
    return strings.Join(parts, "|"), nil
}

// validateKeys checks that the item populates every schema key with a value
// whose type matches the table's AttributeDefinitions.
func (t *Table) validateKeys(item map[string]*AttributeValue) *APIError {
    for _, ks := range t.KeySchema {
        v := item[ks.AttributeName]
        if v == nil {
            return validationException("The provided key element does not match the schema")
        }
        defType, defined := t.AttributeDefs[ks.AttributeName]
        if !defined {
            return validationException("The provided key element does not match the schema")
        }
        limit := 1024
        if ks.KeyType == "HASH" {
            limit = 2048
        }
        switch {
        case v.S != nil:
            if defType != "S" {
                return validationException("The provided key element does not match the schema")
            }
            if *v.S == "" {
                return validationException("One or more parameter values were invalid: The AttributeValue for a key attribute cannot contain an empty string value. Key: %s", ks.AttributeName)
            }
            if len(*v.S) > limit {
                return validationException("One or more parameter values were invalid: Size exceeds %d bytes for String", limit)
            }
        case v.N != nil:
            if defType != "N" {
                return validationException("The provided key element does not match the schema")
            }
            if !validNumber(*v.N) {
                return validationException("One or more parameter values were invalid: Number is invalid or not exactly representable")
            }
        case v.B != nil:
            if defType != "B" {
                return validationException("The provided key element does not match the schema")
            }
            if len(v.B) == 0 {
                return validationException("One or more parameter values were invalid: The AttributeValue for a key attribute cannot contain an empty binary value. Key: %s", ks.AttributeName)
            }
            if len(v.B) > limit {
                return validationException("One or more parameter values were invalid: Size exceeds %d bytes for Binary", limit)
            }
        default:
            return validationException("The provided key element does not match the schema")
        }
    }
    return nil
}

// ===========================================================================
// Item sizing and deep copies
// ===========================================================================

// attrSize approximates the byte size of an attribute value per DynamoDB's
// item-sizing rules: strings/numbers count their UTF-8 length, binary counts
// raw bytes, scalars are 1 byte, sets sum members, lists/maps add container
// overhead plus 1 byte per element.
func attrSize(v *AttributeValue) int {
    if v == nil {
        return 0
    }
    switch {
    case v.S != nil:
        return len(*v.S)
    case v.N != nil:
        return len(*v.N)
    case v.B != nil:
        return len(v.B)
    case v.BOOL != nil:
        return 1
    case v.NULL != nil:
        return 1
    case v.SS != nil:
        n := 0
        for _, s := range v.SS {
            n += len(s)
        }
        return n
    case v.NS != nil:
        n := 0
        for _, s := range v.NS {
            n += len(s)
        }
        return n
    case v.BS != nil:
        n := 0
        for _, b := range v.BS {
            n += len(b)
        }
        return n
    case v.L != nil:
        n := 3
        for _, el := range v.L {
            n += attrSize(el) + 1
        }
        return n
    case v.M != nil:
        n := 3
        for k, el := range v.M {
            n += len(k) + attrSize(el) + 1
        }
        return n
    }
    return 0
}

// itemSize is the sum of attribute names and value sizes.
func itemSize(item map[string]*AttributeValue) int {
    total := 0
    for name, v := range item {
        total += len(name) + attrSize(v)
    }
    return total
}

func deepCopyItem(item map[string]*AttributeValue) map[string]*AttributeValue {
    out := make(map[string]*AttributeValue, len(item))
    for k, v := range item {
        out[k] = deepCopyValue(v)
    }
    return out
}

func deepCopyValue(v *AttributeValue) *AttributeValue {
    if v == nil {
        return nil
    }
    c := &AttributeValue{}
    if v.S != nil {
        s := *v.S
        c.S = &s
    }
    if v.N != nil {
        s := *v.N
        c.N = &s
    }
    if v.B != nil {
        c.B = append([]byte(nil), v.B...)
    }
    if v.BOOL != nil {
        b := *v.BOOL
        c.BOOL = &b
    }
    if v.NULL != nil {
        b := *v.NULL
        c.NULL = &b
    }
    if v.SS != nil {
        c.SS = append([]string(nil), v.SS...)
    }
    if v.NS != nil {
        c.NS = append([]string(nil), v.NS...)
    }
    if v.BS != nil {
        c.BS = make([][]byte, len(v.BS))
        for i, b := range v.BS {
            c.BS[i] = append([]byte(nil), b...)
        }
    }
    if v.L != nil {
        c.L = make([]*AttributeValue, len(v.L))
        for i, el := range v.L {
            c.L[i] = deepCopyValue(el)
        }
    }
    if v.M != nil {
        c.M = make(map[string]*AttributeValue, len(v.M))
        for k, el := range v.M {
            c.M[k] = deepCopyValue(el)
        }
    }
    return c
}

// ===========================================================================
// PutItem
// ===========================================================================

// maxItemSize is DynamoDB's 400 KB item limit.
const maxItemSize = 400 * 1024

// PutItemInput is the PutItem request.
type PutItemInput struct {
    TableName                   string                     `json:"TableName"`
    Item                        map[string]*AttributeValue `json:"Item"`
    ReturnValues                string                     `json:"ReturnValues"`
    ReturnConsumedCapacity      string                     `json:"ReturnConsumedCapacity"`
    ReturnItemCollectionMetrics string                     `json:"ReturnItemCollectionMetrics"`
}

// PutItemOutput is the PutItem response.
type PutItemOutput struct {
    Attributes       map[string]*AttributeValue `json:"Attributes,omitempty"`
    ConsumedCapacity *ConsumedCapacity          `json:"ConsumedCapacity,omitempty"`
}

// units renders a capacity number the way DynamoDB does on the wire, e.g. 1.0.
type units float64

func (u units) MarshalJSON() ([]byte, error) {
    s := strconv.FormatFloat(float64(u), 'f', -1, 64)
    if !strings.ContainsAny(s, ".eE") {
        s += ".0"
    }
    return []byte(s), nil
}

// Capacity is a capacity amount.
type Capacity struct {
    CapacityUnits units `json:"CapacityUnits"`
}

// ConsumedCapacity is the charge reported for an operation.
type ConsumedCapacity struct {
    TableName              string              `json:"TableName,omitempty"`
    CapacityUnits          units               `json:"CapacityUnits"`
    Table                  *Capacity           `json:"Table,omitempty"`
    LocalSecondaryIndexes  map[string]Capacity `json:"LocalSecondaryIndexes,omitempty"`
    GlobalSecondaryIndexes map[string]Capacity `json:"GlobalSecondaryIndexes,omitempty"`
}

// writeCapacityUnits is the number of WCUs a PutItem consumes: one unit per
// 1 KB of item size, minimum one unit.
func writeCapacityUnits(sizeBytes int) units {
    u := (sizeBytes + 1023) / 1024
    if u < 1 {
        u = 1
    }
    return units(u)
}

// PutItem validates the request, stores the item (replacing any previous
// item with the same primary key) and reports the write capacity consumed.
func (st *Store) PutItem(in *PutItemInput) (*PutItemOutput, *APIError) {
    if in == nil {
        in = &PutItemInput{}
    }
    if in.TableName == "" {
        return nil, validationException("1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null")
    }
    if l := len(in.TableName); l < 3 || l > 255 {
        return nil, validationException("1 validation error detected: Value '%s' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3 and less than or equal to 255", in.TableName)
    }
    if !tableNamePattern.MatchString(in.TableName) {
        return nil, validationException("1 validation error detected: Value '%s' at 'tableName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+", in.TableName)
    }
    if in.Item == nil {
        return nil, validationException("1 validation error detected: Value null at 'item' failed to satisfy constraint: Member must not be null")
    }
    if len(in.Item) == 0 {
        return nil, validationException("One or more parameter values were invalid: An item must contain at least one attribute")
    }
    if err := validateEnum("returnValues", in.ReturnValues, "NONE", "ALL_OLD"); err != nil {
        return nil, err
    }
    if err := validateEnum("returnConsumedCapacity", in.ReturnConsumedCapacity, "INDEXES", "TOTAL", "NONE"); err != nil {
        return nil, err
    }
    if err := validateEnum("returnItemCollectionMetrics", in.ReturnItemCollectionMetrics, "SIZE", "NONE"); err != nil {
        return nil, err
    }
    for name, v := range in.Item {
        if name == "" {
            return nil, validationException("One or more parameter values were invalid: Empty attribute name")
        }
        if err := validateAttr(v); err != nil {
            return nil, err
        }
    }

    table, ok := st.GetTable(in.TableName)
    if !ok {
        return nil, resourceNotFoundException(in.TableName)
    }
    if err := table.validateKeys(in.Item); err != nil {
        return nil, err
    }
    size := itemSize(in.Item)
    if size > maxItemSize {
        return nil, validationException("Item size to update has exceeded the maximum allowed size")
    }
    key, kerr := table.keyString(in.Item)
    if kerr != nil {
        return nil, kerr
    }

    table.mu.Lock()
    if table.items == nil {
        table.items = make(map[string]map[string]*AttributeValue)
    }
    old := table.items[key]
    table.items[key] = deepCopyItem(in.Item)
    table.mu.Unlock()

    out := &PutItemOutput{}
    if rc := in.ReturnConsumedCapacity; rc == "TOTAL" || rc == "INDEXES" {
        wcu := writeCapacityUnits(size)
        cc := &ConsumedCapacity{TableName: in.TableName, CapacityUnits: wcu}
        if rc == "INDEXES" {
            cc.Table = &Capacity{CapacityUnits: wcu}
        }
        out.ConsumedCapacity = cc
    }
    if in.ReturnValues == "ALL_OLD" && old != nil {
        out.Attributes = deepCopyItem(old)
    }
    return out, nil
}

// tableNamePattern enforces the DynamoDB table-name character set.
var tableNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)

// ===========================================================================
// HTTP adapter — AWS JSON 1.0 wire protocol with X-Amz-Target routing
// ===========================================================================

// Handler serves DynamoDB-style requests: POST with an X-Amz-Target header
// selecting the action and an AWS JSON 1.0 body. Actions floci-go has not
// implemented yet answer UnknownOperationException.
type Handler struct {
    Store *Store
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        w.WriteHeader(http.StatusMethodNotAllowed)
        return
    }
    body, err := io.ReadAll(r.Body)
    if err != nil {
        writeError(w, &APIError{
            Type:   "com.amazon.coral.service#SerializationException",
            Status: http.StatusBadRequest,
        })
        return
    }
    action := r.Header.Get("X-Amz-Target")
    if i := strings.LastIndex(action, "."); i >= 0 {
        action = action[i+1:]
    }
    switch action {
    case "PutItem":
        h.putItem(w, body)
    default:
        writeError(w, &APIError{
            Type:   "com.amazon.coral.service#UnknownOperationException",
            Status: http.StatusBadRequest,
        })
    }
}

func (h *Handler) putItem(w http.ResponseWriter, body []byte) {
    var in PutItemInput
    if err := json.Unmarshal(body, &in); err != nil {
        var ave *attrValueError
        if errors.As(err, &ave) {
            writeError(w, validationException("%s", ave.Error()))
            return
        }
        writeError(w, &APIError{
            Type:    "com.amazon.coral.service#SerializationException",
            Message: err.Error(),
            Status:  http.StatusBadRequest,
        })
        return
    }
    out, apiErr := h.Store.PutItem(&in)
    if apiErr != nil {
        writeError(w, apiErr)
        return
    }
    writeJSON(w, out)
}

func writeJSON(w http.ResponseWriter, v any) {
    data, err := json.Marshal(v)
    if err != nil {
        writeError(w, &APIError{
            Type:    dynamoErrPrefix + "InternalFailure",
            Message: err.Error(),
            Status:  http.StatusInternalServerError,
        })
        return
    }
    w.Header().Set("Content-Type", "application/x-amz-json-1.0")
    w.Header().Set("x-amzn-RequestId", newRequestID())
    w.WriteHeader(http.StatusOK)
    _, _ = w.Write(data)
}

func writeError(w http.ResponseWriter, e *APIError) {
    w.Header().Set("Content-Type", "application/x-amz-json-1.0")
    w.Header().Set("x-amzn-RequestId", newRequestID())
    w.WriteHeader(e.Status)
    _ = json.NewEncoder(w).Encode(e)
}

func newRequestID() string {
    var b [8]byte
    if _, err := rand.Read(b[:]); err != nil {
        return "00000000-0000-0000-0000-000000000000"
    }
    return hex.EncodeToString(b[:])
}

// ===========================================================================
// Standalone server entry point
// ===========================================================================

// main starts a standalone floci-go server (ad-hoc testing convenience; the
// Go tests exercise Store and Handler directly).
func main() {
    addr := os.Getenv("FLOCI_ADDR")
    if addr == "" {
        addr = ":8000"
    }
    log.Printf("floci-go (DynamoDB local emulator) listening on %s", addr)
    if err := http.ListenAndServe(addr, &Handler{Store: NewStore()}); err != nil {
        log.Fatal(err)
    }
}