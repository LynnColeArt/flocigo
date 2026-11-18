// Package ddb - DescribeTable (ticket flocigo/ticket-031).
package ddb

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"time"
)

// DescribeTableTarget is the X-Amz-Target served by this file. Mount it in
// service.go's dispatcher:
//
//	case "DynamoDB_20120810.DescribeTable":
//		return s.DescribeTable(body)
const DescribeTableTarget = "DynamoDB_20120810.DescribeTable"

// describeTableInput is the DescribeTable request payload.
type describeTableInput struct {
	TableName *string
}

// ddbFault is a DynamoDB protocol exception raised by ddb operations.
// HTTPStatus/Code/Message are exposed so the shared error layer (service.go
// + errmap) can render it; wireType carries the AWS __type envelope.
type ddbFault struct {
	httpStatus int
	wireType   string
	code       string
	message    string
}

func (e *ddbFault) Error() string { return e.wireType + ": " + e.message }

// HTTPStatus reports the mapped HTTP status code.
func (e *ddbFault) HTTPStatus() int { return e.httpStatus }

// Code reports the bare DynamoDB error code.
func (e *ddbFault) Code() string { return e.code }

// Message reports the human-readable fault text.
func (e *ddbFault) Message() string { return e.message }

func serializationFault(format string, args ...any) *ddbFault {
	return &ddbFault{
		httpStatus: http.StatusBadRequest,
		wireType:   "com.amazon.coral.service#SerializationException",
		code:       "SerializationException",
		message:    fmt.Sprintf(format, args...),
	}
}

// notFoundFault mirrors DynamoDB Local's exact not-found wording.
func notFoundFault(table string) *ddbFault {
	return &ddbFault{
		httpStatus: http.StatusBadRequest,
		wireType:   "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
		code:       "ResourceNotFoundException",
		message:    fmt.Sprintf("Requested resource not found: Table: %s not found", table),
	}
}

// DescribeTable serves DynamoDB_20120810.DescribeTable: the live
// TableDescription wrapped as {"Table": {...}}, or
// ResourceNotFoundException when no table with that name exists.
func (s *Service) DescribeTable(payload []byte) (any, error) {
	return DescribeTableOp(s, payload)
}

// DescribeTableOp implements DescribeTable for any service value; the table
// registry is resolved dynamically (Service.tables, or one hop via
// Service.store.Tables), so this op drops into internal/ddb without
// touching store.go.
func DescribeTableOp(svc any, payload []byte) (any, error) {
	var in describeTableInput
	if err := json.Unmarshal(payload, &in); err != nil {
		return nil, serializationFault("Unexpected parsing error: %v", err)
	}
	if in.TableName == nil || *in.TableName == "" {
		return nil, serializationFault("TableName must not be null")
	}
	name := *in.TableName

	tbl, ok := lookupStoredTable(svc, name)
	if !ok {
		return nil, notFoundFault(name)
	}
	return map[string]any{"Table": renderTableDescription(tbl, name)}, nil
}

// ---------------------------------------------------------------------------
// TableDescription rendering
// ---------------------------------------------------------------------------

// describeTableKey is one KeySchema element on the wire.
type describeTableKey struct {
	AttributeName string
	KeyType       string
}

// renderTableDescription builds the Table document with exactly the
// ticket-031 fields. Store state is read reflectively: tracked fields win,
// and emulator defaults fill any gap (ACTIVE status, PROVISIONED billing,
// live item count and size, as in the floci Java emulator).
func renderTableDescription(tbl reflect.Value, tableName string) map[string]any {
	v := indirectVal(tbl)
	items := describeItems(v)
	return map[string]any{
		"TableName":   tableName,
		"TableStatus": describeTableStatus(v),
		"BillingModeSummary": map[string]any{
			"BillingMode": describeBillingMode(v),
		},
		"KeySchema":        describeKeySchema(v),
		"CreationDateTime": describeEpochSeconds(v),
		"ItemCount":        describeItemCount(v, items),
		"TableSizeBytes":   describeSizeBytes(v, items),
	}
}

// describeTableStatus: tables are created ACTIVE; a tracked lifecycle
// status wins when the store keeps one.
func describeTableStatus(v reflect.Value) string {
	if s := fieldString(v, "Status", "TableStatus", "tableStatus", "status"); s != "" {
		return s
	}
	return "ACTIVE"
}

// describeBillingMode: CreateTable defaults to PROVISIONED (AWS contract).
func describeBillingMode(v reflect.Value) string {
	if s := fieldString(v, "BillingMode", "billingMode"); s != "" {
		return s
	}
	if summary := fieldStruct(v, "BillingModeSummary"); summary.IsValid() {
		if s := fieldString(summary, "BillingMode", "billingMode"); s != "" {
			return s
		}
	}
	return "PROVISIONED"
}

// describeKeySchema renders the primary key in stored order (HASH first, as
// CreateTable recorded it). A name->type map is also tolerated and emitted
// deterministically, HASH before RANGE.
func describeKeySchema(v reflect.Value) []describeTableKey {
	keys := []describeTableKey{}
	schema := indirectVal(firstField(v, "KeySchema", "TableKeySchema", "keySchema", "Schema", "schema", "Keys"))
	switch schema.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < schema.Len(); i++ {
			if k, ok := describeKeyElement(schema.Index(i), reflect.Value{}); ok {
				keys = append(keys, k)
			}
		}
	case reflect.Map:
		for _, mk := range schema.MapKeys() {
			if k, ok := describeKeyElement(mk, schema.MapIndex(mk)); ok {
				keys = append(keys, k)
			}
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].KeyType != keys[j].KeyType {
				return keys[i].KeyType < keys[j].KeyType
			}
			return keys[i].AttributeName < keys[j].AttributeName
		})
	}
	return keys
}

// describeKeyElement reads one key element from an element struct
// (AttributeName/KeyType), a bare attribute-name string, or a (name, type)
// map pair.
func describeKeyElement(nameV, typeV reflect.Value) (describeTableKey, bool) {
	elem := indirectVal(nameV)
	var key describeTableKey
	switch {
	case elem.IsValid() && elem.Kind() == reflect.Struct:
		key.AttributeName = fieldString(elem, "AttributeName", "Name")
		key.KeyType = fieldString(elem, "KeyType", "Type", "Role")
	case elem.IsValid() && elem.Kind() == reflect.String:
		key.AttributeName = elem.String()
		if t := indirectVal(typeV); t.IsValid() {
			switch t.Kind() {
			case reflect.String:
				key.KeyType = t.String()
			case reflect.Struct:
				key.KeyType = fieldString(t, "KeyType", "Type", "Role")
			}
		}
	default:
		return describeTableKey{}, false
	}
	if key.AttributeName == "" || key.KeyType == "" {
		return describeTableKey{}, false
	}
	return key, true
}

var timeType = reflect.TypeOf(time.Time{})

// describeEpochSeconds returns CreationDateTime as epoch seconds with
// fractional precision - the numeric form the real service emits.
func describeEpochSeconds(v reflect.Value) float64 {
	f := indirectVal(firstField(v, "CreatedAt", "CreationTime", "CreationDate", "createdAt", "creationTime", "created"))
	if !f.IsValid() {
		return 0
	}
	if f.Type() == timeType {
		return reflectedTimeSeconds(f)
	}
	if n, ok := fieldNumber(v, "CreatedAt", "CreationTime", "CreationDate", "createdAt", "creationTime", "created"); ok {
		if n > 1e11 { // millisecond epochs
			return n / 1e3
		}
		return n
	}
	return 0
}

// reflectedTimeSeconds renders a captured time.Time as epoch seconds.
// Values derived from unexported store fields refuse Interface(), so the
// time package's internal (wall, ext) encoding is read directly; numeric
// getters are permitted on such reflect-read-only values.
func reflectedTimeSeconds(t reflect.Value) float64 {
	if t.CanInterface() {
		if tt, ok := t.Interface().(time.Time); ok {
			return float64(tt.UnixMilli()) / 1e3
		}
	}
	wallV, extV := t.FieldByName("wall"), t.FieldByName("ext")
	if !wallV.IsValid() || !extV.IsValid() ||
		wallV.Kind() != reflect.Uint64 || extV.Kind() != reflect.Int64 {
		return 0
	}
	wall, ext := wallV.Uint(), extV.Int()
	if wall == 0 && ext == 0 {
		return 0 // zero time: the store never recorded a creation date
	}
	const (
		hasMonotonic   = uint64(1) << 63
		wallToInternal = int64(688117) * 86400 // seconds from year 1 to 1885
		unixToInternal = int64(719162) * 86400 // seconds from year 1 to 1970
	)
	abs := ext
	if wall&hasMonotonic != 0 { // monotonic clock: 33-bit seconds since 1885
		abs = wallToInternal + int64((wall>>30)&((uint64(1)<<33)-1))
	}
	return float64(abs-unixToInternal) + float64(wall&((uint64(1)<<30)-1))/1e9
}

// describeItems locates the live item container on the stored table.
func describeItems(v reflect.Value) reflect.Value {
	return firstField(v, "Items", "items", "Rows", "rows", "Entries", "entries", "Records", "records")
}

// describeItemCount: a maintained counter wins; otherwise count live items,
// summing the innermost containers of pk->sk nested layouts.
func describeItemCount(v reflect.Value, items reflect.Value) int {
	if n, ok := fieldNumber(v, "ItemCount", "itemCount"); ok && n >= 0 {
		return int(n)
	}
	m := indirectVal(items)
	switch m.Kind() {
	case reflect.Map:
		if m.Len() == 0 {
			return 0
		}
		sample := indirectVal(m.MapIndex(m.MapKeys()[0]))
		if sample.IsValid() && (sample.Kind() == reflect.Map || sample.Kind() == reflect.Slice) {
			total := 0
			iter := m.MapRange()
			for iter.Next() {
				inner := indirectVal(iter.Value())
				if inner.IsValid() && (inner.Kind() == reflect.Map || inner.Kind() == reflect.Slice) {
					total += inner.Len()
				}
			}
			return total
		}
		return m.Len()
	case reflect.Slice, reflect.Array:
		return m.Len()
	}
	return 0
}

// describeSizeBytes: a maintained aggregate wins; otherwise sum the tracked
// per-item sizes (or estimate from the serialized attribute body, matching
// how DynamoDB derives TableSizeBytes from serialized items).
func describeSizeBytes(v reflect.Value, items reflect.Value) int64 {
	if n, ok := fieldNumber(v, "TableSizeBytes", "sizeBytes", "TableSize"); ok && n >= 0 {
		return int64(n)
	}
	m := indirectVal(items)
	var total int64
	switch m.Kind() {
	case reflect.Map:
		iter := m.MapRange()
		for iter.Next() {
			total += describeItemSize(iter.Value())
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < m.Len(); i++ {
			total += describeItemSize(m.Index(i))
		}
	}
	return total
}

// describeItemSize reads a tracked size off one item record, or estimates one.
func describeItemSize(elem reflect.Value) int64 {
	e := indirectVal(elem)
	if !e.IsValid() {
		return 0
	}
	switch e.Kind() {
	case reflect.Struct:
		if n, ok := fieldNumber(e, "SizeBytes", "sizeBytes", "Size", "size", "EncodedSize", "ByteSize", "SizeInBytes"); ok {
			return int64(n)
		}
		if attrs := describeAttrMap(e); attrs.IsValid() {
			return mirrorBytes(attrs)
		}
		return 0
	case reflect.Map:
		return mirrorBytes(e)
	}
	return 0
}

// describeAttrMap locates the attribute payload on an item record.
func describeAttrMap(e reflect.Value) reflect.Value {
	m := indirectVal(firstField(e, "Attrs", "attrs", "Attributes", "attributes", "Data", "data", "Values", "values"))
	if m.IsValid() && m.Kind() == reflect.Map {
		return m
	}
	return reflect.Value{}
}

// mirrorBytes sizes the serialized attribute body in bytes.
func mirrorBytes(v reflect.Value) int64 {
	b, err := json.Marshal(mirrorValue(v, 0))
	if err != nil {
		return 0
	}
	return int64(len(b))
}

// mirrorValue copies v into plain Go values: values captured through
// unexported store fields are reflect-read-only and cannot be handed to
// json.Marshal directly. Depth-capped; never panics.
func mirrorValue(v reflect.Value, depth int) any {
	if !v.IsValid() || depth > 8 {
		return nil
	}
	v = indirectVal(v)
	if !v.IsValid() {
		return nil
	}
	switch v.Kind() {
	case reflect.Bool:
		return v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint()
	case reflect.Float32, reflect.Float64:
		return v.Float()
	case reflect.String:
		return v.String()
	case reflect.Slice:
		if v.IsNil() {
			return nil
		}
		fallthrough
	case reflect.Array:
		out := make([]any, v.Len())
		for i := range out {
			out[i] = mirrorValue(v.Index(i), depth+1)
		}
		return out
	case reflect.Map:
		out := make(map[string]any, v.Len())
		iter := v.MapRange()
		for iter.Next() {
			k := iter.Key()
			if k.Kind() != reflect.String {
				return nil
			}
			out[k.String()] = mirrorValue(iter.Value(), depth+1)
		}
		return out
	case reflect.Struct:
		out := make(map[string]any, v.NumField())
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			if t.Field(i).PkgPath != "" {
				continue
			}
			out[t.Field(i).Name] = mirrorValue(v.Field(i), depth+1)
		}
		return out
	}
	return nil
}

// ---------------------------------------------------------------------------
// Reflective store adapters (read-only; never panic)
// ---------------------------------------------------------------------------

// indirectVal dereferences pointers and interfaces; it yields the zero Value
// when it lands on nil or an unsupported value.
func indirectVal(v reflect.Value) reflect.Value {
	for v.IsValid() {
		switch v.Kind() {
		case reflect.Pointer, reflect.Interface:
			if v.IsNil() {
				return reflect.Value{}
			}
			v = v.Elem()
		default:
			return v
		}
	}
	return v
}

// firstField returns the first present field among names (embedded structs
// are followed by FieldByName).
func firstField(v reflect.Value, names ...string) reflect.Value {
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return reflect.Value{}
	}
	for _, name := range names {
		if f := v.FieldByName(name); f.IsValid() {
			return f
		}
	}
	return reflect.Value{}
}

func fieldString(v reflect.Value, names ...string) string {
	f := indirectVal(firstField(v, names...))
	if f.IsValid() && f.Kind() == reflect.String {
		return f.String()
	}
	return ""
}

func fieldStruct(v reflect.Value, name string) reflect.Value {
	f := indirectVal(firstField(v, name))
	if f.IsValid() && f.Kind() == reflect.Struct {
		return f
	}
	return reflect.Value{}
}

// fieldNumber reads numeric fields as float64 (permitted on values captured
// through unexported fields).
func fieldNumber(v reflect.Value, names ...string) (float64, bool) {
	f := indirectVal(firstField(v, names...))
	if !f.IsValid() {
		return 0, false
	}
	switch f.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(f.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(f.Uint()), true
	case reflect.Float32, reflect.Float64:
		return f.Float(), true
	}
	return 0, false
}

// ---------------------------------------------------------------------------
// Store table lookup
// ---------------------------------------------------------------------------

var (
	// tableRegistryFieldNames covers the live name->table registries used by
	// store.go layouts.
	tableRegistryFieldNames = []string{"tables", "Tables", "tablesByName", "tablesMap", "allTables"}
	// tableStoreFieldNames covers aggregate stores one hop from Service
	// (Service.store.Tables is the common layout).
	tableStoreFieldNames = []string{"store", "Store", "st", "db", "repo", "backend", "storage"}
)

// lookupStoredTable resolves the table for name on svc without hard-coding
// the store layout: Service.tables directly, or one hop via Service.store.
func lookupStoredTable(svc any, name string) (reflect.Value, bool) {
	v := indirectVal(reflect.ValueOf(svc))
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return reflect.Value{}, false
	}
	tbl, found, has := registryLookup(v, name)
	if has {
		return tbl, found
	}
	for _, fieldName := range tableStoreFieldNames {
		store := indirectVal(firstField(v, fieldName))
		if !store.IsValid() || store.Kind() != reflect.Struct {
			continue
		}
		tbl, found, has := registryLookup(store, name)
		if has {
			return tbl, found
		}
	}
	return reflect.Value{}, false
}

// registryLookup looks name up in structVal's registry. has reports whether
// structVal holds a registry at all (its miss is authoritative:
// ResourceNotFoundException); found reports the hit with tbl set.
func registryLookup(structVal reflect.Value, name string) (tbl reflect.Value, found, has bool) {
	for _, fieldName := range tableRegistryFieldNames {
		reg := indirectVal(firstField(structVal, fieldName))
		if !reg.IsValid() || reg.Kind() != reflect.Map || reg.Type().Key().Kind() != reflect.String {
			continue
		}
		key := reflect.ValueOf(name)
		if key.Type() != reg.Type().Key() {
			key = key.Convert(reg.Type().Key())
		}
		if entry := reg.MapIndex(key); entry.IsValid() {
			if t := indirectVal(entry); t.IsValid() {
				return t, true, true
			}
		}
		return reflect.Value{}, false, true
	}
	return reflect.Value{}, false, false
}
