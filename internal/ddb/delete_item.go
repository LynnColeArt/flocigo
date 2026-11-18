package ddb

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"unsafe"
)

// DeleteItem (ticket floci-go/ticket-016): remove the item identified by Key
// from the table; when ReturnValues is ALL_OLD the previously stored item is
// echoed back under "Attributes".
//
// Wire behavior (DynamoDB 2012-08-10, AWS JSON 1.0 protocol):
//   - item deleted, ALL_OLD requested -> 200 {"Attributes": {...}}
//   - key not present                 -> 200 {} (DeleteItem is idempotent)
//   - unknown table                   -> 400 ResourceNotFoundException
//   - bad Key / TableName / ReturnValues / key shape -> 400 ValidationException
//
// The in-memory table store is resolved with small reflective helpers so this
// file cooperates with the layout shared by the create/put/get/update
// operations (canonically keyed item maps, struct tables, mutex-guarded
// stores, and stores holding native Go values instead of wire-shaped
// AttributeValue maps) without modifying any other file.

const deleteItemErrPrefix = "com.amazonaws.dynamodb.v20120810#"

var deleteItemAVTypes = map[string]bool{
	"S": true, "N": true, "B": true, "BOOL": true, "NULL": true,
	"M": true, "L": true, "SS": true, "NS": true, "BS": true,
}

// deleteItemInput carries the subset of the DeleteItem request used here.
type deleteItemInput struct {
	TableName    string         `json:"TableName"`
	Key          map[string]any `json:"Key"`
	ReturnValues string         `json:"ReturnValues"`
}

// DeleteItem handles the DeleteItem action.
func (s *Service) DeleteItem(w http.ResponseWriter, r *http.Request) { s.deleteItemRun(w, r) }

// deleteItem is the conventional dispatcher alias.
func (s *Service) deleteItem(w http.ResponseWriter, r *http.Request) { s.deleteItemRun(w, r) }

// handleDeleteItem is an alternate dispatcher alias.
func (s *Service) handleDeleteItem(w http.ResponseWriter, r *http.Request) { s.deleteItemRun(w, r) }

// deleteItemRun validates the request, deletes the item and writes the reply.
func (s *Service) deleteItemRun(w http.ResponseWriter, r *http.Request) {
	defer deleteItemRecover(w)

	var in deleteItemInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		deleteItemFail(w, http.StatusBadRequest, "ValidationException", "The request is not valid JSON.")
		return
	}
	if in.TableName == "" {
		deleteItemFail(w, http.StatusBadRequest, "ValidationException",
			"1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null")
		return
	}
	if in.ReturnValues != "" && in.ReturnValues != "NONE" && in.ReturnValues != "ALL_OLD" {
		deleteItemFail(w, http.StatusBadRequest, "ValidationException",
			fmt.Sprintf("1 validation error detected: Value '%s' at 'returnValues' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]", in.ReturnValues))
		return
	}
	if len(in.Key) == 0 {
		deleteItemFail(w, http.StatusBadRequest, "ValidationException",
			"1 validation error detected: Value null at 'key' failed to satisfy constraint: Member must not be null")
		return
	}
	want := make(map[string]any, len(in.Key))
	for name, raw := range in.Key {
		av, ok := raw.(map[string]any)
		if !ok || len(av) != 1 {
			deleteItemFail(w, http.StatusBadRequest, "ValidationException",
				"One or more parameter values were invalid: Unsupported AttributeValue")
			return
		}
		for typ, val := range av {
			if str, isStr := val.(string); isStr && str == "" && (typ == "S" || typ == "B") {
				deleteItemFail(w, http.StatusBadRequest, "ValidationException",
					fmt.Sprintf("One or more parameter values were invalid: The AttributeValue for a key attribute cannot contain an empty string value. Key: %s", name))
				return
			}
		}
		want[name] = deleteItemCanon(raw)
	}

	unlock := deleteItemAcquireLock(s)
	defer unlock()

	tables, found := deleteItemTableMap(s)
	if !found {
		deleteItemTableNotFound(w, in.TableName)
		return
	}
	tv := tables.MapIndex(reflect.ValueOf(in.TableName))
	if !tv.IsValid() {
		deleteItemTableNotFound(w, in.TableName)
		return
	}
	table := reflect.Indirect(tv)
	if table.Kind() == reflect.Struct {
		if hash, rng, known := deleteItemKeyNames(table); known {
			if !deleteItemKeyShape(w, hash, rng, in.Key) {
				return
			}
		}
	}

	var old map[string]any
	deleted := false
	if items, ok := deleteItemItemsValue(table); ok {
		old, deleted = deleteItemApply(items, want)
	}
	if !deleted || in.ReturnValues != "ALL_OLD" {
		deleteItemReply(w, map[string]any{})
		return
	}
	deleteItemReply(w, map[string]any{"Attributes": deleteItemToWire(old)})
}

// deleteItemKeyShape verifies that the request key matches the table schema.
func deleteItemKeyShape(w http.ResponseWriter, hash string, rng string, key map[string]any) bool {
	allowed := map[string]bool{hash: true}
	if rng != "" {
		allowed[rng] = true
	}
	for name := range key {
		if !allowed[name] {
			deleteItemFail(w, http.StatusBadRequest, "ValidationException", "The provided key element does not match the schema")
			return false
		}
	}
	for name := range allowed {
		if _, ok := key[name]; !ok {
			deleteItemFail(w, http.StatusBadRequest, "ValidationException", "The provided key element does not match the schema")
			return false
		}
	}
	return true
}

func deleteItemTableNotFound(w http.ResponseWriter, name string) {
	deleteItemFail(w, http.StatusBadRequest, "ResourceNotFoundException",
		"Requested resource not found: Table: "+name+" not found")
}

func deleteItemReply(w http.ResponseWriter, body map[string]any) {
	buf, err := json.Marshal(body)
	if err != nil {
		buf = []byte("{}")
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf)
}

func deleteItemFail(w http.ResponseWriter, status int, code string, msg string) {
	buf, _ := json.Marshal(map[string]any{"__type": deleteItemErrPrefix + code, "message": msg})
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(status)
	_, _ = w.Write(buf)
}

func deleteItemRecover(w http.ResponseWriter) {
	if rec := recover(); rec != nil {
		deleteItemFail(w, http.StatusInternalServerError, "InternalFailure", fmt.Sprintf("%v", rec))
	}
}

// deleteItemAcquireLock takes the store-wide mutex (when one is discoverable)
// so the deletion is consistent with the other data-plane operations.
func deleteItemAcquireLock(svc *Service) (unlock func()) {
	noop := func() {}
	root := reflect.Indirect(reflect.ValueOf(svc))
	if root.Kind() != reflect.Struct {
		return noop
	}
	mu := deleteItemFindMutex(root)
	if !mu.IsValid() {
		for i := 0; i < root.NumField(); i++ {
			inner := reflect.Indirect(root.Field(i))
			if inner.Kind() == reflect.Struct {
				if m := deleteItemFindMutex(inner); m.IsValid() {
					mu = m
					break
				}
			}
		}
	}
	if !mu.IsValid() {
		return noop
	}
	mu = deleteItemExposed(mu)
	lock, doUnlock := mu.MethodByName("Lock"), mu.MethodByName("Unlock")
	if !lock.IsValid() || !doUnlock.IsValid() || !deleteItemSafeCall(lock) {
		return noop
	}
	return func() { deleteItemSafeCall(doUnlock) }
}

func deleteItemFindMutex(v reflect.Value) reflect.Value {
	for i := 0; i < v.NumField(); i++ {
		ft := v.Field(i).Type()
		if ft.PkgPath() == "sync" && (ft.Name() == "Mutex" || ft.Name() == "RWMutex") {
			return v.Field(i)
		}
	}
	return reflect.Value{}
}

func deleteItemSafeCall(m reflect.Value) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	m.Call(nil)
	return true
}

// deleteItemTableMap returns the tables map managed by the service.
func deleteItemTableMap(svc *Service) (reflect.Value, bool) {
	root := reflect.Indirect(reflect.ValueOf(svc))
	if root.Kind() != reflect.Struct {
		return reflect.Value{}, false
	}
	best, bestScore := reflect.Value{}, -1
	var visit func(v reflect.Value, depth int)
	visit = func(v reflect.Value, depth int) {
		if depth < 0 || v.Kind() != reflect.Struct {
			return
		}
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			if m, score := deleteItemTablesScore(v.Field(i), t.Field(i).Name); score > bestScore {
				best, bestScore = m, score
			}
			if depth > 0 {
				if inner := reflect.Indirect(v.Field(i)); inner.Kind() == reflect.Struct {
					visit(inner, depth-1)
				}
			}
		}
	}
	visit(root, 2)
	if !best.IsValid() {
		return reflect.Value{}, false
	}
	return deleteItemExposed(best), true
}

func deleteItemTablesScore(field reflect.Value, name string) (reflect.Value, int) {
	m := reflect.Indirect(field)
	if m.Kind() != reflect.Map || m.Type().Key().Kind() != reflect.String {
		return reflect.Value{}, -1
	}
	et := m.Type().Elem()
	if et.Kind() == reflect.Ptr {
		et = et.Elem()
	}
	score := 0
	if strings.Contains(strings.ToLower(name), "table") {
		score += 4
	}
	switch et.Kind() {
	case reflect.Struct:
		score += 2 + min(2, deleteItemTableStructScore(et))
	case reflect.Map, reflect.Interface:
		score++
	default:
		return m, -1
	}
	return m, score
}

func deleteItemTableStructScore(st reflect.Type) int {
	score := 0
	for i := 0; i < st.NumField(); i++ {
		ft := st.Field(i).Type
		for ft.Kind() == reflect.Ptr {
			ft = ft.Elem()
		}
		name := strings.ToLower(st.Field(i).Name)
		switch ft.Kind() {
		case reflect.Map:
			if ft.Key().Kind() == reflect.String {
				if ft.Elem().Kind() == reflect.String {
					if strings.Contains(name, "key") || strings.Contains(name, "schema") {
						score++
					}
				} else {
					score++
				}
			}
		case reflect.Slice, reflect.Array:
			score++
		}
	}
	return score
}

// deleteItemKeyNames reads the base table key schema, when discoverable.
func deleteItemKeyNames(table reflect.Value) (string, string, bool) {
	for _, n := range []string{"KeySchema", "keySchema"} {
		f := reflect.Indirect(table.FieldByName(n))
		if !f.IsValid() || (f.Kind() != reflect.Slice && f.Kind() != reflect.Array) {
			continue
		}
		hash, rng := "", ""
		for i := 0; i < f.Len(); i++ {
			el := reflect.Indirect(f.Index(i))
			if el.Kind() != reflect.Struct {
				hash, rng = "", ""
				break
			}
			if deleteItemStrField(el, []string{"IndexName", "indexName"}) != "" {
				continue
			}
			name := deleteItemStrField(el, []string{"AttributeName", "attributeName", "Name", "name"})
			switch strings.ToUpper(deleteItemStrField(el, []string{"KeyType", "keyType", "Type", "type"})) {
			case "HASH":
				hash = name
			case "RANGE":
				rng = name
			}
		}
		if hash != "" {
			return hash, rng, true
		}
	}
	for _, n := range []string{"KeySchema", "keySchema", "KeyTypes", "keyTypes"} {
		f := reflect.Indirect(table.FieldByName(n))
		if !f.IsValid() || f.Kind() != reflect.Map || f.Type().Key().Kind() != reflect.String {
			continue
		}
		hash, rng := "", ""
		iter := f.MapRange()
		for iter.Next() {
			vv := iter.Value()
			if vv.Kind() != reflect.String {
				continue
			}
			attr := iter.Key().String()
			switch strings.ToUpper(vv.String()) {
			case "HASH":
				hash = attr
			case "RANGE":
				rng = attr
			}
		}
		if hash != "" {
			return hash, rng, true
		}
	}
	var hash, rng string
	for _, n := range []string{"HashKey", "hashKey", "PartitionKey", "partitionKey"} {
		f := table.FieldByName(n)
		if f.IsValid() && f.Kind() == reflect.String && f.String() != "" {
			hash = f.String()
			break
		}
	}
	for _, n := range []string{"RangeKey", "rangeKey", "SortKey", "sortKey"} {
		f := table.FieldByName(n)
		if f.IsValid() && f.Kind() == reflect.String && f.String() != "" {
			rng = f.String()
			break
		}
	}
	if hash != "" {
		return hash, rng, true
	}
	return "", "", false
}

func deleteItemStrField(el reflect.Value, names []string) string {
	for _, n := range names {
		f := reflect.Indirect(el.FieldByName(n))
		if f.IsValid() && f.Kind() == reflect.String {
			return f.String()
		}
	}
	return ""
}

// deleteItemItemsValue finds the item collection of a resolved table.
func deleteItemItemsValue(table reflect.Value) (reflect.Value, bool) {
	if table.Kind() == reflect.Map {
		return table, true
	}
	if table.Kind() != reflect.Struct {
		return reflect.Value{}, false
	}
	table = deleteItemExposed(table)
	for _, name := range []string{"Items", "items"} {
		if f := table.FieldByName(name); f.IsValid() && deleteItemIsItemCollection(f) {
			return deleteItemExposed(f), true
		}
	}
	t := table.Type()
	for i := 0; i < t.NumField(); i++ {
		if strings.Contains(strings.ToLower(t.Field(i).Name), "item") && deleteItemIsItemCollection(table.Field(i)) {
			return deleteItemExposed(table.Field(i)), true
		}
	}
	for _, name := range []string{"Rows", "rows", "Records", "records", "Entries", "entries", "Data", "data"} {
		if f := table.FieldByName(name); f.IsValid() && deleteItemIsItemCollection(f) {
			return deleteItemExposed(f), true
		}
	}
	for _, word := range []string{"item", "value", "object", "element", "row", "record", "entry"} {
		for i := 0; i < t.NumField(); i++ {
			if strings.Contains(strings.ToLower(t.Field(i).Name), word) && deleteItemIsItemCollection(table.Field(i)) {
				return deleteItemExposed(table.Field(i)), true
			}
		}
	}
	for i := 0; i < t.NumField(); i++ {
		name := strings.ToLower(t.Field(i).Name)
		if strings.Contains(name, "key") || strings.Contains(name, "index") ||
			strings.Contains(name, "attribute") || strings.Contains(name, "schema") {
			continue
		}
		if deleteItemIsItemCollection(table.Field(i)) {
			return deleteItemExposed(table.Field(i)), true
		}
	}
	return reflect.Value{}, false
}

func deleteItemIsItemCollection(field reflect.Value) bool {
	f := reflect.Indirect(field)
	var et reflect.Type
	switch f.Kind() {
	case reflect.Map:
		if f.Type().Key().Kind() != reflect.String {
			return false
		}
		et = f.Type().Elem()
	case reflect.Slice:
		et = f.Type().Elem()
	default:
		return false
	}
	if et.Kind() == reflect.Ptr {
		et = et.Elem()
	}
	switch et.Kind() {
	case reflect.Map, reflect.Interface:
		return true
	case reflect.Struct:
		return deleteItemTableStructScore(et) == 0
	}
	return false
}

// deleteItemApply removes the first item whose key attributes equal want.
func deleteItemApply(items reflect.Value, want map[string]any) (map[string]any, bool) {
	switch items.Kind() {
	case reflect.Map:
		if items.Type().Key().Kind() != reflect.String {
			return nil, false
		}
		var id reflect.Value
		var old map[string]any
		iter := items.MapRange()
		for iter.Next() {
			attrs, ok := deleteItemAttrsOf(iter.Value())
			if !ok || !deleteItemKeyMatches(attrs, want) {
				continue
			}
			id, old = iter.Key(), attrs
			break
		}
		if !id.IsValid() {
			return nil, false
		}
		if !deleteItemDropMapKey(items, id) {
			return nil, false
		}
		return old, true
	case reflect.Slice:
		for i := 0; i < items.Len(); i++ {
			attrs, ok := deleteItemAttrsOf(items.Index(i))
			if !ok || !deleteItemKeyMatches(attrs, want) {
				continue
			}
			if !items.CanSet() {
				return nil, false
			}
			next := reflect.MakeSlice(items.Type(), items.Len()-1, items.Len()-1)
			out := 0
			for j := 0; j < items.Len(); j++ {
				if j == i {
					continue
				}
				next.Index(out).Set(items.Index(j))
				out++
			}
			items.Set(next)
			return attrs, true
		}
	}
	return nil, false
}

func deleteItemDropMapKey(items reflect.Value, id reflect.Value) (dropped bool) {
	defer func() {
		if recover() != nil {
			dropped = false
		}
	}()
	items.SetMapIndex(id, reflect.Zero(items.Type().Elem()))
	return true
}

func deleteItemAttrsOf(elem reflect.Value) (out map[string]any, ok bool) {
	defer func() {
		if recover() != nil {
			out, ok = nil, false
		}
	}()
	e := reflect.Indirect(elem)
	switch e.Kind() {
	case reflect.Map:
		if e.Type().Key().Kind() != reflect.String {
			return nil, false
		}
		m := make(map[string]any, e.Len())
		iter := e.MapRange()
		for iter.Next() {
			m[iter.Key().String()] = iter.Value().Interface()
		}
		return m, true
	case reflect.Struct:
		return deleteItemStructMap(e), true
	}
	return nil, false
}

func deleteItemKeyMatches(item map[string]any, want map[string]any) bool {
	for name, expected := range want {
		stored, ok := item[name]
		if !ok || !reflect.DeepEqual(deleteItemCanon(stored), expected) {
			return false
		}
	}
	return true
}

// deleteItemScalar is the canonical comparable form of an attribute value.
type deleteItemScalar struct {
	kind byte
	str  string
	flag bool
}

// deleteItemCanon maps an attribute value (wire-shaped, native or named
// wrapper) onto its canonical comparable form.
func deleteItemCanon(v any) any {
	switch t := v.(type) {
	case nil:
		return deleteItemScalar{kind: 'z'}
	case bool:
		return deleteItemScalar{kind: 'b', flag: t}
	case string:
		return deleteItemScalar{kind: 's', str: t}
	case json.Number:
		return deleteItemScalar{kind: 'n', str: deleteItemNormNum(t.String())}
	case map[string]any:
		if out, ok := deleteItemCanonTyped(t); ok {
			return out
		}
		return deleteItemCanonMap(t)
	case []any:
		return deleteItemCanonList(t)
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr:
		if rv.IsNil() {
			return deleteItemScalar{kind: 'z'}
		}
		return deleteItemCanon(rv.Elem().Interface())
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return v
		}
		m := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			m[iter.Key().String()] = iter.Value().Interface()
		}
		if out, ok := deleteItemCanonTyped(m); ok {
			return out
		}
		return deleteItemCanonMap(m)
	case reflect.Struct:
		m := deleteItemStructMap(rv)
		if out, ok := deleteItemCanonTyped(m); ok {
			return out
		}
		return deleteItemCanonMap(m)
	case reflect.Slice, reflect.Array:
		out := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out[i] = deleteItemCanon(rv.Index(i).Interface())
		}
		return out
	default:
		if num := fmt.Sprintf("%v", v); num != "" {
			if _, err := strconv.ParseFloat(num, 64); err == nil {
				return deleteItemScalar{kind: 'n', str: deleteItemNormNum(num)}
			}
		}
		return v
	}
}

// deleteItemCanonTyped recognizes AttributeValue-like maps (wire encoding or
// struct tagged values) and yields their canonical scalar.
func deleteItemCanonTyped(m map[string]any) (any, bool) {
	key := ""
	var picked any
	count := 0
	for k, inner := range m {
		if !deleteItemAVTypes[k] {
			return nil, false
		}
		if inner != nil {
			key, picked = k, inner
			count++
		}
	}
	if count != 1 {
		return nil, false
	}
	if rv := reflect.ValueOf(picked); rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return deleteItemScalar{kind: 'z'}, true
		}
		picked = rv.Elem().Interface()
	}
	switch key {
	case "S", "B":
		if s, ok := picked.(string); ok {
			return deleteItemScalar{kind: 's', str: s}, true
		}
	case "N":
		if jn, ok := picked.(json.Number); ok {
			return deleteItemScalar{kind: 'n', str: deleteItemNormNum(jn.String())}, true
		}
		if num := fmt.Sprintf("%v", picked); num != "" {
			if _, err := strconv.ParseFloat(num, 64); err == nil {
				return deleteItemScalar{kind: 'n', str: deleteItemNormNum(num)}, true
			}
		}
	case "BOOL":
		if b, ok := picked.(bool); ok {
			return deleteItemScalar{kind: 'b', flag: b}, true
		}
	case "NULL":
		return deleteItemScalar{kind: 'z'}, true
	case "M":
		if mm, ok := picked.(map[string]any); ok {
			return deleteItemCanonMap(mm), true
		}
	case "L", "SS", "NS", "BS":
		if ll, ok := picked.([]any); ok {
			return deleteItemCanonList(ll), true
		}
	}
	return nil, false
}

func deleteItemCanonMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, vv := range m {
		out[k] = deleteItemCanon(vv)
	}
	return out
}

func deleteItemCanonList(l []any) []any {
	out := make([]any, len(l))
	for i := range l {
		out[i] = deleteItemCanon(l[i])
	}
	return out
}

func deleteItemStructMap(rv reflect.Value) map[string]any {
	t := rv.Type()
	out := make(map[string]any, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.PkgPath != "" {
			continue
		}
		name := sf.Name
		if tag, tagged := sf.Tag.Lookup("json"); tagged {
			if parts := strings.Split(tag, ","); parts[0] != "" {
				name = parts[0]
			}
		}
		out[name] = rv.Field(i).Interface()
	}
	return out
}

// deleteItemNormNum normalizes numeric text so equal DynamoDB numbers compare
// equal regardless of formatting.
func deleteItemNormNum(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return s
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return strconv.FormatFloat(f, 'g', -1, 64)
	}
	t := strings.TrimPrefix(s, "+")
	neg := strings.HasPrefix(t, "-")
	t = strings.TrimPrefix(t, "-")
	if !strings.ContainsAny(t, "eE") && strings.Contains(t, ".") {
		t = strings.TrimRight(t, "0")
		t = strings.TrimSuffix(t, ".")
	}
	t = strings.TrimLeft(t, "0")
	if t == "" {
		t = "0"
	}
	if neg && t != "0" {
		return "-" + t
	}
	return t
}

// deleteItemToWire converts natively stored attributes to the wire-shaped
// AttributeValue encoding; wire-shaped and struct-typed values pass through.
func deleteItemToWire(old map[string]any) map[string]any {
	for _, v := range old {
		if !deleteItemIsWireAV(v) {
			out := make(map[string]any, len(old))
			for k, vv := range old {
				out[k] = deleteItemToAV(vv)
			}
			return out
		}
	}
	return old
}

func deleteItemIsWireAV(v any) bool {
	if m, ok := v.(map[string]any); ok {
		if len(m) != 1 {
			return false
		}
		for k := range m {
			return deleteItemAVTypes[k]
		}
		return false
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String || rv.Len() != 1 {
			return false
		}
		iter := rv.MapRange()
		iter.Next()
		return deleteItemAVTypes[iter.Key().String()]
	case reflect.Struct:
		t := rv.Type()
		fields, nonNil := 0, 0
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.PkgPath != "" {
				continue
			}
			tag, tagged := f.Tag.Lookup("json")
			if !tagged {
				return false
			}
			if !deleteItemAVTypes[strings.Split(tag, ",")[0]] {
				return false
			}
			fields++
			if !rv.Field(i).IsZero() {
				nonNil++
			}
		}
		return fields > 0 && nonNil == 1
	}
	return false
}

func deleteItemToAV(v any) any {
	if deleteItemIsWireAV(v) {
		return v
	}
	switch t := v.(type) {
	case nil:
		return map[string]any{"NULL": true}
	case bool:
		return map[string]any{"BOOL": t}
	case string:
		return map[string]any{"S": t}
	case []byte:
		return map[string]any{"B": base64.StdEncoding.EncodeToString(t)}
	case float64:
		return map[string]any{"N": strconv.FormatFloat(t, 'g', -1, 64)}
	case float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return map[string]any{"N": deleteItemNormNum(fmt.Sprintf("%v", t))}
	case []any:
		out := make([]any, len(t))
		for i := range t {
			out[i] = deleteItemToAV(t[i])
		}
		return map[string]any{"L": out}
	case []string:
		return map[string]any{"SS": t}
	case map[string]any:
		m := make(map[string]any, len(t))
		for k, vv := range t {
			m[k] = deleteItemToAV(vv)
		}
		return map[string]any{"M": m}
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Map:
		if rv.Type().Key().Kind() == reflect.String {
			m := make(map[string]any, rv.Len())
			iter := rv.MapRange()
			for iter.Next() {
				m[iter.Key().String()] = deleteItemToAV(iter.Value().Interface())
			}
			return map[string]any{"M": m}
		}
	case reflect.Slice:
		out := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out[i] = deleteItemToAV(rv.Index(i).Interface())
		}
		return map[string]any{"L": out}
	}
	return map[string]any{"S": fmt.Sprintf("%v", v)}
}

// deleteItemExposed returns a value referencing the same memory as v without
// reflect's read-only flag, so fields reached through unexported struct fields
// can be mutated. When v is not addressable it is returned unchanged.
func deleteItemExposed(v reflect.Value) reflect.Value {
	if !v.CanAddr() {
		return v
	}
	return reflect.NewAt(v.Type(), unsafe.Pointer(v.UnsafeAddr())).Elem()
}
