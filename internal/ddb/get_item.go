package ddb

// get_item.go â DynamoDB_20120810.GetItem for the in-memory DynamoDB store.
//
// Wire behavior for the DynamoDB JSON API action DynamoDB_20120810.GetItem:
//
//	200  {"Item": {...}}            item found (optionally projected, plus
//	                                ConsumedCapacity when requested)
//	200  {}                         key does not match any stored item
//	400  ValidationException        bad request shape or a key that does not
//	                                fit the table's pk[/sk] key schema
//	400  ResourceNotFoundException  unknown table
//	400  SerializationException     body is not parsable JSON
//
// ConsistentRead is honored: the in-memory store always serves the latest
// committed state (all reads are strongly consistent), and a consistent read
// doubles the reported read capacity units.

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"reflect"
	"strconv"
	"strings"
)

// getItemRequest is the wire shape of a GetItem request.
type getItemRequest struct {
	TableName                *string                    `json:"TableName"`
	Key                      *map[string]map[string]any `json:"Key"`
	ConsistentRead           bool                       `json:"ConsistentRead"`
	ProjectionExpression     string                     `json:"ProjectionExpression"`
	ExpressionAttributeNames map[string]string          `json:"ExpressionAttributeNames"`
	ReturnConsumedCapacity   string                     `json:"ReturnConsumedCapacity"`
}

// getItemResponse is the wire shape of a GetItem result.
type getItemResponse struct {
	Item             *map[string]any  `json:"Item,omitempty"`
	ConsumedCapacity *getItemCapacity `json:"ConsumedCapacity,omitempty"`
}

// getItemCapacity is the ConsumedCapacity entry reported for a GetItem.
type getItemCapacity struct {
	TableName     string  `json:"TableName"`
	CapacityUnits float64 `json:"CapacityUnits"`
}

// GetItem is the exported entry point used by the action dispatcher.
func (s *Service) GetItem(w http.ResponseWriter, r *http.Request) {
	s.getItem(w, r)
}

// getItem implements DynamoDB_20120810.GetItem: read an item by the table's
// partition key (plus its sort key when the table defines one) and echo the
// stored attributes. A miss yields an empty response body.
func (s *Service) getItem(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeGetItemError(w, http.StatusBadRequest, "SerializationException",
			"Unexpected error while reading the request body")
		return
	}

	var req getItemRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeGetItemError(w, http.StatusBadRequest, "SerializationException",
			"Unexpected error deserializing the request body")
		return
	}

	if req.TableName == nil || *req.TableName == "" {
		writeGetItemError(w, http.StatusBadRequest, "ValidationException",
			"1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null")
		return
	}
	tableName := *req.TableName
	if len(tableName) > 255 {
		writeGetItemError(w, http.StatusBadRequest, "ValidationException",
			"1 validation error detected: Value '"+tableName+"' at 'tableName' failed to satisfy constraint: Member must have length less than or equal to 255")
		return
	}

	if req.Key == nil {
		writeGetItemError(w, http.StatusBadRequest, "ValidationException",
			"1 validation error detected: Value null at 'key' failed to satisfy constraint: Member must not be null")
		return
	}
	if len(*req.Key) == 0 {
		writeGetItemError(w, http.StatusBadRequest, "ValidationException",
			"1 validation error detected: Value '{}' at 'key' failed to satisfy constraint: Member must have length greater than or equal to 1")
		return
	}

	rcc := req.ReturnConsumedCapacity
	if rcc == "" {
		rcc = "NONE"
	}
	switch rcc {
	case "NONE", "TOTAL", "INDEXES":
	default:
		writeGetItemError(w, http.StatusBadRequest, "ValidationException",
			"1 validation error detected: Value '"+rcc+"' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]")
		return
	}

	// Hold the store's read lock while the snapshot is located and encoded;
	// with ConsistentRead this is always the latest committed state.
	s.store.mu.RLock()
	table, ok := s.store.Tables[tableName]
	if !ok || table == nil {
		s.store.mu.RUnlock()
		writeGetItemError(w, http.StatusBadRequest, "ResourceNotFoundException",
			"Requested resource not found: Table: "+tableName+" not found")
		return
	}

	item, schemaOK := getItemFind(table.Items, *req.Key, table.PK, table.SK)
	if !schemaOK {
		s.store.mu.RUnlock()
		writeGetItemError(w, http.StatusBadRequest, "ValidationException",
			"The provided key element does not match the schema")
		return
	}

	resp := getItemResponse{}
	if item != nil {
		if pe := strings.TrimSpace(req.ProjectionExpression); pe != "" {
			projected, emsg := getItemProject(item, pe, req.ExpressionAttributeNames)
			if emsg != "" {
				s.store.mu.RUnlock()
				writeGetItemError(w, http.StatusBadRequest, "ValidationException", emsg)
				return
			}
			item = projected
		}
		resp.Item = &item
	}
	if rcc == "TOTAL" || rcc == "INDEXES" {
		units := 0.5 // eventual read of an item up to 4 KB
		if req.ConsistentRead {
			units = 1.0
		}
		resp.ConsumedCapacity = &getItemCapacity{TableName: tableName, CapacityUnits: units}
	}

	out := &bytes.Buffer{}
	enc := json.NewEncoder(out)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(resp)
	s.store.mu.RUnlock()

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	_, _ = w.Write(bytes.TrimRight(out.Bytes(), "\n"))
}

// getItemFind validates the request key against the table's key schema (the
// partition key, plus the sort key when the table defines one â by attribute
// name and count) and scans the table's items for the one identified by the
// key. The second return value is false when the key does not match the
// schema; (nil, true) means the key is valid but no item matches.
func getItemFind(items map[string]map[string]any, key map[string]map[string]any, pk, sk string) (map[string]any, bool) {
	pkVal, hasPK := key[pk]
	skVal, hasSK := key[sk]
	switch {
	case !hasPK, sk != "" && !hasSK, sk == "" && len(key) != 1, sk != "" && len(key) != 2:
		return nil, false
	}
	if _, _, ok := getItemTypedParts(pkVal); !ok {
		return nil, false
	}
	if sk != "" {
		if _, _, ok := getItemTypedParts(skVal); !ok {
			return nil, false
		}
	}
	for _, stored := range items {
		if sp, ok := stored[pk]; !ok || !getItemKeyEqual(pkVal, sp) {
			continue
		}
		if sk != "" {
			if ss, ok := stored[sk]; !ok || !getItemKeyEqual(skVal, ss) {
				continue
			}
		}
		return stored, true
	}
	return nil, true
}

// getItemTypedParts splits a DynamoDB typed value {"S": "x"} into its tag
// and raw value. It fails for anything that is not a single-tag map.
func getItemTypedParts(v map[string]any) (tag string, raw any, ok bool) {
	if len(v) != 1 {
		return "", nil, false
	}
	for tag, raw = range v {
		return tag, raw, true
	}
	return "", nil, false
}

// getItemKeyEqual compares a request key attribute value against the stored
// one. Numeric keys compare by numeric value ("3" matches "3.0") and binary
// keys compare by decoded bytes; other types compare by value.
func getItemKeyEqual(req, stored map[string]any) bool {
	rTag, rVal, ok := getItemTypedParts(req)
	if !ok {
		return false
	}
	sTag, sVal, ok := getItemTypedParts(stored)
	if !ok || rTag != sTag {
		return false
	}
	switch rTag {
	case "N":
		rn, rOK := getItemNumber(rVal)
		sn, sOK := getItemNumber(sVal)
		return rOK && sOK && rn.Cmp(sn) == 0
	case "B":
		rb, rOK := getItemBytes(rVal)
		sb, sOK := getItemBytes(sVal)
		return rOK && sOK && bytes.Equal(rb, sb)
	}
	return reflect.DeepEqual(rVal, sVal)
}

// getItemNumber parses a DynamoDB decimal, accepting strings ("3.14"),
// json.Number and float64 values.
func getItemNumber(v any) (*big.Float, bool) {
	switch n := v.(type) {
	case string:
		f, _, err := big.ParseFloat(strings.TrimSpace(n), 10, 240, big.ToNearestEven)
		return f, err == nil && f != nil
	case json.Number:
		f, _, err := big.ParseFloat(string(n), 10, 240, big.ToNearestEven)
		return f, err == nil && f != nil
	case float64:
		return big.NewFloat(n), true
	}
	return nil, false
}

// getItemBytes decodes a DynamoDB binary value (base64 text on the wire).
func getItemBytes(v any) ([]byte, bool) {
	switch b := v.(type) {
	case string:
		raw, err := base64.StdEncoding.DecodeString(b)
		if err != nil {
			return nil, false
		}
		return raw, true
	case []byte:
		return b, true
	}
	return nil, false
}

// getItemSeg is one segment of a document path: an attribute name plus an
// optional single list index.
type getItemSeg struct {
	name string
	idx  []int // nil, or [i]
}

// getItemProject narrows item to the attributes named by the projection
// expression. Paths that resolve to nothing are omitted, exactly as
// DynamoDB does. It returns the projected document or a ValidationException
// message.
func getItemProject(item map[string]any, expr string, names map[string]string) (map[string]any, string) {
	out := make(map[string]any)
	for _, raw := range strings.Split(expr, ",") {
		tok := strings.TrimSpace(raw)
		if tok == "" {
			return nil, "Syntax error in projection expression"
		}
		segs, emsg := getItemParsePath(tok, names)
		if emsg != "" {
			return nil, emsg
		}
		val, ok := getItemWalkPath(item, segs)
		if !ok {
			continue // attributes a projection does not match are omitted
		}
		if len(segs) == 1 && len(segs[0].idx) == 0 {
			out[segs[0].name] = val
		} else {
			getItemNest(out, segs, val)
		}
	}
	return out, ""
}

// getItemParsePath parses one projection-expression path into segments,
// resolving "#placeholder" names via the expression attribute names.
func getItemParsePath(tok string, names map[string]string) ([]getItemSeg, string) {
	segs := make([]getItemSeg, 0, 4)
	for _, part := range strings.Split(tok, ".") {
		if part == "" {
			return nil, "Syntax error; token: " + tok
		}
		name, idx := part, -1
		if i := strings.IndexByte(part, '['); i >= 0 {
			if !strings.HasSuffix(part, "]") {
				return nil, "Syntax error; token: " + tok
			}
			name = part[:i]
			inner := strings.TrimSuffix(part[i+1:], "]")
			if strings.ContainsAny(inner, "[]") {
				return nil, "Syntax error; token: " + tok
			}
			n, err := strconv.Atoi(strings.TrimSpace(inner))
			if err != nil || n < 0 {
				return nil, "Syntax error; token: " + tok
			}
			idx = n
		}
		if name == "" {
			return nil, "Syntax error; token: " + tok
		}
		var resolved string
		if strings.HasPrefix(name, "#") {
			r, ok := names[name]
			if !ok {
				return nil, "Undefined expression attribute name: " + name + " in projection"
			}
			resolved = r
		} else {
			if !getItemIsIdent(name) {
				return nil, "Syntax error; token: " + tok
			}
			resolved = name
		}
		seg := getItemSeg{name: resolved}
		if idx >= 0 {
			seg.idx = []int{idx}
		}
		segs = append(segs, seg)
	}
	return segs, ""
}

// getItemIsIdent reports whether s is a bare attribute identifier allowed
// directly in a projection expression.
func getItemIsIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case r >= '0' && r <= '9':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// getItemWalkPath resolves a document path against a stored item and returns
// the typed value at the end of the path.
func getItemWalkPath(item map[string]any, segs []getItemSeg) (map[string]any, bool) {
	first := segs[0]
	cur, ok := getItemTyped(item[first.name])
	if !ok {
		return nil, false
	}
	if first.idx != nil {
		cur, ok = getItemListElem(cur, first.idx[0])
		if !ok {
			return nil, false
		}
	}
	for _, seg := range segs[1:] {
		container, ok := cur["M"].(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = getItemTyped(container[seg.name])
		if !ok {
			return nil, false
		}
		if seg.idx != nil {
			cur, ok = getItemListElem(cur, seg.idx[0])
			if !ok {
				return nil, false
			}
		}
	}
	return cur, true
}

// getItemTyped checks that raw is a well-formed DynamoDB typed value map.
func getItemTyped(raw any) (map[string]any, bool) {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, false
	}
	if _, _, ok := getItemTypedParts(m); !ok {
		return nil, false
	}
	return m, true
}

// getItemListElem returns element i of a {"L": [...]} typed value as a
// typed value map.
func getItemListElem(typed map[string]any, i int) (map[string]any, bool) {
	list, ok := typed["L"].([]any)
	if !ok || i >= len(list) {
		return nil, false
	}
	return getItemTyped(list[i])
}

// getItemNest places a projected value into out following the document
// path, creating nested maps and lists as required (later paths win on
// structural conflicts, matching DynamoDB's last-write behavior).
func getItemNest(out map[string]any, segs []getItemSeg, val map[string]any) {
	cur := out
	for i := 0; i < len(segs); i++ {
		seg := segs[i]
		last := i == len(segs)-1
		if seg.idx == nil {
			if last {
				cur[seg.name] = val
				return
			}
			next, ok := cur[seg.name].(map[string]any)
			if !ok {
				next = make(map[string]any)
				cur[seg.name] = next
			}
			cur = next
			continue
		}
		list, ok := cur[seg.name].([]any)
		if !ok {
			list = make([]any, 0, seg.idx[0]+1)
		}
		for len(list) <= seg.idx[0] {
			list = append(list, nil)
		}
		cur[seg.name] = list
		if last {
			list[seg.idx[0]] = val
			return
		}
		next, ok := list[seg.idx[0]].(map[string]any)
		if !ok {
			next = make(map[string]any)
			list[seg.idx[0]] = next
		}
		cur = next
	}
}

// writeGetItemError emits the AWS JSON error envelope used by DynamoDB.
func writeGetItemError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.Header().Set("x-amzn-ErrorType", code)
	w.WriteHeader(status)
	body := struct {
		Type    string `json:"__type"`
		Message string `json:"Message"`
	}{Type: "com.amazon.coral.service#" + code, Message: message}
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(body)
	_, _ = w.Write(bytes.TrimRight(buf.Bytes(), "\n"))
}
