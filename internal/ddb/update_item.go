package ddb

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// uieTarget is the DynamoDB API 2012-08-10 JSON 1.0 target prefix.
const uieTarget = "DynamoDB_20120810."

// ----------------------------------------------------------- entry points --
// Several conventional handler shapes are provided so the op can be reached
// by whichever dispatch mechanism lives inside this package.

// handleUpdateItem serves DynamoDB_20120810.UpdateItem (dispatcher method style).
func (s *Service) handleUpdateItem(w http.ResponseWriter, r *http.Request) {
	uieServe(s, w, r)
}

// HandleUpdateItem is an exported alias of handleUpdateItem.
func (s *Service) HandleUpdateItem(w http.ResponseWriter, r *http.Request) {
	uieServe(s, w, r)
}

// UpdateItem is an exported handler alias (reflection based dispatch).
func (s *Service) UpdateItem(w http.ResponseWriter, r *http.Request) {
	uieServe(s, w, r)
}

const uxAVUnused = 0

// updateItem is a lowercase alias of handleUpdateItem.
func (s *Service) updateItem(w http.ResponseWriter, r *http.Request) {
	uieServe(s, w, r)
}

// handleUpdateItem serves UpdateItem for function style dispatchers.
func handleUpdateItem(s *Service, w http.ResponseWriter, r *http.Request) {
	uieServe(s, w, r)
}

// -------------------------------------------------------------- operation --

// uieServe implements the UpdateItem operation: it fetches the current item
// through the service's own GetItem op, evaluates the optional
// ConditionExpression against it, applies the UpdateExpression on a decoded
// copy of the item and writes the result back through the service's own
// PutItem op so the shared store stays consistent with every other op.
func uieServe(svc *Service, w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		uieWriteErr(w, &uxErr{Code: "SerializationException", Msg: "unable to read request body"})
		return
	}
	var req map[string]json.RawMessage
	if e := json.Unmarshal(body, &req); e != nil {
		uieWriteErr(w, &uxErr{Code: "SerializationException", Msg: "invalid request body"})
		return
	}
	field := func(name string) (json.RawMessage, bool) {
		if v, ok := req[name]; ok {
			return v, true
		}
		for k, v := range req {
			if strings.EqualFold(k, name) {
				return v, true
			}
		}
		return nil, false
	}

	var table string
	if v, ok := field("TableName"); ok {
		_ = json.Unmarshal(v, &table)
	}
	if table == "" {
		uieWriteErr(w, uxValid("TableName is a required parameter"))
		return
	}
	var key map[string]uxAV
	if v, ok := field("Key"); ok {
		_ = json.Unmarshal(v, &key)
	}
	if len(key) == 0 {
		uieWriteErr(w, uxValid("Key is a required parameter"))
		return
	}
	names := map[string]string{}
	if v, ok := field("ExpressionAttributeNames"); ok {
		_ = json.Unmarshal(v, &names)
	}
	values := map[string]uxAV{}
	if v, ok := field("ExpressionAttributeValues"); ok {
		if e := json.Unmarshal(v, &values); e != nil {
			uieWriteErr(w, uxValid("ExpressionAttributeValues contains an invalid value"))
			return
		}
	}
	var updExpr, condExpr, retVal string
	if v, ok := field("UpdateExpression"); ok {
		_ = json.Unmarshal(v, &updExpr)
	}
	if v, ok := field("ConditionExpression"); ok {
		_ = json.Unmarshal(v, &condExpr)
	}
	if v, ok := field("ReturnValues"); ok {
		_ = json.Unmarshal(v, &retVal)
	}
	switch retVal {
	case "", "NONE", "ALL_OLD", "ALL_NEW", "UPDATED_OLD", "UPDATED_NEW":
	default:
		uieWriteErr(w, uxValid("Unknown ReturnValues: %s", retVal))
		return
	}

	if strings.TrimSpace(updExpr) == "" {
		updExpr = ""
	}

	// Fetch the current item through the existing GetItem op.
	gb, mErr := json.Marshal(map[string]any{"TableName": table, "Key": key})
	if mErr != nil {
		uieWriteErr(w, &uxErr{Code: "InternalFailure", Msg: mErr.Error()})
		return
	}
	status, raw, fErr := uieForward(svc, "GetItem", gb)
	if fErr != nil {
		uieWriteErr(w, fErr)
		return
	}
	if status >= 400 {
		uieWriteErr(w, uieRemoteErr(status, raw))
		return
	}
	var gresp struct {
		Item map[string]uxAV `json:"Item"`
	}
	_ = json.Unmarshal(raw, &gresp)
	old := gresp.Item
	exists := old != nil

	var acts []uxAction
	var cond *uxCond
	usedN := map[string]bool{}
	usedV := map[string]bool{}
	if updExpr != "" {
		a, un, uv, e3 := uxParseUpdate(updExpr, names)
		if e3 != nil {
			uieWriteErr(w, e3)
			return
		}
		acts = a
		for k := range un {
			usedN[k] = true
		}
		for k := range uv {
			usedV[k] = true
		}
	}
	if condExpr != "" {
		c, un, uv, e3 := uxParseCondition(condExpr, names)
		if e3 != nil {
			uieWriteErr(w, e3)
			return
		}
		cond = c
		for k := range un {
			usedN[k] = true
		}
		for k := range uv {
			usedV[k] = true
		}
	}
	if updExpr != "" || condExpr != "" {
		var unused []string
		for k := range names {
			if !usedN[k] {
				unused = append(unused, k)
			}
		}
		if len(unused) > 0 {
			sort.Strings(unused)
			uieWriteErr(w, uxValid("Value provided in ExpressionAttributeNames unused in expressions: keys: {%s}", strings.Join(unused, ", ")))
			return
		}
		unused = unused[:0]
		for k := range values {
			if !usedV[k] {
				unused = append(unused, k)
			}
		}
		if len(unused) > 0 {
			sort.Strings(unused)
			uieWriteErr(w, uxValid("Value provided in ExpressionAttributeValues unused in expressions: keys: {%s}", strings.Join(unused, ", ")))
			return
		}
		for k := range usedV {
			if _, ok := values[k]; !ok {
				uieWriteErr(w, uxValid("An expression attribute value used in the expression is not defined"))
				return
			}
		}
	}

	if len(acts) > 0 {
		for i := 0; i < len(acts); i++ {
			for j := i + 1; j < len(acts); j++ {
				if uxOverlaps(acts[i].path, acts[j].path) {
					uieWriteErr(w, uxValid("Two document paths overlap with each other"))
					return
				}
			}
		}
		for _, act := range acts {
			top := act.path.comps[0].attr
			if _, isKey := key[top]; isKey {
				uieWriteErr(w, uxValid("One or more parameter values were invalid: Cannot update attribute %s. This attribute is part of the key", top))
				return
			}
		}
	}

	if cond != nil {
		okc, e4 := uxEvalCond(cond, old, values)
		if e4 != nil {
			uieWriteErr(w, e4)
			return
		}
		if !okc {
			uieWriteErr(w, &uxErr{Code: "ConditionalCheckFailedException", Msg: "The conditional request failed"})
			return
		}
	}

	var item map[string]uxAV
	var updOld, updNew map[string]uxAV
	if len(acts) > 0 {
		base := old
		if !exists {
			base = map[string]uxAV{}
			for k, v := range key {
				base[k] = uxCloneVal(v)
			}
		}
		ap := &uxApply{root: base, values: values, oldOut: map[string]uxAV{}, newOut: map[string]uxAV{}}
		for _, act := range acts {
			if e5 := ap.run(act); e5 != nil {
				uieWriteErr(w, e5)
				return
			}
		}
		item = base
		updOld, updNew = ap.oldOut, ap.newOut

		// Persist through the existing PutItem op so the shared table state,
		// key handling and validation remain identical to the other ops.
		pb, pErr := json.Marshal(map[string]any{"TableName": table, "Item": item})
		if pErr != nil {
			uieWriteErr(w, &uxErr{Code: "InternalFailure", Msg: pErr.Error()})
			return
		}
		status, raw, fErr := uieForward(svc, "PutItem", pb)
		if fErr != nil {
			uieWriteErr(w, fErr)
			return
		}
		if status >= 400 {
			uieWriteErr(w, uieRemoteErr(status, raw))
			return
		}
	} else {
		item = old
	}

	var attrs map[string]uxAV
	switch retVal {
	case "ALL_OLD":
		if exists {
			attrs = old
		}
	case "ALL_NEW":
		if item != nil {
			attrs = item
		}
	case "UPDATED_OLD":
		if len(updOld) > 0 {
			attrs = updOld
		}
	case "UPDATED_NEW":
		if len(updNew) > 0 {
			attrs = updNew
		}
	}
	resp := map[string]any{}
	if len(attrs) > 0 {
		resp["Attributes"] = attrs
	}
	uieWriteJSON(w, resp)
}

// ------------------------------------------------------- internal plumbing

// uieRouter locates the HTTP entry point of the service without hard wiring a
// particular method name (ServeHTTP, Handler() http.Handler, or any no-arg
// method returning http.Handler).
func uieRouter(svc *Service) http.Handler {
	if h, ok := any(svc).(http.Handler); ok {
		return h
	}
	if h, ok := any(svc).(interface{ Handler() http.Handler }); ok {
		if r := h.Handler(); r != nil {
			return r
		}
	}
	if svc == nil {
		return nil
	}
	v := reflect.ValueOf(svc)
	if !v.IsValid() || (v.Kind() != reflect.Ptr && v.Kind() != reflect.Struct) {
		return nil
	}
	handlerType := reflect.TypeOf((*http.Handler)(nil)).Elem()
	for i := 0; i < v.Type().NumMethod(); i++ {
		mt := v.Type().Method(i).Type
		if mt.NumIn() != 1 || mt.NumOut() != 1 || mt.Out(0) != handlerType {
			continue
		}
		var out []reflect.Value
		func() {
			defer func() { _ = recover() }()
			out = v.Method(i).Call(nil)
		}()
		if len(out) == 1 && !out[0].IsNil() {
			if h, ok := out[0].Interface().(http.Handler); ok {
				return h
			}
		}
	}
	return nil
}

// uieForward runs one internal DynamoDB op against the service.
func uieForward(svc *Service, target string, payload []byte) (int, []byte, *uxErr) {
	h := uieRouter(svc)
	if h == nil {
		return 0, nil, &uxErr{Code: "InternalFailure", Msg: "dynamodb router unavailable"}
	}
	req, err := http.NewRequest(http.MethodPost, "/", bytes.NewReader(payload))
	if err != nil {
		return 0, nil, &uxErr{Code: "InternalFailure", Msg: "unable to build request"}
	}
	req.Header.Set("X-Amz-Target", uieTarget+target)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	rec := &uieRecorder{hdr: http.Header{}}
	func() {
		defer func() {
			if rv := recover(); rv != nil {
				if rec.code == 0 {
					rec.code = http.StatusInternalServerError
				}
			}
		}()
		h.ServeHTTP(rec, req)
	}()
	if rec.code == 0 {
		rec.code = http.StatusOK
	}
	return rec.code, rec.body.Bytes(), nil
}

type uieRecorder struct {
	code int
	body bytes.Buffer
	hdr  http.Header
}

func (x *uieRecorder) Header() http.Header { return x.hdr }
func (x *uieRecorder) Write(p []byte) (int, error) {
	return x.body.Write(p)
}
func (x *uieRecorder) WriteHeader(status int) { x.code = status }

func uieRemoteErr(status int, body []byte) *uxErr {
	var m map[string]any
	_ = json.Unmarshal(body, &m)
	typ := ""
	msg := ""
	if m != nil {
		if s, ok := m["__type"].(string); ok {
			typ = s
		}
		if s, ok := m["message"].(string); ok {
			msg = s
		} else if s, ok := m["Message"].(string); ok {
			msg = s
		}
	}
	if i := strings.LastIndex(typ, "#"); i >= 0 {
		typ = typ[i+1:]
	}
	if typ == "" {
		typ = "InternalFailure"
	}
	if msg == "" {
		msg = statusLine(status)
	}
	return &uxErr{Code: typ, Msg: msg}
}

func statusLine(status int) string {
	if status == 0 {
		return "internal error"
	}
	return "request failed with status " + strconv.Itoa(status)
}

func uieWriteJSON(w http.ResponseWriter, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		b = []byte("{}")
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)
}

func uieWriteErr(w http.ResponseWriter, e *uxErr) {
	status := http.StatusBadRequest
	switch e.Code {
	case "InternalFailure", "InternalServerError", "ServiceUnavailable":
		status = http.StatusInternalServerError
	}
	resp := map[string]any{
		"__type": "com.amazonaws.dynamodb.v20120810#" + e.Code,
	}
	if e.Msg != "" {
		resp["message"] = e.Msg
	}
	b, err := json.Marshal(resp)
	if err != nil {
		b = []byte("{\"__type\":\"com.amazonaws.dynamodb.v20120810#InternalFailure\"}")
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(status)
	_, _ = w.Write(b)
}