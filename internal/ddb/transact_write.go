// TransactWriteItems: atomic all-or-nothing multi-item writes (1..25 operations)
// with conflict detection (duplicate item targets, condition-expression
// evaluation against a simulated transaction snapshot) wired on top of the
// existing per-operation handlers so wire behavior stays identical to them.
package ddb

import (
  "bytes"
  "crypto/rand"
  "encoding/base64"
  "encoding/hex"
  "encoding/json"
  "errors"
  "io"
  "math"
  "net/http"
  "reflect"
  "sort"
  "strconv"
  "strings"
  "sync"
  "unsafe"
)

// txnMu serializes transaction processing.
var txnMu sync.Mutex

const (
  txnMaxItems    = 25
  txnTypePrefix  = "com.amazonaws.dynamodb.v20120810#"
  txnCancelMsg   = "Transaction cancelled, please request cancellation for this transaction"
  txnCondFailMsg = "The conditional request failed"
)

const (
  txnKindPut = iota
  txnKindUpdate
  txnKindDelete
  txnKindCondCheck
)

type txnAction struct {
  Index      int
  Kind       int
  Table      string
  Op         map[string]any
  Key        map[string]any
  KeyOK      bool
  Item       map[string]any
  UpdateExpr string
  Cond       string
  Names      map[string]string
  Values     map[string]any
  RVOld      bool
}

func (a *txnAction) opName() string {
  switch a.Kind {
  case txnKindPut:
    return "PutItem"
  case txnKindUpdate:
    return "UpdateItem"
  case txnKindDelete:
    return "DeleteItem"
  }
  return ""
}

func txnStrPtr(s string) *string { return &s }

type txnCancelReason struct {
  Code    string         `json:"Code"`
  Message *string        `json:"Message"`
  Item    map[string]any `json:"Item,omitempty"`
}

type txnErrorBody struct {
  Type    string            `json:"__type"`
  Message string            `json:"message"`
  Reasons []txnCancelReason `json:"CancellationReasons,omitempty"`
}

func txnRequestID() string {
  b := make([]byte, 16)
  _, _ = rand.Read(b)
  return hex.EncodeToString(b)
}

func txnWriteJSON(w http.ResponseWriter, status int, v any) {
  body, _ := json.Marshal(v)
  w.Header().Set("Content-Type", "application/x-amz-json-1.0")
  w.Header().Set("x-amzn-RequestId", txnRequestID())
  w.WriteHeader(status)
  _, _ = w.Write(body)
}

func txnWriteError(w http.ResponseWriter, status int, typ, msg string) {
  txnWriteJSON(w, status, txnErrorBody{Type: typ, Message: msg})
}

func txnWriteValidation(w http.ResponseWriter, msg string) {
  txnWriteError(w, http.StatusBadRequest, txnTypePrefix+"ValidationException", msg)
}

func txnWriteCanceled(w http.ResponseWriter, reasons []txnCancelReason) {
  txnWriteJSON(w, http.StatusBadRequest, txnErrorBody{
    Type:    txnTypePrefix + "TransactionCanceledException",
    Message: txnCancelMsg,
    Reasons: reasons,
  })
}

func txnWriteSuccess(w http.ResponseWriter) {
  w.Header().Set("Content-Type", "application/x-amz-json-1.0")
  w.Header().Set("x-amzn-RequestId", txnRequestID())
  w.WriteHeader(http.StatusOK)
  _, _ = w.Write([]byte("{}"))
}

func txnWriteRaw(w http.ResponseWriter, status int, body []byte) {
  w.Header().Set("Content-Type", "application/x-amz-json-1.0")
  w.Header().Set("x-amzn-RequestId", txnRequestID())
  w.WriteHeader(status)
  _, _ = w.Write(body)
}

// TransactWriteItems is the exported alias for the TransactWriteItems action.
func (s *Service) TransactWriteItems(w http.ResponseWriter, r *http.Request) {
  s.handleTransactWriteItems(w, r)
}

// handleTransactWriteItems implements the DynamoDB_20120810.TransactWriteItems action.
func (s *Service) handleTransactWriteItems(w http.ResponseWriter, r *http.Request) {
  var raw []byte
  if r != nil && r.Body != nil {
    raw, _ = io.ReadAll(r.Body)
  }
  if len(raw) == 0 {
    txnWriteValidation(w, "Missing required member: TransactItems")
    return
  }
  var req struct {
    TransactItems      []json.RawMessage `json:"TransactItems"`
    ClientRequestToken string            `json:"ClientRequestToken"`
  }
  if err := json.Unmarshal(raw, &req); err != nil {
    txnWriteValidation(w, "Invalid request body")
    return
  }
  n := len(req.TransactItems)
  if n == 0 {
    txnWriteValidation(w, "TransactItems must contain at least one operation")
    return
  }
  if n > txnMaxItems {
    txnWriteValidation(w, "Transaction request cannot include more than 25 items")
    return
  }

  acts := make([]*txnAction, 0, n)
  for i, rawItem := range req.TransactItems {
    var elem map[string]any
    if err := json.Unmarshal(rawItem, &elem); err != nil {
      txnWriteValidation(w, "TransactItems must contain valid item operations")
      return
    }
    a, msg := txnParseAction(elem)
    if msg != "" {
      txnWriteValidation(w, msg)
      return
    }
    a.Index = i
    acts = append(acts, a)
  }
  s.txnResolvePutKeys(acts)

  txnMu.Lock()
  defer txnMu.Unlock()

  // Conflict detection: at most one operation per item.
  seen := make(map[string]struct{}, len(acts))
  for _, a := range acts {
    if !a.KeyOK {
      continue
    }
    ck := txnCanonicalKey(a.Table, a.Key)
    if _, dup := seen[ck]; dup {
      txnWriteValidation(w, "Transaction request cannot include multiple operations on one item")
      return
    }
    seen[ck] = struct{}{}
  }

  reasons := make([]txnCancelReason, len(acts))
  for i := range reasons {
    reasons[i] = txnCancelReason{Code: "None"}
  }

  // Snapshot + simulate the transaction in order, then evaluate every
  // condition against the projected final state. Nothing is written unless
  // every condition passes (atomicity).
  snaps := make(map[string]*txnSnap, len(acts))
  cancel := false
  for _, a := range acts {
    sn, st, resp := s.txnSnapFor(snaps, a)
    if st != 0 {
      txnWriteRaw(w, st, resp)
      return
    }
    if sn == nil {
      continue
    }
    switch a.Kind {
    case txnKindPut:
      sn.cur = txnCopyMap(a.Item)
    case txnKindDelete:
      sn.cur = nil
    case txnKindUpdate:
      if nc, ok := txnSimUpdate(sn.cur, a.UpdateExpr, a.Names, a.Values); ok {
        sn.cur = nc
      }
    }
    if a.Cond != "" {
      pass, err := txnEvalCondition(a.Cond, a.Names, a.Values, sn.cur)
      if err == nil && !pass {
        cancel = true
        fr := txnCancelReason{Code: "ConditionalCheckFailed", Message: txnStrPtr(txnCondFailMsg)}
        if a.RVOld && sn.pre != nil {
          fr.Item = sn.pre
        }
        reasons[a.Index] = fr
      }
    }
  }
  if cancel {
    txnWriteCanceled(w, reasons)
    return
  }

  // Apply phase: delegate every write to the regular operation handlers.
  for _, a := range acts {
    if a.Kind == txnKindCondCheck {
      continue
    }
    op := txnCopyMap(a.Op)
    delete(op, "ReturnValuesOnConditionCheckFailure")
    body, err := json.Marshal(op)
    if err != nil {
      continue
    }
    st, resp := s.invokeAction(a.opName(), body)
    if st != 200 {
      s.txnRollback(snaps)
      for i := range reasons {
        reasons[i] = txnCancelReason{Code: "None"}
      }
      reasons[a.Index] = txnReasonFromWire(resp)
      txnWriteCanceled(w, reasons)
      return
    }
  }
  txnWriteSuccess(w)
}

func txnParseAction(elem map[string]any) (*txnAction, string) {
  var a txnAction
  ops := 0
  if v, ok := elem["Put"]; ok {
    if m, ok2 := v.(map[string]any); ok2 {
      a.Kind, a.Op, ops = txnKindPut, m, ops+1
    }
  }
  if v, ok := elem["Update"]; ok {
    if m, ok2 := v.(map[string]any); ok2 {
      a.Kind, a.Op, ops = txnKindUpdate, m, ops+1
    }
  }
  if v, ok := elem["Delete"]; ok {
    if m, ok2 := v.(map[string]any); ok2 {
      a.Kind, a.Op, ops = txnKindDelete, m, ops+1
    }
  }
  if v, ok := elem["ConditionCheck"]; ok {
    if m, ok2 := v.(map[string]any); ok2 {
      a.Kind, a.Op, ops = txnKindCondCheck, m, ops+1
    }
  }
  if ops != 1 {
    return nil, "TransactItems must contain valid item operations"
  }
  t, _ := a.Op["TableName"].(string)
  if t == "" {
    return nil, "TableName is required"
  }
  a.Table = t
  switch a.Kind {
  case txnKindPut:
    it, _ := a.Op["Item"].(map[string]any)
    if it == nil {
      return nil, "Item is required"
    }
    a.Item = it
  default:
    k, _ := a.Op["Key"].(map[string]any)
    if len(k) == 0 {
      return nil, "Key is required"
    }
    a.Key = k
    a.KeyOK = true
  }
  if ce, ok := a.Op["ConditionExpression"].(string); ok && ce != "" {
    a.Cond = ce
  }
  if nm, ok := a.Op["ExpressionAttributeNames"].(map[string]any); ok {
    a.Names = make(map[string]string, len(nm))
    for k, v := range nm {
      if s2, ok2 := v.(string); ok2 {
        a.Names[k] = s2
      }
    }
  }
  if vv, ok := a.Op["ExpressionAttributeValues"].(map[string]any); ok {
    a.Values = vv
  }
  if rv, ok := a.Op["ReturnValuesOnConditionCheckFailure"].(string); ok {
    a.RVOld = strings.EqualFold(rv, "ALL_OLD")
  }
  if a.Kind == txnKindUpdate {
    if ue, ok := a.Op["UpdateExpression"].(string); ok {
      a.UpdateExpr = ue
    }
  }
  return &a, ""
}

func (s *Service) txnResolvePutKeys(acts []*txnAction) {
  cache := map[string][]string{}
  var need []*txnAction
  for _, a := range acts {
    if a.Kind != txnKindPut {
      continue
    }
    names, ok := cache[a.Table]
    if !ok {
      names, _ = txnTableKeySchema(s.ensureStore(), a.Table)
      cache[a.Table] = names
    }
    if len(names) > 0 && txnFillPutKey(a, names) {
      continue
    }
    need = append(need, a)
  }
  var tally map[string]map[string]int
  for _, a := range acts {
    if a.Kind == txnKindPut || !a.KeyOK {
      continue
    }
    if tally == nil {
      tally = map[string]map[string]int{}
    }
    b := tally[a.Table]
    if b == nil {
      b = map[string]int{}
      tally[a.Table] = b
    }
    for k := range a.Key {
      b[k]++
    }
  }
  for _, a := range need {
    b := tally[a.Table]
    if len(b) == 0 {
      continue
    }
    names := make([]string, 0, len(b))
    for k := range b {
      names = append(names, k)
    }
    sort.Strings(names)
    _ = txnFillPutKey(a, names)
  }
}

func txnFillPutKey(a *txnAction, names []string) bool {
  k := make(map[string]any, len(names))
  for _, n := range names {
    v, ok := a.Item[n]
    if !ok {
      return false
    }
    k[n] = v
  }
  a.Key = k
  a.KeyOK = true
  return true
}

type txnSnap struct {
  Table string
  Key   map[string]any
  pre   map[string]any
  cur   map[string]any
}

func (s *Service) txnSnapFor(snaps map[string]*txnSnap, a *txnAction) (*txnSnap, int, []byte) {
  if !a.KeyOK {
    return nil, 0, nil
  }
  id := txnCanonicalKey(a.Table, a.Key)
  if sn, ok := snaps[id]; ok {
    return sn, 0, nil
  }
  body, err := json.Marshal(map[string]any{"TableName": a.Table, "Key": a.Key, "ConsistentRead": true})
  if err != nil {
    return nil, 0, nil
  }
  st, resp := s.invokeAction("GetItem", body)
  if st != 200 {
    return nil, st, resp
  }
  var out struct {
    Item map[string]any `json:"Item"`
  }
  _ = json.Unmarshal(resp, &out)
  sn := &txnSnap{Table: a.Table, Key: a.Key, pre: txnCopyMap(out.Item)}
  sn.cur = txnCopyMap(out.Item)
  snaps[id] = sn
  return sn, 0, nil
}

func (s *Service) txnRollback(snaps map[string]*txnSnap) {
  for _, sn := range snaps {
    if sn.pre != nil {
      if b, err := json.Marshal(map[string]any{"TableName": sn.Table, "Item": sn.pre}); err == nil {
        _, _ = s.invokeAction("PutItem", b)
      }
    } else if sn.Key != nil {
      if b, err := json.Marshal(map[string]any{"TableName": sn.Table, "Key": sn.Key}); err == nil {
        _, _ = s.invokeAction("DeleteItem", b)
      }
    }
  }
}

func txnReasonFromWire(resp []byte) txnCancelReason {
  var wire map[string]any
  _ = json.Unmarshal(resp, &wire)
  typ, _ := wire["__type"].(string)
  code := "Validation"
  switch {
  case strings.Contains(typ, "ConditionalCheckFailed"):
    code = "ConditionalCheckFailed"
  case strings.Contains(typ, "Throttling"):
    code = "ThrottlingError"
  case strings.Contains(typ, "ProvisionedThroughput"):
    code = "ProvisionedThroughputExceeded"
  case strings.Contains(typ, "ItemCollectionSize"):
    code = "ItemCollectionSizeLimitExceeded"
  case strings.Contains(typ, "TransactionInProgress"):
    code = "TransactionInProgressException"
  }
  msg := ""
  if sv, ok := wire["message"].(string); ok {
    msg = sv
  } else if sv, ok := wire["Message"].(string); ok {
    msg = sv
  }
  r := txnCancelReason{Code: code}
  if code == "ConditionalCheckFailed" && msg == "" {
    msg = txnCondFailMsg
  }
  if msg != "" {
    r.Message = txnStrPtr(msg)
  }
  return r
}

// ---- canonical keys / copying ----

func txnCanonicalKey(table string, key map[string]any) string {
  keys := make([]string, 0, len(key))
  for k := range key {
    keys = append(keys, k)
  }
  sort.Strings(keys)
  var b strings.Builder
  b.WriteString(table)
  b.WriteByte(0)
  for _, k := range keys {
    b.WriteString(k)
    b.WriteByte(1)
    txnStableJSON(&b, key[k])
    b.WriteByte(0)
  }
  return b.String()
}

func txnStableJSON(b *strings.Builder, v any) {
  switch t := v.(type) {
  case map[string]any:
    keys := make([]string, 0, len(t))
    for k := range t {
      keys = append(keys, k)
    }
    sort.Strings(keys)
    b.WriteByte('{')
    for i, k := range keys {
      if i > 0 {
        b.WriteByte(',')
      }
      b.WriteString(strconv.Quote(k))
      b.WriteByte(':')
      txnStableJSON(b, t[k])
    }
    b.WriteByte('}')
  case []any:
    b.WriteByte('[')
    for i, e := range t {
      if i > 0 {
        b.WriteByte(',')
      }
      txnStableJSON(b, e)
    }
    b.WriteByte(']')
  case string:
    b.WriteString(strconv.Quote(t))
  default:
    enc, err := json.Marshal(t)
    if err != nil {
      b.WriteString("null")
      return
    }
    b.Write(enc)
  }
}

func txnCopyMap(in map[string]any) map[string]any {
  if in == nil {
    return nil
  }
  out := make(map[string]any, len(in))
  for k, v := range in {
    out[k] = txnCopyVal(v)
  }
  return out
}

func txnCopyVal(v any) any {
  switch t := v.(type) {
  case map[string]any:
    m := make(map[string]any, len(t))
    for k, vv := range t {
      m[k] = txnCopyVal(vv)
    }
    return m
  case []any:
    s := make([]any, len(t))
    for i, vv := range t {
      s[i] = txnCopyVal(vv)
    }
    return s
  default:
    return v
  }
}

// ---- reflective table-key-schema lookup (for Put key derivation) ----

func txnScanForStore(v reflect.Value) *Store {
  if !v.IsValid() {
    return nil
  }
  target := reflect.TypeOf((*Store)(nil))
  for v.IsValid() && (v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface) {
    if v.IsNil() {
      return nil
    }
    v = v.Elem()
  }
  if !v.IsValid() {
    return nil
  }
  if v.Type() == target {
    if vv, ok := v.Interface().(*Store); ok {
      return vv
    }
  }
  if v.Kind() == reflect.Struct {
    for i := 0; i < v.NumField(); i++ {
      f := txnFieldRead(v, i)
      if !f.IsValid() || f.Type() != target {
        continue
      }
      if vv, ok := f.Interface().(*Store); ok {
        return vv
      }
    }
  }
  return nil
}

// txnRepairMaps initializes any nil map fields of the store so the zero
// value of Store is usable.
func txnRepairMaps(v reflect.Value, depth int) {
  if depth > 4 {
    return
  }
  v = reflect.Indirect(v)
  if !v.IsValid() || v.Kind() != reflect.Struct {
    return
  }
  rt := v.Type()
  if strings.HasPrefix(rt.PkgPath(), "sync") {
    return
  }
  for i := 0; i < rt.NumField(); i++ {
    f := txnFieldRead(v, i)
    if !f.IsValid() {
      continue
    }
    switch f.Kind() {
    case reflect.Map:
      if f.IsNil() && f.CanSet() {
        f.Set(reflect.MakeMap(f.Type()))
      }
    case reflect.Ptr:
      if f.IsNil() {
        continue
      }
      txnRepairMaps(f, depth+1)
    case reflect.Struct:
      if f.Type() == reflect.TypeOf(sync.Mutex{}) || f.Type() == reflect.TypeOf(sync.RWMutex{}) {
        continue
      }
      txnRepairMaps(f, depth+1)
    }
  }
}

func txnFieldRead(v reflect.Value, i int) reflect.Value {
  f := v.Field(i)
  if f.CanInterface() {
    return f
  }
  if !v.CanAddr() || !f.CanAddr() {
    return reflect.Value{}
  }
  return reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem()
}

func txnDeref(v reflect.Value) reflect.Value {
  for v.IsValid() && (v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface) {
    if v.IsNil() {
      return reflect.Value{}
    }
    v = v.Elem()
  }
  return v
}

func txnMaterialize(v reflect.Value) reflect.Value {
  if !v.IsValid() {
    return v
  }
  if v.CanAddr() {
    return v
  }
  if !v.CanInterface() {
    return reflect.Value{}
  }
  p := reflect.New(v.Type())
  p.Elem().Set(v)
  return p.Elem()
}

func txnTableKeySchema(st *Store, table string) ([]string, bool) {
  if st == nil || table == "" {
    return nil, false
  }
  rv := reflect.ValueOf(st)
  if rv.Kind() != reflect.Ptr || rv.IsNil() {
    return nil, false
  }
  rs := rv.Elem()
  if rs.Kind() != reflect.Struct {
    return nil, false
  }
  m := txnFindTableMap(rs)
  if !m.IsValid() {
    return nil, false
  }
  idx := m.MapIndex(reflect.ValueOf(table).Convert(m.Type().Key()))
  if !idx.IsValid() {
    return nil, false
  }
  e := txnMaterialize(txnDeref(idx))
  if e.Kind() != reflect.Struct {
    return nil, false
  }
  sch := txnFindKeySchema(e)
  if len(sch) == 0 {
    return nil, false
  }
  return sch, true
}

func txnFindTableMap(rs reflect.Value) reflect.Value {
  var fallback reflect.Value
  rt := rs.Type()
  for i := 0; i < rt.NumField(); i++ {
    f := txnFieldRead(rs, i)
    if !f.IsValid() || f.Kind() != reflect.Map {
      continue
    }
    if f.Type().Key().Kind() != reflect.String {
      continue
    }
    base := f.Type().Elem()
    for base.Kind() == reflect.Ptr || base.Kind() == reflect.Interface {
      base = base.Elem()
    }
    if base.Kind() != reflect.Struct {
      continue
    }
    nm := strings.ToLower(rt.Field(i).Name)
    if strings.Contains(nm, "table") {
      return f
    }
    if !fallback.IsValid() {
      fallback = f
    }
  }
  return fallback
}

func txnFindKeySchema(tbl reflect.Value) []string {
  tt := tbl.Type()
  for i := 0; i < tt.NumField(); i++ {
    nm := strings.ReplaceAll(strings.ToLower(tt.Field(i).Name), "_", "")
    if !strings.Contains(nm, "keyschema") {
      continue
    }
    f := txnFieldRead(tbl, i)
    if f.Kind() == reflect.Slice || f.Kind() == reflect.Array {
      if sch := txnReadSchemaSlice(f); len(sch) > 0 {
        return sch
      }
    }
  }
  for i := 0; i < tt.NumField(); i++ {
    f := txnFieldRead(tbl, i)
    if (f.Kind() == reflect.Slice || f.Kind() == reflect.Array) && f.Len() > 0 {
      if sch := txnReadSchemaSlice(f); len(sch) > 0 {
        return sch
      }
    }
  }
  return nil
}

func txnReadSchemaSlice(f reflect.Value) []string {
  var hash, rng string
  for i := 0; i < f.Len(); i++ {
    e := txnDeref(f.Index(i))
    name, ktype := "", ""
    switch e.Kind() {
    case reflect.Struct:
      et := e.Type()
      for j := 0; j < et.NumField(); j++ {
        fv := txnFieldRead(e, j)
        if !fv.IsValid() || fv.Kind() != reflect.String {
          continue
        }
        low := strings.ToLower(et.Field(j).Name)
        sv := fv.String()
        switch {
        case strings.Contains(low, "attributename"):
          name = sv
        case strings.Contains(low, "keytype"):
          ktype = sv
        case sv == "HASH" || sv == "RANGE":
          ktype = sv
        }
      }
    case reflect.Map:
      mk := txnMaterialize(e)
      if !mk.IsValid() {
        continue
      }
      ks := mk.MapIndex(reflect.ValueOf("AttributeName"))
      if ks.IsValid() {
        name, _ = ks.Interface().(string)
      }
      kt := mk.MapIndex(reflect.ValueOf("KeyType"))
      if kt.IsValid() {
        ktype, _ = kt.Interface().(string)
      }
    }
    if name == "" {
      continue
    }
    switch ktype {
    case "HASH":
      hash = name
    case "RANGE":
      rng = name
    default:
      if hash == "" {
        hash = name
      } else if rng == "" {
        rng = name
      }
    }
  }
  var out []string
  if hash != "" {
    out = append(out, hash)
  }
  if rng != "" {
    out = append(out, rng)
  }
  return out
}

// ---- condition expression engine ----

type txnTok struct {
  kind byte // 0: word/placeholder, 1: punctuation
  text string
}

var txnErrSyntax = errors.New("invalid expression")

func txnIdentByte(b byte) bool {
  return b == '_' || (b >= '0' && b <= '9') || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

func txnLex(s string) ([]txnTok, error) {
  toks := make([]txnTok, 0, len(s)/2+1)
  i, n := 0, len(s)
  for i < n {
    c := s[i]
    switch {
    case c == ' ' || c == '\t' || c == '\r' || c == '\n':
      i++
    case c == '(' || c == ')' || c == ',' || c == '.' || c == '[' || c == ']' || c == '=' || c == '+' || c == '-':
      toks = append(toks, txnTok{1, string(c)})
      i++
    case c == '<':
      if i+1 < n && s[i+1] == '=' {
        toks = append(toks, txnTok{1, "<="})
        i += 2
      } else if i+1 < n && s[i+1] == '>' {
        toks = append(toks, txnTok{1, "<>"})
        i += 2
      } else {
        toks = append(toks, txnTok{1, "<"})
        i++
      }
    case c == '>':
      if i+1 < n && s[i+1] == '=' {
        toks = append(toks, txnTok{1, ">="})
        i += 2
      } else {
        toks = append(toks, txnTok{1, ">"})
        i++
      }
    case c == '#' || c == ':':
      j := i + 1
      for j < n && (txnIdentByte(s[j]) || s[j] == '-') {
        j++
      }
      if j == i+1 {
        return nil, txnErrSyntax
      }
      toks = append(toks, txnTok{0, s[i:j]})
      i = j
    case txnIdentByte(c):
      j := i
      for j < n && txnIdentByte(s[j]) {
        j++
      }
      toks = append(toks, txnTok{0, s[i:j]})
      i = j
    default:
      return nil, txnErrSyntax
    }
  }
  return toks, nil
}

type txnPathSeg struct {
  name  string
  idx   int
  isIdx bool
}

type txnOperand struct {
  kind byte
  segs []txnPathSeg
  raw  string
  arg1 *txnOperand
  arg2 *txnOperand
}

const (
  txnOPath byte = iota + 1
  txnOValue
  txnOSize
  txnOAttrType
  txnOListAppend
  txnOIfNotExists
  txnOAdd
  txnOSub
)

type txnPred struct {
  kind byte
  op   string
  ants []*txnOperand
  kids []*txnPred
}

const (
  txnPAnd byte = iota + 1
  txnPOr
  txnPNot
  txnPCmp
  txnPBetween
  txnPIn
  txnPExists
  txnPNotExists
  txnPBeginsWith
  txnPContains
  txnPAttrType
)

type txnCondParser struct {
  toks   []txnTok
  names  map[string]string
  values map[string]any
  pos    int
}

func (p *txnCondParser) peek() (txnTok, bool) {
  if p.pos < len(p.toks) {
    return p.toks[p.pos], true
  }
  return txnTok{}, false
}

func (p *txnCondParser) peekWord() (string, bool) {
  if t, ok := p.peek(); ok && t.kind == 0 {
    return t.text, true
  }
  return "", false
}

func (p *txnCondParser) isWordI(w string) bool {
  if t, ok := p.peekWord(); ok {
    return strings.EqualFold(t, w)
  }
  return false
}

func (p *txnCondParser) peekPunct(t string) bool {
  if tok, ok := p.peek(); ok && tok.kind == 1 && tok.text == t {
    return true
  }
  return false
}

func (p *txnCondParser) expectPunct(t string) bool {
  if p.peekPunct(t) {
    p.pos++
    return true
  }
  return false
}

func (p *txnCondParser) resolveName(w string) string {
  if strings.HasPrefix(w, "#") {
    return p.names[w]
  }
  return w
}

func (p *txnCondParser) parsePath() ([]txnPathSeg, bool) {
  w, ok := p.peekWord()
  if !ok {
    return nil, false
  }
  name := p.resolveName(w)
  if name == "" {
    return nil, false
  }
  p.pos++
  segs := []txnPathSeg{{name: name}}
  for {
    if p.peekPunct(".") {
      p.pos++
      nw, ok2 := p.peekWord()
      if !ok2 {
        return nil, false
      }
      nm := p.resolveName(nw)
      if nm == "" {
        return nil, false
      }
      p.pos++
      segs = append(segs, txnPathSeg{name: nm})
      continue
    }
    if p.peekPunct("[") {
      p.pos++
      it, ok2 := p.peekWord()
      if !ok2 {
        return nil, false
      }
      idx, err := strconv.Atoi(it.text)
      if err != nil {
        return nil, false
      }
      p.pos++
      if !p.expectPunct("]") {
        return nil, false
      }
      segs = append(segs, txnPathSeg{idx: idx, isIdx: true})
      continue
    }
    break
  }
  return segs, true
}

func (p *txnCondParser) parseOperand() (*txnOperand, error) {
  w, ok := p.peekWord()
  if !ok {
    return nil, txnErrSyntax
  }
  if strings.HasPrefix(w, ":") {
    p.pos++
    return &txnOperand{kind: txnOValue, raw: w}, nil
  }
  if p.pos+1 < len(p.toks) && p.toks[p.pos+1].kind == 1 && p.toks[p.pos+1].text == "(" {
    switch strings.ToLower(w) {
    case "size":
      p.pos += 2
      arg, err := p.parseOperand()
      if err != nil {
        return nil, err
      }
      if !p.expectPunct(")") {
        return nil, txnErrSyntax
      }
      return &txnOperand{kind: txnOSize, arg1: arg}, nil
    case "attribute_type":
      p.pos += 2
      a, err := p.parseOperand()
      if err != nil {
        return nil, err
      }
      if !p.expectPunct(",") {
        return nil, txnErrSyntax
      }
      b, err := p.parseOperand()
      if err != nil {
        return nil, err
      }
      if !p.expectPunct(")") {
        return nil, txnErrSyntax
      }
      return &txnOperand{kind: txnOAttrType, arg1: a, arg2: b}, nil
    }
  }
  segs, ok := p.parsePath()
  if !ok {
    return nil, txnErrSyntax
  }
  return &txnOperand{kind: txnOPath, segs: segs}, nil
}

func (p *txnCondParser) parseCond() (*txnPred, error) { return p.parseOr() }

func (p *txnCondParser) parseOr() (*txnPred, error) {
  l, err := p.parseAnd()
  if err != nil {
    return nil, err
  }
  for p.isWordI("or") {
    p.pos++
    r, err2 := p.parseAnd()
    if err2 != nil {
      return nil, err2
    }
    l = &txnPred{kind: txnPOr, kids: []*txnPred{l, r}}
  }
  return l, nil
}

func (p *txnCondParser) parseAnd() (*txnPred, error) {
  l, err := p.parseNot()
  if err != nil {
    return nil, err
  }
  for p.isWordI("and") {
    p.pos++
    r, err2 := p.parseNot()
    if err2 != nil {
      return nil, err2
    }
    l = &txnPred{kind: txnPAnd, kids: []*txnPred{l, r}}
  }
  return l, nil
}

func (p *txnCondParser) parseNot() (*txnPred, error) {
  if p.isWordI("not") {
    p.pos++
    k, err := p.parseNot()
    if err != nil {
      return nil, err
    }
    return &txnPred{kind: txnPNot, kids: []*txnPred{k}}, nil
  }
  return p.parseCmp()
}

func (p *txnCondParser) parseCmp() (*txnPred, error) {
  if p.peekPunct("(") {
    p.pos++
    cc, err := p.parseCond()
    if err != nil {
      return nil, err
    }
    if !p.expectPunct(")") {
      return nil, txnErrSyntax
    }
    return cc, nil
  }
  if w, ok := p.peekWord(); ok {
    fold := strings.ToLower(w)
    switch fold {
    case "attribute_exists", "attribute_not_exists":
      p.pos++
      if !p.expectPunct("(") {
        return nil, txnErrSyntax
      }
      op, err := p.parseOperand()
      if err != nil {
        return nil, err
      }
      if !p.expectPunct(")") {
        return nil, txnErrSyntax
      }
      if op.kind != txnOPath {
        return nil, txnErrSyntax
      }
      k := txnPExists
      if fold == "attribute_not_exists" {
        k = txnPNotExists
      }
      return &txnPred{kind: k, ants: []*txnOperand{op}}, nil
    case "begins_with", "contains", "attribute_type":
      p.pos++
      if !p.expectPunct("(") {
        return nil, txnErrSyntax
      }
      a, err := p.parseOperand()
      if err != nil {
        return nil, err
      }
      if !p.expectPunct(",") {
        return nil, txnErrSyntax
      }
      b, err := p.parseOperand()
      if err != nil {
        return nil, err
      }
      if !p.expectPunct(")") {
        return nil, txnErrSyntax
      }
      k := txnPBeginsWith
      if fold == "contains" {
        k = txnPContains
      } else if fold == "attribute_type" {
        k = txnPAttrType
      }
      return &txnPred{kind: k, ants: []*txnOperand{a, b}}, nil
    }
  }
  lhs, err := p.parseOperand()
  if err != nil {
    return nil, err
  }
  if t, ok := p.peek(); ok && t.kind == 1 {
    switch t.text {
    case "=", "<>", "<", "<=", ">", ">=":
      p.pos++
      rhs, err := p.parseOperand()
      if err != nil {
        return nil, err
      }
      return &txnPred{kind: txnPCmp, op: t.text, ants: []*txnOperand{lhs, rhs}}, nil
    }
  }
  if p.isWordI("between") {
    p.pos++
    lo, err := p.parseOperand()
    if err != nil {
      return nil, err
    }
    if !p.isWordI("and") {
      return nil, txnErrSyntax
    }
    p.pos++
    hi, err := p.parseOperand()
    if err != nil {
      return nil, err
    }
    return &txnPred{kind: txnPBetween, ants: []*txnOperand{lhs, lo, hi}}, nil
  }
  if p.isWordI("in") {
    p.pos++
    if !p.expectPunct("(") {
      return nil, txnErrSyntax
    }
    list := []*txnOperand{lhs}
    for {
      alt, err := p.parseOperand()
      if err != nil {
        return nil, err
      }
      list = append(list, alt)
      if p.peekPunct(",") {
        p.pos++
        continue
      }
      break
    }
    if !p.expectPunct(")") {
      return nil, txnErrSyntax
    }
    return &txnPred{kind: txnPIn, ants: list}, nil
  }
  return nil, txnErrSyntax
}

func txnEvalCondition(expr string, names map[string]string, values map[string]any, item map[string]any) (bool, error) {
  toks, err := txnLex(expr)
  if err != nil {
    return false, err
  }
  p := &txnCondParser{toks: toks, names: names, values: values}
  pred, err := p.parseCond()
  if err != nil {
    return false, err
  }
  if p.pos != len(p.toks) {
    return false, txnErrSyntax
  }
  return txnEvalPred(pred, names, values, item), nil
}

func txnEvalPred(pred *txnPred, names map[string]string, values map[string]any, item map[string]any) bool {
  switch pred.kind {
  case txnPAnd:
    for _, k := range pred.kids {
      if !txnEvalPred(k, names, values, item) {
        return false
      }
    }
    return true
  case txnPOr:
    for _, k := range pred.kids {
      if txnEvalPred(k, names, values, item) {
        return true
      }
    }
    return false
  case txnPNot:
    return !txnEvalPred(pred.kids[0], names, values, item)
  case txnPExists:
    _, ok := txnResolveOperand(pred.ants[0], names, values, item)
    return ok
  case txnPNotExists:
    _, ok := txnResolveOperand(pred.ants[0], names, values, item)
    return !ok
  case txnPBeginsWith:
    a, aok := txnResolveOperand(pred.ants[0], names, values, item)
    b, bok := txnResolveOperand(pred.ants[1], names, values, item)
    if !aok || !bok {
      return false
    }
    if txnKindOf(a) == "S" && txnKindOf(b) == "S" {
      as, _ := txnPayloadOf(a, "S").(string)
      bs, _ := txnPayloadOf(b, "S").(string)
      return strings.HasPrefix(as, bs)
    }
    if txnKindOf(a) == "B" && txnKindOf(b) == "B" {
      ab, ok1 := txnDecodeB(a)
      bb, ok2 := txnDecodeB(b)
      return ok1 && ok2 && bytes.HasPrefix(ab, bb)
    }
    return false
  case txnPContains:
    a, aok := txnResolveOperand(pred.ants[0], names, values, item)
    b, bok := txnResolveOperand(pred.ants[1], names, values, item)
    if !aok || !bok {
      return false
    }
    switch txnKindOf(a) {
    case "S":
      if txnKindOf(b) != "S" {
        return false
      }
      as, _ := txnPayloadOf(a, "S").(string)
      bs, _ := txnPayloadOf(b, "S").(string)
      return strings.Contains(as, bs)
    case "B":
      if txnKindOf(b) != "B" {
        return false
      }
      ab, ok1 := txnDecodeB(a)
      bb, ok2 := txnDecodeB(b)
      return ok1 && ok2 && bytes.Contains(ab, bb)
    case "SS", "NS", "BS", "L":
      for _, e := range txnSetOrList(a) {
        if txnTypedEq(e, b) {
          return true
        }
      }
    }
    return false
  case txnPAttrType:
    a, aok := txnResolveOperand(pred.ants[0], names, values, item)
    t, tok := txnResolveOperand(pred.ants[1], names, values, item)
    if !aok || !tok {
      return false
    }
    tn, _ := txnPayloadOf(t, "S").(string)
    return tn != "" && txnKindOf(a) == tn
  case txnPCmp:
    a, aok := txnResolveOperand(pred.ants[0], names, values, item)
    b, bok := txnResolveOperand(pred.ants[1], names, values, item)
    if !aok || !bok {
      return false
    }
    return txnCompare(pred.op, a, b)
  case txnPBetween:
    a, aok := txnResolveOperand(pred.ants[0], names, values, item)
    lo, lok := txnResolveOperand(pred.ants[1], names, values, item)
    hi, hik := txnResolveOperand(pred.ants[2], names, values, item)
    if !aok || !lok || !hik {
      return false
    }
    return txnCompare(">=", a, lo) && txnCompare("<=", a, hi)
  case txnPIn:
    a, aok := txnResolveOperand(pred.ants[0], names, values, item)
    if !aok {
      return false
    }
    for _, alt := range pred.ants[1:] {
      b, bok := txnResolveOperand(alt, names, values, item)
      if bok && txnTypedEq(a, b) {
        return true
      }
    }
    return false
  }
  return false
}

func txnResolveOperand(op *txnOperand, names map[string]string, values map[string]any, item map[string]any) (any, bool) {
  if op == nil {
    return nil, false
  }
  switch op.kind {
  case txnOPath:
    return txnPathGet(item, op.segs)
  case txnOValue:
    v, ok := values[op.raw]
    return v, ok
  case txnOSize:
    v, ok := txnResolveOperand(op.arg1, names, values, item)
    if !ok {
      return map[string]any{"N": "0"}, true
    }
    n, ok2 := txnSizeOf(v)
    if !ok2 {
      return nil, false
    }
    return map[string]any{"N": strconv.Itoa(n)}, true
  case txnOAttrType:
    v, ok := txnResolveOperand(op.arg1, names, values, item)
    if !ok {
      return map[string]any{"BOOL": false}, true
    }
    t, ok2 := txnResolveOperand(op.arg2, names, values, item)
    if !ok2 {
      return nil, false
    }
    tn, _ := txnPayloadOf(t, "S").(string)
    return map[string]any{"BOOL": tn != "" && txnKindOf(v) == tn}, true
  case txnOListAppend:
    a, ok := txnResolveOperand(op.arg1, names, values, item)
    if !ok {
      return nil, false
    }
    b, ok2 := txnResolveOperand(op.arg2, names, values, item)
    if !ok2 {
      return nil, false
    }
    la, ok3 := txnAsList(a)
    lb, ok4 := txnAsList(b)
    if !ok3 || !ok4 {
      return nil, false
    }
    out := make([]any, 0, len(la)+len(lb))
    for _, e := range la {
      out = append(out, txnCopyVal(e))
    }
    for _, e := range lb {
      out = append(out, txnCopyVal(e))
    }
    return map[string]any{"L": out}, true
  case txnOIfNotExists:
    v, ok := txnResolveOperand(op.arg1, names, values, item)
    if ok {
      return v, true
    }
    return txnResolveOperand(op.arg2, names, values, item)
  case txnOAdd, txnOSub:
    a, ok := txnResolveOperand(op.arg1, names, values, item)
    if !ok {
      return nil, false
    }
    b, ok2 := txnResolveOperand(op.arg2, names, values, item)
    if !ok2 {
      return nil, false
    }
    fa, ok3 := txnNumOf(a)
    fb, ok4 := txnNumOf(b)
    if !ok3 || !ok4 {
      return nil, false
    }
    if op.kind == txnOSub {
      fb = -fb
    }
    return map[string]any{"N": txnFormatNum(fa + fb)}, true
  }
  return nil, false
}

func txnPathGet(item map[string]any, segs []txnPathSeg) (any, bool) {
  var cur any
  atRoot := true
  for _, sg := range segs {
    if sg.isIdx {
      if atRoot {
        return nil, false
      }
      lst, ok := txnAsList(cur)
      if !ok || sg.idx < 0 || sg.idx >= len(lst) {
        return nil, false
      }
      cur = lst[sg.idx]
      continue
    }
    if atRoot {
      v, ok := item[sg.name]
      if !ok {
        return nil, false
      }
      cur = v
      atRoot = false
      continue
    }
    mm, ok := txnAsMap(cur)
    if !ok {
      return nil, false
    }
    v, ok2 := mm[sg.name]
    if !ok2 {
      return nil, false
    }
    cur = v
  }
  return cur, true
}

func txnKindOf(v any) string {
  m, ok := v.(map[string]any)
  if !ok || len(m) != 1 {
    return ""
  }
  for k := range m {
    return k
  }
  return ""
}

func txnPayloadOf(v any, field string) any {
  m, ok := v.(map[string]any)
  if !ok {
    return nil
  }
  return m[field]
}

func txnNumOf(v any) (float64, bool) {
  s, ok := txnPayloadOf(v, "N").(string)
  if !ok {
    return 0, false
  }
  f, err := strconv.ParseFloat(s, 64)
  if err != nil {
    return 0, false
  }
  return f, true
}

func txnDecodeB(v any) ([]byte, bool) {
  s, ok := txnPayloadOf(v, "B").(string)
  if !ok {
    return nil, false
  }
  b, err := base64.StdEncoding.DecodeString(s)
  if err != nil {
    return nil, false
  }
  return b, true
}

func txnFormatNum(f float64) string {
  if !math.IsInf(f, 0) && !math.IsNaN(f) && f == math.Trunc(f) && math.Abs(f) < 1e15 {
    return strconv.FormatInt(int64(f), 10)
  }
  return strconv.FormatFloat(f, 'g', -1, 64)
}

func txnSizeOf(v any) (int, bool) {
  switch txnKindOf(v) {
  case "S":
    s, _ := txnPayloadOf(v, "S").(string)
    return len(s), true
  case "B":
    b, ok := txnDecodeB(v)
    return len(b), ok
  case "SS", "NS", "BS", "L":
    l, _ := txnPayloadOf(v, txnKindOf(v)).([]any)
    return len(l), true
  case "M":
    m, _ := txnPayloadOf(v, "M").(map[string]any)
    return len(m), true
  }
  return 0, false
}

func txnAsMap(v any) (map[string]any, bool) {
  m, ok := v.(map[string]any)
  if !ok {
    return nil, false
  }
  inner, ok2 := m["M"].(map[string]any)
  return inner, ok2
}

func txnAsList(v any) ([]any, bool) {
  m, ok := v.(map[string]any)
  if !ok {
    return nil, false
  }
  inner, ok2 := m["L"].([]any)
  return inner, ok2
}

func txnSetOrList(v any) []any {
  for _, k := range []string{"SS", "NS", "BS", "L"} {
    if l, ok := txnPayloadOf(v, k).([]any); ok {
      return l
    }
  }
  return nil
}

func txnTypedEq(a, b any) bool {
  ka := txnKindOf(a)
  if ka == "" {
    return reflect.DeepEqual(a, b)
  }
  if txnKindOf(b) != ka {
    return false
  }
  switch ka {
  case "S":
    as, _ := txnPayloadOf(a, "S").(string)
    bs, _ := txnPayloadOf(b, "S").(string)
    return as == bs
  case "N":
    fa, ok1 := txnNumOf(a)
    fb, ok2 := txnNumOf(b)
    return ok1 && ok2 && fa == fb
  case "B":
    ab, ok1 := txnDecodeB(a)
    bb, ok2 := txnDecodeB(b)
    return ok1 && ok2 && bytes.Equal(ab, bb)
  case "BOOL":
    ab, _ := txnPayloadOf(a, "BOOL").(bool)
    bb, _ := txnPayloadOf(b, "BOOL").(bool)
    return ab == bb
  case "NULL":
    return true
  case "SS", "NS", "BS":
    la, _ := txnPayloadOf(a, ka).([]any)
    lb, _ := txnPayloadOf(b, ka).([]any)
    if len(la) != len(lb) {
      return false
    }
    used := make([]bool, len(lb))
    for _, e := range la {
      found := false
      for j, f := range lb {
        if !used[j] && txnTypedEq(e, f) {
          used[j] = true
          found = true
          break
        }
      }
      if !found {
        return false
      }
    }
    return true
  case "L":
    la, _ := txnPayloadOf(a, "L").([]any)
    lb, _ := txnPayloadOf(b, "L").([]any)
    if len(la) != len(lb) {
      return false
    }
    for i := range la {
      if !txnTypedEq(la[i], lb[i]) {
        return false
      }
    }
    return true
  case "M":
    ma, _ := txnPayloadOf(a, "M").(map[string]any)
    mb, _ := txnPayloadOf(b, "M").(map[string]any)
    if len(ma) != len(mb) {
      return false
    }
    for k, av := range ma {
      bv, ok := mb[k]
      if !ok || !txnTypedEq(av, bv) {
        return false
      }
    }
    return true
  }
  return reflect.DeepEqual(a, b)
}

func txnTypedLess(a, b any) (bool, bool) {
  ka := txnKindOf(a)
  if ka == "" || txnKindOf(b) != ka {
    return false, false
  }
  switch ka {
  case "S":
    as, _ := txnPayloadOf(a, "S").(string)
    bs, _ := txnPayloadOf(b, "S").(string)
    return as < bs, true
  case "N":
    fa, ok1 := txnNumOf(a)
    fb, ok2 := txnNumOf(b)
    if !ok1 || !ok2 {
      return false, false
    }
    return fa < fb, true
  case "B":
    ab, ok1 := txnDecodeB(a)
    bb, ok2 := txnDecodeB(b)
    if !ok1 || !ok2 {
      return false, false
    }
    return bytes.Compare(ab, bb) < 0, true
  }
  return false, false
}

func txnCompare(op string, a, b any) bool {
  switch op {
  case "=":
    return txnTypedEq(a, b)
  case "<>":
    return !txnTypedEq(a, b)
  case "<":
    lt, ok := txnTypedLess(a, b)
    return ok && lt
  case "<=":
    lt, ok := txnTypedLess(a, b)
    if ok && lt {
      return true
    }
    return txnTypedEq(a, b)
  case ">":
    gt, ok := txnTypedLess(b, a)
    return ok && gt
  case ">=":
    gt, ok := txnTypedLess(b, a)
    if ok && gt {
      return true
    }
    return txnTypedEq(a, b)
  }
  return false
}

// ---- update expression simulation (snapshot only) ----

func txnSimUpdate(cur map[string]any, expr string, names map[string]string, values map[string]any) (map[string]any, bool) {
  if strings.TrimSpace(expr) == "" {
    return cur, true
  }
  toks, err := txnLex(expr)
  if err != nil {
    return cur, false
  }
  p := &txnCondParser{toks: toks, names: names, values: values}
  root := cur
  if root == nil {
    root = map[string]any{}
  }
  stage := ""
  for p.pos < len(p.toks) {
    if w, ok := p.peekWord(); ok {
      fold := strings.ToLower(w)
      if fold == "set" || fold == "remove" || fold == "add" || fold == "delete" {
        stage = fold
        p.pos++
        continue
      }
    }
    if stage == "" {
      return cur, false
    }
    switch stage {
    case "set":
      segs, ok := p.parsePath()
      if !ok {
        return cur, false
      }
      if !p.expectPunct("=") {
        return cur, false
      }
      val, ok2 := p.parseSimOperandEval(root)
      if !ok2 {
        return cur, false
      }
      leaf := txnLocate(root, segs, true)
      if !leaf.ok {
        return cur, false
      }
      txnLeafSet(leaf, val)
    case "remove":
      segs, ok := p.parsePath()
      if !ok {
        return cur, false
      }
      leaf := txnLocate(root, segs, false)
      if leaf.ok {
        txnLeafRemove(leaf)
      }
    case "add":
      segs, ok := p.parsePath()
      if !ok {
        return cur, false
      }
      val, ok2 := p.parseSimOperandEval(root)
      if !ok2 {
        return cur, false
      }
      leaf := txnLocate(root, segs, true)
      if !leaf.ok {
        return cur, false
      }
      if !txnLeafAdd(leaf, val) {
        return cur, false
      }
    case "delete":
      segs, ok := p.parsePath()
      if !ok {
        return cur, false
      }
      val, ok2 := p.parseSimOperandEval(root)
      if !ok2 {
        return cur, false
      }
      leaf := txnLocate(root, segs, false)
      if !leaf.ok {
        return cur, false
      }
      if !txnLeafDelete(leaf, val) {
        return cur, false
      }
    default:
      return cur, false
    }
    if p.peekPunct(",") {
      p.pos++
    }
  }
  return root, true
}

func (p *txnCondParser) parseSimOperandEval(item map[string]any) (any, bool) {
  op, err := p.parseSimOperand()
  if err != nil {
    return nil, false
  }
  v, ok := txnResolveOperand(op, p.names, p.values, item)
  if !ok {
    return nil, false
  }
  return v, true
}

func (p *txnCondParser) parseSimOperand() (*txnOperand, error) {
  w, ok := p.peekWord()
  if !ok {
    return nil, txnErrSyntax
  }
  if strings.HasPrefix(w, ":") {
    p.pos++
    return &txnOperand{kind: txnOValue, raw: w}, nil
  }
  if p.pos+1 < len(p.toks) && p.toks[p.pos+1].kind == 1 && p.toks[p.pos+1].text == "(" {
    switch strings.ToLower(w) {
    case "list_append":
      p.pos += 2
      a, err := p.parseSimOperand()
      if err != nil {
        return nil, err
      }
      if !p.expectPunct(",") {
        return nil, txnErrSyntax
      }
      b, err := p.parseSimOperand()
      if err != nil {
        return nil, err
      }
      if !p.expectPunct(")") {
        return nil, txnErrSyntax
      }
      return &txnOperand{kind: txnOListAppend, arg1: a, arg2: b}, nil
    case "if_not_exists":
      p.pos += 2
      a, err := p.parseSimOperand()
      if err != nil {
        return nil, err
      }
      if !p.expectPunct(",") {
        return nil, txnErrSyntax
      }
      b, err := p.parseSimOperand()
      if err != nil {
        return nil, err
      }
      if !p.expectPunct(")") {
        return nil, txnErrSyntax
      }
      return &txnOperand{kind: txnOIfNotExists, arg1: a, arg2: b}, nil
    }
  }
  segs, ok := p.parsePath()
  if !ok {
    return nil, txnErrSyntax
  }
  op := &txnOperand{kind: txnOPath, segs: segs}
  for p.peekPunct("+") || p.peekPunct("-") {
    t, _ := p.peek()
    p.pos++
    rhs, err := p.parseSimOperand()
    if err != nil {
      return nil, err
    }
    k := txnOAdd
    if t.text == "-" {
      k = txnOSub
    }
    op = &txnOperand{kind: k, arg1: op, arg2: rhs}
  }
  return op, nil
}

type txnLeaf struct {
  m      map[string]any
  name   string
  isList bool
  idx    int
  ok     bool
}

func txnLocate(root map[string]any, segs []txnPathSeg, create bool) txnLeaf {
  if len(segs) == 0 {
    return txnLeaf{}
  }
  var container any = root
  atRoot := true
  for i := 0; i < len(segs)-1; i++ {
    sg := segs[i]
    if sg.isIdx {
      lst, ok := txnAsList(container)
      if !ok || sg.idx < 0 || sg.idx >= len(lst) {
        return txnLeaf{}
      }
      container = lst[sg.idx]
      atRoot = false
      continue
    }
    var m map[string]any
    if atRoot {
      m, _ = container.(map[string]any)
      if m == nil {
        return txnLeaf{}
      }
    } else {
      m, _ = txnAsMap(container)
      if m == nil {
        return txnLeaf{}
      }
    }
    v, e := m[sg.name]
    if !e {
      if !create {
        return txnLeaf{}
      }
      v = map[string]any{"M": map[string]any{}}
      m[sg.name] = v
    }
    container = v
    atRoot = false
  }
  last := segs[len(segs)-1]
  if last.isIdx {
    om, _ := container.(map[string]any)
    if om == nil {
      return txnLeaf{}
    }
    lst, ok := om["L"].([]any)
    if !ok || last.idx < 0 || last.idx >= len(lst) {
      return txnLeaf{}
    }
    return txnLeaf{m: om, name: "L", isList: true, idx: last.idx, ok: true}
  }
  if atRoot {
    return txnLeaf{m: root, name: last.name, ok: true}
  }
  mm, _ := txnAsMap(container)
  if mm == nil {
    return txnLeaf{}
  }
  return txnLeaf{m: mm, name: last.name, ok: true}
}

func txnLeafSet(l txnLeaf, val any) {
  v := txnCopyVal(val)
  if l.isList {
    lst, _ := l.m["L"].([]any)
    if l.idx >= 0 && l.idx < len(lst) {
      lst[l.idx] = v
      l.m["L"] = lst
    }
    return
  }
  l.m[l.name] = v
}

func txnLeafRemove(l txnLeaf) {
  if l.isList {
    lst, _ := l.m["L"].([]any)
    if l.idx >= 0 && l.idx < len(lst) {
      out := make([]any, 0, len(lst)-1)
      out = append(out, lst[:l.idx]...)
      out = append(out, lst[l.idx+1:]...)
      l.m["L"] = out
    }
    return
  }
  delete(l.m, l.name)
}

func txnLeafAdd(l txnLeaf, val any) bool {
  if l.isList {
    return false
  }
  cur, exists := l.m[l.name]
  if !exists {
    l.m[l.name] = txnCopyVal(val)
    return true
  }
  k := txnKindOf(cur)
  switch k {
  case "N":
    fa, ok1 := txnNumOf(cur)
    fv, ok2 := txnNumOf(val)
    if !ok1 || !ok2 {
      return false
    }
    l.m[l.name] = map[string]any{"N": txnFormatNum(fa + fv)}
    return true
  case "SS", "NS", "BS":
    lst, _ := txnPayloadOf(cur, k).([]any)
    nv, _ := txnPayloadOf(val, k).([]any)
    if nv == nil {
      return false
    }
    nl := make([]any, 0, len(lst)+len(nv))
    for _, e := range lst {
      nl = append(nl, txnCopyVal(e))
    }
    for _, e := range nv {
      dup := false
      for _, f := range nl {
        if txnTypedEq(e, f) {
          dup = true
          break
        }
      }
      if !dup {
        nl = append(nl, txnCopyVal(e))
      }
    }
    l.m[l.name] = map[string]any{k: nl}
    return true
  }
  return false
}

func txnLeafDelete(l txnLeaf, val any) bool {
  if l.isList {
    return false
  }
  cur, exists := l.m[l.name]
  if !exists {
    return true
  }
  k := txnKindOf(cur)
  if k != "SS" && k != "NS" && k != "BS" {
    return false
  }
  lst, _ := txnPayloadOf(cur, k).([]any)
  nv, _ := txnPayloadOf(val, k).([]any)
  if nv == nil {
    return false
  }
  nl := make([]any, 0, len(lst))
  for _, e := range lst {
    rm := false
    for _, f := range nv {
      if txnTypedEq(e, f) {
        rm = true
        break
      }
    }
    if !rm {
      nl = append(nl, txnCopyVal(e))
    }
  }
  l.m[l.name] = map[string]any{k: nl}
  return true
}
