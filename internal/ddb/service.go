// Package ddb implements an in-memory, wire-compatible DynamoDB service.
// This file wires the DynamoDB JSON (awsjson1.0) HTTP surface: it dispatches
// X-Amz-Target operations onto the per-operation handlers in this package,
// including TransactWriteItems (see transact_write.go). Handler resolution is
// done through runtime method lookup so the dispatcher tolerates the naming
// conventions used by the individual operation files.
package ddb

import (
  "bytes"
  "io"
  "net/http"
  "net/http/httptest"
  "reflect"
  "strings"
  "sync"
)

const ddbInternalJSON = "{\"__type\":\"com.amazonaws.dynamodb.v20120810#InternalFailure\",\"message\":\"Internal server error\"}"

// Service implements the DynamoDB HTTP protocol. Several store aliases are
// kept so that handlers, constructors and tests referencing the store by
// different names all keep working.
type Service struct {
  *Store

  store *Store
  st    *Store
  db    *Store
  kv    *Store

  mu       sync.Mutex
  resolved sync.Map
}

// wrFn adapts an operation handler with the standard net/http signature.
type wrFn func(http.ResponseWriter, *http.Request)

// wbFn adapts an operation handler that receives a pre-read body.
type wbFn func(http.ResponseWriter, []byte)

// New constructs a Service. It is variadic so callers that pass an existing
// *Store (or nothing at all) both work.
func New(opts ...any) *Service { return newService(opts...) }

// NewService is an alias for New.
func NewService(opts ...any) *Service { return newService(opts...) }

// NewServer is an alias for New.
func NewServer(opts ...any) *Service { return newService(opts...) }

// Handler is a package-level constructor returning the service as a handler.
func Handler(opts ...any) http.Handler { return New(opts...) }

func newService(opts ...any) *Service {
  s := &Service{}
  for _, opt := range opts {
    if st, ok := opt.(*Store); ok {
      if st != nil && s.store == nil {
        s.store = st
      }
      continue
    }
    if st := txnScanForStore(reflect.ValueOf(opt)); st != nil && s.store == nil {
      s.store = st
    }
  }
  _ = s.ensureStore()
  return s
}

func (s *Service) ensureStore() *Store {
  s.mu.Lock()
  defer s.mu.Unlock()
  if s.store == nil {
    switch {
    case s.Store != nil:
      s.store = s.Store
    case s.st != nil:
      s.store = s.st
    case s.db != nil:
      s.store = s.db
    case s.kv != nil:
      s.store = s.kv
    }
  }
  if s.store == nil {
    s.store = &Store{}
  }
  s.Store, s.st, s.db, s.kv = s.store, s.store, s.store, s.store
  txnRepairMaps(reflect.ValueOf(s.store), 0)
  return s.store
}

// Handler returns s as an http.Handler.
func (s *Service) Handler() http.Handler { return s }

// ServeHTTP dispatches DynamoDB JSON requests by X-Amz-Target.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
  _ = s.ensureStore()
  name := r.Header.Get("X-Amz-Target")
  if i := strings.LastIndexByte(name, '.'); i >= 0 && strings.HasPrefix(name, "DynamoDB_") {
    name = name[i+1:]
  }
  if name == "TransactWriteItems" {
    s.handleTransactWriteItems(w, r)
    return
  }
  if h, ok := s.resolveAction(name); ok {
    h(w, r)
    return
  }
  s.writeUnknownAction(w)
}

func (s *Service) writeUnknownAction(w http.ResponseWriter) {
  w.Header().Set("Content-Type", "application/x-amz-json-1.0")
  w.Header().Set("x-amzn-RequestId", txnRequestID())
  w.WriteHeader(http.StatusBadRequest)
  _, _ = io.WriteString(w, "{\"__type\":\"UnknownOperationException\"}")
}

func (s *Service) resolveAction(name string) (wrFn, bool) {
  if name == "" {
    return nil, false
  }
  if v, ok := s.resolved.Load(name); ok {
    if fn, ok2 := v.(wrFn); ok2 && fn != nil {
      return fn, true
    }
    return nil, false
  }
  fn, ok := s.lookupAction(name)
  if ok {
    s.resolved.Store(name, fn)
  }
  return fn, ok
}

func wbWrap(h wbFn) wrFn {
  return func(w http.ResponseWriter, r *http.Request) {
    var b []byte
    if r.Body != nil {
      b, _ = io.ReadAll(r.Body)
    }
    h(w, b)
  }
}

func wrErrWrap(h func(http.ResponseWriter, *http.Request) error) wrFn {
  return func(w http.ResponseWriter, r *http.Request) {
    if err := h(w, r); err != nil {
      if w.Header().Get("Content-Type") == "" {
        w.Header().Set("Content-Type", "application/x-amz-json-1.0")
        w.WriteHeader(http.StatusInternalServerError)
        _, _ = w.Write([]byte(ddbInternalJSON))
      }
    }
  }
}

func (s *Service) lookupAction(name string) (wrFn, bool) {
  switch name {
  case "CreateTable":
    if h, ok := any(s).(interface {
      handleCreateTable(http.ResponseWriter, *http.Request)
    }); ok {
      return h.handleCreateTable, true
    }
    if h, ok := any(s).(interface {
      handleCreateTable(http.ResponseWriter, []byte)
    }); ok {
      return wbWrap(h.handleCreateTable), true
    }
    if h, ok := any(s).(interface {
      createTable(http.ResponseWriter, *http.Request)
    }); ok {
      return h.createTable, true
    }
    if h, ok := any(s).(interface {
      createTable(http.ResponseWriter, []byte)
    }); ok {
      return wbWrap(h.createTable), true
    }
    if h, ok := any(s).(interface {
      CreateTable(http.ResponseWriter, *http.Request)
    }); ok {
      return h.CreateTable, true
    }
    if h, ok := any(s).(interface {
      CreateTable(http.ResponseWriter, []byte)
    }); ok {
      return wbWrap(h.CreateTable), true
    }
    if h, ok := any(s).(interface {
      handleCreateTable(http.ResponseWriter, *http.Request) error
    }); ok {
      return wrErrWrap(h.handleCreateTable), true
    }
    if h, ok := any(s).(interface {
      createTable(http.ResponseWriter, *http.Request) error
    }); ok {
      return wrErrWrap(h.createTable), true
    }
    if h, ok := any(s).(interface {
      CreateTable(http.ResponseWriter, *http.Request) error
    }); ok {
      return wrErrWrap(h.CreateTable), true
    }
  case "GetItem":
    if h, ok := any(s).(interface {
      handleGetItem(http.ResponseWriter, *http.Request)
    }); ok {
      return h.handleGetItem, true
    }
    if h, ok := any(s).(interface {
      handleGetItem(http.ResponseWriter, []byte)
    }); ok {
      return wbWrap(h.handleGetItem), true
    }
    if h, ok := any(s).(interface {
      getItem(http.ResponseWriter, *http.Request)
    }); ok {
      return h.getItem, true
    }
    if h, ok := any(s).(interface {
      getItem(http.ResponseWriter, []byte)
    }); ok {
      return wbWrap(h.getItem), true
    }
    if h, ok := any(s).(interface {
      GetItem(http.ResponseWriter, *http.Request)
    }); ok {
      return h.GetItem, true
    }
    if h, ok := any(s).(interface {
      GetItem(http.ResponseWriter, []byte)
    }); ok {
      return wbWrap(h.GetItem), true
    }
    if h, ok := any(s).(interface {
      handleGetItem(http.ResponseWriter, *http.Request) error
    }); ok {
      return wrErrWrap(h.handleGetItem), true
    }
    if h, ok := any(s).(interface {
      getItem(http.ResponseWriter, *http.Request) error
    }); ok {
      return wrErrWrap(h.getItem), true
    }
    if h, ok := any(s).(interface {
      GetItem(http.ResponseWriter, *http.Request) error
    }); ok {
      return wrErrWrap(h.GetItem), true
    }
  case "PutItem":
    if h, ok := any(s).(interface {
      handlePutItem(http.ResponseWriter, *http.Request)
    }); ok {
      return h.handlePutItem, true
    }
    if h, ok := any(s).(interface {
      handlePutItem(http.ResponseWriter, []byte)
    }); ok {
      return wbWrap(h.handlePutItem), true
    }
    if h, ok := any(s).(interface {
      putItem(http.ResponseWriter, *http.Request)
    }); ok {
      return h.putItem, true
    }
    if h, ok := any(s).(interface {
      putItem(http.ResponseWriter, []byte)
    }); ok {
      return wbWrap(h.putItem), true
    }
    if h, ok := any(s).(interface {
      PutItem(http.ResponseWriter, *http.Request)
    }); ok {
      return h.PutItem, true
    }
    if h, ok := any(s).(interface {
      PutItem(http.ResponseWriter, []byte)
    }); ok {
      return wbWrap(h.PutItem), true
    }
    if h, ok := any(s).(interface {
      handlePutItem(http.ResponseWriter, *http.Request) error
    }); ok {
      return wrErrWrap(h.handlePutItem), true
    }
    if h, ok := any(s).(interface {
      putItem(http.ResponseWriter, *http.Request) error
    }); ok {
      return wrErrWrap(h.putItem), true
    }
    if h, ok := any(s).(interface {
      PutItem(http.ResponseWriter, *http.Request) error
    }); ok {
      return wrErrWrap(h.PutItem), true
    }
  case "DeleteItem":
    if h, ok := any(s).(interface {
      handleDeleteItem(http.ResponseWriter, *http.Request)
    }); ok {
      return h.handleDeleteItem, true
    }
    if h, ok := any(s).(interface {
      handleDeleteItem(http.ResponseWriter, []byte)
    }); ok {
      return wbWrap(h.handleDeleteItem), true
    }
    if h, ok := any(s).(interface {
      deleteItem(http.ResponseWriter, *http.Request)
    }); ok {
      return h.deleteItem, true
    }
    if h, ok := any(s).(interface {
      deleteItem(http.ResponseWriter, []byte)
    }); ok {
      return wbWrap(h.deleteItem), true
    }
    if h, ok := any(s).(interface {
      DeleteItem(http.ResponseWriter, *http.Request)
    }); ok {
      return h.DeleteItem, true
    }
    if h, ok := any(s).(interface {
      DeleteItem(http.ResponseWriter, []byte)
    }); ok {
      return wbWrap(h.DeleteItem), true
    }
    if h, ok := any(s).(interface {
      handleDeleteItem(http.ResponseWriter, *http.Request) error
    }); ok {
      return wrErrWrap(h.handleDeleteItem), true
    }
    if h, ok := any(s).(interface {
      deleteItem(http.ResponseWriter, *http.Request) error
    }); ok {
      return wrErrWrap(h.deleteItem), true
    }
    if h, ok := any(s).(interface {
      DeleteItem(http.ResponseWriter, *http.Request) error
    }); ok {
      return wrErrWrap(h.DeleteItem), true
    }
  case "UpdateItem":
    if h, ok := any(s).(interface {
      handleUpdateItem(http.ResponseWriter, *http.Request)
    }); ok {
      return h.handleUpdateItem, true
    }
    if h, ok := any(s).(interface {
      handleUpdateItem(http.ResponseWriter, []byte)
    }); ok {
      return wbWrap(h.handleUpdateItem), true
    }
    if h, ok := any(s).(interface {
      updateItem(http.ResponseWriter, *http.Request)
    }); ok {
      return h.updateItem, true
    }
    if h, ok := any(s).(interface {
      updateItem(http.ResponseWriter, []byte)
    }); ok {
      return wbWrap(h.updateItem), true
    }
    if h, ok := any(s).(interface {
      UpdateItem(http.ResponseWriter, *http.Request)
    }); ok {
      return h.UpdateItem, true
    }
    if h, ok := any(s).(interface {
      UpdateItem(http.ResponseWriter, []byte)
    }); ok {
      return wbWrap(h.UpdateItem), true
    }
    if h, ok := any(s).(interface {
      handleUpdateItem(http.ResponseWriter, *http.Request) error
    }); ok {
      return wrErrWrap(h.handleUpdateItem), true
    }
    if h, ok := any(s).(interface {
      updateItem(http.ResponseWriter, *http.Request) error
    }); ok {
      return wrErrWrap(h.updateItem), true
    }
    if h, ok := any(s).(interface {
      UpdateItem(http.ResponseWriter, *http.Request) error
    }); ok {
      return wrErrWrap(h.UpdateItem), true
    }
  case "Query":
    if h, ok := any(s).(interface {
      handleQuery(http.ResponseWriter, *http.Request)
    }); ok {
      return h.handleQuery, true
    }
    if h, ok := any(s).(interface {
      handleQuery(http.ResponseWriter, []byte)
    }); ok {
      return wbWrap(h.handleQuery), true
    }
    if h, ok := any(s).(interface {
      query(http.ResponseWriter, *http.Request)
    }); ok {
      return h.query, true
    }
    if h, ok := any(s).(interface {
      query(http.ResponseWriter, []byte)
    }); ok {
      return wbWrap(h.query), true
    }
    if h, ok := any(s).(interface {
      Query(http.ResponseWriter, *http.Request)
    }); ok {
      return h.Query, true
    }
    if h, ok := any(s).(interface {
      Query(http.ResponseWriter, []byte)
    }); ok {
      return wbWrap(h.Query), true
    }
    if h, ok := any(s).(interface {
      handleQuery(http.ResponseWriter, *http.Request) error
    }); ok {
      return wrErrWrap(h.handleQuery), true
    }
    if h, ok := any(s).(interface {
      query(http.ResponseWriter, *http.Request) error
    }); ok {
      return wrErrWrap(h.query), true
    }
    if h, ok := any(s).(interface {
      Query(http.ResponseWriter, *http.Request) error
    }); ok {
      return wrErrWrap(h.Query), true
    }
  }
  return s.reflectAction(name)
}

func (s *Service) reflectAction(name string) (wrFn, bool) {
  t := reflect.TypeOf(s)
  if t == nil {
    return nil, false
  }
  wt := reflect.TypeOf((*http.ResponseWriter)(nil)).Elem()
  rt := reflect.TypeOf((*http.Request)(nil))
  bt := reflect.TypeOf([]byte(nil))
  for i := 0; i < t.NumMethod(); i++ {
    m := t.Method(i)
    base := m.Name
    for _, pfx := range []string{"handle", "Handle", "serve", "Serve", "do", "Do", "run", "Run"} {
      if strings.HasPrefix(base, pfx) && len(base) > len(pfx) {
        base = base[len(pfx):]
        break
      }
    }
    base = strings.TrimSuffix(base, "Handler")
    base = strings.TrimSuffix(base, "handler")
    if !strings.EqualFold(base, name) {
      continue
    }
    mt := m.Type
    if mt.NumIn() != 3 || mt.NumOut() != 0 {
      continue
    }
    idx := i
    if mt.In(1) == wt && mt.In(2) == rt {
      return func(w http.ResponseWriter, r *http.Request) {
        reflect.ValueOf(s).Method(idx).Call([]reflect.Value{reflect.ValueOf(w), reflect.ValueOf(r)})
      }, true
    }
    if mt.In(1) == wt && mt.In(2) == bt {
      return wbWrap(func(w http.ResponseWriter, b []byte) {
        reflect.ValueOf(s).Method(idx).Call([]reflect.Value{reflect.ValueOf(w), reflect.ValueOf(b)})
      }), true
    }
  }
  return nil, false
}

// invokeAction runs an operation handler in-process and returns the raw wire
// response. TransactWriteItems uses this to reuse PutItem / DeleteItem /
// UpdateItem / GetItem semantics so behavior stays identical to the
// individual operations (validation, expressions, error shapes).
func (s *Service) invokeAction(name string, body []byte) (int, []byte) {
  status, resp := 500, []byte(ddbInternalJSON)
  if h, ok := s.resolveAction(name); ok {
    req := httptest.NewRequest(http.MethodPost, "http://dynamodb.local/", bytes.NewReader(body))
    req.Header.Set("Content-Type", "application/x-amz-json-1.0")
    req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+name)
    req.ContentLength = int64(len(body))
    rec := httptest.NewRecorder()
    func() {
      defer func() {
        if r := recover(); r != nil {
          rec.Code = 500
          rec.Body.Reset()
          rec.Body.WriteString(ddbInternalJSON)
        }
      }()
      h(rec, req)
    }()
    return rec.Code, rec.Body.Bytes()
  }
  return status, resp
}
