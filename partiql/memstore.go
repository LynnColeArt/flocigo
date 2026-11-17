package partiql

import (
  "fmt"
  "sort"
  "sync"
)

// MemStore is the default in-memory TableStore: the runtime behind the local
// emulator and the PartiQL tests.
type MemStore struct {
  mu     sync.Mutex
  tables map[string]*memTable
}

type memTable struct {
  def   TableDef
  items []*memItem
  byKey map[string]*memItem
}

type memItem struct {
  key  string
  data map[string]*AttrValue
}

// NewMemStore creates an empty store.
func NewMemStore() *MemStore {
  return &MemStore{tables: map[string]*memTable{}}
}

func tableNotFound(table string) *Error {
  return errNotFound("table not found: %s", table)
}

func (s *MemStore) Schema(table string) (KeySchema, error) {
  s.mu.Lock()
  defer s.mu.Unlock()
  t, ok := s.tables[table]
  if !ok {
    return KeySchema{}, tableNotFound(table)
  }
  return KeySchema{Table: t.def.Table, Partition: t.def.Partition, Sort: t.def.Sort}, nil
}

func (s *MemStore) ScanItems(table string) ([]map[string]*AttrValue, error) {
  s.mu.Lock()
  defer s.mu.Unlock()
  t, ok := s.tables[table]
  if !ok {
    return nil, tableNotFound(table)
  }
  out := make([]map[string]*AttrValue, 0, len(t.items))
  for _, it := range t.items {
    out = append(out, cloneItem(it.data))
  }
  return out, nil
}

func (s *MemStore) PartitionItems(table string, pk *AttrValue) ([]map[string]*AttrValue, error) {
  s.mu.Lock()
  defer s.mu.Unlock()
  t, ok := s.tables[table]
  if !ok {
    return nil, tableNotFound(table)
  }
  type row struct {
    sk   *AttrValue
    item map[string]*AttrValue
  }
  rows := make([]row, 0, len(t.items))
  for _, it := range t.items {
    pvv, ok := it.data[t.def.Partition]
    if !ok || !EqualAttr(pvv, pk) {
      continue
    }
    var sk *AttrValue
    if t.def.Sort != "" {
      sk = it.data[t.def.Sort]
    }
    rows = append(rows, row{sk: sk, item: it.data})
  }
  sort.SliceStable(rows, func(i, j int) bool {
    return CompareKeyOrder(rows[i].sk, rows[j].sk) < 0
  })
  out := make([]map[string]*AttrValue, 0, len(rows))
  for _, r := range rows {
    out = append(out, cloneItem(r.item))
  }
  return out, nil
}

func (s *MemStore) GetItem(table string, key map[string]*AttrValue) (map[string]*AttrValue, bool, error) {
  s.mu.Lock()
  defer s.mu.Unlock()
  t, ok := s.tables[table]
  if !ok {
    return nil, false, tableNotFound(table)
  }
  ks, err := t.canonicalKey(key)
  if err != nil {
    return nil, false, err
  }
  it, ok := t.byKey[ks]
  if !ok {
    return nil, false, nil
  }
  return cloneItem(it.data), true, nil
}

func (s *MemStore) PutItem(table string, item map[string]*AttrValue, requireNew bool) error {
  s.mu.Lock()
  defer s.mu.Unlock()
  t, ok := s.tables[table]
  if !ok {
    return tableNotFound(table)
  }
  ks, err := t.canonicalKey(item)
  if err != nil {
    return err
  }
  prev, exists := t.byKey[ks]
  if requireNew && exists {
    return &Error{Code: CodeDuplicate, Msg: fmt.Sprintf("duplicate key exists: %s", describeKey(t.def, item))}
  }
  data := cloneItem(item)
  if exists {
    prev.data = data
    return nil
  }
  rec := &memItem{key: ks, data: data}
  t.items = append(t.items, rec)
  t.byKey[ks] = rec
  return nil
}

func (s *MemStore) DeleteItem(table string, key map[string]*AttrValue) error {
  s.mu.Lock()
  defer s.mu.Unlock()
  t, ok := s.tables[table]
  if !ok {
    return tableNotFound(table)
  }
  ks, err := t.canonicalKey(key)
  if err != nil {
    return err
  }
  it, ok := t.byKey[ks]
  if !ok {
    return nil
  }
  delete(t.byKey, ks)
  for i, x := range t.items {
    if x == it {
      t.items = append(t.items[:i], t.items[i+1:]...)
      break
    }
  }
  return nil
}

func (s *MemStore) CreateTable(def TableDef) error {
  s.mu.Lock()
  defer s.mu.Unlock()
  if _, exists := s.tables[def.Table]; exists {
    return &Error{Code: CodeInUse, Msg: fmt.Sprintf("table already exists: %s", def.Table)}
  }
  if def.Partition == "" {
    return errValidation("CREATE TABLE requires a HASH primary key")
  }
  if def.Sort != "" && def.Sort == def.Partition {
    return errValidation("the range key must differ from the partition key")
  }
  s.tables[def.Table] = &memTable{def: def, byKey: map[string]*memItem{}}
  return nil
}

func (s *MemStore) DropTable(table string) error {
  s.mu.Lock()
  defer s.mu.Unlock()
  if _, ok := s.tables[table]; !ok {
    return tableNotFound(table)
  }
  delete(s.tables, table)
  return nil
}

// canonicalKey builds the composite key string from pk/sk attribute values.
func (t *memTable) canonicalKey(values map[string]*AttrValue) (string, error) {
  pv, ok := values[t.def.Partition]
  if !ok || pv == nil {
    return "", errValidation("missing partition key attribute %q", t.def.Partition)
  }
  if !isKeyScalar(pv) {
    return "", errValidation("partition key %q must be a string, number, or binary value", t.def.Partition)
  }
  ks := CanonicalValue(pv)
  if t.def.Sort != "" {
    sv, ok := values[t.def.Sort]
    if !ok || sv == nil {
      return "", errValidation("missing sort key attribute %q", t.def.Sort)
    }
    if !isKeyScalar(sv) {
      return "", errValidation("sort key %q must be a string, number, or binary value", t.def.Sort)
    }
    ks += "\x00" + CanonicalValue(sv)
  }
  return ks, nil
}

func describeKey(def TableDef, item map[string]*AttrValue) string {
  out := "{" + escapeAttr(def.Partition) + ":" + keyJSON(item[def.Partition])
  if def.Sort != "" {
    out += "," + escapeAttr(def.Sort) + ":" + keyJSON(item[def.Sort])
  }
  return out + "}"
}

func escapeAttr(name string) string {
  return fmt.Sprintf("%q", name)
}

func keyJSON(v *AttrValue) string {
  if v == nil {
    return "null"
  }
  switch v.Type {
  case "S":
    return fmt.Sprintf("%q", v.S)
  case "N":
    return v.N
  case "B":
    return fmt.Sprintf("\"%x\"", v.B)
  }
  return v.Debug()
}
