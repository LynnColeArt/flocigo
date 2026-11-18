package ddb

import (
	"sort"
	"strconv"
	"sync"
)

// entry is a partition-scoped item with its pk/sk values.
type entry struct {
	pk, sk map[string]any
	item   map[string]any
}

// IndexDef describes a local or global secondary index.
type IndexDef struct {
	Name      string
	PK, SK    string
	LSI       bool
	ProjType  string
	ProjAttrs []string
}

// Table holds the stored items of one table plus its indexes.
type Table struct {
	Name           string
	AttrDefs       map[string]string
	PK, SK         string
	LSIs           map[string]*IndexDef
	GSIs           map[string]*IndexDef
	BillingMode    string
	RCU, WCU       int64
	StreamEnabled  bool
	StreamViewType string
	Created        float64
	TableID        string
	Items          map[string]map[string]any
	IndexItems     map[string]map[string]map[string]any
}

// Store is the backing state: table name to table.
type Store struct {
	mu     sync.RWMutex
	tables map[string]*Table
}

func newStore() *Store {
	return &Store{tables: make(map[string]*Table)}
}

// GetTable returns a table by name.
func (st *Store) GetTable(name string) (*Table, bool) {
	t, ok := st.tables[name]
	return t, ok
}

// TableExists reports whether a table exists.
func (st *Store) TableExists(name string) bool {
	_, ok := st.tables[name]
	return ok
}

// keyIDOf builds a unique, typed identifier for a key attribute value.
func keyIDOf(a map[string]any) string {
	switch t := attrTypeOf(a); t {
	case "S":
		v := attrString(a)
		return "S" + strconv.Itoa(len(v)) + ":" + v
	case "N":
		if r, ok := parseNumber(attrString(a)); ok {
			return "N" + r.RatString()
		}
		return "N!" + attrString(a)
	case "B":
		b := attrBytes(a)
		return "B" + strconv.Itoa(len(b)) + ":" + string(b)
	default:
		return "X"
	}
}

// storageKeyFor computes the storage key of an item for the base table.
func storageKeyFor(pk, sk string, item map[string]any) string {
	id := keyIDOf(item[pk])
	if sk != "" {
		id += "\x00" + keyIDOf(item[sk])
	}
	return id
}

// indexStorageKey computes the storage key of an item for an index.
// It reports false for sparse entries (missing index key attributes).
func indexStorageKey(idx *IndexDef, item map[string]any) (string, bool) {
	 pkv, ok := item[idx.PK]
	if !ok {
		return "", false
	}
	id := keyIDOf(pkv)
	if idx.SK != "" {
		skv, ok2 := item[idx.SK]
		if !ok2 {
			return "", false
		}
		id += "\x00" + keyIDOf(skv)
	}
	return id, true
}

func (t *Table) indexMap(name string) map[string]map[string]any {
	if t.IndexItems == nil {
		t.IndexItems = map[string]map[string]map[string]any{}
	}
	m, ok := t.IndexItems[name]
	if !ok {
		m = map[string]map[string]any{}
		t.IndexItems[name] = m
	}
	return m
}

func (t *Table) addIndexEntries(item map[string]any) {
	for _, idx := range t.GSIs {
		if k, ok := indexStorageKey(idx, item); ok {
			t.indexMap(idx.Name)[k] = item
		}
	}
	for _, idx := range t.LSIs {
		if k, ok := indexStorageKey(idx, item); ok {
			t.indexMap(idx.Name)[k] = item
		}
	}
}

func (t *Table) removeIndexEntries(item map[string]any) {
	for _, idx := range t.GSIs {
		if k, ok := indexStorageKey(idx, item); ok {
			if m, ok2 := t.IndexItems[idx.Name]; ok2 {
				delete(m, k)
			}
		}
	}
	for _, idx := range t.LSIs {
		if k, ok := indexStorageKey(idx, item); ok {
			if m, ok2 := t.IndexItems[idx.Name]; ok2 {
				delete(m, k)
			}
		}
	}
}

// putItem stores an item, returning the previous item if one was replaced.
func (t *Table) putItem(item map[string]any) (map[string]any, bool) {
	id := storageKeyFor(t.PK, t.SK, item)
	prev, replaced := t.Items[id]
	if replaced {
		t.removeIndexEntries(prev)
	}
	t.Items[id] = item
	t.addIndexEntries(item)
	return prev, replaced
}

// getItem returns the item stored under the given key.
func (t *Table) getItem(key map[string]any) (map[string]any, bool) {
	id := storageKeyFor(t.PK, t.SK, key)
	it, ok := t.Items[id]
	return it, ok
}

// deleteItem removes an item, returning the deleted item if it existed.
func (t *Table) deleteItem(key map[string]any) (map[string]any, bool) {
	id := storageKeyFor(t.PK, t.SK, key)
	prev, ok := t.Items[id]
	if !ok {
		return nil, false
	}
	delete(t.Items, id)
	t.removeIndexEntries(prev)
	return prev, true
}

// partitionEntries returns the items of one partition, sorted by sort key.
func (t *Table) partitionEntries(idxName string, pk map[string]any) []entry {
	ipk, isk := t.PK, t.SK
	var src map[string]map[string]any
	if idxName == "" {
		src = t.Items
	} else {
		idx := t.GSIs[idxName]
		if idx == nil {
			idx = t.LSIs[idxName]
		}
		if idx == nil {
			return nil
		}
		ipk, isk = idx.PK, idx.SK
		m, ok := t.IndexItems[idxName]
		if !ok {
			return nil
		}
		src = m
	}
	pkID := keyIDOf(pk)
	out := make([]entry, 0, 8)
	for _, item := range src {
		if keyIDOf(item[ipk]) != pkID {
			continue
		}
		e := entry{pk: item[ipk], item: item}
		if isk != "" {
			e.sk = item[isk]
		}
		out = append(out, e)
	}
	if isk != "" {
		sort.SliceStable(out, func(i, j int) bool {
			return compareAttr(out[i].sk, out[j].sk) < 0
		})
	}
	return out
}

// validKeyValue validates a key attribute value.
func validKeyValue(a map[string]any, name string) *apiErr {
	switch attrTypeOf(a) {
	case "S":
		if attrString(a) == "" {
			return ddbErrorf("ValidationException", "One or more parameter values were invalid: The AttributeValue for a key attribute cannot contain an empty string value. Key: %s", name)
		}
	case "N":
		if _, ok := parseNumber(attrString(a)); !ok {
			return ddbError("ValidationException", "The provided key element does not match the schema")
		}
	}
	return nil
}

// itemKeyFromAttrs extracts & validates the primary key of an item.
func itemKeyFromAttrs(t *Table, item map[string]any) (map[string]any, *apiErr) {
	key := map[string]any{}
	names := []string{t.PK}
	if t.SK != "" {
		names = append(names, t.SK)
	}
	for _, name := range names {
		v, ok := item[name]
		if !ok {
			return nil, ddbErrorf("ValidationException", "One or more parameter values were invalid: Missing the key %s in the item", name)
		}
		a, ok2 := toAttr(v)
		if !ok2 || attrTypeOf(a) != t.AttrDefs[name] {
			return nil, ddbError("ValidationException", "The provided key element does not match the schema")
		}
		if aerr := validKeyValue(a, name); aerr != nil {
			return nil, aerr
		}
		key[name] = a
	}
	return key, nil
}

// itemKeyFromKeyParam extracts & validates a Key request parameter.
func itemKeyFromKeyParam(t *Table, m map[string]any) (map[string]any, *apiErr) {
	if m == nil {
		m = map[string]any{}
	}
	key, aerr := itemKeyFromAttrs(t, m)
	if aerr != nil {
		return nil, aerr
	}
	allowed := 1
	if t.SK != "" {
		allowed = 2
	}
	if len(m) > allowed {
		return nil, ddbError("ValidationException", "The provided key element does not match the schema")
	}
	return key, nil
}

// checkIndexKeyTypes validates index key attribute types when present.
func checkIndexKeyTypes(t *Table, item map[string]any) *apiErr {
	for _, idx := range t.GSIs {
		if aerr := checkOneIndexKey(t, idx, item); aerr != nil {
			return aerr
		}
	}
	for _, idx := range t.LSIs {
		if aerr := checkOneIndexKey(t, idx, item); aerr != nil {
			return aerr
		}
	}
	return nil
}

func checkOneIndexKey(t *Table, idx *IndexDef, item map[string]any) *apiErr {
	for _, name := range []string{idx.PK, idx.SK} {
		if name == "" {
			continue
		}
		v, ok := item[name]
		if !ok {
			continue
		}
		a, ok2 := toAttr(v)
		if !ok2 || attrTypeOf(a) != t.AttrDefs[name] {
			expected := t.AttrDefs[name]
			actual := ""
			if ok2 {
				actual = attrTypeOf(a)
			}
			if actual == "" || expected == "" {
				return ddbError("ValidationException", "The provided key element does not match the schema")
			}
			return ddbErrorf("ValidationException", "One or more parameter values were invalid: Type mismatch for Index Key %s Expected: %s Actual: %s Index: %s", name, expected, actual, t.Name)
		}
	}
	return nil
}
