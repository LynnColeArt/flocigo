package tagging

import (
	"sort"
	"strings"
	"sync"
)

// Resolver reports resource existence to the tag store. The DynamoDB engine
// in floci-go adapts its live table metadata to this interface: for index
// lookups the parent table name is paired with the index name, and the
// adapter must report existence only when the index is attached to an
// existing table.
type Resolver interface {
	Exists(table, index string) bool
}

// MapResolver is an in-memory Resolver over a table/index name set. It is
// used by tests and serves as a reference for engine-side adapters.
type MapResolver struct {
	mu     sync.RWMutex
	tables map[string]map[string]bool
}

// NewMapResolver returns an empty MapResolver.
func NewMapResolver() *MapResolver {
	return &MapResolver{tables: make(map[string]map[string]bool)}
}

// AddTable registers a table.
func (r *MapResolver) AddTable(table string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tables[table] == nil {
		r.tables[table] = make(map[string]bool)
	}
}

// AddIndex registers an index of a table, registering the table too.
func (r *MapResolver) AddIndex(table, index string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tables[table] == nil {
		r.tables[table] = make(map[string]bool)
	}
	r.tables[table][index] = true
}

// RemoveTable forgets a table together with all of its indexes.
func (r *MapResolver) RemoveTable(table string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.tables, table)
}

// RemoveIndex forgets a single index of a table.
func (r *MapResolver) RemoveIndex(table, index string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if idxs, ok := r.tables[table]; ok {
		delete(idxs, index)
	}
}

// Exists implements Resolver.
func (r *MapResolver) Exists(table, index string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if index == "" {
		_, ok := r.tables[table]
		return ok
	}
	return r.tables[table][index]
}

// Store holds the ARN-keyed tag metadata for tables and indexes. It is safe
// for concurrent use.
type Store struct {
	res Resolver

	mu   sync.RWMutex
	tags map[string]map[string]string // canonical ARN -> tag key -> value
}

// NewStore returns an empty tag store bound to res.
func NewStore(res Resolver) *Store {
	return &Store{res: res, tags: make(map[string]map[string]string)}
}

// TagResource attaches or overwrites tags on an existing table or index,
// keyed by the resource ARN.
// Error paths: InvalidArn, ValidationException, ResourceNotFoundException.
func (s *Store) TagResource(resourceArn string, tags []Tag) error {
	a, err := ParseARN(resourceArn)
	if err != nil {
		return err
	}
	if err := validateTags(tags); err != nil {
		return err
	}
	return s.put(a, tags)
}

// AttachTagsOnCreate stores the tags supplied with CreateTable. It validates
// the ARN and the tag set but skips the resource-existence check, so it can
// be called at any point of the CreateTable flow.
func (s *Store) AttachTagsOnCreate(resourceArn string, tags []Tag) error {
	if len(tags) == 0 {
		return nil
	}
	a, err := ParseARN(resourceArn)
	if err != nil {
		return err
	}
	if err := validateTags(tags); err != nil {
		return err
	}
	s.write(a, tags)
	return nil
}

// ListTagsOfResource returns the tags of an existing table or index, sorted
// by key so responses are deterministic. Untagged resources yield an empty
// (non-nil) slice. Error paths: InvalidArn, ResourceNotFoundException.
func (s *Store) ListTagsOfResource(resourceArn string) ([]Tag, error) {
	a, err := ParseARN(resourceArn)
	if err != nil {
		return nil, err
	}
	if !s.res.Exists(a.Table, a.Index) {
		return nil, notFound(a)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Tag, 0)
	for key, value := range s.tags[a.String()] {
		out = append(out, Tag{Key: key, Value: value})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// UntagResource removes the given tag keys from an existing table or index.
// Keys that are not present are ignored.
// Error paths: InvalidArn, ValidationException, ResourceNotFoundException.
func (s *Store) UntagResource(resourceArn string, keys []string) error {
	a, err := ParseARN(resourceArn)
	if err != nil {
		return err
	}
	if err := validateTagKeys(keys); err != nil {
		return err
	}
	if !s.res.Exists(a.Table, a.Index) {
		return notFound(a)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	arnKey := a.String()
	if m := s.tags[arnKey]; len(m) > 0 {
		for _, key := range keys {
			delete(m, key)
		}
		if len(m) == 0 {
			delete(s.tags, arnKey)
		}
	}
	return nil
}

// DropResource removes all tag metadata for a table and every index under
// it. The engine calls it from DeleteTable; unknown or malformed ARNs are
// ignored so it can be invoked unconditionally.
func (s *Store) DropResource(resourceArn string) {
	a, err := ParseARN(resourceArn)
	if err != nil || a.Kind != KindTable {
		return
	}
	prefix := a.String() + "/index/"
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tags, a.String())
	for key := range s.tags {
		if strings.HasPrefix(key, prefix) {
			delete(s.tags, key)
		}
	}
}

// put checks existence and then writes tags for a.
func (s *Store) put(a ARN, tags []Tag) error {
	if !s.res.Exists(a.Table, a.Index) {
		return notFound(a)
	}
	s.write(a, tags)
	return nil
}

// write stores or merges tags; tags must already be validated.
func (s *Store) write(a ARN, tags []Tag) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := a.String()
	dst := s.tags[key]
	if dst == nil {
		dst = make(map[string]string, len(tags))
	}
	for _, t := range tags {
		dst[t.Key] = t.Value
	}
	s.tags[key] = dst
}

func notFound(a ARN) *Error {
	if a.Kind == KindIndex {
		return errf(ErrResourceNotFound, "Requested resource not found: Table: %s/index/%s not found", a.Table, a.Index)
	}
	return errf(ErrResourceNotFound, "Requested resource not found: Table: %s not found", a.Table)
}
