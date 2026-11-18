// Package sqs implements the floci-go SQS API: the AWS SQS query
// protocol wire format (form-encoded requests, XML responses) and the
// queue actions. Queues are persisted as one JSON file per queue
// inside a data directory.
package sqs

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const queueFileExt = ".json"

// Queue is the persisted state of a single SQS queue.
type Queue struct {
	Name         string            `json:"name"`
	Arn          string            `json:"arn,omitempty"`
	AccountID    string            `json:"accountId,omitempty"`
	Region       string            `json:"region,omitempty"`
	Attributes   map[string]string `json:"attributes"`
	Tags         map[string]string `json:"tags,omitempty"`
	CreatedAt    time.Time         `json:"createdAt"`
	LastModified time.Time         `json:"lastModified"`
}

// QueueArn builds the ARN of a queue.
func QueueArn(region, accountID, name string) string {
	return fmt.Sprintf("arn:aws:sqs:%s:%s:%s", region, accountID, name)
}

// Store is the file-backed queue store: one JSON file per queue, named
// after the queue, inside a single data directory. It is safe for
// concurrent use.
type Store struct {
	dir string
	mu  sync.Mutex
}

// NewStore opens (creating it when missing) the queue data directory.
func NewStore(dir string) (*Store, error) {
	if dir == "" {
		return nil, fmt.Errorf("floci-go: queue data directory is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("floci-go: create queue data directory %q: %w", dir, err)
	}
	return &Store{dir: dir}, nil
}

// Dir returns the data directory backing the store.
func (s *Store) Dir() string { return s.dir }

// Path returns the file backing a queue. Callers must pass validated
// queue names; the storage-side checks live in path.
func (s *Store) Path(name string) string {
	return filepath.Join(s.dir, name+queueFileExt)
}

// GetOrCreate returns the persisted queue for q.Name, creating and
// saving q when no queue file exists yet. The boolean reports whether
// a new queue file was created. The existence check and the create
// share one lock, so concurrent creates for the same queue name are
// serialized.
func (s *Store) GetOrCreate(q *Queue) (*Queue, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok, err := s.load(q.Name); err != nil {
		return nil, false, err
	} else if ok {
		return existing, false, nil
	}
	if err := s.save(q); err != nil {
		return nil, false, err
	}
	return q, true, nil
}

// Load reads a queue by name; ok is false when no queue file exists.
func (s *Store) Load(name string) (*Queue, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.load(name)
}

// Save rewrites a queue file atomically and bumps LastModified.
func (s *Store) Save(q *Queue) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.save(q)
}

// List returns every queue in the data directory, in file name order.
func (s *Store) List() ([]*Queue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("floci-go: read queue directory %q: %w", s.dir, err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), queueFileExt) {
			continue
		}
		names = append(names, strings.TrimSuffix(entry.Name(), queueFileExt))
	}
	sort.Strings(names)

	queues := make([]*Queue, 0, len(names))
	for _, name := range names {
		q, ok, err := s.load(name)
		if err != nil {
			return nil, err
		}
		if ok {
			queues = append(queues, q)
		}
	}
	return queues, nil
}

func (s *Store) path(name string) (string, error) {
	if !isQueueFileName(name) {
		return "", fmt.Errorf("floci-go: invalid queue file name %q", name)
	}
	return filepath.Join(s.dir, name+queueFileExt), nil
}

// isQueueFileName is the storage-side backstop for queue names that
// the API layer already validated: letters, digits, hyphens and
// underscores (dots arrive via the ".fifo" suffix).
func isQueueFileName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if strings.ContainsAny(name, `/\`) {
		return false
	}
	for _, r := range name {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z',
			r == '-', r == '_':
		case r == '.':
		default:
			return false
		}
	}
	return true
}

func (s *Store) load(name string) (*Queue, bool, error) {
	p, err := s.path(name)
	if err != nil {
		return nil, false, err
	}
	b, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("floci-go: read queue file %q: %w", p, err)
	}
	var q Queue
	if err := json.Unmarshal(b, &q); err != nil {
		return nil, false, fmt.Errorf("floci-go: decode queue file %q: %w", p, err)
	}
	if q.Name == "" {
		q.Name = name
	}
	return &q, true, nil
}

func (s *Store) save(q *Queue) error {
	if q.Name == "" {
		return fmt.Errorf("floci-go: queue name is required")
	}
	p, err := s.path(q.Name)
	if err != nil {
		return err
	}
	q.LastModified = time.Now().UTC()

	b, err := json.MarshalIndent(q, "", "  ")
	if err != nil {
		return fmt.Errorf("floci-go: encode queue %q: %w", q.Name, err)
	}
	b = append(b, '\n')

	// Write to a side file and rename it over the queue file, so an
	// interrupted write never leaves a partial queue file behind.
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return fmt.Errorf("floci-go: write queue file %q: %w", tmp, err)
	}
	if err := os.Rename(tmp, p); err != nil {
		return fmt.Errorf("floci-go: replace queue file %q: %w", p, err)
	}
	return nil
}
