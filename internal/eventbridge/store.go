package eventbridge

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// busRec is one event bus together with its rules table.
type busRec struct {
	Name     string              `json:"name"`
	Arn      string              `json:"arn"`
	Created  int64               `json:"created,omitempty"`
	Modified int64               `json:"modified,omitempty"`
	Rules    map[string]*ruleRec `json:"rules,omitempty"`
}

// ruleRec is a single rule owned by an event bus.
type ruleRec struct {
	Name               string `json:"name"`
	Arn                string `json:"arn"`
	EventBusName       string `json:"eventBusName,omitempty"`
	EventPattern       string `json:"eventPattern,omitempty"`
	ScheduleExpression string `json:"scheduleExpression,omitempty"`
	State              string `json:"state"`
	Description        string `json:"description,omitempty"`
	RoleArn            string `json:"roleArn,omitempty"`
	ManagedBy          string `json:"managedBy,omitempty"`
	Created            int64  `json:"created,omitempty"`
	Modified           int64  `json:"modified,omitempty"`
}

// storeFile is the on-disk layout in the floci store file: a bus-name
// keyed index where each event bus holds its own rules table.
type storeFile struct {
	Version int                `json:"version"`
	Buses   map[string]*busRec `json:"buses"`
}

// resolveStorePath accepts either a store file path or a directory; a
// directory binds to eventbridge.json inside it.
func resolveStorePath(path string) string {
	if path == "" {
		return ""
	}
	if strings.HasSuffix(path, string(os.PathSeparator)) {
		return filepath.Join(path, "eventbridge.json")
	}
	if fi, err := os.Stat(path); err == nil && fi.IsDir() {
		return filepath.Join(path, "eventbridge.json")
	}
	return path
}

// load reads the store file into memory. Callers must hold s.mu.
func (s *Service) load() {
	s.buses = map[string]*busRec{}
	if s.StorePath == "" {
		return
	}
	raw, err := os.ReadFile(s.StorePath)
	if err != nil {
		return
	}
	var file storeFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return
	}
	if file.Buses != nil {
		s.buses = file.Buses
	}
}

// ensureState lazily materializes the default event bus and repairs
// legacy records missing their rules table. Callers must hold s.mu.
func (s *Service) ensureState() {
	if s.buses == nil {
		s.buses = map[string]*busRec{}
	}
	for _, rec := range s.buses {
		if rec.Rules == nil {
			rec.Rules = map[string]*ruleRec{}
		}
	}
	if _, ok := s.buses[defaultBusName]; !ok {
		now := time.Now().Unix()
		s.buses[defaultBusName] = &busRec{
			Name:     defaultBusName,
			Arn:      s.busARN(defaultBusName),
			Created:  now,
			Modified: now,
			Rules:    map[string]*ruleRec{},
		}
	}
}

// save atomically rewrites the store file. Callers must hold s.mu.
func (s *Service) save() {
	if s.StorePath == "" {
		return
	}
	raw, err := json.Marshal(storeFile{Version: 1, Buses: s.buses})
	if err != nil {
		return
	}
	dir := filepath.Dir(s.StorePath)
	if dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0o755)
	}
	tmp := s.StorePath + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return
	}
	_ = os.Rename(tmp, s.StorePath)
}
