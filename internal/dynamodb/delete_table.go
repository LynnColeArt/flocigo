// DeleteTable plus the shared wire error and TableDescription plumbing used
// by the table actions in this package.
package dynamodb

import (
	"encoding/json"
	"reflect"
	"time"
)

// Wire error types that the DynamoDB JSON protocol uses for the failures the
// table actions can raise.
const (
	typeResourceNotFoundException = "com.amazon.coral.service#ResourceNotFoundException"
	typeSerializationException    = "com.amazon.coral.service#SerializationException"
	typeValidationException       = "com.amazon.coral.validate#ValidationException"
	typeInternalFailure           = "com.amazon.coral.service#InternalFailure"
)

// localTableARNPrefix mirrors how DynamoDB Local renders table ARNs.
const localTableARNPrefix = "arn:aws:dynamodb:ddblocal:000000000000:table/"

// opError is an action level error that the transport layer renders as a
// DynamoDB JSON protocol error document.
type opError struct {
	// StatusCode is the HTTP status the transport must reply with.
	StatusCode int
	// Type is the fully qualified wire type carried in the __type member.
	Type string
	// Message is the human readable detail that accompanies __type.
	Message string
}

func (e *opError) Error() string { return e.Type + ": " + e.Message }

// opErrorBody encodes the JSON error document for the wire protocol.
func opErrorBody(e *opError) []byte {
	raw, _ := json.Marshal(struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}{Type: e.Type, Message: e.Message})
	return raw
}

// errReply maps an action error onto the (status, body) pair the transport
// layer needs to answer a failed request.
func errReply(err error) (int, []byte) {
	if e, ok := err.(*opError); ok {
		return e.StatusCode, opErrorBody(e)
	}
	return 400, opErrorBody(&opError{
		StatusCode: 400,
		Type:       typeInternalFailure,
		Message:    err.Error(),
	})
}

// DeleteTableInput mirrors the DynamoDB DeleteTable request structure.
type DeleteTableInput struct {
	TableName string
}

// DeleteTableOutput mirrors the DynamoDB DeleteTable response structure.
type DeleteTableOutput struct {
	TableDescription map[string]any `json:"TableDescription,omitempty"`
}

// DeleteTable removes the named table from the store and returns its final
// description, just like DynamoDB. Removing an unknown table fails with
// ResourceNotFoundException.
func DeleteTable(s *Store, in DeleteTableInput) (DeleteTableOutput, error) {
	if err := checkTableNameShape(in.TableName, "tableName"); err != nil {
		return DeleteTableOutput{}, err
	}
	table, found := s.removeTable(in.TableName)
	if !found {
		return DeleteTableOutput{}, &opError{
			StatusCode: 400,
			Type:       typeResourceNotFoundException,
			Message:    "Requested resource not found: Table: " + in.TableName + " not found",
		}
	}
	if stored, ok := storedDescription(table); ok {
		return DeleteTableOutput{TableDescription: copyDescription(stored)}, nil
	}
	return DeleteTableOutput{TableDescription: rebuiltDescription(table)}, nil
}

// storedDescription returns the TableDescription the table answered
// CreateTable with, when the store keeps such a snapshot. DeleteTable must
// echo exactly the metadata DynamoDB produced for the table, so the stored
// description is preferred whenever it exists.
func storedDescription(t *Table) (map[string]any, bool) {
	if t == nil {
		return nil, false
	}
	v := reflect.Indirect(reflect.ValueOf(t))
	if !v.IsValid() {
		return nil, false
	}
	for _, field := range []string{"Description", "TableDescription"} {
		f := v.FieldByName(field)
		if !f.IsValid() || !f.CanInterface() || f.Kind() != reflect.Map {
			continue
		}
		if m, ok := f.Interface().(map[string]any); ok && m != nil {
			return m, true
		}
	}
	return nil, false
}

// rebuiltDescription reconstructs the TableDescription from the table record
// for stores that do not persist the CreateTable snapshot. At this point of
// the emulation tables hold no items, so ItemCount and TableSizeBytes are
// both 0 and every table reports status ACTIVE.
func rebuiltDescription(t *Table) map[string]any {
	if t == nil {
		return map[string]any{}
	}
	return map[string]any{
		"AttributeDefinitions": t.AttributeDefinitions,
		"CreationDateTime":     epochSeconds(t.CreationDateTime),
		"ItemCount":            0,
		"KeySchema":            t.KeySchema,
		"TableArn":             localTableARNPrefix + t.Name,
		"TableName":            t.Name,
		"TableSizeBytes":       0,
		"TableStatus":          "ACTIVE",
	}
}

// epochSeconds renders the creation timestamp the way DynamoDB writes
// CreationDateTime on the wire: seconds since the epoch, with fractional
// precision.
func epochSeconds(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case time.Time:
		return float64(t.UnixNano()) / float64(time.Second)
	case int64:
		return float64(t)
	}
	return 0
}

// copyDescription clones a description so that responses can never alias
// stored state.
func copyDescription(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// snapshotTableNames returns a fresh copy of every table name in the store so
// that callers can sort and page the names without racing against concurrent
// writes.
func (s *Store) snapshotTableNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.tables))
	for name := range s.tables {
		names = append(names, name)
	}
	return names
}

// removeTable deletes the named table from the store and returns the removed
// record so that the caller can build the DeleteTable response.
func (s *Store) removeTable(name string) (*Table, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	table, ok := s.tables[name]
	if !ok {
		return nil, false
	}
	delete(s.tables, name)
	return table, true
}

// ServeDeleteTable processes a raw DeleteTable request body and returns the
// HTTP status code plus the JSON response.
func ServeDeleteTable(s *Store, body []byte) (int, []byte) {
	var in DeleteTableInput
	if err := json.Unmarshal(body, &in); err != nil {
		raw, _ := json.Marshal(map[string]string{
			"__type":  typeSerializationException,
			"Message": "Failed to serialize the request.",
		})
		return 400, raw
	}
	out, err := DeleteTable(s, in)
	if err != nil {
		return errReply(err)
	}
	resp, _ := json.Marshal(out)
	return 200, resp
}
