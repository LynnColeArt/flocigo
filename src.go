package main

// DynamoDB_20120810.UpdateTable for floci-go.
//
// Local semantics (per ticket flocigo/UpdateTable):
//   - read the table's metadata; ResourceNotFoundException when the table does
//     not exist,
//   - change the billing mode (PAY_PER_REQUEST <-> PROVISIONED),
//   - update GSIs: add (Create), modify (Update: provisioned throughput),
//     delete (Delete),
//   - LSIs have no post-create deltas in DynamoDB, so the stored
//     LocalSecondaryIndexes are carried through unchanged into the returned
//     TableDescription (and new GSIs may not collide with LSI names),
//   - merge AttributeDefinitions, persist the deltas and return the updated
//     TableDescription.
//
// Error paths: ResourceNotFoundException (unknown table), LimitExceededException
// (more than 20 GSIs / throughput above 40000 units) plus ValidationException
// for malformed deltas. Message strings follow AWS / DynamoDB-Local formats
// where the ticket leaves them unpinned they are close approximations.
//
// Everything is expressed as plain JSON-shaped maps (the floci wire model) so
// the code plugs directly into the Go metadata store used by the other ops.

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

// UpdateTableAPITarget is the Smithy target id served by this implementation.
const UpdateTableAPITarget = "DynamoDB_20120810.UpdateTable"

const (
	utModeProvisioned   = "PROVISIONED"
	utModePayPerRequest = "PAY_PER_REQUEST"
	utStatusActive      = "ACTIVE"
	utKeyHash           = "HASH"
	utKeyRange          = "RANGE"

	utMaxGlobalSecondaryIndexes = 20
	utMaxProvisionedUnits       = 40000

	utDefaultRegion  = "us-east-1"
	utDefaultAccount = "000000000000"

	utWireErrorPrefix = "com.amazonaws.dynamodb.v20120810#"
)

// UpdateTableError is a DynamoDB API error produced by UpdateTable.
type UpdateTableError struct {
	Code    string // ResourceNotFoundException | LimitExceededException | ValidationException | ...
	Message string
}

func (e *UpdateTableError) Error() string { return e.Code + ": " + e.Message }

func utInvalid(format string, args ...any) *UpdateTableError {
	return &UpdateTableError{Code: "ValidationException", Message: fmt.Sprintf(format, args...)}
}

func utLimit(format string, args ...any) *UpdateTableError {
	return &UpdateTableError{Code: "LimitExceededException", Message: fmt.Sprintf(format, args...)}
}

func utTableNotFound(name string) *UpdateTableError {
	return &UpdateTableError{
		Code:    "ResourceNotFoundException",
		Message: "Requested resource not found: Table: " + name + " not found",
	}
}

// UpdateTableStore abstracts floci-go's table metadata storage. Stored metadata
// is a TableDescription-shaped map (plus internal "__"-prefixed bookkeeping keys).
type UpdateTableStore interface {
	GetUpdateTableMeta(tableName string) (map[string]any, bool)
	PutUpdateTableMeta(tableName string, meta map[string]any)
}

// MemUpdateTableStore is a thread-safe in-memory UpdateTableStore.
type MemUpdateTableStore struct {
	mu     sync.RWMutex
	tables map[string]map[string]any
}

func NewMemUpdateTableStore() *MemUpdateTableStore {
	return &MemUpdateTableStore{tables: map[string]map[string]any{}}
}

func (s *MemUpdateTableStore) GetUpdateTableMeta(name string) (map[string]any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tables[name]
	return t, ok
}

func (s *MemUpdateTableStore) PutUpdateTableMeta(name string, meta map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tables[name] = meta
}

// DefaultUpdateTableStore is used by UpdateTable and HandleUpdateTableWire.
var DefaultUpdateTableStore UpdateTableStore = NewMemUpdateTableStore()

// SeedUpdateTable installs a baseline TableDescription-shaped metadata map so
// tests (and bootstrap code) can create tables without going through CreateTable.
func SeedUpdateTable(name string, meta map[string]any) {
	DefaultUpdateTableStore.PutUpdateTableMeta(name, utCopyMap(meta))
}

var updateTableMu sync.Mutex // serializes read-modify-write cycles

// UpdateTable reads the table metadata, applies the UpdateTable deltas, persists
// the result and returns the updated TableDescription. On error the store is
// left untouched.
func UpdateTable(req map[string]any) (map[string]any, error) {
	desc, aerr := ApplyUpdateTable(DefaultUpdateTableStore, req)
	if aerr != nil {
		return nil, aerr
	}
	return desc, nil
}

// ApplyUpdateTable performs the whole UpdateTable operation against the store.
func ApplyUpdateTable(store UpdateTableStore, req map[string]any) (map[string]any, *UpdateTableError) {
	if store == nil {
		store = DefaultUpdateTableStore
	}
	if req == nil {
		return nil, utInvalid("Request body may not be null")
	}
	updateTableMu.Lock()
	defer updateTableMu.Unlock()

	// 1. Resolve the table (ResourceNotFoundException path).
	tableName := utString(req, "TableName")
	if tableName == "" {
		return nil, utInvalid("1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null")
	}
	stored, found := store.GetUpdateTableMeta(tableName)
	if !found {
		return nil, utTableNotFound(tableName)
	}
	table := utCopyMap(stored)
	if table == nil {
		table = map[string]any{}
	}
	if utString(table, "TableName") == "" {
		table["TableName"] = tableName
	}

	// 2. Billing mode must be a valid enum (tables default to PROVISIONED).
	curMode := utBillingModeOf(table)
	newMode := curMode
	if mode, ok := utStringOK(req, "BillingMode"); ok {
		if mode != utModeProvisioned && mode != utModePayPerRequest {
			return nil, utInvalid("1 validation error detected: Value '%s' at 'billingMode' failed to satisfy constraint: Member must satisfy enum value set: [%s, %s]", mode, utModeProvisioned, utModePayPerRequest)
		}
		newMode = mode
	}

	// 3. Table-level ProvisionedThroughput: validated up front because switching
	//    PAY_PER_REQUEST -> PROVISIONED requires it.
	reqRCU, reqWCU, reqHasTP := float64(0), float64(0), false
	if tp, ok := utMap(req, "ProvisionedThroughput"); ok {
		if newMode == utModeProvisioned {
			rcu, wcu, verr := utCheckCapacity(tp, "provisionedThroughput")
			if verr != nil {
				return nil, verr
			}
			reqRCU, reqWCU, reqHasTP = rcu, wcu, true
		}
		// With PAY_PER_REQUEST billing a ProvisionedThroughput block is accepted but ignored.
	} else if newMode == utModeProvisioned && curMode != utModeProvisioned {
		return nil, utInvalid("One or more parameter values were invalid: ProvisionedThroughput is required when switching from PAY_PER_REQUEST to PROVISIONED")
	}

	// 4. AttributeDefinitions are merged first so index key validation can resolve
	//    attributes introduced by this very request; types are immutable.
	if err := utMergeAttributeDefinitions(table, req); err != nil {
		return nil, err
	}

	// 5. GlobalSecondaryIndexUpdates (Create / Update / Delete), in order.
	touched, err := utApplyGlobalSecondaryIndexUpdates(table, req, newMode)
	if err != nil {
		return nil, err
	}

	// 6. Apply the billing-mode switch / table throughput change.
	utApplyBillingMode(table, curMode, newMode, reqHasTP, reqRCU, reqWCU, touched)

	// 7. Remaining updatable fields.
	if v, ok := req["DeletionProtectionEnabled"]; ok {
		table["DeletionProtectionEnabled"] = v
	}
	if class, ok := utStringOK(req, "TableClass"); ok {
		if class != "STANDARD" && class != "STANDARD_INFREQUENT_ACCESS" {
			return nil, utInvalid("1 validation error detected: Value '%s' at 'tableClass' failed to satisfy constraint: Member must satisfy enum value set: [%s, %s]", class, "STANDARD", "STANDARD_INFREQUENT_ACCESS")
		}
		table["TableClassSummary"] = map[string]any{"TableClass": class}
	}
	if _, ok := table["TableStatus"]; !ok {
		table["TableStatus"] = utStatusActive
	}

	// 8. Persist and return the updated TableDescription.
	store.PutUpdateTableMeta(tableName, table)
	desc := utCopyMap(table)
	for k := range desc {
		if strings.HasPrefix(k, "__") { // internal bookkeeping keys never leave the store
			delete(desc, k)
		}
	}
	return desc, nil
}

// utApplyGlobalSecondaryIndexUpdates walks GlobalSecondaryIndexUpdates in order,
// validating and applying each delta. It returns the set of index names touched
// by this request (needed for the billing-mode inheritance rule).
func utApplyGlobalSecondaryIndexUpdates(table, req map[string]any, mode string) (map[string]bool, *UpdateTableError) {
	touched := map[string]bool{}
	updates, ok := utSlice(req, "GlobalSecondaryIndexUpdates")
	if !ok || len(updates) == 0 {
		return touched, nil
	}
	gsis, _ := utSlice(table, "GlobalSecondaryIndexes")
	lsis, _ := utSlice(table, "LocalSecondaryIndexes")

	for i, raw := range updates {
		entry, ok := raw.(map[string]any)
		if !ok {
			return nil, utInvalid("One or more parameter values were invalid: Invalid GlobalSecondaryIndexUpdates element")
		}
		actions := 0
		var createGSI, updateGSI, deleteGSI map[string]any
		if m, ok := utMap(entry, "Create"); ok {
			createGSI, actions = m, actions+1
		}
		if m, ok := utMap(entry, "Update"); ok {
			updateGSI, actions = m, actions+1
		}
		if m, ok := utMap(entry, "Delete"); ok {
			deleteGSI, actions = m, actions+1
		}
		if actions != 1 {
			return nil, utInvalid("One or more parameter values were invalid: Exactly one of Create, Update or Delete must be specified for each GlobalSecondaryIndexUpdates element")
		}

		switch {
		case createGSI != nil:
			gsi, err := utBuildCreatedGSI(table, createGSI, i, mode)
			if err != nil {
				return nil, err
			}
			name := utString(createGSI, "IndexName")
			if utFindGSI(gsis, name) >= 0 || utFindGSI(lsis, name) >= 0 {
				return nil, utInvalid("One or more parameter values were invalid: Index already exists: %s", name)
			}
			if len(gsis)+1 > utMaxGlobalSecondaryIndexes {
				return nil, utLimit("Number of GlobalSecondaryIndexes exceeds per-table limit of %d", utMaxGlobalSecondaryIndexes)
			}
			gsis = append(gsis, gsi)
			touched[name] = true

		case updateGSI != nil:
			name := utString(updateGSI, "IndexName")
			idx := utFindGSI(gsis, name)
			if idx < 0 {
				return nil, utInvalid("One or more parameter values were invalid: Index not found: %s", name)
			}
			gm, ok := gsis[idx].(map[string]any)
			if !ok {
				return nil, utInvalid("One or more parameter values were invalid: Index not found: %s", name)
			}
			if mode == utModeProvisioned {
				thr, hasTP := utMap(updateGSI, "ProvisionedThroughput")
				if !hasTP {
					return nil, utInvalid("One or more parameter values were invalid: ProvisionedThroughput is required to update the global secondary index %s when the BillingMode is PROVISIONED", name)
				}
				rcu, wcu, terr := utCheckCapacity(thr, fmt.Sprintf("globalSecondaryIndexes.%d.member.provisionedThroughput", i+1))
				if terr != nil {
					return nil, terr
				}
				gm["ProvisionedThroughput"] = utCapacityMap(rcu, wcu)
			} else {
				gm["ProvisionedThroughput"] = utZeroThroughput()
			}
			gm["IndexStatus"] = utStatusActive
			touched[name] = true

		case deleteGSI != nil:
			name := utString(deleteGSI, "IndexName")
			idx := utFindGSI(gsis, name)
			if idx < 0 {
				return nil, utInvalid("One or more parameter values were invalid: Index not found: %s", name)
			}
			gsis = append(gsis[:idx], gsis[idx+1:]...)
			touched[name] = true
		}
	}

	if len(gsis) == 0 {
		delete(table, "GlobalSecondaryIndexes") // an empty list is omitted on the wire
	} else {
		table["GlobalSecondaryIndexes"] = gsis
	}
	return touched, nil
}

// utBuildCreatedGSI validates a Create action and renders the new index entry
// (IndexStatus ACTIVE, IndexArn, ProvisionedThroughput zeroed in on-demand mode).
func utBuildCreatedGSI(table, create map[string]any, member int, mode string) (map[string]any, *UpdateTableError) {
	name := utString(create, "IndexName")
	keySchema, hasSchema := utSlice(create, "KeySchema")
	if name == "" || !hasSchema || len(keySchema) == 0 {
		return nil, utInvalid("One or more parameter values were invalid: Index Name or Key Schema is missing")
	}
	if err := utValidateKeySchema(keySchema, table, "Index"); err != nil {
		return nil, err
	}
	projection, hasProjection := utMap(create, "Projection")
	if !hasProjection {
		return nil, utInvalid("One or more parameter values were invalid: Index Projection is missing")
	}
	switch ptype := utString(projection, "ProjectionType"); ptype {
	case "ALL", "KEYS_ONLY", "INCLUDE":
	default:
		return nil, utInvalid("One or more parameter values were invalid: Invalid index ProjectionType: %s", ptype)
	}
	if nonKeys, ok := utSlice(projection, "NonKeyAttributes"); ok && len(nonKeys) > 20 {
		return nil, utInvalid("One or more parameter values were invalid: Number of NonKeyAttributes cannot be more than 20")
	}

	rcu, wcu := float64(0), float64(0)
	if mode == utModeProvisioned {
		thr, hasTP := utMap(create, "ProvisionedThroughput")
		if !hasTP {
			return nil, utInvalid("One or more parameter values were invalid: ProvisionedThroughput must be specified for the global secondary index %s when the BillingMode is PROVISIONED", name)
		}
		var verr *UpdateTableError
		rcu, wcu, verr = utCheckCapacity(thr, fmt.Sprintf("globalSecondaryIndexes.%d.member.provisionedThroughput", member+1))
		if verr != nil {
			return nil, verr
		}
	}

	return map[string]any{
		"IndexName":             name,
		"KeySchema":             utCopy(keySchema),
		"Projection":            utCopy(projection),
		"IndexStatus":           utStatusActive,
		"IndexArn":              utIndexArn(table, name),
		"IndexSizeBytes":        float64(0),
		"ItemCount":             float64(0),
		"ProvisionedThroughput": utCapacityMap(rcu, wcu),
	}, nil
}

// utValidateKeySchema checks the KeySchema shape and that every key attribute
// is defined in the (already merged) table AttributeDefinitions.
func utValidateKeySchema(kss []any, table map[string]any, kind string) *UpdateTableError {
	var keys []string
	haveHash, haveRange := false, false
	hashAttr, rangeAttr := "", ""
	for _, raw := range kss {
		km, ok := raw.(map[string]any)
		if !ok {
			return utInvalid("One or more parameter values were invalid: Invalid %s KeySchema element", kind)
		}
		attr := utString(km, "AttributeName")
		keyType := utString(km, "KeyType")
		switch keyType {
		case utKeyHash:
			if haveHash {
				return utInvalid("One or more parameter values were invalid: %s KeySchema cannot contain more than one HASH key", kind)
			}
			haveHash, hashAttr = true, attr
		case utKeyRange:
			if haveRange {
				return utInvalid("One or more parameter values were invalid: %s KeySchema cannot contain more than one RANGE key", kind)
			}
			haveRange, rangeAttr = true, attr
		default:
			return utInvalid("One or more parameter values were invalid: Invalid KeyType %s in %s KeySchema", keyType, kind)
		}
		keys = append(keys, attr)
	}
	if !haveHash {
		return utInvalid("One or more parameter values were invalid: %s KeySchema must contain a HASH key", kind)
	}
	if rangeAttr != "" && rangeAttr == hashAttr {
		return utInvalid("One or more parameter values were invalid: The attribute %s cannot be used as both the hash and the range key", rangeAttr)
	}

	defs, _ := utSlice(table, "AttributeDefinitions")
	defined := map[string]string{}
	descs := make([]string, 0, len(defs))
	for _, raw := range defs {
		dm, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		n, t := utString(dm, "AttributeName"), utString(dm, "AttributeType")
		defined[n] = t
		descs = append(descs, n+":"+t)
	}
	var missing []string
	for _, k := range keys {
		if _, ok := defined[k]; !ok {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return utInvalid("One or more parameter values were invalid: Some index key attributes are not defined in AttributeDefinitions. Keys: [%s], AttributeDefinitions: [%s]",
			strings.Join(missing, ", "), strings.Join(descs, ", "))
	}
	return nil
}

// utMergeAttributeDefinitions adds new AttributeDefinitions; redefining an
// existing attribute with a different type fails (types are immutable).
func utMergeAttributeDefinitions(table, req map[string]any) *UpdateTableError {
	additions, ok := utSlice(req, "AttributeDefinitions")
	if !ok {
		return nil
	}
	merged, _ := utSlice(table, "AttributeDefinitions")
	types := make(map[string]string, len(merged))
	for _, raw := range merged {
		if dm, ok := raw.(map[string]any); ok {
			types[utString(dm, "AttributeName")] = utString(dm, "AttributeType")
		}
	}
	for _, raw := range additions {
		dm, ok := raw.(map[string]any)
		if !ok {
			return utInvalid("One or more parameter values were invalid: Invalid AttributeDefinition")
		}
		name := utString(dm, "AttributeName")
		atype := utString(dm, "AttributeType")
		if name == "" {
			return utInvalid("One or more parameter values were invalid: AttributeName is missing in AttributeDefinitions")
		}
		if atype != "S" && atype != "N" && atype != "B" {
			return utInvalid("1 validation error detected: Value '%s' at 'attributeDefinitions' failed to satisfy constraint: Member must satisfy enum value set: [B, N, S]", atype)
		}
		if existing, dup := types[name]; dup {
			if existing != atype {
				return utInvalid("One or more parameter values were invalid: Type mismatch for Attribute %s: existing %s, new %s", name, existing, atype)
			}
			continue
		}
		types[name] = atype
		merged = append(merged, utCopy(dm))
	}
	table["AttributeDefinitions"] = merged
	return nil
}

// utApplyBillingMode applies the billing-mode switch / table throughput change.
// GSIs carried over from PAY_PER_REQUEST inherit the table's new throughput
// unless this request explicitly updated or created them.
func utApplyBillingMode(table map[string]any, curMode, newMode string, reqHasTP bool, reqRCU, reqWCU float64, touched map[string]bool) {
	switch newMode {
	case utModePayPerRequest:
		table["ProvisionedThroughput"] = utZeroThroughput()
		for _, raw := range utSlice(table, "GlobalSecondaryIndexes") {
			if gm, ok := raw.(map[string]any); ok {
				gm["ProvisionedThroughput"] = utZeroThroughput()
				gm["IndexStatus"] = utStatusActive
			}
		}
		if curMode != utModePayPerRequest {
			table["BillingModeSummary"] = map[string]any{
				"BillingMode":                        utModePayPerRequest,
				"LastUpdateToPayPerRequestDateTime": utNowEpoch(),
			}
		}

	case utModeProvisioned:
		if reqHasTP {
			table["ProvisionedThroughput"] = utCapacityMap(reqRCU, reqWCU)
		} else if _, ok := utMap(table, "ProvisionedThroughput"); !ok {
			table["ProvisionedThroughput"] = utZeroThroughput()
		}
		tp, _ := utMap(table, "ProvisionedThroughput")
		for _, raw := range utSlice(table, "GlobalSecondaryIndexes") {
			gm, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if !touched[utString(gm, "IndexName")] {
				cur, _ := utMap(gm, "ProvisionedThroughput")
				if utNumberOf(cur, "ReadCapacityUnits") == 0 && utNumberOf(cur, "WriteCapacityUnits") == 0 {
					gm["ProvisionedThroughput"] = utCapacityMap(utNumberOf(tp, "ReadCapacityUnits"), utNumberOf(tp, "WriteCapacityUnits"))
				}
			}
			gm["IndexStatus"] = utStatusActive
		}
		if curMode != utModeProvisioned {
			if summary, ok := utMap(table, "BillingModeSummary"); ok {
				delete(summary, "LastUpdateToPayPerRequestDateTime")
				summary["BillingMode"] = utModeProvisioned
			} else {
				table["BillingModeSummary"] = map[string]any{"BillingMode": utModeProvisioned}
			}
		}
	}
}

func utFindGSI(indexes []any, name string) int {
	if name == "" {
		return -1
	}
	for i, raw := range indexes {
		if gm, ok := raw.(map[string]any); ok && utString(gm, "IndexName") == name {
			return i
		}
	}
	return -1
}

func utCapacityMap(rcu, wcu float64) map[string]any {
	return map[string]any{
		"ReadCapacityUnits":      rcu,
		"WriteCapacityUnits":     wcu,
		"NumberOfDecreasesToday": float64(0),
	}
}

func utZeroThroughput() map[string]any { return utCapacityMap(0, 0) }

func utCheckCapacity(pt map[string]any, path string) (float64, float64, *UpdateTableError) {
	rcu, okR := utNumberOK(pt, "ReadCapacityUnits")
	if !okR {
		return 0, 0, utInvalid("1 validation error detected: Value null at '%s.readCapacityUnits' failed to satisfy constraint: Member must not be null", path)
	}
	wcu, okW := utNumberOK(pt, "WriteCapacityUnits")
	if !okW {
		return 0, 0, utInvalid("1 validation error detected: Value null at '%s.writeCapacityUnits' failed to satisfy constraint: Member must not be null", path)
	}
	if e := utCapacityInRange(rcu, path+".readCapacityUnits"); e != nil {
		return 0, 0, e
	}
	if e := utCapacityInRange(wcu, path+".writeCapacityUnits"); e != nil {
		return 0, 0, e
	}
	return rcu, wcu, nil
}

func utCapacityInRange(v float64, path string) *UpdateTableError {
	if v < 1 {
		return utInvalid("1 validation error detected: Value '%s' at '%s' failed to satisfy constraint: Member must have value greater than 0", utNumStr(v), path)
	}
	if v > utMaxProvisionedUnits {
		return utLimit("The request would exceed the maximum allowed provisioned throughput: %s of %s exceeds the limit of %d", path, utNumStr(v), utMaxProvisionedUnits)
	}
	return nil
}

func utIndexArn(table map[string]any, index string) string {
	arn := utString(table, "TableArn")
	if arn == "" {
		arn = "arn:aws:dynamodb:" + utDefaultRegion + ":" + utDefaultAccount + ":table/" + utString(table, "TableName")
	} else if i := strings.Index(arn, "/index/"); i >= 0 {
		arn = arn[:i]
	}
	return arn + "/index/" + index
}

func utBillingModeOf(meta map[string]any) string {
	if summary, ok := utMap(meta, "BillingModeSummary"); ok {
		if m := utString(summary, "BillingMode"); m == utModeProvisioned || m == utModePayPerRequest {
			return m
		}
	}
	return utModeProvisioned
}

func utNowEpoch() float64 {
	return float64(time.Now().UnixNano()) / float64(time.Second)
}

func utNumStr(v float64) string {
	return fmt.Sprintf("%v", v)
}

func utCopy(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[k] = utCopy(val)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = utCopy(val)
		}
		return out
	default:
		return v
	}
}

func utCopyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out, _ := utCopy(m).(map[string]any)
	return out
}

func utString(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}

func utStringOK(m map[string]any, key string) (string, bool) {
	v, ok := m[key].(string)
	return v, ok
}

func utMap(m map[string]any, key string) (map[string]any, bool) {
	v, ok := m[key].(map[string]any)
	return v, ok
}

func utSlice(m map[string]any, key string) ([]any, bool) {
	return utAsList(m[key])
}

// utAsList tolerates both []any (encoding/json) and typed Go test fixtures.
func utAsList(v any) ([]any, bool) {
	switch list := v.(type) {
	case []any:
		return list, true
	case []map[string]any:
		out := make([]any, len(list))
		for i, e := range list {
			out[i] = e
		}
		return out, true
	default:
		return nil, false
	}
}

func utNumberOK(m map[string]any, key string) (float64, bool) {
	switch v := m[key].(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case json.Number:
		if f, err := v.Float64(); err == nil {
			return f, true
		}
	}
	return 0, false
}

func utNumberOf(m map[string]any, key string) float64 {
	v, _ := utNumberOK(m, key)
	return v
}

// UpdateTableWireResult is the transport-level rendering of an UpdateTable call.
type UpdateTableWireResult struct {
	StatusCode int    // 200 on success, 400 for client errors
	Body       []byte
	ErrorType  string // value for the X-Amzn-Errortype header; empty on success
}

// HandleUpdateTableWire parses a raw UpdateTable request document, applies the
// operation and renders the AWS JSON 1.x success/error envelope for
// X-Amz-Target: DynamoDB_20120810.UpdateTable.
func HandleUpdateTableWire(body []byte) UpdateTableWireResult {
	var req map[string]any
	if err := json.Unmarshal(body, &req); err != nil || req == nil {
		typ := utWireErrorPrefix + "SerializationException"
		return UpdateTableWireResult{StatusCode: 400, ErrorType: typ, Body: utWireErrorBody(typ, "Failed to parse the UpdateTable request document")}
	}
	desc, aerr := ApplyUpdateTable(DefaultUpdateTableStore, req)
	if aerr != nil {
		typ := utWireErrorPrefix + aerr.Code
		return UpdateTableWireResult{StatusCode: 400, ErrorType: typ, Body: utWireErrorBody(typ, aerr.Message)}
	}
	out, merr := json.Marshal(desc)
	if merr != nil {
		typ := utWireErrorPrefix + "InternalServerError"
		return UpdateTableWireResult{StatusCode: 500, ErrorType: typ, Body: utWireErrorBody(typ, "Internal server error")}
	}
	return UpdateTableWireResult{StatusCode: 200, Body: out}
}

func utWireErrorBody(typ, message string) []byte {
	b, _ := json.Marshal(map[string]any{"__type": typ, "message": message})
	return b
}
