package ddb

// BatchGetItem (DynamoDB_20120810.BatchGetItem).
//
// Behavior mirrors the service / DynamoDB Local:
//   - up to 100 Gets across all tables (per-table Keys list also capped at
//     100, validated like the API shape constraint);
//   - every table must exist or the whole request fails;
//   - key schema, key types, key sizes and duplicate keys are validated up
//     front and fail the whole request;
//   - ProjectionExpression support with ExpressionAttributeNames, nested
//     document paths and list index selection;
//   - partial results: keys whose items could not be returned (throughput
//     errors, or the 16 MB response cap being reached) are echoed back in
//     UnprocessedKeys with the original projection settings;
//   - ConsumedCapacity when ReturnConsumedCapacity is TOTAL or INDEXES.

import (
    "encoding/json"
    "math"
)

// KeysAndAttributes models one table entry of a BatchGetItem request.
type KeysAndAttributes struct {
    Keys                     *[]map[string]AttrVal `json:"Keys"`
    ProjectionExpression     *string               `json:"ProjectionExpression"`
    ExpressionAttributeNames map[string]string     `json:"ExpressionAttributeNames"`
    ConsistentRead           *bool                 `json:"ConsistentRead"`
}

// BatchGetItemHandler implements the DynamoDB_20120810.BatchGetItem target.
type BatchGetItemHandler struct {
    store Store
}

func NewBatchGetItemHandler(store Store) *BatchGetItemHandler {
    return &BatchGetItemHandler{store: store}
}

func (h *BatchGetItemHandler) Target() string { return "BatchGetItem" }

type tableGetRequest struct {
    table      string
    ka         KeysAndAttributes
    keys       []map[string]AttrVal
    projection []exprPath
}

func (h *BatchGetItemHandler) Invoke(body []byte) (interface{}, *APIError) {
    var envelope struct {
        RequestItems           json.RawMessage `json:"RequestItems"`
        ReturnConsumedCapacity string          `json:"ReturnConsumedCapacity"`
    }
    if err := json.Unmarshal(body, &envelope); err != nil {
        return nil, serializationErr("Unable to parse request body")
    }
    if len(envelope.RequestItems) == 0 || string(envelope.RequestItems) == "null" {
        return nil, memberNullError("requestItems")
    }
    var rawItems map[string]json.RawMessage
    if err := json.Unmarshal(envelope.RequestItems, &rawItems); err != nil {
        return nil, serializationErr("Unable to parse requestItems")
    }
    if len(rawItems) == 0 {
        return nil, memberMinLengthError("requestItems")
    }
    tables := sortedKeys(rawItems)
    schemas := make(map[string]TableSchema, len(tables))
    for _, t := range tables {
        schema, err := h.store.Schema(t)
        if err != nil {
            return nil, asAPIError(err)
        }
        schemas[t] = schema
    }

    var reqs []tableGetRequest
    totalKeys := 0
    for _, t := range tables {
        var ka KeysAndAttributes
        if err := json.Unmarshal(rawItems[t], &ka); err != nil {
            return nil, asAPIError(err)
        }
        if ka.Keys == nil {
            return nil, memberNullError("requestItems." + t + ".keys")
        }
        if len(*ka.Keys) == 0 {
            return nil, memberMinLengthError("requestItems." + t + ".keys")
        }
        if len(*ka.Keys) > batchGetMaxItemsPerTable {
            return nil, memberMaxLengthError("requestItems."+t+".keys", batchGetMaxItemsPerTable)
        }
        totalKeys += len(*ka.Keys)
        var projection []exprPath
        if ka.ProjectionExpression != nil {
            paths, perr := parseProjection(*ka.ProjectionExpression, ka.ExpressionAttributeNames)
            if perr != nil {
                return nil, perr
            }
            projection = paths
        } else if len(ka.ExpressionAttributeNames) > 0 {
            return nil, unusedNamesError(ka.ExpressionAttributeNames)
        }
        seen := make(map[string]bool, len(*ka.Keys))
        keys := make([]map[string]AttrVal, 0, len(*ka.Keys))
        for _, raw := range *ka.Keys {
            key, kerr := validateKeyShape(schemas[t], raw)
            if kerr != nil {
                return nil, kerr
            }
            id := keyString(key)
            if seen[id] {
                return nil, validation("Provided list of item keys contains duplicates")
            }
            seen[id] = true
            keys = append(keys, key)
        }
        reqs = append(reqs, tableGetRequest{table: t, ka: ka, keys: keys, projection: projection})
    }
    if totalKeys > batchGetMaxItems {
        return nil, validation("Too many items requested for batch retrieval")
    }

    responses := make(map[string][]map[string]AttrVal, len(reqs))
    unprocessed := make(map[string]interface{}, len(reqs))
    needCapacity := envelope.ReturnConsumedCapacity == "TOTAL" || envelope.ReturnConsumedCapacity == "INDEXES"
    capacities := make([]map[string]interface{}, 0, len(reqs))
    var totalBytes int
    exhausted := false
    for _, r := range reqs {
        consistent := r.ka.ConsistentRead != nil && *r.ka.ConsistentRead
        items := []map[string]AttrVal{}
        var remaining []map[string]AttrVal
        var units float64
        for _, key := range r.keys {
            if exhausted {
                remaining = append(remaining, key)
                continue
            }
            item, found, err := h.store.GetItem(r.table, key, consistent)
            if err != nil {
                ae := asAPIError(err)
                if ae == nil {
                    ae = internalErr("storage failure")
                }
                if ae.Type == "ProvisionedThroughputExceededException" {
                    exhausted = true
                    remaining = append(remaining, key)
                    continue
                }
                return nil, ae
            }
            if !found {
                continue
            }
            result := item
            if r.projection != nil {
                projected, perr := projectItem(item, r.projection)
                if perr != nil {
                    return nil, perr
                }
                result = projected
            }
            size := itemSize(item)
            if totalBytes+size > batchGetMaxTotalBytes {
                exhausted = true
                remaining = append(remaining, key)
                continue
            }
            totalBytes += size
            units += readCapacityUnits(size, consistent)
            items = append(items, result)
        }
        responses[r.table] = items
        if len(remaining) > 0 {
            unprocessed[r.table] = echoKeysAndAttributes(r.ka, remaining)
        }
        if needCapacity {
            capacities = append(capacities, capacityEntry(r.table, units, envelope.ReturnConsumedCapacity))
        }
    }
    resp := map[string]interface{}{
        "Responses":       responses,
        "UnprocessedKeys": unprocessed,
    }
    if needCapacity {
        resp["ConsumedCapacity"] = capacities
    }
    return resp, nil
}

func echoKeysAndAttributes(ka KeysAndAttributes, keys []map[string]AttrVal) map[string]interface{} {
    out := map[string]interface{}{"Keys": keys}
    if ka.ProjectionExpression != nil {
        out["ProjectionExpression"] = *ka.ProjectionExpression
    }
    if ka.ExpressionAttributeNames != nil {
        out["ExpressionAttributeNames"] = ka.ExpressionAttributeNames
    }
    if ka.ConsistentRead != nil {
        out["ConsistentRead"] = *ka.ConsistentRead
    }
    return out
}

// Capacity accounting: strong reads consume one unit per item per 4 KB
// (eventual reads half that); writes consume one unit per item per 1 KB.
func readCapacityUnits(size int, consistent bool) float64 {
    units := math.Ceil(float64(size) / 4096)
    if units < 1 {
        units = 1
    }
    if !consistent {
        units *= 0.5
    }
    return units
}

func writeCapacityUnits(size int) float64 {
    units := math.Ceil(float64(size) / 1024)
    if units < 1 {
        units = 1
    }
    return units
}

func capacityEntry(table string, units float64, mode string) map[string]interface{} {
    entry := map[string]interface{}{"TableName": table, "CapacityUnits": units}
    if mode == "INDEXES" {
        entry["Table"] = map[string]interface{}{"CapacityUnits": units}
    }
    return entry
}

// projectItem materializes the projection described by paths.
func projectItem(item map[string]AttrVal, paths []exprPath) (map[string]AttrVal, *APIError) {
    out := make(map[string]AttrVal, len(paths))
    for _, p := range paths {
        leaf, found, err := resolveDocPath(item, p)
        if err != nil {
            return nil, err
        }
        if !found {
            continue
        }
        if aerr := insertIntoMap(out, p.parts, 0, leaf); aerr != nil {
            return nil, aerr
        }
    }
    return out, nil
}

// insertIntoMap inserts leaf at parts[from:] into m, creating intermediate
// containers as needed. List index gaps are filled with NULL attributes and
// the result is truncated after the largest projected index, mirroring the
// DynamoDB Local projection behaviour.
func insertIntoMap(m map[string]AttrVal, parts []exprPart, from int, leaf AttrVal) *APIError {
    part := parts[from]
    if part.isIdx {
        return invalidDocPathError()
    }
    if from == len(parts)-1 {
        m[part.name] = copyAttr(leaf)
        return nil
    }
    next := parts[from+1]
    child, ok := m[part.name]
    usable := ok && childIsContainerFor(child, next.isIdx)
    if next.isIdx {
        list := []AttrVal{}
        if usable {
            list = append([]AttrVal(nil), child.L...)
        }
        updated, err := insertIntoList(list, parts, from+1, leaf)
        if err != nil {
            return err
        }
        m[part.name] = AttrVal{Type: "L", L: updated}
        return nil
    }
    sub := map[string]AttrVal{}
    if usable {
        sub = make(map[string]AttrVal, len(child.M))
        for k, v := range child.M {
            sub[k] = v
        }
    }
    if err := insertIntoMap(sub, parts, from+1, leaf); err != nil {
        return err
    }
    m[part.name] = AttrVal{Type: "M", M: sub}
    return nil
}

func insertIntoList(list []AttrVal, parts []exprPart, from int, leaf AttrVal) ([]AttrVal, *APIError) {
    part := parts[from]
    if !part.isIdx {
        return list, invalidDocPathError()
    }
    for len(list) <= part.idx {
        list = append(list, MakeNULL())
    }
    if from == len(parts)-1 {
        list[part.idx] = copyAttr(leaf)
        return list, nil
    }
    next := parts[from+1]
    child := list[part.idx]
    usable := childIsContainerFor(child, next.isIdx)
    if next.isIdx {
        sub := []AttrVal{}
        if usable {
            sub = append([]AttrVal(nil), child.L...)
        }
        updated, err := insertIntoList(sub, parts, from+1, leaf)
        if err != nil {
            return list, err
        }
        list[part.idx] = AttrVal{Type: "L", L: updated}
        return list, nil
    }
    sub := map[string]AttrVal{}
    if usable {
        sub = make(map[string]AttrVal, len(child.M))
        for k, v := range child.M {
            sub[k] = v
        }
    }
    if err := insertIntoMap(sub, parts, from+1, leaf); err != nil {
        return list, err
    }
    list[part.idx] = AttrVal{Type: "M", M: sub}
    return list, nil
}

func childIsContainerFor(v AttrVal, isIndex bool) bool {
    if isIndex {
        return v.Type == "L"
    }
    return v.Type == "M"
}
