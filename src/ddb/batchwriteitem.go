package ddb

// BatchWriteItem (DynamoDB_20120810.BatchWriteItem).
//
// Behavior mirrors the service / DynamoDB Local:
//   - up to 25 Puts / Deletes / ConditionChecks across all tables (each
//     table's request list also capped at 25, validated like the API shape
//     constraint);
//   - every table must exist or the whole request fails;
//   - request shape, key schema, key types, key sizes, item size (400 KB)
//     and duplicate keys within a table are validated up front and fail the
//     whole request with ValidationException;
//   - ConditionCheck expressions are compiled and validated with the
//     expression engine before anything executes (undefined / unused
//     ExpressionAttributeNames/Values, syntax errors);
//   - conditions that evaluate to false make DynamoDB skip the item
//     silently: no error, no mutation, and it is NOT reported in
//     UnprocessedItems (BatchWriteItem is not atomic);
//   - if a table runs out of throughput mid-batch, that request plus all
//     remaining ones are echoed back in UnprocessedItems.

import (
    "encoding/json"
    "fmt"
)

type putRequestWire struct {
    Item map[string]AttrVal `json:"Item"`
}

type deleteRequestWire struct {
    Key map[string]AttrVal `json:"Key"`
}

type conditionCheckWire struct {
    Key                       map[string]AttrVal `json:"Key"`
    ConditionExpression       *string            `json:"ConditionExpression"`
    ExpressionAttributeNames  map[string]string  `json:"ExpressionAttributeNames"`
    ExpressionAttributeValues map[string]AttrVal `json:"ExpressionAttributeValues"`
}

type writeRequestWire struct {
    PutRequest     *putRequestWire     `json:"PutRequest"`
    DeleteRequest  *deleteRequestWire  `json:"DeleteRequest"`
    ConditionCheck *conditionCheckWire `json:"ConditionCheck"`
}

type batchWriteOp struct {
    table  string
    kind   string
    item   map[string]AttrVal
    key    map[string]AttrVal
    cond   condNode
    values map[string]AttrVal
    wire   writeRequestWire
}

// BatchWriteItemHandler implements the DynamoDB_20120810.BatchWriteItem
// target.
type BatchWriteItemHandler struct {
    store Store
}

func NewBatchWriteItemHandler(store Store) *BatchWriteItemHandler {
    return &BatchWriteItemHandler{store: store}
}

func (h *BatchWriteItemHandler) Target() string { return "BatchWriteItem" }

func (h *BatchWriteItemHandler) Invoke(body []byte) (interface{}, *APIError) {
    var envelope struct {
        RequestItems                json.RawMessage `json:"RequestItems"`
        ReturnConsumedCapacity      string          `json:"ReturnConsumedCapacity"`
        ReturnItemCollectionMetrics string          `json:"ReturnItemCollectionMetrics"`
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

    var ops []batchWriteOp
    total := 0
    for _, t := range tables {
        var list []writeRequestWire
        if err := json.Unmarshal(rawItems[t], &list); err != nil {
            return nil, asAPIError(err)
        }
        if len(list) == 0 {
            return nil, memberMinLengthError("requestItems." + t)
        }
        if len(list) > batchWriteMaxRequests {
            return nil, memberMaxLengthError("requestItems."+t, batchWriteMaxRequests)
        }
        total += len(list)
        seen := make(map[string]bool, len(list))
        for i := range list {
            op, oerr := h.validateOp(schemas[t], t, i, &list[i])
            if oerr != nil {
                return nil, oerr
            }
            dup := keyString(op.key)
            if seen[dup] {
                return nil, validation("Provided list of item keys contains duplicates")
            }
            seen[dup] = true
            ops = append(ops, *op)
        }
    }
    if total > batchWriteMaxRequests {
        return nil, validation("Too many items requested for batch write")
    }

    needCapacity := envelope.ReturnConsumedCapacity == "TOTAL" || envelope.ReturnConsumedCapacity == "INDEXES"
    capacity := make(map[string]float64, len(tables))
    unprocessed := make(map[string][]map[string]interface{}, len(tables))
    writeFailed := false
    for _, op := range ops {
        if writeFailed {
            unprocessed[op.table] = append(unprocessed[op.table], wireToAny(op.wire))
            continue
        }
        if op.cond != nil {
            current, _, err := h.store.GetItem(op.table, op.key, false)
            if err != nil {
                ae := asAPIError(err)
                if ae == nil {
                    ae = internalErr("storage failure")
                }
                if ae.Type == "ProvisionedThroughputExceededException" {
                    writeFailed = true
                    unprocessed[op.table] = append(unprocessed[op.table], wireToAny(op.wire))
                    continue
                }
                return nil, ae
            }
            passed, cerr := op.cond.eval(&condEvalCtx{item: current, values: op.values})
            if cerr != nil {
                return nil, cerr
            }
            if !passed {
                // Condition evaluated to false: the request is skipped
                // silently - DynamoDB does not report condition failures for
                // batch writes, and the batch continues.
                continue
            }
        }
        var err error
        switch op.kind {
        case "PutRequest":
            err = h.store.PutItem(op.table, op.item)
            if err == nil && needCapacity {
                capacity[op.table] += writeCapacityUnits(itemSize(op.item))
            }
        case "DeleteRequest":
            err = h.store.DeleteItem(op.table, op.key)
            if err == nil && needCapacity {
                capacity[op.table] += 1
            }
        }
        if err != nil {
            ae := asAPIError(err)
            if ae == nil {
                ae = internalErr("storage failure")
            }
            if ae.Type == "ProvisionedThroughputExceededException" {
                writeFailed = true
                unprocessed[op.table] = append(unprocessed[op.table], wireToAny(op.wire))
                continue
            }
            return nil, ae
        }
    }
    resp := map[string]interface{}{"UnprocessedItems": unprocessed}
    if envelope.ReturnItemCollectionMetrics == "SIZE" {
        resp["ItemCollectionMetrics"] = map[string]interface{}{}
    }
    if needCapacity {
        entries := make([]map[string]interface{}, 0, len(tables))
        for _, t := range tables {
            entries = append(entries, capacityEntry(t, capacity[t], envelope.ReturnConsumedCapacity))
        }
        resp["ConsumedCapacity"] = entries
    }
    return resp, nil
}

func (h *BatchWriteItemHandler) validateOp(schema TableSchema, table string, idx int, w *writeRequestWire) (*batchWriteOp, *APIError) {
    count := 0
    if w.PutRequest != nil {
        count++
    }
    if w.DeleteRequest != nil {
        count++
    }
    if w.ConditionCheck != nil {
        count++
    }
    if count != 1 {
        return nil, validation("One or more parameter values are not valid: The batch write request must contain exactly one of PutRequest, DeleteRequest or ConditionCheck")
    }
    op := &batchWriteOp{table: table, wire: *w}
    switch {
    case w.PutRequest != nil:
        op.kind = "PutRequest"
        if w.PutRequest.Item == nil {
            return nil, memberNullError(fmt.Sprintf("requestItems.%s.%d.putRequest.item", table, idx))
        }
        item, err := validatePutItem(schema, w.PutRequest.Item)
        if err != nil {
            return nil, err
        }
        op.item = item
        op.key = primaryKeySubset(schema, item)
    case w.DeleteRequest != nil:
        op.kind = "DeleteRequest"
        if w.DeleteRequest.Key == nil {
            return nil, memberNullError(fmt.Sprintf("requestItems.%s.%d.deleteRequest.key", table, idx))
        }
        key, err := validateKeyShape(schema, w.DeleteRequest.Key)
        if err != nil {
            return nil, err
        }
        op.key = key
    case w.ConditionCheck != nil:
        op.kind = "ConditionCheck"
        cc := w.ConditionCheck
        if cc.Key == nil {
            return nil, memberNullError(fmt.Sprintf("requestItems.%s.%d.conditionCheck.key", table, idx))
        }
        key, err := validateKeyShape(schema, cc.Key)
        if err != nil {
            return nil, err
        }
        op.key = key
        if cc.ConditionExpression == nil {
            return nil, memberNullError(fmt.Sprintf("requestItems.%s.%d.conditionCheck.conditionExpression", table, idx))
        }
        node, cerr := compileCondition(*cc.ConditionExpression, cc.ExpressionAttributeNames, cc.ExpressionAttributeValues)
        if cerr != nil {
            return nil, cerr
        }
        op.cond = node
        op.values = cc.ExpressionAttributeValues
    }
    return op, nil
}

// wireToAny rebuilds the original request entry for unprocessed responses.
func wireToAny(w writeRequestWire) map[string]interface{} {
    switch {
    case w.PutRequest != nil:
        return map[string]interface{}{"PutRequest": map[string]interface{}{"Item": w.PutRequest.Item}}
    case w.DeleteRequest != nil:
        return map[string]interface{}{"DeleteRequest": map[string]interface{}{"Key": w.DeleteRequest.Key}}
    case w.ConditionCheck != nil:
        cc := map[string]interface{}{"Key": w.ConditionCheck.Key}
        if w.ConditionCheck.ConditionExpression != nil {
            cc["ConditionExpression"] = *w.ConditionCheck.ConditionExpression
        }
        if w.ConditionCheck.ExpressionAttributeNames != nil {
            cc["ExpressionAttributeNames"] = w.ConditionCheck.ExpressionAttributeNames
        }
        if w.ConditionCheck.ExpressionAttributeValues != nil {
            cc["ExpressionAttributeValues"] = w.ConditionCheck.ExpressionAttributeValues
        }
        return map[string]interface{}{"ConditionCheck": cc}
    }
    return map[string]interface{}{}
}
