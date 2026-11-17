package ddb

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"
)

// Request model ----------------------------------------------------------------

// TransactWriteItem is a single action of the transaction.
type TransactWriteItem struct {
	Put            *PutOp
	Delete         *DeleteOp
	Update         *UpdateOp
	ConditionCheck *ConditionCheckOp
}

type commonOp struct {
	TableName                           string
	ConditionExpression                 string
	ExpressionAttributeNames            map[string]*string
	ExpressionAttributeValues           map[string]*AttributeValue
	ReturnValuesOnConditionCheckFailure string
}

type PutOp struct {
	commonOp
	Item map[string]*AttributeValue
}

type DeleteOp struct {
	commonOp
	Key map[string]*AttributeValue
}

type UpdateOp struct {
	commonOp
	Key              map[string]*AttributeValue
	UpdateExpression string
}

type ConditionCheckOp struct {
	commonOp
	Key map[string]*AttributeValue
}

// TransactWriteItemsInput is the parsed TransactWriteItems request.
type TransactWriteItemsInput struct {
	TransactItems          []TransactWriteItem
	ReturnConsumedCapacity string
	ClientRequestToken     string
}

// Wire decoding -------------------------------------------------------------------

type wireOpJSON struct {
	TableName string `json:"TableName"`

	Item map[string]json.RawMessage `json:"Item"`
	Key  map[string]json.RawMessage `json:"Key"`

	UpdateExpression                    string                     `json:"UpdateExpression"`
	ConditionExpression                 string                     `json:"ConditionExpression"`
	ExpressionAttributeNames            map[string]*string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues           map[string]json.RawMessage `json:"ExpressionAttributeValues"`
	ReturnValuesOnConditionCheckFailure string                     `json:"ReturnValuesOnConditionCheckFailure"`
}

func decodeWireOp(raw json.RawMessage) (*wireOpJSON, *APIError) {
	var w wireOpJSON
	if err := json.Unmarshal(raw, &w); err != nil {
		return nil, validationErr("Failed to parse the request body: %v", err)
	}
	return &w, nil
}

func decodeAttrValueMap(raw map[string]json.RawMessage) (map[string]*AttributeValue, *APIError) {
	if raw == nil {
		return nil, nil
	}
	out := make(map[string]*AttributeValue, len(raw))
	for k, r := range raw {
		v, err := decodeAttrValue(r)
		if err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, nil
}

// decodeAttrValue decodes one DynamoDB JSON attribute value.
func decodeAttrValue(raw json.RawMessage) (*AttributeValue, *APIError) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, validationErr("Supplied AttributeValue is empty, must contain exactly one of the supported datatypes")
	}
	if len(m) == 0 {
		return nil, validationErr("Supplied AttributeValue is empty, must contain exactly one of the supported datatypes")
	}
	if len(m) > 1 {
		return nil, validationErr("Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes")
	}
	av := &AttributeValue{}
	for k, r := range m {
		switch k {
		case "NULL":
			av.NULL = true
		case "BOOL":
			var b bool
			if err := json.Unmarshal(r, &b); err != nil {
				return nil, validationErr("Unsupported type conversion: BOOL must be a bool")
			}
			av.BOOL = &b
		case "S":
			var s string
			if err := json.Unmarshal(r, &s); err != nil {
				return nil, validationErr("Unsupported type conversion: S must be a string")
			}
			av.S = s
		case "N":
			var s string
			if err := json.Unmarshal(r, &s); err != nil {
				return nil, validationErr("Unsupported type conversion: N must be a string")
			}
			if _, ok := parseRat(s); !ok {
				return nil, validationErr("One or more parameter values are not valid: The parameter cannot be converted to a numeric value: %s", s)
			}
			av.N = s
		case "B":
			var s string
			if err := json.Unmarshal(r, &s); err != nil {
				return nil, validationErr("Unsupported type conversion: B must be a base64 string")
			}
			b, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return nil, validationErr("One or more parameter values had invalid base64 encoding")
			}
			av.B = b
		case "SS", "NS":
			var arr []string
			if err := json.Unmarshal(r, &arr); err != nil {
				return nil, validationErr("Unsupported type conversion: %s must be a list of strings", k)
			}
			if k == "NS" {
				for _, s := range arr {
					if _, ok := parseRat(s); !ok {
						return nil, validationErr("One or more parameter values are not valid: The parameter cannot be converted to a numeric value: %s", s)
					}
				}
				av.NS = arr
			} else {
				av.SS = arr
			}
		case "BS":
			var arr []string
			if err := json.Unmarshal(r, &arr); err != nil {
				return nil, validationErr("Unsupported type conversion: BS must be a list of base64 strings")
			}
			bs := make([][]byte, 0, len(arr))
			for _, s := range arr {
				b, err := base64.StdEncoding.DecodeString(s)
				if err != nil {
					return nil, validationErr("One or more parameter values had invalid base64 encoding")
				}
				bs = append(bs, b)
			}
			av.BS = bs
		case "M":
			var mm map[string]json.RawMessage
			if err := json.Unmarshal(r, &mm); err != nil {
				return nil, validationErr("Unsupported type conversion: M must be a map")
			}
			mv, err := decodeAttrValueMap(mm)
			if err != nil {
				return nil, err
			}
			av.M = mv
		case "L":
			var arr []json.RawMessage
			if err := json.Unmarshal(r, &arr); err != nil {
				return nil, validationErr("Unsupported type conversion: L must be a list")
			}
			lv := make([]*AttributeValue, 0, len(arr))
			for _, x := range arr {
				ev, err := decodeAttrValue(x)
				if err != nil {
					return nil, err
				}
				lv = append(lv, ev)
			}
			av.L = lv
		default:
			return nil, validationErr("One or more parameter values were invalid: Unsupported datatype: %s", k)
		}
	}
	return av, nil
}

// ParseTransactWriteItems decodes the JSON request body of TransactWriteItems.
func ParseTransactWriteItems(body []byte) (*TransactWriteItemsInput, *APIError) {
	var wire struct {
		TransactItems          []map[string]json.RawMessage `json:"TransactItems"`
		ReturnConsumedCapacity string                       `json:"ReturnConsumedCapacity"`
		ClientRequestToken     string                       `json:"ClientRequestToken"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		return nil, validationErr("Failed to parse the request body: %v", err)
	}
	in := &TransactWriteItemsInput{
		ReturnConsumedCapacity: wire.ReturnConsumedCapacity,
		ClientRequestToken:     wire.ClientRequestToken,
	}
	for _, raw := range wire.TransactItems {
		var item TransactWriteItem
		count := 0
		for k, rawOp := range raw {
			w, err := decodeWireOp(rawOp)
			if err != nil {
				return nil, err
			}
			values, err := decodeAttrValueMap(w.ExpressionAttributeValues)
			if err != nil {
				return nil, err
			}
			common := commonOp{
				TableName:                           w.TableName,
				ConditionExpression:                 w.ConditionExpression,
				ExpressionAttributeNames:            w.ExpressionAttributeNames,
				ExpressionAttributeValues:           values,
				ReturnValuesOnConditionCheckFailure: w.ReturnValuesOnConditionCheckFailure,
			}
			switch k {
			case "Put":
				count++
				itemMap, err := decodeAttrValueMap(w.Item)
				if err != nil {
					return nil, err
				}
				item.Put = &PutOp{commonOp: common, Item: itemMap}
			case "Delete":
				count++
				keyMap, err := decodeAttrValueMap(w.Key)
				if err != nil {
					return nil, err
				}
				item.Delete = &DeleteOp{commonOp: common, Key: keyMap}
			case "Update":
				count++
				keyMap, err := decodeAttrValueMap(w.Key)
				if err != nil {
					return nil, err
				}
				item.Update = &UpdateOp{commonOp: common, Key: keyMap, UpdateExpression: w.UpdateExpression}
			case "ConditionCheck":
				count++
				keyMap, err := decodeAttrValueMap(w.Key)
				if err != nil {
					return nil, err
				}
				item.ConditionCheck = &ConditionCheckOp{commonOp: common, Key: keyMap}
			}
		}
		if count != 1 {
			return nil, validationErr("TransactItems can only contain one of Put, Delete, Update or ConditionCheck")
		}
		in.TransactItems = append(in.TransactItems, item)
	}
	return in, nil
}

// Transaction execution -------------------------------------------------------------

type txnOp struct {
	kind    string // PUT | DELETE | UPDATE | CONDITION_CHECK
	table   *Table
	enc     string
	lockKey string
	key     map[string]*AttributeValue
	item    map[string]*AttributeValue // PUT
	prog    *updateProgram             // UPDATE
	cond    *compiledCond
	retOld  bool
	names   map[string]*string
	values  map[string]*AttributeValue
}

func transactionKindLabel(kind string) string {
	switch kind {
	case "PUT":
		return "Put"
	case "DELETE":
		return "Delete"
	case "UPDATE":
		return "Update"
	case "CONDITION_CHECK":
		return "ConditionCheck"
	}
	return kind
}

// normalizeTxnOp parses and validates one action: table lookup, key-schema
// checks and expression compilation all happen before anything is executed.
func (db *DB) normalizeTxnOp(it TransactWriteItem) (*txnOp, *APIError) {
	var kind, tblName, condExpr, updateExpr, rvcf string
	var names map[string]*string
	var values map[string]*AttributeValue
	var keyMap, itemMap map[string]*AttributeValue
	switch {
	case it.Put != nil:
		kind = "PUT"
		tblName = it.Put.TableName
		itemMap = it.Put.Item
		condExpr = it.Put.ConditionExpression
		names = it.Put.ExpressionAttributeNames
		values = it.Put.ExpressionAttributeValues
		rvcf = it.Put.ReturnValuesOnConditionCheckFailure
	case it.Delete != nil:
		kind = "DELETE"
		tblName = it.Delete.TableName
		keyMap = it.Delete.Key
		condExpr = it.Delete.ConditionExpression
		names = it.Delete.ExpressionAttributeNames
		values = it.Delete.ExpressionAttributeValues
		rvcf = it.Delete.ReturnValuesOnConditionCheckFailure
	case it.Update != nil:
		kind = "UPDATE"
		tblName = it.Update.TableName
		keyMap = it.Update.Key
		updateExpr = it.Update.UpdateExpression
		condExpr = it.Update.ConditionExpression
		names = it.Update.ExpressionAttributeNames
		values = it.Update.ExpressionAttributeValues
		rvcf = it.Update.ReturnValuesOnConditionCheckFailure
	case it.ConditionCheck != nil:
		kind = "CONDITION_CHECK"
		tblName = it.ConditionCheck.TableName
		keyMap = it.ConditionCheck.Key
		condExpr = it.ConditionCheck.ConditionExpression
		names = it.ConditionCheck.ExpressionAttributeNames
		values = it.ConditionCheck.ExpressionAttributeValues
		rvcf = it.ConditionCheck.ReturnValuesOnConditionCheckFailure
	default:
		return nil, validationErr("TransactItems can only contain one of Put, Delete, Update or ConditionCheck")
	}
	if tblName == "" {
		return nil, validationErr("TableName cannot be empty")
	}
	if rvcf != "" && rvcf != "ALL_OLD" && rvcf != "NONE" {
		return nil, validationErr("ReturnValuesOnConditionCheckFailure must be ALL_OLD or NONE")
	}
	table, err := db.Table(tblName)
	if err != nil {
		return nil, err
	}
	var op txnOp
	op.kind = kind
	op.table = table
	op.names = names
	op.values = values
	op.retOld = rvcf == "ALL_OLD"
	if kind == "PUT" {
		if len(itemMap) == 0 {
			return nil, schemaErr()
		}
		enc, key, kerr := table.keyOf(itemMap)
		if kerr != nil {
			return nil, kerr
		}
		op.enc = enc
		op.key = key
		op.item = itemMap
	} else {
		if len(keyMap) == 0 {
			return nil, validationErr("The Key parameter is required for a %s operation in TransactItems", transactionKindLabel(kind))
		}
		enc, key, kerr := keyFromMap(table, keyMap)
		if kerr != nil {
			return nil, kerr
		}
		op.enc = enc
		op.key = key
	}
	cond, cerr := compileCondition("ConditionExpression", condExpr, names, values)
	if cerr != nil {
		return nil, cerr
	}
	op.cond = cond
	if kind == "UPDATE" {
		prog, uerr := compileUpdate(updateExpr, names, values)
		if uerr != nil {
			return nil, uerr
		}
		if name, hit := prog.touchesKey(table.HashKey, table.RangeKey, names); hit {
			return nil, validationErr("One or more parameter values were invalid: Cannot update attribute %s. This attribute is part of the key", name)
		}
		op.prog = prog
	}
	op.lockKey = table.Name + "\x00" + op.enc
	return &op, nil
}

// TransactWriteItems executes up to 25 actions atomically: either all are
// applied or none are. Conflicting writes to the same item within one
// request are rejected; concurrent transactions touching the same items get
// TransactionConflictException; failed conditions cancel the transaction and
// are reported per action via TransactionCanceledException.
func (db *DB) TransactWriteItems(in *TransactWriteItemsInput) (map[string]any, *APIError) {
	if in == nil {
		in = &TransactWriteItemsInput{}
	}
	if len(in.TransactItems) == 0 {
		return nil, validationErr("TransactionRequest can not be empty")
	}
	if len(in.TransactItems) > maxTransactItems {
		return nil, validationErr("Transaction request cannot contain more than 25 actions")
	}
	mode := strings.ToUpper(in.ReturnConsumedCapacity)
	switch mode {
	case "", "NONE", "TOTAL", "INDEXES":
	default:
		return nil, validationErr("ReturnConsumedCapacity must be one of TOTAL, INDEXES or NONE")
	}

	// Validate and normalize every action up front.
	ops := make([]*txnOp, 0, len(in.TransactItems))
	for _, it := range in.TransactItems {
		op, aerr := db.normalizeTxnOp(it)
		if aerr != nil {
			return nil, aerr
		}
		ops = append(ops, op)
	}

	// Conflict detection: the same item must appear at most once.
	lockKeys := make([]string, len(ops))
	seen := make(map[string]int, len(ops))
	for i, op := range ops {
		lockKeys[i] = op.lockKey
		if j, ok := seen[op.lockKey]; ok {
			return nil, validationErr("Transaction request cannot include multiple operations on one item -- Multiple operations on one item accounted in [%d, %d] of TransactItems", j, i)
		}
		seen[op.lockKey] = i
	}

	// Lock the items for the duration of the transaction; a concurrent
	// transaction that already holds any of them causes a conflict.
	h, aerr := db.beginTxn(lockKeys)
	if aerr != nil {
		return nil, aerr // TransactionConflictException
	}

	// Phase 1: evaluate every condition against the pre-transaction state.
	reasons := make([]*CancellationReason, len(ops))
	cancelled := false
	for i, op := range ops {
		if op.cond == nil {
			continue
		}
		snap, _ := op.table.getItem(op.enc)
		ok, cerr := op.cond.eval(snap)
		if cerr != nil {
			h.Release()
			return nil, cerr
		}
		if !ok {
			cancelled = true
			r := &CancellationReason{
				Code:    "ConditionalCheckFailed",
				Message: strPtr("The conditional request failed"),
			}
			if op.retOld && snap != nil {
				r.Item = snap
			}
			reasons[i] = r
		}
	}
	if cancelled {
		h.Release()
		return nil, transactionCanceledErr(reasons)
	}

	// Phase 2: apply all mutations in request order. Any unexpected failure
	// rolls the whole transaction back (atomicity).
	type undoRec struct {
		table   *Table
		enc     string
		existed bool
		item    map[string]*AttributeValue
	}
	var undos []undoRec
	rollback := func() {
		for i := len(undos) - 1; i >= 0; i-- {
			u := undos[i]
			if u.existed {
				u.table.putItemRaw(u.enc, u.item)
			} else {
				u.table.deleteItem(u.enc)
			}
		}
	}
	for _, op := range ops {
		snap, existed := op.table.getItem(op.enc)
		var aerr *APIError
		switch op.kind {
		case "PUT":
			op.table.putItemRaw(op.enc, op.item)
		case "DELETE":
			op.table.deleteItem(op.enc)
		case "UPDATE":
			aerr = op.table.applyUpdate(op.enc, op.key, op.prog, op.names, op.values)
		}
		if aerr != nil {
			rollback()
			h.Release()
			return nil, aerr
		}
		undos = append(undos, undoRec{table: op.table, enc: op.enc, existed: existed, item: snap})
	}
	h.Release()

	if mode != "TOTAL" && mode != "INDEXES" {
		return nil, nil
	}
	order := make([]string, 0, len(ops))
	counts := make(map[string]int, len(ops))
	for _, op := range ops {
		if _, ok := counts[op.table.Name]; !ok {
			order = append(order, op.table.Name)
		}
		counts[op.table.Name]++
	}
	caps := make([]any, 0, len(order))
	for _, name := range order {
		units := json.RawMessage(strconv.FormatFloat(float64(counts[name]), 'f', 1, 64))
		entry := map[string]any{"TableName": name, "CapacityUnits": units}
		if mode == "INDEXES" {
			entry["Table"] = map[string]any{"CapacityUnits": units}
		}
		caps = append(caps, entry)
	}
	return map[string]any{"ConsumedCapacity": caps}, nil
}

// Wire handler ---------------------------------------------------------------------

// TransactWriteItemsRoute is the X-Amz-Target handled below.
const TransactWriteItemsRoute = "DynamoDB_20120810.TransactWriteItems"

// HandleTransactWriteItems processes the raw request body and returns the
// HTTP status code and the JSON response body to send on the wire.
func HandleTransactWriteItems(db *DB, body []byte) (int, []byte) {
	in, aerr := ParseTransactWriteItems(body)
	if aerr != nil {
		return aerr.Status, aerr.Body()
	}
	out, aerr := db.TransactWriteItems(in)
	if aerr != nil {
		return aerr.Status, aerr.Body()
	}
	if out == nil {
		return 200, []byte("{}")
	}
	b, jerr := json.Marshal(out)
	if jerr != nil {
		e := internalErr(jerr)
		return e.Status, e.Body()
	}
	return 200, b
}
