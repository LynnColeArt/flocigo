package ddb

import (
	"net/http"
)

type eskKey struct {
	pk map[string]any
	sk map[string]any
}

// Query handles the DynamoDB Query action.
func (s *Service) Query(w http.ResponseWriter, r *http.Request) {
	s.query(w, r)
}

func (s *Service) query(w http.ResponseWriter, r *http.Request) {
	out, aerr := s.doQuery(r)
	if aerr != nil {
		ddbWriteError(w, aerr)
		return
	}
	ddbWriteResult(w, out)
}

func (s *Service) doQuery(r *http.Request) (map[string]any, *apiErr) {
	req, aerr := ddbDecodeRequest(r)
	if aerr != nil {
		return nil, aerr
	}
	tn := reqString(req, "TableName")
	if tn == "" || len(tn) < 3 || len(tn) > 255 {
		return nil, ddbError("ValidationException", msgBadTableName)
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	t, ok := s.store.tables[tn]
	if !ok {
		return nil, ddbErrorf("ResourceNotFoundException", "Requested resource not found: Table: %s not found", tn)
	}

	sch := keyRef{pk: t.PK, pkType: t.AttrDefs[t.PK]}
	if t.SK != "" {
		sch.sk = t.SK
		sch.skType = t.AttrDefs[t.SK]
	}
	idxName := reqString(req, "IndexName")
	var idxDef *IndexDef
	if idxName != "" {
		idxDef = t.GSIs[idxName]
		if idxDef == nil {
			idxDef = t.LSIs[idxName]
			sch.lsi = idxDef != nil
		}
		if idxDef == nil {
			return nil, ddbErrorf("ValidationException", "The table does not have the specified index: %s", idxName)
		}
		sch.isIndex = true
		sch.pk = idxDef.PK
		sch.pkType = t.AttrDefs[idxDef.PK]
		if idxDef.SK != "" {
			sch.sk = idxDef.SK
			sch.skType = t.AttrDefs[idxDef.SK]
		} else {
			sch.sk = ""
			sch.skType = ""
		}
	}

	consistentRead, hasCR := reqBoolValue(req, "ConsistentRead")
	if hasCR && consistentRead && sch.isIndex && !sch.lsi {
		return nil, ddbError("ValidationException", "Consistent reads are not supported on global secondary indexes")
	}

	rcc := reqString(req, "ReturnConsumedCapacity")
	if aerr := checkRCC(rcc); aerr != nil {
		return nil, aerr
	}

	matcher, aerr := buildKeyCondition(req, sch)
	if aerr != nil {
		return nil, aerr
	}

	filter, aerr := buildQueryFilter(req)
	if aerr != nil {
		return nil, aerr
	}
	if filter != nil {
		roots := map[string]bool{}
		collectFilterRoots(filter, roots)
		if sch.pk != "" && roots[sch.pk] {
			return nil, ddbErrorf("ValidationException", "Filter Expression can only contain non-primary key attributes: Primary key attribute: %s", sch.pk)
		}
		if sch.sk != "" && roots[sch.sk] {
			return nil, ddbErrorf("ValidationException", "Filter Expression can only contain non-primary key attributes: Primary key attribute: %s", sch.sk)
		}
	}

	countOnly := false
	selectStr := reqString(req, "Select")
	if aerr := checkSelectEnum(selectStr); aerr != nil {
		return nil, aerr
	}
	_, hasPE := req["ProjectionExpression"]
	_, hasATG := req["AttributesToGet"]
	if hasPE && hasATG {
		return nil, ddbError("ValidationException", "Cannot specify both ProjectionExpression and AttributesToGet")
	}
	switch selectStr {
	case "COUNT":
		if hasPE || hasATG {
			return nil, ddbError("ValidationException", "One or more parameter values were invalid: Select type COUNT requires that no projection expression be specified")
		}
		countOnly = true
	case "ALL_PROJECTED_ATTRIBUTES":
		if !sch.isIndex {
			return nil, ddbError("ValidationException", "One or more parameter values were invalid: Select type ALL_PROJECTED_ATTRIBUTES is only supported for Queries with an IndexName")
		}
	case "SPECIFIC_ATTRIBUTES":
		if !hasPE && !hasATG {
			return nil, ddbError("ValidationException", "One or more parameter values were invalid: Select type SPECIFIC_ATTRIBUTES requires a projection expression")
		}
	}
	var projPaths [][]pathPart
	if !countOnly {
		projPaths, aerr = parseProjectionParams(req)
		if aerr != nil {
			return nil, aerr
		}
		if projPaths == nil && idxDef != nil && !sch.lsi && idxDef.ProjType != "ALL" {
			var names []string
			names = append(names, idxDef.PK)
			if idxDef.SK != "" {
				names = append(names, idxDef.SK)
			}
			if idxDef.ProjType == "INCLUDE" {
				names = append(names, idxDef.ProjAttrs...)
			}
			ctxp := &exprCtx{names: map[string]string{}, values: map[string]map[string]any{}, label: "ProjectionExpression"}
			paths := make([][]pathPart, 0, len(names))
			for _, n := range names {
				parts, aerr2 := ctxp.parsePathFromText(n)
				if aerr2 != nil {
					return nil, aerr2
				}
				paths = append(paths, parts)
			}
			projPaths = paths
		}
	}

	limit := int64(0)
	if v, ok := req["Limit"]; ok {
		n, ok2 := reqIntValue(req, "Limit")
		if !ok2 || n <= 0 {
			return nil, ddbErrorf("ValidationException", "1 validation error detected: Value '%s' at 'limit' failed to satisfy constraint: Member must have value greater than 0", reqValueString(v))
		}
		limit = int64(n)
	}

	forward := true
	if v, ok := req["ScanIndexForward"]; ok {
		b, ok2 := v.(bool)
		if !ok2 {
			return nil, ddbError("ValidationException", "One or more parameter values were invalid: ScanIndexForward must be a boolean")
		}
		forward = b
	}

	var esk *eskKey
	if v, ok := req["ExclusiveStartKey"]; ok && v != nil {
		m, okm := v.(map[string]any)
		if !okm {
			return nil, ddbError("ValidationException", "The provided key element does not match the schema")
		}
		esk, aerr = buildExclusiveStartKey(sch, m)
		if aerr != nil {
			return nil, aerr
		}
	}

	entries := t.partitionEntries(idxName, matcher.pk)
	if !forward {
		reverseEntries(entries)
	}
	if esk != nil {
		entries = applyESK(entries, esk, matcher.pk, forward, sch.sk != "")
	}

	items := []map[string]any{}
	count := 0
	scanned := int64(0)
	var lastItem map[string]any
	haveLEK := false
	for i, e := range entries {
		if !matcher.match(e) {
			continue
		}
		scanned = scanned + 1
		keep := true
		if filter != nil {
			keep, aerr = filter.eval(e.item)
			if aerr != nil {
				return nil, aerr
			}
		}
		if keep {
			count++
			if !countOnly {
				if projPaths != nil {
					items = append(items, projectItem(e.item, projPaths))
				} else {
					items = append(items, e.item)
				}
			}
		}
		if limit > 0 && scanned >= limit {
			if i+1 < len(entries) {
				for j := i + 1; j < len(entries); j++ {
					if matcher.match(entries[j]) {
						lastItem = e.item
						haveLEK = true
						break
					}
				}
			}
			break
		}
	}

	out := map[string]any{}
	if !countOnly && len(items) > 0 {
		out["Items"] = items
	}
	out["Count"] = count
	out["ScannedCount"] = scanned
	if haveLEK {
		out["LastEvaluatedKey"] = buildLEK(sch, lastItem)
	}
	units := 0.5
	if hasCR && consistentRead {
		units = 1.0
	}
	addConsumedCapacity(out, rcc, tn, units)
	return out, nil
}

func buildQueryFilter(req map[string]any) (condNode, *apiErr) {
	if v, ok := req["FilterExpression"]; ok && v != nil {
		if _, hasQF := req["QueryFilter"]; hasQF {
			return nil, ddbError("ValidationException", "One or more parameter values were invalid: You cannot specify both FilterExpression and QueryFilter")
		}
		s, isStr := v.(string)
		if !isStr {
			return nil, ddbError("ValidationException", "One or more parameter values were invalid: FilterExpression must be a string")
		}
		return parseConditionExpression(s, req["ExpressionAttributeNames"], req["ExpressionAttributeValues"], "FilterExpression")
	}
	if qf, ok := req["QueryFilter"].(map[string]any); ok {
		return parseLegacyConditions(qf, "QueryFilter")
	}
	return nil, nil
}

func buildLEK(sch keyRef, item map[string]any) map[string]any {
	out := map[string]any{}
	if v, ok := item[sch.pk]; ok {
		out[sch.pk] = v
	}
	if sch.sk != "" {
		if v, ok := item[sch.sk]; ok {
			out[sch.sk] = v
		}
	}
	return out
}

func buildExclusiveStartKey(sch keyRef, m map[string]any) (*eskKey, *apiErr) {
	allowed := 1
	if sch.sk != "" {
		allowed = 2
	}
	if len(m) > allowed {
		return nil, ddbError("ValidationException", "The provided key element does not match the schema")
	}
	pkv, ok := m[sch.pk]
	if !ok {
		return nil, ddbError("ValidationException", "The provided key element does not match the schema")
	}
	pa, ok2 := toAttr(pkv)
	if !ok2 || attrTypeOf(pa) != sch.pkType {
		return nil, ddbError("ValidationException", "The provided key element does not match the schema")
	}
	k := &eskKey{pk: pa}
	if sch.sk != "" {
		skv, ok3 := m[sch.sk]
		if !ok3 {
			return nil, ddbError("ValidationException", "The provided key element does not match the schema")
		}
		sa, ok4 := toAttr(skv)
		if !ok4 || attrTypeOf(sa) != sch.skType {
			return nil, ddbError("ValidationException", "The provided key element does not match the schema")
		}
		k.sk = sa
	}
	return k, nil
}

func applyESK(entries []entry, esk *eskKey, pk map[string]any, forward, hasSk bool) []entry {
	order := compareAttr(esk.pk, pk)
	if forward {
		switch {
		case order > 0:
			return nil
		case order < 0:
			return entries
		}
		if !hasSk {
			return nil
		}
		for i, e := range entries {
			if compareAttr(e.sk, esk.sk) > 0 {
				return entries[i:]
			}
		}
		return nil
	}
	switch {
	case order < 0:
		return nil
	case order > 0:
		return entries
	}
	if !hasSk {
		return nil
	}
	for i, e := range entries {
		if compareAttr(e.sk, esk.sk) < 0 {
			return entries[i:]
		}
	}
	return nil
}

func checkSelectEnum(v string) *apiErr {
	switch v {
	case "", "ALL_ATTRIBUTES", "ALL_PROJECTED_ATTRIBUTES", "SPECIFIC_ATTRIBUTES", "COUNT":
		return nil
	}
	return ddbErrorf("ValidationException", "1 validation error detected: Value '%s' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES, COUNT, SPECIFIC_ATTRIBUTES]", v)
}

func reverseEntries(es []entry) {
	for i, j := 0, len(es)-1; i < j; i, j = i+1, j-1 {
		es[i], es[j] = es[j], es[i]
	}
}
