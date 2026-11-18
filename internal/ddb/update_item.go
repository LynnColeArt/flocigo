package ddb

func (s *Service) updateItem(w http.ResponseWriter, r *http.Request) {
	out, aerr := s.doUpdateItem(r)
	if aerr != nil {
		ddbWriteError(w, aerr)
		return
	}
	ddbWriteResult(w, out)
}

func (s *Service) doUpdateItem(r *http.Request) (map[string]any, *apiErr) {
	req, aerr := ddbDecodeRequest(r)
	if aerr != nil {
		return nil, aerr
	}
	tn := reqString(req, "TableName")
	if tn == "" || len(tn) < 3 || len(tn) > 255 {
		return nil, ddbError("ValidationException", msgBadTableName)
	}
	rv := reqString(req, "ReturnValues")
	if aerr := checkReturnValues(rv); aerr != nil {
		return nil, aerr
	}
	rcc := reqString(req, "ReturnConsumedCapacity")
	if aerr := checkRCC(rcc); aerr != nil {
		return nil, aerr
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	t, ok := s.store.tables[tn]
	if !ok {
		return nil, ddbErrorf("ResourceNotFoundException", "Requested resource not found: Table: %s not found", tn)
	}
	keyAny, hasKey := req["Key"]
	km, okMap := keyAny.(map[string]any)
	if !hasKey || !okMap {
		km = map[string]any{}
	}
	key, aerr := itemKeyFromKeyParam(t, km)
	if aerr != nil {
		return nil, aerr
	}
	prev, hasPrev := t.getItem(key)
	cond, aerr := buildCondition(req)
	if aerr != nil {
		return nil, aerr
	}
	if cond != nil {
		target := map[string]any{}
		if hasPrev {
			target = prev
		}
		okc, aerr := cond.eval(target)
		if aerr != nil {
			return nil, aerr
		}
		if !okc {
			return nil, conditionFailed(req, prev, hasPrev)
		}
	}
	newItem := map[string]any{}
	if hasPrev {
		newItem = cloneAttr(prev)
	}
	newItem[t.PK] = key[t.PK]
	if t.SK != "" {
		newItem[t.SK] = key[t.SK]
	}
	var touched []updOp
	if ue, ok := req["UpdateExpression"]; ok {
		ueStr, isStr := ue.(string)
		if !isStr {
			return nil, ddbError("ValidationException", "One or more parameter values were invalid: UpdateExpression must be a string")
		}
		ops, aerr := parseUpdateExpression(ueStr, req["ExpressionAttributeNames"], req["ExpressionAttributeValues"])
		if aerr != nil {
			return nil, aerr
		}
		touched, aerr = applyUpdateOps(newItem, ops, t)
		if aerr != nil {
			return nil, aerr
		}
	}
	if aerr := checkIndexKeyTypes(t, newItem); aerr != nil {
		return nil, aerr
	}
	t.putItem(newItem)
	out := map[string]any{}
	switch rv {
	case "ALL_OLD":
		if hasPrev {
			out["Attributes"] = prev
		}
	case "ALL_NEW":
		out["Attributes"] = newItem
	case "UPDATED_NEW":
		merged := map[string]any{}
		for _, op := range touched {
			v, ok2 := getPathValue(newItem, op.parts)
			if !ok2 {
				continue
			}
			m, ok3 := v.(map[string]any)
			if !ok3 {
				continue
			}
			mergeNested(merged, nestAny(op.parts, m))
		}
		if len(merged) > 0 {
			out["Attributes"] = merged
		}
	case "UPDATED_OLD":
		if hasPrev {
			merged := map[string]any{}
			for _, op := range touched {
				v, ok2 := getPathValue(prev, op.parts)
				if !ok2 {
					continue
				}
				m, ok3 := v.(map[string]any)
				if !ok3 {
					continue
				}
				mergeNested(merged, nestAny(op.parts, m))
			}
			if len(merged) > 0 {
				out["Attributes"] = merged
			}
		}
	}
	addConsumedCapacity(out, rcc, tn, 1.0)
	return out, nil
}
