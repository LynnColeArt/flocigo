package ddb

func (s *Service) getItem(w http.ResponseWriter, r *http.Request) {
	out, aerr := s.doGetItem(r)
	if aerr != nil {
		ddbWriteError(w, aerr)
		return
	}
	ddbWriteResult(w, out)
}

func (s *Service) doGetItem(r *http.Request) (map[string]any, *apiErr) {
	req, aerr := ddbDecodeRequest(r)
	if aerr != nil {
		return nil, aerr
	}
	tn := reqString(req, "TableName")
	if tn == "" || len(tn) < 3 || len(tn) > 255 {
		return nil, ddbError("ValidationException", msgBadTableName)
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
	projPaths, aerr := parseProjectionParams(req)
	if aerr != nil {
		return nil, aerr
	}
	item, has := t.getItem(key)
	out := map[string]any{}
	if has {
		itemOut := item
		if projPaths != nil {
			itemOut = projectItem(item, projPaths)
		}
		if len(itemOut) > 0 {
			out["Item"] = itemOut
		}
	}
	consistentRead, hasCR := reqBoolValue(req, "ConsistentRead")
	units := 0.5
	if hasCR && consistentRead {
		units = 1.0
	}
	addConsumedCapacity(out, rcc, tn, units)
	return out, nil
}
