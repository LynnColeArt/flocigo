package ddb

import (
	"net/http"
)

func (s *Service) putItem(w http.ResponseWriter, r *http.Request) {
	out, aerr := s.doPutItem(r)
	if aerr != nil {
		ddbWriteError(w, aerr)
		return
	}
	ddbWriteResult(w, out)
}

func (s *Service) doPutItem(r *http.Request) (map[string]any, *apiErr) {
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
	itemAny, hasItem := req["Item"]
	itemMap, okMap := itemAny.(map[string]any)
	if !hasItem || !okMap {
		itemMap = map[string]any{}
	}
	item := cloneAttr(itemMap)
	normalizeItemAttrs(item)
	key, aerr := itemKeyFromAttrs(t, item)
	if aerr != nil {
		return nil, aerr
	}
	if aerr := validateItemAttrValues(item); aerr != nil {
		return nil, aerr
	}
	if aerr := checkIndexKeyTypes(t, item); aerr != nil {
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
	storedPrev, replaced := t.putItem(item)
	out := map[string]any{}
	if rv == "ALL_OLD" && replaced {
		out["Attributes"] = storedPrev
	}
	addConsumedCapacity(out, rcc, tn, 1.0)
	return out, nil
}
