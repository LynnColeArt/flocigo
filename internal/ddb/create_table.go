package ddb

import (
	"fmt"
	"strings"
	"time"
)

type attrDef struct {
	name, typ string
}

func (s *Service) createTable(w http.ResponseWriter, r *http.Request) {
	out, aerr := s.doCreateTable(r)
	if aerr != nil {
		ddbWriteError(w, aerr)
		return
	}
	ddbWriteResult(w, out)
}

func (s *Service) doCreateTable(r *http.Request) (map[string]any, *apiErr) {
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
	if _, exists := s.store.tables[tn]; exists {
		return nil, ddbErrorf("ResourceInUseException", "Table already exists: %s", tn)
	}
	attrDefs := map[string]string{}
	var defs []attrDef
	rawDefs, _ := reqListValue(req, "AttributeDefinitions")
	for i, el := range rawDefs {
		m, ok := el.(map[string]any)
		if !ok {
			return nil, ddbErrorf("ValidationException", "One or more parameter values were invalid: Invalid AttributeDefinition at position %d", i+1)
		}
		name := reqString(m, "AttributeName")
		typ := reqString(m, "AttributeType")
		if name == "" {
			return nil, ddbErrorf("ValidationException", "One or more parameter values were invalid: AttributeName must be present at position %d", i+1)
		}
		if typ != "S" && typ != "N" && typ != "B" {
			return nil, ddbErrorf("ValidationException", "1 validation error detected: Value '%s' at 'attributeDefinitions.%d.member.attributeType' failed to satisfy constraint: Member must satisfy enum value set: [B, N, S]", typ, i+1)
		}
		if _, dup := attrDefs[name]; dup {
			return nil, ddbErrorf("ValidationException", "One or more parameter values were invalid: Duplicate attribute name: %s", name)
		}
		attrDefs[name] = typ
		defs = append(defs, attrDef{name, typ})
	}
	pkTable, skTable, aerr := parseKeySchemaList(req["KeySchema"])
	if aerr != nil {
		return nil, aerr
	}
	if aerr := checkKeyAttrsDefined(attrDefs, []string{pkTable, skTable}, tn); aerr != nil {
		return nil, aerr
	}
	billing := reqString(req, "BillingMode")
	if billing == "" {
		billing = "PROVISIONED"
	}
	if billing != "PROVISIONED" && billing != "PAY_PER_REQUEST" {
		return nil, ddbErrorf("ValidationException", "1 validation error detected: Value '%s' at 'billingMode' failed to satisfy constraint: Member must satisfy enum value set: [PAY_PER_REQUEST, PROVISIONED]", billing)
	}
	rcu, wcu := int64(0), int64(0)
	if ptm, ok := reqMapValue(req, "ProvisionedThroughput"); ok {
		rcu, _ = reqIntValue(ptm, "ReadCapacityUnits")
		wcu, _ = reqIntValue(ptm, "WriteCapacityUnits")
	}
	if billing == "PAY_PER_REQUEST" && (rcu > 0 || wcu > 0) {
		return nil, ddbError("ValidationException", "One or more parameter values were invalid: Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST")
	}
	table := &Table{
		Name:        tn,
		AttrDefs:    attrDefs,
		PK:          pkTable,
		SK:          skTable,
		BillingMode: billing,
		RCU:         rcu,
		WCU:         wcu,
		Items:       map[string]map[string]any{},
		IndexItems:  map[string]map[string]map[string]any{},
		GSIs:        map[string]*IndexDef{},
		LSIs:        map[string]*IndexDef{},
		TableID:     newUUID(),
		Created:     nowFloat(),
	}
	arn := fmt.Sprintf("arn:aws:dynamodb:us-east-1:000000000000:table/%s", tn)
	desc := map[string]any{
		"TableName":             tn,
		"AttributeDefinitions":  attrDefDescs(defs),
		"KeySchema":             keySchemaDesc(pkTable, skTable),
		"ItemCount":             0,
		"TableStatus":           "ACTIVE",
		"CreationDateTime":      table.Created,
		"TableArn":              arn,
		"TableId":               table.TableID,
		"ProvisionedThroughput": ptDesc(billing, rcu, wcu),
	}
	if billing == "PAY_PER_REQUEST" {
		desc["BillingModeSummary"] = map[string]any{"BillingMode": "PAY_PER_REQUEST"}
	}
	streamEnabled, streamViewType := false, ""
	if ss, ok := reqMapValue(req, "StreamSpecification"); ok {
		streamEnabled, _ = reqBoolValue(ss, "StreamEnabled")
		streamViewType = reqString(ss, "StreamViewType")
		if streamEnabled {
			switch streamViewType {
			case "KEYS_ONLY", "NEW_IMAGE", "OLD_IMAGE", "NEW_AND_OLD_IMAGES":
			case "":
				streamViewType = "NEW_AND_OLD_IMAGES"
			default:
				return nil, ddbErrorf("ValidationException", "1 validation error detected: Value '%s' at 'streamSpecification.streamViewType' failed to satisfy constraint: Member must satisfy enum value set: [KEYS_ONLY, NEW_AND_OLD_IMAGES, NEW_IMAGE, OLD_IMAGE]", streamViewType)
			}
		}
	}
	table.StreamEnabled = streamEnabled
	table.StreamViewType = streamViewType
	if streamEnabled {
		desc["StreamSpecification"] = map[string]any{"StreamEnabled": true, "StreamViewType": streamViewType}
		label := time.Now().UTC().Format("2006-01-02T15:04:05.000")
		desc["LatestStreamLabel"] = label
		desc["LatestStreamArn"] = arn + "/stream/" + label
	}
	var gsiDescs []map[string]any
	gsiList, _ := reqListValue(req, "GlobalSecondaryIndexes")
	for _, el := range gsiList {
		m, ok := el.(map[string]any)
		if !ok {
			return nil, ddbError("ValidationException", "One or more parameter values were invalid: Invalid GlobalSecondaryIndex")
		}
		name := reqString(m, "IndexName")
		if name == "" {
			return nil, ddbError("ValidationException", "One or more parameter values were invalid: IndexName must be present")
		}
		if _, dup := table.GSIs[name]; dup {
			return nil, ddbErrorf("ValidationException", "One or more parameter values were invalid: Duplicate index name: %s", name)
		}
		ipk, isk, aerr := parseKeySchemaList(m["KeySchema"])
		if aerr != nil {
			return nil, aerr
		}
		if aerr := checkKeyAttrsDefined(attrDefs, []string{ipk, isk}, name); aerr != nil {
			return nil, aerr
		}
		projType, projAttrs, aerr := parseProjectionDef(m["Projection"])
		if aerr != nil {
			return nil, aerr
		}
		ircu, iwcu := rcu, wcu
		if ptm, ok := reqMapValue(m, "ProvisionedThroughput"); ok {
			ircu, _ = reqIntValue(ptm, "ReadCapacityUnits")
			iwcu, _ = reqIntValue(ptm, "WriteCapacityUnits")
		}
		table.GSIs[name] = &IndexDef{Name: name, PK: ipk, SK: isk, ProjType: projType, ProjAttrs: projAttrs}
		gd := map[string]any{
			"IndexName":   name,
			"KeySchema":   keySchemaDesc(ipk, isk),
			"Projection":  projDesc(projType, projAttrs),
			"IndexStatus": "ACTIVE",
			"IndexArn":    fmt.Sprintf("%s/index/%s", arn, name),
		}
		if billing != "PAY_PER_REQUEST" {
			gd["ProvisionedThroughput"] = ptDesc("PROVISIONED", ircu, iwcu)
		}
		gsiDescs = append(gsiDescs, gd)
	}
	var lsiDescs []map[string]any
	lsiList, _ := reqListValue(req, "LocalSecondaryIndexes")
	for _, el := range lsiList {
		m, ok := el.(map[string]any)
		if !ok {
			return nil, ddbError("ValidationException", "One or more parameter values were invalid: Invalid LocalSecondaryIndex")
		}
		name := reqString(m, "IndexName")
		if name == "" {
			return nil, ddbError("ValidationException", "One or more parameter values were invalid: IndexName must be present")
		}
		if _, dup := table.LSIs[name]; dup {
			return nil, ddbErrorf("ValidationException", "One or more parameter values were invalid: Duplicate index name: %s", name)
		}
		ipk, isk, aerr := parseKeySchemaList(m["KeySchema"])
		if aerr != nil {
			return nil, aerr
		}
		if aerr := checkKeyAttrsDefined(attrDefs, []string{ipk, isk}, name); aerr != nil {
			return nil, aerr
		}
		projType, projAttrs, aerr := parseProjectionDef(m["Projection"])
		if aerr != nil {
			return nil, aerr
		}
		table.LSIs[name] = &IndexDef{Name: name, PK: ipk, SK: isk, LSI: true, ProjType: projType, ProjAttrs: projAttrs}
		ld := map[string]any{
			"IndexName":      name,
			"KeySchema":      keySchemaDesc(ipk, isk),
			"Projection":     projDesc(projType, projAttrs),
			"IndexSizeBytes": 0,
			"ItemCount":      0,
			"IndexArn":       fmt.Sprintf("%s/index/%s", arn, name),
		}
		lsiDescs = append(lsiDescs, ld)
	}
	if len(gsiDescs) > 0 {
		desc["GlobalSecondaryIndexes"] = gsiDescs
	}
	if len(lsiDescs) > 0 {
		desc["LocalSecondaryIndexes"] = lsiDescs
	}
	s.store.tables[tn] = table
	return map[string]any{"TableDescription": desc}, nil
}

func nowFloat() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

func attrDefDescs(defs []attrDef) []map[string]any {
	out := make([]map[string]any, 0, len(defs))
	for _, d := range defs {
		out = append(out, map[string]any{"AttributeName": d.name, "AttributeType": d.typ})
	}
	return out
}

func keySchemaDesc(pk, sk string) []map[string]any {
	out := []map[string]any{{"AttributeName": pk, "KeyType": "HASH"}}
	if sk != "" {
		out = append(out, map[string]any{"AttributeName": sk, "KeyType": "RANGE"})
	}
	return out
}

func parseKeySchemaList(v any) (string, string, *apiErr) {
	list, ok := v.([]any)
	if !ok {
		return "", "", ddbError("ValidationException", "One or more parameter values were invalid: KeySchema must be a list")
	}
	var pk, sk string
	for i, el := range list {
		m, ok := el.(map[string]any)
		if !ok {
			return "", "", ddbErrorf("ValidationException", "One or more parameter values were invalid: Invalid KeySchema member at position %d", i+1)
		}
		name := reqString(m, "AttributeName")
		kt := reqString(m, "KeyType")
		if name == "" {
			return "", "", ddbErrorf("ValidationException", "One or more parameter values were invalid: AttributeName must be present in KeySchema member %d", i+1)
		}
		if kt != "HASH" && kt != "RANGE" {
			return "", "", ddbErrorf("ValidationException", "1 validation error detected: Value '%s' at 'keySchema.%d.member.keyType' failed to satisfy constraint: Member must satisfy enum value set: [HASH, RANGE]", kt, i+1)
		}
		if kt == "HASH" {
			if pk != "" {
				return "", "", ddbError("ValidationException", "One or more parameter values were invalid: The KeySchema contains more than one HASH key element")
			}
			pk = name
		} else {
			if sk != "" {
				return "", "", ddbError("ValidationException", "One or more parameter values were invalid: The KeySchema contains more than one RANGE key element")
			}
			sk = name
		}
	}
	if pk == "" {
		return "", "", ddbError("ValidationException", "One or more parameter values were invalid: The KeySchema must contain a HASH key type element")
	}
	if pk == sk {
		return "", "", ddbError("ValidationException", "One or more parameter values were invalid: Both the Hash Key and the Range Key element in the KeySchema cannot have the same name")
	}
	return pk, sk, nil
}

func checkKeyAttrsDefined(attrDefs map[string]string, names []string, indexName string) *apiErr {
	var missing []string
	for _, n := range names {
		if n == "" {
			continue
		}
		if _, ok := attrDefs[n]; !ok {
			missing = append(missing, n)
		}
	}
	if len(missing) > 0 {
		return ddbErrorf("ValidationException", "One or more parameter values were invalid: Some index key attributes are not defined in AttributeDefinitions. Keys: [%s], IndexName: %s", strings.Join(missing, ", "), indexName)
	}
	return nil
}

func ptDesc(billing string, rcu, wcu int64) map[string]any {
	if billing == "PAY_PER_REQUEST" {
		return map[string]any{"NumberOfDecreasesToday": 0}
	}
	return map[string]any{"NumberOfDecreasesToday": 0, "ReadCapacityUnits": rcu, "WriteCapacityUnits": wcu}
}

func parseProjectionDef(v any) (string, []string, *apiErr) {
	m, ok := v.(map[string]any)
	if !ok {
		return "", nil, ddbError("ValidationException", "One or more parameter values were invalid: Projection must be a map")
	}
	pt := reqString(m, "ProjectionType")
	if pt == "" {
		pt = "ALL"
	}
	if pt != "ALL" && pt != "KEYS_ONLY" && pt != "INCLUDE" {
		return "", nil, ddbErrorf("ValidationException", "1 validation error detected: Value '%s' at 'projection.projectionType' failed to satisfy constraint: Member must satisfy enum value set: [ALL, INCLUDE, KEYS_ONLY]", pt)
	}
	var attrs []string
	if lst, ok := reqListValue(m, "NonKeyAttributes"); ok {
		for _, el := range lst {
			s, _ := el.(string)
			attrs = append(attrs, s)
		}
	}
	if pt == "INCLUDE" && len(attrs) == 0 {
		return "", nil, ddbError("ValidationException", "One or more parameter values were invalid: NonKeyAttributes must be present when ProjectionType is INCLUDE")
	}
	return pt, attrs, nil
}

func projDesc(pt string, attrs []string) map[string]any {
	d := map[string]any{"ProjectionType": pt}
	if pt == "INCLUDE" {
		d["NonKeyAttributes"] = attrs
	}
	return d
}
