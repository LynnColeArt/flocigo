package ddb

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
)

// ---- CreateTable request members -------------------------------------------

type provisionedThroughputIn struct {
	ReadCapacityUnits  int64 `json:"ReadCapacityUnits"`
	WriteCapacityUnits int64 `json:"WriteCapacityUnits"`
}

type projectionIn struct {
	ProjectionType   string   `json:"ProjectionType"`
	NonKeyAttributes []string `json:"NonKeyAttributes,omitempty"`
}

type globalSecondaryIndexIn struct {
	IndexName             string                   `json:"IndexName"`
	KeySchema             []KeySchemaElement       `json:"KeySchema"`
	Projection            *projectionIn            `json:"Projection,omitempty"`
	ProvisionedThroughput *provisionedThroughputIn `json:"ProvisionedThroughput,omitempty"`
}

type localSecondaryIndexIn struct {
	IndexName  string             `json:"IndexName"`
	KeySchema  []KeySchemaElement `json:"KeySchema"`
	Projection *projectionIn      `json:"Projection,omitempty"`
}

type tagIn struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// createTableRequest is the CreateTable_2012-08-10 request document.
type createTableRequest struct {
	TableName                 string                   `json:"TableName"`
	AttributeDefinitions      []AttributeDefinition    `json:"AttributeDefinitions"`
	KeySchema                 []KeySchemaElement       `json:"KeySchema"`
	BillingMode               string                   `json:"BillingMode,omitempty"`
	ProvisionedThroughput     *provisionedThroughputIn `json:"ProvisionedThroughput,omitempty"`
	GlobalSecondaryIndexes    []globalSecondaryIndexIn `json:"GlobalSecondaryIndexes,omitempty"`
	LocalSecondaryIndexes     []localSecondaryIndexIn  `json:"LocalSecondaryIndexes,omitempty"`
	Tags                      []tagIn                  `json:"Tags,omitempty"`
	TableClass                string                   `json:"TableClass,omitempty"`
	DeletionProtectionEnabled bool                     `json:"DeletionProtectionEnabled"`
}

// ---- CreateTable response members (TableDescription) ------------------------

type billingModeSummary struct {
	BillingMode string `json:"BillingMode"`
}

type tableClassSummary struct {
	TableClass string `json:"TableClass"`
}

type provisionedThroughputDesc struct {
	NumberOfDecreasesToday int64 `json:"NumberOfDecreasesToday"`
	ReadCapacityUnits      int64 `json:"ReadCapacityUnits"`
	WriteCapacityUnits     int64 `json:"WriteCapacityUnits"`
}

type indexDescription struct {
	IndexName             string                     `json:"IndexName"`
	IndexArn              string                     `json:"IndexArn"`
	IndexStatus           string                     `json:"IndexStatus,omitempty"`
	IndexSizeBytes        int64                      `json:"IndexSizeBytes"`
	ItemCount             int64                      `json:"ItemCount"`
	KeySchema             []KeySchemaElement         `json:"KeySchema"`
	Projection            *projectionIn              `json:"Projection,omitempty"`
	ProvisionedThroughput *provisionedThroughputDesc `json:"ProvisionedThroughput,omitempty"`
}

// tableDescription mirrors the DynamoDB TableDescription document.
type tableDescription struct {
	TableName                 string                     `json:"TableName"`
	TableStatus               string                     `json:"TableStatus"`
	CreationDateTime          float64                    `json:"CreationDateTime"`
	TableArn                  string                     `json:"TableArn"`
	TableId                   string                     `json:"TableId"`
	AttributeDefinitions      []AttributeDefinition      `json:"AttributeDefinitions"`
	KeySchema                 []KeySchemaElement         `json:"KeySchema"`
	BillingModeSummary        *billingModeSummary        `json:"BillingModeSummary,omitempty"`
	ProvisionedThroughput     *provisionedThroughputDesc `json:"ProvisionedThroughput"`
	GlobalSecondaryIndexes    []indexDescription         `json:"GlobalSecondaryIndexes,omitempty"`
	LocalSecondaryIndexes     []indexDescription         `json:"LocalSecondaryIndexes,omitempty"`
	ItemCount                 int64                      `json:"ItemCount"`
	TableSizeBytes            int64                      `json:"TableSizeBytes"`
	DeletionProtectionEnabled bool                       `json:"DeletionProtectionEnabled"`
	TableClassSummary         *tableClassSummary         `json:"TableClassSummary,omitempty"`
}

type createTableResponse struct {
	TableDescription tableDescription `json:"TableDescription"`
}

// serveCreateTable implements CreateTable: validate + persist the pk/sk schema
// in the store file, then answer with an ACTIVE TableDescription. A table name
// that already exists is rejected with ResourceInUseException.
func (s *TableService) serveCreateTable(w http.ResponseWriter, body []byte) {
	var req createTableRequest
	if err := json.Unmarshal(body, &req); err != nil {
		ddbWriteError(w, &ddbError{Status: http.StatusBadRequest, Code: "SerializationException"})
		return
	}
	table, apiErr := s.validateCreateTable(&req)
	if apiErr != nil {
		ddbWriteError(w, apiErr)
		return
	}
	created, err := s.store.CreateTable(table)
	if err != nil {
		if errors.Is(err, ErrResourceInUse) {
			ddbWriteError(w, &ddbError{
				Status:  http.StatusBadRequest,
				Code:    "ResourceInUseException",
				Message: "Table already exists: " + req.TableName,
			})
			return
		}
		ddbWriteError(w, &ddbError{Status: http.StatusInternalServerError, Code: "InternalServerError", Message: err.Error()})
		return
	}
	ddbWriteResponse(w, createTableResponse{TableDescription: *s.describeTable(created)})
}

// ddbTableNamePattern is the allowed table-name character set.
var ddbTableNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)

// validateCreateTable applies the CreateTable constraints and materializes the
// StoredTable record (pk/sk schema) to insert into the store file.
func (s *TableService) validateCreateTable(req *createTableRequest) (*StoredTable, *ddbError) {
	name := req.TableName
	switch {
	case name == "":
		return nil, ddbErrValidation("1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null")
	case len(name) < 3:
		return nil, ddbErrValidation(fmt.Sprintf("1 validation error detected: Value '%s' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3", name))
	case len(name) > 255:
		return nil, ddbErrValidation("1 validation error detected: Value at 'tableName' failed to satisfy constraint: Member must have length less than or equal to 255")
	case !ddbTableNamePattern.MatchString(name):
		return nil, ddbErrValidation("Special characters are not allowed in table names except '.' (period), '-' (hyphen) and '_' (underscore)")
	}

	billing := req.BillingMode
	if billing == "" {
		billing = BillingModeProvisioned
	}
	if billing != BillingModeProvisioned && billing != BillingModePayPerRequest {
		return nil, ddbErrValidation(fmt.Sprintf("1 validation error detected: Value '%s' at 'billingMode' failed to satisfy constraint: Member must satisfy enum value set: [PROVISIONED, PAY_PER_REQUEST]", billing))
	}

	switch req.TableClass {
	case "", "STANDARD", "STANDARD_INFREQUENT_ACCESS":
	default:
		return nil, ddbErrValidation(fmt.Sprintf("1 validation error detected: Value '%s' at 'tableClass' failed to satisfy constraint: Member must satisfy enum value set: [STANDARD, STANDARD_INFREQUENT_ACCESS]", req.TableClass))
	}

	attrTypes := make(map[string]string, len(req.AttributeDefinitions))
	for _, ad := range req.AttributeDefinitions {
		if ad.AttributeName == "" {
			return nil, ddbErrValidation("One or more parameter values were invalid: AttributeName cannot be empty in AttributeDefinitions")
		}
		switch ad.AttributeType {
		case "S", "N", "B":
		default:
			return nil, ddbErrValidation(fmt.Sprintf("One or more parameter values were invalid: AttributeType %s is not valid, must be one of: S, N, or B", ad.AttributeType))
		}
		if _, dup := attrTypes[ad.AttributeName]; dup {
			return nil, ddbErrValidation(fmt.Sprintf("One or more parameter values were invalid: Duplicate AttributeName: %s in AttributeDefinitions", ad.AttributeName))
		}
		attrTypes[ad.AttributeName] = ad.AttributeType
	}

	if len(req.KeySchema) == 0 {
		return nil, ddbErrValidation("One or more parameter values were invalid: The list of KeySchemaElements contains no elements")
	}
	if len(req.KeySchema) > 2 {
		return nil, ddbErrValidation("One or more parameter values were invalid: Number of KeySchemaElements in KeySchema exceeds the limit of 2")
	}
	if req.KeySchema[0].KeyType != KeyTypeHash {
		return nil, ddbErrValidation("Invalid KeySchema: The first KeySchemaElement is not a HASH key type")
	}
	if req.KeySchema[0].AttributeName == "" {
		return nil, ddbErrValidation("One or more parameter values were invalid: KeySchema AttributeName cannot be empty")
	}
	if len(req.KeySchema) == 2 && req.KeySchema[1].KeyType != KeyTypeRange {
		return nil, ddbErrValidation("Invalid KeySchema: The second KeySchemaElement is not a RANGE key type")
	}

	keyAttrs := []string{req.KeySchema[0].AttributeName}
	if len(req.KeySchema) == 2 {
		keyAttrs = append(keyAttrs, req.KeySchema[1].AttributeName)
	}
	if missing := missingKeyAttributes(keyAttrs, attrTypes); len(missing) > 0 {
		return nil, ddbErrValidation(fmt.Sprintf("One or more parameter values were invalid: Some index key attributes are not defined in AttributeDefinitions. Keys: [%s]",
			strings.Join(missing, ", ")))
	}

	if len(req.GlobalSecondaryIndexes) > 20 {
		return nil, ddbErrValidation("One or more parameter values were invalid: Number of GlobalSecondaryIndexes exceeds the limit of 20")
	}
	if len(req.LocalSecondaryIndexes) > 5 {
		return nil, ddbErrValidation("One or more parameter values were invalid: Number of LocalSecondaryIndexes exceeds the limit of 5")
	}

	gsiRecords := make([]IndexRecord, 0, len(req.GlobalSecondaryIndexes))
	lsiRecords := make([]IndexRecord, 0, len(req.LocalSecondaryIndexes))
	indexNames := map[string]bool{}

	for _, gsi := range req.GlobalSecondaryIndexes {
		if gsi.IndexName == "" {
			return nil, ddbErrValidation("One or more parameter values were invalid: IndexName is required for GlobalSecondaryIndexes")
		}
		if indexNames[gsi.IndexName] {
			return nil, ddbErrValidation(fmt.Sprintf("One or more parameter values were invalid: Duplicate index name: %s", gsi.IndexName))
		}
		indexNames[gsi.IndexName] = true
		if apiErr := indexKeySchemaError(gsi.KeySchema); apiErr != nil {
			return nil, apiErr
		}
		if missing := missingKeyAttributes(indexKeyAttributes(gsi.KeySchema), attrTypes); len(missing) > 0 {
			return nil, ddbErrValidation(fmt.Sprintf("One or more parameter values were invalid: Some index key attributes are not defined in AttributeDefinitions. Keys: [%s], IndexName: %s",
				strings.Join(missing, ", "), gsi.IndexName))
		}
		projType := "ALL"
		if gsi.Projection != nil && gsi.Projection.ProjectionType != "" {
			projType = gsi.Projection.ProjectionType
		}
		rec := IndexRecord{
			Name:           gsi.IndexName,
			ARN:            s.indexArnOf(name, gsi.IndexName),
			KeySchema:      gsi.KeySchema,
			ProjectionType: projType,
		}
		if gsi.Projection != nil {
			rec.NonKeyAttributes = gsi.Projection.NonKeyAttributes
		}
		if billing == BillingModeProvisioned && gsi.ProvisionedThroughput != nil {
			rec.ReadCapacityUnits = gsi.ProvisionedThroughput.ReadCapacityUnits
			rec.WriteCapacityUnits = gsi.ProvisionedThroughput.WriteCapacityUnits
		}
		gsiRecords = append(gsiRecords, rec)
	}

	for _, lsi := range req.LocalSecondaryIndexes {
		if lsi.IndexName == "" {
			return nil, ddbErrValidation("One or more parameter values were invalid: IndexName is required for LocalSecondaryIndexes")
		}
		if indexNames[lsi.IndexName] {
			return nil, ddbErrValidation(fmt.Sprintf("One or more parameter values were invalid: Duplicate index name: %s", lsi.IndexName))
		}
		indexNames[lsi.IndexName] = true
		if apiErr := indexKeySchemaError(lsi.KeySchema); apiErr != nil {
			return nil, apiErr
		}
		if lsi.KeySchema[0].AttributeName != keyAttrs[0] {
			return nil, ddbErrValidation(fmt.Sprintf("One or more parameter values were invalid: LocalSecondaryIndex %s must have the same HASH key attribute as the table", lsi.IndexName))
		}
		if missing := missingKeyAttributes(indexKeyAttributes(lsi.KeySchema), attrTypes); len(missing) > 0 {
			return nil, ddbErrValidation(fmt.Sprintf("One or more parameter values were invalid: Some index key attributes are not defined in AttributeDefinitions. Keys: [%s], IndexName: %s",
				strings.Join(missing, ", "), lsi.IndexName))
		}
		projType := "ALL"
		if lsi.Projection != nil && lsi.Projection.ProjectionType != "" {
			projType = lsi.Projection.ProjectionType
		}
		rec := IndexRecord{
			Name:           lsi.IndexName,
			ARN:            s.indexArnOf(name, lsi.IndexName),
			KeySchema:      lsi.KeySchema,
			ProjectionType: projType,
		}
		if lsi.Projection != nil {
			rec.NonKeyAttributes = lsi.Projection.NonKeyAttributes
		}
		lsiRecords = append(lsiRecords, rec)
	}

	tags := make(map[string]string, len(req.Tags))
	for _, tag := range req.Tags {
		if tag.Key == "" {
			return nil, ddbErrValidation("One or more parameter values were invalid: Tag Key cannot be empty")
		}
		tags[tag.Key] = tag.Value
	}

	table := &StoredTable{
		Name:                      name,
		Status:                    StatusActive,
		CreationDateTime:          ddbNow(),
		TableID:                   ddbTableID(),
		ARN:                       s.arnOf(name),
		Region:                    s.region,
		AccountID:                 s.account,
		PK:                        keyAttrs[0],
		KeySchema:                 req.KeySchema,
		AttributeDefinitions:      req.AttributeDefinitions,
		BillingMode:               billing,
		GlobalSecondaryIndexes:    gsiRecords,
		LocalSecondaryIndexes:     lsiRecords,
		TableClass:                req.TableClass,
		DeletionProtectionEnabled: req.DeletionProtectionEnabled,
		Items:                     map[string]json.RawMessage{},
	}
	if len(keyAttrs) == 2 {
		table.SK = keyAttrs[1]
	}
	if billing == BillingModeProvisioned && req.ProvisionedThroughput != nil {
		table.ReadCapacityUnits = req.ProvisionedThroughput.ReadCapacityUnits
		table.WriteCapacityUnits = req.ProvisionedThroughput.WriteCapacityUnits
	}
	if len(tags) > 0 {
		table.Tags = tags
	}
	return table, nil
}

// indexKeySchemaError validates an index KeySchema (at least one HASH element,
// at most one RANGE element).
func indexKeySchemaError(ks []KeySchemaElement) *ddbError {
	switch {
	case len(ks) == 0:
		return ddbErrValidation("One or more parameter values were invalid: The list of KeySchemaElements contains no elements")
	case len(ks) > 2:
		return ddbErrValidation("One or more parameter values were invalid: Number of KeySchemaElements exceeds the limit of 2")
	case ks[0].KeyType != KeyTypeHash:
		return ddbErrValidation("Invalid Index KeySchema: The first KeySchemaElement is not a HASH key type")
	case len(ks) == 2 && ks[1].KeyType != KeyTypeRange:
		return ddbErrValidation("Invalid Index KeySchema: The second KeySchemaElement is not a RANGE key type")
	}
	return nil
}

// indexKeyAttributes returns the attribute names referenced by a key schema.
func indexKeyAttributes(ks []KeySchemaElement) []string {
	out := make([]string, 0, len(ks))
	for _, el := range ks {
		out = append(out, el.AttributeName)
	}
	return out
}

// missingKeyAttributes returns the sorted names from keys absent from defined.
func missingKeyAttributes(keys []string, defined map[string]string) []string {
	var missing []string
	for _, key := range keys {
		if _, ok := defined[key]; !ok {
			missing = append(missing, key)
		}
	}
	sort.Strings(missing)
	return missing
}

// describeTable renders the wire TableDescription for a stored table.
func (s *TableService) describeTable(t *StoredTable) *tableDescription {
	desc := &tableDescription{
		TableName:                 t.Name,
		TableStatus:               t.Status,
		CreationDateTime:          t.CreationDateTime,
		TableArn:                  t.ARN,
		TableId:                   t.TableID,
		AttributeDefinitions:      t.AttributeDefinitions,
		KeySchema:                 t.KeySchema,
		ProvisionedThroughput:     &provisionedThroughputDesc{ReadCapacityUnits: t.ReadCapacityUnits, WriteCapacityUnits: t.WriteCapacityUnits},
		GlobalSecondaryIndexes:    indexDescriptions(t.GlobalSecondaryIndexes, true, t.BillingMode == BillingModeProvisioned),
		LocalSecondaryIndexes:     indexDescriptions(t.LocalSecondaryIndexes, false, false),
		ItemCount:                 int64(len(t.Items)),
		DeletionProtectionEnabled: t.DeletionProtectionEnabled,
	}
	if t.BillingMode == BillingModePayPerRequest {
		desc.BillingModeSummary = &billingModeSummary{BillingMode: t.BillingMode}
	}
	if t.TableClass != "" {
		desc.TableClassSummary = &tableClassSummary{TableClass: t.TableClass}
	}
	return desc
}

// indexDescriptions renders stored index records into wire descriptions.
func indexDescriptions(recs []IndexRecord, global, provisioned bool) []indexDescription {
	if len(recs) == 0 {
		return nil
	}
	out := make([]indexDescription, 0, len(recs))
	for _, rec := range recs {
		projType := rec.ProjectionType
		if projType == "" {
			projType = "ALL"
		}
		desc := indexDescription{
			IndexName:      rec.Name,
			IndexArn:       rec.ARN,
			IndexSizeBytes: 0,
			ItemCount:      0,
			KeySchema:      rec.KeySchema,
			Projection:     &projectionIn{ProjectionType: projType, NonKeyAttributes: rec.NonKeyAttributes},
		}
		if global {
			desc.IndexStatus = StatusActive
			if provisioned {
				desc.ProvisionedThroughput = &provisionedThroughputDesc{
					ReadCapacityUnits:  rec.ReadCapacityUnits,
					WriteCapacityUnits: rec.WriteCapacityUnits,
				}
			}
		}
		out = append(out, desc)
	}
	return out
}
