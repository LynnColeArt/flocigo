package main

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "regexp"
    "strings"
    "time"
)

// DynamoDB table name pattern: [a-zA-Z0-9_.-]+, 3-255 characters.
var tableNameRE = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)

const (
    defaultRCU = int64(10)
    defaultWCU = int64(10)
)

// CreateTableInput is the DynamoDB CreateTable request document. Index, SSE
// and tag fields are parsed but not applied yet; this ticket covers base
// tables with pk/sk key schemas.
type CreateTableInput struct {
    TableName                 string                      `json:"TableName"`
    AttributeDefinitions      []AttributeDefinition       `json:"AttributeDefinitions"`
    KeySchema                 []KeySchemaElement          `json:"KeySchema"`
    BillingMode               string                      `json:"BillingMode"`
    ProvisionedThroughput     *ProvisionedThroughputInput `json:"ProvisionedThroughput"`
    GlobalSecondaryIndexes    json.RawMessage             `json:"GlobalSecondaryIndexes"`
    LocalSecondaryIndexes     json.RawMessage             `json:"LocalSecondaryIndexes"`
    SSESpecification          json.RawMessage             `json:"SSESpecification"`
    Tags                      json.RawMessage             `json:"Tags"`
    DeletionProtectionEnabled bool                        `json:"DeletionProtectionEnabled"`
    TableClass                string                      `json:"TableClass"`
}

type createTableOutput struct {
    TableDescription *TableDescription `json:"TableDescription"`
}

// createTable implements DynamoDB_20120810.CreateTable: validate the request,
// persist the table (pk/sk schema) to the store file, and return a
// TableDescription with TableStatus ACTIVE.
func (s *Server) createTable(w http.ResponseWriter, r *http.Request) {
    body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
    if err != nil {
        writeAPIError(w, errValidation("could not read request body"))
        return
    }
    var in CreateTableInput
    if err := json.Unmarshal(body, &in); err != nil {
        writeAPIError(w, errSerialization())
        return
    }
    if e := validateCreateTable(&in); e != nil {
        writeAPIError(w, e)
        return
    }

    billingMode := in.BillingMode
    if billingMode == "" {
        billingMode = billingProvisioned
    }
    rcu, wcu := defaultRCU, defaultWCU
    if pt := in.ProvisionedThroughput; pt != nil {
        rSpec := pt.ReadCapacityUnits != nil && *pt.ReadCapacityUnits > 0
        wSpec := pt.WriteCapacityUnits != nil && *pt.WriteCapacityUnits > 0
        if (rSpec || wSpec) && billingMode == billingPayPerRequest {
            writeAPIError(w, errValidation("One or more parameter values were invalid: ReadCapacityUnits and WriteCapacityUnits cannot be specified when BillingMode is PAY_PER_REQUEST"))
            return
        }
        if rSpec {
            rcu = *pt.ReadCapacityUnits
        }
        if wSpec {
            wcu = *pt.WriteCapacityUnits
        }
    }
    if billingMode == billingPayPerRequest {
        rcu, wcu = 0, 0
    }

    region, account := s.resolveRegionAccount(r)
    table := &Table{
        TableName:            in.TableName,
        TableID:              newUUID(),
        TableArn:             fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s", region, account, in.TableName),
        TableStatus:          tableStatusActive,
        CreationDateTime:     epochSeconds(time.Now()),
        AttributeDefinitions: in.AttributeDefinitions,
        KeySchema:            in.KeySchema,
        PartitionKey:         in.KeySchema[0].AttributeName,
        PartitionKeyType:     in.KeySchema[0].KeyType,
        BillingMode:          billingMode,
        ReadCapacityUnits:    rcu,
        WriteCapacityUnits:   wcu,
    }
    if len(in.KeySchema) == 2 {
        table.SortKey = in.KeySchema[1].AttributeName
        table.SortKeyType = in.KeySchema[1].KeyType
    }

    exists, err := s.store.CreateTable(table)
    if err != nil {
        writeAPIError(w, ddbErr("InternalServerError", err.Error()))
        return
    }
    if exists {
        writeAPIError(w, errResourceInUse(in.TableName))
        return
    }

    writeJSON(w, http.StatusOK, &createTableOutput{TableDescription: table.description()})
}

// validateCreateTable applies DynamoDB's CreateTable request contract.
func validateCreateTable(in *CreateTableInput) *apiError {
    invalid := func(msg string) *apiError {
        return errValidation("One or more parameter values were invalid: " + msg)
    }

    name := in.TableName
    if name == "" {
        return invalid("TableName must not be empty")
    }
    if len(name) < 3 || len(name) > 255 {
        return errValidation("Table names must be between 3 and 255 characters long")
    }
    if !tableNameRE.MatchString(name) {
        return errValidation("Invalid table name. Table names must match the pattern [a-zA-Z0-9_.-]+")
    }

    if len(in.KeySchema) == 0 {
        return invalid("KeySchema must include a HASH key")
    }
    if len(in.KeySchema) > 2 {
        return invalid("KeySchema cannot have more than a HASH and a RANGE key")
    }
    pk := in.KeySchema[0]
    if pk.KeyType != keyHash {
        return invalid("The first KeySchemaElement must have a KeyType of HASH")
    }
    if pk.AttributeName == "" {
        return invalid("KeySchema AttributeName must not be empty")
    }
    var sk *KeySchemaElement
    if len(in.KeySchema) == 2 {
        second := in.KeySchema[1]
        if second.KeyType != keyRange {
            return invalid("The second KeySchemaElement must have a KeyType of RANGE")
        }
        if second.AttributeName == "" {
            return invalid("KeySchema AttributeName must not be empty")
        }
        if second.AttributeName == pk.AttributeName {
            return invalid("Duplicate key attribute names are not allowed in KeySchema")
        }
        sk = &second
    }

    defs := make(map[string]string, len(in.AttributeDefinitions))
    for i := range in.AttributeDefinitions {
        ad := &in.AttributeDefinitions[i]
        if ad.AttributeName == "" {
            return invalid("AttributeDefinitions AttributeName must not be empty")
        }
        switch ad.AttributeType {
        case attrString, attrNumber, attrBinary:
        case "":
            return invalid("AttributeDefinitions AttributeType must not be empty")
        default:
            return invalid("AttributeType " + ad.AttributeType + " is not supported. Supported types are S (string), N (number) and B (binary)")
        }
        if _, dup := defs[ad.AttributeName]; dup {
            return invalid("Duplicate attribute definition for " + ad.AttributeName)
        }
        defs[ad.AttributeName] = ad.AttributeType
    }

    keyNames := []string{pk.AttributeName}
    if sk != nil {
        keyNames = append(keyNames, sk.AttributeName)
    }
    for _, key := range keyNames {
        if _, ok := defs[key]; !ok {
            return invalid("Some index key attributes are not defined in AttributeDefinitions. Keys: [" +
                strings.Join(keyNames, ", ") + "], AttributeDefinitions: [" + strings.Join(attrNames(in.AttributeDefinitions), ", ") + "]")
        }
    }

    mode := in.BillingMode
    if mode != "" && mode != billingProvisioned && mode != billingPayPerRequest {
        return invalid("BillingMode must be one of PROVISIONED or PAY_PER_REQUEST")
    }
    return nil
}

func attrNames(defs []AttributeDefinition) []string {
    names := make([]string, len(defs))
    for i := range defs {
        names[i] = defs[i].AttributeName
    }
    return names
}
