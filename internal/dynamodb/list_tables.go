// Package dynamodb implements the DynamoDB API actions emulated by floci-go.
// This file carries the ListTables action: an alphabetically sorted,
// paginated listing of the tables held by the store.
package dynamodb

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
)

// listTablesDefaultLimit is the page size DynamoDB uses when the caller does
// not restrict ListTables with an explicit Limit.
const listTablesDefaultLimit = 100

// tableNameRegexString is the documented table name syntax. The same
// constraints apply to ExclusiveStartTableName because that parameter is a
// cursor into the sorted table name space.
const tableNameRegexString = `[a-zA-Z0-9_.-]+`

var tableNameRegex = regexp.MustCompile(tableNameRegexString)

// ListTablesInput mirrors the DynamoDB ListTables request structure.
type ListTablesInput struct {
	// ExclusiveStartTableName resumes the listing after this table name.
	ExclusiveStartTableName *string
	// Limit caps the number of table names returned on one page.
	Limit *int
}

// ListTablesOutput mirrors the DynamoDB ListTables response structure.
type ListTablesOutput struct {
	// LastEvaluatedTableName is only present when at least one more table
	// follows this page; pass it back as ExclusiveStartTableName to fetch
	// the next page.
	LastEvaluatedTableName *string `json:"LastEvaluatedTableName,omitempty"`
	// TableNames lists this page's names in alphabetical order.
	TableNames []string `json:"TableNames,omitempty"`
}

// ListTables answers the ListTables action with an alphabetically sorted,
// paginated list of the stored table names.
func ListTables(s *Store, in ListTablesInput) (ListTablesOutput, error) {
	return listTableNames(s.snapshotTableNames(), in)
}

// listTableNames implements the ListTables pagination rules on top of a
// snapshot of table names, which keeps the paging rules testable in
// isolation.
func listTableNames(names []string, in ListTablesInput) (ListTablesOutput, error) {
	limit := listTablesDefaultLimit
	if in.Limit != nil {
		limit = *in.Limit
		if limit < 1 {
			return ListTablesOutput{}, &opError{
				StatusCode: 400,
				Type:       typeValidationException,
				Message: fmt.Sprintf("1 validation error detected: Value '%d' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1", limit),
			}
		}
	}

	start := ""
	if in.ExclusiveStartTableName != nil {
		start = *in.ExclusiveStartTableName
		if start != "" {
			if err := checkTableNameShape(start, "exclusiveStartTableName"); err != nil {
				return ListTablesOutput{}, err
			}
		}
	}

	sort.Strings(names)

	// ExclusiveStartTableName is an exclusive cursor: the page starts with
	// the first name strictly greater than the cursor.
	pos := sort.Search(len(names), func(i int) bool { return names[i] > start })

	end := pos + limit
	if end > len(names) {
		end = len(names)
	}
	page := names[pos:end]

	out := ListTablesOutput{}
	if len(page) > 0 {
		out.TableNames = page
		if end < len(names) {
			// Another page follows, so hand a resume cursor to the client.
			out.LastEvaluatedTableName = &page[len(page)-1]
		}
	}
	return out, nil
}

// checkTableNameShape enforces the documented table name constraints and
// renders violations the way DynamoDB does on the wire, including the
// attribute path quoted in its validation messages.
func checkTableNameShape(name, attribute string) error {
	if len(name) < 3 {
		return &opError{
			StatusCode: 400,
			Type:       typeValidationException,
			Message: fmt.Sprintf("1 validation error detected: Value '%s' at '%s' failed to satisfy constraint: Member must have length greater than or equal to 3", name, attribute),
		}
	}
	if len(name) > 255 {
		return &opError{
			StatusCode: 400,
			Type:       typeValidationException,
			Message: fmt.Sprintf("1 validation error detected: Value '%s' at '%s' failed to satisfy constraint: Member must have length less than or equal to 255", name, attribute),
		}
	}
	if !tableNameRegex.MatchString(name) {
		return &opError{
			StatusCode: 400,
			Type:       typeValidationException,
			Message: fmt.Sprintf("1 validation error detected: Value '%s' at '%s' failed to satisfy constraint: Member must satisfy regular expression pattern: %s", name, attribute, tableNameRegexString),
		}
	}
	return nil
}

// ServeListTables processes a raw ListTables request body and returns the
// HTTP status code plus the JSON response. The transport layer owns
// Content-Type "application/x-amz-json-1.0" and mirrors a failing body's
// __type into its x-amzn-ErrorType response header.
func ServeListTables(s *Store, body []byte) (int, []byte) {
	var in ListTablesInput
	if err := json.Unmarshal(body, &in); err != nil {
		raw, _ := json.Marshal(map[string]string{
			"__type":  typeSerializationException,
			"Message": "Failed to serialize the request.",
		})
		return 400, raw
	}
	out, err := ListTables(s, in)
	if err != nil {
		return errReply(err)
	}
	resp, _ := json.Marshal(out)
	return 200, resp
}
