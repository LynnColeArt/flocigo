// Package tagging implements ARN-keyed resource tag metadata for floci-go's
// DynamoDB emulator. Tags are addressed by resource ARN and cover both
// tables (arn:<p>:dynamodb:<r>:<a>:table/<name>) and their secondary
// indexes (arn:...:table/<name>/index/<name>).
//
// Every API path surfaces the AWS error paths as typed *Error values:
//
//   - ResourceNotFoundException: the ARN is well-formed but no such table
//     or index exists.
//   - InvalidArn: the ARN is malformed or names a non-taggable resource.
//   - ValidationException: the tag payload violates the tag schema.
package tagging

import (
	"errors"
	"fmt"
	"strings"
)

// Wire error codes emulated by this package.
const (
	CodeResourceNotFound = "ResourceNotFoundException"
	CodeInvalidArn       = "InvalidArn"
	CodeValidation       = "ValidationException"
)

// ErrorTypePrefix is the AWS JSON protocol __type prefix used on the wire,
// e.g. com.amazonaws.dynamodb.v20120810#ResourceNotFoundException.
const ErrorTypePrefix = "com.amazonaws.dynamodb.v20120810#"

// Error is a typed, AWS-style error returned by the tagging APIs.
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Is lets errors.Is match any *Error carrying the same code, so callers can
// test against the sentinels below regardless of the specific message.
func (e *Error) Is(target error) bool {
	te, ok := target.(*Error)
	return ok && te != nil && te.Code == e.Code
}

// Sentinels for errors.Is; individual failures carry specific messages.
var (
	// ErrResourceNotFound matches ResourceNotFoundException failures.
	ErrResourceNotFound = &Error{Code: CodeResourceNotFound, Message: "Requested resource not found"}
	// ErrInvalidArn matches InvalidArn failures.
	ErrInvalidArn = &Error{Code: CodeInvalidArn, Message: "Invalid resource ARN"}
	// ErrValidation matches ValidationException failures.
	ErrValidation = &Error{Code: CodeValidation, Message: "One or more parameter values were invalid"}
)

func errf(sentinel *Error, format string, args ...any) *Error {
	return &Error{Code: sentinel.Code, Message: fmt.Sprintf(format, args...)}
}

// IsInvalidArn reports whether err is (or wraps) an InvalidArn error.
func IsInvalidArn(err error) bool { return hasCode(err, CodeInvalidArn) }

// IsResourceNotFound reports whether err is (or wraps) a
// ResourceNotFoundException error.
func IsResourceNotFound(err error) bool { return hasCode(err, CodeResourceNotFound) }

// IsValidation reports whether err is (or wraps) a ValidationException.
func IsValidation(err error) bool { return hasCode(err, CodeValidation) }

func hasCode(err error, code string) bool {
	var e *Error
	return errors.As(err, &e) && e.Code == code
}

// Kind enumerates the taggable resource kinds.
type Kind string

const (
	KindTable Kind = "table"
	KindIndex Kind = "index"
)

// ARN is a parsed DynamoDB resource ARN for a table or one of its indexes.
type ARN struct {
	Partition string
	Region    string
	AccountID string
	Table     string
	Index     string // empty for table ARNs
	Kind      Kind
}

// String renders the canonical ARN form used as the tag-store key.
func (a ARN) String() string {
	if a.Kind == KindIndex {
		return fmt.Sprintf("arn:%s:dynamodb:%s:%s:table/%s/index/%s", a.Partition, a.Region, a.AccountID, a.Table, a.Index)
	}
	return fmt.Sprintf("arn:%s:dynamodb:%s:%s:table/%s", a.Partition, a.Region, a.AccountID, a.Table)
}

// ParseARN parses and validates a DynamoDB table or index resource ARN.
// Malformed input, non-dynamodb services, non-taggable resource types and
// empty resource names produce an *Error with code InvalidArn.
func ParseARN(raw string) (ARN, error) {
	a := ARN{}
	s := strings.TrimSpace(raw)
	if s == "" {
		return a, errf(ErrInvalidArn, "resource ARN must not be empty")
	}
	parts := strings.Split(s, ":")
	if len(parts) != 6 {
		return a, errf(ErrInvalidArn, "Invalid ARN: %q does not have six components", s)
	}
	if parts[0] != "arn" {
		return a, errf(ErrInvalidArn, "Invalid ARN: %q does not start with 'arn:'", s)
	}
	partition := strings.ToLower(parts[1])
	service := strings.ToLower(parts[2])
	region, accountID, resource := parts[3], parts[4], parts[5]
	switch {
	case partition == "":
		return a, errf(ErrInvalidArn, "Invalid ARN: %q has an empty partition", s)
	case service != "dynamodb":
		return a, errf(ErrInvalidArn, "Invalid ARN: service %q is not taggable, only dynamodb resources are", parts[2])
	case region == "":
		return a, errf(ErrInvalidArn, "Invalid ARN: %q has an empty region", s)
	case !isAccountID(accountID):
		return a, errf(ErrInvalidArn, "Invalid ARN: %q has a malformed account id %q", s, accountID)
	}
	segs := strings.Split(resource, "/")
	switch {
	case len(segs) == 2 && segs[0] == "table" && segs[1] != "":
		a = ARN{Partition: partition, Region: region, AccountID: accountID, Table: segs[1], Kind: KindTable}
	case len(segs) == 4 && segs[0] == "table" && segs[1] != "" && segs[2] == "index" && segs[3] != "":
		a = ARN{Partition: partition, Region: region, AccountID: accountID, Table: segs[1], Index: segs[3], Kind: KindIndex}
	default:
		return a, errf(ErrInvalidArn, "Invalid ARN: resource %q is not a taggable table or index", resource)
	}
	return a, nil
}

// isAccountID reports whether s is a 12-digit AWS account id.
func isAccountID(s string) bool {
	if len(s) != 12 {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// Tag is a single resource tag (the wire shape of the DynamoDB Tag member).
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

const (
	maxTagsPerResource = 50
	maxTagKeyLength    = 128
	maxTagValueLength  = 256
)

// validateTags enforces the CreateTable/TagResource tag contract.
func validateTags(tags []Tag) error {
	if len(tags) == 0 {
		return errf(ErrValidation, "Tags must not be empty")
	}
	if len(tags) > maxTagsPerResource {
		return errf(ErrValidation, "Number of tags exceeds the maximum of %d", maxTagsPerResource)
	}
	seen := make(map[string]struct{}, len(tags))
	for i, t := range tags {
		if t.Key == "" {
			return errf(ErrValidation, "Tag at index %d: key must not be empty", i)
		}
		if len(t.Key) > maxTagKeyLength {
			return errf(ErrValidation, "Tag at index %d: key exceeds the maximum length of %d", i, maxTagKeyLength)
		}
		if strings.HasPrefix(strings.ToLower(t.Key), "aws:") {
			return errf(ErrValidation, "Tag at index %d: key %q uses the reserved aws: prefix", i, t.Key)
		}
		if len(t.Value) > maxTagValueLength {
			return errf(ErrValidation, "Tag at index %d: value exceeds the maximum length of %d", i, maxTagValueLength)
		}
		if _, dup := seen[t.Key]; dup {
			return errf(ErrValidation, "Duplicate tag key %q", t.Key)
		}
		seen[t.Key] = struct{}{}
	}
	return nil
}

// validateTagKeys enforces the UntagResource TagKeys contract.
func validateTagKeys(keys []string) error {
	if len(keys) == 0 {
		return errf(ErrValidation, "TagKeys must contain at least one key")
	}
	for i, key := range keys {
		if key == "" {
			return errf(ErrValidation, "TagKeys at index %d must not be empty", i)
		}
		if len(key) > maxTagKeyLength {
			return errf(ErrValidation, "TagKeys at index %d exceeds the maximum length of %d", i, maxTagKeyLength)
		}
	}
	return nil
}
