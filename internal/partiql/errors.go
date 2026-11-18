// Package partiql implements the DynamoDB expression language used by the
// Java implementation: condition expressions, update expressions, key
// condition expressions and projection expressions, together with the exact
// ValidationException taxonomy. Grammar failures reproduce the ANTLR-style
// message of the Java parsers (the offending token plus nearby text):
//
//	Invalid UpdateExpression: Syntax error; token: "<EOF>", near: "a = :v SET"
//
// and evaluation failures follow the Java message catalogue (document paths,
// missing attributes, operand type mismatches, overlapping update paths,
// number overflow/underflow, empty sets, unused expression attributes).
//
// Translated from:
//	- ExpressionEvaluator.java (condition grammar + evaluation)
//	- DynamoDbUpdateExpressionSyntax.java (update grammar + document paths)
//	- DynamoDbConditionKeys.java (key condition structure + validation)
package partiql

import (
	"fmt"
	"strings"
)

// CodeValidation is the AWS error code for every expression failure.
const CodeValidation = "ValidationException"

// ExprKind names the expression clause of an API call. It supplies the
// "Invalid <Kind>: ..." prefix the Java parsers put on grammar errors.
type ExprKind string

const (
	KindCondition    ExprKind = "ConditionExpression"
	KindConditionChk ExprKind = "ConditionCheckExpression"
	KindFilter       ExprKind = "FilterExpression"
	KindKeyCondition ExprKind = "KeyConditionExpression"
	KindUpdate       ExprKind = "UpdateExpression"
	KindProjection   ExprKind = "ProjectionExpression"
)

// ValidationError carries the AWS error code and exact message, mirroring
// the Java ValidationException.
type ValidationError struct {
	Code string
	Msg  string
}

func (e *ValidationError) Error() string { return e.Code + ": " + e.Msg }

func verr(code, format string, args ...interface{}) *ValidationError {
	return &ValidationError{Code: code, Msg: fmt.Sprintf(format, args...)}
}

func invalidf(kind ExprKind, format string, args ...interface{}) *ValidationError {
	return verr(CodeValidation, "Invalid %s: %s", string(kind), fmt.Sprintf(format, args...))
}

// syntaxErr reproduces the ANTLR-style grammar failure of the Java parsers.
func syntaxErr(kind ExprKind, tok, near string) *ValidationError {
	return invalidf(kind, "Syntax error; token: %q, near: %q", tok, near)
}

// reservedKeywordErr stays unprefixed, as in the Java implementation.
func reservedKeywordErr(word string) *ValidationError {
	return verr(CodeValidation, "Attribute name is a reserved keyword; reserved keyword: %s", word)
}

func nameNotDefinedErr(ref string) *ValidationError {
	return verr(CodeValidation, "An expression attribute name used in the expression is not defined; attribute name: %s", ref)
}

func valueNotDefinedErr(ref string) *ValidationError {
	return verr(CodeValidation, "An expression attribute value used in the expression is not defined; attribute value: %s", ref)
}

// operandTypeErr is the shared "wrong operand type" failure of the Java
// evaluator, used for comparators, functions and update operators alike.
func operandTypeErr(kind ExprKind, op, operand string) *ValidationError {
	return invalidf(kind, "Incorrect operand type for operator or value; operator: %s, operand type: %s", op, operand)
}

// documentPathErr stays unprefixed; the Java evaluator raises it whenever
// navigation to a write target is impossible.
func documentPathErr() *ValidationError {
	return verr(CodeValidation, "The document path provided in the expression is invalid for update")
}

// missingAttrErr stays unprefixed and covers value-side references (SET
// operands, list_append arguments) to attributes that are absent.
func missingAttrErr() *ValidationError {
	return verr(CodeValidation, "The provided expression refers to an attribute that does not exist in the item")
}

func overlapErr(kind ExprKind) *ValidationError {
	return invalidf(kind, "Two document paths overlap with each other")
}

func addDeleteDupErr() *ValidationError {
	return invalidf(KindUpdate, "Attempting to ADD or DELETE the same item twice")
}

func inLimitErr(kind ExprKind) *ValidationError {
	return invalidf(kind, "IN operand list contains more than 100 elements")
}

func divisionByZeroErr() *ValidationError {
	return verr(CodeValidation, "The expression contains a division by zero")
}

func numberOverflowErr() *ValidationError {
	return verr(CodeValidation, "Number overflow. Resulting number is too large")
}

func numberUnderflowErr() *ValidationError {
	return verr(CodeValidation, "Number underflow. Resulting number is too close to zero")
}

// emptySetErr mirrors the Java set validation (including its historical
// "An string set" wording).
func emptySetErr(dynType string) *ValidationError {
	return verr(CodeValidation, "One or more parameter values were invalid: An %s set may not be empty", setWord(dynType))
}

func setWord(t string) string {
	switch t {
	case TypeSS:
		return "string"
	case TypeNS:
		return "number"
	case TypeBS:
		return "binary"
	}
	return t
}

func badValueErr(format string, args ...interface{}) *ValidationError {
	return verr(CodeValidation, "One or more parameter values were invalid: "+format, args...)
}

// Unused expression attribute names / values are reported with the Java
// wording and the sorted list of unused keys.
func unusedValuesErr(keys []string) *ValidationError {
	return verr(CodeValidation, "Value provided in ExpressionAttributeValues unused in expressions: keys: {%s}", strings.Join(keys, ", "))
}

func unusedNamesErr(keys []string) *ValidationError {
	return verr(CodeValidation, "Value provided in ExpressionAttributeNames unused in expressions: keys: {%s}", strings.Join(keys, ", "))
}
