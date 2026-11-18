package ddb

// apiErr is the DDB-level error: an AWS JSON 1.0 fault with the canonical
// __type identifier and a message. Each operation returns it instead of
// *error so the JSON writer can render Floci's exact fault shape.
type apiErr struct {
	Type    string // e.g. "com.amazonaws.dynamodb.v20120810#ValidationException"
	Message string
}

func (e *apiErr) Error() string { return e.Type + ": " + e.Message }

// ddbShape prefixes the DynamoDB wire fault type.
const ddbShape = "com.amazonaws.dynamodb.v20120810#"

// ValidationException, ResourceNotFoundException and friends.
func errValidation(msg string) *apiErr {
	return &apiErr{Type: ddbShape + "ValidationException", Message: msg}
}

func errResourceNotFound(msg string) *apiErr {
	return &apiErr{Type: ddbShape + "ResourceNotFoundException", Message: msg}
}

func errTableNotFound(msg string) *apiErr {
	return &apiErr{Type: ddbShape + "ResourceNotFoundException", Message: msg}
}

func errKeyNotFound(msg string) *apiErr {
	return &apiErr{Type: ddbShape + "ResourceNotFoundException", Message: msg}
}

func errItemNotFound(msg string) *apiErr {
	return &apiErr{Type: ddbShape + "ResourceNotFoundException", Message: msg}
}

func errConditionalCheckFailed(msg string) *apiErr {
	return &apiErr{Type: ddbShape + "ConditionalCheckFailedException", Message: msg}
}

func errInUse(msg string) *apiErr {
	return &apiErr{Type: ddbShape + "ResourceInUseException", Message: msg}
}

func errInternal(msg string) *apiErr {
	return &apiErr{Type: ddbShape + "InternalFailure", Message: msg}
}
