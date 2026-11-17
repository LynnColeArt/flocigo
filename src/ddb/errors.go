package ddb

import (
    "encoding/json"
    "fmt"
    "net/http"
)

// APIError represents an error reported over the DynamoDB HTTP-JSON protocol.
type APIError struct {
    Status  int    `json:"-"`
    Type    string `json:"-"`
    Message string `json:"-"`
}

func (e *APIError) Error() string { return e.Type + ": " + e.Message }

func validation(msg string) *APIError {
    return &APIError{Status: 400, Type: "ValidationException", Message: msg}
}

func resourceNotFound(table string) *APIError {
    return &APIError{Status: 400, Type: "ResourceNotFoundException", Message: "Requested resource not found: Table: " + table + " not found"}
}

func throughputErr() *APIError {
    return &APIError{Status: 400, Type: "ProvisionedThroughputExceededException", Message: "The provisioned throughput for the table has been exceeded"}
}

func serializationErr(msg string) *APIError {
    return &APIError{Status: 400, Type: "SerializationException", Message: msg}
}

func internalErr(msg string) *APIError {
    return &APIError{Status: 500, Type: "InternalServerError", Message: msg}
}

// Shape (API model) level validation errors, formatted like the real service.
func memberNullError(path string) *APIError {
    return validation(fmt.Sprintf("1 validation error detected: Value null at '%s' failed to satisfy constraint: Member must not be null", path))
}

func memberMinLengthError(path string) *APIError {
    return validation(fmt.Sprintf("1 validation error detected: Value '[]' at '%s' failed to satisfy constraint: Member must have length greater than 0", path))
}

func memberMaxLengthError(path string, max int) *APIError {
    return validation(fmt.Sprintf("1 validation error detected: Value '[]' at '%s' failed to satisfy constraint: Member must have length less than or equal to %d", path, max))
}

// asAPIError normalizes storage layer errors to API errors.
func asAPIError(err error) *APIError {
    if err == nil {
        return nil
    }
    if ae, ok := err.(*APIError); ok {
        return ae
    }
    return internalErr(err.Error())
}

// Body renders the JSON error document. Both capitalized and lowercase
// message keys are included to satisfy different AWS SDK generations.
func (e *APIError) Body() []byte {
    b, err := json.Marshal(map[string]string{
        "__type":  "com.amazonaws.dynamodb.v20120810#" + e.Type,
        "message": e.Message,
        "Message": e.Message,
    })
    if err != nil {
        return []byte(`{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError"}`)
    }
    return b
}

// WriteTo emits the error over HTTP using the AWS JSON protocol conventions.
func (e *APIError) WriteTo(w http.ResponseWriter) {
    w.Header().Set("Content-Type", "application/x-amz-json-1.0")
    w.Header().Set("X-Amzn-Errortype", e.Type)
    w.WriteHeader(e.Status)
    w.Write(e.Body())
}
