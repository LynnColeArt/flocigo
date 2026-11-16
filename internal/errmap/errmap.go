// Package errmap maps internal floci-go errors to exact AWS error wire shapes.
// S3 uses REST/XML, DynamoDB uses x-amz-json-1.0, SQS uses query/XML.
package errmap

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
)

type AWSError struct {
	Code    string
	Message string
	Status  int
}

func (e AWSError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

type xmlError struct {
	XMLName  xml.Name `xml:"Error"`
	Code     string   `xml:"Code"`
	Message  string   `xml:"Message"`
	Resource string   `xml:"Resource,omitempty"`
}

func S3XML(w http.ResponseWriter, err AWSError) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(err.Status)
	xml.NewEncoder(w).Encode(xmlError{Code: err.Code, Message: err.Message})
}

func DDBJSON(w http.ResponseWriter, err AWSError) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(err.Status)
	json.NewEncoder(w).Encode(map[string]any{"__type": "com.amazonaws.dynamodb.v20120810#" + err.Code, "message": err.Message})
}

func SQSQuery(w http.ResponseWriter, err AWSError) {
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(200)
	fmt.Fprintf(w, `<ErrorResponse><Error><Type>Sender</Type><Code>%s</Code><Message>%s</Message></Error></ErrorResponse>`, err.Code, err.Message)
}

var (
	NoSuchBucket   = AWSError{Code: "NoSuchBucket", Message: "The specified bucket does not exist", Status: http.StatusNotFound}
	NoSuchKey      = AWSError{Code: "NoSuchKey", Message: "The specified key does not exist", Status: http.StatusNotFound}
	InvalidBucket  = AWSError{Code: "InvalidBucketName", Message: "The specified bucket is not valid", Status: http.StatusBadRequest}
	BucketNotEmpty = AWSError{Code: "BucketNotEmpty", Message: "The bucket you tried to delete is not empty", Status: http.StatusConflict}
	AccessDenied   = AWSError{Code: "AccessDenied", Message: "Access Denied", Status: http.StatusForbidden}
	NotImplemented = func(op string) AWSError {
		return AWSError{Code: "NotImplemented", Message: fmt.Sprintf("floci-go: %s is not yet implemented — use Floci for this operation", op), Status: http.StatusNotImplemented}
	}
	ResourceInUse       = AWSError{Code: "ResourceInUseException", Message: "Requested resource is already in use", Status: http.StatusBadRequest}
	ResourceNotFoundDDB = AWSError{Code: "ResourceNotFoundException", Message: "Requested resource not found", Status: http.StatusBadRequest}
	QueueNotFound       = AWSError{Code: "QueueDoesNotExist", Message: "The specified queue does not exist", Status: http.StatusBadRequest}
	ValidationException = func(msg string) AWSError {
		return AWSError{Code: "ValidationException", Message: msg, Status: http.StatusBadRequest}
	}
)
