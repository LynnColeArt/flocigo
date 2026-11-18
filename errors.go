package main

import (
    "encoding/xml"
    "net/http"
)

// s3Error pairs an S3 error code with the HTTP status floci reports it with.
type s3Error struct {
    status  int
    code    string
    message string
}

// Error implements error with a code: message representation.
func (e *s3Error) Error() string { return e.code + ": " + e.message }

// The S3 error codes floci-go reports.
var (
    // ErrInvalidBucketName: the name violates the bucket naming rules (400).
    ErrInvalidBucketName = &s3Error{
        status:  http.StatusBadRequest,
        code:    "InvalidBucketName",
        message: "The specified bucket is not valid.",
    }
    // ErrBucketAlreadyOwnedByYou: the bucket already exists and is yours.
    // floci treats CreateBucket as idempotent and folds this into a 200.
    ErrBucketAlreadyOwnedByYou = &s3Error{
        status:  http.StatusOK,
        code:    "BucketAlreadyOwnedByYou",
        message: "Your previous request to create the named bucket succeeded and you already own it.",
    }
    // ErrNoSuchBucket: no such bucket (404).
    ErrNoSuchBucket = &s3Error{
        status:  http.StatusNotFound,
        code:    "NoSuchBucket",
        message: "The specified bucket does not exist.",
    }
    // ErrBucketNotEmpty: the bucket still contains objects (409).
    ErrBucketNotEmpty = &s3Error{
        status:  http.StatusConflict,
        code:    "BucketNotEmpty",
        message: "The bucket you tried to delete is not empty.",
    }
    // ErrMethodNotAllowed: unsupported method on this resource (405).
    ErrMethodNotAllowed = &s3Error{
        status:  http.StatusMethodNotAllowed,
        code:    "MethodNotAllowed",
        message: "The specified method is not allowed against this resource.",
    }
    // ErrNotImplemented: functionality handled by a later ticket (501).
    ErrNotImplemented = &s3Error{
        status:  http.StatusNotImplemented,
        code:    "NotImplemented",
        message: "A header you provided implies functionality that is not implemented.",
    }
    // ErrInternalError: unexpected server-side failure (500).
    ErrInternalError = &s3Error{
        status:  http.StatusInternalServerError,
        code:    "InternalError",
        message: "We encountered an internal error. Please try again.",
    }
)

// errorDoc is the XML document S3 attaches to error responses.
type errorDoc struct {
    XMLName   xml.Name `xml:"Error"`
    Code      string   `xml:"Code"`
    Message   string   `xml:"Message"`
    Resource  string   `xml:"Resource"`
    RequestID string   `xml:"RequestId"`
}

// replyError writes e as the standard S3 XML error document.
func replyError(w http.ResponseWriter, r *http.Request, e *s3Error) {
    doc := errorDoc{
        Code:      e.code,
        Message:   e.message,
        Resource:  r.URL.Path,
        RequestID: newRequestID(),
    }
    body, err := xml.Marshal(doc)
    if err != nil {
        http.Error(w, doc.Code, e.status)
        return
    }
    w.Header().Set("Content-Type", "application/xml")
    w.Header().Set("x-amz-request-id", doc.RequestID)
    w.WriteHeader(e.status)
    _, _ = w.Write(append([]byte(xml.Header), body...))
}
