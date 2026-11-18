package s3

import "fmt"

// s3Error is a protocol-level S3 failure rendered as an XML <Error> body.
type s3Error struct {
	Status  int
	Code    string
	Message string
	// Extra carries optional context elements (e.g. StringToSign for
	// SignatureDoesNotMatch replies).
	Extra map[string]string
}

// Error implements the error interface.
func (e *s3Error) Error() string { return e.Code + ": " + e.Message }

func s3err(status int, code, format string, args ...any) *s3Error {
	return &s3Error{Status: status, Code: code, Message: fmt.Sprintf(format, args...)}
}

func errNoSuchBucket() *s3Error {
	return s3err(404, "NoSuchBucket", "The specified bucket does not exist")
}

func errNoSuchKey(_ string) *s3Error {
	return s3err(404, "NoSuchKey", "The specified key does not exist.")
}

func errNoSuchUpload(_ string) *s3Error {
	return s3err(404, "NoSuchUpload", "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.")
}

// asS3Error maps arbitrary errors onto the S3 error wire type.
func asS3Error(err error) *s3Error {
	if e, ok := err.(*s3Error); ok {
		return e
	}
	return s3err(500, "InternalError", "We encountered an internal error. Please try again.")
}
