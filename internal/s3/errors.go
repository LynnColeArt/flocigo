package s3

import (
    "encoding/xml"
    "errors"
    "fmt"
    "net/http"
    "strconv"
    "strings"
)

// Error is an S3 protocol error. Handlers and backends surface failures as
// *Error values; the server renders them as standard S3 <Error> XML
// documents with the mapped HTTP status code.
type Error struct {
    Code     string
    Status   int
    Message  string
    Resource string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func errf(code string, status int, format string, args ...any) *Error {
    return &Error{Code: code, Status: status, Message: fmt.Sprintf(format, args...)}
}

func errNoSuchBucket() *Error {
    return errf("NoSuchBucket", http.StatusNotFound, "The specified bucket does not exist")
}

func errNoSuchKey() *Error {
    return errf("NoSuchKey", http.StatusNotFound, "The specified key does not exist.")
}

func errNoSuchUpload() *Error {
    return errf("NoSuchUpload", http.StatusNotFound,
        "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.")
}

func errMethodNotAllowed() *Error {
    return errf("MethodNotAllowed", http.StatusMethodNotAllowed, "The specified method is not allowed against this resource.")
}

func errMalformedXML() *Error {
    return errf("MalformedXML", http.StatusBadRequest, "The XML you provided was not well-formed or did not validate against our published schema")
}

func errInvalidArgument(format string, args ...any) *Error {
    return errf("InvalidArgument", http.StatusBadRequest, format, args...)
}

func errInvalidBucketName() *Error {
    return errf("InvalidBucketName", http.StatusBadRequest, "The specified bucket is not valid.")
}

func errInvalidPart() *Error {
    return errf("InvalidPart", http.StatusBadRequest,
        "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.")
}

func errInvalidPartOrder() *Error {
    return errf("InvalidPartOrder", http.StatusBadRequest,
        "The list of parts was not in ascending order. Parts must be ordered by part number.")
}

func errEntityTooSmall() *Error {
    return errf("EntityTooSmall", http.StatusBadRequest, "Your proposed upload is smaller than the minimum allowed size")
}

func errInvalidRange() *Error {
    return errf("InvalidRange", http.StatusRequestedRangeNotSatisfiable, "The requested range is not satisfiable")
}

func errPreconditionFailed() *Error {
    return errf("PreconditionFailed", http.StatusPreconditionFailed, "At least one of the pre-conditions you specified did not hold")
}

func errBucketNotEmpty() *Error {
    return errf("BucketNotEmpty", http.StatusConflict, "The bucket you tried to delete is not empty")
}

func errInternal() *Error {
    return errf("InternalError", http.StatusInternalServerError, "We encountered an internal error. Please try again.")
}

var errSignatureMismatch = &Error{
    Code:    "SignatureDoesNotMatch",
    Status:  http.StatusForbidden,
    Message: "The request signature we calculated does not match the signature you provided. Check your key and signing method.",
}

// renderError normalizes arbitrary errors (including backend sentinels) into
// protocol errors.
func renderError(err error) *Error {
    if e, ok := err.(*Error); ok {
        return e
    }
    switch {
    case errors.Is(err, ErrNoSuchBucket):
        return errNoSuchBucket()
    case errors.Is(err, ErrNoSuchKey):
        return errNoSuchKey()
    case errors.Is(err, ErrNoSuchUpload):
        return errNoSuchUpload()
    case errors.Is(err, ErrBucketNotEmpty):
        return errBucketNotEmpty()
    case errors.Is(err, ErrBucketAlreadyOwnedByYou):
        return errf("BucketAlreadyOwnedByYou", http.StatusConflict,
            "Your previous request to create the named bucket succeeded and you already own it.")
    case errors.Is(err, ErrBucketAlreadyExists):
        return errf("BucketAlreadyExists", http.StatusConflict,
            "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.")
    case errors.Is(err, ErrInvalidPart):
        return errInvalidPart()
    case errors.Is(err, ErrInvalidPartOrder):
        return errInvalidPartOrder()
    case errors.Is(err, ErrEntityTooSmall):
        return errEntityTooSmall()
    default:
        return errInternal()
    }
}

// errorXML is the wire form of Error.
type errorXML struct {
    XMLName   xml.Name `xml:"Error"`
    Code      string   `xml:"Code"`
    Message   string   `xml:"Message"`
    Resource  string   `xml:"Resource,omitempty"`
    Key       string   `xml:"Key,omitempty"`
    Bucket    string   `xml:"BucketName,omitempty"`
    RequestID string   `xml:"RequestId"`
    HostID    string   `xml:"HostId"`
}

// writeError renders err; bucket/key feed Resource plus the extra Key /
// BucketName elements S3 adds for NoSuchKey / NoSuchBucket. HEAD requests
// carry the status only (no body).
func (s *Server) writeError(w http.ResponseWriter, r *http.Request, requestID, hostID string, err error, bucket, key string) {
    se := renderError(err)
    doc := errorXML{RequestID: requestID, HostID: hostID}
    doc.Code = se.Code
    doc.Message = se.Message
    if bucket != "" {
        doc.Resource = "/" + bucket
        if key != "" {
            doc.Resource += "/" + key
        }
    }
    switch se.Code {
    case "NoSuchKey":
        if key != "" {
            doc.Key = key
        }
    case "NoSuchBucket", "BucketAlreadyExists", "BucketAlreadyOwnedByYou":
        if bucket != "" {
            doc.Bucket = bucket
        }
    }
    if r.Method == http.MethodHead {
        w.WriteHeader(se.Status)
        return
    }
    body, merr := xml.Marshal(doc)
    if merr != nil {
        http.Error(w, merr.Error(), http.StatusInternalServerError)
        return
    }
    raw := []byte(strings.ReplaceAll(string(body), "&#34;", `"`))
    w.Header().Set("Content-Type", "application/xml")
    w.Header().Set("Content-Length", strconv.Itoa(len(xml.Header)+len(raw)))
    w.WriteHeader(se.Status)
    w.Write([]byte(xml.Header))
    w.Write(raw)
}
