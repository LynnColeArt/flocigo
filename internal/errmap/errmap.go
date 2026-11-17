// Package errmap maps floci's internal error values onto the exact error
// names, wire shapes and HTTP status codes Floci emits for the three AWS
// protocol families floci implements:
//
//	S3   - REST XML     <Error><Code>..</Code>...</Error>
//	DDB  - AWS JSON 1.0 {"__type":"...","message":".."}
//	SQS  - Query XML    <ErrorResponse><Error><Type>..</Type>...</Error></ErrorResponse>
//
// Operations raise canonical *E values carrying the canonical AWS code (for
// example "NoSuchKey", "ConditionalCheckFailedException" or
// "QueueDoesNotExist"). Protocol handlers pipe them through the family
// renderer (S3, DDB or SQS) and write the resulting Out to the HTTP
// response, which yields byte-identical error names, documents and status
// codes on every op's error path.
package errmap

import (
    "bytes"
    "encoding/json"
    "errors"
    "net/http"
    "strings"
)

// Coder is implemented by error values from other floci packages that
// already carry a canonical AWS error code. From keeps their code and their
// own message intact.
type Coder interface {
    error
    ErrorCode() string
}

// internalCode is the family agnostic code substituted for raw Go errors
// that reach a renderer without having been mapped by the operation; every
// family has a matching internal error in the catalog.
const internalCode = "InternalError"

// Per-family fallbacks used when a code is unknown to the target family and
// the operation did not pin a status.
const (
    s3InternalMsg    = "We encountered an internal error. Please try again."
    ddbInternalMsg   = "We encountered an internal error. Please try again."
    sqsInternalMsg   = "We encountered an internal error. Please try again."
    ddbInternalShape = DDBShapePrefix + "InternalServerError"
)

// E is the canonical error value exchanged between floci operations and
// protocol handlers.
type E struct {
    // Code is the canonical AWS error code, e.g. "NoSuchKey".
    Code string

    // Message is the human readable message. When empty the family renderer
    // substitutes the catalog's canonical message for the code.
    Message string

    // Status optionally pins the HTTP status; when non-zero it overrides the
    // catalog status in every family. Leave it zero unless the operation
    // needs a status errmap cannot infer.
    Status int

    // Extra carries pre-serialized JSON members appended to the DynamoDB
    // error body (e.g. CancellationReasons for TransactionCanceledException).
    // Either a bare member list or a full JSON object is accepted. Ignored by
    // the other families.
    Extra []byte

    // Ctx carries optional context rendered as sibling elements inside the
    // S3 <Error> document. Recognized keys, in render order: Key,
    // BucketName, UploadId, PartNumber, VersionId, Resource, Condition.
    // Ignored by the other families.
    Ctx map[string]string

    // Cause is the underlying internal error, if any.
    Cause error
}

// New returns a canonical error for code. Pass an empty message to have the
// protocol renderer substitute the exact AWS message from the catalog.
func New(code, msg string) *E {
    return &E{Code: code, Message: msg}
}

// Wrap behaves like New but records err as the cause, keeping it available
// to errors.Is / errors.As.
func Wrap(err error, code, msg string) *E {
    return &E{Code: code, Message: msg, Cause: err}
}

// From converts any Go error into the canonical *E:
//
//   - *E (wrapped or not) passes through unchanged;
//   - errors implementing Coder keep their ErrorCode and their own message;
//   - anything else, including nil, becomes the family agnostic internal
//     error with an empty message, so renderers emit Floci's exact default
//     message for the target family.
func From(err error) *E {
    var e *E
    if errors.As(err, &e) && e != nil {
        return e
    }
    var c Coder
    if errors.As(err, &c) {
        return &E{Code: c.ErrorCode(), Message: c.Error()}
    }
    return &E{Code: internalCode}
}

// ErrorCode satisfies Coder so *E can be handled like any coded error.
func (e *E) ErrorCode() string {
    if e == nil {
        return ""
    }
    return e.Code
}

// Error implements error.
func (e *E) Error() string {
    if e == nil {
        return "<nil>"
    }
    s := e.Code
    if e.Message != "" {
        s += ": " + e.Message
    }
    if e.Cause != nil {
        cause := e.Cause.Error()
        if s == "" {
            s = cause
        } else {
            s += ": " + cause
        }
    }
    if s == "" {
        s = internalCode
    }
    return s
}

// Unwrap exposes the cause to errors.Is / errors.As.
func (e *E) Unwrap() error {
    return e.Cause
}

// With records wire context (see E.Ctx) and returns e for chaining.
func (e *E) With(key, val string) *E {
    if e.Ctx == nil {
        e.Ctx = make(map[string]string)
    }
    e.Ctx[key] = val
    return e
}

// Out is a fully rendered protocol error, ready to be written to the HTTP
// response of the request it belongs to.
type Out struct {
    Status int
    Header http.Header
    Body   []byte
}

// Write applies the rendered error to w: headers first, then the status
// code, then the body. Call it last in a handler's error path so the error
// headers win over handler-specific ones.
func (o Out) Write(w http.ResponseWriter) error {
    for k, vs := range o.Header {
        for _, v := range vs {
            w.Header().Set(k, v)
        }
    }
    w.WriteHeader(o.Status)
    _, err := w.Write(o.Body)
    return err
}

// xmlText escapes s for use inside an XML text node the way the AWS XML
// protocol writers do: &, < and > are replaced, quotes are left untouched
// because they are legal inside element text.
func xmlText(s string) string {
    if !strings.ContainsAny(s, "&<>") {
        return s
    }
    var b strings.Builder
    b.Grow(len(s) + 16)
    for i := 0; i < len(s); i++ {
        switch s[i] {
        case '&':
            b.WriteString("&amp;")
        case '<':
            b.WriteString("&lt;")
        case '>':
            b.WriteString("&gt;")
        default:
            b.WriteByte(s[i])
        }
    }
    return b.String()
}

// jsonStr serializes s as a JSON string literal the way the AWS JSON
// protocol writers do: without HTML escaping of <, > and &.
func jsonStr(s string) string {
    var buf bytes.Buffer
    enc := json.NewEncoder(&buf)
    enc.SetEscapeHTML(false)
    if err := enc.Encode(s); err != nil {
        return "\"\""
    }
    return strings.TrimSuffix(buf.String(), "\n")
}
