package errmap

import (
    "bytes"
    "net/http"
    "strings"
)

const ddbContentType = "application/x-amz-json-1.0"

// DDB renders err as a DynamoDB (AWS JSON 1.0) error:
//
//	{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException",
//	 "message":"The conditional request failed"}
//
// The "message" member is omitted when there is nothing to say (e.g. coral
// SerializationException). It also sets the X-Amzn-ErrorType (full shape id),
// X-Amzn-Requestid and Content-Type response headers.
func DDB(err error, reqID string) Out {
    e := From(err)
    shape, status, msg := ddbResolve(e)

    var b strings.Builder
    b.WriteString(`{"__type":`)
    b.WriteString(jsonStr(shape))
    if msg != "" {
        b.WriteString(`,"message":`)
        b.WriteString(jsonStr(msg))
    }
    if extra := ddbExtraMembers(e.Extra); extra != "" {
        b.WriteString(",")
        b.WriteString(extra)
    }
    b.WriteString("}")

    h := http.Header{}
    h.Set("Content-Type", ddbContentType)
    h.Set("X-Amzn-ErrorType", shape)
    h.Set("X-Amzn-Requestid", reqID)
    return Out{Status: status, Header: h, Body: []byte(b.String())}
}

// ddbResolve maps e onto the DynamoDB wire triple (shape id, status,
// message).
func ddbResolve(e *E) (shape string, status int, msg string) {
    if d, ok := Lookup(e.Code); ok && d.DDBStatus != 0 {
        shape, status = d.DDBShape, d.DDBStatus
        msg = e.Message
        if msg == "" {
            msg = d.DDBMsg
        }
    } else if e.Status != 0 {
        // The operation pinned a status for a code this family does not
        // know: prefix the code unless it already carries a namespace and
        // fall back to the generic message when none was supplied.
        shape, status, msg = ddbWireShape(e.Code), e.Status, e.Message
        if msg == "" {
            msg = ddbInternalMsg
        }
    } else {
        shape, status, msg = ddbInternalShape, http.StatusInternalServerError, ddbInternalMsg
    }
    if e.Status != 0 {
        status = e.Status
    }
    return shape, status, msg
}

// ddbWireShape prefixes code with the DynamoDB shape prefix unless it
// already carries a namespace (coral errors are passed in fully qualified).
func ddbWireShape(code string) string {
    if strings.Contains(code, "#") {
        return code
    }
    return DDBShapePrefix + code
}

// ddbExtraMembers normalizes E.Extra (bare members or a full JSON object)
// into a bare member list.
func ddbExtraMembers(extra []byte) string {
    t := bytes.TrimSpace(extra)
    if len(t) > 1 && t[0] == '{' && t[len(t)-1] == '}' {
        t = bytes.TrimSpace(t[1 : len(t)-1])
    }
    return string(t)
}

// DDBStatus returns the HTTP status DynamoDB uses for code, or 500 when
// unknown.
func DDBStatus(code string) int {
    if d, ok := Lookup(code); ok && d.DDBStatus != 0 {
        return d.DDBStatus
    }
    return http.StatusInternalServerError
}
