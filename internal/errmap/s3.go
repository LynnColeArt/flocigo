package errmap

import (
    "net/http"
    "strings"
)

// s3XMLDecl is the declaration prefix of S3 XML error documents.
const s3XMLDecl = `<?xml version="1.0" encoding="UTF-8"?>`

// s3CtxOrder is the fixed order of the optional context elements rendered
// between <Message> and <RequestId>, matching Floci's S3 error documents.
var s3CtxOrder = []string{
    "Key",
    "BucketName",
    "UploadId",
    "PartNumber",
    "VersionId",
    "Resource",
    "Condition",
}

// S3 renders err as an S3 (REST XML) error document:
//
//	<?xml version="1.0" encoding="UTF-8"?>
//	<Error><Code>NoSuchKey</Code><Message>..</Message><Key>..</Key>
//	<RequestId>..</RequestId><HostId>..</HostId></Error>
//
// reqID becomes both <RequestId> and the X-Amz-Request-Id response header;
// hostID becomes <HostId> and X-Amz-Id-2.
func S3(err error, reqID, hostID string) Out {
    e := From(err)
    name, status, msg := s3Resolve(e)

    var b strings.Builder
    b.WriteString(s3XMLDecl)
    b.WriteString("<Error><Code>")
    b.WriteString(xmlText(name))
    b.WriteString("</Code><Message>")
    b.WriteString(xmlText(msg))
    for _, k := range s3CtxOrder {
        if v, ok := e.Ctx[k]; ok {
            b.WriteString("<" + k + ">")
            b.WriteString(xmlText(v))
            b.WriteString("</" + k + ">")
        }
    }
    b.WriteString("<RequestId>")
    b.WriteString(xmlText(reqID))
    b.WriteString("</RequestId><HostId>")
    b.WriteString(xmlText(hostID))
    b.WriteString("</HostId></Error>")

    h := http.Header{}
    h.Set("Content-Type", "application/xml")
    h.Set("X-Amz-Request-Id", reqID)
    h.Set("X-Amz-Id-2", hostID)
    return Out{Status: status, Header: h, Body: []byte(b.String())}
}

// s3Resolve maps e onto the S3 wire triple (code, status, message).
func s3Resolve(e *E) (name string, status int, msg string) {
    if d, ok := Lookup(e.Code); ok && d.S3Status != 0 {
        name = d.Code
        if d.S3Name != "" {
            name = d.S3Name
        }
        status = d.S3Status
        msg = e.Message
        if msg == "" {
            msg = d.S3Msg
        }
    } else if e.Status != 0 {
        // The operation pinned a status for a code this family does not
        // know: keep the code verbatim and fall back to the generic S3
        // message when none was supplied.
        name, status, msg = e.Code, e.Status, e.Message
        if msg == "" {
            msg = s3InternalMsg
        }
    } else {
        name, status, msg = internalCode, http.StatusInternalServerError, s3InternalMsg
    }
    if e.Status != 0 {
        status = e.Status
    }
    return name, status, msg
}

// S3Status returns the HTTP status S3 uses for code, or 500 when unknown.
// Useful for body-less error paths such as HEAD requests.
func S3Status(code string) int {
    if d, ok := Lookup(code); ok && d.S3Status != 0 {
        return d.S3Status
    }
    return http.StatusInternalServerError
}
