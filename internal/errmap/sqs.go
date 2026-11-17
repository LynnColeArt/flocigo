package errmap

import (
    "net/http"
    "strings"
)

// sqsXMLDecl is the declaration prefix of SQS query-protocol XML documents.
const sqsXMLDecl = `<?xml version="1.0"?>`

// SQS renders err as an SQS (Query) error:
//
//	<?xml version="1.0"?>
//	<ErrorResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
//	  <Error><Type>Sender</Type><Code>..</Code><Message>..</Message><Detail/></Error>
//	  <RequestId>..</RequestId>
//	</ErrorResponse>
func SQS(err error, reqID string) Out {
    e := From(err)
    name, typ, status, msg := sqsResolve(e)

    var b strings.Builder
    b.WriteString(sqsXMLDecl)
    b.WriteString(`<ErrorResponse xmlns="`)
    b.WriteString(SQSNamespace)
    b.WriteString(`"><Error><Type>`)
    b.WriteString(xmlText(typ))
    b.WriteString("</Type><Code>")
    b.WriteString(xmlText(name))
    b.WriteString("</Code><Message>")
    b.WriteString(xmlText(msg))
    b.WriteString("</Message><Detail/></Error><RequestId>")
    b.WriteString(xmlText(reqID))
    b.WriteString("</RequestId></ErrorResponse>")

    h := http.Header{}
    h.Set("Content-Type", "text/xml")
    return Out{Status: status, Header: h, Body: []byte(b.String())}
}

// sqsResolve maps e onto the SQS wire quadruple (code, type, status,
// message).
func sqsResolve(e *E) (name, typ string, status int, msg string) {
    if d, ok := Lookup(e.Code); ok && d.SQSStatus != 0 {
        name = e.Code
        if d.SQSName != "" {
            name = d.SQSName
        }
        typ, status = d.SQSType, d.SQSStatus
        msg = e.Message
        if msg == "" {
            msg = d.SQSMsg
        }
    } else if e.Status != 0 {
        // The operation pinned a status for a code this family does not
        // know: keep the code verbatim, classify by status and fall back to
        // the generic message when none was supplied.
        name, typ, status, msg = e.Code, Sender, e.Status, e.Message
        if e.Status >= 500 {
            typ = Receiver
        }
        if msg == "" {
            msg = sqsInternalMsg
        }
    } else {
        name, typ, status, msg = internalCode, Receiver, http.StatusInternalServerError, sqsInternalMsg
    }
    if e.Status != 0 {
        status = e.Status
    }
    return name, typ, status, msg
}

// SQSStatus returns the HTTP status SQS uses for code, or 500 when unknown.
func SQSStatus(code string) int {
    if d, ok := Lookup(code); ok && d.SQSStatus != 0 {
        return d.SQSStatus
    }
    return http.StatusInternalServerError
}
