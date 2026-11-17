package main

import (
    "crypto/rand"
    "encoding/hex"
    "encoding/xml"
    "net/http"
)

// --- response document models ------------------------------------------------

// ownerXML is the owner block of ListAllMyBucketsResult.
type ownerXML struct {
    ID          string `xml:"ID"`
    DisplayName string `xml:"DisplayName"`
}

// bucketXML is one entry of the ListAllMyBucketsResult bucket list.
type bucketXML struct {
    Name         string `xml:"Name"`
    CreationDate string `xml:"CreationDate"`
}

// listAllMyBucketsResultXML is the GET / document.
type listAllMyBucketsResultXML struct {
    XMLName xml.Name `xml:"ListAllMyBucketsResult"`
    Xmlns   string   `xml:"xmlns,attr"`
    Owner   ownerXML `xml:"Owner"`
    Buckets struct {
        Bucket []bucketXML `xml:"Bucket"`
    } `xml:"Buckets"`
}

// listBucketResultXML is the GET /{bucket} document (empty listing while
// object storage is not implemented).
type listBucketResultXML struct {
    XMLName     xml.Name `xml:"ListBucketResult"`
    Xmlns       string   `xml:"xmlns,attr"`
    Name        string   `xml:"Name"`
    Prefix      string   `xml:"Prefix"`
    Marker      string   `xml:"Marker"`
    MaxKeys     int      `xml:"MaxKeys"`
    IsTruncated bool     `xml:"IsTruncated"`
}

// --- response helpers ---------------------------------------------------------

// setBasicHeaders stamps the S3 headers every response carries.
func setBasicHeaders(w http.ResponseWriter) {
    w.Header().Set("x-amz-request-id", newRequestID())
}

// writeXML marshals v and writes it as an XML response with the given status.
func writeXML(w http.ResponseWriter, r *http.Request, status int, v interface{}) {
    body, err := xml.Marshal(v)
    if err != nil {
        replyError(w, r, ErrInternalError)
        return
    }
    setBasicHeaders(w)
    w.Header().Set("Content-Type", "application/xml")
    w.WriteHeader(status)
    _, _ = w.Write(append([]byte(xml.Header), body...))
}

// newRequestID returns a random 16-character request id, like the ones S3
// hands out.
func newRequestID() string {
    var b [8]byte
    if _, err := rand.Read(b[:]); err != nil {
        return "0000000000000000"
    }
    return strings.ToUpper(hex.EncodeToString(b[:]))
}
