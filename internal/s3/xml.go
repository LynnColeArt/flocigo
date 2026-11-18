package s3

import (
    "encoding/xml"
    "fmt"
    "net/http"
    "strconv"
    "strings"
    "time"
)

const s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/"

const iso8601Millis = "2006-01-02T15:04:05.000Z"

// rfc1123GMT formats Last-Modified response headers.
const rfc1123GMT = "Mon, 02 Jan 2006 15:04:05 GMT"

// s3Time renders timestamps the way S3 wire documents do: ISO 8601 with
// millisecond precision in UTC.
type s3Time struct{ time.Time }

func s3t(t time.Time) s3Time { return s3Time{t.UTC().Truncate(time.Millisecond)} }

func (t s3Time) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
    return e.EncodeElement(t.Format(iso8601Millis), start)
}

type Owner struct {
    ID          string `xml:"ID"`
    DisplayName string `xml:"DisplayName"`
}

type BucketEntry struct {
    Name         string `xml:"Name"`
    CreationDate s3Time `xml:"CreationDate"`
}

type ListAllMyBucketsResult struct {
    XMLName xml.Name      `xml:"ListAllMyBucketsResult"`
    Xmlns   string        `xml:"xmlns,attr"`
    Owner   Owner         `xml:"Owner"`
    Buckets []BucketEntry `xml:"Buckets>Bucket"`
}

type ObjectEntry struct {
    Key          string `xml:"Key"`
    LastModified s3Time `xml:"LastModified"`
    ETag         string `xml:"ETag"`
    Size         int64  `xml:"Size"`
    Owner        *Owner `xml:"Owner,omitempty"`
    StorageClass string `xml:"StorageClass"`
}

type CommonPrefix struct {
    Prefix string `xml:"Prefix"`
}

// ListBucketResult is the ListObjects (V1) response.
type ListBucketResult struct {
    XMLName        xml.Name       `xml:"ListBucketResult"`
    Xmlns          string         `xml:"xmlns,attr"`
    Name           string         `xml:"Name"`
    Prefix         string         `xml:"Prefix"`
    Marker         string         `xml:"Marker"`
    MaxKeys        int            `xml:"MaxKeys"`
    Delimiter      string         `xml:"Delimiter,omitempty"`
    EncodingType   string         `xml:"EncodingType,omitempty"`
    IsTruncated    bool           `xml:"IsTruncated"`
    NextMarker     string         `xml:"NextMarker,omitempty"`
    Contents       []ObjectEntry  `xml:"Contents"`
    CommonPrefixes []CommonPrefix `xml:"CommonPrefixes"`
}

// ListBucketResultV2 is the ListObjectsV2 response.
type ListBucketResultV2 struct {
    XMLName               xml.Name       `xml:"ListBucketResult"`
    Xmlns                 string         `xml:"xmlns,attr"`
    Name                  string         `xml:"Name"`
    Prefix                string         `xml:"Prefix"`
    Delimiter             string         `xml:"Delimiter,omitempty"`
    StartAfter            string         `xml:"StartAfter,omitempty"`
    ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
    NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
    KeyCount              int            `xml:"KeyCount"`
    MaxKeys               int            `xml:"MaxKeys"`
    EncodingType          string         `xml:"EncodingType,omitempty"`
    IsTruncated           bool           `xml:"IsTruncated"`
    Contents              []ObjectEntry  `xml:"Contents"`
    CommonPrefixes        []CommonPrefix `xml:"CommonPrefixes"`
}

type CopyObjectResult struct {
    XMLName      xml.Name `xml:"CopyObjectResult"`
    Xmlns        string   `xml:"xmlns,attr"`
    ETag         string   `xml:"ETag"`
    LastModified s3Time   `xml:"LastModified"`
}

type CopyPartResult struct {
    XMLName      xml.Name `xml:"CopyPartResult"`
    Xmlns        string   `xml:"xmlns,attr"`
    ETag         string   `xml:"ETag"`
    LastModified s3Time   `xml:"LastModified"`
}

type InitiateMultipartUploadResult struct {
    XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
    Xmlns    string   `xml:"xmlns,attr"`
    Bucket   string   `xml:"Bucket"`
    Key      string   `xml:"Key"`
    UploadID string   `xml:"UploadId"`
}

// CompleteMultipartUpload is the POST ?uploadId request body.
type CompleteMultipartUpload struct {
    XMLName xml.Name       `xml:"CompleteMultipartUpload"`
    Parts   []CompletePart `xml:"Part"`
}

type CompletePart struct {
    PartNumber int    `xml:"PartNumber"`
    ETag       string `xml:"ETag"`
}

type CompleteMultipartUploadResult struct {
    XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
    Xmlns    string   `xml:"xmlns,attr"`
    Location string   `xml:"Location"`
    Bucket   string   `xml:"Bucket"`
    Key      string   `xml:"Key"`
    ETag     string   `xml:"ETag"`
}

type PartEntry struct {
    PartNumber   int    `xml:"PartNumber"`
    LastModified s3Time `xml:"LastModified"`
    ETag         string `xml:"ETag"`
    Size         int64  `xml:"Size"`
}

type ListPartsResult struct {
    XMLName              xml.Name    `xml:"ListPartsResult"`
    Xmlns                string      `xml:"xmlns,attr"`
    Bucket               string      `xml:"Bucket"`
    Key                  string      `xml:"Key"`
    UploadID             string      `xml:"UploadId"`
    PartNumberMarker     int         `xml:"PartNumberMarker"`
    NextPartNumberMarker int         `xml:"NextPartNumberMarker,omitempty"`
    MaxParts             int         `xml:"MaxParts"`
    IsTruncated          bool        `xml:"IsTruncated"`
    Initiator            Owner       `xml:"Initiator"`
    Owner                Owner       `xml:"Owner"`
    StorageClass         string      `xml:"StorageClass"`
    Parts                []PartEntry `xml:"Part"`
}

type UploadEntry struct {
    Key          string `xml:"Key"`
    UploadID     string `xml:"UploadId"`
    Initiator    Owner  `xml:"Initiator"`
    Owner        Owner  `xml:"Owner"`
    StorageClass string `xml:"StorageClass"`
    Initiated    s3Time `xml:"Initiated"`
}

type ListMultipartUploadsResult struct {
    XMLName            xml.Name       `xml:"ListMultipartUploadsResult"`
    Xmlns              string         `xml:"xmlns,attr"`
    Bucket             string         `xml:"Bucket"`
    KeyMarker          string         `xml:"KeyMarker"`
    UploadIDMarker     string         `xml:"UploadIdMarker"`
    NextKeyMarker      string         `xml:"NextKeyMarker,omitempty"`
    NextUploadIDMarker string         `xml:"NextUploadIdMarker,omitempty"`
    Delimiter          string         `xml:"Delimiter,omitempty"`
    Prefix             string         `xml:"Prefix"`
    MaxUploads         int            `xml:"MaxUploads"`
    IsTruncated        bool           `xml:"IsTruncated"`
    Uploads            []UploadEntry  `xml:"Upload"`
    CommonPrefixes     []CommonPrefix `xml:"CommonPrefixes,omitempty"`
}

type DeleteRequest struct {
    XMLName xml.Name              `xml:"Delete"`
    Quiet   bool                  `xml:"Quiet"`
    Objects []DeleteRequestObject `xml:"Object"`
}

type DeleteRequestObject struct {
    Key string `xml:"Key"`
}

type Deleted struct {
    Key string `xml:"Key"`
}

type DeleteErr struct {
    Key     string `xml:"Key"`
    Code    string `xml:"Code"`
    Message string `xml:"Message"`
}

type DeleteResult struct {
    XMLName xml.Name    `xml:"DeleteResult"`
    Xmlns   string      `xml:"xmlns,attr"`
    Deleted []Deleted   `xml:"Deleted"`
    Errors  []DeleteErr `xml:"Error"`
}

type CreateBucketConfiguration struct {
    XMLName            xml.Name `xml:"CreateBucketConfiguration"`
    LocationConstraint string   `xml:"LocationConstraint"`
}

type LocationConstraint struct {
    XMLName xml.Name `xml:"LocationConstraint"`
    Xmlns   string   `xml:"xmlns,attr"`
    Value   string   `xml:",chardata"`
}

// writeXML marshals v and writes it preceded by the XML declaration. Go
// escapes double quotes inside character data as &#34;; S3 emits literal
// quotes (ETag values are wrapped in them), so the escape is undone to match
// the S3 wire format exactly.
func writeXML(w http.ResponseWriter, status int, v any) {
    raw, err := xml.Marshal(v)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    body := []byte(strings.ReplaceAll(string(raw), "&#34;", `"`))
    w.Header().Set("Content-Type", "application/xml")
    w.Header().Set("Content-Length", strconv.Itoa(len(xml.Header)+len(body)))
    w.WriteHeader(status)
    w.Write([]byte(xml.Header))
    w.Write(body)
}

func quoteETag(etag string) string { return `"` + etag + `"` }

func unquoteETag(s string) string { return strings.Trim(strings.TrimSpace(s), `"`) }

// escapePath percent-encodes s for embedding in a URL path ('/' kept).
func escapePath(s string) string { return awsEscape(s, true) }

// encodeKey percent-encodes an object key for encoding-type=url responses
// ('/' kept, space as '+', matching S3's URL encoding of list results).
func encodeKey(s string) string { return awsEscape(s, false) }

func awsEscape(s string, spacePercent bool) string {
    var b strings.Builder
    for i := 0; i < len(s); i++ {
        c := s[i]
        switch {
        case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
            c == '-', c == '_', c == '.', c == '~', c == '/':
            b.WriteByte(c)
        case c == ' ' && !spacePercent:
            b.WriteByte('+')
        default:
            fmt.Fprintf(&b, "%%%02X", c)
        }
    }
    return b.String()
}
