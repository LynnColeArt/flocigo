package s3

import (
	"bytes"
	"encoding/xml"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const s3XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"

// s3Time renders timestamps the way S3 serializes them in XML documents.
func s3Time(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000") + "Z"
}

type xmlOwner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

func xmlFlociOwner() xmlOwner { return xmlOwner{ID: OwnerID, DisplayName: OwnerDisplay} }

type xmlBucket struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type xmlListBuckets struct {
	XMLName xml.Name    `xml:"ListAllMyBucketsResult"`
	Xmlns   string      `xml:"xmlns,attr"`
	Owner   xmlOwner    `xml:"Owner"`
	Buckets []xmlBucket `xml:"Buckets>Bucket"`
}

type xmlObjectEntry struct {
	Key          string    `xml:"Key"`
	LastModified string    `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
	Size         int64     `xml:"Size"`
	StorageClass string    `xml:"StorageClass,omitempty"`
	Owner        *xmlOwner `xml:"Owner,omitempty"`
}

type xmlCommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type xmlListObjectsV1 struct {
	XMLName        xml.Name          `xml:"ListBucketResult"`
	Xmlns          string            `xml:"xmlns,attr"`
	Name           string            `xml:"Name"`
	Prefix         string            `xml:"Prefix"`
	Marker         string            `xml:"Marker"`
	NextMarker     string            `xml:"NextMarker,omitempty"`
	MaxKeys        int               `xml:"MaxKeys"`
	Delimiter      string            `xml:"Delimiter,omitempty"`
	EncodingType   string            `xml:"EncodingType,omitempty"`
	IsTruncated    bool              `xml:"IsTruncated"`
	Contents       []xmlObjectEntry  `xml:"Contents,omitempty"`
	CommonPrefixes []xmlCommonPrefix `xml:"CommonPrefixes,omitempty"`
}

type xmlListObjectsV2 struct {
	XMLName               xml.Name          `xml:"ListBucketResult"`
	Xmlns                 string            `xml:"xmlns,attr"`
	Name                  string            `xml:"Name"`
	Prefix                string            `xml:"Prefix"`
	StartAfter            string            `xml:"StartAfter,omitempty"`
	ContinuationToken     string            `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string            `xml:"NextContinuationToken,omitempty"`
	KeyCount              int               `xml:"KeyCount"`
	MaxKeys               int               `xml:"MaxKeys"`
	Delimiter             string            `xml:"Delimiter,omitempty"`
	EncodingType          string            `xml:"EncodingType,omitempty"`
	IsTruncated           bool              `xml:"IsTruncated"`
	Contents              []xmlObjectEntry  `xml:"Contents,omitempty"`
	CommonPrefixes        []xmlCommonPrefix `xml:"CommonPrefixes,omitempty"`
}

type xmlDeleteRequest struct {
	XMLName xml.Name             `xml:"Delete"`
	Quiet   bool                 `xml:"Quiet"`
	Objects []xmlDeleteReqObject `xml:"Object"`
}

type xmlDeleteReqObject struct {
	Key string `xml:"Key"`
}

type xmlDeletedObject struct {
	Key string `xml:"Key"`
}

type xmlDeleteError struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

type xmlDeleteResult struct {
	XMLName xml.Name           `xml:"DeleteResult"`
	Xmlns   string             `xml:"xmlns,attr"`
	Deleted []xmlDeletedObject `xml:"Deleted,omitempty"`
	Errors  []xmlDeleteError   `xml:"Error,omitempty"`
}

type xmlLocationConstraint struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	Xmlns   string   `xml:"xmlns,attr"`
	Value   string   `xml:",chardata"`
}

type xmlVersioning struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Xmlns   string   `xml:"xmlns,attr"`
	Status  string   `xml:"Status,omitempty"`
}

type xmlTag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type xmlTagging struct {
	XMLName xml.Name `xml:"Tagging"`
	Xmlns   string   `xml:"xmlns,attr"`
	TagSet  []xmlTag `xml:"TagSet>Tag"`
}

type xmlGrantee struct {
	XSIType     string `xml:"http://www.w3.org/2001/XMLSchema-instance type,attr"`
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type xmlGrant struct {
	Grantee    xmlGrantee `xml:"Grantee"`
	Permission string     `xml:"Permission"`
}

type xmlACL struct {
	XMLName           xml.Name   `xml:"AccessControlPolicy"`
	Xmlns             string     `xml:"xmlns,attr"`
	Owner             xmlOwner   `xml:"Owner"`
	AccessControlList []xmlGrant `xml:"AccessControlList>Grant"`
}

type xmlInitiateMultipart struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type xmlCompleteMultipart struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type xmlCompleteRequest struct {
	XMLName xml.Name          `xml:"CompleteMultipartUpload"`
	Parts   []xmlCompletePart `xml:"Part"`
}

type xmlCompletePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type xmlCopyObject struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	Xmlns        string   `xml:"xmlns,attr"`
	ETag         string   `xml:"ETag"`
	LastModified string   `xml:"LastModified"`
}

type xmlCopyPart struct {
	XMLName      xml.Name `xml:"CopyPartResult"`
	Xmlns        string   `xml:"xmlns,attr"`
	ETag         string   `xml:"ETag"`
	LastModified string   `xml:"LastModified"`
}

type xmlPart struct {
	PartNumber   int    `xml:"PartNumber"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

type xmlListParts struct {
	XMLName              xml.Name  `xml:"ListPartsResult"`
	Xmlns                string    `xml:"xmlns,attr"`
	Bucket               string    `xml:"Bucket"`
	Key                  string    `xml:"Key"`
	UploadID             string    `xml:"UploadId"`
	PartNumberMarker     int       `xml:"PartNumberMarker"`
	NextPartNumberMarker int       `xml:"NextPartNumberMarker,omitempty"`
	MaxParts             int       `xml:"MaxParts"`
	IsTruncated          bool      `xml:"IsTruncated"`
	Part                 []xmlPart `xml:"Part,omitempty"`
	Initiator            xmlOwner  `xml:"Initiator"`
	Owner                xmlOwner  `xml:"Owner"`
	StorageClass         string    `xml:"StorageClass"`
}

type xmlUpload struct {
	Key          string   `xml:"Key"`
	UploadID     string   `xml:"UploadId"`
	Initiator    xmlOwner `xml:"Initiator"`
	Owner        xmlOwner `xml:"Owner"`
	StorageClass string   `xml:"StorageClass"`
	Initiated    string   `xml:"Initiated"`
}

type xmlListUploads struct {
	XMLName            xml.Name          `xml:"ListMultipartUploadsResult"`
	Xmlns              string            `xml:"xmlns,attr"`
	Bucket             string            `xml:"Bucket"`
	KeyMarker          string            `xml:"KeyMarker"`
	UploadIDMarker     string            `xml:"UploadIdMarker"`
	NextKeyMarker      string            `xml:"NextKeyMarker"`
	NextUploadIDMarker string            `xml:"NextUploadIdMarker"`
	Delimiter          string            `xml:"Delimiter,omitempty"`
	Prefix             string            `xml:"Prefix,omitempty"`
	MaxUploads         int               `xml:"MaxUploads"`
	IsTruncated        bool              `xml:"IsTruncated"`
	Upload             []xmlUpload       `xml:"Upload,omitempty"`
	CommonPrefixes     []xmlCommonPrefix `xml:"CommonPrefixes,omitempty"`
}

type xmlErrorResponse struct {
	XMLName           xml.Name `xml:"Error"`
	Code              string   `xml:"Code"`
	Message           string   `xml:"Message"`
	Resource          string   `xml:"Resource,omitempty"`
	RequestID         string   `xml:"RequestId"`
	AWSAccessKeyID    string   `xml:"AWSAccessKeyId,omitempty"`
	StringToSign      string   `xml:"StringToSign,omitempty"`
	SignatureProvided string   `xml:"SignatureProvided,omitempty"`
}

// writeXML renders an XML body with the fixed S3 response headers.
func writeXML(w http.ResponseWriter, status int, doc any) {
	var buf bytes.Buffer
	buf.WriteString(xml.Header)
	enc := xml.NewEncoder(&buf)
	if err := enc.Encode(doc); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/xml")
	h.Set("Content-Length", strconv.Itoa(buf.Len()))
	w.WriteHeader(status)
	_, _ = w.Write(buf.Bytes())
}

// writeError renders e as an S3 XML error; HEAD replies carry no body.
func writeError(w http.ResponseWriter, r *http.Request, e *s3Error) {
	if e == nil {
		e = s3err(500, "InternalError", "internal error")
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(e.Status)
		return
	}
	resp := xmlErrorResponse{
		Code:      e.Code,
		Message:   e.Message,
		Resource:  r.URL.Path,
		RequestID: w.Header().Get("x-amz-request-id"),
	}
	if resp.RequestID == "" {
		resp.RequestID = "request-id"
	}
	if e.Extra != nil {
		resp.AWSAccessKeyID = e.Extra["AWSAccessKeyId"]
		resp.StringToSign = e.Extra["StringToSign"]
		resp.SignatureProvided = e.Extra["SignatureProvided"]
	}
	writeXML(w, e.Status, resp)
}

// writeEmpty answers with no body (PUT/DELETE/POST acknowledgements).
func writeEmpty(w http.ResponseWriter, status int) {
	if status >= 200 && status < 300 && status != 204 {
		w.Header().Set("Content-Length", "0")
	}
	w.WriteHeader(status)
}

// encodeS3Key applies the encoding-type=url transform to listed keys.
func encodeS3Key(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}

// tagListToXML maps tags into a sorted TagSet document.
func tagListToXML(tags map[string]string) *xmlTagging {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := &xmlTagging{Xmlns: s3XMLNS}
	for _, k := range keys {
		out.TagSet = append(out.TagSet, xmlTag{Key: k, Value: tags[k]})
	}
	return out
}

func aclDocument() *xmlACL {
	return &xmlACL{
		Xmlns: s3XMLNS,
		Owner: xmlFlociOwner(),
		AccessControlList: []xmlGrant{{
			Grantee: xmlGrantee{
				XSIType:     "CanonicalUser",
				ID:          OwnerID,
				DisplayName: OwnerDisplay,
			},
			Permission: "FULL_CONTROL",
		}},
	}
}
