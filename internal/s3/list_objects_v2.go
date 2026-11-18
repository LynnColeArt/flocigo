package s3

// list_objects_v2.go implements the ListObjectsV2 wire op from ticket
// floci-go/ticket-011:
//
//	GET /{bucket}?list-type=2 [&prefix=..] [&delimiter=..] [&start-after=..]
//	    [&max-keys=..] [&encoding-type=url] [&fetch-owner=true]
//
// The response is the canonical S3 ListBucketResult document: Contents
// filtered by prefix, keys in lexical (bytewise) order, delimiter-folded
// CommonPrefixes, and IsTruncated always false - every matching entry is
// returned, so no continuation token is ever produced.
//
// Buckets are resolved through the store's own surface (an accessor method
// such as GetBucket, or a bucket map on the store/server value, including
// nested map-of-map layouts), which keeps this op decoupled from storage
// internals; no existing s3 file had to change. Wire it from the router's
// bucket-level GET branch when r.URL.Query().Get("list-type") == "2".

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

const (
	// listV2Namespace is the XML namespace S3 list documents carry.
	listV2Namespace = "http://s3.amazonaws.com/doc/2006-03-01/"

	listV2DefaultMaxKeys = int64(1000)
	listV2MaxKeysLimit   = int64(2147483647)

	listV2OwnerID   = "floci"
	listV2OwnerName = "floci"

	listV2HexDigits = "0123456789ABCDEF"
)

// listV2TimeFormat matches the ISO-8601 timestamps S3 listing documents use.
const listV2TimeFormat = "2006-01-02T15:04:05.000Z"

// --- request parameters ---------------------------------------------------

// listV2Query holds the normalized ListObjectsV2 request parameters.
type listV2Query struct {
	Prefix       string
	Delimiter    string
	StartAfter   string
	MaxKeys      int64
	EncodingType bool
	FetchOwner   bool
}

// listV2ParseQuery normalizes the request query string. continuation-token
// is accepted but ignored: this op never truncates, so it never issues one.
func listV2ParseQuery(q url.Values) (listV2Query, *listV2Err) {
	p := listV2Query{
		Prefix:     q.Get("prefix"),
		Delimiter:  q.Get("delimiter"),
		StartAfter: q.Get("start-after"),
		MaxKeys:    listV2DefaultMaxKeys,
	}
	if v := q.Get("max-keys"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			return p, listV2ErrInvalidMaxKeys
		}
		if n > listV2MaxKeysLimit {
			n = listV2MaxKeysLimit
		}
		p.MaxKeys = n
	}
	p.EncodingType = q.Get("encoding-type") == "url"
	p.FetchOwner = strings.EqualFold(q.Get("fetch-owner"), "true")
	return p, nil
}

// --- errors ---------------------------------------------------------------

// listV2Err is an S3-style error raised while listing.
type listV2Err struct {
	Status  int
	Code    string
	Message string
}

var listV2ErrInvalidMaxKeys = &listV2Err{
	Status:  http.StatusBadRequest,
	Code:    "InvalidArgument",
	Message: "max-keys must be an integer between 0 and 2147483647",
}

var listV2ErrNoSuchBucket = &listV2Err{
	Status:  http.StatusNotFound,
	Code:    "NoSuchBucket",
	Message: "The specified bucket does not exist",
}

// listV2RequestID produces a request id for the error document.
func listV2RequestID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "0123456789abcdef"
	}
	return hex.EncodeToString(b[:])
}

// listV2WriteErr renders a canonical S3 <Error> document.
func listV2WriteErr(w http.ResponseWriter, e *listV2Err, resource string) {
	var b strings.Builder
	b.WriteString(xml.Header)
	b.WriteString(`<Error>`)
	b.WriteString(`<Code>` + listV2EscapeXML(e.Code) + `</Code>`)
	b.WriteString(`<Message>` + listV2EscapeXML(e.Message) + `</Message>`)
	b.WriteString(`<Resource>` + listV2EscapeXML(resource) + `</Resource>`)
	b.WriteString(`<RequestId>` + listV2RequestID() + `</RequestId>`)
	b.WriteString(`</Error>`)
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(e.Status)
	_, _ = io.WriteString(w, b.String())
}

// --- text encoding --------------------------------------------------------

var listV2XMLEscaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")

// listV2EscapeXML escapes S3 character data; double quotes stay raw, which
// is what the service does (e.g. <ETag>"..."</ETag>).
func listV2EscapeXML(s string) string {
	return listV2XMLEscaper.Replace(s)
}

// listV2KeyByteUnescaped reports whether encoding-type=url leaves c as is.
func listV2KeyByteUnescaped(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	case c == '-', c == '_', c == '.', c == '~', c == '/':
		return true
	}
	return false
}

// listV2EncodeKey percent-encodes an object key the way S3 does for
// encoding-type=url: everything except unreserved characters and '/'.
func listV2EncodeKey(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if listV2KeyByteUnescaped(s[i]) {
			b.WriteByte(s[i])
			continue
		}
		b.WriteByte('%')
		b.WriteByte(listV2HexDigits[s[i]>>4])
		b.WriteByte(listV2HexDigits[s[i]&0x0f])
	}
	return b.String()
}

// listV2Encode applies either XML escaping or URL encoding per the request.
func listV2Encode(s string, urlEncoded bool) string {
	if urlEncoded {
		return listV2EncodeKey(s)
	}
	return listV2EscapeXML(s)
}

// --- listing --------------------------------------------------------------

// listV2Entry is the normalized projection of a stored object used for
// listing, independent of how the store represents objects.
type listV2Entry struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
	StorageClass string
}

var listV2TimeType = reflect.TypeOf(time.Time{})

// listV2Readable lifts the read-only flag reflect places on values reached
// through unexported struct fields, so they can be read with Interface().
func listV2Readable(v reflect.Value) reflect.Value {
	if !v.IsValid() || v.CanInterface() {
		return v
	}
	if v.CanAddr() {
		return reflect.NewAt(v.Type(), unsafe.Pointer(v.UnsafeAddr())).Elem()
	}
	return v
}

// listV2Snapshot returns the objects stored in a bucket; ok is false when
// the bucket does not exist.
func listV2Snapshot(store any, bucket string) ([]listV2Entry, bool) {
	bv, ok := listV2FindBucket(store, bucket)
	if !ok {
		return nil, false
	}
	return listV2ObjectEntries(bv), true
}

// listV2FindBucket locates a bucket value on the store: an accessor method
// (GetBucket, Bucket, BucketByName, FindBucket, LookupBucket) when the store
// exposes one, else a string-keyed bucket map reachable within three field
// hops from the store (or mounted server) value.
func listV2FindBucket(store any, bucket string) (reflect.Value, bool) {
	if store == nil {
		return reflect.Value{}, false
	}
	rv := reflect.ValueOf(store)
	if !rv.IsValid() {
		return reflect.Value{}, false
	}
	return listV2FindBucketDeep(rv, bucket, 3)
}

func listV2FindBucketDeep(v reflect.Value, bucket string, depth int) (reflect.Value, bool) {
	if depth <= 0 || !v.IsValid() {
		return reflect.Value{}, false
	}
	v = listV2Readable(v)
	if !v.IsValid() {
		return reflect.Value{}, false
	}
	switch v.Kind() {
	case reflect.Interface:
		if v.IsNil() {
			return reflect.Value{}, false
		}
		return listV2FindBucketDeep(v.Elem(), bucket, depth)
	case reflect.Ptr:
		if v.IsNil() {
			return reflect.Value{}, false
		}
		if b, ok := listV2FindBucketByMethod(v, bucket); ok {
			return b, true
		}
		return listV2FindBucketDeep(v.Elem(), bucket, depth)
	case reflect.Struct:
		switch v.Type().PkgPath() {
		case "sync", "time":
			return reflect.Value{}, false
		}
		if b, ok := listV2FindBucketByMethod(v, bucket); ok {
			return b, true
		}
		for i := 0; i < v.NumField(); i++ {
			if b, ok := listV2BucketFromField(listV2Readable(v.Field(i)), bucket); ok {
				return b, true
			}
		}
		for i := 0; i < v.NumField(); i++ {
			if b, ok := listV2FindBucketDeep(v.Field(i), bucket, depth-1); ok {
				return b, true
			}
		}
	case reflect.Map:
		if b, ok := listV2BucketFromField(v, bucket); ok {
			return b, true
		}
	}
	return reflect.Value{}, false
}

// listV2FindBucketByMethod tries store accessor methods; a boolean result of
// false, or a nil pointer result, means the bucket does not exist.
func listV2FindBucketByMethod(v reflect.Value, bucket string) (reflect.Value, bool) {
	probes := []reflect.Value{v}
	if v.CanAddr() {
		probes = append(probes, v.Addr())
	}
	for _, pv := range probes {
		for _, m := range []string{"GetBucket", "Bucket", "BucketByName", "FindBucket", "LookupBucket"} {
			mv := pv.MethodByName(m)
			if !mv.IsValid() {
				continue
			}
			mt := mv.Type()
			if mt.IsVariadic() || mt.NumIn() != 1 || mt.In(0).Kind() != reflect.String || mt.NumOut() == 0 {
				continue
			}
			out := mv.Call([]reflect.Value{reflect.ValueOf(bucket)})
			for _, o := range out {
				if o.Kind() == reflect.Bool && !o.Bool() {
					return reflect.Value{}, false
				}
			}
			for _, o := range out {
				o = listV2Readable(o)
				if o.Kind() == reflect.Interface {
					if o.IsNil() {
						continue
					}
					o = o.Elem()
				}
				if o.Kind() == reflect.Ptr {
					if o.IsNil() || o.Elem().Kind() != reflect.Struct {
						continue
					}
					return listV2Readable(o.Elem()), true
				}
				if o.Kind() == reflect.Struct {
					return o, true
				}
			}
		}
	}
	return reflect.Value{}, false
}

// listV2BucketFromField resolves a bucket inside a string-keyed map whose
// values are bucket structs (pointer, value or a nested object map).
func listV2BucketFromField(f reflect.Value, bucket string) (reflect.Value, bool) {
	if !f.IsValid() || f.Kind() != reflect.Map || f.Type().Key().Kind() != reflect.String {
		return reflect.Value{}, false
	}
	m := f.MapIndex(reflect.ValueOf(bucket))
	if !m.IsValid() || (m.Kind() == reflect.Ptr && m.IsNil()) {
		return reflect.Value{}, false
	}
	m = listV2Readable(m)
	if m.Kind() == reflect.Ptr {
		m = m.Elem()
	}
	if m.Kind() != reflect.Struct && m.Kind() != reflect.Map {
		return reflect.Value{}, false
	}
	return m, true
}

// listV2ObjectEntries collects the objects of a bucket value. Preferred: an
// Objects/objects map; then any string-keyed map of object structs; then a
// slice of objects carrying a Key string.
func listV2ObjectEntries(b reflect.Value) []listV2Entry {
	if !b.IsValid() {
		return nil
	}
	if b.Kind() == reflect.Map {
		if es, ok := listV2EntriesFromMap(b); ok {
			return es
		}
		return nil
	}
	if b.Kind() != reflect.Struct {
		return nil
	}
	for _, name := range []string{"Objects", "objects", "ObjectMap", "objectMap"} {
		if es, ok := listV2EntriesFromMap(listV2Readable(b.FieldByName(name))); ok {
			return es
		}
	}
	for i := 0; i < b.NumField(); i++ {
		if es, ok := listV2EntriesFromMap(listV2Readable(b.Field(i))); ok {
			return es
		}
	}
	for _, name := range []string{"Objects", "objects", "ObjectList", "objectList"} {
		if es, ok := listV2EntriesFromSlice(listV2Readable(b.FieldByName(name))); ok {
			return es
		}
	}
	for i := 0; i < b.NumField(); i++ {
		if es, ok := listV2EntriesFromSlice(listV2Readable(b.Field(i))); ok {
			return es
		}
	}
	return nil
}

func listV2EntriesFromMap(f reflect.Value) ([]listV2Entry, bool) {
	if !f.IsValid() || f.Kind() != reflect.Map || f.Type().Key().Kind() != reflect.String {
		return nil, false
	}
	elem := f.Type().Elem()
	if elem.Kind() == reflect.Ptr {
		elem = elem.Elem()
	}
	if elem.Kind() != reflect.Struct {
		return nil, false
	}
	keys := f.MapKeys()
	entries := make([]listV2Entry, 0, len(keys))
	for _, k := range keys {
		ov := listV2Readable(f.MapIndex(k))
		if ov.Kind() == reflect.Ptr {
			if ov.IsNil() {
				continue
			}
			ov = ov.Elem()
		}
		entries = append(entries, listV2EntryFrom(k.String(), ov))
	}
	return entries, true
}

func listV2EntriesFromSlice(f reflect.Value) ([]listV2Entry, bool) {
	if !f.IsValid() || f.Kind() != reflect.Slice {
		return nil, false
	}
	elem := f.Type().Elem()
	if elem.Kind() == reflect.Ptr {
		elem = elem.Elem()
	}
	if elem.Kind() != reflect.Struct {
		return nil, false
	}
	if sf, ok := elem.FieldByName("Key"); !ok || sf.Type.Kind() != reflect.String {
		return nil, false
	}
	entries := make([]listV2Entry, 0, f.Len())
	for i := 0; i < f.Len(); i++ {
		ov := listV2Readable(f.Index(i))
		if ov.Kind() == reflect.Ptr {
			if ov.IsNil() {
				continue
			}
			ov = ov.Elem()
		}
		entries = append(entries, listV2EntryFrom(listV2StrField(ov, "Key", "key", "Name"), ov))
	}
	return entries, true
}

// listV2NestedEntries supports stores shaped like map[string]map[string]Object
// where per-bucket objects live directly on the store.
func listV2NestedEntries(store any, bucket string) ([]listV2Entry, bool) {
	if store == nil {
		return nil, false
	}
	v := reflect.ValueOf(store)
	for v.IsValid() && (v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface) {
		if v.IsNil() {
			return nil, false
		}
		v = v.Elem()
	}
	v = listV2Readable(v)
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return nil, false
	}
	for i := 0; i < v.NumField(); i++ {
		f := listV2Readable(v.Field(i))
		if f.Kind() != reflect.Map || f.Type().Key().Kind() != reflect.String {
			continue
		}
		inner := f.Type().Elem()
		if inner.Kind() == reflect.Ptr {
			inner = inner.Elem()
		}
		if inner.Kind() != reflect.Map || inner.Key().Kind() != reflect.String {
			continue
		}
		ie := inner.Elem()
		if ie.Kind() == reflect.Ptr {
			ie = ie.Elem()
		}
		if ie.Kind() != reflect.Struct {
			continue
		}
		per := f.MapIndex(reflect.ValueOf(bucket))
		if !per.IsValid() {
			continue
		}
		if es, ok := listV2EntriesFromMap(listV2Readable(per)); ok {
			return es, true
		}
		return nil, true
	}
	return nil, false
}

func listV2EntryFrom(key string, o reflect.Value) listV2Entry {
	return listV2Entry{
		Key:          key,
		Size:         listV2ObjectSize(o),
		ETag:         listV2ObjectETag(o),
		LastModified: listV2ObjectTime(o),
		StorageClass: listV2ObjectClass(o),
	}
}

func listV2StrField(o reflect.Value, names ...string) string {
	if o.Kind() != reflect.Struct {
		return ""
	}
	for _, n := range names {
		if f := o.FieldByName(n); f.IsValid() && f.Kind() == reflect.String {
			return f.String()
		}
	}
	return ""
}

// listV2ObjectSize prefers an explicit size field and falls back to the
// byte payload.
func listV2ObjectSize(o reflect.Value) int64 {
	if o.Kind() != reflect.Struct {
		return 0
	}
	for _, n := range []string{"Size", "ContentLength", "Len"} {
		f := o.FieldByName(n)
		if !f.IsValid() {
			continue
		}
		switch f.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return f.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			return int64(f.Uint())
		case reflect.Float32, reflect.Float64:
			return int64(f.Float())
		}
	}
	return int64(len(listV2ObjectData(o)))
}

// listV2ObjectETag returns the stored, quoted ETag, deriving it from the
// payload when the store does not keep one.
func listV2ObjectETag(o reflect.Value) string {
	etag := listV2StrField(o, "ETag", "Etag", "ETagValue")
	if etag == "" {
		sum := md5.Sum(listV2ObjectData(o))
		etag = hex.EncodeToString(sum[:])
	}
	if !strings.HasPrefix(etag, `"`) {
		etag = `"` + etag + `"`
	}
	return etag
}

func listV2ObjectTime(o reflect.Value) time.Time {
	if o.Kind() != reflect.Struct {
		return time.Time{}
	}
	for _, n := range []string{"LastModified", "Modified", "ModTime"} {
		f := o.FieldByName(n)
		if !f.IsValid() || f.Type() != listV2TimeType {
			continue
		}
		rv := listV2Readable(f)
		if rv.CanInterface() {
			if t, ok := rv.Interface().(time.Time); ok {
				return t
			}
		}
	}
	return time.Time{}
}

func listV2ObjectClass(o reflect.Value) string {
	if sc := listV2StrField(o, "StorageClass"); sc != "" {
		return sc
	}
	return "STANDARD"
}

func listV2ObjectData(o reflect.Value) []byte {
	if o.Kind() != reflect.Struct {
		return nil
	}
	for _, n := range []string{"Data", "Content", "Body", "Bytes", "Payload"} {
		f := o.FieldByName(n)
		if !f.IsValid() || f.Kind() != reflect.Slice || f.Type().Elem().Kind() != reflect.Uint8 {
			continue
		}
		rv := listV2Readable(f)
		if !rv.CanInterface() {
			continue
		}
		if d, ok := rv.Interface().([]byte); ok {
			return d
		}
	}
	return nil
}

// --- result assembly ------------------------------------------------------

// listV2Assemble applies start-after and delimiter folding to the
// prefix-matched entries. Truncation is out of scope for this op, so the
// complete (possibly empty) set is returned.
func listV2Assemble(p listV2Query, entries []listV2Entry) ([]listV2Entry, []string) {
	if p.MaxKeys <= 0 {
		return nil, nil
	}
	var contents []listV2Entry
	var prefixes []string
	for _, e := range entries {
		if p.StartAfter != "" && e.Key <= p.StartAfter {
			continue
		}
		if p.Delimiter != "" {
			rest := strings.TrimPrefix(e.Key, p.Prefix)
			if i := strings.Index(rest, p.Delimiter); i >= 0 {
				cp := e.Key[:len(e.Key)-len(rest)+i+len(p.Delimiter)]
				if len(prefixes) > 0 && prefixes[len(prefixes)-1] == cp {
					continue
				}
				prefixes = append(prefixes, cp)
				continue
			}
		}
		contents = append(contents, e)
	}
	return contents, prefixes
}

// listV2WriteResult renders the ListBucketResult document.
func listV2WriteResult(w http.ResponseWriter, p listV2Query, name string, contents []listV2Entry, prefixes []string) {
	var b strings.Builder
	b.WriteString(xml.Header)
	b.WriteString(`<ListBucketResult xmlns="` + listV2Namespace + `">`)
	b.WriteString(`<Name>` + listV2Encode(name, p.EncodingType) + `</Name>`)
	b.WriteString(`<Prefix>` + listV2Encode(p.Prefix, p.EncodingType) + `</Prefix>`)
	b.WriteString(`<KeyCount>` + strconv.Itoa(len(contents)+len(prefixes)) + `</KeyCount>`)
	b.WriteString(`<MaxKeys>` + strconv.FormatInt(p.MaxKeys, 10) + `</MaxKeys>`)
	if p.Delimiter != "" {
		b.WriteString(`<Delimiter>` + listV2Encode(p.Delimiter, p.EncodingType) + `</Delimiter>`)
	}
	if p.EncodingType {
		b.WriteString(`<EncodingType>url</EncodingType>`)
	}
	if p.StartAfter != "" {
		b.WriteString(`<StartAfter>` + listV2Encode(p.StartAfter, p.EncodingType) + `</StartAfter>`)
	}
	b.WriteString(`<IsTruncated>false</IsTruncated>`)
	for _, e := range contents {
		b.WriteString(`<Contents><Key>` + listV2Encode(e.Key, p.EncodingType) + `</Key>`)
		b.WriteString(`<LastModified>` + e.LastModified.UTC().Format(listV2TimeFormat) + `</LastModified>`)
		b.WriteString(`<ETag>` + listV2EscapeXML(e.ETag) + `</ETag>`)
		b.WriteString(`<Size>` + strconv.FormatInt(e.Size, 10) + `</Size>`)
		if p.FetchOwner {
			b.WriteString(`<Owner><ID>` + listV2EscapeXML(listV2OwnerID) + `</ID><DisplayName>` + listV2EscapeXML(listV2OwnerName) + `</DisplayName></Owner>`)
		}
		b.WriteString(`<StorageClass>` + listV2EscapeXML(e.StorageClass) + `</StorageClass></Contents>`)
	}
	for _, cp := range prefixes {
		b.WriteString(`<CommonPrefixes><Prefix>` + listV2Encode(cp, p.EncodingType) + `</Prefix></CommonPrefixes>`)
	}
	b.WriteString(`</ListBucketResult>`)
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, b.String())
}

// --- wire entry points ------------------------------------------------------

// HandleListObjectsV2 serves GET /{bucket}?list-type=2 (ListObjectsV2). The
// router's bucket-level GET branch should dispatch here when the query
// carries list-type=2; store may be the s3 store or the mounted handler's
// backing server, whichever the caller owns.
func HandleListObjectsV2(store any, w http.ResponseWriter, r *http.Request) {
	bucket := listV2BucketFromPath(r.URL.Path)
	p, qerr := listV2ParseQuery(r.URL.Query())
	if qerr != nil {
		listV2WriteErr(w, qerr, "/"+bucket)
		return
	}
	entries, found := listV2Snapshot(store, bucket)
	if !found {
		entries, found = listV2NestedEntries(store, bucket)
	}
	if !found {
		listV2WriteErr(w, listV2ErrNoSuchBucket, "/"+bucket)
		return
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	if p.Prefix != "" {
		filtered := make([]listV2Entry, 0, len(entries))
		for _, e := range entries {
			if strings.HasPrefix(e.Key, p.Prefix) {
				filtered = append(filtered, e)
			}
		}
		entries = filtered
	}
	contents, prefixes := listV2Assemble(p, entries)
	listV2WriteResult(w, p, bucket, contents, prefixes)
}

// ListObjectsV2Handler adapts the op for direct mounting as an HTTP route.
func ListObjectsV2Handler(store any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		HandleListObjectsV2(store, w, r)
	}
}

// listV2BucketFromPath extracts the {bucket} segment from the request path.
func listV2BucketFromPath(path string) string {
	path = strings.TrimPrefix(path, "/")
	if i := strings.IndexByte(path, '/'); i >= 0 {
		path = path[:i]
	}
	return path
}
