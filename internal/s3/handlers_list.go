package s3

import (
    "encoding/base64"
    "net/http"
    "net/url"
    "strconv"
    "strings"
)

type listQuery struct {
    Prefix       string
    Delimiter    string
    Marker       string // v1 marker
    StartAfter   string // v2 start-after
    ContToken    string // v2 continuation token
    FetchOwner   bool   // v2 fetch-owner
    EncodingType string
    MaxKeys      int
}

func parseListQuery(q url.Values) (*listQuery, *Error) {
    lq := &listQuery{
        Prefix:       q.Get("prefix"),
        Delimiter:    q.Get("delimiter"),
        Marker:       q.Get("marker"),
        StartAfter:   q.Get("start-after"),
        ContToken:    q.Get("continuation-token"),
        EncodingType: q.Get("encoding-type"),
        FetchOwner:   q.Get("fetch-owner") == "true",
        MaxKeys:      1000,
    }
    if v := q.Get("max-keys"); v != "" {
        n, err := strconv.Atoi(v)
        if err != nil || n < 0 {
            return nil, errInvalidArgument("max-keys must be a non-negative integer")
        }
        lq.MaxKeys = n
    }
    if lq.EncodingType != "" && lq.EncodingType != "url" {
        return nil, errInvalidArgument("Invalid Encoding Method specified in Request")
    }
    return lq, nil
}

// objectElements materializes sorted backend objects into list XML entries.
func (s *Server) objectElements(objs []ObjectInfo, withOwner bool) []ObjectEntry {
    out := make([]ObjectEntry, 0, len(objs))
    for _, o := range objs {
        entry := ObjectEntry{
            Key:          o.Key,
            LastModified: s3t(o.LastModified),
            ETag:         quoteETag(o.ETag),
            Size:         o.Size,
            StorageClass: "STANDARD",
        }
        if withOwner {
            own := s.owner
            entry.Owner = &own
        }
        out = append(out, entry)
    }
    return out
}

type listCursorItem struct {
    cursor   string // sort position: the key, or the rolled-up common prefix
    isPrefix bool
    obj      *ObjectEntry
    prefix   string
}

// buildItems turns sorted object entries into the ordered item stream that
// list responses page over. With a delimiter, keys sharing a prefix chunk
// collapse into a single CommonPrefix item.
func buildItems(entries []ObjectEntry, prefix, delimiter string) []listCursorItem {
    items := make([]listCursorItem, 0, len(entries))
    last := ""
    for i := range entries {
        e := entries[i]
        if prefix != "" && !strings.HasPrefix(e.Key, prefix) {
            continue
        }
        if delimiter != "" {
            rest := e.Key[len(prefix):]
            if idx := strings.Index(rest, delimiter); idx >= 0 {
                cp := e.Key[:len(prefix)+idx+len(delimiter)]
                if cp == last {
                    continue // key already covered by a listed common prefix
                }
                last = cp
                items = append(items, listCursorItem{cursor: cp, isPrefix: true, prefix: cp})
                continue
            }
        }
        items = append(items, listCursorItem{cursor: e.Key, obj: &entries[i]})
    }
    return items
}

// paginateItems returns up to max items positioned strictly after start,
// plus the resume cursor and whether further items exist.
func paginateItems(items []listCursorItem, start string, max int) (list []listCursorItem, next string, truncated bool) {
    if max <= 0 {
        return nil, "", false
    }
    i := 0
    for i < len(items) && items[i].cursor <= start {
        i++
    }
    for ; i < len(items) && len(list) < max; i++ {
        list = append(list, items[i])
    }
    if i < len(items) {
        truncated = true
        next = list[len(list)-1].cursor
    }
    return
}

func encodeKeyIfRequested(encodingType string) func(string) string {
    if encodingType == "url" {
        return encodeKey
    }
    return func(s string) string { return s }
}

func (s *Server) handleListObjectsV1(rc *requestCtx) {
    lq, lerr := parseListQuery(rc.r.URL.Query())
    if lerr != nil {
        rc.fail(lerr)
        return
    }
    objs, err := s.store.AllObjects(rc.bucket)
    if err != nil {
        rc.fail(err)
        return
    }
    entries := s.objectElements(objs, true)
    items := buildItems(entries, lq.Prefix, lq.Delimiter)
    list, next, truncated := paginateItems(items, lq.Marker, lq.MaxKeys)

    res := ListBucketResult{
        Xmlns:       s3Namespace,
        Name:        rc.bucket,
        MaxKeys:     lq.MaxKeys,
        IsTruncated: truncated,
    }
    enc := encodeKeyIfRequested(lq.EncodingType)
    res.Prefix = enc(lq.Prefix)
    res.Marker = enc(lq.Marker)
    res.Delimiter = enc(lq.Delimiter)
    if lq.EncodingType == "url" {
        res.EncodingType = "url"
    }
    for _, it := range list {
        if it.isPrefix {
            res.CommonPrefixes = append(res.CommonPrefixes, CommonPrefix{Prefix: enc(it.prefix)})
            continue
        }
        e := *it.obj
        e.Key = enc(e.Key)
        res.Contents = append(res.Contents, e)
    }
    if truncated && lq.Delimiter != "" {
        res.NextMarker = enc(next)
    }
    rc.xml(http.StatusOK, res)
}

func (s *Server) handleListObjectsV2(rc *requestCtx) {
    lq, lerr := parseListQuery(rc.r.URL.Query())
    if lerr != nil {
        rc.fail(lerr)
        return
    }
    cursor := lq.Marker
    token := lq.ContToken
    if token != "" {
        decoded, derr := decodeListToken(token)
        if derr != nil {
            rc.fail(errInvalidArgument("The continuation token provided is incorrect"))
            return
        }
        cursor = decoded
    } else if lq.StartAfter != "" {
        cursor = lq.StartAfter
    }
    objs, err := s.store.AllObjects(rc.bucket)
    if err != nil {
        rc.fail(err)
        return
    }
    entries := s.objectElements(objs, lq.FetchOwner)
    items := buildItems(entries, lq.Prefix, lq.Delimiter)
    list, next, truncated := paginateItems(items, cursor, lq.MaxKeys)

    res := ListBucketResultV2{
        Xmlns:       s3Namespace,
        Name:        rc.bucket,
        KeyCount:    len(list),
        MaxKeys:     lq.MaxKeys,
        IsTruncated: truncated,
    }
    enc := encodeKeyIfRequested(lq.EncodingType)
    res.Prefix = enc(lq.Prefix)
    res.Delimiter = enc(lq.Delimiter)
    if lq.StartAfter != "" && token == "" {
        res.StartAfter = enc(lq.StartAfter)
    }
    if token != "" {
        res.ContinuationToken = token
    }
    if lq.EncodingType == "url" {
        res.EncodingType = "url"
    }
    for _, it := range list {
        if it.isPrefix {
            res.CommonPrefixes = append(res.CommonPrefixes, CommonPrefix{Prefix: enc(it.prefix)})
            continue
        }
        e := *it.obj
        e.Key = enc(e.Key)
        res.Contents = append(res.Contents, e)
    }
    if truncated {
        res.NextContinuationToken = encodeListToken(next)
    }
    rc.xml(http.StatusOK, res)
}

// Continuation tokens are opaque to clients: base64url of the resume cursor.
func encodeListToken(cursor string) string {
    return base64.RawURLEncoding.EncodeToString([]byte(cursor))
}

func decodeListToken(token string) (string, error) {
    raw, err := base64.RawURLEncoding.DecodeString(token)
    if err != nil {
        return "", err
    }
    return string(raw), nil
}
