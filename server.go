// Server routes requests by the X-Amz-Target header, in the same manner as
// the DynamoDB JSON-RPC service, and renders x-amz-json-1.0 responses.
package main

import (
    "encoding/json"
    "net/http"
    "strings"
)

// Server dispatches DynamoDB operations against a Store.
type Server struct {
    store     *Store
    region    string
    accountID string
}

// NewServer wires a server to a store, with fallback region/account used in
// TableArns when a request carries no SigV4 credentials.
func NewServer(store *Store, region, accountID string) *Server {
    if region == "" {
        region = "us-east-1"
    }
    if accountID == "" {
        accountID = "000000000000"
    }
    return &Server{store: store, region: region, accountID: accountID}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        // DynamoDB only serves POSTs carrying an X-Amz-Target header; anything
        // else is a protocol-level unknown operation.
        writeAPIError(w, errUnknownOperation())
        return
    }

    switch r.Header.Get("X-Amz-Target") {
    case targetPrefix + ".CreateTable":
        s.createTable(w, r)
    default:
        writeAPIError(w, errUnknownOperation())
    }
}

// resolveRegionAccount derives the region and account id used in TableArns.
// Following DynamoDB (Local), both come from the SigV4 credential scope of
// the request when available, falling back to the configured defaults.
func (s *Server) resolveRegionAccount(r *http.Request) (string, string) {
    auth := r.Header.Get("Authorization")
    if idx := strings.Index(auth, "Credential="); idx >= 0 {
        scope := auth[idx+len("Credential="):]
        if end := strings.IndexByte(scope, ','); end >= 0 {
            scope = scope[:end]
        }
        if parts := strings.Split(strings.TrimSpace(scope), "/"); len(parts) >= 3 {
            account := s.accountID
            if isTwelveDigits(parts[0]) {
                account = parts[0]
            }
            return parts[2], account
        }
    }
    return s.region, s.accountID
}

func isTwelveDigits(v string) bool {
    if len(v) != 12 {
        return false
    }
    for i := 0; i < len(v); i++ {
        if v[i] < '0' || v[i] > '9' {
            return false
        }
    }
    return true
}

func setJSONHeaders(w http.ResponseWriter) {
    w.Header().Set("Content-Type", "application/x-amz-json-1.0")
    w.Header().Set("x-amzn-RequestId", newRequestID())
}

func writeJSON(w http.ResponseWriter, status int, v any) {
    payload, err := json.Marshal(v)
    if err != nil {
        writeAPIError(w, ddbErr("InternalServerError", err.Error()))
        return
    }
    setJSONHeaders(w)
    w.WriteHeader(status)
    _, _ = w.Write(payload)
}

func writeAPIError(w http.ResponseWriter, e *apiError) {
    setJSONHeaders(w)
    code := e.Type
    if i := strings.LastIndex(e.Type, "#"); i >= 0 {
        code = e.Type[i+1:]
    }
    w.Header().Set("X-Amzn-Errortype", code)
    payload := struct {
        Type    string `json:"__type"`
        Message string `json:"message,omitempty"`
    }{Type: e.Type, Message: e.Message}
    encoded, err := json.Marshal(payload)
    if err != nil {
        encoded = []byte(`{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError"}`)
    }
    w.WriteHeader(e.Status)
    _, _ = w.Write(encoded)
}
