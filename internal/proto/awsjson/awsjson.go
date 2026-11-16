// Package awsjson handles the AWS x-amz-json-1.0 wire protocol for DynamoDB.
package awsjson

import (
	"encoding/json"
	"io"
	"net/http"
)

type Handler func(ctx interface{}, body []byte) (map[string]any, error)

func Serve(w http.ResponseWriter, r *http.Request, handle Handler, opName string) {
	target := r.Header.Get("X-Amz-Target")
	if target == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"__type":"com.amazonaws.dynamodb.v20120810#MissingAuthenticationToken"}`))
		return
	}
	body, e := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if e != nil {
		w.WriteHeader(400)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(200)
	result, e := handle(r.Context(), body)
	if e != nil {
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(map[string]any{"__type": "InternalException"})
		return
	}
	json.NewEncoder(w).Encode(result)
	_ = opName
}
