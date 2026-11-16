// Package awsquery handles the AWS Query/XML wire protocol for SQS.
package awsquery

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"
)

func Parse(r *http.Request) (action string, params map[string]string, err error) {
	if e := r.ParseForm(); e != nil {
		return "", nil, e
	}
	action = r.Form.Get("Action")
	params = map[string]string{}
	for k, v := range r.Form {
		params[k] = strings.Join(v, "")
	}
	return action, params, nil
}

func Serve(w http.ResponseWriter, r *http.Request, respond any) {
	body, e := xml.Marshal(respond)
	if e != nil {
		return
	}
	w.Header().Set("Content-Type", "text/xml")
	fmt.Fprintf(w, `<?xml version="1.0"?><%sResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">%s</%sResponse>`, action(r), string(body), action(r))
}

func action(r *http.Request) string {
	if e := r.ParseForm(); e != nil {
		return "Unknown"
	}
	return r.Form.Get("Action")
}

func ReadBody(r *http.Request) (string, error) {
	b, e := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	return string(b), e
}
