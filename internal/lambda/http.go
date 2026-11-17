// Package lambda is the Go port of Floci's fake local AWS Lambda service,
// scoped to in-process execution (the upstream Docker orchestration does not
// apply client-side).
//
// Execution model: every function is bound at CreateFunction time to an
// in-process Runner executing inside the server process itself.
//
//   - RequestResponse invokes run synchronously on the calling goroutine.
//   - Event invokes run on background goroutines tracked by an idle counter
//     (WaitForIdle lets callers join on outstanding work).
//   - DryRun validates the request and answers 204 without executing.
//
// Runner is pluggable (RegisterRunner), so a deferred executor could later be
// added behind the same interface. Shipped runners: echoRunner (in-process
// stub for runtime "floci/echo", also the fallback for runtimes that cannot
// be executed locally) and scriptRunner (custom runtime "provided.*":
// extracts the function zip and runs its "bootstrap" executable on the local
// host, event on stdin, result on stdout).
//
// Scope: CreateFunction, ListFunctions, Invoke, GetFunction, DeleteFunction
// over the /2015-03-31 Lambda API paths, plus code download. S3 code
// packages, layers, aliases/versions, event source mappings and concurrency
// settings are intentionally out of scope (upstream is ~26.5K LOC).
package lambda

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"io"
	"encoding/json"
	"strconv"
	"strings"
)

// ServeHTTP routes the AWS Lambda /2015-03-31 API paths understood by
// floci-go.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
  segs := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
  switch {
  case len(segs) == 1 && segs[0] == "":
    if r.Method == http.MethodGet {
      s.handleInfo(w)
      return
    }
    s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
  case len(segs) == 2 && segs[0] == apiVersion && segs[1] == "functions":
    switch r.Method {
    case http.MethodGet:
      s.handleList(w, r)
    case http.MethodPost:
      s.handleCreate(w, r)
    default:
      s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
    }
  case len(segs) == 3 && segs[0] == apiVersion && segs[1] == "functions":
    switch r.Method {
    case http.MethodGet:
      s.handleGet(w, r, segs[2])
    case http.MethodDelete:
      s.handleDelete(w, r, segs[2])
    default:
      s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
    }
  case len(segs) == 4 && segs[0] == apiVersion && segs[1] == "functions":
    s.handleSubresource(w, r, segs[2], segs[3])
  default:
    s.writeError(w, http.StatusNotFound, "ResourceNotFoundException", "unknown API route "+r.URL.Path)
  }
}

func (s *Service) handleSubresource(w http.ResponseWriter, r *http.Request, name, sub string) {
  switch sub {
  case "invocations":
    if r.Method != http.MethodPost {
      s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowedException", "invocations accepts POST only")
      return
    }
    s.handleInvoke(w, r, name)
  case "code":
    if r.Method != http.MethodGet {
      s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowedException", "code accepts GET only")
      return
    }
    s.handleCode(w, r, name)
  default:
    s.writeError(w, http.StatusNotFound, "ResourceNotFoundException", "unknown API route "+r.URL.Path)
  }
}

func (s *Service) handleInfo(w http.ResponseWriter) {
  w.Header().Set("Content-Type", "application/json")
  _ = json.NewEncoder(w).Encode(map[string]any{
    "service":        "floci-go",
    "version":        version,
    "awsLambdaAPI":   apiVersion,
    "executionModel": "in-process-runner",
    "endpoints": map[string]string{
      "createFunction":  "POST /2015-03-31/functions",
      "listFunctions":   "GET /2015-03-31/functions",
      "getFunction":     "GET /2015-03-31/functions/{name}",
      "deleteFunction":  "DELETE /2015-03-31/functions/{name}",
      "invoke":          "POST /2015-03-31/functions/{name}/invocations",
      "getFunctionCode": "GET /2015-03-31/functions/{name}/code",
    },
  })
}

func (s *Service) handleCreate(w http.ResponseWriter, r *http.Request) {
  var req CreateFunctionRequest
  if err := json.NewDecoder(io.LimitReader(r.Body, maxCodeBytes)).Decode(&req); err != nil {
    s.writeError(w, http.StatusBadRequest, "ValidationException", "invalid JSON request body: "+err.Error())
    return
  }
  cfg, err := s.CreateFunction(req)
  if err != nil {
    s.writeAPIError(w, err)
    return
  }
  w.Header().Set("Location", "/2015-03-31/functions/"+cfg.FunctionName)
  w.Header().Set("Content-Type", "application/json")
  w.WriteHeader(http.StatusCreated)
  _ = json.NewEncoder(w).Encode(cfg)
}

func (s *Service) handleList(w http.ResponseWriter, r *http.Request) {
  q := r.URL.Query()
  maxItems := 0
  if v := q.Get("MaxItems"); v != "" {
    n, err := strconv.Atoi(v)
    if err != nil {
      s.writeError(w, http.StatusBadRequest, "InvalidParameterValueException", "MaxItems must be an integer")
      return
    }
    maxItems = n
  }
  res, err := s.ListFunctions(q.Get("Marker"), maxItems)
  if err != nil {
    s.writeAPIError(w, err)
    return
  }
  w.Header().Set("Content-Type", "application/json")
  _ = json.NewEncoder(w).Encode(res)
}

func (s *Service) handleGet(w http.ResponseWriter, r *http.Request, name string) {
  res, err := s.GetFunction(name, s.baseURL(r))
  if err != nil {
    s.writeAPIError(w, err)
    return
  }
  w.Header().Set("Content-Type", "application/json")
  _ = json.NewEncoder(w).Encode(res)
}

// baseURL derives the public base URL used in Code.Location links.
func (s *Service) baseURL(r *http.Request) string {
  host := r.Host
  if host == "" {
    host = "127.0.0.1"
  }
  return "http://" + host
}

func (s *Service) handleDelete(w http.ResponseWriter, r *http.Request, name string) {
  if err := s.DeleteFunction(name); err != nil {
    s.writeAPIError(w, err)
    return
  }
  w.WriteHeader(http.StatusNoContent)
}

func (s *Service) handleCode(w http.ResponseWriter, r *http.Request, name string) {
  code, err := s.GetFunctionCode(name)
  if err != nil {
    s.writeAPIError(w, err)
    return
  }
  w.Header().Set("Content-Type", "application/zip")
  w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name+".zip"))
  w.WriteHeader(http.StatusOK)
  _, _ = w.Write(code)
}

func (s *Service) handleInvoke(w http.ResponseWriter, r *http.Request, name string) {
  payload, err := io.ReadAll(io.LimitReader(r.Body, maxPayloadBytes+1))
  if err != nil {
    s.writeError(w, http.StatusBadRequest, "InvalidParameterValueException", "could not read invocation payload: "+err.Error())
    return
  }
  if len(payload) > maxPayloadBytes {
    s.writeError(w, http.StatusRequestEntityTooLarge, "PayloadTooLargeException", "payload must be smaller than 6MB")
    return
  }
  opts := InvokeOptions{
    InvocationType: r.Header.Get(headerInvocationType),
    LogType:        r.Header.Get(headerLogType),
  }
  if cc := r.Header.Get(headerClientContext); cc != "" {
    if decoded, derr := base64.StdEncoding.DecodeString(cc); derr == nil {
      opts.ClientContext = decoded
    } else {
      opts.ClientContext = []byte(cc)
    }
  }
  inv, err := s.Invoke(name, payload, opts)
  if err != nil {
    s.writeAPIError(w, err)
    return
  }
  w.Header().Set("X-Amzn-RequestId", inv.RequestID)
  if opts.LogType == logTypeTail && inv.InvocationType == invocationRequestResponse {
    w.Header().Set("X-Amz-Log-Result", base64.StdEncoding.EncodeToString(tailBytes(inv.Logs.Bytes(), logTailBytes)))
  }
  switch inv.InvocationType {
  case invocationDryRun:
    w.WriteHeader(http.StatusNoContent)
  case invocationEvent:
    w.WriteHeader(http.StatusAccepted)
  default:
    w.Header().Set("Content-Type", "application/json")
    w.Header().Set("X-Amz-Executed-Version", "$LATEST")
    if inv.IsError {
      et := inv.ErrorType
      if et == "" {
        et = "HandlerError"
      }
      w.Header().Set("X-Amz-Function-Error", et)
      if len(inv.Response) == 0 {
        inv.Response = errorPayload(et, et)
      }
    }
    w.WriteHeader(http.StatusOK)
    if len(inv.Response) > 0 {
      _, _ = w.Write(inv.Response)
    }
  }
}

func (s *Service) writeAPIError(w http.ResponseWriter, err error) {
  var apiErr *APIError
  if !errors.As(err, &apiErr) {
    apiErr = &APIError{Status: http.StatusInternalServerError, Type: "InternalFailure", Message: err.Error()}
  }
  s.writeError(w, apiErr.Status, apiErr.Type, apiErr.Message)
}

func (s *Service) writeError(w http.ResponseWriter, status int, errType, msg string) {
  w.Header().Set("x-amzn-ErrorType", errType)
  w.Header().Set("Content-Type", "application/json")
  w.WriteHeader(status)
  _ = json.NewEncoder(w).Encode(map[string]string{
    "Type":    "User",
    "__type":  errType,
    "message": msg,
  })
}
