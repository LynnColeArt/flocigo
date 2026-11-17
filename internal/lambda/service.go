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
	"bytes"
	"context"
	crand "crypto/rand"
	"errors"
	"fmt"
	"encoding/hex"
	"net/http"
	"io"
	"encoding/json"
	"log"
	"crypto/sha256"
	"sort"
	"strings"
	"sync"
	"time"
	"archive/zip"
)

const (
  version    = "0.1.0"
  apiVersion = "2015-03-31"
)

const (
  invocationRequestResponse = "RequestResponse"
  invocationEvent           = "Event"
  invocationDryRun          = "DryRun"
)

const (
  logTypeNone = "None"
  logTypeTail = "Tail"
)

const (
  maxPayloadBytes = 6 << 20  // AWS ceiling for direct invoke payloads
  maxCodeBytes    = 64 << 20 // uncompressed zip ceiling
  historyLimit    = 100      // invocations retained per function
  logTailBytes    = 4096     // X-Amz-Log-Result tail size
)

// Header names used by the Invoke API.
const (
  headerInvocationType = "X-Amz-Invocation-Type"
  headerLogType        = "X-Amz-Log-Type"
  headerClientContext  = "X-Amz-Client-Context"
)

// APIError is a structured AWS-style error. HTTP handlers turn it into a
// status code, an x-amzn-ErrorType header and a JSON body of the form
// {"Type":"User","__type":<Type>,"message":<Message>}.
type APIError struct {
  Status  int
  Type    string
  Message string
}

func (e *APIError) Error() string { return e.Type + ": " + e.Message }

func errInvalidParameter(format string, a ...any) *APIError {
  return &APIError{Status: http.StatusBadRequest, Type: "InvalidParameterValueException", Message: fmt.Sprintf(format, a...)}
}

func errNotFound(format string, a ...any) *APIError {
  return &APIError{Status: http.StatusNotFound, Type: "ResourceNotFoundException", Message: fmt.Sprintf(format, a...)}
}

func errConflict(format string, a ...any) *APIError {
  return &APIError{Status: http.StatusConflict, Type: "ConflictException", Message: fmt.Sprintf(format, a...)}
}

// EnvironmentConfig is the AWS Environment block.
type EnvironmentConfig struct {
  Variables map[string]string `json:"Variables,omitempty"`
}

// FunctionCode is the create-time code location. floci-go supports direct zip
// packages only (S3-hosted code is rejected).
type FunctionCode struct {
  ZipFile         string `json:"ZipFile,omitempty"`
  S3Bucket        string `json:"S3Bucket,omitempty"`
  S3Key           string `json:"S3Key,omitempty"`
  S3ObjectVersion string `json:"S3ObjectVersion,omitempty"`
}

// CreateFunctionRequest is the body of POST /2015-03-31/functions.
type CreateFunctionRequest struct {
  FunctionName string             `json:"FunctionName"`
  Runtime      string             `json:"Runtime"`
  Role         string             `json:"Role"`
  Handler      string             `json:"Handler"`
  Description  string             `json:"Description"`
  Timeout      int                `json:"Timeout"`
  MemorySize   int                `json:"MemorySize"`
  Code         *FunctionCode      `json:"Code"`
  Environment  *EnvironmentConfig `json:"Environment"`
  Tags         map[string]string  `json:"Tags"`
}

// FunctionConfiguration mirrors the AWS Lambda configuration JSON (subset).
type FunctionConfiguration struct {
  FunctionName     string             `json:"FunctionName"`
  FunctionArn      string             `json:"FunctionArn"`
  Runtime          string             `json:"Runtime"`
  Role             string             `json:"Role"`
  Handler          string             `json:"Handler"`
  Description      string             `json:"Description,omitempty"`
  Timeout          int                `json:"Timeout"`
  MemorySize       int                `json:"MemorySize"`
  CodeSize         int64              `json:"CodeSize"`
  CodeSha256       string             `json:"CodeSha256"`
  PackageType      string             `json:"PackageType"`
  Environment      *EnvironmentConfig `json:"Environment,omitempty"`
  Version          string             `json:"Version"`
  State            string             `json:"State"`
  LastUpdateStatus string             `json:"LastUpdateStatus"`
  LastModified     string             `json:"LastModified"`
  Architectures    []string           `json:"Architectures"`
}

// FunctionCodeLocation is the Code block returned by GetFunction.
type FunctionCodeLocation struct {
  Location       string `json:"Location"`
  RepositoryType string `json:"RepositoryType"`
}

// GetFunctionResult is the shape of GET /2015-03-31/functions/{name}.
type GetFunctionResult struct {
  Configuration FunctionConfiguration `json:"Configuration"`
  Code          FunctionCodeLocation  `json:"Code"`
  Tags          map[string]string     `json:"Tags,omitempty"`
}

// ListFunctionsResult is the shape of GET /2015-03-31/functions.
type ListFunctionsResult struct {
  Functions  []FunctionConfiguration `json:"Functions"`
  NextMarker string                  `json:"NextMarker,omitempty"`
}

// Invocation is one in-process invocation. Runners read Event/Env and fill in
// Response, IsError, ErrorType and Logs.
type Invocation struct {
  RequestID      string
  InvocationType string
  Event          []byte
  Response       []byte
  IsError        bool
  ErrorType      string
  Logs           bytes.Buffer
  Deadline       time.Time
  Env            map[string]string
}

// Runner executes a single function invocation in-process. Returning an error
// signals an infrastructure/runner failure (surfaced as
// X-Amz-Function-Error: InternalError); function-level failures must be
// reported via in.IsError/in.ErrorType instead (HTTP 200 + header, like AWS).
type Runner interface {
  Invoke(ctx context.Context, in *Invocation) error
}

// RunnerFactory binds a code package to a runner instance at create time.
type RunnerFactory func(cfg *FunctionConfiguration, code []byte) (Runner, error)

// InvokeOptions parameterises a Go-level Invoke call.
type InvokeOptions struct {
  InvocationType string
  LogType        string
  ClientContext  []byte
}

type invocationRecord struct {
  RequestID string
  Type      string
  Error     string
  Response  []byte
  At        time.Time
}

// function is the internal record for one created function.
type function struct {
  cfg     FunctionConfiguration
  zip     []byte
  tags    map[string]string
  runner  Runner
  mu      sync.Mutex
  history []invocationRecord
}

// Service is the floci-go fake Lambda API; it implements http.Handler.
type Service struct {
  Region    string
  AccountID string

  mu              sync.Mutex
  functions       map[string]*function
  runnerFactories map[string]RunnerFactory

  idleMu   sync.Mutex
  idleN    int
  idleChan chan struct{}
}

// NewService creates an empty fake Lambda service.
func NewService() *Service {
  s := &Service{
    Region:          "us-east-1",
    AccountID:       "000000000000",
    functions:       map[string]*function{},
    runnerFactories: map[string]RunnerFactory{},
    idleChan:        make(chan struct{}),
  }
  _ = s.RegisterRunner("floci/echo", func(*FunctionConfiguration, []byte) (Runner, error) { return echoRunner{}, nil })
  return s
}

// RegisterRunner binds runtime to an in-process runner factory; functions
// created with that runtime afterwards use the factory.
func (s *Service) RegisterRunner(runtime string, factory RunnerFactory) error {
  if runtime == "" || factory == nil {
    return errors.New("floci-go: runtime and factory are required")
  }
  s.mu.Lock()
  s.runnerFactories[strings.ToLower(runtime)] = factory
  s.mu.Unlock()
  return nil
}

// newRunner selects the in-process runner for a runtime name: an exact
// registry hit wins, then the custom-runtime bootstrap runner, then the echo
// stub (the documented fallback that keeps unknown runtimes invocable).
func (s *Service) newRunner(runtime string, cfg *FunctionConfiguration, code []byte) Runner {
  lower := strings.ToLower(runtime)
  s.mu.Lock()
  factory := s.runnerFactories[lower]
  s.mu.Unlock()
  if factory != nil {
    r, err := factory(cfg, code)
    if err == nil && r != nil {
      return r
    }
    if err != nil {
      log.Printf("floci-go: runner factory for runtime %q failed: %v (falling back)", runtime, err)
    }
  }
  if strings.HasPrefix(lower, "provided") {
    return &scriptRunner{code: code, cfg: cfg, region: s.Region}
  }
  return echoRunner{}
}

func (s *Service) functionArn(name string) string {
  return fmt.Sprintf("arn:aws:lambda:%s:%s:function:%s", s.Region, s.AccountID, name)
}

func functionKey(name string) string { return strings.ToLower(name) }

// CreateFunction validates the request, stores the function record and binds
// an in-process runner to it (the execution model decision).
func (s *Service) CreateFunction(req CreateFunctionRequest) (*FunctionConfiguration, error) {
  if req.FunctionName == "" {
    return nil, errInvalidParameter("FunctionName is required")
  }
  if len(req.FunctionName) > 64 || !validFunctionName(req.FunctionName) {
    return nil, errInvalidParameter("FunctionName must match [a-zA-Z0-9-_]+ and be at most 64 characters")
  }
  if req.Runtime == "" {
    return nil, errInvalidParameter("Runtime is required")
  }
  if req.Code == nil {
    return nil, errInvalidParameter("Code is required")
  }
  if req.Code.S3Bucket != "" || req.Code.S3Key != "" || req.Code.S3ObjectVersion != "" {
    return nil, errInvalidParameter("S3-hosted code is not supported by floci-go; provide Code.ZipFile (base64) instead")
  }
  code, err := base64.StdEncoding.DecodeString(req.Code.ZipFile)
  if err != nil {
    return nil, errInvalidParameter("could not base64-decode Code.ZipFile: %v", err)
  }
  if len(code) == 0 {
    return nil, errInvalidParameter("Code.ZipFile must not be empty")
  }
  if _, err := zip.NewReader(bytes.NewReader(code), int64(len(code))); err != nil {
    return nil, errInvalidParameter("Code.ZipFile is not a valid ZIP archive: %v", err)
  }
  timeout := req.Timeout
  if timeout == 0 {
    timeout = 3
  }
  if timeout < 1 || timeout > 900 {
    return nil, errInvalidParameter("Timeout must be between 1 and 900 seconds")
  }
  memory := req.MemorySize
  if memory == 0 {
    memory = 128
  }
  if memory < 128 || memory > 10240 {
    return nil, errInvalidParameter("MemorySize must be between 128 and 10240 MB")
  }

  sum := sha256.Sum256(code)
  cfg := FunctionConfiguration{
    FunctionName:     req.FunctionName,
    FunctionArn:      s.functionArn(req.FunctionName),
    Runtime:          req.Runtime,
    Handler:          req.Handler,
    Description:      req.Description,
    Timeout:          timeout,
    MemorySize:       memory,
    CodeSize:         int64(len(code)),
    CodeSha256:       base64.StdEncoding.EncodeToString(sum[:]),
    PackageType:      "Zip",
    Version:          "$LATEST",
    State:            "Active",
    LastUpdateStatus: "Successful",
    LastModified:     time.Now().UTC().Format(time.RFC3339),
    Architectures:    []string{"x86_64"},
  }
  if cfg.Handler == "" {
    cfg.Handler = "floci/handler"
  }
  if req.Role == "" {
    cfg.Role = fmt.Sprintf("arn:aws:iam::%s:role/floci-lambda-role", s.AccountID)
  } else {
    cfg.Role = req.Role
  }
  if req.Environment != nil {
    env := &EnvironmentConfig{Variables: map[string]string{}}
    for k, v := range req.Environment.Variables {
      env.Variables[k] = v
    }
    cfg.Environment = env
  }

  fn := &function{cfg: cfg, zip: code, tags: copyTags(req.Tags)}
  fn.runner = s.newRunner(req.Runtime, &fn.cfg, code)

  s.mu.Lock()
  defer s.mu.Unlock()
  if _, exists := s.functions[functionKey(req.FunctionName)]; exists {
    return nil, errConflict("Function already exist: %s", s.functionArn(req.FunctionName))
  }
  s.functions[functionKey(req.FunctionName)] = fn
  out := fn.cfg
  return &out, nil
}

func copyTags(in map[string]string) map[string]string {
  if in == nil {
    return nil
  }
  out := map[string]string{}
  for k, v := range in {
    out[k] = v
  }
  return out
}

func validFunctionName(name string) bool {
  for _, c := range name {
    switch {
    case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_':
    default:
      return false
    }
  }
  return true
}

// ListFunctions returns configurations sorted by name with Marker/MaxItems
// pagination (default MaxItems: 50).
func (s *Service) ListFunctions(marker string, maxItems int) (*ListFunctionsResult, error) {
  if maxItems == 0 {
    maxItems = 50
  }
  if maxItems < 1 || maxItems > 10000 {
    return nil, errInvalidParameter("MaxItems must be between 1 and 10000")
  }
  s.mu.Lock()
  defer s.mu.Unlock()

  keys := make([]string, 0, len(s.functions))
  for k := range s.functions {
    keys = append(keys, k)
  }
  sort.Strings(keys)
  start := 0
  if marker != "" {
    start = -1
    for i, k := range keys {
      if k == functionKey(marker) {
        start = i + 1
        break
      }
    }
    if start < 0 {
      return nil, errInvalidParameter("Invalid Marker: %s", marker)
    }
  }
  out := make([]FunctionConfiguration, 0, maxItems)
  for i := start; i < len(keys) && len(out) < maxItems; i++ {
    out = append(out, s.functions[keys[i]].cfg)
  }
  res := &ListFunctionsResult{Functions: out}
  if len(out) > 0 && start+len(out) < len(keys) {
    res.NextMarker = out[len(out)-1].FunctionName
  }
  return res, nil
}

// GetFunction resolves a function; baseURL builds the download Location for
// the code zip.
func (s *Service) GetFunction(name, baseURL string) (*GetFunctionResult, error) {
  if baseURL == "" {
    baseURL = "http://127.0.0.1"
  }
  s.mu.Lock()
  fn, ok := s.functions[functionKey(name)]
  s.mu.Unlock()
  if !ok {
    return nil, errNotFound("Function not found: %s", s.functionArn(name))
  }
  return &GetFunctionResult{
    Configuration: fn.cfg,
    Code: FunctionCodeLocation{
      Location:       baseURL + "/2015-03-31/functions/" + fn.cfg.FunctionName + "/code",
      RepositoryType: "floci",
    },
    Tags: copyTags(fn.tags),
  }, nil
}

// GetFunctionCode returns the original code zip of a function.
func (s *Service) GetFunctionCode(name string) ([]byte, error) {
  s.mu.Lock()
  fn, ok := s.functions[functionKey(name)]
  s.mu.Unlock()
  if !ok {
    return nil, errNotFound("Function not found: %s", s.functionArn(name))
  }
  return fn.zip, nil
}

// DeleteFunction removes the record and stops its in-process runner.
func (s *Service) DeleteFunction(name string) error {
  key := functionKey(name)
  s.mu.Lock()
  fn, ok := s.functions[key]
  if ok {
    delete(s.functions, key)
  }
  s.mu.Unlock()
  if !ok {
    return errNotFound("Function not found: %s", s.functionArn(name))
  }
  if closer, ok := fn.runner.(io.Closer); ok {
    _ = closer.Close()
  }
  return nil
}

// Invoke executes a function per the execution model: RequestResponse runs
// synchronously in the caller's goroutine, Event on a background goroutine,
// DryRun validates only.
func (s *Service) Invoke(name string, payload []byte, opts InvokeOptions) (*Invocation, error) {
  it := opts.InvocationType
  if it == "" {
    it = invocationRequestResponse
  }
  switch it {
  case invocationRequestResponse, invocationEvent, invocationDryRun:
  default:
    return nil, errInvalidParameter("Unknown invocation type %q", it)
  }
  s.mu.Lock()
  fn, ok := s.functions[functionKey(name)]
  s.mu.Unlock()
  if !ok {
    return nil, errNotFound("Function not found: %s", s.functionArn(name))
  }
  timeout := time.Duration(fn.cfg.Timeout) * time.Second
  inv := &Invocation{
    RequestID:      newRequestID(),
    InvocationType: it,
    Event:          limitCopy(payload, maxPayloadBytes),
    Env:            map[string]string{},
    Deadline:       time.Now().Add(timeout),
  }
  if len(opts.ClientContext) > 0 {
    inv.Env["floci_client_context"] = string(opts.ClientContext)
  }
  switch it {
  case invocationDryRun:
    return inv, nil
  case invocationEvent:
    s.trackAsync()
    go func() {
      defer s.untrackAsync()
      s.runInvocation(fn, inv)
    }()
    return inv, nil
  default:
    s.runInvocation(fn, inv)
    return inv, nil
  }
}

// runInvocation executes one invocation against the bound runner and records
// the outcome in the per-function history.
func (s *Service) runInvocation(fn *function, inv *Invocation) {
  env := map[string]string{}
  if fn.cfg.Environment != nil {
    for k, v := range fn.cfg.Environment.Variables {
      env[k] = v
    }
  }
  for k, v := range inv.Env {
    env[k] = v
  }
  inv.Env = env

  ctx, cancel := context.WithDeadline(context.Background(), inv.Deadline)
  defer cancel()

  var err error
  if fn.runner == nil {
    err = errors.New("no in-process runner bound")
  } else {
    err = safeInvoke(fn.runner, ctx, inv)
  }

  switch {
  case ctx.Err() == context.DeadlineExceeded:
    inv.IsError = true
    inv.ErrorType = "TimeoutError"
    inv.Response = errorPayload("TimeoutError", fmt.Sprintf("handler exceeded %d seconds", fn.cfg.Timeout))
  case err != nil:
    log.Printf("floci-go: invoke of %s failed: %v", fn.cfg.FunctionName, err)
    inv.IsError = true
    inv.ErrorType = "InternalError"
    inv.Response = errorPayload("InternalError", fmt.Sprintf("runner failure: %v", err))
  }

  rec := invocationRecord{RequestID: inv.RequestID, Type: inv.InvocationType, At: time.Now().UTC()}
  if inv.IsError {
    rec.Error = inv.ErrorType
  }
  rec.Response = limitCopy(inv.Response, logTailBytes)
  fn.mu.Lock()
  fn.history = append(fn.history, rec)
  if n := len(fn.history); n > historyLimit {
    fn.history = fn.history[n-historyLimit:]
  }
  fn.mu.Unlock()
}

func safeInvoke(r Runner, ctx context.Context, in *Invocation) (err error) {
  defer func() {
    if p := recover(); p != nil {
      err = fmt.Errorf("in-process runner panic: %v", p)
    }
  }()
  return r.Invoke(ctx, in)
}

func (s *Service) trackAsync() {
  s.idleMu.Lock()
  s.idleN++
  s.idleMu.Unlock()
}

func (s *Service) untrackAsync() {
  s.idleMu.Lock()
  s.idleN--
  if s.idleN == 0 {
    close(s.idleChan)
    s.idleChan = make(chan struct{})
  }
  s.idleMu.Unlock()
}

// WaitForIdle blocks until every scheduled Event invocation has completed or
// ctx expires; returns whether the service reached an idle state.
func (s *Service) WaitForIdle(ctx context.Context) bool {
  s.idleMu.Lock()
  for s.idleN > 0 {
    ch := s.idleChan
    s.idleMu.Unlock()
    select {
    case <-ch:
    case <-ctx.Done():
      return false
    }
    s.idleMu.Lock()
  }
  s.idleMu.Unlock()
  return true
}

// Close deletes every function and releases runner resources (extracted code
// directories for the custom runtime).
func (s *Service) Close() error {
  s.mu.Lock()
  fns := s.functions
  s.functions = map[string]*function{}
  s.mu.Unlock()
  for _, fn := range fns {
    if closer, ok := fn.runner.(io.Closer); ok {
      _ = closer.Close()
    }
  }
  return nil
}

func newRequestID() string {
  var b [16]byte
  if _, err := crand.Read(b[:]); err != nil {
    for i := range b {
      b[i] = byte(time.Now().UnixNano() >> uint(i))
    }
  }
  b[6] = (b[6] & 0x0f) | 0x40
  b[8] = (b[8] & 0x3f) | 0x80
  return fmt.Sprintf("%s-%s-%s-%s-%s",
    hex.EncodeToString(b[0:4]), hex.EncodeToString(b[4:6]),
    hex.EncodeToString(b[6:8]), hex.EncodeToString(b[8:10]),
    hex.EncodeToString(b[10:16]))
}

func errorPayload(errType, msg string) []byte {
  b, _ := json.Marshal(map[string]string{"errorType": errType, "errorMessage": msg})
  return b
}

func limitCopy(b []byte, limit int) []byte {
  if len(b) > limit {
    b = b[:limit]
  }
  out := make([]byte, len(b))
  copy(out, b)
  return out
}

func tailBytes(b []byte, limit int) []byte {
  if len(b) > limit {
    return b[len(b)-limit:]
  }
  return b
}
