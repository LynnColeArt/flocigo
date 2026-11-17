// Command floci-go is a Go port of floci, the fake local AWS Lambda service.
//
// Execution model decision: in-process function runner (no deferred,
// Docker-backed Lambda execution - the upstream container orchestration does
// not apply client-side). Every function is bound at CreateFunction time to
// an in-process Runner executing inside the server process itself:
//
//   - RequestResponse invokes run synchronously on the calling goroutine; the
//     result (or the function error) flows straight back to the caller.
//   - Event invokes run on background goroutines tracked by an idle counter
//     (WaitForIdle lets callers join on outstanding work).
//   - DryRun validates the request and answers 204 without executing.
//
// Runner is pluggable (RegisterRunner), so a deferred executor could later be
// added behind the same interface. Shipped runners: echoRunner (the
// in-process stub for runtime "floci/echo", also the fallback for runtimes
// that cannot be executed locally) and scriptRunner (custom runtime
// "provided.*": extracts the function zip and runs its "bootstrap" executable
// on the local host, event on stdin, result on stdout).
//
// Scope: CreateFunction, ListFunctions, Invoke, GetFunction, DeleteFunction
// over the /2015-03-31 Lambda API paths, plus code download. S3 code
// packages, layers, aliases/versions, event source mappings and concurrency
// settings are intentionally out of scope (upstream is ~26.5K LOC).
package main

import (
  "archive/zip"
  "bytes"
  "context"
  crand "crypto/rand"
  "crypto/sha256"
  "encoding/base64"
  "encoding/hex"
  "encoding/json"
  "errors"
  "fmt"
  "io"
  "log"
  "net/http"
  "os"
  "os/exec"
  "path/filepath"
  "sort"
  "strconv"
  "strings"
  "sync"
  "time"
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

// echoRunner is the in-process stub runner: it echoes the event payload back
// unchanged. It backs runtime "floci/echo" and acts as the fallback for
// runtimes that floci-go cannot execute locally, keeping the fake invocable
// without Docker-backed function execution.
type echoRunner struct{}

func (echoRunner) Invoke(_ context.Context, in *Invocation) error {
  in.Response = limitCopy(in.Event, maxPayloadBytes)
  return nil
}

// scriptRunner runs a custom-runtime code package on the local host: the zip
// must contain an executable "bootstrap"; the event is piped to its stdin and
// its stdout is returned verbatim as the invocation result (stderr feeds the
// invocation logs). This keeps function execution in-process (no containers)
// while still exercising real user code.
type scriptRunner struct {
  code   []byte
  cfg    *FunctionConfiguration
  region string

  once  sync.Once
  dir   string
  ready error
}

func (sr *scriptRunner) Invoke(ctx context.Context, in *Invocation) error {
  sr.once.Do(sr.extract)
  if sr.ready != nil {
    return sr.ready
  }
  bin := filepath.Join(sr.dir, "bootstrap")
  if fi, err := os.Stat(bin); err != nil || fi.IsDir() {
    in.IsError = true
    in.ErrorType = "HandlerError"
    in.Response = errorPayload("HandlerError", "bootstrap not found in the code package")
    return nil
  }

  cmd := exec.CommandContext(ctx, bin)
  cmd.Dir = sr.dir
  cmd.Env = sr.envFor(in)
  cmd.Stdin = bytes.NewReader(in.Event)
  var out, errBuf bytes.Buffer
  cmd.Stdout = &out
  cmd.Stderr = io.MultiWriter(&errBuf, &in.Logs)
  cmd.WaitDelay = 2 * time.Second
  if err := cmd.Run(); err != nil {
    msg := strings.TrimSpace(string(tailBytes(errBuf.Bytes(), logTailBytes)))
    if msg == "" {
      msg = err.Error()
    }
    in.Logs.WriteString("bootstrap failure: " + msg)
    in.IsError = true
    in.ErrorType = "HandlerError"
    in.Response = errorPayload("HandlerError", "bootstrap exited abnormally: "+msg)
    return nil
  }
  in.Response = out.Bytes()
  return nil
}

func (sr *scriptRunner) extract() {
  zr, err := zip.NewReader(bytes.NewReader(sr.code), int64(len(sr.code)))
  if err != nil {
    sr.ready = fmt.Errorf("custom runtime code package: %v", err)
    return
  }
  dir, err := os.MkdirTemp("", "floci-go-")
  if err != nil {
    sr.ready = err
    return
  }
  sr.dir = dir
  total := int64(0)
  for _, zf := range zr.File {
    clean := filepath.Clean(zf.Name)
    if filepath.IsAbs(clean) || strings.Contains(clean, "..") {
      sr.ready = fmt.Errorf("unsafe zip entry %q", zf.Name)
      return
    }
    target := filepath.Join(dir, clean)
    if !strings.HasPrefix(target, dir+string(filepath.Separator)) {
      sr.ready = fmt.Errorf("unsafe zip entry %q", zf.Name)
      return
    }
    if zf.FileInfo().IsDir() {
      if err := os.MkdirAll(target, 0o755); err != nil {
        sr.ready = err
        return
      }
      continue
    }
    if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
      sr.ready = err
      return
    }
    rc, err := zf.Open()
    if err != nil {
      sr.ready = err
      return
    }
    data, readErr := io.ReadAll(rc)
    _ = rc.Close()
    if readErr != nil {
      sr.ready = readErr
      return
    }
    total += int64(len(data))
    if total > maxCodeBytes {
      sr.ready = fmt.Errorf("uncompressed code exceeds %d bytes", maxCodeBytes)
      return
    }
    mode := os.FileMode(0o644)
    if zf.Mode()&0o111 != 0 {
      mode = 0o755
    }
    if err := os.WriteFile(target, data, mode); err != nil {
      sr.ready = err
      return
    }
  }
}

func (sr *scriptRunner) envFor(in *Invocation) []string {
  env := append(os.Environ(),
    "FLOCI=1",
    "FLOCI_EXECUTION_MODEL=in-process",
    "AWS_REGION="+sr.region,
    "AWS_DEFAULT_REGION="+sr.region,
    "AWS_LAMBDA_FUNCTION_NAME="+sr.cfg.FunctionName,
    "AWS_LAMBDA_FUNCTION_VERSION="+sr.cfg.Version,
    "AWS_LAMBDA_FUNCTION_MEMORY_SIZE="+strconv.Itoa(sr.cfg.MemorySize),
    "AWS_LAMBDA_FUNCTION_TIMEOUT="+strconv.Itoa(sr.cfg.Timeout),
    "AWS_REQUEST_ID="+in.RequestID,
    "_HANDLER="+sr.cfg.Handler,
  )
  if sr.cfg.Environment != nil {
    for k, v := range sr.cfg.Environment.Variables {
      env = append(env, k+"="+v)
    }
  }
  for k, v := range in.Env {
    env = append(env, k+"="+v)
  }
  return env
}

// Close removes the extracted code directory.
func (sr *scriptRunner) Close() error {
  if sr.dir != "" {
    _ = os.RemoveAll(sr.dir)
    sr.dir = ""
  }
  return nil
}

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

func main() {
  port := strings.TrimSpace(os.Getenv("FLOCI_PORT"))
  if port == "" {
    port = strings.TrimSpace(os.Getenv("PORT"))
  }
  if port == "" {
    port = "5010"
  }
  svc := NewService()
  defer func() {
    _ = svc.Close()
  }()
  addr := ":" + port
  log.Printf("floci-go %s (execution model: in-process runner) listening on %s", version, addr)
  if err := http.ListenAndServe(addr, svc); err != nil {
    log.Fatal(err)
  }
}
