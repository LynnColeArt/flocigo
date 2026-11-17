package main

import (
  "archive/zip"
  "bytes"
  "context"
  "crypto/sha256"
  "encoding/base64"
  "encoding/json"
  "errors"
  "fmt"
  "io"
  "net/http"
  "net/http/httptest"
  "net/url"
  "os"
  "runtime"
  "sort"
  "strings"
  "sync/atomic"
  "testing"
  "time"
)

type apiResult struct {
  status int
  header http.Header
  body   []byte
}

func do(t *testing.T, h http.Handler, method, target string, headers map[string]string, body []byte) apiResult {
  t.Helper()
  var rdr io.Reader
  if body != nil {
    rdr = bytes.NewReader(body)
  }
  req := httptest.NewRequest(method, target, rdr)
  for k, v := range headers {
    req.Header.Set(k, v)
  }
  rec := httptest.NewRecorder()
  h.ServeHTTP(rec, req)
  return apiResult{status: rec.Code, header: rec.Header(), body: rec.Body.Bytes()}
}

func errorTypeOf(t *testing.T, body []byte) string {
  t.Helper()
  var m map[string]any
  if err := json.Unmarshal(body, &m); err != nil {
    t.Fatalf("error body %q is not JSON: %v", body, err)
  }
  et, _ := m["__type"].(string)
  return et
}

func buildZip(entries map[string]string, mode os.FileMode) []byte {
  names := make([]string, 0, len(entries))
  for n := range entries {
    names = append(names, n)
  }
  sort.Strings(names)
  var buf bytes.Buffer
  zw := zip.NewWriter(&buf)
  for _, n := range names {
    hdr := &zip.FileHeader{Name: n, Method: zip.Deflate}
    hdr.SetMode(mode)
    f, err := zw.CreateHeader(hdr)
    if err != nil {
      panic(err)
    }
    if _, err := f.Write([]byte(entries[n])); err != nil {
      panic(err)
    }
  }
  if err := zw.Close(); err != nil {
    panic(err)
  }
  return buf.Bytes()
}

func createFn(t *testing.T, svc *Service, name, rt string, code []byte) *FunctionConfiguration {
  t.Helper()
  body, err := json.Marshal(map[string]any{
    "FunctionName": name,
    "Runtime":      rt,
    "Code":         map[string]any{"ZipFile": base64.StdEncoding.EncodeToString(code)},
    "Timeout":      5,
    "MemorySize":   256,
    "Tags":         map[string]string{"team": "floci"},
  })
  if err != nil {
    t.Fatal(err)
  }
  res := do(t, svc, http.MethodPost, "/2015-03-31/functions", nil, body)
  if res.status != http.StatusCreated {
    t.Fatalf("create %s: status %d, body %s", name, res.status, res.body)
  }
  var cfg FunctionConfiguration
  if err := json.Unmarshal(res.body, &cfg); err != nil {
    t.Fatalf("create %s: bad response %q: %v", name, res.body, err)
  }
  return &cfg
}

type countingRunner struct {
  n     *int64
  delay time.Duration
}

func (c countingRunner) Invoke(ctx context.Context, in *Invocation) error {
  if c.delay > 0 {
    select {
    case <-time.After(c.delay):
    case <-ctx.Done():
      return ctx.Err()
    }
  }
  atomic.AddInt64(c.n, 1)
  in.Response = []byte(fmt.Sprintf(`{"count":%d}`, atomic.LoadInt64(c.n)))
  in.Logs.WriteString("invocation-ok")
  return nil
}

type failingRunner struct{}

func (failingRunner) Invoke(_ context.Context, in *Invocation) error {
  in.IsError = true
  in.ErrorType = "UserFailure"
  in.Response = errorPayload("UserFailure", "boom")
  return nil
}

type brokenRunner struct{}

func (brokenRunner) Invoke(_ context.Context, _ *Invocation) error {
  return errors.New("kaboom")
}

func TestCreateAndGetFunction(t *testing.T) {
  svc := NewService()
  defer func() { _ = svc.Close() }()

  code := buildZip(map[string]string{"handler.py": "print('stub')"}, 0o644)
  cfg := createFn(t, svc, "alpha", "floci/echo", code)

  if cfg.FunctionName != "alpha" {
    t.Fatalf("FunctionName = %q", cfg.FunctionName)
  }
  if !strings.HasSuffix(cfg.FunctionArn, ":000000000000:function:alpha") {
    t.Fatalf("FunctionArn = %q", cfg.FunctionArn)
  }
  if !strings.HasPrefix(cfg.Role, "arn:aws:iam::") {
    t.Fatalf("Role default missing: %q", cfg.Role)
  }
  if cfg.State != "Active" || cfg.Version != "$LATEST" || cfg.PackageType != "Zip" {
    t.Fatalf("unexpected configuration: %+v", cfg)
  }
  if cfg.CodeSize != int64(len(code)) || cfg.Timeout != 5 || cfg.MemorySize != 256 {
    t.Fatalf("unexpected code/limits: %+v", cfg)
  }
  sum := sha256.Sum256(code)
  wantSha := base64.StdEncoding.EncodeToString(sum[:])
  if cfg.CodeSha256 != wantSha {
    t.Fatalf("CodeSha256 = %q, want %q", cfg.CodeSha256, wantSha)
  }
  if cfg.LastModified == "" {
    t.Fatal("LastModified missing")
  }

  res := do(t, svc, http.MethodGet, "/2015-03-31/functions/ALPHA", nil, nil)
  if res.status != http.StatusOK {
    t.Fatalf("get: status %d, body %s", res.status, res.body)
  }
  var got struct {
    Configuration FunctionConfiguration `json:"Configuration"`
    Code          struct {
      Location       string `json:"Location"`
      RepositoryType string `json:"RepositoryType"`
    } `json:"Code"`
    Tags map[string]string `json:"Tags"`
  }
  if err := json.Unmarshal(res.body, &got); err != nil {
    t.Fatalf("get: bad body %q: %v", res.body, err)
  }
  if got.Configuration.FunctionName != "alpha" || got.Code.RepositoryType != "floci" {
    t.Fatalf("unexpected get response: %s", res.body)
  }
  if !strings.HasSuffix(got.Code.Location, "/2015-03-31/functions/alpha/code") {
    t.Fatalf("Code.Location = %q", got.Code.Location)
  }
  if got.Tags["team"] != "floci" {
    t.Fatalf("Tags = %v", got.Tags)
  }

  u, err := url.Parse(got.Code.Location)
  if err != nil {
    t.Fatalf("bad Location %q: %v", got.Code.Location, err)
  }
  codeRes := do(t, svc, http.MethodGet, u.Path, nil, nil)
  if codeRes.status != http.StatusOK || !bytes.Equal(codeRes.body, code) {
    t.Fatalf("code download: status %d, zip mismatch", codeRes.status)
  }
  if ct := codeRes.header.Get("Content-Type"); ct != "application/zip" {
    t.Fatalf("code Content-Type = %q", ct)
  }

  res404 := do(t, svc, http.MethodGet, "/2015-03-31/functions/ghost", nil, nil)
  if res404.status != http.StatusNotFound || errorTypeOf(t, res404.body) != "ResourceNotFoundException" {
    t.Fatalf("expected 404 ResourceNotFoundException, got %d %s", res404.status, res404.body)
  }
  if et := res404.header.Get("x-amzn-ErrorType"); et != "ResourceNotFoundException" {
    t.Fatalf("x-amzn-ErrorType = %q", et)
  }
}

func TestInvokeRequestResponseEcho(t *testing.T) {
  svc := NewService()
  defer func() { _ = svc.Close() }()
  createFn(t, svc, "echo-fn", "floci/echo", buildZip(map[string]string{"app.go": "stub"}, 0o644))

  event := []byte(`{"hello":"floci","numbers":[1,2,3]}`)
  res := do(t, svc, http.MethodPost, "/2015-03-31/functions/echo-fn/invocations",
    map[string]string{"X-Amz-Invocation-Type": "RequestResponse"}, event)
  if res.status != http.StatusOK {
    t.Fatalf("status %d, body %s", res.status, res.body)
  }
  if !bytes.Equal(res.body, event) {
    t.Fatalf("echo mismatch: %s", res.body)
  }
  if res.header.Get("X-Amz-Executed-Version") != "$LATEST" {
    t.Fatalf("Executed-Version = %q", res.header.Get("X-Amz-Executed-Version"))
  }
  if res.header.Get("X-Amzn-RequestId") == "" {
    t.Fatal("X-Amzn-RequestId missing")
  }
  if res.header.Get("X-Amz-Function-Error") != "" {
    t.Fatalf("unexpected Function-Error header %q", res.header.Get("X-Amz-Function-Error"))
  }

  // Unknown runtimes stay invocable via the documented echo fallback.
  createFn(t, svc, "py-fn", "python3.13-not-installed", buildZip(map[string]string{"m.py": "stub"}, 0o644))
  res2 := do(t, svc, http.MethodPost, "/2015-03-31/functions/py-fn/invocations", nil, event)
  if res2.status != http.StatusOK || !bytes.Equal(res2.body, event) {
    t.Fatalf("fallback invoke: status %d body %s", res2.status, res2.body)
  }
}

func TestInvokeEventAndDryRun(t *testing.T) {
  svc := NewService()
  defer func() { _ = svc.Close() }()

  var count int64
  if err := svc.RegisterRunner("floci/test", func(*FunctionConfiguration, []byte) (Runner, error) {
    return countingRunner{n: &count, delay: 25 * time.Millisecond}, nil
  }); err != nil {
    t.Fatal(err)
  }
  createFn(t, svc, "event-fn", "floci/test", buildZip(map[string]string{"a": "b"}, 0o644))

  res := do(t, svc, http.MethodPost, "/2015-03-31/functions/event-fn/invocations",
    map[string]string{"X-Amz-Invocation-Type": "Event"}, []byte(`{"async":true}`))
  if res.status != http.StatusAccepted || len(res.body) != 0 {
    t.Fatalf("event invoke: status %d body %q", res.status, res.body)
  }
  ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
  defer cancel()
  if !svc.WaitForIdle(ctx) {
    t.Fatal("scheduled Event invocation never finished")
  }
  if n := atomic.LoadInt64(&count); n != 1 {
    t.Fatalf("runner executed %d times, want 1", n)
  }
  svc.mu.Lock()
  fn := svc.functions["event-fn"]
  svc.mu.Unlock()
  fn.mu.Lock()
  records := len(fn.history)
  typ := fn.history[0].Type
  errType := fn.history[0].Error
  fn.mu.Unlock()
  if records != 1 || typ != "Event" || errType != "" {
    t.Fatalf("history = %d records (%s, %q)", records, typ, errType)
  }

  dry := do(t, svc, http.MethodPost, "/2015-03-31/functions/event-fn/invocations",
    map[string]string{"X-Amz-Invocation-Type": "DryRun"}, []byte(`{}`))
  if dry.status != http.StatusNoContent {
    t.Fatalf("dry run: status %d", dry.status)
  }
  if n := atomic.LoadInt64(&count); n != 1 {
    t.Fatalf("DryRun executed the runner %d extra times", n-1)
  }

  bad := do(t, svc, http.MethodPost, "/2015-03-31/functions/event-fn/invocations",
    map[string]string{"X-Amz-Invocation-Type": "Bogus"}, []byte(`{}`))
  if bad.status != http.StatusBadRequest || errorTypeOf(t, bad.body) != "InvalidParameterValueException" {
    t.Fatalf("bad invocation type: status %d body %s", bad.status, bad.body)
  }
}

func TestInvokeFunctionErrorSurface(t *testing.T) {
  svc := NewService()
  defer func() { _ = svc.Close() }()
  if err := svc.RegisterRunner("floci/test-fail", func(*FunctionConfiguration, []byte) (Runner, error) {
    return failingRunner{}, nil
  }); err != nil {
    t.Fatal(err)
  }
  if err := svc.RegisterRunner("floci/test-broken", func(*FunctionConfiguration, []byte) (Runner, error) {
    return brokenRunner{}, nil
  }); err != nil {
    t.Fatal(err)
  }
  createFn(t, svc, "bad-fn", "floci/test-fail", buildZip(map[string]string{"a": "b"}, 0o644))
  createFn(t, svc, "broken-fn", "floci/test-broken", buildZip(map[string]string{"a": "b"}, 0o644))

  res := do(t, svc, http.MethodPost, "/2015-03-31/functions/bad-fn/invocations", nil, []byte(`{}`))
  if res.status != http.StatusOK {
    t.Fatalf("function error must still answer 200, got %d", res.status)
  }
  if res.header.Get("X-Amz-Function-Error") != "UserFailure" {
    t.Fatalf("Function-Error = %q", res.header.Get("X-Amz-Function-Error"))
  }
  if !strings.Contains(string(res.body), "boom") {
    t.Fatalf("error payload %q", res.body)
  }

  resBroken := do(t, svc, http.MethodPost, "/2015-03-31/functions/broken-fn/invocations", nil, []byte(`{}`))
  if resBroken.header.Get("X-Amz-Function-Error") != "InternalError" {
    t.Fatalf("broken runner: Function-Error = %q body %s", resBroken.header.Get("X-Amz-Function-Error"), resBroken.body)
  }
}

func TestInvokeLogTail(t *testing.T) {
  svc := NewService()
  defer func() { _ = svc.Close() }()
  var count int64
  if err := svc.RegisterRunner("floci/test", func(*FunctionConfiguration, []byte) (Runner, error) {
    return countingRunner{n: &count}, nil
  }); err != nil {
    t.Fatal(err)
  }
  createFn(t, svc, "logs-fn", "floci/test", buildZip(map[string]string{"a": "b"}, 0o644))
  res := do(t, svc, http.MethodPost, "/2015-03-31/functions/logs-fn/invocations",
    map[string]string{"X-Amz-Log-Type": "Tail"}, []byte(`{}`))
  if res.status != http.StatusOK {
    t.Fatalf("status %d", res.status)
  }
  decoded, err := base64.StdEncoding.DecodeString(res.header.Get("X-Amz-Log-Result"))
  if err != nil || !strings.Contains(string(decoded), "invocation-ok") {
    t.Fatalf("LogResult = %q (decode err %v)", res.header.Get("X-Amz-Log-Result"), err)
  }
}

func TestListFunctionsPagination(t *testing.T) {
  svc := NewService()
  defer func() { _ = svc.Close() }()
  for _, n := range []string{"c-fn", "a-fn", "b-fn"} {
    createFn(t, svc, n, "floci/echo", buildZip(map[string]string{"a": "b"}, 0o644))
  }

  res := do(t, svc, http.MethodGet, "/2015-03-31/functions", nil, nil)
  if res.status != http.StatusOK {
    t.Fatalf("status %d", res.status)
  }
  var full ListFunctionsResult
  if err := json.Unmarshal(res.body, &full); err != nil {
    t.Fatalf("bad body: %v", err)
  }
  if len(full.Functions) != 3 || full.NextMarker != "" {
    t.Fatalf("unexpected listing: %s", res.body)
  }
  for i, want := range []string{"a-fn", "b-fn", "c-fn"} {
    if full.Functions[i].FunctionName != want {
      t.Fatalf("listing order: got %q want %q", full.Functions[i].FunctionName, want)
    }
  }

  resPage := do(t, svc, http.MethodGet, "/2015-03-31/functions?MaxItems=2", nil, nil)
  var page ListFunctionsResult
  if err := json.Unmarshal(resPage.body, &page); err != nil {
    t.Fatalf("bad page body: %v", err)
  }
  if page.NextMarker != "b-fn" || len(page.Functions) != 2 {
    t.Fatalf("unexpected page: %s", resPage.body)
  }

  resTail := do(t, svc, http.MethodGet, "/2015-03-31/functions?Marker=b-fn", nil, nil)
  var tail ListFunctionsResult
  if err := json.Unmarshal(resTail.body, &tail); err != nil {
    t.Fatalf("bad tail body: %v", err)
  }
  if len(tail.Functions) != 1 || tail.Functions[0].FunctionName != "c-fn" || tail.NextMarker != "" {
    t.Fatalf("unexpected tail: %s", resTail.body)
  }

  resBad := do(t, svc, http.MethodGet, "/2015-03-31/functions?Marker=zzz", nil, nil)
  if resBad.status != http.StatusBadRequest || errorTypeOf(t, resBad.body) != "InvalidParameterValueException" {
    t.Fatalf("bad marker: status %d body %s", resBad.status, resBad.body)
  }
  resBadMax := do(t, svc, http.MethodGet, "/2015-03-31/functions?MaxItems=nope", nil, nil)
  if resBadMax.status != http.StatusBadRequest {
    t.Fatalf("bad MaxItems: status %d", resBadMax.status)
  }
}

func TestDeleteFunction(t *testing.T) {
  svc := NewService()
  defer func() { _ = svc.Close() }()
  createFn(t, svc, "doomed", "floci/echo", buildZip(map[string]string{"a": "b"}, 0o644))

  del := do(t, svc, http.MethodDelete, "/2015-03-31/functions/doomed", nil, nil)
  if del.status != http.StatusNoContent || len(del.body) != 0 {
    t.Fatalf("delete: status %d body %q", del.status, del.body)
  }
  if get := do(t, svc, http.MethodGet, "/2015-03-31/functions/doomed", nil, nil); get.status != http.StatusNotFound {
    t.Fatalf("after delete, get = %d", get.status)
  }
  if del2 := do(t, svc, http.MethodDelete, "/2015-03-31/functions/doomed", nil, nil); del2.status != http.StatusNotFound {
    t.Fatalf("second delete = %d", del2.status)
  }
  if inv := do(t, svc, http.MethodPost, "/2015-03-31/functions/doomed/invocations", nil, []byte(`{}`)); inv.status != http.StatusNotFound {
    t.Fatalf("invoke deleted = %d body %s", inv.status, inv.body)
  }
}

func TestCreateFunctionValidation(t *testing.T) {
  svc := NewService()
  defer func() { _ = svc.Close() }()
  good := base64.StdEncoding.EncodeToString(buildZip(map[string]string{"a": "b"}, 0o644))

  cases := []struct {
    name   string
    body   string
    status int
    errTyp string
  }{
    {"missing name", `{"Runtime":"floci/echo","Code":{"ZipFile":"` + good + `"}}`, 400, "InvalidParameterValueException"},
    {"invalid name chars", `{"FunctionName":"bad name","Runtime":"floci/echo","Code":{"ZipFile":"` + good + `"}}`, 400, "InvalidParameterValueException"},
    {"name too long", `{"FunctionName":"` + strings.Repeat("a", 65) + `","Runtime":"floci/echo","Code":{"ZipFile":"` + good + `"}}`, 400, "InvalidParameterValueException"},
    {"missing runtime", `{"FunctionName":"rtless","Code":{"ZipFile":"` + good + `"}}`, 400, "InvalidParameterValueException"},
    {"missing code", `{"FunctionName":"nocode","Runtime":"floci/echo"}`, 400, "InvalidParameterValueException"},
    {"s3 code unsupported", `{"FunctionName":"s3fn","Runtime":"floci/echo","Code":{"S3Bucket":"b","S3Key":"k"}}`, 400, "InvalidParameterValueException"},
    {"zip not base64", `{"FunctionName":"bada","Runtime":"floci/echo","Code":{"ZipFile":"!!!"}}`, 400, "InvalidParameterValueException"},
    {"zip not an archive", `{"FunctionName":"badb","Runtime":"floci/echo","Code":{"ZipFile":"` + base64.StdEncoding.EncodeToString([]byte("not-a-zip")) + `"}}`, 400, "InvalidParameterValueException"},
    {"timeout too high", `{"FunctionName":"slow","Runtime":"floci/echo","Code":{"ZipFile":"` + good + `"},"Timeout":1000}`, 400, "InvalidParameterValueException"},
    {"memory too low", `{"FunctionName":"tiny","Runtime":"floci/echo","Code":{"ZipFile":"` + good + `"},"MemorySize":100}`, 400, "InvalidParameterValueException"},
  }
  for _, tc := range cases {
    res := do(t, svc, http.MethodPost, "/2015-03-31/functions", nil, []byte(tc.body))
    if res.status != tc.status || errorTypeOf(t, res.body) != tc.errTyp {
      t.Fatalf("%s: status %d, body %s (want %d %s)", tc.name, res.status, res.body, tc.status, tc.errTyp)
    }
  }

  createFn(t, svc, "dup-fn", "floci/echo", buildZip(map[string]string{"a": "b"}, 0o644))
  dup := do(t, svc, http.MethodPost, "/2015-03-31/functions", nil, []byte(
    `{"FunctionName":"DUP-FN","Runtime":"floci/echo","Code":{"ZipFile":"` + good + `"}}`))
  if dup.status != http.StatusConflict || errorTypeOf(t, dup.body) != "ConflictException" {
    t.Fatalf("duplicate: status %d body %s", dup.status, dup.body)
  }

  res := do(t, svc, http.MethodPost, "/2015-03-31/functions", nil, []byte(
    `{"FunctionName":"defaults","Runtime":"floci/echo","Code":{"ZipFile":"` + good + `"}}`))
  if res.status != http.StatusCreated {
    t.Fatalf("defaults: status %d body %s", res.status, res.body)
  }
  var cfg FunctionConfiguration
  if err := json.Unmarshal(res.body, &cfg); err != nil {
    t.Fatal(err)
  }
  if cfg.Timeout != 3 || cfg.MemorySize != 128 || cfg.Handler != "floci/handler" || !strings.HasPrefix(cfg.Role, "arn:aws:iam::") {
    t.Fatalf("unexpected defaults: %+v", cfg)
  }
}

func TestScriptRunnerCustomRuntime(t *testing.T) {
  if runtime.GOOS == "windows" {
    t.Skip("custom runtime scripts need a POSIX host")
  }
  if _, err := os.Stat("/bin/sh"); err != nil {
    t.Skip("no /bin/sh available")
  }
  svc := NewService()
  defer func() { _ = svc.Close() }()

  okScript := `#!/bin/sh
cat > /dev/null
printf '{"ok":true,"fn":"%s"}' "$AWS_LAMBDA_FUNCTION_NAME"
`
  createFn(t, svc, "booty", "provided.al2023", buildZip(map[string]string{"bootstrap": okScript}, 0o755))

  res := do(t, svc, http.MethodPost, "/2015-03-31/functions/booty/invocations", nil, []byte(`{"ping":1}`))
  if res.status != http.StatusOK || res.header.Get("X-Amz-Function-Error") != "" {
    t.Fatalf("script invoke: status %d body %s", res.status, res.body)
  }
  if string(res.body) != `{"ok":true,"fn":"booty"}` {
    t.Fatalf("script output = %q", res.body)
  }

  failScript := `#!/bin/sh
echo "script failure" 1>&2
exit 3
`
  createFn(t, svc, "booty-fail", "provided.al2023", buildZip(map[string]string{"bootstrap": failScript}, 0o755))
  resFail := do(t, svc, http.MethodPost, "/2015-03-31/functions/booty-fail/invocations", nil, []byte(`{}`))
  if resFail.status != http.StatusOK || resFail.header.Get("X-Amz-Function-Error") != "HandlerError" {
    t.Fatalf("failing bootstrap: status %d header %q body %s", resFail.status, resFail.header.Get("X-Amz-Function-Error"), resFail.body)
  }
  var ep map[string]any
  if err := json.Unmarshal(resFail.body, &ep); err != nil || !strings.Contains(fmt.Sprintf("%v", ep["errorMessage"]), "script failure") {
    t.Fatalf("failing bootstrap payload = %s (err %v)", resFail.body, err)
  }

  createFn(t, svc, "booty-missing", "provided.al2023", buildZip(map[string]string{"main.py": "stub"}, 0o644))
  resMissing := do(t, svc, http.MethodPost, "/2015-03-31/functions/booty-missing/invocations", nil, []byte(`{}`))
  if resMissing.status != http.StatusOK || resMissing.header.Get("X-Amz-Function-Error") != "HandlerError" {
    t.Fatalf("missing bootstrap: status %d body %s", resMissing.status, resMissing.body)
  }
  if !strings.Contains(string(resMissing.body), "bootstrap") {
    t.Fatalf("missing bootstrap payload = %q", resMissing.body)
  }
}

func TestRoutingAndServiceInfo(t *testing.T) {
  svc := NewService()
  defer func() { _ = svc.Close() }()

  info := do(t, svc, http.MethodGet, "/", nil, nil)
  if info.status != http.StatusOK || !strings.Contains(string(info.body), "in-process") {
    t.Fatalf("service info: status %d body %s", info.status, info.body)
  }

  unknown := do(t, svc, http.MethodGet, "/nope", nil, nil)
  if unknown.status != http.StatusNotFound || errorTypeOf(t, unknown.body) != "ResourceNotFoundException" {
    t.Fatalf("unknown route: status %d body %s", unknown.status, unknown.body)
  }

  badMethod := do(t, svc, http.MethodPut, "/2015-03-31/functions", nil, nil)
  if badMethod.status != http.StatusMethodNotAllowed {
    t.Fatalf("PUT /functions: status %d", badMethod.status)
  }
}
