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
	"bytes"
	"context"
	"os/exec"
	"path/filepath"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"archive/zip"
)

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
