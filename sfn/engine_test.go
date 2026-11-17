package sfn

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func createAndStart(t *testing.T, svc *Service, name, def, input string) string {
	t.Helper()
	arn, err := svc.CreateStateMachine(name, def)
	if err != nil {
		t.Fatalf("CreateStateMachine(%s): %v", name, err)
	}
	execArn, err := svc.StartExecution(arn, "", input)
	if err != nil {
		t.Fatalf("StartExecution: %v", err)
	}
	return execArn
}

func waitForStatus(t *testing.T, svc *Service, arn string, want string) ExecutionDescription {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		d, err := svc.DescribeExecution(arn)
		if err != nil {
			t.Fatalf("DescribeExecution: %v", err)
		}
		if d.Status == want {
			return d
		}
		if d.Status != StatusRunning {
			t.Fatalf("execution finished with status %q, want %q", d.Status, want)
		}
		time.Sleep(2 * time.Millisecond)
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for execution status %q", want)
		}
	}
}

func mustHistory(t *testing.T, svc *Service, arn string) []HistoryEvent {
	t.Helper()
	h, err := svc.GetExecutionHistory(arn)
	if err != nil {
		t.Fatalf("GetExecutionHistory: %v", err)
	}
	return h
}

func eventTypes(hist []HistoryEvent) []string {
	out := make([]string, 0, len(hist))
	for _, ev := range hist {
		out = append(out, ev.Type)
	}
	return out
}

func expectHistSequence(t *testing.T, hist []HistoryEvent, want ...string) {
	t.Helper()
	got := eventTypes(hist)
	if len(got) != len(want) {
		t.Fatalf("history event types = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("history event types = %v, want %v", got, want)
		}
	}
	for i, ev := range hist {
		if ev.Id != int64(i+1) {
			t.Fatalf("event %d has id %d", i, ev.Id)
		}
		if i > 0 && ev.PreviousEventId != hist[i-1].Id {
			t.Fatalf("event %d previousEventId = %d, want %d", i, ev.PreviousEventId, hist[i-1].Id)
		}
	}
}

func TestPassStatePayloadTemplateWithIntrinsics(t *testing.T) {
	svc := NewService()
	def := `{
		"StartAt": "Compute",
		"States": {
			"Compute": {
				"Type": "Pass",
				"Parameters": {
					"greeting.$": "States.Format('Hello, {}!', $.name)",
					"numbers.$": "States.Array($.a, $.b)",
					"total.$": "States.MathAdd($.a, $.b)",
					"nested": {"flagText.$": "States.JsonToString($.flag)"}
				},
				"ResultPath": "$.computed",
				"End": true
			}
		}
	}`
	execArn := createAndStart(t, svc, "intr-sm", def, `{"name":"floci","a":2,"b":3,"flag":true}`)
	d := waitForStatus(t, svc, execArn, StatusSucceeded)
	want := `{"a":2,"b":3,"computed":{"greeting":"Hello, floci!","nested":{"flagText":"true"},"numbers":[2,3],"total":5},"flag":true,"name":"floci"}`
	if d.Output == nil || *d.Output != want {
		t.Fatalf("output = %v, want %s", d.Output, want)
	}
	hist := mustHistory(t, svc, execArn)
	expectHistSequence(t, hist,
		EventExecutionStarted,
		"PassStateEntered",
		"PassStateExited",
		EventExecutionSucceeded,
	)
	if hist[1].StateEnteredEventDetails == nil || hist[1].StateEnteredEventDetails.Name != "Compute" {
		t.Fatalf("PassStateEntered details missing: %+v", hist[1])
	}
	if hist[2].StateExitedEventDetails == nil || hist[2].StateExitedEventDetails.Output != *d.Output {
		t.Fatalf("PassStateExited details mismatch: %+v", hist[2])
	}
}

func TestWaitStateUsesSeconds(t *testing.T) {
	svc := NewService()
	def := `{"StartAt":"Pause","States":{"Pause":{"Type":"Wait","Seconds":0.05,"End":true}}}`
	t0 := time.Now()
	execArn := createAndStart(t, svc, "wait-sm", def, `{"n":1}`)
	waitForStatus(t, svc, execArn, StatusSucceeded)
	if elapsed := time.Since(t0); elapsed < 40*time.Millisecond {
		t.Fatalf("Wait did not wait long enough: %v", elapsed)
	}
	hist := mustHistory(t, svc, execArn)
	expectHistSequence(t, hist,
		EventExecutionStarted,
		"WaitStateEntered",
		"WaitStateExited",
		EventExecutionSucceeded,
	)
	if hist[1].StateEnteredEventDetails == nil || hist[1].StateEnteredEventDetails.Input != `{"n":1}` {
		t.Fatalf("WaitStateEntered input mismatch: %+v", hist[1])
	}
}

func TestChoiceRouting(t *testing.T) {
	svc := NewService()
	def := `{
		"StartAt": "Branch",
		"States": {
			"Branch": {
				"Type": "Choice",
				"Choices": [
					{"Variable": "$.v", "NumericLessThan": 10, "Next": "Low"},
					{"Variable": "$.v", "NumericEquals": 10, "Next": "Mid"}
				],
				"Default": "Other"
			},
			"Low": {"Type": "Pass", "End": true},
			"Mid": {"Type": "Pass", "End": true},
			"Other": {"Type": "Pass", "End": true}
		}
	}`
	execArn := createAndStart(t, svc, "choice-sm", def, `{"v":3}`)
	d := waitForStatus(t, svc, execArn, StatusSucceeded)
	if d.Output == nil || *d.Output != `{"v":3}` {
		t.Fatalf("output = %v, want passthrough", d.Output)
	}
	expectHistSequence(t, mustHistory(t, svc, execArn),
		EventExecutionStarted,
		"ChoiceStateEntered",
		"ChoiceStateExited",
		"PassStateEntered",
		"PassStateExited",
		EventExecutionSucceeded,
	)
}

func TestChoiceNoMatchFailsExecution(t *testing.T) {
	svc := NewService()
	def := `{
		"StartAt": "Branch",
		"States": {
			"Branch": {
				"Type": "Choice",
				"Choices": [{"Variable": "$.v", "NumericEquals": 1, "Next": "One"}]
			},
			"One": {"Type": "Pass", "End": true}
		}
	}`
	execArn := createAndStart(t, svc, "choice-fail-sm", def, `{"v":2}`)
	d := waitForStatus(t, svc, execArn, StatusFailed)
	if d.Error == nil || *d.Error != ErrNameNoChoiceMatched {
		t.Fatalf("error = %v, want %s", d.Error, ErrNameNoChoiceMatched)
	}
	expectHistSequence(t, mustHistory(t, svc, execArn),
		EventExecutionStarted,
		"ChoiceStateEntered",
		EventExecutionFailed,
	)
}

func TestTaskHandlerRetryThenSucceed(t *testing.T) {
	svc := NewService()
	calls := 0
	svc.Handler = func(resource string, payload any) (any, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("lambda exploded")
		}
		return map[string]any{"ok": true}, nil
	}
	def := `{
		"StartAt": "Call",
		"States": {
			"Call": {
				"Type": "Task",
				"Resource": "arn:aws:lambda:us-east-1:000000000000:function:flaky",
				"Parameters": {"input.$": "$.n"},
				"Retry": [{"ErrorEquals": ["States.TaskFailed"], "IntervalSeconds": 0.01, "MaxAttempts": 2, "BackoffRate": 1}],
				"ResultPath": "$.res",
				"End": true
			}
		}
	}`
	execArn := createAndStart(t, svc, "retry-sm", def, `{"n":41}`)
	d := waitForStatus(t, svc, execArn, StatusSucceeded)
	if calls != 2 {
		t.Fatalf("handler calls = %d, want 2", calls)
	}
	want := `{"n":41,"res":{"ok":true}}`
	if d.Output == nil || *d.Output != want {
		t.Fatalf("output = %v, want %s", d.Output, want)
	}
	hist := mustHistory(t, svc, execArn)
	expectHistSequence(t, hist,
		EventExecutionStarted,
		EventTaskStateEntered,
		EventTaskScheduled,
		EventTaskFailed,
		EventTaskScheduled,
		EventTaskSucceeded,
		EventTaskStateExited,
		EventExecutionSucceeded,
	)
	tf := hist[3].TaskFailedEventDetails
	if tf == nil || tf.Error != ErrNameTaskFailed || !strings.Contains(tf.Cause, "lambda exploded") {
		t.Fatalf("TaskFailed details mismatch: %+v", hist[3])
	}
}

func TestTaskHandlerCatchFailure(t *testing.T) {
	svc := NewService()
	svc.Handler = func(resource string, payload any) (any, error) {
		return nil, errors.New("boom")
	}
	def := `{
		"StartAt": "Call",
		"States": {
			"Call": {
				"Type": "Task",
				"Resource": "arn:aws:lambda:us-east-1:000000000000:function:broken",
				"Catch": [{"ErrorEquals": ["States.TaskFailed"], "ResultPath": "$.error", "Next": "Recover"}],
				"End": true
			},
			"Recover": {"Type": "Pass", "End": true}
		}
	}`
	execArn := createAndStart(t, svc, "catch-sm", def, `{}`)
	d := waitForStatus(t, svc, execArn, StatusSucceeded)
	want := `{"error":{"Error":"States.TaskFailed","Cause":"boom"}}`
	if d.Output == nil || *d.Output != want {
		t.Fatalf("output = %v, want %s", d.Output, want)
	}
	// No TaskStateExited after a catch: the state failed.
	expectHistSequence(t, mustHistory(t, svc, execArn),
		EventExecutionStarted,
		EventTaskStateEntered,
		EventTaskScheduled,
		EventTaskFailed,
		"PassStateEntered",
		"PassStateExited",
		EventExecutionSucceeded,
	)
}

func TestTaskResultSelector(t *testing.T) {
	svc := NewService()
	var seenPayload string
	svc.Handler = func(resource string, payload any) (any, error) {
		seenPayload = jsonText(payload)
		return map[string]any{"flag": true, "other": 1}, nil
	}
	def := `{
		"StartAt": "Call",
		"States": {
			"Call": {
				"Type": "Task",
				"Resource": "arn:aws:lambda:us-east-1:000000000000:function:sel",
				"ResultSelector": {"ok.$": "$.flag"},
				"ResultPath": "$.res",
				"End": true
			}
		}
	}`
	execArn := createAndStart(t, svc, "selector-sm", def, `{"x":1}`)
	d := waitForStatus(t, svc, execArn, StatusSucceeded)
	want := `{"res":{"ok":true},"x":1}`
	if d.Output == nil || *d.Output != want {
		t.Fatalf("output = %v, want %s", d.Output, want)
	}
	if seenPayload != `{"x":1}` {
		t.Fatalf("handler payload = %s, want the full input", seenPayload)
	}
}

func TestFailStateFailsExecution(t *testing.T) {
	svc := NewService()
	def := `{"StartAt":"Boom","States":{"Boom":{"Type":"Fail","Error":"Boom","Cause":"kaboom"}}}`
	execArn := createAndStart(t, svc, "fail-sm", def, `{}`)
	d := waitForStatus(t, svc, execArn, StatusFailed)
	if d.Error == nil || *d.Error != "Boom" || d.Cause == nil || *d.Cause != "kaboom" {
		t.Fatalf("describe error/cause = %v/%v, want Boom/kaboom", d.Error, d.Cause)
	}
	if d.StopDate == nil {
		t.Fatal("StopDate should be set for terminal executions")
	}
	hist := mustHistory(t, svc, execArn)
	expectHistSequence(t, hist,
		EventExecutionStarted,
		"FailStateEntered",
		EventExecutionFailed,
	)
	if hist[2].ExecutionFailedEventDetails == nil || hist[2].ExecutionFailedEventDetails.Error != "Boom" {
		t.Fatalf("ExecutionFailed details mismatch: %+v", hist[2])
	}
}

func TestContextObjectPaths(t *testing.T) {
	svc := NewService()
	def := `{
		"StartAt": "UseContext",
		"States": {
			"UseContext": {
				"Type": "Pass",
				"Parameters": {
					"execId.$": "$$.Execution.Id",
					"stateName.$": "$$.State.Name",
					"retryCount.$": "$$.State.RetryCount"
				},
				"End": true
			}
		}
	}`
	execArn := createAndStart(t, svc, "ctx-sm", def, `{}`)
	d := waitForStatus(t, svc, execArn, StatusSucceeded)
	if d.Output == nil {
		t.Fatal("missing output")
	}
	out := *d.Output
	if !strings.Contains(out, "arn:aws:states:us-east-1:000000000000:execution:ctx-sm:") {
		t.Fatalf("output missing execution id: %s", out)
	}
	if !strings.Contains(out, `"stateName":"UseContext"`) || !strings.Contains(out, `"retryCount":0`) {
		t.Fatalf("output missing context fields: %s", out)
	}
}

func TestDuplicateExecutionNameRejected(t *testing.T) {
	svc := NewService()
	smArn, err := svc.CreateStateMachine("dup-sm", `{"StartAt":"Only","States":{"Only":{"Type":"Pass","End":true}}}`)
	if err != nil {
		t.Fatalf("CreateStateMachine: %v", err)
	}
	e1, err := svc.StartExecution(smArn, "same", "{}")
	if err != nil {
		t.Fatalf("StartExecution: %v", err)
	}
	defer svc.StopExecution(e1)
	_, err = svc.StartExecution(smArn, "same", "{}")
	if err == nil || !strings.Contains(err.Error(), "ExecutionAlreadyExists") {
		t.Fatalf("expected ExecutionAlreadyExists, got %v", err)
	}
}

func TestServiceValidationErrors(t *testing.T) {
	svc := NewService()
	if _, err := svc.CreateStateMachine("bad", `{not json`); err == nil || !strings.Contains(err.Error(), "InvalidDefinition") {
		t.Fatalf("expected InvalidDefinition, got %v", err)
	}
	smArn, err := svc.CreateStateMachine("good", `{"StartAt":"Only","States":{"Only":{"Type":"Pass","End":true}}}`)
	if err != nil {
		t.Fatalf("CreateStateMachine: %v", err)
	}
	if _, err = svc.StartExecution("arn:aws:states:us-east-1:000000000000:stateMachine:nope", "", "{}"); err == nil || !strings.Contains(err.Error(), "StateMachineDoesNotExist") {
		t.Fatalf("expected StateMachineDoesNotExist, got %v", err)
	}
	if _, err = svc.StartExecution(smArn, "x", `{oops`); err == nil || !strings.Contains(err.Error(), "InvalidExecutionInput") {
		t.Fatalf("expected InvalidExecutionInput, got %v", err)
	}
	miss := "arn:aws:states:us-east-1:000000000000:execution:nope:missing"
	if _, err = svc.DescribeExecution(miss); err == nil || !strings.Contains(err.Error(), "ExecutionDoesNotExist") {
		t.Fatalf("expected ExecutionDoesNotExist, got %v", err)
	}
	if _, err = svc.GetExecutionHistory(miss); err == nil || !strings.Contains(err.Error(), "ExecutionDoesNotExist") {
		t.Fatalf("expected ExecutionDoesNotExist, got %v", err)
	}
	if err = svc.StopExecution(miss); err == nil || !strings.Contains(err.Error(), "ExecutionDoesNotExist") {
		t.Fatalf("expected ExecutionDoesNotExist, got %v", err)
	}
}
