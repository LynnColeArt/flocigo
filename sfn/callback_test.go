package sfn

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func countEvents(hist []HistoryEvent, typ string) int {
	n := 0
	for _, ev := range hist {
		if ev.Type == typ {
			n++
		}
	}
	return n
}

func waitForHistory(t *testing.T, svc *Service, arn string, cond func([]HistoryEvent) bool) []HistoryEvent {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		h := mustHistory(t, svc, arn)
		if cond(h) {
			return h
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for history condition; got events %v", eventTypes(h))
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func lastTaskScheduledParams(t *testing.T, hist []HistoryEvent) map[string]any {
	t.Helper()
	for i := len(hist) - 1; i >= 0; i-- {
		ev := hist[i]
		if ev.Type == EventTaskScheduled {
			var p map[string]any
			if err := json.Unmarshal([]byte(ev.TaskScheduledEventDetails.Parameters), &p); err != nil {
				t.Fatalf("parsing TaskScheduled parameters: %v", err)
			}
			return p
		}
	}
	t.Fatal("no TaskScheduled event in history")
	return nil
}

func lastTaskScheduledToken(t *testing.T, hist []HistoryEvent) string {
	t.Helper()
	p := lastTaskScheduledParams(t, hist)
	tok, _ := p["token"].(string)
	if tok == "" {
		t.Fatalf("no token in TaskScheduled parameters: %v", p)
	}
	return tok
}

func callbackDef(extra string) string {
	return `{
		"StartAt": "Callback",
		"States": {
			"Callback": {
				"Type": "Task",
				"Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
				"Parameters": {"token.$": "$$.Task.Token"` + extra + `},
				"ResultPath": "$.reply",
				"End": true
			}
		}
	}`
}

func TestCallbackTaskSendTaskSuccess(t *testing.T) {
	svc := NewService()
	def := callbackDef(`, "message.$": "$.msg"`)
	arn := createAndStart(t, svc, "cb-sm", def, `{"msg":"hi"}`)
	hist := waitForHistory(t, svc, arn, func(h []HistoryEvent) bool { return countEvents(h, EventTaskScheduled) >= 1 })
	token := lastTaskScheduledToken(t, hist)
	if p := lastTaskScheduledParams(t, hist); p["message"] != "hi" {
		t.Fatalf("task payload did not include the message: %v", p)
	}
	if got := svc.PendingTaskCount(); got != 1 {
		t.Fatalf("pending task count = %d, want 1", got)
	}
	// Heartbeats are accepted even without HeartbeatSeconds.
	if err := svc.SendTaskHeartbeat(token); err != nil {
		t.Fatalf("SendTaskHeartbeat: %v", err)
	}
	if err := svc.SendTaskSuccess(token, `{"answer": 42}`); err != nil {
		t.Fatalf("SendTaskSuccess: %v", err)
	}
	if got := svc.PendingTaskCount(); got != 0 {
		t.Fatalf("pending task count = %d, want 0 after delivery", got)
	}
	d := waitForStatus(t, svc, arn, StatusSucceeded)
	want := `{"msg":"hi","reply":{"answer":42}}`
	if d.Output == nil || *d.Output != want {
		t.Fatalf("output = %v, want %s", d.Output, want)
	}
	final := mustHistory(t, svc, arn)
	expectHistSequence(t, final,
		EventExecutionStarted,
		EventTaskStateEntered,
		EventTaskScheduled,
		EventTaskSucceeded,
		EventTaskStateExited,
		EventExecutionSucceeded,
	)
	if err := svc.SendTaskSuccess(token, `{}`); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken for reused token, got %v", err)
	}
}

func TestCallbackTaskSendTaskFailureWithCatch(t *testing.T) {
	svc := NewService()
	def := `{
		"StartAt": "Callback",
		"States": {
			"Callback": {
				"Type": "Task",
				"Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
				"Parameters": {"token.$": "$$.Task.Token"},
				"Catch": [{"ErrorEquals": ["CustomError"], "ResultPath": "$.err", "Next": "Recover"}],
				"End": true
			},
			"Recover": {"Type": "Pass", "End": true}
		}
	}`
	arn := createAndStart(t, svc, "cb-fail-sm", def, `{}`)
	hist := waitForHistory(t, svc, arn, func(h []HistoryEvent) bool { return countEvents(h, EventTaskScheduled) >= 1 })
	token := lastTaskScheduledToken(t, hist)
	if err := svc.SendTaskFailure(token, "CustomError", "because reasons"); err != nil {
		t.Fatalf("SendTaskFailure: %v", err)
	}
	d := waitForStatus(t, svc, arn, StatusSucceeded)
	want := `{"err":{"Error":"CustomError","Cause":"because reasons"}}`
	if d.Output == nil || *d.Output != want {
		t.Fatalf("output = %v, want %s", d.Output, want)
	}
	final := mustHistory(t, svc, arn)
	expectHistSequence(t, final,
		EventExecutionStarted,
		EventTaskStateEntered,
		EventTaskScheduled,
		EventTaskFailed,
		"PassStateEntered",
		"PassStateExited",
		EventExecutionSucceeded,
	)
	if final[3].TaskFailedEventDetails == nil || final[3].TaskFailedEventDetails.Error != "CustomError" {
		t.Fatalf("TaskFailed details mismatch: %+v", final[3])
	}
}

func TestCallbackRetryThenSuccess(t *testing.T) {
	svc := NewService()
	def := `{
		"StartAt": "RetryMe",
		"States": {
			"RetryMe": {
				"Type": "Task",
				"Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
				"Parameters": {"token.$": "$$.Task.Token"},
				"Retry": [{"ErrorEquals": ["States.TaskFailed"], "IntervalSeconds": 0.01, "MaxAttempts": 2, "BackoffRate": 1}],
				"ResultPath": "$",
				"End": true
			}
		}
	}`
	arn := createAndStart(t, svc, "cb-retry-sm", def, `{}`)
	hist := waitForHistory(t, svc, arn, func(h []HistoryEvent) bool { return countEvents(h, EventTaskScheduled) >= 1 })
	token1 := lastTaskScheduledToken(t, hist)
	if err := svc.SendTaskFailure(token1, ErrNameTaskFailed, "transient"); err != nil {
		t.Fatalf("SendTaskFailure: %v", err)
	}
	hist2 := waitForHistory(t, svc, arn, func(h []HistoryEvent) bool { return countEvents(h, EventTaskScheduled) >= 2 })
	token2 := lastTaskScheduledToken(t, hist2)
	if token2 == token1 {
		t.Fatal("retry did not issue a fresh task token")
	}
	if err := svc.SendTaskFailure(token1, ErrNameTaskFailed, "again"); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken for consumed token, got %v", err)
	}
	if err := svc.SendTaskSuccess(token2, `done`); err != nil {
		t.Fatalf("SendTaskSuccess: %v", err)
	}
	d := waitForStatus(t, svc, arn, StatusSucceeded)
	if d.Output == nil || *d.Output != `"done"` {
		t.Fatalf("output = %v, want a JSON string", d.Output)
	}
	expectHistSequence(t, mustHistory(t, svc, arn),
		EventExecutionStarted,
		EventTaskStateEntered,
		EventTaskScheduled,
		EventTaskFailed,
		EventTaskScheduled,
		EventTaskSucceeded,
		EventTaskStateExited,
		EventExecutionSucceeded,
	)
}

func TestCallbackHeartbeatKeepsTaskAlive(t *testing.T) {
	svc := NewService()
	def := `{
		"StartAt": "Callback",
		"States": {
			"Callback": {
				"Type": "Task",
				"Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
				"Parameters": {"token.$": "$$.Task.Token"},
				"HeartbeatSeconds": 0.05,
				"ResultPath": "$",
				"End": true
			}
		}
	}`
	arn := createAndStart(t, svc, "cb-hb-sm", def, `{}`)
	hist := waitForHistory(t, svc, arn, func(h []HistoryEvent) bool { return countEvents(h, EventTaskScheduled) >= 1 })
	token := lastTaskScheduledToken(t, hist)
	if err := svc.SendTaskHeartbeat(token); err != nil {
		t.Fatalf("SendTaskHeartbeat: %v", err)
	}
	if err := svc.SendTaskSuccess(token, `ok`); err != nil {
		t.Fatalf("SendTaskSuccess: %v", err)
	}
	d := waitForStatus(t, svc, arn, StatusSucceeded)
	if d.Output == nil || *d.Output != `"ok"` {
		t.Fatalf("output = %v", d.Output)
	}
	final := mustHistory(t, svc, arn)
	if countEvents(final, EventTaskTimedOut) != 0 {
		t.Fatal("heartbeat within window should have prevented TaskTimedOut")
	}
	expectHistSequence(t, final,
		EventExecutionStarted,
		EventTaskStateEntered,
		EventTaskScheduled,
		EventTaskSucceeded,
		EventTaskStateExited,
		EventExecutionSucceeded,
	)
}

func TestCallbackHeartbeatTimeoutFailsExecution(t *testing.T) {
	svc := NewService()
	def := `{
		"StartAt": "Callback",
		"States": {
			"Callback": {
				"Type": "Task",
				"Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
				"Parameters": {"token.$": "$$.Task.Token"},
				"HeartbeatSeconds": 0.1,
				"TimeoutSeconds": 5,
				"End": true
			}
		}
	}`
	arn := createAndStart(t, svc, "cb-hbt-sm", def, `{}`)
	hist := waitForHistory(t, svc, arn, func(h []HistoryEvent) bool {
		return countEvents(h, EventTaskTimedOut) >= 1
	})
	token := lastTaskScheduledToken(t, hist)
	d := waitForStatus(t, svc, arn, StatusFailed)
	if d.Error == nil || *d.Error != ErrNameTimeout {
		t.Fatalf("error = %v, want %s", d.Error, ErrNameTimeout)
	}
	final := mustHistory(t, svc, arn)
	if countEvents(final, EventTaskSucceeded) != 0 {
		t.Fatal("timed-out task must not report TaskSucceeded")
	}
	expectHistSequence(t, final,
		EventExecutionStarted,
		EventTaskStateEntered,
		EventTaskScheduled,
		EventTaskTimedOut,
		EventExecutionFailed,
	)
	if err := svc.SendTaskSuccess(token, `{}`); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken after timeout, got %v", err)
	}
}

func TestCallbackStateTimeoutFailsExecution(t *testing.T) {
	svc := NewService()
	def := callbackDef(``)[:0] + `{
		"StartAt": "Callback",
		"States": {
			"Callback": {
				"Type": "Task",
				"Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
				"Parameters": {"token.$": "$$.Task.Token"},
				"TimeoutSeconds": 0.1,
				"End": true
			}
		}
	}`
	arn := createAndStart(t, svc, "cb-st-sm", def, `{}`)
	waitForHistory(t, svc, arn, func(h []HistoryEvent) bool {
		return countEvents(h, EventTaskTimedOut) >= 1
	})
	d := waitForStatus(t, svc, arn, StatusFailed)
	if d.Error == nil || *d.Error != ErrNameTimeout {
		t.Fatalf("error = %v, want %s", d.Error, ErrNameTimeout)
	}
	expectHistSequence(t, mustHistory(t, svc, arn),
		EventExecutionStarted,
		EventTaskStateEntered,
		EventTaskScheduled,
		EventTaskTimedOut,
		EventExecutionFailed,
	)
}

func TestStopExecutionAbortsCallbackWait(t *testing.T) {
	svc := NewService()
	arn := createAndStart(t, svc, "cb-stop-sm", callbackDef(``), `{}`)
	hist := waitForHistory(t, svc, arn, func(h []HistoryEvent) bool { return countEvents(h, EventTaskScheduled) >= 1 })
	token := lastTaskScheduledToken(t, hist)
	if err := svc.StopExecution(arn); err != nil {
		t.Fatalf("StopExecution: %v", err)
	}
	d := waitForStatus(t, svc, arn, StatusAborted)
	if d.StopDate == nil {
		t.Fatal("StopDate must be set for aborted executions")
	}
	final := mustHistory(t, svc, arn)
	expectHistSequence(t, final,
		EventExecutionStarted,
		EventTaskStateEntered,
		EventTaskScheduled,
		EventExecutionAborted,
	)
	if err := svc.SendTaskSuccess(token, `{}`); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken after stop, got %v", err)
	}
}

func TestMachineLevelTimeoutTimesOutExecution(t *testing.T) {
	svc := NewService()
	def := `{
		"TimeoutSeconds": 0.1,
		"StartAt": "Callback",
		"States": {
			"Callback": {
				"Type": "Task",
				"Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
				"Parameters": {"token.$": "$$.Task.Token"},
				"End": true
			}
		}
	}`
	arn := createAndStart(t, svc, "mlt-sm", def, `{}`)
	hist := waitForHistory(t, svc, arn, func(h []HistoryEvent) bool {
		return countEvents(h, EventExecutionTimedOut) >= 1
	})
	d := waitForStatus(t, svc, arn, StatusTimedOut)
	if d.Error == nil || *d.Error != ErrNameTimeout {
		t.Fatalf("error = %v, want %s", d.Error, ErrNameTimeout)
	}
	expectHistSequence(t, hist,
		EventExecutionStarted,
		EventTaskStateEntered,
		EventTaskScheduled,
		EventExecutionTimedOut,
	)
	token := lastTaskScheduledToken(t, hist)
	if err := svc.SendTaskSuccess(token, `{}`); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken after machine timeout, got %v", err)
	}
}

func TestTaskTokenAPIErrors(t *testing.T) {
	svc := NewService()
	if err := svc.SendTaskHeartbeat("nope"); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
	if err := svc.SendTaskFailure("nope", "E", "C"); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
	if err := svc.SendTaskSuccess("nope", `{}`); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
	arn := createAndStart(t, svc, "cb-api-sm", callbackDef(``), `{}`)
	hist := waitForHistory(t, svc, arn, func(h []HistoryEvent) bool { return countEvents(h, EventTaskScheduled) >= 1 })
	token := lastTaskScheduledToken(t, hist)
	if err := svc.SendTaskSuccess(token, `not-json`); !errors.Is(err, ErrInvalidOutput) {
		t.Fatalf("expected ErrInvalidOutput, got %v", err)
	}
	if got := svc.PendingTaskCount(); got != 1 {
		t.Fatalf("failed delivery must not consume the token; pending = %d", got)
	}
	if err := svc.SendTaskSuccess(token, `{}`); err != nil {
		t.Fatalf("SendTaskSuccess: %v", err)
	}
	waitForStatus(t, svc, arn, StatusSucceeded)
}
