package sfn

import (
	"testing"
)

func TestMapStateHistorySequence(t *testing.T) {
	svc := NewService()
	def := `{
		"StartAt": "Fan",
		"States": {
			"Fan": {
				"Type": "Map",
				"ItemsPath": "$.items",
				"Iterator": {
					"StartAt": "Dup",
					"States": {
						"Dup": {
							"Type": "Pass",
							"Parameters": {"v.$": "States.Format('{}!', $.v)"},
							"End": true
						}
					}
				},
				"End": true
			}
		}
	}`
	arn := createAndStart(t, svc, "map-sm", def, `{"items":[{"v":"One"},{"v":"Two"}]}`)
	d := waitForStatus(t, svc, arn, StatusSucceeded)
	want := `[{"v":"One!"},{"v":"Two!"}]`
	if d.Output == nil || *d.Output != want {
		t.Fatalf("output = %v, want %s", d.Output, want)
	}
	hist := mustHistory(t, svc, arn)
	expectHistSequence(t, hist,
		EventExecutionStarted,
		EventMapStateEntered,
		EventMapStateStarted,
		EventMapIterationStarted, // iteration 0
		"PassStateEntered",
		"PassStateExited",
		EventMapIterationSucceeded,
		EventMapIterationStarted, // iteration 1
		"PassStateEntered",
		"PassStateExited",
		EventMapIterationSucceeded,
		EventMapStateSucceeded,
		EventMapStateExited,
		EventExecutionSucceeded,
	)
	if d0 := hist[3].MapIterationStartedEventDetails; d0 == nil || d0.Index != 0 || d0.Name != "Fan" {
		t.Fatalf("first MapIterationStarted mismatch: %+v", hist[3])
	}
	if d1 := hist[7].MapIterationStartedEventDetails; d1 == nil || d1.Index != 1 {
		t.Fatalf("second MapIterationStarted mismatch: %+v", hist[7])
	}
	if s := hist[2].MapStateStartedEventDetails; s == nil || s.Length != 2 {
		t.Fatalf("MapStateStarted mismatch: %+v", hist[2])
	}
	if exited := hist[5].StateExitedEventDetails; exited == nil || exited.Output != `{"v":"One!"}` {
		t.Fatalf("iteration output mismatch: %+v", hist[5])
	}
}

func TestParallelStateHistorySequence(t *testing.T) {
	svc := NewService()
	def := `{
		"StartAt": "P",
		"States": {
			"P": {
				"Type": "Parallel",
				"Branches": [
					{"StartAt": "B1", "States": {"B1": {"Type": "Pass", "Result": {"from": "one"}, "End": true}}},
					{"StartAt": "B2", "States": {"B2": {"Type": "Pass", "End": true}}}
				],
				"End": true
			}
		}
	}`
	arn := createAndStart(t, svc, "par-sm", def, `{}`)
	d := waitForStatus(t, svc, arn, StatusSucceeded)
	want := `[{"from":"one"},{}}]`
	want = `[{"from":"one"},{}]`
	if d.Output == nil || *d.Output != want {
		t.Fatalf("output = %v, want %s", d.Output, want)
	}
	hist := mustHistory(t, svc, arn)
	expectHistSequence(t, hist,
		EventExecutionStarted,
		EventParallelStateEntered,
		EventParallelStateStarted,
		"PassStateEntered",
		"PassStateExited",
		"PassStateEntered",
		"PassStateExited",
		EventParallelStateSucceeded,
		EventParallelStateExited,
		EventExecutionSucceeded,
	)
	if s := hist[2].ParallelStateStartedEventDetails; s == nil || s.Length != 2 {
		t.Fatalf("ParallelStateStarted mismatch: %+v", hist[2])
	}
	if e := hist[1].StateEnteredEventDetails; e == nil || e.Name != "P" {
		t.Fatalf("ParallelStateEntered mismatch: %+v", hist[1])
	}
}
