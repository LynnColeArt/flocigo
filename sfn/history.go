package sfn

import (
	"sync"
	"time"
)

// History event type names, mirroring the AWS GetExecutionHistory event enum.
const (
	EventExecutionStarted  = "ExecutionStarted"
	EventExecutionSucceeded = "ExecutionSucceeded"
	EventExecutionFailed   = "ExecutionFailed"
	EventExecutionAborted  = "ExecutionAborted"
	EventExecutionTimedOut = "ExecutionTimedOut"

	EventTaskStateEntered = "TaskStateEntered"
	EventTaskStateExited  = "TaskStateExited"
	EventTaskScheduled    = "TaskScheduled"
	EventTaskSucceeded    = "TaskSucceeded"
	EventTaskFailed       = "TaskFailed"
	EventTaskTimedOut     = "TaskTimedOut"

	EventMapStateEntered       = "MapStateEntered"
	EventMapStateStarted       = "MapStateStarted"
	EventMapStateSucceeded     = "MapStateSucceeded"
	EventMapStateFailed        = "MapStateFailed"
	EventMapStateExited        = "MapStateExited"
	EventMapIterationStarted   = "MapIterationStarted"
	EventMapIterationSucceeded = "MapIterationSucceeded"
	EventMapIterationFailed    = "MapIterationFailed"

	EventParallelStateEntered   = "ParallelStateEntered"
	EventParallelStateStarted   = "ParallelStateStarted"
	EventParallelStateSucceeded = "ParallelStateSucceeded"
	EventParallelStateFailed    = "ParallelStateFailed"
	EventParallelStateExited    = "ParallelStateExited"
)

// HistoryEvent mirrors the AWS GetExecutionHistory event shape.
type HistoryEvent struct {
	Id              int64     `json:"id"`
	PreviousEventId int64     `json:"previousEventId,omitempty"`
	Timestamp       time.Time `json:"timestamp"`
	Type            string    `json:"type"`

	ExecutionStartedEventDetails      *ExecutionStartedEventDetails      `json:"executionStartedEventDetails,omitempty"`
	ExecutionSucceededEventDetails    *ExecutionSucceededEventDetails    `json:"executionSucceededEventDetails,omitempty"`
	ExecutionFailedEventDetails       *ExecutionFailedEventDetails       `json:"executionFailedEventDetails,omitempty"`
	ExecutionAbortedEventDetails      *ExecutionAbortedEventDetails      `json:"executionAbortedEventDetails,omitempty"`
	ExecutionTimedOutEventDetails     *ExecutionTimedOutEventDetails     `json:"executionTimedOutEventDetails,omitempty"`
	StateEnteredEventDetails          *StateEnteredEventDetails          `json:"stateEnteredEventDetails,omitempty"`
	StateExitedEventDetails           *StateExitedEventDetails           `json:"stateExitedEventDetails,omitempty"`
	TaskStateEnteredEventDetails      *TaskStateEnteredEventDetails      `json:"taskStateEnteredEventDetails,omitempty"`
	TaskScheduledEventDetails         *TaskScheduledEventDetails         `json:"taskScheduledEventDetails,omitempty"`
	TaskSucceededEventDetails         *TaskSucceededEventDetails         `json:"taskSucceededEventDetails,omitempty"`
	TaskFailedEventDetails            *TaskFailedEventDetails            `json:"taskFailedEventDetails,omitempty"`
	TaskTimedOutEventDetails          *TaskTimedOutEventDetails          `json:"taskTimedOutEventDetails,omitempty"`
	MapStateStartedEventDetails       *MapStateStartedEventDetails       `json:"mapStateStartedEventDetails,omitempty"`
	MapIterationStartedEventDetails   *MapIterationStartedEventDetails   `json:"mapIterationStartedEventDetails,omitempty"`
	MapIterationSucceededEventDetails *MapIterationSucceededEventDetails `json:"mapIterationSucceededEventDetails,omitempty"`
	MapIterationFailedEventDetails    *MapIterationFailedEventDetails    `json:"mapIterationFailedEventDetails,omitempty"`
	ParallelStateStartedEventDetails  *ParallelStateStartedEventDetails  `json:"parallelStateStartedEventDetails,omitempty"`
}

type ExecutionStartedEventDetails struct {
	Name            string `json:"name,omitempty"`
	RoleArn         string `json:"roleArn,omitempty"`
	Input           string `json:"input,omitempty"`
	StateMachineArn string `json:"stateMachineArn,omitempty"`
}

type ExecutionSucceededEventDetails struct {
	Output string `json:"output,omitempty"`
}

type ExecutionFailedEventDetails struct {
	Error string `json:"error,omitempty"`
	Cause string `json:"cause,omitempty"`
}

type ExecutionAbortedEventDetails struct {
	Cause string `json:"cause,omitempty"`
}

type ExecutionTimedOutEventDetails struct {
	Cause string `json:"cause,omitempty"`
}

// StateEnteredEventDetails is used for Pass/Wait/Choice (with input), for
// Succeed/Fail (name only) and for Parallel/Map (name + input).
type StateEnteredEventDetails struct {
	Name  string `json:"name"`
	Input string `json:"input,omitempty"`
}

type StateExitedEventDetails struct {
	Name   string `json:"name"`
	Output string `json:"output,omitempty"`
}

type TaskStateEnteredEventDetails struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type TaskScheduledEventDetails struct {
	Resource       string  `json:"resource"`
	ResourceType   string  `json:"resourceType"`
	Region         string  `json:"region,omitempty"`
	Parameters     string  `json:"parameters,omitempty"`
	TimeoutSeconds float64 `json:"timeoutSeconds,omitempty"`
}

type TaskSucceededEventDetails struct {
	Resource string `json:"resource"`
	Output   string `json:"output,omitempty"`
}

type TaskFailedEventDetails struct {
	Resource string `json:"resource"`
	Error    string `json:"error,omitempty"`
	Cause    string `json:"cause,omitempty"`
}

type TaskTimedOutEventDetails struct {
	Resource string `json:"resource"`
	Error    string `json:"error,omitempty"`
	Cause    string `json:"cause,omitempty"`
}

type MapStateStartedEventDetails struct {
	Length int `json:"length"`
}

type MapIterationStartedEventDetails struct {
	Name  string `json:"name,omitempty"`
	Index int    `json:"index"`
	Type  string `json:"type,omitempty"`
}

type MapIterationSucceededEventDetails struct {
	Name  string `json:"name,omitempty"`
	Index int    `json:"index"`
	Type  string `json:"type,omitempty"`
}

type MapIterationFailedEventDetails struct {
	Name  string `json:"name,omitempty"`
	Index int    `json:"index"`
	Type  string `json:"type,omitempty"`
}

type ParallelStateStartedEventDetails struct {
	Length int `json:"length"`
}

// historyLog records the ordered event list of one execution.
type historyLog struct {
	mu     sync.Mutex
	events []HistoryEvent
}

func (h *historyLog) append(typ string, details any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	id := int64(len(h.events) + 1)
	ev := HistoryEvent{
		Id:        id,
		Timestamp: time.Now().UTC(),
		Type:      typ,
	}
	if id > 1 {
		ev.PreviousEventId = id - 1
	}
	setEventDetails(&ev, details)
	h.events = append(h.events, ev)
}

func setEventDetails(ev *HistoryEvent, d any) {
	if d == nil {
		return
	}
	switch t := d.(type) {
	case *ExecutionStartedEventDetails:
		ev.ExecutionStartedEventDetails = t
	case *ExecutionSucceededEventDetails:
		ev.ExecutionSucceededEventDetails = t
	case *ExecutionFailedEventDetails:
		ev.ExecutionFailedEventDetails = t
	case *ExecutionAbortedEventDetails:
		ev.ExecutionAbortedEventDetails = t
	case *ExecutionTimedOutEventDetails:
		ev.ExecutionTimedOutEventDetails = t
	case *StateEnteredEventDetails:
		ev.StateEnteredEventDetails = t
	case *StateExitedEventDetails:
		ev.StateExitedEventDetails = t
	case *TaskStateEnteredEventDetails:
		ev.TaskStateEnteredEventDetails = t
	case *TaskScheduledEventDetails:
		ev.TaskScheduledEventDetails = t
	case *TaskSucceededEventDetails:
		ev.TaskSucceededEventDetails = t
	case *TaskFailedEventDetails:
		ev.TaskFailedEventDetails = t
	case *TaskTimedOutEventDetails:
		ev.TaskTimedOutEventDetails = t
	case *MapStateStartedEventDetails:
		ev.MapStateStartedEventDetails = t
	case *MapIterationStartedEventDetails:
		ev.MapIterationStartedEventDetails = t
	case *MapIterationSucceededEventDetails:
		ev.MapIterationSucceededEventDetails = t
	case *MapIterationFailedEventDetails:
		ev.MapIterationFailedEventDetails = t
	case *ParallelStateStartedEventDetails:
		ev.ParallelStateStartedEventDetails = t
	}
}

func (h *historyLog) snapshot() []HistoryEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]HistoryEvent, len(h.events))
	copy(out, h.events)
	return out
}
