package sfn

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Control-plane error surfaces mirroring AWS Step Functions APIs.
var (
	// ErrInvalidToken is returned when a task token does not match a currently
	// pending callback (never issued, already delivered, expired or canceled).
	ErrInvalidToken = errors.New("InvalidToken")
	// ErrInvalidOutput is returned by SendTaskSuccess for non-JSON output.
	ErrInvalidOutput = errors.New("InvalidOutput")
)

type callbackResult struct {
	success bool
	payload any
	errName string
	cause   string
}

// pendingTask represents a suspended waitForTaskToken Task state (or an
// activity) waiting for an external worker to call SendTaskSuccess/
// SendTaskFailure. results is buffered so the delivering API never blocks.
type pendingTask struct {
	token      string
	execArn    string
	machineArn string
	stateName  string
	startedAt  time.Time
	results    chan callbackResult
	hb         chan struct{}
}

func (s *Service) registerCallback(cb *pendingTask) {
	s.mu.Lock()
	s.callbacks[cb.token] = cb
	s.mu.Unlock()
}

func (s *Service) removeCallback(token string) {
	s.mu.Lock()
	delete(s.callbacks, token)
	s.mu.Unlock()
}

func (s *Service) lookupCallback(token string) (*pendingTask, bool) {
	s.mu.Lock()
	cb, ok := s.callbacks[token]
	s.mu.Unlock()
	return cb, ok
}

// takeCallback atomically removes a pending callback:
// delivery is exactly-once, subsequent calls get ErrInvalidToken.
func (s *Service) takeCallback(token string) (*pendingTask, bool) {
	s.mu.Lock()
	cb, ok := s.callbacks[token]
	if ok {
		delete(s.callbacks, token)
	}
	s.mu.Unlock()
	return cb, ok
}

// PendingTaskCount returns how many tasks currently wait for a callback.
func (s *Service) PendingTaskCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.callbacks)
}

// SendTaskSuccess resumes the suspended Task state identified by taskToken,
// replacing the task result with the given (JSON-encoded) output.
func (s *Service) SendTaskSuccess(taskToken string, output string) error {
	var payload any
	if strings.TrimSpace(output) == "" {
		payload = nil
	} else {
		v, err := decodeJSONValue([]byte(output))
		if err != nil {
			return fmt.Errorf("%w: output must be valid JSON: %v", ErrInvalidOutput, err)
		}
		payload = v
	}
	cb, ok := s.takeCallback(taskToken)
	if !ok {
		return fmt.Errorf("%w: %q does not match a pending task (already delivered, expired or canceled)", ErrInvalidToken, taskToken)
	}
	cb.results <- callbackResult{success: true, payload: payload}
	return nil
}

// SendTaskFailure resumes the suspended Task state with a failure. An empty
// error name defaults to States.TaskFailed (catchable/retryable).
func (s *Service) SendTaskFailure(taskToken string, taskError string, cause string) error {
	cb, ok := s.takeCallback(taskToken)
	if !ok {
		return fmt.Errorf("%w: %q does not match a pending task (already delivered, expired or canceled)", ErrInvalidToken, taskToken)
	}
	if taskError == "" {
		taskError = ErrNameTaskFailed
	}
	cb.results <- callbackResult{success: false, errName: taskError, cause: cause}
	return nil
}

// SendTaskHeartbeat feeds the heartbeat watchdog of the pending task. It is
// accepted even for tasks without HeartbeatSeconds (matching AWS leniency).
func (s *Service) SendTaskHeartbeat(taskToken string) error {
	cb, ok := s.lookupCallback(taskToken)
	if !ok {
		return fmt.Errorf("%w: %q does not match a pending task (already delivered, expired or canceled)", ErrInvalidToken, taskToken)
	}
	select {
	case cb.hb <- struct{}{}:
	default:
	}
	return nil
}
