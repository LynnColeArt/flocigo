package sfn

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Execution status values (AWS DescribeExecution spelling).
const (
	StatusRunning   = "RUNNING"
	StatusSucceeded = "SUCCEEDED"
	StatusFailed    = "FAILED"
	StatusAborted   = "ABORTED"
	StatusTimedOut  = "TIMED_OUT"
)

// ExecutionDescription mirrors the AWS DescribeExecution output.
type ExecutionDescription struct {
	ExecutionArn    string     `json:"executionArn"`
	StateMachineArn string     `json:"stateMachineArn"`
	Name            string     `json:"name"`
	Status          string     `json:"status"`
	StartDate       time.Time  `json:"startDate"`
	StopDate        *time.Time `json:"stopDate,omitempty"`
	Input           string     `json:"input"`
	Output          *string    `json:"output,omitempty"`
	Error           *string    `json:"error,omitempty"`
	Cause           *string    `json:"cause,omitempty"`
}

// ResourceHandler mocks non-callback task integrations (e.g. Lambda).
// Set it before starting executions; it must be safe for concurrent use.
type ResourceHandler func(resource string, payload any) (any, error)

type machineRecord struct {
	arn       string
	name      string
	def       *machine
	createdAt time.Time
}

type Service struct {
	mu sync.Mutex

	// Region and Account are used for all synthesized ARNs.
	Region  string
	Account string
	// Handler mocks non-callback Task resources; nil means such tasks fail
	// with States.TaskFailed.
	Handler ResourceHandler

	machines   map[string]*machineRecord
	executions map[string]*execution
	callbacks  map[string]*pendingTask
}

func NewService() *Service {
	return &Service{
		Region:     "us-east-1",
		Account:    "000000000000",
		machines:   make(map[string]*machineRecord),
		executions: make(map[string]*execution),
		callbacks:  make(map[string]*pendingTask),
	}
}

func (s *Service) roleArn() string {
	return fmt.Sprintf("arn:aws:iam::%s:role/stepfunctions-local", s.Account)
}

// CreateStateMachine validates and stores an ASL definition.
func (s *Service) CreateStateMachine(name string, definition string) (string, error) {
	rawVal, err := decodeJSONValue([]byte(definition))
	if err != nil {
		return "", fmt.Errorf("InvalidDefinition: definition is not valid JSON: %v", err)
	}
	raw, ok := rawVal.(map[string]any)
	if !ok {
		return "", fmt.Errorf("InvalidDefinition: definition must be a JSON object")
	}
	mDef, perr := parseMachine(name, raw)
	if perr != nil {
		return "", fmt.Errorf("InvalidDefinition: %v", perr)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	arn := fmt.Sprintf("arn:aws:states:%s:%s:stateMachine:%s", s.Region, s.Account, name)
	if _, exists := s.machines[arn]; exists {
		return "", fmt.Errorf("StateMachineAlreadyExists: a state machine named %q already exists", name)
	}
	s.machines[arn] = &machineRecord{arn: arn, name: name, def: mDef, createdAt: time.Now().UTC()}
	return arn, nil
}

// StartExecution starts the interpreter in its own goroutine.
func (s *Service) StartExecution(stateMachineArn string, executionName string, input string) (string, error) {
	s.mu.Lock()
	rec, ok := s.machines[stateMachineArn]
	s.mu.Unlock()
	if !ok {
		return "", fmt.Errorf("StateMachineDoesNotExist: %s", stateMachineArn)
	}
	if input == "" {
		input = "{}"
	}
	inputVal, err := decodeJSONValue([]byte(input))
	if err != nil {
		return "", fmt.Errorf("InvalidExecutionInput: input must be valid JSON: %v", err)
	}
	s.mu.Lock()
	name := executionName
	if name == "" {
		name = newUUID()
	}
	arn := fmt.Sprintf("arn:aws:states:%s:%s:execution:%s:%s", s.Region, s.Account, rec.name, name)
	if _, dup := s.executions[arn]; dup {
		s.mu.Unlock()
		return "", fmt.Errorf("ExecutionAlreadyExists: an execution named %q already exists for this state machine", name)
	}
	ex := &execution{
		svc:         s,
		arn:         arn,
		name:        name,
		machineArn:  stateMachineArn,
		machineName: rec.name,
		machine:     rec.def,
		input:       input,
		inputVal:    inputVal,
		started:     time.Now().UTC(),
		roleArn:     s.roleArn(),
		stopCh:      make(chan struct{}),
		done:        make(chan struct{}),
		hist:        &historyLog{},
		status:      StatusRunning,
	}
	if rec.def.hasTimeout {
		// Machine-level TimeoutSeconds: fires even while the interpreter is
		// blocked on a callback or Wait. (The timer is intentionally not
		// retained; a short-lived stray timer is harmless in the emulator.)
		ex.timeoutCh = time.NewTimer(secondsDur(rec.def.timeoutSec)).C
	}
	s.executions[arn] = ex
	s.mu.Unlock()
	ex.hist.append(EventExecutionStarted, &ExecutionStartedEventDetails{
		Name:            name,
		Input:           input,
		StateMachineArn: stateMachineArn,
		RoleArn:         ex.roleArn,
	})
	go s.runExecution(ex)
	return arn, nil
}

func (s *Service) DescribeExecution(arn string) (ExecutionDescription, error) {
	s.mu.Lock()
	ex, ok := s.executions[arn]
	s.mu.Unlock()
	if !ok {
		return ExecutionDescription{}, fmt.Errorf("ExecutionDoesNotExist: %s", arn)
	}
	return ex.describe(), nil
}

func (ex *execution) describe() ExecutionDescription {
	ex.mu.Lock()
	defer ex.mu.Unlock()
	d := ExecutionDescription{
		ExecutionArn:    ex.arn,
		StateMachineArn: ex.machineArn,
		Name:            ex.name,
		Status:          ex.status,
		StartDate:       ex.started,
		Input:           ex.input,
	}
	if ex.hasStopDate {
		sd := ex.stopDate
		d.StopDate = &sd
	}
	switch ex.status {
	case StatusSucceeded:
		out := ex.output
		d.Output = &out
	case StatusFailed, StatusTimedOut:
		errName, cause := ex.errName, ex.errCause
		d.Error = &errName
		d.Cause = &cause
	}
	return d
}

func (s *Service) StopExecution(arn string) error {
	s.mu.Lock()
	ex, ok := s.executions[arn]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("ExecutionDoesNotExist: %s", arn)
	}
	ex.stop()
	return nil
}

func (ex *execution) stop() {
	ex.mu.Lock()
	defer ex.mu.Unlock()
	if ex.stopped {
		return
	}
	ex.stopped = true
	close(ex.stopCh)
}

// GetExecutionHistory returns the ordered event list of one execution.
func (s *Service) GetExecutionHistory(arn string) ([]HistoryEvent, error) {
	s.mu.Lock()
	ex, ok := s.executions[arn]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("ExecutionDoesNotExist: %s", arn)
	}
	return ex.hist.snapshot(), nil
}

// ---------------------------------------------------------------------------
// Execution interpreter
// ---------------------------------------------------------------------------

type execution struct {
	svc         *Service
	arn         string
	name        string
	machineArn  string
	machineName string
	machine     *machine
	input       string
	inputVal    any
	started     time.Time
	roleArn     string

	stopCh    chan struct{}          // closed by StopExecution
	timeoutCh <-chan time.Time       // machine-level TimeoutSeconds (nil = none)
	done      chan struct{}          // closed when the execution is terminal
	hist      *historyLog

	mu               sync.Mutex
	status           string
	output           string
	errName          string
	errCause         string
	stopDate         time.Time
	hasStopDate      bool
	stopped          bool
	terminalRecorded bool
}

var (
	errAbortedSentinel        = &stateError{ErrName: "__aborted__", Cause: "Execution was stopped"}
	errMachineTimeoutSentinel = &stateError{ErrName: ErrNameTimeout, Cause: "Machine-level timeout exceeded", machineScope: true}
)

func (s *Service) runExecution(ex *execution) {
	out, err := ex.runMachine(ex.machine, ex.inputVal)
	ex.finish(out, err)
}

func (ex *execution) finish(result any, err *stateError) {
	ex.mu.Lock()
	if ex.terminalRecorded {
		ex.mu.Unlock()
		return
	}
	ex.terminalRecorded = true
	now := time.Now().UTC()
	ex.stopDate = now
	ex.hasStopDate = true
	switch {
	case ex.stopped:
		ex.status = StatusAborted
		ex.hist.append(EventExecutionAborted, &ExecutionAbortedEventDetails{Cause: "Execution was stopped by StopExecution"})
	case err == nil:
		ex.status = StatusSucceeded
		ex.output = jsonText(result)
		ex.hist.append(EventExecutionSucceeded, &ExecutionSucceededEventDetails{Output: ex.output})
	case err.machineScope:
		ex.status = StatusTimedOut
		ex.errName = err.ErrName
		ex.errCause = "Execution timed out (machine-level TimeoutSeconds exceeded)"
		ex.hist.append(EventExecutionTimedOut, &ExecutionTimedOutEventDetails{Cause: ex.errCause})
	default:
		ex.status = StatusFailed
		ex.errName = err.ErrName
		ex.errCause = err.Cause
		ex.hist.append(EventExecutionFailed, &ExecutionFailedEventDetails{Error: err.ErrName, Cause: err.Cause})
	}
	ex.mu.Unlock()
	close(ex.done)
}

func (ex *execution) runMachine(m *machine, input any) (any, *stateError) {
	out := input
	cur := m.startAt
	for {
		st := m.states[cur]
		nout, next, err := ex.runState(st, out)
		if err != nil {
			return nil, err
		}
		out = nout
		if next == "" {
			return out, nil
		}
		cur = next
	}
}

func (ex *execution) runState(st *stateDef, input any) (any, string, *stateError) {
	in, serr := applyInputPath(input, st.inputPath)
	if serr != nil {
		return nil, "", serr
	}
	// The State(Task)Entered event is emitted once; retries only re-emit
	// TaskScheduled, matching real Step Functions history behavior.
	ex.hist.append(historyEnterType(st), historyEnterDetails(st, in))
	for attempt := 0; ; attempt++ {
		out, next, serr := ex.runStateAttempt(st, in, attempt)
		if serr == nil {
			return out, next, nil
		}
		if serr == errAbortedSentinel || serr.machineScope {
			return nil, "", serr
		}
		if rule := matchRetry(st.retry, serr.ErrName); rule != nil && attempt < rule.maxAttempts {
			oc := ex.sleepInterruptible(retryDelay(rule, attempt))
			if oc != waitCompleted {
				return nil, "", waitOutcomeError(oc)
			}
			continue
		}
		if cr := matchCatch(st.catch, serr.ErrName); cr != nil {
			errObj := map[string]any{"Error": serr.ErrName, "Cause": serr.Cause}
			merged, merr := mergeResultPath(in, cr.resultPath, errObj)
			if merr != nil {
				return nil, "", merr
			}
			out, oerr := applyOutputPath(merged, st.outputPath)
			if oerr != nil {
				return nil, "", oerr
			}
			return out, cr.next, nil
		}
		return nil, "", serr
	}
}

func (ex *execution) runStateAttempt(st *stateDef, in any, attempt int) (any, string, *stateError) {
	switch st.typ {
	case "Pass":
		var res any = in
		if st.hasParams {
			r, err := applyPayloadTemplate(st.params, in, ex.buildContext(st, attempt, ""))
			if err != nil {
				return nil, "", wrapWith(err, ErrNameParameterPathFormat)
			}
			res = r
		} else if st.hasResult {
			res = deepCopy(st.result)
		}
		merged, merr := mergeResultPath(in, st.resultPath, res)
		if merr != nil {
			return nil, "", merr
		}
		out, oerr := applyOutputPath(merged, st.outputPath)
		if oerr != nil {
			return nil, "", oerr
		}
		ex.emitStateExited(st, out)
		return out, nextState(st), nil

	case "Succeed":
		out, oerr := applyOutputPath(in, st.outputPath)
		if oerr != nil {
			return nil, "", oerr
		}
		return out, "", nil

	case "Fail":
		return nil, "", newStateError(st.failError, st.failCause)

	case "Wait":
		d, werr := waitDuration(st, in, ex.buildContext(st, attempt, ""))
		if werr != nil {
			return nil, "", werr
		}
		if oc := ex.sleepInterruptible(d); oc != waitCompleted {
			return nil, "", waitOutcomeError(oc)
		}
		out, oerr := applyOutputPath(in, st.outputPath)
		if oerr != nil {
			return nil, "", oerr
		}
		ex.emitStateExited(st, out)
		return out, nextState(st), nil

	case "Choice":
		cx := ex.buildContext(st, attempt, "")
		next, matched := "", false
		for _, rn := range st.choices {
			if evalChoice(rn, in, cx) {
				next, matched = rn.next, true
				break
			}
		}
		if !matched && st.hasDefault {
			next, matched = st.defaultNext, true
		}
		if !matched {
			return nil, "", newStateError(ErrNameNoChoiceMatched, fmt.Sprintf("no choice rule matched the input in state %q and no Default was specified", st.name))
		}
		out, oerr := applyOutputPath(in, st.outputPath)
		if oerr != nil {
			return nil, "", oerr
		}
		ex.emitStateExited(st, out)
		return out, next, nil

	case "Task":
		return ex.runTaskAttempt(st, in, attempt)

	case "Parallel":
		ex.hist.append(EventParallelStateStarted, &ParallelStateStartedEventDetails{Length: len(st.branches)})
		results := make([]any, len(st.branches))
		for i, b := range st.branches {
			bout, berr := ex.runMachine(b, in)
			if berr != nil {
				ex.hist.append(EventParallelStateFailed, nil)
				return nil, "", berr
			}
			results[i] = bout
		}
		return ex.finalizeComposite(st, in, results)

	case "Map":
		itemsPath := "$"
		if st.itemsPath != nil && *st.itemsPath != "" {
			itemsPath = *st.itemsPath
		}
		items, rerr := resolveRefPath(in, nil, itemsPath)
		if rerr != nil {
			return nil, "", newStateError(ErrNameRuntime, fmt.Sprintf("Map state %q: ItemsPath %q did not resolve", st.name, itemsPath))
		}
		arr, ok := items.([]any)
		if !ok {
			return nil, "", newStateError(ErrNameRuntime, fmt.Sprintf("Map state %q: ItemsPath %q did not resolve to an array", st.name, itemsPath))
		}
		ex.hist.append(EventMapStateStarted, &MapStateStartedEventDetails{Length: len(arr)})
		results := make([]any, 0, len(arr))
		for i, item := range arr {
			ex.hist.append(EventMapIterationStarted, &MapIterationStartedEventDetails{Name: st.name, Index: i, Type: "Map"})
			itemInput := item
			cxItem := ex.buildContext(st, attempt, "")
			cxItem["Map"] = map[string]any{"Item": map[string]any{"Index": json.Number(strconv.Itoa(i)), "Value": item}}
			if st.itemSelector != nil {
				sel, serr := applyPayloadTemplate(st.itemSelector, item, cxItem)
				if serr != nil {
					return nil, "", wrapWith(serr, ErrNameRuntime)
				}
				itemInput = sel
			}
			iout, ierr := ex.runMachine(st.iterator, itemInput)
			if ierr != nil {
				ex.hist.append(EventMapIterationFailed, &MapIterationFailedEventDetails{Name: st.name, Index: i, Type: "Map"})
				ex.hist.append(EventMapStateFailed, nil)
				return nil, "", ierr
			}
			ex.hist.append(EventMapIterationSucceeded, &MapIterationSucceededEventDetails{Name: st.name, Index: i, Type: "Map"})
			results = append(results, iout)
		}
		return ex.finalizeComposite(st, in, results)

	default:
		return nil, "", newStateError(ErrNameRuntime, "unsupported state type: "+st.typ)
	}
}

// finalizeComposite applies ResultSelector/ResultPath/OutputPath and emits
// the success + exited events shared by Parallel and Map states.
func (ex *execution) finalizeComposite(st *stateDef, in any, res any) (any, string, *stateError) {
	if st.resultSelector != nil {
		s, err := applyResultSelector(st.resultSelector, res)
		if err != nil {
			return nil, "", wrapWith(err, ErrNameResultSelectorPath)
		}
		res = s
	}
	merged, merr := mergeResultPath(in, st.resultPath, res)
	if merr != nil {
		return nil, "", merr
	}
	out, oerr := applyOutputPath(merged, st.outputPath)
	if oerr != nil {
		return nil, "", oerr
	}
	if st.typ == "Parallel" {
		ex.hist.append(EventParallelStateSucceeded, nil)
	} else {
		ex.hist.append(EventMapStateSucceeded, nil)
	}
	ex.emitStateExited(st, out)
	return out, nextState(st), nil
}

// runTaskAttempt schedules one attempt of a Task state (callback or mocked).
func (ex *execution) runTaskAttempt(st *stateDef, in any, attempt int) (any, string, *stateError) {
	cbMode := st.isCallbackResource()
	token := ""
	if cbMode {
		token = newUUID()
	}
	cx := ex.buildContext(st, attempt, token)
	var payload any = in
	if st.hasParams {
		p, err := applyPayloadTemplate(st.params, in, cx)
		if err != nil {
			return nil, "", wrapWith(err, ErrNameParameterPathFormat)
		}
		payload = p
	}
	var res any
	var terr *stateError
	if cbMode {
		cb := &pendingTask{
			token:      token,
			execArn:    ex.arn,
			machineArn: ex.machineArn,
			stateName:  st.name,
			startedAt:  time.Now().UTC(),
			results:    make(chan callbackResult, 1),
			hb:         make(chan struct{}, 1),
		}
		ex.svc.registerCallback(cb)
		scheduledID := ex.hist.appendPtr()
		_ = scheduledID
		ex.hist.append(EventTaskScheduled, &TaskScheduledEventDetails{
			Resource:       st.resource,
			ResourceType:   resourceType(st.resource),
			Region:         ex.svc.Region,
			Parameters:     jsonText(payload),
			TimeoutSeconds: taskDetailTimeout(st),
		})
		cres, werr := ex.waitForCallback(st, cb)
		ex.svc.removeCallback(token)
		if werr != nil {
			terr = werr
		} else if !cres.success {
			terr = newStateError(cres.errName, cres.cause)
			ex.hist.append(EventTaskFailed, &TaskFailedEventDetails{
				Resource: st.resource,
				Error:    cres.errName,
				Cause:    cres.cause,
			})
		} else {
			res = cres.payload
		}
	} else {
		ex.hist.append(EventTaskScheduled, &TaskScheduledEventDetails{
			Resource:       st.resource,
			ResourceType:   resourceType(st.resource),
			Region:         ex.svc.Region,
			Parameters:     jsonText(payload),
			TimeoutSeconds: taskDetailTimeout(st),
		})
		if h := ex.svc.Handler; h != nil {
			r, herr := h(st.resource, payload)
			if herr != nil {
				terr = newStateError(ErrNameTaskFailed, herr.Error())
				ex.hist.append(EventTaskFailed, &TaskFailedEventDetails{
					Resource: st.resource,
					Error:    ErrNameTaskFailed,
					Cause:    herr.Error(),
				})
			} else {
				res = r
			}
		} else {
			cause := fmt.Sprintf("no resource handler configured for %q; set Service.Handler or use a .waitForTaskToken resource driven by SendTaskSuccess/SendTaskFailure", st.resource)
			terr = newStateError(ErrNameTaskFailed, cause)
			ex.hist.append(EventTaskFailed, &TaskFailedEventDetails{
				Resource: st.resource,
				Error:    ErrNameTaskFailed,
				Cause:    cause,
			})
		}
	}
	if terr != nil {
		return nil, "", terr
	}
	// Success path.
	out := res
	if st.resultSelector != nil {
		s, err := applyResultSelector(st.resultSelector, res)
		if err != nil {
			return nil, "", wrapWith(err, ErrNameResultSelectorPath)
		}
		out = s
	}
	ex.hist.append(EventTaskSucceeded, &TaskSucceededEventDetails{
		Resource: st.resource,
		Output:   jsonText(res),
	})
	merged, merr := mergeResultPath(in, st.resultPath, out)
	if merr != nil {
		return nil, "", merr
	}
	output, oerr := applyOutputPath(merged, st.outputPath)
	if oerr != nil {
		return nil, "", oerr
	}
	ex.emitStateExited(st, output)
	return output, nextState(st), nil
}

func taskDetailTimeout(st *stateDef) float64 {
	if st.hasTimeout {
		return st.timeoutSec
	}
	return 0
}

// waitForCallback blocks until the external worker resolves the task, the
// state/machine times out, a heartbeat is missed, or the execution is stopped.
func (ex *execution) waitForCallback(st *stateDef, cb *pendingTask) (callbackResult, *stateError) {
	var stateTimerC <-chan time.Time
	if st.hasTimeout {
		stateTimer := time.NewTimer(secondsDur(st.timeoutSec))
		stateTimerC = stateTimer.C
		defer stateTimer.Stop()
	}
	var hbTimer *time.Timer
	var hbC <-chan time.Time
	if st.hasHeartbeat {
		hbTimer = time.NewTimer(secondsDur(st.heartbeatSec))
		hbC = hbTimer.C
		defer hbTimer.Stop()
	}
	for {
		select {
		case r := <-cb.results:
			return r, nil
		case <-ex.stopCh:
			return callbackResult{}, errAbortedSentinel
		case <-ex.timeoutCh:
			return callbackResult{}, errMachineTimeoutSentinel
		case <-stateTimerC:
			cause := fmt.Sprintf("Task state %q timed out (TimeoutSeconds=%g)", st.name, st.timeoutSec)
			ex.appendTaskTimedOut(st, cause)
			return callbackResult{}, newStateError(ErrNameTimeout, cause)
		case <-cb.hb:
			// Worker heartbeat arrived; restart the watchdog.
			if hbTimer != nil {
				if !hbTimer.Stop() {
					select {
					case <-hbTimer.C:
					default:
					}
				}
				hbTimer.Reset(secondsDur(st.heartbeatSec))
			}
		case <-hbC:
			if hbTimer != nil {
				cause := fmt.Sprintf("Task state %q timed out waiting for a heartbeat (HeartbeatSeconds=%g)", st.name, st.heartbeatSec)
				ex.appendTaskTimedOut(st, cause)
				return callbackResult{}, newStateError(ErrNameTimeout, cause)
			}
		}
	}
}

func (ex *execution) appendTaskTimedOut(st *stateDef, cause string) {
	ex.hist.append(EventTaskTimedOut, &TaskTimedOutEventDetails{
		Resource: st.resource,
		Error:    ErrNameTimeout,
		Cause:    cause,
	})
}

func (st *stateDef) isCallbackResource() bool {
	return strings.HasSuffix(st.resource, ".waitForTaskToken") ||
		strings.HasSuffix(st.resource, ":waitForTaskToken") ||
		strings.Contains(st.resource, ":activity:")
}

func resourceType(res string) string {
	if strings.Contains(res, ":activity:") {
		return "activity"
	}
	if strings.Contains(res, "lambda") {
		return "lambda"
	}
	return "custom"
}

func historyEnterType(st *stateDef) string {
	if st.typ == "Task" {
		return EventTaskStateEntered
	}
	return st.typ + "StateEntered"
}

func historyEnterDetails(st *stateDef, in any) any {
	if st.typ == "Task" {
		return &TaskStateEnteredEventDetails{Name: st.name, Type: st.typ}
	}
	switch st.typ {
	case "Succeed", "Fail":
		return &StateEnteredEventDetails{Name: st.name}
	}
	return &StateEnteredEventDetails{Name: st.name, Input: jsonText(in)}
}

func historyExitType(st *stateDef) string {
	if st.typ == "Task" {
		return EventTaskStateExited
	}
	return st.typ + "StateExited"
}

func (ex *execution) emitStateExited(st *stateDef, out any) {
	ex.hist.append(historyExitType(st), &StateExitedEventDetails{Name: st.name, Output: jsonText(out)})
}

func nextState(st *stateDef) string {
	if st.end {
		return ""
	}
	return st.next
}

// ---------------------------------------------------------------------------
// Error matching / delays / waits
// ---------------------------------------------------------------------------

func errorMatches(patterns []string, errName string) bool {
	for _, p := range patterns {
		if p == "States.ALL" || p == errName {
			return true
		}
		if strings.HasSuffix(p, ".*") && strings.HasPrefix(errName, strings.TrimSuffix(p, "*")) {
			return true
		}
	}
	return false
}

func matchRetry(rules []*retryRule, errName string) *retryRule {
	for _, r := range rules {
		if errorMatches(r.errors, errName) {
			return r
		}
	}
	return nil
}

func matchCatch(rules []*catchRule, errName string) *catchRule {
	for _, c := range rules {
		if errorMatches(c.errors, errName) {
			return c
		}
	}
	return nil
}

func retryDelay(r *retryRule, attempt int) time.Duration {
	f := r.interval * math.Pow(r.backoff, float64(attempt))
	return secondsDur(f)
}

type waitOutcome int

const (
	waitCompleted waitOutcome = iota
	waitAborted
	waitMachineTimeout
)

func waitOutcomeError(oc waitOutcome) *stateError {
	if oc == waitMachineTimeout {
		return errMachineTimeoutSentinel
	}
	return errAbortedSentinel
}

func (ex *execution) sleepInterruptible(d time.Duration) waitOutcome {
	if d <= 0 {
		return waitCompleted
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return waitCompleted
	case <-ex.stopCh:
		return waitAborted
	case <-ex.timeoutCh:
		return waitMachineTimeout
	}
}

func secondsDur(f float64) time.Duration {
	if f <= 0 {
		return 0
	}
	return time.Duration(f * float64(time.Second))
}

func waitDuration(st *stateDef, in any, cx any) (time.Duration, *stateError) {
	switch {
	case st.hasWaitSeconds:
		return secondsDur(st.waitSeconds), nil
	case st.hasWaitSecondsPath:
		v, err := resolveRefPath(in, cx, st.waitSecondsPath)
		if err != nil {
			return 0, newStateError(ErrNameRuntime, fmt.Sprintf("Wait state %q: SecondsPath %q did not resolve", st.name, st.waitSecondsPath))
		}
		f, ok := asFloat(v)
		if !ok || f < 0 {
			return 0, newStateError(ErrNameRuntime, fmt.Sprintf("Wait state %q: SecondsPath %q did not resolve to a non-negative number", st.name, st.waitSecondsPath))
		}
		return secondsDur(f), nil
	case st.hasWaitTimestamp:
		return untilTimestamp(st.waitTimestamp, st)
	case st.hasWaitTimestampPath:
		v, err := resolveRefPath(in, cx, st.waitTimestampPath)
		if err != nil {
			return 0, newStateError(ErrNameRuntime, fmt.Sprintf("Wait state %q: TimestampPath %q did not resolve", st.name, st.waitTimestampPath))
		}
		s, ok := v.(string)
		if !ok {
			return 0, newStateError(ErrNameRuntime, fmt.Sprintf("Wait state %q: TimestampPath %q did not resolve to a timestamp string", st.name, st.waitTimestampPath))
		}
		return untilTimestamp(s, st)
	}
	return 0, newStateError(ErrNameRuntime, fmt.Sprintf("Wait state %q has no wait directive", st.name))
}

func untilTimestamp(ts string, st *stateDef) (time.Duration, *stateError) {
	t, ok := parseTimestamp(ts)
	if !ok {
		return 0, newStateError(ErrNameRuntime, fmt.Sprintf("Wait state %q: %q is not a valid RFC3339 timestamp", st.name, ts))
	}
	d := time.Until(t)
	if d < 0 {
		d = 0
	}
	return d, nil
}

// ---------------------------------------------------------------------------
// Payload templates, selectors and paths
// ---------------------------------------------------------------------------

// applyInputPath applies InputPath; a null (empty marker) yields {}.
func applyInputPath(input any, p *string) (any, *stateError) {
	if p == nil {
		return input, nil
	}
	if *p == "" {
		return map[string]any{}, nil
	}
	v, err := resolveRefPath(input, nil, *p)
	if err != nil {
		return nil, newStateError(ErrNameRuntime, fmt.Sprintf("InputPath %q could not be applied to the input: %v", *p, err))
	}
	return v, nil
}

func applyOutputPath(input any, p *string) (any, *stateError) {
	if p == nil {
		return input, nil
	}
	if *p == "" {
		return map[string]any{}, nil
	}
	v, err := resolveRefPath(input, nil, *p)
	if err != nil {
		return nil, newStateError(ErrNameRuntime, fmt.Sprintf("OutputPath %q could not be applied to the input: %v", *p, err))
	}
	return v, nil
}

// applyPayloadTemplate builds a Parameters/ItemSelector payload: keys with the
// '.$' suffix hold either a reference path ($./$$. incl. $$.Task.Token) or a
// full intrinsic function invocation; every other value is a literal with
// nested payload templates expanded recursively.
func applyPayloadTemplate(tmpl map[string]any, input any, cx any) (map[string]any, error) {
	out := make(map[string]any, len(tmpl))
	resolver := func(path string) (any, error) {
		return resolveRefPath(input, cx, path)
	}
	for k, v := range tmpl {
		if strings.HasSuffix(k, ".$") {
			key := strings.TrimSuffix(k, ".$")
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("field %q with a '.$' suffix must hold a reference path or intrinsic function string", k)
			}
			var val any
			switch {
			case isPathString(s):
				rv, err := resolveRefPath(input, cx, s)
				if err != nil {
					return nil, fmt.Errorf("could not resolve reference path %q for field %q: %v", s, k, err)
				}
				val = rv
			case strings.HasPrefix(s, intrinsicPrefix):
				iv, err := EvalIntrinsic(s, resolver)
				if err != nil {
					return nil, err
				}
				val = iv
			case s == "":
				return nil, fmt.Errorf("field %q has an empty path expression", k)
			default:
				return nil, fmt.Errorf("field %q holds %q which is neither a reference path nor an intrinsic function", k, s)
			}
			out[key] = val
			continue
		}
		if m, ok := v.(map[string]any); ok {
			sub, err := applyPayloadTemplate(m, input, cx)
			if err != nil {
				return nil, err
			}
			out[k] = sub
			continue
		}
		if arr, ok := v.([]any); ok {
			out[k] = deepCopy(arr)
			continue
		}
		out[k] = v
	}
	return out, nil
}

// applyResultSelector builds the ResultSelector object; its '.$' fields may
// be paths or intrinsic functions evaluated against the raw task result.
func applyResultSelector(sel map[string]any, result any) (map[string]any, error) {
	out := make(map[string]any, len(sel))
	resolver := func(path string) (any, error) {
		return resolveRefPath(result, nil, path)
	}
	for k, v := range sel {
		if strings.HasSuffix(k, ".$") {
			key := strings.TrimSuffix(k, ".$")
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("ResultSelector field %q must be a string", k)
			}
			var val any
			if isPathString(s) {
				rv, err := resolveRefPath(result, nil, s)
				if err != nil {
					return nil, fmt.Errorf("ResultSelector path %q could not be resolved: %v", s, err)
				}
				val = rv
			} else if strings.HasPrefix(s, intrinsicPrefix) {
				iv, err := EvalIntrinsic(s, resolver)
				if err != nil {
					return nil, err
				}
				val = iv
			} else {
				return nil, fmt.Errorf("ResultSelector field %q holds %q which is neither a reference path nor an intrinsic function", k, s)
			}
			out[key] = val
			continue
		}
		if m, ok := v.(map[string]any); ok {
			sub, err := applyResultSelector(m, result)
			if err != nil {
				return nil, err
			}
			out[k] = sub
			continue
		}
		out[k] = v
	}
	return out, nil
}

// buildContext constructs the $$. context object (Execution, State,
// StateMachine and, for waitForTaskToken tasks, Task.Token).
func (ex *execution) buildContext(st *stateDef, retryCount int, taskToken string) map[string]any {
	cx := map[string]any{
		"Execution": map[string]any{
			"Id":        ex.arn,
			"Name":      ex.name,
			"Input":     ex.inputVal,
			"RoleArn":   ex.roleArn,
			"StartTime": ex.started.Format(time.RFC3339),
		},
		"State": map[string]any{
			"Name":        st.name,
			"EnteredTime": time.Now().UTC().Format(time.RFC3339),
			"RetryCount":  json.Number(strconv.Itoa(retryCount)),
		},
		"StateMachine": map[string]any{
			"Id":   ex.machineArn,
			"Name": ex.machineName,
		},
	}
	if taskToken != "" {
		cx["Task"] = map[string]any{"Token": taskToken}
	}
	return cx
}
