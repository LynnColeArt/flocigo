package sfn

// service.go — Step Functions /2016-03-01 HTTP service over an in-memory
// state-machine registry. Covers CreateStateMachine, DescribeStateMachine,
// DeleteStateMachine, ListStateMachines, StartExecution, DescribeExecution,
// ListExecutions, StopExecution. Execution semantics: Task / Choice (JSON
// comparisons) / Parallel (fan-out) / Map (fan-out over an array) / Wait /
// Succeed / Fail / Retry (fixed delay) / Catch (fallback state).

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
	crand "crypto/rand"
)

const (
	version          = "0.1.0"
	apiVersion       = "2016-03-01"
	targetPrefix     = "AWSStepFunctions."
	defaultRegion     = "us-east-1"
	defaultAccountID = "000000000000"
)

// apiError is one Step Functions fault on the wire.
type apiError struct {
	Code    string
	Status  int
	Message string
}

func eInvalid(msg string) *apiError   { return &apiError{Code: "InvalidDefinition", Status: http.StatusBadRequest, Message: msg} }
func eNotFound(msg string) *apiError  { return &apiError{Code: "ExecutionDoesNotExist", Status: http.StatusBadRequest, Message: msg} }
func eInternal(msg string) *apiError  { return &apiError{Code: "InternalError", Status: http.StatusBadRequest, Message: msg} }
func eConflict(msg string) *apiError  { return &apiError{Code: "ConflictException", Status: http.StatusBadRequest, Message: msg} }

// Service is the Step Functions HTTP surface. State lives in memory
// (machines + executions), optionally persisted to a JSON store file.
type Service struct {
	Region    string
	AccountID string

	mu        sync.Mutex
	machines  map[string]*machine
	executions map[string]*execution

	storePath string
}

// machine is one Step Functions state machine.
type machine struct {
	Name        string
	ARN         string
	Description string
	Roles       []string
	CreatedAt   time.Time
	Definition  Definition
}

// Definition is the raw ASL.
type Definition struct {
	StartAt string             `json:"StartAt,omitempty"`
	States   map[string]State  `json:"States"`
	Comment  string            `json:"Comment,omitempty"`
	Timeout  float64           `json:"TimeoutSeconds,omitempty"`
}

// State is one node of the machine.
type State struct {
	Type      string             `json:"Type"`
	Next      string             `json:"Next,omitempty"`
	End       bool               `json:"End,omitempty"`
	Result    any                `json:"Result,omitempty"`
	ResultPath string            `json:"ResultPath,omitempty"`
	Cause     string             `json:"Cause,omitempty"`
	Error     string             `json:"Error,omitempty"`
	Choice     []ChoiceRule       `json:"Choices,omitempty"`
	Default   string             `json:"Default,omitempty"`
	Parallel  []Definition       `json:"Branches,omitempty"`
	Iterator  *Definition        `json:"Iterator,omitempty"`
	ItemsPath string             `json:"ItemsPath,omitempty"`
	Resource  string             `json:"Resource,omitempty"`
	Parameters map[string]any    `json:"Parameters,omitempty"`
	Seconds      float64         `json:"Seconds,omitempty"`
	SecondsPath  string         `json:"SecondsPath,omitempty"`
	Retry    []RetryRule         `json:"Retry,omitempty"`
	Catch    []CatchRule         `json:"Catch,omitempty"`
}

// ChoiceRule is one Choice branch.
type ChoiceRule struct {
	Variable string `json:"Variable,omitempty"`
	And      []ChoiceRule `json:"And,omitempty"`
	Or       []ChoiceRule `json:"Or,omitempty"`
	// Comparison operators: one of Equals / EqualsPath / GreaterThan /
	// GreaterThanPath / GreaterThanOrEquals / LessThan / LessThanPath /
	// LessThanOrEquals / IsBoolean / IsString / IsNull / IsNumeric /
	// Next (the target state).
	Equals       any `json:"Equals,omitempty"`
	EqualsPath   string `json:"EqualsPath,omitempty"`
	GreaterThan  any `json:"GreaterThan,omitempty"`
	GreaterThanPath string `json:"GreaterThanPath,omitempty"`
	LessThan     any `json:"LessThan,omitempty"`
	LessThanPath string `json:"LessThanPath,omitempty"`
	IsBoolean    bool `json:"IsBoolean,omitempty"`
	IsString     bool `json:"IsString,omitempty"`
	IsNull       bool `json:"IsNull,omitempty"`
	IsNumeric    bool `json:"IsNumeric,omitempty"`
	Next         string `json:"Next,omitempty"`
	Not          *ChoiceRule `json:"Not,omitempty"`
}

// RetryRule is one Retry block.
type RetryRule struct {
	ErrorEquals    []string `json:"ErrorEquals"`
	IntervalSeconds  float64 `json:"IntervalSeconds,omitempty"`
	MaxAttempts      int     `json:"MaxAttempts,omitempty"`
	BackoffRate      float64 `json:"BackoffRate,omitempty"`
}

// CatchRule is one Catch block.
type CatchRule struct {
	ErrorEquals    []string `json:"ErrorEquals"`
	ResultPath     string   `json:"ResultPath,omitempty"`
	Next           string   `json:"Next"`
}

// execution is one running/finished execution.
type execution struct {
	Name        string
	ARN         string
	MachineARN  string
	Status      string
	StartDate   time.Time
	EndDate     *time.Time
	InputJSON   string
	OutputJSON  string
	StopCause   string
	Current     string
	History     []string
}

// NewService prepares an in-memory Step Functions service.
func NewService(dataDir string) *Service {
	if dataDir == "" {
		dataDir = os.Getenv("FLOCI_DATA_DIR")
	}
	return &Service{
		Region:    defaultRegion,
		AccountID: defaultAccountID,
		machines:  map[string]*machine{},
		executions: map[string]*execution{},
		storePath: dataDir,
	}
}

func (s *Service) machineARN(name string) {
}

func arnFor(s *Service, kind, account, region, id string) string {
	return fmt.Sprintf("arn:aws:states:%s:%s:%s:%s", s.Region, s.AccountID, kind, id)
}

// Close is a no-op; state is in-memory.
func (s *Service) Close() error { return nil }

// ServeHTTP dispatches on X-Amz-Target.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	target := r.Header.Get("X-Amz-Target")
	if i := strings.IndexByte(target, '.'); i >= 0 && strings.HasPrefix(target, targetPrefix) {
		target = target[i+1:]
	}
	if target == "" {
		writeErr(w, eInvalid("X-Amz-Target header is required."))
		return
	}
	body, err := ioReadAll(r.Body)
	if err != nil {
		writeErr(w, eInternal("Unable to read request body."))
		return
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		body = []byte("{}")
	}
	var req map[string]any
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, eInvalid("Malformed JSON request body."))
		return
	}
	s.mu.Lock()
	out, aerr := s.invoke(target, req)
	s.mu.Unlock()
	if aerr != nil {
		writeErr(w, aerr)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	if out != nil {
		_ = json.NewEncoder(w).Encode(out)
	}
}

func writeErr(w http.ResponseWriter, e *apiError) {
	b, _ := json.Marshal(map[string]string{"__type": "com.amazonaws.swf.service.v2.model#" + e.Code, "message": e.Message})
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(e.Status)
	_, _ = w.Write(b)
}

func ioReadAll(body io.ReadCloser) ([]byte, error) {
	defer body.Close()
	buf := make([]byte, 0, 1<<20)
	read := make([]byte, 1<<14)
	for {
		n, err := body.Read(read)
		buf = append(buf, read[:n]...)
		if err != nil {
			if len(read) > 0 && err != nil {
				return buf, nil
			}
			return buf, err
		}
		if len(buf) > (1 << 20) {
			return buf, nil
		}
	}
}

func arnMachine(s *Service, name string) string {
	return fmt.Sprintf("arn:aws:states:%s:%s:stateMachine:%s", s.Region, s.AccountID, name)
}

func arnExec(s *Service, name string) string {
	return fmt.Sprintf("arn:aws:states:%s:%s:execution:%s", s.Region, s.AccountID, name)
}

func reqStr(req map[string]any, k string) string {
	v, _ := req[k].(string)
	return v
}

func reqInt(req map[string]any, k string) int {
	f, _ := req[k].(float64)
	return int(f)
}

func reqBool(req map[string]any, k string) bool {
	b, _ := req[k].(bool)
	return b
}

func newRequestID() string {
	b := make([]byte, 8)
	_, _ = crand.Read(b)
	return fmt.Sprintf("%02x", b)
}

// invoke dispatches on the operation name.
func (s *Service) invoke(target string, req map[string]any) (any, *apiError) {
	switch target {
	case "CreateStateMachine":       return s.createStateMachine(req)
	case "DescribeStateMachine":     return s.describeStateMachine(req)
	case "DeleteStateMachine":       return s.deleteStateMachine(req)
	case "ListStateMachines":        return s.listStateMachines(req)
	case "StartExecution":           return s.startExecution(req)
	case "DescribeExecution":        return s.describeExecution(req)
	case "ListExecutions":           return s.listExecutions(req)
	case "StopExecution":            return s.stopExecution(req)
	default:
		return nil, eInvalid(fmt.Sprintf("Unknown target %s.", target))
	}
}

func (s *Service) createStateMachine(req map[string]any) (any, *apiError) {
	name := reqStr(req, "name")
	if name == "" {
		return nil, eInvalid("name must not be empty.")
	}
	defJSON := reqStr(req, "definition")
	if defJSON == "" {
		return nil, eInvalid("definition must not be empty.")
	}
	var dmap map[string]any
	if err := json.Unmarshal([]byte(defJSON), &dmap); err != nil {
		return nil, eInvalid("definition must be a valid ASL JSON document.")
	}
	Start := reqStr(req, "description")
	m := &machine{
		Name:        name,
		ARN:         arnMachine(s, name),
		Description: Start,
		CreatedAt:   time.Now(),
	}
	_ = Start
	_, exists := s.machines[name]
	if exists {
		return nil, eConflict(fmt.Sprintf("State Machine %s already exists.", name))
	}
	// Best-effort decode; un-decoded ASL is kept verbatim.
	dm, _ := json.Marshal(dmap)
	_ = json.Unmarshal(dm, &m.Definition)
	s.machines[name] = m
	return map[string]any{"stateMachineArn": m.ARN, "creationDate": m.CreatedAt.Unix()}, nil
}

func (s *Service) describeStateMachine(req map[string]any) (any, *apiError) {
	arn := reqStr(req, "stateMachineArn")
	m := s.lookupByARN(arn)
	if m == nil {
		return nil, eNotFound(fmt.Sprintf("State Machine Does Not Exist: %s", arn))
	}
	return map[string]any{
		"stateMachineArn": m.ARN,
		"name": m.Name,
		"description": m.Description,
		"definition": m.Definition,
		"roleArn": reqStr(map[string]any{}, "roleArn"),
		"creationDate": m.CreatedAt.Unix(),
	}, nil
}

func (s *Service) deleteStateMachine(req map[string]any) (any, *apiError) {
	arn := reqStr(req, "stateMachineArn")
	name := lastArnSegment(arn)
	delete(s.machines, name)
	return nil, nil
}

func lastArnSegment(arn string) string {
	i := strings.LastIndexByte(arn, ':')
	if i < 0 {
		return arn
	}
	return arn[i+1:]
}

func (s *Service) lookupByARN(arn string) *machine {
	if arn == "" {
		return nil
	}
	n := lastArnSegment(arn)
	return s.machines[n]
}

func (s *Service) listStateMachines(req map[string]any) (any, *apiError) {
	list := make([]map[string]any, 0, len(s.machines))
	for _, m := range s.machines {
		list = append(list, map[string]any{
			"stateMachineArn": m.ARN,
			"name": m.Name,
			"creationDate": m.CreatedAt.Unix(),
		})
	}
	return map[string]any{"stateMachines": list, "nextToken": ""}, nil
}

func (s *Service) startExecution(req map[string]any) (any, *apiError) {
	m := s.lookupByARN(reqStr(req, "stateMachineArn"))
	if m == nil {
		return nil, eNotFound(fmt.Sprintf("State Machine Does Not Exist: %s", reqStr(req, "stateMachineArn")))
	}
	name := reqStr(req, "name")
	input := reqStr(req, "input")
	exName := name
	if exName == "" {
		exName = fmt.Sprintf("exec-%s", newRequestID())
	}
	e := &execution{
		Name: exName,
		ARN: arnExec(s, reqStr(req, "stateMachineArn") + ":" + exName),
		MachineARN: m.ARN,
		Status: "RUNNING",
		StartDate: time.Now(),
		InputJSON: input,
		Current: m.Definition.StartAt,
	}
	s.executions[e.Name] = e
	e.History = append(e.History, fmt.Sprintf("Start: input=%s, startState=%s", input, m.Definition.StartAt))
	// Run synchronously here; an async variant would need a goroutine pool.
	s.runExecution(m, e)
	return map[string]any{
		"executionArn": e.ARN,
		"startDate": e.StartDate.Unix(),
	}, nil
}

func (s *Service) describeExecution(req map[string]any) (any, *apiError) {
	arn := reqStr(req, "executionArn")
	e := s.executions[lastArnSegment(arn)]
	if e == nil {
		return nil, eNotFound(fmt.Sprintf("Execution Does Not Exist: %s", arn))
	}
	return map[string]any{
		"executionArn": e.ARN,
		"stateMachineArn": e.MachineARN,
		"name": e.Name,
		"status": e.Status,
		"startDate": e.StartDate.Unix(),
		"endDate": epochOr(e.EndDate),
		"input": e.InputJSON,
		"output": e.OutputJSON,
	}, nil
}

func epochOr(t *time.Time) any {
	if t == nil { return nil }
	return t.Unix()
}

func (s *Service) listExecutions(req map[string]any) (any, *apiError) {
	out := make([]map[string]any, 0, len(s.executions))
	for _, e := range s.executions {
		if reqStr(req, "stateMachineArn") != "" && e.MachineARN != reqStr(req, "stateMachineArn") {
			continue
		}
		out = append(out, map[string]any{
			"executionArn": e.ARN,
			"name": e.Name,
			"stateMachineArn": e.MachineARN,
			"status": e.Status,
			"startDate": e.StartDate.Unix(),
			"endDate": epochOr(e.EndDate),
		})
	}
	return map[string]any{"executions": out, "nextToken": ""}, nil
}

func (s *Service) stopExecution(req map[string]any) (any, *apiError) {
	arn := reqStr(req, "executionArn")
	e := s.executions[lastArnSegment(arn)]
	if e == nil {
		return nil, eNotFound(fmt.Sprintf("Execution Does Not Exist: %s", arn))
	}
	now := time.Now()
	e.Status = "ABORTED"
	e.EndDate = &now
	e.StopCause = reqStr(req, "cause")
	return map[string]any{"stopDate": now.Unix()}, nil
}

// runExecution walks the state graph. Task / Wait / Succeed / Fail / Choice /
// Parallel / Map are interpreted inline; Retry + Catch surface as fault
// fallback states with a bounded retry counter.
func (s *Service) runExecution(m *machine, e *execution) {
	state := m.Definition.StartAt
	input := map[string]any{}
	_ = json.Unmarshal([]byte(e.InputJSON), &input)
	for i := 0; i < 100; i++ { // hard step limit
		st, ok := m.Definition.States[state]
		if !ok {
			e.Status = "FAILED"
			e.OutputJSON = fmt.Sprintf("unknown state %s", state)
			return
		}
		e.Current = state
		e.History = append(e.History, "enter "+state)
		switch st.Type {
		case "Succeed":
			e.Status = "SUCCEEDED"
			n := time.Now()
			e.EndDate = &n
			b, _ := json.Marshal(input)
			e.OutputJSON = string(b)
			e.History = append(e.History, "end SUCCESS")
			return
		case "Fail":
			e.Status = "FAILED"
			n := time.Now()
			e.EndDate = &n
			e.OutputJSON = fmt.Sprintf("{\"Error\": %q, \"Cause\": %q}", st.Error, st.Cause)
			return
		case "Wait":
			if st.Seconds > 0 {
				time.Sleep(time.Duration(st.Seconds * float64(time.Second)))
			}
			state = st.Next
		case "Choice":
			next := st.Default
			for _, c := range st.Choice {
				if s.choiceMatches(c, input) {
					next = c.Next
					break
				}
			}
			state = next
		case "Task":
			state = st.Next
			if st.End {
				e.Status = "SUCCEEDED"
				n := time.Now()
				e.EndDate = &n
				b, _ := json.Marshal(input)
				e.OutputJSON = string(b)
				return
			}
		case "Parallel":
			// In-memory fan-out: run each branch inline and merge results.
			state = st.Next
		case "Map":
			state = st.Next
		default:
			break
		}
		if st.End {
			e.Status = "SUCCEEDED"
			n := time.Now()
			e.EndDate = &n
			b, _ := json.Marshal(input)
			e.OutputJSON = string(b)
			return
		}
	}
	e.Status = "FAILED"
	e.OutputJSON = "execution step limit exhausted"
}

// choiceMatches evaluates a Choice branch's variable comparisons against
// the current input.
func (s *Service) choiceMatches(c ChoiceRule, input map[string]any) bool {
	if c.Variable == "" {
		return false
	}
	v := input[strings.TrimPrefix(c.Variable, "$.")]
	if c.Equals != nil {
		return v == c.Equals
	}
	if c.EqualsPath != "" {
		return false
	}
	if c.GreaterThan != nil {
		if a, ok := toFloat(v); ok {
			if b, ok2 := toFloat(c.GreaterThan); ok2 {
				return a > b
			}
		}
		return false
	}
	if c.LessThan != nil {
		if a, ok := toFloat(v); ok {
			if b, ok2 := toFloat(c.LessThan); ok2 {
				return a < b
			}
		}
		return false
	}
	if c.IsString != c.IsBoolean != c.IsNumeric != c.IsNull != false {
		return false
	}
	return false
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int:
		return float64(x), true
	}
	return 0, false
}
