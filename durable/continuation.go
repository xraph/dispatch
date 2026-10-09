package durable

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

const (
	EventWorkflowContinued = "workflow.continued_as_new"
	EventRunStarted        = "workflow.run_started"
	EventSignalCarried     = "workflow.signal_carried"
	EventSignalConsumed    = "workflow.signal_consumed"
)

// ContinueSpec contains resolved successor options. ExecutionDeadlineAt is inherited.
type ContinueSpec struct {
	RunID        string        `json:"run_id"`
	WorkflowType string        `json:"workflow_type"`
	BuildID      string        `json:"build_id"`
	Queue        string        `json:"queue"`
	Input        []byte        `json:"input,omitempty"`
	RunTimeout   time.Duration `json:"run_timeout,omitempty"`
}

// RunMetadata identifies one run within an immutable chain without its payloads.
type RunMetadata struct {
	RetryAttempt   int64     `json:"retry_attempt,omitempty"`
	RunAvailableAt time.Time `json:"run_available_at,omitzero"`
	Key
	FirstRunID          string        `json:"first_run_id"`
	PreviousRunID       string        `json:"previous_run_id,omitempty"`
	RunNumber           int64         `json:"run_number"`
	FirstStartedAt      time.Time     `json:"first_started_at"`
	CreatedAt           time.Time     `json:"created_at"`
	RunTimeout          time.Duration `json:"run_timeout,omitempty"`
	RunDeadlineAt       time.Time     `json:"run_deadline_at,omitempty"`
	ExecutionDeadlineAt time.Time     `json:"execution_deadline_at,omitempty"`
}

// CarriedSignal retains the first acceptance coordinates through every handoff.
type CarriedSignal struct {
	Version  int       `json:"version"`
	Source   Key       `json:"source"`
	Sequence int64     `json:"sequence"`
	Time     time.Time `json:"time"`
	Signal   Signal    `json:"signal"`
}

// SignalConsumption binds one accepted message to a stable receive command.
type SignalConsumption struct {
	Version   int    `json:"version"`
	CommandID string `json:"command_id"`
	SignalID  string `json:"signal_id"`
}

// ContinuedRun is the source run's store-generated terminal event.
type ContinuedRun struct {
	Version int          `json:"version"`
	Next    ContinueSpec `json:"next"`
}

// RunStarted records the successor's immutable lineage in its own history.
type RunStarted struct {
	Version int         `json:"version"`
	Run     RunMetadata `json:"run"`
}

// RunMetadataOf copies the lineage used in successor and child-result history.
func RunMetadataOf(e Execution) RunMetadata {
	metadata := RunMetadata{Key: e.Key, FirstRunID: e.FirstRunID, PreviousRunID: e.PreviousRunID, RunNumber: e.RunNumber, FirstStartedAt: e.FirstStartedAt, CreatedAt: e.CreatedAt, RunTimeout: e.RunTimeout, RunDeadlineAt: e.RunDeadlineAt, ExecutionDeadlineAt: e.ExecutionDeadlineAt}
	if e.WorkflowAttempt() > 1 || !e.AvailableAt().Equal(e.CreatedAt) {
		metadata.RetryAttempt, metadata.RunAvailableAt = e.WorkflowAttempt(), e.AvailableAt()
	}
	return metadata
}

func (r RunMetadata) Validate() error {
	return ValidateRunMetadata(Execution{RetryAttempt: r.RetryAttempt, RunAvailableAt: r.RunAvailableAt, Key: r.Key, State: StateRunning, FirstRunID: r.FirstRunID, PreviousRunID: r.PreviousRunID, RunNumber: r.RunNumber, FirstStartedAt: r.FirstStartedAt, CreatedAt: r.CreatedAt, RunTimeout: r.RunTimeout, RunDeadlineAt: r.RunDeadlineAt, ExecutionDeadlineAt: r.ExecutionDeadlineAt})
}

func (s ContinueSpec) Validate() error {
	if !identifier(s.RunID) || !identifier(s.WorkflowType) || !identifier(s.BuildID) || !identifier(s.Queue) || len(s.Input) > 1<<20 {
		return fmt.Errorf("%w: invalid continuation options", ErrInvalid)
	}
	return validateExecutionTimeouts(StartRequest{RunTimeout: s.RunTimeout})
}

func validateContinuation(r CommitRequest) error {
	if r.Continuation == nil {
		if r.State == StateContinuedAsNew {
			return fmt.Errorf("%w: continuation requires a successor", ErrInvalid)
		}
		return nil
	}
	if r.State != StateContinuedAsNew || len(r.Output) != 0 || len(r.Events) >= 1000 || r.TaskUpdate != nil && r.TaskUpdate.Action != TaskComplete {
		return fmt.Errorf("%w: invalid continuation transition", ErrInvalid)
	}
	for _, event := range r.Events {
		switch event.Type {
		case EventWorkflowContinued, EventRunStarted, EventSignalReceived, EventSignalCarried,
			"workflow.completed", "workflow.failed", "workflow.cancelled", EventWorkflowTerminated, "workflow.timed_out":
			return fmt.Errorf("%w: continuation cannot inject store-owned events", ErrInvalid)
		}
	}
	return r.Continuation.Validate()
}

// ContinuationBatch is staged completely before either run becomes visible.
type ContinuationBatch struct {
	Spec       ContinueSpec
	RetryEvent *EventInput
	Execution  Execution
	History    []Event
	Terminal   EventInput
}

// PrepareContinuation builds a successor and carries accepted, unconsumed signals.
// The store owns the identity/run locks, uniqueness checks and atomic installation.
func PrepareContinuation(current Execution, task Task, r CommitRequest, history []Event, now time.Time) (*ContinuationBatch, error) {
	if r.Continuation == nil {
		return nil, nil
	}
	if err := validateContinuation(r); err != nil {
		return nil, err
	}
	if task.Kind != TaskWorkflow || task.LeaseKind != "" || current.NextRunID != "" || current.RunNumber == math.MaxInt64 || current.State != StateRunning {
		return nil, fmt.Errorf("%w: continuation requires an ordinary workflow grant", ErrInvalid)
	}
	if err := ValidateRunMetadata(current); err != nil {
		return nil, err
	}
	if err := CheckExecutionDeadline(current, now); err != nil {
		return nil, err
	}
	if r.Continuation.RunID == current.RunID || r.Continuation.RunID == current.FirstRunID || r.Continuation.RunID == current.PreviousRunID {
		return nil, ErrExists
	}
	if len(history) == 0 || len(history) > 100000 || int64(len(history)) != current.LastSequence {
		return nil, fmt.Errorf("%w: continuation history unavailable or exceeds limit", ErrInvalid)
	}
	for i, event := range history {
		if event.Sequence != int64(i+1) {
			return nil, ErrInvalid
		}
	}
	batch, err := prepareRunSuccessor(current, *r.Continuation, history, r.Events, now, 1, now)
	if err != nil {
		return nil, err
	}
	terminal, err := json.Marshal(ContinuedRun{Version: 1, Next: *r.Continuation})
	if err != nil {
		return nil, err
	}
	batch.Terminal = EventInput{Type: EventWorkflowContinued, Payload: terminal}
	return batch, nil
}

func prepareRunSuccessor(current Execution, spec ContinueSpec, history []Event, inputs []EventInput, now time.Time, attempt int64, available time.Time) (*ContinuationBatch, error) {
	if len(history) == 0 || len(history) > 100000 || int64(len(history)) != current.LastSequence || current.RunNumber == math.MaxInt64 {
		return nil, ErrInvalid
	}
	for i, event := range history {
		if event.Sequence != int64(i+1) {
			return nil, ErrInvalid
		}
	}
	carried, err := pendingContinuationSignals(current.Key, history, inputs)
	if err != nil {
		return nil, err
	}
	key := current.Key
	key.RunID = spec.RunID
	e, err := NewExecution(StartRequest{Key: key, RequestID: "successor", WorkflowType: spec.WorkflowType, BuildID: spec.BuildID, Queue: spec.Queue, Input: spec.Input, RunTimeout: spec.RunTimeout, RetryPolicy: current.RetryPolicy}, now)
	if err != nil {
		return nil, err
	}
	e.FirstRunID, e.PreviousRunID, e.RunNumber = current.FirstRunID, current.RunID, current.RunNumber+1
	e.FirstStartedAt, e.ExecutionDeadlineAt = current.FirstStartedAt, current.ExecutionDeadlineAt
	e.RetryAttempt, e.RunAvailableAt = attempt, available
	e.RunDeadlineAt, _, err = ResolveExecutionDeadlines(StartRequest{RunTimeout: e.RunTimeout}, available)
	if err != nil {
		return nil, err
	}
	e.LastSequence = int64(2 + len(carried))
	if metadataErr := ValidateRunMetadata(e); metadataErr != nil {
		return nil, metadataErr
	}
	lineage, err := json.Marshal(RunStarted{Version: 1, Run: RunMetadataOf(e)})
	if err != nil {
		return nil, err
	}
	b := &ContinuationBatch{Execution: e, Spec: spec, History: []Event{
		{EventInput: EventInput{Type: "execution.started", Payload: bytes.Clone(spec.Input)}, Sequence: 1, Time: e.CreatedAt},
		{EventInput: EventInput{Type: EventRunStarted, Payload: lineage}, Sequence: 2, Time: e.CreatedAt},
	}}
	for _, signal := range carried {
		payload, encodeErr := json.Marshal(signal)
		if encodeErr != nil {
			return nil, encodeErr
		}
		b.History = append(b.History, Event{EventInput: EventInput{Type: EventSignalCarried, Payload: payload}, Sequence: int64(len(b.History) + 1), Time: e.CreatedAt})
	}
	return b, nil
}

func (c CarriedSignal) Validate(target Key) error {
	if c.Version != 1 || c.Source.Validate() != nil || c.Source.Namespace != target.Namespace || c.Source.WorkflowID != target.WorkflowID || c.Source.RunID == target.RunID || c.Sequence < 1 || !validRunTime(c.Time) || c.Signal.Validate() != nil {
		return ErrInvalid
	}
	return nil
}

func decodeRunPayload(data []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return ErrInvalid
	}
	return nil
}

func pendingContinuationSignals(key Key, history []Event, inputs []EventInput) ([]CarriedSignal, error) {
	type signalCommand struct {
		Version int
		ID      string
		Kind    TaskKind
		Name    string
	}
	commands := make(map[string]signalCommand)
	consumed := make(map[string]bool)
	completed := make(map[string]bool)
	seen := make(map[string]CarriedSignal)
	queues := make(map[string][]string)
	offsets := make(map[string]int)
	var ordered []string
	visit := func(event Event) error {
		switch event.Type {
		case EventCancellationRequested, "workflow.cancellation_started":
			return fmt.Errorf("%w: accepted cancellation prevents continuation", ErrInvalid)
		case "workflow.command_scheduled":
			var command signalCommand
			if err := json.Unmarshal(event.Payload, &command); err != nil {
				return fmt.Errorf("%w: malformed command", ErrInvalid)
			}
			if command.Kind == "signal" {
				if command.Version != 1 || !identifier(command.ID) || !identifier(command.Name) {
					return ErrInvalid
				}
				if _, exists := commands[command.ID]; exists {
					return ErrInvalid
				}
				commands[command.ID] = command
			}
		case EventSignalReceived, EventSignalCarried:
			var signal CarriedSignal
			if event.Type == EventSignalReceived {
				signal = CarriedSignal{Version: 1, Source: key, Sequence: event.Sequence, Time: event.Time}
				if err := decodeRunPayload(event.Payload, &signal.Signal); err != nil {
					return err
				}
				if signal.Signal.Validate() != nil {
					return ErrInvalid
				}
			} else {
				if err := decodeRunPayload(event.Payload, &signal); err != nil {
					return err
				}
				if err := signal.Validate(key); err != nil {
					return err
				}
			}
			if _, exists := seen[signal.Signal.ID]; exists {
				return ErrInvalid
			}
			seen[signal.Signal.ID] = signal
			ordered = append(ordered, signal.Signal.ID)
			queues[signal.Signal.Name] = append(queues[signal.Signal.Name], signal.Signal.ID)
		case EventSignalConsumed:
			var value SignalConsumption
			if err := decodeRunPayload(event.Payload, &value); err != nil {
				return err
			}
			command, known := commands[value.CommandID]
			signal, accepted := seen[value.SignalID]
			if value.Version != 1 || !known || !accepted || consumed[value.SignalID] || completed[value.CommandID] || command.Name != signal.Signal.Name {
				return ErrInvalid
			}
			queue, offset := queues[command.Name], offsets[command.Name]
			if offset >= len(queue) || queue[offset] != value.SignalID {
				return ErrInvalid
			}
			offsets[command.Name]++
			consumed[value.SignalID], completed[value.CommandID] = true, true
		}
		return nil
	}
	for _, event := range history {
		if err := visit(event); err != nil {
			return nil, err
		}
	}
	for _, input := range inputs {
		if err := visit(Event{EventInput: input}); err != nil {
			return nil, err
		}
	}
	var pending []CarriedSignal
	size := 0
	for _, id := range ordered {
		if consumed[id] {
			continue
		}
		signal := seen[id]
		payload, err := json.Marshal(signal)
		if err != nil {
			return nil, err
		}
		size += len(payload)
		if len(pending) >= 998 || size > 4<<20 {
			return nil, fmt.Errorf("%w: pending signals exceed continuation carry limit", ErrInvalid)
		}
		pending = append(pending, signal)
	}
	return pending, nil
}
