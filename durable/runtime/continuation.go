package runtime

import (
	"bytes"
	"fmt"
	"time"

	"github.com/xraph/dispatch/durable"
)

// EventContinuationRequested captures the SDK decision before the store closes the run.
const EventContinuationRequested = "workflow.continuation_requested"

// ContinueOptions selects the successor's routing and run timeout. Empty strings
// inherit the source. A nil timeout inherits; a pointer to zero removes the run limit.
// The chain's absolute execution deadline never changes.
type ContinueOptions struct {
	WorkflowType string         `json:"workflow_type,omitempty"`
	BuildID      string         `json:"build_id,omitempty"`
	Queue        string         `json:"queue,omitempty"`
	RunTimeout   *time.Duration `json:"run_timeout,omitempty"`
}

// Continuation records both caller options and resolved successor values.
// Queue is resolved from the source task when the caller leaves it empty.
type Continuation struct {
	Version      int                  `json:"version"`
	CommandCount int64                `json:"command_count"`
	Options      ContinueOptions      `json:"options"`
	Next         durable.ContinueSpec `json:"next"`
}

type continuationError struct{ workflow *Workflow }

func (*continuationError) Error() string { return "durable runtime: continue as new" }

// ContinueAsNew closes this run and starts its successor atomically when the
// workflow returns this error. Return nil output and make no later SDK calls.
// Unread signals follow the chain; pending tasks and child futures do not.
func (w *Workflow) ContinueAsNew(input []byte, options ContinueOptions) error {
	w.checkOperation()
	if w.freezeNormal {
		w.blocked = true
		panic(flowControl{})
	}
	if w.cancelling {
		w.stop(fmt.Errorf("%w: cancellation cleanup cannot continue as new", durable.ErrInvalid))
	}
	options = cloneContinueOptions(options)
	queue := ""
	if w.history.continuation != nil {
		queue = w.history.continuation.Next.Queue
	}
	next, err := resolveContinuation(w.execution, input, options, queue)
	if err != nil {
		w.stop(err)
	}
	value := &Continuation{Version: 1, CommandCount: int64(w.cursor), Options: options, Next: next}
	if err := validateContinuationOptions(*value); err != nil {
		w.stop(err)
	}
	if w.history.continuation != nil {
		if !sameContinuation(*value, *w.history.continuation) {
			w.stop(fmt.Errorf("%w: continuation changed", ErrNondeterministic))
		}
	} else {
		w.checkEventCapacity()
	}
	w.continuation = value
	return &continuationError{workflow: w}
}

func cloneContinueOptions(options ContinueOptions) ContinueOptions {
	if options.RunTimeout != nil {
		value := *options.RunTimeout
		options.RunTimeout = &value
	}
	return options
}

func resolveContinuation(e durable.Execution, input []byte, options ContinueOptions, queue string) (durable.ContinueSpec, error) {
	identity, err := durable.Fingerprint("continue-as-new", e.Key)
	if err != nil {
		return durable.ContinueSpec{}, err
	}
	next := durable.ContinueSpec{RunID: "continue-" + identity, WorkflowType: e.WorkflowType, BuildID: e.BuildID, Queue: queue, RunTimeout: e.RunTimeout, Input: bytes.Clone(input)}
	if options.WorkflowType != "" {
		next.WorkflowType = options.WorkflowType
	}
	if options.BuildID != "" {
		next.BuildID = options.BuildID
	}
	if options.Queue != "" {
		next.Queue = options.Queue
	}
	if options.RunTimeout != nil {
		next.RunTimeout = *options.RunTimeout
	}
	return next, nil
}

func validateContinuationOptions(value Continuation) error {
	if value.Version != 1 || value.CommandCount < 0 || !validID(value.Next.WorkflowType) || value.Next.Queue != "" && !validID(value.Next.Queue) {
		return fmt.Errorf("%w: invalid continuation version, command count or runtime routing", durable.ErrInvalid)
	}
	next := value.Next
	if next.Queue == "" {
		next.Queue = "inherited"
	}
	return next.Validate()
}

func sameContinueSpec(a, b durable.ContinueSpec) bool {
	return a.RunID == b.RunID && a.WorkflowType == b.WorkflowType && a.BuildID == b.BuildID && a.Queue == b.Queue && a.RunTimeout == b.RunTimeout && bytes.Equal(a.Input, b.Input)
}

func sameContinuation(a, b Continuation) bool {
	x, y := a.Options, b.Options
	return a.Version == b.Version && a.CommandCount == b.CommandCount && sameContinueSpec(a.Next, b.Next) &&
		x.WorkflowType == y.WorkflowType && x.BuildID == y.BuildID && x.Queue == y.Queue &&
		(x.RunTimeout == nil && y.RunTimeout == nil || x.RunTimeout != nil && y.RunTimeout != nil && *x.RunTimeout == *y.RunTimeout)
}

func parseContinuation(history *replayHistory, execution durable.Execution, event durable.Event) error {
	if event.Type == EventContinuationRequested {
		var value Continuation
		if err := decode(event.Payload, &value); err != nil {
			return err
		}
		expected, err := resolveContinuation(execution, value.Next.Input, value.Options, value.Next.Queue)
		if err != nil || history.continuation != nil || history.executionCancellation != nil || validateContinuationOptions(value) != nil || value.CommandCount != int64(len(history.commands)) || value.Next.Validate() != nil || !sameContinueSpec(value.Next, expected) || event.Sequence != execution.LastSequence-1 {
			return fmt.Errorf("%w: invalid continuation request", ErrHistory)
		}
		history.continuation = &value
		return nil
	}
	var terminal durable.ContinuedRun
	if err := decode(event.Payload, &terminal); err != nil {
		return err
	}
	if terminal.Version != 1 || history.continuation == nil || !sameContinueSpec(terminal.Next, history.continuation.Next) || execution.NextRunID != terminal.Next.RunID || durable.ValidateRunMetadata(execution) != nil {
		return fmt.Errorf("%w: continuation terminal differs from captured request", ErrHistory)
	}
	history.terminal = durable.StateContinuedAsNew
	return nil
}

func parseRunStarted(execution durable.Execution, event durable.Event) error {
	var start durable.RunStarted
	if err := decode(event.Payload, &start); err != nil {
		return err
	}
	if start.Version != 1 || event.Sequence != 2 || !event.Time.Equal(execution.CreatedAt) || start.Run.RunNumber < 2 || start.Run.Validate() != nil || !sameRunMetadata(start.Run, durable.RunMetadataOf(execution)) {
		return fmt.Errorf("%w: invalid successor lineage", ErrHistory)
	}
	return nil
}

func sameRunMetadata(a, b durable.RunMetadata) bool {
	return a.Key == b.Key && a.FirstRunID == b.FirstRunID && a.PreviousRunID == b.PreviousRunID && a.RunNumber == b.RunNumber && a.RunTimeout == b.RunTimeout &&
		a.CreatedAt.Equal(b.CreatedAt) && a.FirstStartedAt.Equal(b.FirstStartedAt) && a.RunDeadlineAt.Equal(b.RunDeadlineAt) && a.ExecutionDeadlineAt.Equal(b.ExecutionDeadlineAt)
}

func parseCarriedSignal(history *replayHistory, execution durable.Execution, event durable.Event) error {
	var carried durable.CarriedSignal
	if err := decode(event.Payload, &carried); err != nil {
		return err
	}
	_, duplicate := history.signals[carried.Signal.ID]
	if carried.Validate(execution.Key) != nil || duplicate || !event.Time.Equal(execution.CreatedAt) || carried.Time.After(event.Time) || carried.Time.Before(execution.FirstStartedAt) || carried.Sequence > 100000 {
		return fmt.Errorf("%w: invalid carried signal", ErrHistory)
	}
	history.signals[carried.Signal.ID] = recordedSignal{value: carried.Signal, at: carried.Time, sequence: event.Sequence}
	history.signalQueues[carried.Signal.Name] = append(history.signalQueues[carried.Signal.Name], carried.Signal.ID)
	return nil
}
