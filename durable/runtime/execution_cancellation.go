package runtime

import (
	"errors"
	"fmt"

	"github.com/xraph/dispatch/durable"
)

const (
	// EventCancellationStarted records the atomic fence before cleanup can run.
	EventCancellationStarted = "workflow.cancellation_started"
	// EventWorkflowCancelled records a terminal cancellation after cleanup.
	EventWorkflowCancelled = "workflow.cancelled"
)

// ErrWorkflowCancelled distinguishes run cancellation from individual futures.
var ErrWorkflowCancelled = errors.New("durable runtime: workflow cancelled")

// WorkflowCancelledError identifies the first accepted cancellation request.
type WorkflowCancelledError struct {
	RequestID string
	Reason    string
}

func (e *WorkflowCancelledError) Error() string {
	return fmt.Sprintf("%s: %s", ErrWorkflowCancelled, e.Reason)
}
func (e *WorkflowCancelledError) Unwrap() error { return ErrWorkflowCancelled }

// CancellationStart binds task fencing to the saved normal-command boundary.
type CancellationStart struct {
	Version      int    `json:"version"`
	RequestID    string `json:"request_id"`
	CommandCount int64  `json:"command_count"`
}

// WorkflowCancellationFunc replays cleanup after the pending-task fence commits.
// Return ErrWorkflowCancelled to close cancelled, nil to complete, or another
// error to fail. Use durable commands for effects and explicit control flow.
type WorkflowCancellationFunc func(*Workflow, durable.ExecutionCancellation) ([]byte, error)

// SetCancellationHandler registers cleanup on this evaluation. Register before
// the first command to handle cancellation accepted before the initial decision.
// Later registrations are available only when frozen normal replay reaches them.
func (w *Workflow) SetCancellationHandler(handler WorkflowCancellationFunc) {
	w.checkOperation()
	if handler == nil || w.cancellationHandler != nil || w.cancelling {
		w.stop(fmt.Errorf("%w: invalid, duplicate or nested cancellation handler", durable.ErrInvalid))
	}
	w.cancellationHandler = handler
}

func evaluateExecutionCancellation(execution durable.Execution, events []durable.Event, history replayHistory, handler WorkflowFunc, frozen bool) (Decision, *Workflow, error) {
	cancellation := history.executionCancellation
	prefix := execution
	prefix.State, prefix.Output, prefix.LastSequence = durable.StateRunning, nil, cancellation.sequence-1
	normal, err := parseHistory(prefix, events[:prefix.LastSequence])
	if err != nil {
		return Decision{}, nil, err
	}
	w := &Workflow{key: execution.Key, buildID: execution.BuildID, now: execution.CreatedAt, history: normal, ids: make(map[string]bool), freezeNormal: true}
	// The immutable prefix reconstructs captured state, including query/cleanup
	// closures, but publishes none of the normal path's speculative decisions.
	_, _ = invoke(w, handler, append([]byte(nil), execution.Input...)) //nolint:errcheck // Cancellation replaces the normal application result; replay faults are checked below.
	if replayErr := checkWorkflowReplay(w); replayErr != nil {
		return Decision{}, nil, replayErr
	}
	w.freezeNormal, w.blocked = frozen, false
	w.commands, w.signals, w.selections = nil, nil, nil
	w.acknowledgmentCount = 0
	w.history = history
	if cancellation.started == nil {
		start := &CancellationStart{Version: 1, RequestID: cancellation.value.RequestID, CommandCount: int64(cancellation.commandCount)}
		return Decision{State: durable.StateRunning, CancellationStart: start}, w, nil
	}
	w.cancelling = true
	if cancellation.started.Time.After(w.now) {
		w.now = cancellation.started.Time
	}
	var output []byte
	handlerErr := error(&WorkflowCancelledError{RequestID: cancellation.value.RequestID, Reason: cancellation.value.Reason})
	if w.cancellationHandler != nil {
		output, handlerErr = invoke(w, func(cleanup *Workflow, _ []byte) ([]byte, error) {
			return w.cancellationHandler(cleanup, cancellation.value)
		}, nil)
	}
	return finishEvaluation(w, output, handlerErr)
}
