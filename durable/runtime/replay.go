package runtime

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
	"unicode/utf8"

	"github.com/xraph/dispatch/durable"
)

type recordedOutcome struct {
	child                *ChildWorkflowError
	value                Outcome
	at                   time.Time
	sequence             int64
	cancellation         *Cancellation
	workflowCancellation *durable.ExecutionCancellation
}

type replayHistory struct {
	continuation          *Continuation
	children              map[string]recordedChildStart
	termination           *durable.ExecutionTermination
	timeout               *durable.ExecutionTimeout
	executionCancellation *recordedExecutionCancellation
	cancellationRequests  map[string]bool
	commands              []Command
	selections            map[string]Selection
	signals               map[string]recordedSignal
	signalQueues          map[string][]string
	signalOffsets         map[string]int
	outcomes              map[string]recordedOutcome
	attempts              map[string]recordedAttempt
	scheduled             map[string]time.Time
	terminal              durable.State
	output                []byte
	failure               *ApplicationError
}

// Evaluate replays a complete history snapshot through LastSequence. It produces
// a decision without writing state or running activities. Errors must never be
// converted into successful workflow transitions by a caller.
func Evaluate(execution durable.Execution, events []durable.Event, handler WorkflowFunc) (Decision, error) {
	decision, _, err := evaluateWorkflow(execution, events, handler)
	return decision, err
}

func evaluateWorkflow(execution durable.Execution, events []durable.Event, handler WorkflowFunc) (decision Decision, w *Workflow, evalErr error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			decision, w = Decision{}, nil
			evalErr = fmt.Errorf("%w: %v", ErrWorkflowPanic, recovered)
		}
	}()
	if handler == nil {
		return Decision{}, nil, fmt.Errorf("%w: workflow handler is required", durable.ErrInvalid)
	}
	history, err := parseHistory(execution, events)
	if err != nil {
		return Decision{}, nil, err
	}
	if history.termination != nil || history.timeout != nil {
		return evaluateForcedClosure(execution, events, handler)
	}
	if history.executionCancellation != nil {
		return evaluateExecutionCancellation(execution, events, history, handler, false)
	}
	w = &Workflow{execution: execution, key: execution.Key, buildID: execution.BuildID, now: execution.AvailableAt(), history: history, ids: make(map[string]bool)}
	output, handlerErr := invoke(w, handler, bytes.Clone(execution.Input))
	return finishEvaluation(w, output, handlerErr)
}

func checkWorkflowReplay(w *Workflow) error {
	if w.fault != nil {
		return w.fault
	}
	if w.history.continuation != nil && w.continuation == nil {
		return fmt.Errorf("%w: omitted continuation", ErrNondeterministic)
	}
	if w.cursor < len(w.history.commands) {
		return fmt.Errorf("%w: omitted command %d", ErrNondeterministic, w.cursor+1)
	}
	return nil
}

func finishEvaluation(w *Workflow, output []byte, handlerErr error) (Decision, *Workflow, error) {
	if err := checkWorkflowReplay(w); err != nil {
		return Decision{}, nil, err
	}
	history := w.history
	intent, isContinuation := handlerErr.(*continuationError) //nolint:errorlint // Only a direct SDK intent closes a run; wrapping or joining it is invalid.
	if w.continuation != nil && (!isContinuation || len(output) != 0) || isContinuation && (intent == nil || intent.workflow != w || w.continuation == nil) {
		return Decision{}, nil, fmt.Errorf("%w: continuation intent must be returned with no output", durable.ErrInvalid)
	}
	decision := Decision{Commands: w.commands, Signals: w.signals, Selections: w.selections, State: durable.StateCompleted, Output: bytes.Clone(output)}
	switch {
	case w.blocked:
		decision.State, decision.Output = durable.StateRunning, nil
	case w.cancelling && errors.Is(handlerErr, ErrWorkflowCancelled):
		decision.State, decision.Output = durable.StateCancelled, nil
		value := history.executionCancellation.value
		decision.Cancelled = &value
	case isContinuation:
		decision.State, decision.Output = durable.StateContinuedAsNew, nil
		if history.terminal == "" {
			decision.Continuation = w.continuation
		}
	case handlerErr != nil:
		decision.State, decision.Output = durable.StateFailed, nil
		var failure *ApplicationError
		if errors.As(handlerErr, &failure) {
			if failure == nil {
				return Decision{}, nil, fmt.Errorf("%w: nil application failure", durable.ErrInvalid)
			}
			copyFailure := *failure
			decision.Failure = &copyFailure
		} else {
			decision.Failure = &ApplicationError{Type: "application", Message: handlerErr.Error()}
		}
	}
	if decision.Failure != nil && !validFailure(decision.Failure) {
		return Decision{}, nil, fmt.Errorf("%w: invalid application failure", durable.ErrInvalid)
	}
	if history.terminal != "" && (decision.State != history.terminal || len(decision.Commands) != 0 || len(decision.Signals) != 0 || len(decision.Selections) != 0 ||
		!bytes.Equal(decision.Output, history.output) || !sameFailure(decision.Failure, history.failure)) {
		return Decision{}, nil, fmt.Errorf("%w: terminal result changed", ErrNondeterministic)
	}
	return decision, w, nil
}

func invoke(w *Workflow, handler WorkflowFunc, input []byte) (output []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if _, yielded := recovered.(flowControl); !yielded {
				w.fault = fmt.Errorf("%w: %v", ErrWorkflowPanic, recovered)
			}
		}
	}()
	return handler(w, input)
}

func parseHistory(execution durable.Execution, events []durable.Event) (replayHistory, error) {
	result := replayHistory{children: make(map[string]recordedChildStart), selections: make(map[string]Selection), signals: make(map[string]recordedSignal), signalQueues: make(map[string][]string), signalOffsets: make(map[string]int), outcomes: make(map[string]recordedOutcome), attempts: make(map[string]recordedAttempt), scheduled: make(map[string]time.Time)}
	if len(events) == 0 || len(events) > 100000 || execution.LastSequence != int64(len(events)) ||
		execution.CreatedAt.IsZero() || events[0].Type != EventStarted ||
		!events[0].Time.Equal(execution.CreatedAt) || !bytes.Equal(events[0].Payload, execution.Input) {
		return result, fmt.Errorf("%w: missing or inconsistent execution history", ErrHistory)
	}
	if execution.RunNumber > 1 && (len(events) < 2 || events[1].Type != durable.EventRunStarted) {
		return result, fmt.Errorf("%w: successor lineage missing", ErrHistory)
	}
	commands := make(map[string]Command)
	retryRecorded := false
	carryOpen, carryBytes, carryCount := false, 0, 0
	for i, event := range events {
		if event.Sequence != int64(i+1) || event.Time.IsZero() || result.terminal != "" {
			return result, fmt.Errorf("%w: invalid event sequence %d", ErrHistory, event.Sequence)
		}
		if err := validateExecutionCancellationPhase(result, event.Type); err != nil {
			return result, err
		}
		if event.Type != durable.EventRunStarted && event.Type != durable.EventSignalCarried {
			carryOpen = false
		}
		switch event.Type {
		case durable.EventWorkflowRetryScheduled:
			if err := validateWorkflowRetry(execution, events, i); err != nil {
				return result, err
			}
			retryRecorded = true
		case durable.EventRunStarted:
			if err := parseRunStarted(execution, event); err != nil {
				return result, err
			}
			carryOpen = true
		case durable.EventSignalCarried:
			carryCount++
			carryBytes += len(event.Payload)
			if !carryOpen || carryCount > 998 || carryBytes > 4<<20 {
				return result, fmt.Errorf("%w: invalid signal carry position or size", ErrHistory)
			}
			if err := parseCarriedSignal(&result, execution, event); err != nil {
				return result, err
			}
		case EventContinuationRequested, durable.EventWorkflowContinued:
			if err := parseContinuation(&result, execution, event); err != nil {
				return result, err
			}
		case EventStarted:
			if i != 0 {
				return result, fmt.Errorf("%w: repeated start event", ErrHistory)
			}
		case durable.EventCancellationRequested, EventCancellationStarted, EventWorkflowCancelled:
			if err := parseExecutionCancellation(&result, event); err != nil {
				return result, err
			}
		case durable.EventWorkflowTimedOut:
			if err := parseExecutionTimeout(&result, execution, event); err != nil {
				return result, err
			}
		case durable.EventWorkflowTerminated:
			if err := parseTermination(&result, event); err != nil {
				return result, err
			}
		case durable.EventChildStarted, EventChildStartFailed:
			if err := parseChildStart(&result, commands, event); err != nil {
				return result, err
			}
		case durable.EventChildCompleted, durable.EventChildCancellationAcknowledged:
			if err := parseChildDelivery(&result, commands, execution, event); err != nil {
				return result, err
			}
		case EventChildCancellationFailed:
			if err := parseChildCancellationFailure(&result, commands, event); err != nil {
				return result, err
			}
		case EventCommandScheduled:
			var command Command
			if err := decode(event.Payload, &command); err != nil {
				return result, err
			}
			_, duplicate := commands[command.ID]
			if command.validate() != nil || command.Index != int64(len(result.commands)+1) || duplicate {
				return result, fmt.Errorf("%w: invalid command at event %d", ErrHistory, event.Sequence)
			}
			if err := validateSelectionReferences(command, commands); err != nil {
				return result, err
			}
			if err := validateCancellationReference(command, commands); err != nil {
				return result, err
			}
			if err := validateChildReference(command, commands, execution.Key); err != nil {
				return result, err
			}
			commands[command.ID] = command
			result.scheduled[command.ID] = event.Time
			result.commands = append(result.commands, command)
		case EventFutureCancelled:
			if err := parseCancellation(&result, commands, event); err != nil {
				return result, err
			}
		case EventSelected:
			if err := parseSelection(&result, commands, event); err != nil {
				return result, err
			}
		case EventSignalReceived, EventSignalConsumed:
			if err := parseSignal(&result, commands, event); err != nil {
				return result, err
			}
		case EventActivityDeferred:
			if err := parseActivityHandoff(&result, commands, event); err != nil {
				return result, err
			}
		case EventActivityAttemptStarted, EventActivityAttemptFailed:
			if err := parseActivityAttempt(&result, commands, event); err != nil {
				return result, err
			}
		case EventActivityCompleted, EventTimerFired:
			if err := parseOutcome(&result, commands, event); err != nil {
				return result, err
			}
		case EventWorkflowWaiting:
			if len(event.Payload) != 0 {
				return result, fmt.Errorf("%w: invalid waiting event", ErrHistory)
			}
		case EventWorkflowCompleted:
			result.terminal, result.output = durable.StateCompleted, bytes.Clone(event.Payload)
		case EventWorkflowFailed:
			var failure ApplicationError
			if err := decode(event.Payload, &failure); err != nil {
				return result, err
			}
			if !validFailure(&failure) {
				return result, fmt.Errorf("%w: missing failure type", ErrHistory)
			}
			result.terminal, result.failure = durable.StateFailed, &failure
		default:
			return result, fmt.Errorf("%w: unknown event %q", ErrHistory, event.Type)
		}
	}
	if result.continuation != nil && result.terminal != durable.StateContinuedAsNew {
		return result, fmt.Errorf("%w: continuation has no terminal handoff", ErrHistory)
	}
	if execution.NextRunID != "" && result.terminal != durable.StateContinuedAsNew && !retryRecorded {
		return result, fmt.Errorf("%w: successor has no recorded handoff", ErrHistory)
	}
	for _, command := range result.commands {
		if command.Kind == CommandChild && result.terminal == "" {
			if _, started := result.children[command.ID]; !started {
				return result, fmt.Errorf("%w: child command has no start result", ErrHistory)
			}
		}
		if command.Kind == CommandCancel {
			if _, acknowledged := result.outcomes[command.ID]; !acknowledged {
				return result, fmt.Errorf("%w: cancellation command has no acknowledgment", ErrHistory)
			}
		}
		if command.Kind == CommandSelect && command.Index < int64(len(result.commands)) && !interruptedSelection(result, command) {
			if _, chosen := result.selections[command.ID]; !chosen {
				return result, fmt.Errorf("%w: pending selection precedes later commands", ErrHistory)
			}
		}
	}
	if result.terminal != "" && result.terminal != durable.StateTerminated && result.terminal != durable.StateTimedOut && result.executionCancellation != nil && result.executionCancellation.started == nil {
		return result, fmt.Errorf("%w: terminal result before cancellation fencing", ErrHistory)
	}
	for id, attempt := range result.attempts {
		if _, done := result.outcomes[id]; attempt.failed && attempt.value.RetryAfter == 0 && !done {
			return result, fmt.Errorf("%w: final attempt failure has no outcome", ErrHistory)
		}
	}
	if (result.terminal == "" && execution.State != durable.StateRunning) ||
		(result.terminal != "" && (result.terminal != execution.State || !bytes.Equal(result.output, execution.Output))) {
		return result, fmt.Errorf("%w: terminal projection mismatch", ErrHistory)
	}
	return result, nil
}

func parseOutcome(history *replayHistory, commands map[string]Command, event durable.Event) error {
	var outcome Outcome
	if err := decode(event.Payload, &outcome); err != nil {
		return err
	}
	command, exists := commands[outcome.CommandID]
	_, duplicate := history.outcomes[outcome.CommandID]
	if !exists || duplicate || (outcome.Failure != nil && (len(outcome.Output) != 0 || !validFailure(outcome.Failure))) {
		return fmt.Errorf("%w: invalid outcome at event %d", ErrHistory, event.Sequence)
	}
	if (event.Type == EventActivityCompleted && command.Kind != durable.TaskActivity) ||
		(event.Type == EventTimerFired && (command.Kind != durable.TaskTimer || outcome.Version != 1 || outcome.Attempt != 0 || outcome.Timeout != "" || outcome.Heartbeat != nil || len(outcome.Output) != 0 || outcome.Failure != nil || event.Time.Before(command.Deadline))) {
		return fmt.Errorf("%w: outcome does not match scheduled command", ErrHistory)
	}
	if command.Kind == durable.TaskActivity {
		if err := validateActivityOutcome(history, command, outcome, event.Time); err != nil {
			return err
		}
	}
	history.outcomes[outcome.CommandID] = recordedOutcome{value: outcome, at: event.Time, sequence: event.Sequence}
	return nil
}

func decode(data []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("%w: %w", ErrHistory, err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing JSON content", ErrHistory)
	}
	return nil
}

func sameFailure(a, b *ApplicationError) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func validFailure(failure *ApplicationError) bool {
	return validID(failure.Type) && utf8.ValidString(failure.Message)
}
