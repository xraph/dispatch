package runtime

import (
	"fmt"
	"time"

	"github.com/xraph/dispatch/durable"
)

type recordedExecutionCancellation struct {
	value        durable.ExecutionCancellation
	at           time.Time
	sequence     int64
	commandCount int
	started      *durable.Event
}

func validateExecutionCancellationPhase(history replayHistory, eventType string) error {
	if history.executionCancellation == nil || history.executionCancellation.started != nil {
		return nil
	}
	switch eventType {
	case EventCommandScheduled, EventSelected, EventSignalConsumed, EventFutureCancelled,
		EventWorkflowWaiting, EventWorkflowCompleted, EventWorkflowFailed, durable.EventChildStarted, EventChildStartFailed, EventChildCancellationFailed:
		return fmt.Errorf("%w: normal workflow decision after cancellation acceptance", ErrHistory)
	default:
		return nil
	}
}

func parseExecutionCancellation(history *replayHistory, event durable.Event) error {
	if event.Type == durable.EventCancellationRequested {
		var request durable.ExecutionCancellation
		if err := decode(event.Payload, &request); err != nil {
			return err
		}
		if request.Validate() != nil || history.cancellationRequests[request.RequestID] {
			return fmt.Errorf("%w: invalid or duplicate workflow cancellation request", ErrHistory)
		}
		if history.cancellationRequests == nil {
			history.cancellationRequests = make(map[string]bool)
		}
		history.cancellationRequests[request.RequestID] = true
		if history.executionCancellation == nil {
			history.executionCancellation = &recordedExecutionCancellation{value: request, at: event.Time, sequence: event.Sequence, commandCount: len(history.commands)}
		}
		return nil
	}
	first := history.executionCancellation
	if first == nil {
		return fmt.Errorf("%w: cancellation phase without a request", ErrHistory)
	}
	if event.Time.Before(first.at) {
		return fmt.Errorf("%w: cancellation phase precedes request time", ErrHistory)
	}
	if event.Type == EventCancellationStarted {
		var start CancellationStart
		if err := decode(event.Payload, &start); err != nil {
			return err
		}
		if first.started != nil || start.Version != 1 || start.RequestID != first.value.RequestID || start.CommandCount != int64(first.commandCount) || len(history.commands) != first.commandCount {
			return fmt.Errorf("%w: invalid cancellation fencing boundary", ErrHistory)
		}
		for id, attempt := range history.attempts {
			if _, resolved := history.outcomes[id]; attempt.failed && attempt.value.RetryAfter == 0 && !resolved {
				return fmt.Errorf("%w: cancellation cannot replace a missing final activity outcome", ErrHistory)
			}
		}
		first.started = &event
		for _, command := range history.commands {
			if _, resolved := history.outcomes[command.ID]; !resolved && cancellableKind(command.Kind) {
				history.outcomes[command.ID] = recordedOutcome{workflowCancellation: &first.value, at: event.Time, sequence: event.Sequence}
			}
		}
		return nil
	}
	var terminal durable.ExecutionCancellation
	if err := decode(event.Payload, &terminal); err != nil {
		return err
	}
	if first.started == nil || terminal != first.value || event.Time.Before(first.started.Time) {
		return fmt.Errorf("%w: invalid cancelled terminal event", ErrHistory)
	}
	history.terminal = durable.StateCancelled
	return nil
}

func interruptedSelection(history replayHistory, command Command) bool {
	c := history.executionCancellation
	return c != nil && c.started != nil && command.Index <= int64(c.commandCount)
}
