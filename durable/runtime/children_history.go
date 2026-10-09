package runtime

import (
	"bytes"
	"fmt"
	"time"

	"github.com/xraph/dispatch/durable"
)

type recordedChildStart struct {
	value    durable.ChildStarted
	failure  *ChildWorkflowError
	at       time.Time
	sequence int64
}

func parseChildStart(history *replayHistory, commands map[string]Command, event durable.Event) error {
	var started durable.ChildStarted
	var failed ChildStartFailure
	id := ""
	if event.Type == durable.EventChildStarted {
		if err := decode(event.Payload, &started); err != nil {
			return err
		}
		id = started.CommandID
	} else {
		if err := decode(event.Payload, &failed); err != nil {
			return err
		}
		id = failed.CommandID
	}
	command, known := commands[id]
	_, duplicate := history.children[id]
	if !known || command.Kind != CommandChild || duplicate || event.Time.Before(history.scheduled[id]) {
		return fmt.Errorf("%w: invalid child start", ErrHistory)
	}
	record := recordedChildStart{value: started, at: event.Time, sequence: event.Sequence}
	if event.Type == durable.EventChildStarted {
		if started.Version != 1 || started.Child != command.Child.Key || started.WorkflowType != command.Name || started.BuildID != command.Child.BuildID || started.ParentClosePolicy != command.Child.ParentClosePolicy || !validID(started.Queue) || command.Queue != "" && command.Queue != started.Queue {
			return fmt.Errorf("%w: child start differs from command", ErrHistory)
		}
	} else {
		if failed.Version != 1 || failed.Child != command.Child.Key || failed.Failure == nil || !validFailure(failed.Failure) || failed.Failure.Type != FailureChildStartConflict || !failed.Failure.NonRetryable {
			return fmt.Errorf("%w: invalid child start failure", ErrHistory)
		}
		record.failure = &ChildWorkflowError{Child: failed.Child, Failure: failed.Failure}
		history.outcomes[id] = recordedOutcome{child: record.failure, at: event.Time, sequence: event.Sequence}
	}
	history.children[id] = record
	return nil
}

func parseChildDelivery(history *replayHistory, commands map[string]Command, parent durable.Execution, event durable.Event) error {
	var message durable.ChildMessage
	if err := decode(event.Payload, &message); err != nil {
		return err
	}
	kind, id := durable.ChildDeliveryResult, message.CommandID
	if event.Type == durable.EventChildCancellationAcknowledged {
		kind, id = durable.ChildDeliveryCancelAck, message.CancellationID
	}
	d := durable.ChildDelivery{Source: message.Child, Target: parent.Key, ID: "event", Kind: kind, TargetBuildID: parent.BuildID, TargetQueue: "parent", Message: message}
	start, started := history.children[message.CommandID]
	command, known := commands[id]
	_, duplicate := history.outcomes[id]
	if d.Validate() != nil || message.Parent != parent.Key || !started || start.failure != nil || start.value.Child != message.Child || start.value.ParentClosePolicy != message.Policy || !known || duplicate || event.Time.Before(history.scheduled[id]) {
		return fmt.Errorf("%w: invalid child delivery", ErrHistory)
	}
	outcome := recordedOutcome{at: event.Time, sequence: event.Sequence}
	if kind == durable.ChildDeliveryCancelAck {
		if command.Kind != CommandCancelChild || command.TargetID != message.CommandID {
			return fmt.Errorf("%w: child cancellation acknowledgment target changed", ErrHistory)
		}
	} else {
		if command.Kind != CommandChild {
			return fmt.Errorf("%w: child result command changed", ErrHistory)
		}
		failure, err := childTerminalError(message)
		if err != nil {
			return err
		}
		if failure != nil && failure.Timeout != nil {
			if err := validateChildExecutionTimeout(*failure.Timeout, command, start.at, event.Time); err != nil {
				return err
			}
		}
		outcome.child = failure
		outcome.value.Output = bytes.Clone(message.Output)
	}
	history.outcomes[id] = outcome
	return nil
}

func childTerminalError(message durable.ChildMessage) (*ChildWorkflowError, error) {
	failure := &ChildWorkflowError{Child: message.Child, State: message.State}
	event := message.CloseEvent
	if message.State != durable.StateCompleted && len(message.Output) != 0 {
		return nil, fmt.Errorf("%w: failed child carries output", ErrHistory)
	}
	switch message.State {
	case durable.StateCompleted:
		if event.Type != EventWorkflowCompleted || !bytes.Equal(event.Payload, message.Output) {
			return nil, fmt.Errorf("%w: child completion output changed", ErrHistory)
		}
		return nil, nil
	case durable.StateTimedOut:
		if err := decodeChildExecutionTimeout(event, failure); err != nil {
			return nil, err
		}
	case durable.StateFailed:
		expected := EventWorkflowFailed
		var application ApplicationError
		if event.Type != expected || decode(event.Payload, &application) != nil || !validFailure(&application) {
			return nil, fmt.Errorf("%w: invalid child failure", ErrHistory)
		}
		failure.Failure = &application
	case durable.StateCancelled:
		var cancellation durable.ExecutionCancellation
		if event.Type != EventWorkflowCancelled || decode(event.Payload, &cancellation) != nil || cancellation.Validate() != nil {
			return nil, fmt.Errorf("%w: invalid child cancellation", ErrHistory)
		}
	case durable.StateTerminated:
		var termination durable.ExecutionTermination
		if event.Type != durable.EventWorkflowTerminated || decode(event.Payload, &termination) != nil || termination.Validate() != nil {
			return nil, fmt.Errorf("%w: invalid child termination", ErrHistory)
		}
	default:
		return nil, fmt.Errorf("%w: unsupported child terminal state %q", ErrHistory, message.State)
	}
	return failure, nil
}

func parseChildCancellationFailure(history *replayHistory, commands map[string]Command, event durable.Event) error {
	var failed ChildCancellationFailure
	if err := decode(event.Payload, &failed); err != nil {
		return err
	}
	command, known := commands[failed.CommandID]
	start, started := history.children[failed.TargetID]
	_, duplicate := history.outcomes[failed.CommandID]
	if failed.Version != 1 || !known || command.Kind != CommandCancelChild || command.TargetID != failed.TargetID || !started || start.failure == nil || start.failure.Child != failed.Child || duplicate || event.Time.Before(history.scheduled[failed.CommandID]) {
		return fmt.Errorf("%w: invalid child cancellation failure", ErrHistory)
	}
	history.outcomes[failed.CommandID] = recordedOutcome{child: cloneChildError(start.failure), at: event.Time, sequence: event.Sequence}
	return nil
}
