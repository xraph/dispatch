package runtime

import (
	"fmt"
	"time"

	"github.com/xraph/dispatch/durable"
)

// TaskExecutionTimeout polls expired runs independently of pinned workflow builds.
const TaskExecutionTimeout durable.TaskKind = "execution_timeout"

func parseExecutionTimeout(history *replayHistory, execution durable.Execution, event durable.Event) error {
	var timeout durable.ExecutionTimeout
	if err := decode(event.Payload, &timeout); err != nil {
		return err
	}
	kind, deadline := execution.Deadline()
	if timeout.Validate() != nil || timeout.Kind != kind || !timeout.DeadlineAt.Equal(deadline) || !deadline.After(execution.CreatedAt) || event.Time.Before(deadline) {
		return fmt.Errorf("%w: workflow timeout does not match execution deadline", ErrHistory)
	}
	history.timeout = &timeout
	history.terminal = durable.StateTimedOut
	return nil
}

func decodeChildExecutionTimeout(event durable.EventInput, failure *ChildWorkflowError) error {
	if event.Type != durable.EventWorkflowTimedOut {
		return fmt.Errorf("%w: invalid child timeout event", ErrHistory)
	}
	var timeout durable.ExecutionTimeout
	if decode(event.Payload, &timeout) == nil && timeout.Validate() == nil {
		failure.Timeout = &timeout
		return nil
	}
	// Existing histories may carry the original application-error timeout payload.
	// Both decoders reject unknown fields, so mixed or malformed payloads fail.
	var legacy ApplicationError
	if decode(event.Payload, &legacy) != nil || !validFailure(&legacy) {
		return fmt.Errorf("%w: invalid child timeout payload", ErrHistory)
	}
	failure.Failure = &legacy
	return nil
}

func validateChildExecutionTimeout(timeout durable.ExecutionTimeout, command Command, started, received time.Time, final *durable.RunMetadata) error {
	run, execution, err := durable.ResolveExecutionDeadlines(durable.StartRequest{RunTimeout: command.Child.RunTimeout, ExecutionTimeout: command.Child.ExecutionTimeout}, started)
	if err != nil {
		return fmt.Errorf("%w: invalid child deadline options", ErrHistory)
	}
	if final != nil {
		run, execution = final.RunDeadlineAt, final.ExecutionDeadlineAt
	}
	kind, deadline := (durable.Execution{RunDeadlineAt: run, ExecutionDeadlineAt: execution}).Deadline()
	if timeout.Kind != kind || !timeout.DeadlineAt.Equal(deadline) || received.Before(deadline) {
		return fmt.Errorf("%w: child timeout does not match recorded start", ErrHistory)
	}
	return nil
}

func validateChildFinalRun(final durable.RunMetadata, command Command, started, received time.Time) error {
	_, execution, err := durable.ResolveExecutionDeadlines(durable.StartRequest{ExecutionTimeout: command.Child.ExecutionTimeout}, started)
	if err != nil || final.Validate() != nil || final.RunNumber < 2 || final.FirstRunID != command.Child.Key.RunID || final.Namespace != command.Child.Key.Namespace || final.WorkflowID != command.Child.Key.WorkflowID || !final.FirstStartedAt.Equal(started) || !final.ExecutionDeadlineAt.Equal(execution) || final.CreatedAt.After(received) {
		return fmt.Errorf("%w: child final run differs from original invocation", ErrHistory)
	}
	return nil
}
