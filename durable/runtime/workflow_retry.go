package runtime

import (
	"fmt"

	"github.com/xraph/dispatch/durable"
)

func validateWorkflowRetry(execution durable.Execution, events []durable.Event, index int) error {
	event := events[index]
	var saved durable.WorkflowRetryScheduled
	if err := decode(event.Payload, &saved); err != nil {
		return err
	}
	if index != len(events)-2 || execution.NextRunID == "" || execution.RetryPolicy == nil ||
		(execution.State != durable.StateFailed && execution.State != durable.StateTimedOut) ||
		!event.Time.Equal(events[index+1].Time) || !event.Time.Equal(execution.UpdatedAt) {
		return fmt.Errorf("%w: retry link is not paired with its terminal outcome", ErrHistory)
	}
	// Rebuild the store-owned handoff from its immutable policy, history and
	// closure clock. The queue does not affect the recorded run coordinates.
	prefix := execution
	prefix.State, prefix.Output, prefix.NextRunID, prefix.LastSequence = durable.StateRunning, nil, "", int64(index)
	terminal := events[index+1]
	batch, err := durable.PrepareWorkflowRetry(prefix, execution.State,
		[]durable.EventInput{{Type: terminal.Type, Payload: terminal.Payload}}, events[:index], "replay", event.Time)
	if err != nil || batch == nil || batch.RetryEvent == nil {
		return fmt.Errorf("%w: retry is not allowed by the saved policy and outcome", ErrHistory)
	}
	var expected durable.WorkflowRetryScheduled
	if err := decode(batch.RetryEvent.Payload, &expected); err != nil {
		return err
	}
	if saved.Version != expected.Version || saved.Delay != expected.Delay || saved.FailureType != expected.FailureType ||
		!sameRunMetadata(saved.Next, expected.Next) || execution.NextRunID != expected.Next.RunID {
		return fmt.Errorf("%w: retry differs from its saved policy or source run", ErrHistory)
	}
	return nil
}
