package runtime

import (
	"fmt"
	"time"

	"github.com/xraph/dispatch/durable"
)

func validateCancellationReference(command Command, commands map[string]Command) error {
	if command.Kind != CommandCancel {
		return nil
	}
	target, exists := commands[command.TargetID]
	if !exists || target.Index >= command.Index || !cancellableKind(target.Kind) {
		return fmt.Errorf("%w: cancellation target must be a prior activity, timer or receive", ErrHistory)
	}
	return nil
}

func parseCancellation(history *replayHistory, commands map[string]Command, event durable.Event) error {
	var cancellation Cancellation
	if err := decode(event.Payload, &cancellation); err != nil {
		return err
	}
	command, known := commands[cancellation.CommandID]
	target, targetKnown := commands[cancellation.TargetID]
	_, acknowledged := history.outcomes[cancellation.CommandID]
	_, resolved := history.outcomes[cancellation.TargetID]
	if cancellation.Version != 1 || !known || command.Kind != CommandCancel || command.TargetID != cancellation.TargetID || !targetKnown || acknowledged || cancellation.Cancelled == resolved {
		return fmt.Errorf("%w: invalid cancellation disposition", ErrHistory)
	}
	if cancellation.Cancelled {
		if err := validateCancellationProgress(history, target, cancellation, event.Time); err != nil {
			return err
		}
		history.outcomes[target.ID] = recordedOutcome{cancellation: &cancellation, at: event.Time, sequence: event.Sequence}
	} else if cancellation.Attempt != 0 || cancellation.Heartbeat != nil {
		return fmt.Errorf("%w: metadata on cancellation of a resolved target", ErrHistory)
	}
	history.outcomes[command.ID] = recordedOutcome{value: Outcome{Version: 1, CommandID: command.ID}, at: event.Time, sequence: event.Sequence}
	return nil
}

func validateCancellationProgress(history *replayHistory, target Command, c Cancellation, at time.Time) error {
	if target.Kind != durable.TaskActivity || target.Version == 1 {
		if c.Attempt != 0 || c.Heartbeat != nil {
			return fmt.Errorf("%w: unexpected cancellation activity metadata", ErrHistory)
		}
		return nil
	}
	prior := history.attempts[target.ID]
	if c.Attempt != prior.value.Attempt || at.Before(prior.at) {
		return fmt.Errorf("%w: cancellation attempt mismatch", ErrHistory)
	}
	if prior.value.Attempt == 0 {
		if c.Heartbeat != nil {
			return fmt.Errorf("%w: checkpoint before activity activation", ErrHistory)
		}
		return nil
	}
	if prior.failed {
		if prior.value.RetryAfter == 0 || !sameHeartbeat(c.Heartbeat, prior.value.Heartbeat) {
			return fmt.Errorf("%w: cancellation changed failed attempt progress", ErrHistory)
		}
		return nil
	}
	return validateHeartbeat(prior, c.Heartbeat, at)
}
