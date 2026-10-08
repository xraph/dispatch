package runtime

import (
	"fmt"
	"time"

	"github.com/xraph/dispatch/durable"
)

// ActivityAttempt records a logical attempt and the ownership epoch that started
// it. A failed record also stores the failure and chosen retry delay; zero is final.
type ActivityAttempt struct {
	Version    int                 `json:"version"`
	CommandID  string              `json:"command_id"`
	Attempt    int64               `json:"attempt"`
	Epoch      int64               `json:"epoch"`
	Failure    *ApplicationError   `json:"failure,omitempty"`
	RetryAfter time.Duration       `json:"retry_after,omitempty"`
	Timeout    ActivityTimeoutKind `json:"timeout,omitempty"`
}

type recordedAttempt struct {
	value   ActivityAttempt
	at      time.Time
	retryAt time.Time
	failed  bool
}

func parseActivityAttempt(history *replayHistory, commands map[string]Command, event durable.Event) error {
	var attempt ActivityAttempt
	if err := decode(event.Payload, &attempt); err != nil {
		return err
	}
	command, exists := commands[attempt.CommandID]
	prior := history.attempts[attempt.CommandID]
	_, completed := history.outcomes[attempt.CommandID]
	if !exists || command.Kind != durable.TaskActivity || command.Version != 2 || completed || attempt.Version != 1 || attempt.Attempt < 1 || attempt.Epoch < 1 {
		return fmt.Errorf("%w: invalid activity attempt", ErrHistory)
	}
	next := recordedAttempt{value: attempt, at: event.Time}
	if event.Type == EventActivityAttemptStarted {
		if attempt.Failure != nil || attempt.RetryAfter != 0 || attempt.Timeout != "" || attempt.Attempt != prior.value.Attempt+1 || attempt.Epoch <= prior.value.Epoch ||
			(prior.value.Attempt > 0 && (!prior.failed || prior.value.RetryAfter == 0 || event.Time.Before(prior.retryAt))) {
			return fmt.Errorf("%w: invalid activity attempt start", ErrHistory)
		}
		if err := validateBeforeActivityDeadline(history, command, prior, event.Time); err != nil {
			return err
		}
	} else {
		if prior.value.Attempt != attempt.Attempt || prior.value.Epoch != attempt.Epoch || prior.failed || event.Time.Before(prior.at) || attempt.Failure == nil || !validFailure(attempt.Failure) || attempt.RetryAfter != retryDelay(*command.ActivityOptions.RetryPolicy, attempt.Attempt, attempt.Failure) {
			return fmt.Errorf("%w: invalid activity attempt failure", ErrHistory)
		}
		if attempt.Timeout != "" {
			if err := validateTimeout(history, command, prior, attempt.Timeout, attempt.Failure, event.Time); err != nil {
				return err
			}
		} else if err := validateBeforeActivityDeadline(history, command, prior, event.Time); err != nil {
			return err
		}
		next.failed = true
		if attempt.RetryAfter > 0 {
			retryAt, err := durable.TaskTimeAfter(event.Time, attempt.RetryAfter)
			if err != nil {
				return fmt.Errorf("%w: invalid activity retry time", ErrHistory)
			}
			next.retryAt = retryAt
		}
	}
	history.attempts[attempt.CommandID] = next
	return nil
}

func validateActivityOutcome(history *replayHistory, command Command, outcome Outcome, at time.Time) error {
	if command.Version == 1 {
		if outcome.Version != 1 || outcome.Attempt != 0 || outcome.Timeout != "" {
			return fmt.Errorf("%w: legacy activity outcome version", ErrHistory)
		}
		return nil
	}
	attempt := history.attempts[command.ID]
	if outcome.Timeout != "" && (!attempt.failed || attempt.value.RetryAfter != 0 || attempt.value.Timeout == "") {
		return validateQueuedTimeoutOutcome(history, command, outcome, at)
	}
	if outcome.Version != 2 || outcome.Attempt != attempt.value.Attempt || outcome.Attempt < 1 || at.Before(attempt.at) ||
		(outcome.Failure == nil && attempt.failed) ||
		(outcome.Failure != nil && (!attempt.failed || attempt.value.RetryAfter != 0 || !sameFailure(outcome.Failure, attempt.value.Failure) || outcome.Timeout != attempt.value.Timeout)) {
		return fmt.Errorf("%w: outcome does not match activity attempt", ErrHistory)
	}
	if outcome.Failure == nil {
		return validateBeforeActivityDeadline(history, command, attempt, at)
	}
	return nil
}
