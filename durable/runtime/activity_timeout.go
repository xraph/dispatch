package runtime

import (
	"fmt"
	"time"

	"github.com/xraph/dispatch/durable"
)

// ActivityTimeoutKind identifies the recorded deadline that ended an activity.
type ActivityTimeoutKind string

const (
	TimeoutScheduleToStart ActivityTimeoutKind = "schedule_to_start"
	TimeoutStartToClose    ActivityTimeoutKind = "start_to_close"
	TimeoutScheduleToClose ActivityTimeoutKind = "schedule_to_close"
	TimeoutHeartbeat       ActivityTimeoutKind = "heartbeat"
)

func hasActivityTimeout(options *ActivityOptions) bool {
	return options != nil && (options.ScheduleToStartTimeout > 0 || options.StartToCloseTimeout > 0 || options.ScheduleToCloseTimeout > 0 || options.HeartbeatTimeout > 0)
}

func activityOverallLimit(command Command, scheduled time.Time) (*time.Time, error) {
	if command.ActivityOptions.ScheduleToCloseTimeout == 0 {
		return nil, nil
	}
	deadline, err := durable.TaskTimeAfter(scheduled, command.ActivityOptions.ScheduleToCloseTimeout)
	if err != nil {
		return nil, err
	}
	return &deadline, nil
}

func firstActivityTimeout(options *ActivityOptions) time.Duration {
	if options == nil {
		return 0
	}
	queue, total := options.ScheduleToStartTimeout, options.ScheduleToCloseTimeout
	if queue == 0 || (total > 0 && total < queue) {
		return total
	}
	return queue
}

// activityDeadline derives the current deadline from store-authored event times.
// Overall expiry wins ties, so a total budget cannot turn into another retry.
func activityDeadline(command Command, scheduled time.Time, prior recordedAttempt) (time.Time, ActivityTimeoutKind, error) {
	limit, err := activityOverallLimit(command, scheduled)
	if err != nil {
		return time.Time{}, "", err
	}
	var deadline time.Time
	var kind ActivityTimeoutKind
	if limit != nil {
		deadline, kind = *limit, TimeoutScheduleToClose
	}
	base, delay, phase := scheduled, command.ActivityOptions.ScheduleToStartTimeout, TimeoutScheduleToStart
	if prior.value.Attempt > 0 && !prior.failed {
		base, delay, phase = prior.at, command.ActivityOptions.StartToCloseTimeout, TimeoutStartToClose
	} else if prior.failed {
		base = prior.retryAt
	}
	if delay > 0 {
		candidate, deadlineErr := durable.TaskTimeAfter(base, delay)
		if deadlineErr != nil {
			return time.Time{}, "", deadlineErr
		}
		if deadline.IsZero() || candidate.Before(deadline) {
			deadline, kind = candidate, phase
		}
	}
	if prior.value.Attempt > 0 && !prior.failed && prior.value.HeartbeatEnabled && command.ActivityOptions.HeartbeatTimeout > 0 {
		at := prior.at
		if prior.value.Heartbeat != nil {
			at = prior.value.Heartbeat.At
		}
		candidate, deadlineErr := durable.TaskTimeAfter(at, command.ActivityOptions.HeartbeatTimeout)
		if deadlineErr != nil {
			return time.Time{}, "", deadlineErr
		}
		if deadline.IsZero() || candidate.Before(deadline) {
			deadline, kind = candidate, TimeoutHeartbeat
		}
	}
	return deadline, kind, nil
}

func timeoutFailure(kind ActivityTimeoutKind) *ApplicationError {
	return &ApplicationError{Type: "activity_" + string(kind) + "_timeout", Message: "activity exceeded its " + string(kind) + " deadline", NonRetryable: kind != TimeoutStartToClose && kind != TimeoutHeartbeat}
}

func validateBeforeActivityDeadline(history *replayHistory, command Command, prior recordedAttempt, at time.Time) error {
	deadline, _, err := activityDeadline(command, history.scheduled[command.ID], prior)
	if err != nil || (!deadline.IsZero() && !at.Before(deadline)) {
		return fmt.Errorf("%w: activity execution occurred after its deadline", ErrHistory)
	}
	return nil
}

func validateTimeout(history *replayHistory, command Command, prior recordedAttempt, kind ActivityTimeoutKind, failure *ApplicationError, at time.Time) error {
	deadline, expected, err := activityDeadline(command, history.scheduled[command.ID], prior)
	if err != nil || kind == "" || kind != expected || deadline.IsZero() || at.Before(deadline) || failure == nil || !validFailure(failure) {
		return fmt.Errorf("%w: timeout does not match activity deadline", ErrHistory)
	}
	expectedFailure := timeoutFailure(kind)
	if failure.Type != expectedFailure.Type || failure.NonRetryable != expectedFailure.NonRetryable {
		return fmt.Errorf("%w: invalid timeout failure", ErrHistory)
	}
	return nil
}

func validateQueuedTimeoutOutcome(history *replayHistory, command Command, outcome Outcome, at time.Time) error {
	prior := history.attempts[command.ID]
	if outcome.Version != 2 || outcome.Attempt != prior.value.Attempt || (prior.value.Attempt > 0 && (!prior.failed || prior.value.RetryAfter == 0)) || outcome.Timeout == TimeoutStartToClose || outcome.Timeout == TimeoutHeartbeat || !sameHeartbeat(outcome.Heartbeat, prior.value.Heartbeat) {
		return fmt.Errorf("%w: invalid queued activity timeout", ErrHistory)
	}
	return validateTimeout(history, command, prior, outcome.Timeout, outcome.Failure, at)
}
