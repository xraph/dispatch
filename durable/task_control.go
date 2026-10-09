package durable

import (
	"bytes"
	"fmt"
	"math"
	"time"
)

// ValidateTaskID checks a task identity before any store access.
func ValidateTaskID(id string) error {
	if !identifier(id) {
		return fmt.Errorf("%w: task ID is required", ErrInvalid)
	}
	return nil
}

// ValidateTaskSpec rejects ambiguous availability and invalid relative durations.
func ValidateTaskSpec(task TaskSpec) error {
	if !identifier(task.ID) || !identifier(task.Queue) || !validKind(task.Kind) ||
		task.AvailableAfter < 0 || task.DeadlineAfter < 0 || !validDeadlineLimit(task.DeadlineLimit) ||
		(!task.AvailableAt.IsZero() && (task.AvailableAfter != 0 || !validTaskTime(task.AvailableAt))) ||
		(task.Kind == TaskTimer && task.AvailableAt.IsZero() && task.AvailableAfter == 0) {
		return fmt.Errorf("%w: invalid task identity, routing or deadline", ErrInvalid)
	}
	return nil
}

func validateTaskControl(r CommitRequest) error {
	if r.CancelPendingTasks && (len(r.CancelTasks) != 0 || r.TaskUpdate != nil && r.TaskUpdate.Action != TaskComplete) {
		return fmt.Errorf("%w: pending-task fencing requires a completed source without individual cancellations", ErrInvalid)
	}
	if len(r.Conditions) > 1000 || len(r.CancelTasks) > 1000 {
		return fmt.Errorf("%w: at most 1000 task conditions and cancellations", ErrInvalid)
	}
	conditions := make(map[string]bool, len(r.Conditions))
	for _, condition := range r.Conditions {
		if !identifier(condition.TaskID) || condition.Version <= 0 || conditions[condition.TaskID] {
			return fmt.Errorf("%w: invalid or duplicate task condition", ErrInvalid)
		}
		conditions[condition.TaskID] = true
	}
	cancelled := make(map[string]bool, len(r.CancelTasks))
	for _, taskID := range r.CancelTasks {
		if taskID == r.Token.TaskID || !conditions[taskID] || cancelled[taskID] {
			return fmt.Errorf("%w: task cancellation requires a unique, guarded target distinct from the source", ErrInvalid)
		}
		cancelled[taskID] = true
	}
	u := r.TaskUpdate
	if u == nil {
		return nil
	}
	if (u.AsyncKeyHash != "" && u.Action != TaskAwait) || (r.Token.LeaseKind == LeaseAsync && u.Action == TaskKeep) {
		return fmt.Errorf("%w: invalid asynchronous task update", ErrInvalid)
	}
	if u.Heartbeat != nil && (u.Action != TaskKeep || u.Heartbeat.Timeout < 0 || u.DeadlineAfter == nil || r.Token.LeaseKind != "") {
		return fmt.Errorf("%w: heartbeat activation needs a retained execution grant and explicit deadline", ErrInvalid)
	}
	if !validDeadlineLimit(u.DeadlineLimit) || (u.LeaseDuration != 0 && (u.Action != TaskKeep || ValidateLease(u.LeaseDuration) != nil)) || (r.Token.LeaseKind == LeaseTimeout && u.Action == TaskKeep) {
		return fmt.Errorf("%w: invalid deadline limit or retained grant renewal", ErrInvalid)
	}
	if (r.State != "" && r.State != StateRunning && u.Action != TaskComplete) ||
		(u.DeadlineAfter != nil && *u.DeadlineAfter < 0) || u.RetryAfter < 0 {
		return fmt.Errorf("%w: task update conflicts with execution state or duration", ErrInvalid)
	}
	switch u.Action {
	case TaskAwait:
		if r.Token.LeaseKind != "" || !validHex256(u.AsyncKeyHash) || u.DeadlineAfter != nil || u.DeadlineLimit != nil ||
			u.LeaseDuration != 0 || u.Progress != nil || !u.RetryAt.IsZero() || u.RetryAfter != 0 || u.Heartbeat != nil {
			return fmt.Errorf("%w: asynchronous handoff requires only a secret digest on an execution grant", ErrInvalid)
		}
	case TaskComplete:
		if u.DeadlineAfter != nil || u.DeadlineLimit != nil || u.LeaseDuration != 0 || u.Progress != nil || !u.RetryAt.IsZero() || u.RetryAfter != 0 {
			return fmt.Errorf("%w: completed task cannot be updated", ErrInvalid)
		}
	case TaskKeep:
		if !u.RetryAt.IsZero() || u.RetryAfter != 0 {
			return fmt.Errorf("%w: retained task cannot set retry availability", ErrInvalid)
		}
	case TaskRetry:
		if (u.RetryAt.IsZero() && u.RetryAfter == 0) ||
			(!u.RetryAt.IsZero() && (u.RetryAfter != 0 || !validTaskTime(u.RetryAt))) {
			return fmt.Errorf("%w: retry needs exactly one availability", ErrInvalid)
		}
	default:
		return fmt.Errorf("%w: unknown task action", ErrInvalid)
	}
	return nil
}

func validDeadlineLimit(limit *time.Time) bool {
	return limit == nil || (!limit.IsZero() && validTaskTime(*limit))
}

func capDeadline(deadline time.Time, limit *time.Time) (time.Time, error) {
	if limit == nil {
		return deadline, nil
	}
	normalized, err := TaskTimeAfter(*limit, 0)
	if err != nil {
		return time.Time{}, err
	}
	if deadline.IsZero() || normalized.Before(deadline) {
		return normalized, nil
	}
	return deadline, nil
}

func validTaskTime(value time.Time) bool { return value.Year() >= 1 && value.Year() <= 9999 }

// TaskTimeAfter rounds a relative deadline up, never before the requested delay.
func TaskTimeAfter(base time.Time, delay time.Duration) (time.Time, error) {
	value := base.Add(delay)
	result := Timestamp(value)
	if result.Before(value) {
		result = result.Add(time.Microsecond)
	}
	if delay < 0 || !validTaskTime(result) {
		return time.Time{}, fmt.Errorf("%w: task deadline outside supported time range", ErrInvalid)
	}
	return result, nil
}

// NewTask resolves scheduling against the transition timestamp. Relative fields
// are cleared in the persisted projection; the immutable receipt retains them.
func NewTask(key Key, spec TaskSpec, now time.Time) (Task, error) {
	if err := ValidateTaskSpec(spec); err != nil {
		return Task{}, err
	}
	if spec.AvailableAt.IsZero() {
		available, err := TaskTimeAfter(now, spec.AvailableAfter)
		if err != nil {
			return Task{}, err
		}
		spec.AvailableAt = available
	} else {
		spec.AvailableAt = Timestamp(spec.AvailableAt)
	}
	task := Task{Key: key, TaskSpec: spec, Version: 1}
	if spec.DeadlineAfter > 0 {
		deadline, err := TaskTimeAfter(spec.AvailableAt, spec.DeadlineAfter)
		if err != nil {
			return Task{}, err
		}
		task.DeadlineAt = deadline
	}
	deadline, err := capDeadline(task.DeadlineAt, spec.DeadlineLimit)
	if err != nil {
		return Task{}, err
	}
	task.DeadlineAt = deadline
	task.AvailableAfter, task.DeadlineAfter, task.DeadlineLimit = 0, 0, nil
	task.Payload = bytes.Clone(spec.Payload)
	return task, nil
}

// CheckTaskCondition evaluates a locked task against a caller's observation.
func CheckTaskCondition(task Task, condition TaskCondition, now time.Time) error {
	if task.ID != condition.TaskID || task.Done || task.Version != condition.Version ||
		(condition.DeadlineElapsed && (task.DeadlineAt.IsZero() || task.DeadlineAt.After(now))) {
		return ErrTaskConflict
	}
	return nil
}

// UpdateTask computes source task state after its grant has been validated.
func UpdateTask(task Task, update *TaskUpdate, now time.Time) (Task, error) {
	if task.Version < 1 || task.Version == math.MaxInt64 {
		return Task{}, fmt.Errorf("%w: task version exhausted", ErrInvalid)
	}
	task.Version++
	task.Progress = bytes.Clone(task.Progress)
	if update == nil || update.Action == TaskComplete {
		task.Done = true
		return task, nil
	}
	if update.Action == TaskAwait {
		if task.Kind != TaskActivity || task.LeaseKind != "" || task.HeartbeatEpoch != task.Epoch || task.HeartbeatEpoch < 1 ||
			task.HeartbeatAt.IsZero() || !task.DeadlineAt.After(now) || !validHex256(update.AsyncKeyHash) {
			return Task{}, fmt.Errorf("%w: asynchronous handoff requires an active activity with a deadline", ErrInvalid)
		}
		// Older pollers do not inspect LeaseKind. Keep their eligibility test
		// fenced through the activity deadline without requiring worker renewal.
		task.LeaseKind, task.LeaseUntil, task.AsyncKeyHash = LeaseAsync, task.DeadlineAt, update.AsyncKeyHash
		return task, nil
	}
	if update.Action == TaskKeep && task.HeartbeatEpoch == task.Epoch && task.HeartbeatEpoch > 0 &&
		(update.Heartbeat != nil || update.DeadlineAfter != nil || update.DeadlineLimit != nil || update.Progress != nil) {
		return Task{}, fmt.Errorf("%w: heartbeat grant configuration is immutable; use RecordHeartbeat for progress", ErrTaskConflict)
	}
	if update.Action == TaskRetry {
		available := Timestamp(update.RetryAt)
		if update.RetryAt.IsZero() {
			var err error
			available, err = TaskTimeAfter(now, update.RetryAfter)
			if err != nil {
				return Task{}, err
			}
		}
		task.AvailableAt, task.Owner, task.LeaseUntil, task.LeaseKind = available, "", time.Time{}, ""
		task.AsyncKeyHash = ""
		task.HeartbeatAt, task.HeartbeatLimit = time.Time{}, time.Time{}
		task.HeartbeatTimeout, task.HeartbeatSequence, task.HeartbeatEpoch = 0, 0, 0
	}
	if update.DeadlineAfter != nil {
		task.DeadlineAt = time.Time{}
		if *update.DeadlineAfter > 0 {
			base := now
			if update.Action == TaskRetry {
				base = task.AvailableAt
			}
			deadline, err := TaskTimeAfter(base, *update.DeadlineAfter)
			if err != nil {
				return Task{}, err
			}
			task.DeadlineAt = deadline
		}
	}
	deadline, err := capDeadline(task.DeadlineAt, update.DeadlineLimit)
	if err != nil {
		return Task{}, err
	}
	task.DeadlineAt = deadline
	if update.Heartbeat != nil {
		if task.Kind != TaskActivity || task.LeaseKind != "" {
			return Task{}, fmt.Errorf("%w: heartbeat activation requires an activity execution grant", ErrInvalid)
		}
		task.HeartbeatTimeout, task.HeartbeatAt, task.HeartbeatEpoch = update.Heartbeat.Timeout, now, task.Epoch
		task.HeartbeatSequence, task.HeartbeatLimit = 0, task.DeadlineAt
		deadline, err = heartbeatDeadline(task, now)
		if err != nil {
			return Task{}, err
		}
		task.DeadlineAt = deadline
	}
	if update.LeaseDuration > 0 {
		if until := Timestamp(now.Add(update.LeaseDuration)); until.After(task.LeaseUntil) {
			task.LeaseUntil = until
		}
	}
	if update.Progress != nil {
		task.Progress = bytes.Clone(*update.Progress)
	}
	if !task.DeadlineAt.IsZero() && task.LeaseUntil.After(task.DeadlineAt) {
		task.LeaseUntil = task.DeadlineAt
	}
	return task, nil
}
