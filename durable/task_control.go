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
		task.AvailableAfter < 0 || task.DeadlineAfter < 0 ||
		(!task.AvailableAt.IsZero() && (task.AvailableAfter != 0 || !validTaskTime(task.AvailableAt))) ||
		(task.Kind == TaskTimer && task.AvailableAt.IsZero() && task.AvailableAfter == 0) {
		return fmt.Errorf("%w: invalid task identity, routing or deadline", ErrInvalid)
	}
	return nil
}

func validateTaskControl(r CommitRequest) error {
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
	if (r.State != "" && r.State != StateRunning && u.Action != TaskComplete) ||
		(u.DeadlineAfter != nil && *u.DeadlineAfter < 0) || u.RetryAfter < 0 {
		return fmt.Errorf("%w: task update conflicts with execution state or duration", ErrInvalid)
	}
	switch u.Action {
	case TaskComplete:
		if u.DeadlineAfter != nil || u.Progress != nil || !u.RetryAt.IsZero() || u.RetryAfter != 0 {
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
	task.AvailableAfter, task.DeadlineAfter = 0, 0
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
	if update.Action == TaskRetry {
		available := Timestamp(update.RetryAt)
		if update.RetryAt.IsZero() {
			var err error
			available, err = TaskTimeAfter(now, update.RetryAfter)
			if err != nil {
				return Task{}, err
			}
		}
		task.AvailableAt, task.Owner, task.LeaseUntil = available, "", time.Time{}
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
	if update.Progress != nil {
		task.Progress = bytes.Clone(*update.Progress)
	}
	if !task.DeadlineAt.IsZero() && task.LeaseUntil.After(task.DeadlineAt) {
		task.LeaseUntil = task.DeadlineAt
	}
	return task, nil
}
