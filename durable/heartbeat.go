package durable

import (
	"bytes"
	"fmt"
	"math"
	"time"
)

// HeartbeatConfig enables activity progress. Zero Timeout disables only the
// progress deadline. Activation captures the task's attempt/overall deadline.
type HeartbeatConfig struct {
	Timeout time.Duration `json:"timeout"`
}

// HeartbeatRequest records one progress update under an enabled execution grant.
// Sequence starts at one per grant. Retry the identical request on uncertainty.
type HeartbeatRequest struct {
	Key
	RequestID     string        `json:"request_id"`
	Token         TaskToken     `json:"token"`
	Sequence      int64         `json:"sequence"`
	Progress      []byte        `json:"progress,omitempty"`
	LeaseDuration time.Duration `json:"lease_duration"`
}

// Validate bounds heartbeat input before persistence.
func (r HeartbeatRequest) Validate() error {
	if err := r.Key.Validate(); err != nil {
		return err
	}
	if err := r.Token.Validate(); err != nil {
		return err
	}
	if !identifier(r.RequestID) || r.Sequence < 1 || r.Token.LeaseKind != "" || len(r.Progress) > 1<<20 {
		return fmt.Errorf("%w: heartbeat needs a request ID, positive sequence, execution grant and at most 1 MiB progress", ErrInvalid)
	}
	return ValidateLease(r.LeaseDuration)
}

// ApplyHeartbeat computes a task mutation under the backend's execution/task
// locks. The backend must check the immutable receipt before calling this.
func ApplyHeartbeat(task Task, r HeartbeatRequest, now time.Time) (Task, error) {
	if err := r.Validate(); err != nil {
		return Task{}, err
	}
	if err := CheckLease(task, r.Token, now); err != nil {
		return Task{}, err
	}
	if task.Key != r.Key || task.Kind != TaskActivity || task.HeartbeatEpoch != task.Epoch || task.HeartbeatAt.IsZero() ||
		task.HeartbeatSequence == math.MaxInt64 || r.Sequence != task.HeartbeatSequence+1 || now.Before(task.HeartbeatAt) {
		return Task{}, ErrTaskConflict
	}
	if task.Version < 1 || task.Version == math.MaxInt64 {
		return Task{}, fmt.Errorf("%w: task version exhausted", ErrInvalid)
	}
	task.HeartbeatAt, task.HeartbeatSequence = now, r.Sequence
	task.Progress, task.Version = bytes.Clone(r.Progress), task.Version+1
	deadline, err := heartbeatDeadline(task, now)
	if err != nil {
		return Task{}, err
	}
	task.DeadlineAt = deadline
	if until := Timestamp(now.Add(r.LeaseDuration)); until.After(task.LeaseUntil) {
		task.LeaseUntil = until
	}
	if !deadline.IsZero() && task.LeaseUntil.After(deadline) {
		task.LeaseUntil = deadline
	}
	return task, nil
}

func heartbeatDeadline(task Task, now time.Time) (time.Time, error) {
	deadline := task.HeartbeatLimit
	if task.HeartbeatTimeout > 0 {
		progressDeadline, err := TaskTimeAfter(now, task.HeartbeatTimeout)
		if err != nil {
			return time.Time{}, err
		}
		if deadline.IsZero() || progressDeadline.Before(deadline) {
			deadline = progressDeadline
		}
	}
	return deadline, nil
}
