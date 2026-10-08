package runtime

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/xraph/dispatch/durable"
)

// HeartbeatCheckpoint is the last persisted progress at an attempt's outcome.
// Sequence zero carries the progress inherited when the attempt started.
type HeartbeatCheckpoint struct {
	At       time.Time `json:"at"`
	Epoch    int64     `json:"epoch"`
	Sequence int64     `json:"sequence"`
	Details  []byte    `json:"details,omitempty"`
}

// ActivityError provides activity metadata while preserving ApplicationError
// through errors.As. The checkpoint returned to workflow code is a copy.
type ActivityError struct {
	Failure   *ApplicationError
	Attempt   int64
	Timeout   ActivityTimeoutKind
	Heartbeat *HeartbeatCheckpoint
}

func (e *ActivityError) Error() string { return e.Failure.Error() }
func (e *ActivityError) Unwrap() error { return e.Failure }

// Heartbeat synchronously persists copied progress under this activity grant.
// An identical retry resolves its original receipt without extending it again.
func (a ActivityInfo) Heartbeat(ctx context.Context, details []byte) error {
	if a.heartbeat == nil {
		return fmt.Errorf("%w: heartbeats require a running version 2 activity", durable.ErrInvalid)
	}
	return a.heartbeat(ctx, details)
}

// HeartbeatDetails returns a copy of progress inherited from the previous attempt.
func (a ActivityInfo) HeartbeatDetails() []byte { return bytes.Clone(a.heartbeatDetails) }

func cloneHeartbeat(value *HeartbeatCheckpoint) *HeartbeatCheckpoint {
	if value == nil {
		return nil
	}
	copyValue := *value
	copyValue.Details = bytes.Clone(value.Details)
	return &copyValue
}

func sameHeartbeat(a, b *HeartbeatCheckpoint) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.At.Equal(b.At) && a.Epoch == b.Epoch && a.Sequence == b.Sequence && bytes.Equal(a.Details, b.Details)
}

func validateHeartbeat(prior recordedAttempt, checkpoint *HeartbeatCheckpoint, at time.Time) error {
	if !prior.value.HeartbeatEnabled {
		if checkpoint != nil {
			return fmt.Errorf("%w: checkpoint on an attempt without heartbeats", ErrHistory)
		}
		return nil
	}
	if checkpoint == nil || checkpoint.Epoch != prior.value.Epoch || checkpoint.Sequence < 0 || len(checkpoint.Details) > 1<<20 || checkpoint.At.Before(prior.at) || checkpoint.At.After(at) ||
		!checkpoint.At.Equal(durable.Timestamp(checkpoint.At)) ||
		(checkpoint.Sequence == 0 && (!checkpoint.At.Equal(prior.at) || !bytes.Equal(checkpoint.Details, prior.value.Progress))) {
		return fmt.Errorf("%w: invalid final heartbeat checkpoint", ErrHistory)
	}
	return nil
}

func (w *Worker) activityCheckpoint(ctx context.Context, task durable.Task, prior recordedAttempt) (*HeartbeatCheckpoint, error) {
	if !prior.value.HeartbeatEnabled {
		return nil, nil
	}
	current, err := storeCall(ctx, w, func(callCtx context.Context) (durable.Task, error) {
		return w.store.GetTask(callCtx, task.Key, task.ID)
	})
	if err != nil {
		return nil, err
	}
	if current.Done || current.Token() != task.Token() {
		return nil, durable.ErrLeaseLost
	}
	if current.HeartbeatEpoch != prior.value.Epoch || current.HeartbeatAt.IsZero() {
		return nil, w.effectConflict(ctx, task, "heartbeat state does not match active attempt")
	}
	return &HeartbeatCheckpoint{At: current.HeartbeatAt, Epoch: current.HeartbeatEpoch, Sequence: current.HeartbeatSequence, Details: bytes.Clone(current.Progress)}, nil
}
