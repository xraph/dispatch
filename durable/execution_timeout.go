package durable

import (
	"encoding/json"
	"math"
	"time"
)

const EventWorkflowTimedOut = "workflow.timed_out"

// ExecutionTimeout records the immutable limit responsible for forced closure.
type ExecutionTimeout struct {
	Version    int                  `json:"version"`
	Kind       ExecutionTimeoutKind `json:"kind"`
	DeadlineAt time.Time            `json:"deadline_at"`
}

// Validate checks a timeout payload independently of a saved execution.
func (t ExecutionTimeout) Validate() error {
	if t.Version != 1 || (t.Kind != TimeoutRun && t.Kind != TimeoutExecution) || t.DeadlineAt.IsZero() || t.DeadlineAt.Year() < 1 || t.DeadlineAt.Year() > 9999 || !t.DeadlineAt.Equal(Timestamp(t.DeadlineAt)) {
		return ErrInvalid
	}
	return nil
}

// ExecutionTimeoutTask grants expiry processing independently of ordinary tasks.
type ExecutionTimeoutTask struct {
	Key
	Kind       ExecutionTimeoutKind `json:"kind"`
	DeadlineAt time.Time            `json:"deadline_at"`
	Owner      string               `json:"owner"`
	Epoch      int64                `json:"epoch"`
	Attempt    int64                `json:"attempt"`
	LeaseUntil time.Time            `json:"lease_until"`
}

// ExecutionTimeoutClaimRequest polls all expired builds in one namespace.
type ExecutionTimeoutClaimRequest struct {
	Namespace     string        `json:"namespace"`
	Owner         string        `json:"owner"`
	LeaseDuration time.Duration `json:"lease_duration"`
}

func (r ExecutionTimeoutClaimRequest) Validate() error {
	if !identifier(r.Namespace) || !identifier(r.Owner) {
		return ErrInvalid
	}
	return ValidateLease(r.LeaseDuration)
}

// ExecutionTimeoutRequest closes an expired execution under a live timeout grant.
type ExecutionTimeoutRequest struct {
	Key
	RequestID string `json:"request_id"`
	Owner     string `json:"owner"`
	Epoch     int64  `json:"epoch"`
}

func (r ExecutionTimeoutRequest) Validate() error {
	if err := r.Key.Validate(); err != nil {
		return err
	}
	if !identifier(r.RequestID) || !identifier(r.Owner) || r.Epoch < 1 {
		return ErrInvalid
	}
	return nil
}

// CheckExecutionTimeoutLease rejects expired or superseded timeout ownership.
func CheckExecutionTimeoutLease(task ExecutionTimeoutTask, r ExecutionTimeoutRequest, now time.Time) error {
	if task.Key != r.Key || task.Owner != r.Owner || task.Epoch != r.Epoch || !task.LeaseUntil.After(now) {
		return ErrLeaseLost
	}
	return nil
}

// PrepareExecutionTimeout computes terminal history after the store checks its grant.
func PrepareExecutionTimeout(current Execution, now time.Time) (Execution, Event, Receipt, error) {
	if current.State != StateRunning {
		return Execution{}, Event{}, Receipt{}, ErrClosed
	}
	kind, deadline := current.Deadline()
	if deadline.IsZero() || deadline.After(now) {
		return Execution{}, Event{}, Receipt{}, ErrTaskConflict
	}
	if current.Revision == math.MaxInt64 || current.LastSequence == math.MaxInt64 {
		return Execution{}, Event{}, Receipt{}, ErrInvalid
	}
	timeout := ExecutionTimeout{Version: 1, Kind: kind, DeadlineAt: deadline}
	if err := timeout.Validate(); err != nil {
		return Execution{}, Event{}, Receipt{}, err
	}
	payload, err := json.Marshal(timeout)
	if err != nil {
		return Execution{}, Event{}, Receipt{}, err
	}
	next := current
	next.State = StateTimedOut
	next.Revision++
	next.LastSequence++
	next.Output = nil
	next.UpdatedAt = Timestamp(now)
	event := Event{EventInput: EventInput{Type: EventWorkflowTimedOut, Payload: payload}, Sequence: next.LastSequence, Time: next.UpdatedAt}
	return next, event, Receipt{Revision: next.Revision, FirstSequence: next.LastSequence, LastSequence: next.LastSequence}, nil
}
