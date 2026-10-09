package durable

import (
	"bytes"
	"fmt"
	"time"
)

// NewExecution constructs an independent root using the authoritative creation clock.
// Historical runs sharing a workflow ID are not implicitly one chain.
func NewExecution(r StartRequest, now time.Time) (Execution, error) {
	if err := r.Validate(); err != nil {
		return Execution{}, err
	}
	now = Timestamp(now)
	if !validRunTime(now) {
		return Execution{}, fmt.Errorf("%w: invalid execution creation time", ErrInvalid)
	}
	policy, err := NormalizeWorkflowRetryPolicy(r.RetryPolicy)
	if err != nil {
		return Execution{}, err
	}
	run, execution, err := ResolveExecutionDeadlines(r, now)
	if err != nil {
		return Execution{}, err
	}
	return Execution{Key: r.Key, WorkflowType: r.WorkflowType, BuildID: r.BuildID, State: StateRunning, Revision: 1, LastSequence: 1, Input: bytes.Clone(r.Input), CreatedAt: now, UpdatedAt: now, RunDeadlineAt: run, ExecutionDeadlineAt: execution, FirstRunID: r.RunID, RunNumber: 1, FirstStartedAt: now, RunTimeout: r.RunTimeout, RetryPolicy: policy, RetryAttempt: 1, RunAvailableAt: now}, nil
}

// ValidateRunMetadata checks local lineage and deadline consistency. The store
// separately verifies predecessor/successor rows and atomic relationship changes.
func ValidateRunMetadata(e Execution) error {
	if e.Validate() != nil || !identifier(e.FirstRunID) || e.RunNumber < 1 || !validRunTime(e.CreatedAt) || !validRunTime(e.FirstStartedAt) || e.FirstStartedAt.After(e.CreatedAt) {
		return fmt.Errorf("%w: invalid run lineage", ErrInvalid)
	}
	if e.RunNumber == 1 {
		if e.FirstRunID != e.RunID || e.PreviousRunID != "" || !e.FirstStartedAt.Equal(e.CreatedAt) {
			return fmt.Errorf("%w: inconsistent root run", ErrInvalid)
		}
	} else if e.FirstRunID == e.RunID || !identifier(e.PreviousRunID) || e.PreviousRunID == e.RunID {
		return fmt.Errorf("%w: inconsistent successor run", ErrInvalid)
	}
	if e.NextRunID != "" && (!identifier(e.NextRunID) || e.NextRunID == e.RunID || e.NextRunID == e.FirstRunID || e.NextRunID == e.PreviousRunID || e.State == StateRunning) {
		return fmt.Errorf("%w: invalid next run", ErrInvalid)
	}
	attempt, available := e.WorkflowAttempt(), e.AvailableAt()
	policy, err := NormalizeWorkflowRetryPolicy(e.RetryPolicy)
	if err != nil || !SameWorkflowRetryPolicy(policy, e.RetryPolicy) || attempt < 1 || attempt > e.RunNumber || !validRunTime(available) || available.Before(e.CreatedAt) || attempt == 1 && !available.Equal(e.CreatedAt) {
		return fmt.Errorf("%w: invalid workflow retry metadata", ErrInvalid)
	}
	if attempt > 1 && (!available.After(e.CreatedAt) || !e.ExecutionDeadlineAt.IsZero() && !available.Before(e.ExecutionDeadlineAt)) {
		return fmt.Errorf("%w: retry availability must precede execution expiry and follow creation", ErrInvalid)
	}
	expected, _, err := ResolveExecutionDeadlines(StartRequest{RunTimeout: e.RunTimeout}, available)
	if err != nil || !expected.Equal(e.RunDeadlineAt) {
		return fmt.Errorf("%w: run timeout does not match deadline", ErrInvalid)
	}
	if !e.ExecutionDeadlineAt.IsZero() && (!validRunTime(e.ExecutionDeadlineAt) || !e.ExecutionDeadlineAt.After(e.FirstStartedAt)) {
		return fmt.Errorf("%w: invalid chain execution deadline", ErrInvalid)
	}
	return nil
}

func validRunTime(t time.Time) bool {
	return !t.IsZero() && t.Year() >= 1 && t.Year() <= 9999 && t.Equal(Timestamp(t))
}

// AvailableAt is the first permitted execution time, including retry backoff.
// Older snapshots without the field use their creation time.
func (e Execution) AvailableAt() time.Time {
	if e.RunAvailableAt.IsZero() {
		return e.CreatedAt
	}
	return e.RunAvailableAt
}

// WorkflowAttempt counts run attempts since the last explicit continuation.
func (e Execution) WorkflowAttempt() int64 {
	if e.RetryAttempt == 0 {
		return 1
	}
	return e.RetryAttempt
}

// Clone isolates execution payloads and retry policy from stored state.
func (e Execution) Clone() Execution {
	e.Input, e.Output = bytes.Clone(e.Input), bytes.Clone(e.Output)
	e.RetryPolicy = e.RetryPolicy.Clone()
	return e
}

// Clone isolates a retained start request from caller-owned memory.
func (r StartRequest) Clone() StartRequest {
	r.Input = bytes.Clone(r.Input)
	r.RetryPolicy = r.RetryPolicy.Clone()
	return r
}
