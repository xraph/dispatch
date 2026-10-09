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
	run, execution, err := ResolveExecutionDeadlines(r, now)
	if err != nil {
		return Execution{}, err
	}
	return Execution{Key: r.Key, WorkflowType: r.WorkflowType, BuildID: r.BuildID, State: StateRunning, Revision: 1, LastSequence: 1, Input: bytes.Clone(r.Input), CreatedAt: now, UpdatedAt: now, RunDeadlineAt: run, ExecutionDeadlineAt: execution, FirstRunID: r.RunID, RunNumber: 1, FirstStartedAt: now, RunTimeout: r.RunTimeout}, nil
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
	expected, _, err := ResolveExecutionDeadlines(StartRequest{RunTimeout: e.RunTimeout}, e.CreatedAt)
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
