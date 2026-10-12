package durable

import (
	"fmt"
	"time"
)

// ExecutionTimeoutKind identifies which immutable workflow deadline won.
type ExecutionTimeoutKind string

const (
	TimeoutRun       ExecutionTimeoutKind = "run"
	TimeoutExecution ExecutionTimeoutKind = "execution"
)

func validateExecutionTimeouts(r StartRequest) error {
	for _, timeout := range []time.Duration{r.RunTimeout, r.ExecutionTimeout} {
		if timeout != 0 && timeout < time.Microsecond {
			return fmt.Errorf("%w: workflow timeouts must be zero or at least one microsecond", ErrInvalid)
		}
	}
	return nil
}

// ResolveExecutionDeadlines resolves optional timeouts from the store's creation clock.
func ResolveExecutionDeadlines(r StartRequest, now time.Time) (run, execution time.Time, resultErr error) {
	if err := validateExecutionTimeouts(r); err != nil {
		return time.Time{}, time.Time{}, err
	}
	now = Timestamp(now)
	var deadlines [2]time.Time
	for i, timeout := range []time.Duration{r.RunTimeout, r.ExecutionTimeout} {
		if timeout == 0 {
			continue
		}
		deadlines[i] = Timestamp(now.Add(timeout))
		if deadlines[i].Year() < 1 || deadlines[i].Year() > 9999 {
			return time.Time{}, time.Time{}, fmt.Errorf("%w: workflow deadline outside supported date range", ErrInvalid)
		}
	}
	return deadlines[0], deadlines[1], nil
}

// Deadline returns the earliest limit; execution takes precedence on a tie.
func (e Execution) Deadline() (ExecutionTimeoutKind, time.Time) {
	if !e.RunDeadlineAt.IsZero() && (e.ExecutionDeadlineAt.IsZero() || e.RunDeadlineAt.Before(e.ExecutionDeadlineAt)) {
		return TimeoutRun, e.RunDeadlineAt
	}
	if !e.ExecutionDeadlineAt.IsZero() {
		return TimeoutExecution, e.ExecutionDeadlineAt
	}
	return "", time.Time{}
}

// CheckExecutionDeadline rejects progress at or after the persisted deadline.
// Callers check lifecycle and exact receipts separately before enforcing expiry.
func CheckExecutionDeadline(e Execution, now time.Time) error {
	_, deadline := e.Deadline()
	if !deadline.IsZero() && !deadline.After(now) {
		return ErrExecutionDeadline
	}
	return nil
}
