package durable

import (
	"fmt"
	"math"
	"slices"
	"time"
)

// WorkflowRetryPolicy opts a workflow into new runs after retryable failures.
// Nil and entirely zero policies disable retries. For a configured policy, zero
// fields use a 1s initial interval, coefficient 2, 100x maximum interval and
// unlimited attempts. MaximumAttempts includes the first run; 1 disables retries.
type WorkflowRetryPolicy struct {
	InitialInterval    time.Duration `json:"initial_interval"`
	MaximumInterval    time.Duration `json:"maximum_interval"`
	BackoffCoefficient float64       `json:"backoff_coefficient"`
	MaximumAttempts    int64         `json:"maximum_attempts"`
	NonRetryableTypes  []string      `json:"non_retryable_types,omitempty"`
}

// Clone returns a policy whose failure-type list does not alias the original.
func (p *WorkflowRetryPolicy) Clone() *WorkflowRetryPolicy {
	if p == nil {
		return nil
	}
	copyPolicy := *p
	copyPolicy.NonRetryableTypes = slices.Clone(p.NonRetryableTypes)
	return &copyPolicy
}

// NormalizeWorkflowRetryPolicy validates and copies explicitly configured retry
// options. It does not change the caller's policy or enable an absent policy.
func NormalizeWorkflowRetryPolicy(input *WorkflowRetryPolicy) (*WorkflowRetryPolicy, error) {
	if input == nil || input.InitialInterval == 0 && input.MaximumInterval == 0 && input.BackoffCoefficient == 0 && input.MaximumAttempts == 0 && len(input.NonRetryableTypes) == 0 {
		return nil, nil
	}
	p := input.Clone()
	if p.InitialInterval == 0 {
		p.InitialInterval = time.Second
	}
	if p.BackoffCoefficient == 0 {
		p.BackoffCoefficient = 2
	}
	if p.MaximumInterval == 0 {
		p.MaximumInterval = time.Duration(math.MaxInt64)
		if p.InitialInterval > 0 && p.InitialInterval <= time.Duration(math.MaxInt64/100) {
			p.MaximumInterval = 100 * p.InitialInterval
		}
	}
	if p.InitialInterval < time.Microsecond || p.MaximumInterval < p.InitialInterval || p.BackoffCoefficient < 1 || math.IsNaN(p.BackoffCoefficient) || math.IsInf(p.BackoffCoefficient, 0) || p.MaximumAttempts < 0 || len(p.NonRetryableTypes) > 1000 {
		return nil, fmt.Errorf("%w: invalid workflow retry policy", ErrInvalid)
	}
	for _, kind := range p.NonRetryableTypes {
		if !identifier(kind) || len(kind) > 200 {
			return nil, fmt.Errorf("%w: invalid non-retryable workflow failure type", ErrInvalid)
		}
	}
	slices.Sort(p.NonRetryableTypes)
	p.NonRetryableTypes = slices.Compact(p.NonRetryableTypes)
	return p, nil
}

// SameWorkflowRetryPolicy compares already normalized policy snapshots.
func SameWorkflowRetryPolicy(a, b *WorkflowRetryPolicy) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.InitialInterval == b.InitialInterval && a.MaximumInterval == b.MaximumInterval && a.BackoffCoefficient == b.BackoffCoefficient && a.MaximumAttempts == b.MaximumAttempts && slices.Equal(a.NonRetryableTypes, b.NonRetryableTypes)
}

// WorkflowRetryDelay returns zero when this failure is final. The store resolves
// the returned duration against its clock and persists successor availability.
func WorkflowRetryDelay(policy *WorkflowRetryPolicy, attempt int64, failureType string, nonRetryable bool) (time.Duration, error) {
	p, err := NormalizeWorkflowRetryPolicy(policy)
	if err != nil {
		return 0, err
	}
	if attempt < 1 {
		return 0, fmt.Errorf("%w: invalid workflow retry attempt", ErrInvalid)
	}
	if p == nil || nonRetryable || attempt == math.MaxInt64 || p.MaximumAttempts > 0 && attempt >= p.MaximumAttempts || slices.Contains(p.NonRetryableTypes, failureType) {
		return 0, nil
	}
	delay := float64(p.InitialInterval) * math.Pow(p.BackoffCoefficient, float64(attempt-1))
	if delay >= float64(p.MaximumInterval) {
		return p.MaximumInterval, nil
	}
	return time.Duration(math.Ceil(delay)), nil
}
