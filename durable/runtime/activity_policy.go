package runtime

import (
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/xraph/dispatch/durable"
)

// ActivityOptions are captured in command history before any attempt starts.
// Future timeout and heartbeat settings belong here, not in worker defaults.
type ActivityOptions struct {
	RetryPolicy *RetryPolicy `json:"retry_policy"`
}

// RetryPolicy governs failures across durable activity attempts. Zero fields use
// a 1s initial interval, coefficient 2, 100x maximum interval and unlimited attempts.
// MaximumAttempts includes the first attempt; 1 disables retries.
type RetryPolicy struct {
	InitialInterval    time.Duration `json:"initial_interval"`
	BackoffCoefficient float64       `json:"backoff_coefficient"`
	MaximumInterval    time.Duration `json:"maximum_interval"`
	MaximumAttempts    int64         `json:"maximum_attempts"`
	NonRetryableTypes  []string      `json:"non_retryable_types,omitempty"`
}

func normalizeActivityOptions(options ActivityOptions) (ActivityOptions, error) {
	policy := RetryPolicy{}
	if options.RetryPolicy != nil {
		policy = *options.RetryPolicy
	}
	if policy.InitialInterval == 0 {
		policy.InitialInterval = time.Second
	}
	if policy.BackoffCoefficient == 0 {
		policy.BackoffCoefficient = 2
	}
	if policy.MaximumInterval == 0 {
		policy.MaximumInterval = time.Duration(math.MaxInt64)
		if policy.InitialInterval > 0 && policy.InitialInterval <= time.Duration(math.MaxInt64/100) {
			policy.MaximumInterval = 100 * policy.InitialInterval
		}
	}
	if policy.InitialInterval <= 0 || policy.MaximumInterval < policy.InitialInterval || policy.BackoffCoefficient < 1 || math.IsNaN(policy.BackoffCoefficient) || math.IsInf(policy.BackoffCoefficient, 0) || policy.MaximumAttempts < 0 || len(policy.NonRetryableTypes) > 1000 {
		return ActivityOptions{}, fmt.Errorf("%w: invalid activity retry policy", durable.ErrInvalid)
	}
	policy.NonRetryableTypes = slices.Clone(policy.NonRetryableTypes)
	for _, kind := range policy.NonRetryableTypes {
		if !validID(kind) {
			return ActivityOptions{}, fmt.Errorf("%w: invalid non-retryable failure type", durable.ErrInvalid)
		}
	}
	slices.Sort(policy.NonRetryableTypes)
	policy.NonRetryableTypes = slices.Compact(policy.NonRetryableTypes)
	return ActivityOptions{RetryPolicy: &policy}, nil
}

func sameActivityOptions(a, b *ActivityOptions) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.RetryPolicy == nil || b.RetryPolicy == nil {
		return a.RetryPolicy == b.RetryPolicy
	}
	x, y := a.RetryPolicy, b.RetryPolicy
	return x.InitialInterval == y.InitialInterval && x.MaximumInterval == y.MaximumInterval && x.MaximumAttempts == y.MaximumAttempts && x.BackoffCoefficient == y.BackoffCoefficient && slices.Equal(x.NonRetryableTypes, y.NonRetryableTypes)
}

// retryDelay returns zero for a final failure. Chosen delays are written to
// history and task availability together; no wall clock or randomness is used.
func retryDelay(policy RetryPolicy, attempt int64, failure *ApplicationError) time.Duration {
	if failure == nil || failure.NonRetryable || attempt < 1 || attempt == math.MaxInt64 || (policy.MaximumAttempts > 0 && attempt >= policy.MaximumAttempts) || slices.Contains(policy.NonRetryableTypes, failure.Type) {
		return 0
	}
	delay := float64(policy.InitialInterval) * math.Pow(policy.BackoffCoefficient, float64(attempt-1))
	if delay >= float64(policy.MaximumInterval) {
		return policy.MaximumInterval
	}
	return time.Duration(math.Ceil(delay))
}
