package runtime

import (
	"math"
	"testing"
	"time"
)

func TestActivityBackoffSaturates(t *testing.T) {
	policy := RetryPolicy{InitialInterval: time.Hour, MaximumInterval: time.Duration(math.MaxInt64), BackoffCoefficient: 2}
	failure := &ApplicationError{Type: "temporary", Message: "retry"}
	if got := retryDelay(policy, 1000, failure); got != policy.MaximumInterval {
		t.Fatalf("overflowed backoff: %v", got)
	}
	policy.BackoffCoefficient = 1
	if got := retryDelay(policy, math.MaxInt64-1, failure); got != time.Hour {
		t.Fatalf("constant backoff: %v", got)
	}
	if got := retryDelay(policy, math.MaxInt64, failure); got != 0 {
		t.Fatalf("attempt overflow: %v", got)
	}
}
