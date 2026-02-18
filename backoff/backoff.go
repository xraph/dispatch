// Package backoff provides pluggable retry delay strategies for job execution.
// All strategies are safe for concurrent use (they are stateless).
package backoff

import (
	"math"
	"math/rand/v2"
	"time"
)

// Strategy computes the delay before a retry attempt.
type Strategy interface {
	// Delay returns how long to wait before retry attempt n (1-indexed).
	// Attempt 1 is the first retry after the initial failure.
	Delay(attempt int) time.Duration
}

// ──────────────────────────────────────────────────
// Constant
// ──────────────────────────────────────────────────

// Constant always returns the same delay regardless of attempt number.
type Constant struct {
	Interval time.Duration
}

// NewConstant creates a constant backoff strategy.
func NewConstant(interval time.Duration) *Constant {
	return &Constant{Interval: interval}
}

// Delay returns the fixed interval.
func (c *Constant) Delay(_ int) time.Duration {
	return c.Interval
}

// ──────────────────────────────────────────────────
// Linear
// ──────────────────────────────────────────────────

// Linear increases the delay linearly with the attempt number.
// Delay = min(Initial * attempt, Max).
type Linear struct {
	Initial time.Duration
	Max     time.Duration
}

// NewLinear creates a linear backoff strategy.
func NewLinear(initial, maxDelay time.Duration) *Linear {
	return &Linear{Initial: initial, Max: maxDelay}
}

// Delay returns Initial * attempt, capped at Max.
func (l *Linear) Delay(attempt int) time.Duration {
	d := l.Initial * time.Duration(attempt)
	if l.Max > 0 && d > l.Max {
		return l.Max
	}
	return d
}

// ──────────────────────────────────────────────────
// Exponential
// ──────────────────────────────────────────────────

// Exponential doubles the delay each attempt.
// Delay = min(Initial * 2^(attempt-1), Max).
type Exponential struct {
	Initial time.Duration
	Max     time.Duration
}

// NewExponential creates an exponential backoff strategy.
func NewExponential(initial, maxDelay time.Duration) *Exponential {
	return &Exponential{Initial: initial, Max: maxDelay}
}

// Delay returns Initial * 2^(attempt-1), capped at Max.
func (e *Exponential) Delay(attempt int) time.Duration {
	d := time.Duration(float64(e.Initial) * math.Pow(2, float64(attempt-1)))
	if e.Max > 0 && d > e.Max {
		return e.Max
	}
	return d
}

// ──────────────────────────────────────────────────
// ExponentialWithJitter (full jitter)
// ──────────────────────────────────────────────────

// ExponentialWithJitter applies full jitter to an exponential base.
// Delay = random value in [0, min(Initial * 2^(attempt-1), Max)].
// This prevents thundering herd when many retries happen simultaneously.
type ExponentialWithJitter struct {
	Initial time.Duration
	Max     time.Duration
}

// NewExponentialWithJitter creates an exponential backoff with full jitter.
func NewExponentialWithJitter(initial, maxDelay time.Duration) *ExponentialWithJitter {
	return &ExponentialWithJitter{Initial: initial, Max: maxDelay}
}

// Delay returns a random duration in [0, min(Initial * 2^(attempt-1), Max)].
func (e *ExponentialWithJitter) Delay(attempt int) time.Duration {
	base := float64(e.Initial) * math.Pow(2, float64(attempt-1))
	if e.Max > 0 && base > float64(e.Max) {
		base = float64(e.Max)
	}
	return time.Duration(rand.Float64() * base) //nolint:gosec // jitter intentionally uses non-crypto rand
}

// ──────────────────────────────────────────────────
// Default
// ──────────────────────────────────────────────────

// DefaultStrategy returns the default backoff used by the engine:
// ExponentialWithJitter with 1s initial and 1m max.
func DefaultStrategy() Strategy {
	return NewExponentialWithJitter(1*time.Second, 1*time.Minute)
}
