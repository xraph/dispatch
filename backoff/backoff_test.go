package backoff_test

import (
	"testing"
	"time"

	"github.com/xraph/dispatch/backoff"
)

func TestConstant_ReturnsFixedDelay(t *testing.T) {
	c := backoff.NewConstant(5 * time.Second)
	for attempt := 1; attempt <= 10; attempt++ {
		if got := c.Delay(attempt); got != 5*time.Second {
			t.Errorf("Delay(%d) = %v, want %v", attempt, got, 5*time.Second)
		}
	}
}

func TestLinear_GrowsLinearly(t *testing.T) {
	l := backoff.NewLinear(time.Second, time.Minute)

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 3 * time.Second},
		{5, 5 * time.Second},
		{10, 10 * time.Second},
	}
	for _, tt := range tests {
		if got := l.Delay(tt.attempt); got != tt.want {
			t.Errorf("Delay(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestLinear_CapsAtMax(t *testing.T) {
	l := backoff.NewLinear(time.Second, 5*time.Second)

	if got := l.Delay(10); got != 5*time.Second {
		t.Errorf("Delay(10) = %v, want %v (capped at Max)", got, 5*time.Second)
	}
	if got := l.Delay(100); got != 5*time.Second {
		t.Errorf("Delay(100) = %v, want %v (capped at Max)", got, 5*time.Second)
	}
}

func TestExponential_DoublesEachAttempt(t *testing.T) {
	e := backoff.NewExponential(time.Second, time.Hour)

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 1 * time.Second},  // 1 * 2^0
		{2, 2 * time.Second},  // 1 * 2^1
		{3, 4 * time.Second},  // 1 * 2^2
		{4, 8 * time.Second},  // 1 * 2^3
		{5, 16 * time.Second}, // 1 * 2^4
	}
	for _, tt := range tests {
		if got := e.Delay(tt.attempt); got != tt.want {
			t.Errorf("Delay(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestExponential_CapsAtMax(t *testing.T) {
	e := backoff.NewExponential(time.Second, 10*time.Second)

	// Attempt 5 = 16s > 10s max → should return 10s.
	if got := e.Delay(5); got != 10*time.Second {
		t.Errorf("Delay(5) = %v, want %v (capped at Max)", got, 10*time.Second)
	}
	if got := e.Delay(20); got != 10*time.Second {
		t.Errorf("Delay(20) = %v, want %v (capped at Max)", got, 10*time.Second)
	}
}

func TestExponentialWithJitter_WithinBounds(t *testing.T) {
	e := backoff.NewExponentialWithJitter(time.Second, 10*time.Second)

	for attempt := 1; attempt <= 5; attempt++ {
		// Calculate expected max for this attempt.
		maxDelay := 10 * time.Second // capped at Max

		for range 100 {
			got := e.Delay(attempt)
			if got < 0 {
				t.Errorf("Delay(%d) = %v, should be >= 0", attempt, got)
			}
			if got > maxDelay {
				t.Errorf("Delay(%d) = %v, should be <= %v", attempt, got, maxDelay)
			}
		}
	}
}

func TestExponentialWithJitter_ProducesVariance(t *testing.T) {
	e := backoff.NewExponentialWithJitter(time.Second, time.Minute)

	// Collect 100 samples for attempt 3 and check they're not all the same.
	seen := make(map[time.Duration]bool)
	for range 100 {
		d := e.Delay(3)
		seen[d] = true
	}

	// With jitter, we should see many distinct values.
	if len(seen) < 2 {
		t.Errorf("expected variance in jitter, got only %d distinct values", len(seen))
	}
}

func TestDefaultStrategy_ReturnsExponentialWithJitter(t *testing.T) {
	s := backoff.DefaultStrategy()
	if s == nil {
		t.Fatal("DefaultStrategy() returned nil")
	}

	// Should return a positive delay for attempt 1.
	d := s.Delay(1)
	if d < 0 {
		t.Errorf("DefaultStrategy().Delay(1) = %v, should be >= 0", d)
	}
	if d > time.Second {
		t.Errorf("DefaultStrategy().Delay(1) = %v, should be <= 1s (initial)", d)
	}
}
