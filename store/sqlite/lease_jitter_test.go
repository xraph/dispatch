package sqlite

import (
	"testing"
	"time"
)

// The base jitter must spread concurrent writers while staying bounded before
// exponential scaling. A fixed delay would repeatedly wake writers together.
func TestBusyRetryDelayIsJitteredAroundTheBase(t *testing.T) {
	const (
		samples = 200
		low     = leaseBusyRetryDelay / 2
		high    = leaseBusyRetryDelay + leaseBusyRetryDelay/2
	)

	seen := make(map[time.Duration]struct{}, samples)
	var total time.Duration

	for range samples {
		d := busyRetryDelay()

		if d < low || d >= high {
			t.Fatalf("delay %v outside [%v, %v)", d, low, high)
		}

		seen[d] = struct{}{}
		total += d
	}

	// A constant would produce exactly one distinct value. The threshold is
	// deliberately far below `samples` so this cannot flake on collisions.
	if len(seen) < samples/4 {
		t.Errorf("only %d distinct delays in %d samples: contending writers "+
			"would retry in lockstep", len(seen), samples)
	}

	// The mean should sit near the base. Tolerance is wide because this is
	// a real random source and the test must not flake; it is here to catch
	// a jitter that shifted the centre, not to measure the distribution.
	mean := total / samples
	drift := mean - leaseBusyRetryDelay
	if drift < 0 {
		drift = -drift
	}
	if drift > leaseBusyRetryDelay/4 {
		t.Errorf("mean delay %v drifted from base %v: the retry budget is no "+
			"longer centred on the documented base", mean, leaseBusyRetryDelay)
	}
}
