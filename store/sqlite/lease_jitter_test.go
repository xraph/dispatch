package sqlite

import (
	"testing"
	"time"
)

// TestBusyRetryDelayIsJitteredAroundTheBase covers the two properties the
// retry backoff depends on, both of which a plain constant would break.
//
// SQLite takes one write lock for the whole database, so of N writers that
// collide exactly one wins and the rest retry. With a fixed delay those
// losers sleep the identical interval and wake together to collide again,
// staying in lockstep for as long as it takes them to drain one at a time.
// Spreading the wake-ups is the entire point, so a delay that is always
// the same value is the bug this guards against.
//
// The bound matters just as much in the other direction: the jitter is
// centred on leaseBusyRetryDelay rather than added to it, so that
// maxLeaseBusyRetries attempts still take about as long as they did
// before. A jitter that only ever extended the delay would quietly double
// how long a caller waits before a busy database is reported as an error.
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
			"longer what maxLeaseBusyRetries was tuned for", mean, leaseBusyRetryDelay)
	}
}
