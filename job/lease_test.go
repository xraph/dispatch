package job_test

import (
	"testing"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

func TestWithLeaseTTL(t *testing.T) {
	tests := []struct {
		name string
		give time.Duration
		want time.Duration
	}{
		{name: "positive is applied", give: 6 * time.Hour, want: 6 * time.Hour},
		// A zero or negative TTL would mean the lease expires the instant it
		// is granted, so every job would be reclaimed before its first
		// heartbeat and nothing would ever complete. Ignore it rather than
		// persist it.
		{name: "zero keeps the default", give: 0, want: 0},
		{name: "negative keeps the default", give: -time.Second, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := job.DefaultOptions()
			job.WithLeaseTTL(tt.give)(&opts)

			if opts.LeaseTTL != tt.want {
				t.Errorf("LeaseTTL = %v, want %v", opts.LeaseTTL, tt.want)
			}
		})
	}
}

func TestDefaultOptions_LeaseTTLIsUnsetByDefault(t *testing.T) {
	// Zero means "use the pool's default", which preserves the existing
	// StaleJobThreshold semantics for every definition that says nothing.
	if got := job.DefaultOptions().LeaseTTL; got != 0 {
		t.Errorf("default LeaseTTL = %v, want 0", got)
	}
}

func TestLease_IsExpired(t *testing.T) {
	base := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name  string
		lease job.Lease
		now   time.Time
		want  bool
	}{
		{
			name:  "not yet expired",
			lease: job.Lease{ExpiresAt: base.Add(time.Minute)},
			now:   base,
			want:  false,
		},
		{
			name:  "expired",
			lease: job.Lease{ExpiresAt: base.Add(-time.Second)},
			now:   base,
			want:  true,
		},
		{
			name:  "exactly at expiry is expired",
			lease: job.Lease{ExpiresAt: base},
			now:   base,
			want:  true,
		},
		{
			// A zero ExpiresAt means no lease was ever granted. Treating it
			// as expired would let the reclaim loop steal jobs that were
			// never leased, so it must read as "not held" rather than
			// "expired".
			name:  "zero expiry is never expired",
			lease: job.Lease{},
			now:   base,
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.lease.IsExpired(tt.now); got != tt.want {
				t.Errorf("IsExpired(%v) = %v, want %v", tt.now, got, tt.want)
			}
		})
	}
}

func TestJob_CarriesLeaseFields(t *testing.T) {
	expires := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	j := &job.Job{
		ID:             id.NewJobID(),
		LeaseEpoch:     3,
		LeaseExpiresAt: &expires,
		LeaseTTL:       6 * time.Hour,
		EvictCount:     2,
	}

	if j.ID.IsNil() {
		t.Errorf("ID = nil, want a generated ID")
	}
	if j.LeaseEpoch != 3 {
		t.Errorf("LeaseEpoch = %d, want 3", j.LeaseEpoch)
	}
	if j.LeaseExpiresAt == nil || !j.LeaseExpiresAt.Equal(expires) {
		t.Errorf("LeaseExpiresAt = %v, want %v", j.LeaseExpiresAt, expires)
	}
	if j.LeaseTTL != 6*time.Hour {
		t.Errorf("LeaseTTL = %v, want %v", j.LeaseTTL, 6*time.Hour)
	}
	if j.EvictCount != 2 {
		t.Errorf("EvictCount = %d, want 2", j.EvictCount)
	}
}
