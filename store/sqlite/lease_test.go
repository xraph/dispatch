package sqlite_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/storetest"
)

// TestReclaimExpiredLeasesNonPositiveLimitReclaimsNothing pins the unified
// non-positive-limit contract for job.LeaseStore.ReclaimExpiredLeases: a
// limit <= 0 claims nothing and returns (nil, nil), and — critically —
// leaves the expired job still reclaimable, so a later call with a
// positive limit still returns it.
//
// The negative case is the one that matters here: SQLite itself defines a
// negative LIMIT as "no limit", so before the guard `ReclaimExpiredLeases`
// with a negative limit reclaimed everything rather than nothing.
func TestReclaimExpiredLeasesNonPositiveLimitReclaimsNothing(t *testing.T) {
	s := openSqliteStore(t)
	ctx := context.Background()

	for _, limit := range []int{0, -1} {
		j := storetest.RunningJob("expired", fmt.Sprintf("reclaim-nonpositive-%d", limit), 0)
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("limit=%d: enqueue: %v", limit, err)
		}

		got, err := s.ReclaimExpiredLeases(ctx, limit)
		if err != nil {
			t.Fatalf("limit=%d: ReclaimExpiredLeases: %v", limit, err)
		}
		if len(got) != 0 {
			t.Fatalf("limit=%d: reclaimed %d jobs, want 0", limit, len(got))
		}

		after, err := s.GetJob(ctx, j.ID)
		if err != nil {
			t.Fatalf("limit=%d: get: %v", limit, err)
		}
		if after.State != job.StateRunning {
			t.Fatalf("limit=%d: State = %s, want still running (nothing reclaimed)", limit, after.State)
		}

		// The job must still be reclaimable: a non-positive limit must not
		// have silently consumed it.
		reclaimed, err := s.ReclaimExpiredLeases(ctx, 10)
		if err != nil {
			t.Fatalf("limit=%d: follow-up ReclaimExpiredLeases: %v", limit, err)
		}
		if !storetest.Contains(reclaimed, j.ID) {
			t.Fatalf("limit=%d: job not reclaimed by a follow-up call with a positive limit", limit)
		}
	}
}

func TestLeaseConformance(t *testing.T) {
	// openSqliteStore already opens a migrated store on a per-test temp
	// directory (store/sqlite/reap_test.go:19), so every subtest gets its
	// own database for free.
	storetest.RunLeaseSuite(t, func(t *testing.T) storetest.LeaseStore {
		t.Helper()

		return openSqliteStore(t)
	})
}

func TestDLQConformance(t *testing.T) {
	storetest.RunDLQSuite(t, func(t *testing.T) storetest.DLQStore {
		t.Helper()

		return openSqliteStore(t)
	})
}
