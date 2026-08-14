package sqlite_test

import (
	"context"
	"testing"

	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/storetest"
)

// TestReclaimExpiredLeasesZeroLimitReturnsNothing pins the documented
// limit == 0 behaviour of the sqlite backend (see
// job.LeaseStore.ReclaimExpiredLeases): limit is bound straight into
// `LIMIT ?` with no guard, and `LIMIT 0` matches no row, so nothing is
// reclaimed and the running job is left untouched.
func TestReclaimExpiredLeasesZeroLimitReturnsNothing(t *testing.T) {
	s := openSqliteStore(t)
	ctx := context.Background()

	j := storetest.RunningJob("expired", "reclaim-zero", 0)
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, err := s.ReclaimExpiredLeases(ctx, 0)
	if err != nil {
		t.Fatalf("ReclaimExpiredLeases: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("reclaimed %d jobs, want 0", len(got))
	}

	after, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if after.State != job.StateRunning {
		t.Errorf("State = %s, want still running (nothing reclaimed)", after.State)
	}
}

// TestReclaimExpiredLeasesNegativeLimitIsUnlimited pins the documented
// limit < 0 behaviour of the sqlite backend (see
// job.LeaseStore.ReclaimExpiredLeases): SQLite itself defines a negative
// LIMIT as "no limit", so every expired running job is reclaimed.
func TestReclaimExpiredLeasesNegativeLimitIsUnlimited(t *testing.T) {
	s := openSqliteStore(t)
	ctx := context.Background()

	a := storetest.RunningJob("a", "reclaim-negative-unlimited", 0)
	b := storetest.RunningJob("b", "reclaim-negative-unlimited", 0)
	if err := s.EnqueueJob(ctx, a); err != nil {
		t.Fatalf("enqueue a: %v", err)
	}
	if err := s.EnqueueJob(ctx, b); err != nil {
		t.Fatalf("enqueue b: %v", err)
	}

	got, err := s.ReclaimExpiredLeases(ctx, -1)
	if err != nil {
		t.Fatalf("ReclaimExpiredLeases(-1): %v", err)
	}
	if !storetest.Contains(got, a.ID) || !storetest.Contains(got, b.ID) {
		t.Fatalf("reclaimed %d jobs, want both a and b reclaimed", len(got))
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
