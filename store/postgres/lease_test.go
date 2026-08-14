package postgres_test

import (
	"context"
	"testing"

	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/storetest"
)

// TestReclaimExpiredLeasesZeroLimitReturnsNothing pins the documented
// limit == 0 behaviour of the postgres backend (see
// job.LeaseStore.ReclaimExpiredLeases): limit is bound straight into
// `LIMIT $1` with no guard, and `LIMIT 0` matches no row, so nothing is
// reclaimed and the running job is left untouched.
func TestReclaimExpiredLeasesZeroLimitReturnsNothing(t *testing.T) {
	dsn := startWakePostgres(t)
	s := openWakeStore(t, dsn)
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

// TestReclaimExpiredLeasesNegativeLimitErrors pins the documented
// limit < 0 behaviour of the postgres backend (see
// job.LeaseStore.ReclaimExpiredLeases): Postgres itself rejects a
// negative LIMIT bound value with "LIMIT must not be negative"
// (SQLSTATE 2201W), so the call returns an error rather than any result.
func TestReclaimExpiredLeasesNegativeLimitErrors(t *testing.T) {
	dsn := startWakePostgres(t)
	s := openWakeStore(t, dsn)
	ctx := context.Background()

	if _, err := s.ReclaimExpiredLeases(ctx, -1); err == nil {
		t.Fatal(`ReclaimExpiredLeases(-1) = nil error, want the Postgres "LIMIT must not be negative" error`)
	}
}

func TestLeaseConformance(t *testing.T) {
	dsn := startWakePostgres(t)

	storetest.RunLeaseSuite(t, func(t *testing.T) storetest.LeaseStore {
		t.Helper()

		return openWakeStore(t, dsn)
	})
}
