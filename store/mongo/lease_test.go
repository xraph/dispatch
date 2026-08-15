package mongo_test

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
func TestReclaimExpiredLeasesNonPositiveLimitReclaimsNothing(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)
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
	// One container for the whole suite — startMongo spins a testcontainer
	// and doing that eleven times would dominate the runtime. The suite is
	// written to tolerate a shared store.
	uri := startMongo(t)

	storetest.RunLeaseSuite(t, func(t *testing.T) storetest.LeaseStore {
		t.Helper()

		return openStore(t, uri)
	})
}
