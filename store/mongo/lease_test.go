package mongo_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/storetest"
)

// TestReclaimExpiredLeasesNonPositiveLimitReturnsNothing pins the
// documented non-positive-limit behaviour of the mongo backend (see
// job.LeaseStore.ReclaimExpiredLeases): `if limit <= 0 { return nil, nil }`
// runs before any query, so limit == 0 and limit < 0 both reclaim
// nothing and leave every running job untouched.
//
// This guard is the one that had to be added (commit 6644972) to stop
// make([]*Job, 0, limit) panicking on a negative capacity — it does not
// mean mongo chose "returns nothing" as a considered semantics, only that
// the fix landed there. See the doc comment for the full story.
func TestReclaimExpiredLeasesNonPositiveLimitReturnsNothing(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)
	ctx := context.Background()

	for _, limit := range []int{0, -1} {
		j := storetest.RunningJob("expired", fmt.Sprintf("reclaim-nothing-%d", limit), 0)
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
			t.Errorf("limit=%d: State = %s, want still running (nothing reclaimed)", limit, after.State)
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
