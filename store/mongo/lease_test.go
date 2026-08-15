package mongo_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

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

// TestMigrateBackfillsRunningJobsWithoutLease covers the fleet upgrade: a
// job that was already running when the lease feature shipped has no
// lease_expires_at at all, and every backend's ReclaimExpiredLeases
// requires a non-null expiry. job.Lease.IsExpired deliberately reports
// false for a zero expiry, the pool no longer calls ReapStaleJobs for a
// lease-capable store, and dequeue claims only pending and retrying rows —
// so without a backfill such a job is invisible to every recovery path and
// holds its slot forever.
//
// The assertion is that ReclaimExpiredLeases actually COLLECTS the row,
// not that lease_expires_at became non-null. That distinction is the whole
// point: when the same bug was fixed for SQLite in 245aab6 the first
// backfill wrote a value that was non-null and still permanently
// unreclaimable, and only this stronger assertion caught it.
//
// Both null shapes are exercised because this collection genuinely
// contains both, for the reason documented at jobModel.ResourceRequests:
// EnqueueJob goes through grove's structToMapInsert and writes an explicit
// BSON null, while UpdateJob hands the struct to the driver's own encoder,
// which honors "omitempty" and drops the key entirely. A filter that
// matched only one of them would strand half the fleet's jobs.
func TestMigrateBackfillsRunningJobsWithoutLease(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)
	ctx := context.Background()
	col := rawDatabase(t, uri).Collection("dispatch_jobs")

	// heartbeat_at wins the coalesce: a worker that was alive and
	// reporting right up to the upgrade.
	beat := runningJob("pre-upgrade-heartbeat", 5*time.Minute)
	hb := time.Now().UTC().Add(-2 * time.Minute)
	beat.HeartbeatAt = &hb

	// started_at is the fallback: a worker that died before its first
	// heartbeat. This one also gets the ABSENT-key shape rather than the
	// explicit null.
	start := runningJob("pre-upgrade-started", 3*time.Minute)

	// Neither timestamp survives, so the backfill must fall back to its
	// last resort. This is the arm most likely to be silently wrong,
	// because nothing in the row constrains what gets written.
	bare := runningJob("pre-upgrade-no-times", time.Minute)

	for _, j := range []*job.Job{beat, start, bare} {
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("enqueue %s: %v", j.Name, err)
		}
	}
	if _, err := col.UpdateOne(ctx,
		bson.M{"_id": start.ID.String()},
		bson.M{"$unset": bson.M{"lease_expires_at": ""}},
	); err != nil {
		t.Fatalf("unset lease_expires_at: %v", err)
	}
	if _, err := col.UpdateOne(ctx,
		bson.M{"_id": bare.ID.String()},
		bson.M{"$unset": bson.M{"started_at": "", "heartbeat_at": ""}},
	); err != nil {
		t.Fatalf("unset timestamps: %v", err)
	}

	// Precondition: this is the bug. Every one of these rows is running
	// and none of them is reachable by reclamation.
	stranded, err := s.ReclaimExpiredLeases(ctx, 10)
	if err != nil {
		t.Fatalf("pre-migrate ReclaimExpiredLeases: %v", err)
	}
	for _, j := range []*job.Job{beat, start, bare} {
		if storetest.Contains(stranded, j.ID) {
			t.Fatalf("precondition: %s was reclaimable before the backfill ran", j.Name)
		}
	}

	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	reclaimed, err := s.ReclaimExpiredLeases(ctx, 10)
	if err != nil {
		t.Fatalf("post-migrate ReclaimExpiredLeases: %v", err)
	}
	for _, j := range []*job.Job{beat, start, bare} {
		if !storetest.Contains(reclaimed, j.ID) {
			t.Errorf("%s was not reclaimed after the backfill; it is stranded", j.Name)
		}
	}
}

// TestMigrateBackfillLeavesLeasedJobsAlone pins the other half of the
// contract: the backfill must touch only rows with no expiry at all. A job
// holding a live lease belongs to a healthy worker, and rewriting its
// expiry would evict it mid-run.
func TestMigrateBackfillLeavesLeasedJobsAlone(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)
	ctx := context.Background()

	live := runningJob("live-lease", time.Minute)
	until := time.Now().UTC().Add(10 * time.Minute)
	live.LeaseExpiresAt = &until
	live.LeaseEpoch = 1
	if err := s.EnqueueJob(ctx, live); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	after, err := s.GetJob(ctx, live.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	// Compared at millisecond granularity because that is all a BSON
	// datetime carries; the sub-millisecond difference is the round trip,
	// not the backfill.
	if after.LeaseExpiresAt == nil || after.LeaseExpiresAt.UnixMilli() != until.UnixMilli() {
		t.Fatalf("LeaseExpiresAt = %v, want it untouched at %v", after.LeaseExpiresAt, until)
	}

	reclaimed, err := s.ReclaimExpiredLeases(ctx, 10)
	if err != nil {
		t.Fatalf("ReclaimExpiredLeases: %v", err)
	}
	if storetest.Contains(reclaimed, live.ID) {
		t.Fatal("a job holding a live lease was reclaimed after the backfill")
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
