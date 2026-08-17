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

// TestReclaimAdoptsRunningJobsWithoutLease covers a running job carrying
// no lease at all. Two things produce one. A job already running when a
// fleet upgraded to a lease-aware build has no lease_expires_at, and
// job.Lease.IsExpired reports false for a zero expiry, so reclamation
// skips it forever while dequeue — which claims only pending and retrying
// rows — never looks at it again. A caller claiming through job.Store
// without lease options produces the same row shape at any time, because
// DequeueOpts.Grants() is false when LeaseUntil is zero.
//
// The negative cases carry the safety argument and matter more than the
// positive ones: the second kind of row is perfectly healthy, and evicting
// live work would be worse than the bug being fixed. Silence is the only
// thing separating the two.
//
// Both null shapes are exercised because this collection genuinely holds
// both, for the reason documented at jobModel.ResourceRequests: EnqueueJob
// goes through grove's structToMapInsert and writes an explicit BSON null,
// while UpdateJob hands the struct to the driver's own encoder, which
// honors "omitempty" and drops the key. A filter matching only one of them
// would strand half the collection.
func TestReclaimAdoptsRunningJobsWithoutLease(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)
	ctx := context.Background()
	col := rawDatabase(t, uri).Collection("dispatch_jobs")

	withHeartbeat := func(name string, startedAgo, beatAgo time.Duration) *job.Job {
		j := runningJob(name, startedAgo)
		beat := time.Now().UTC().Add(-beatAgo)
		j.HeartbeatAt = &beat

		return j
	}

	cases := []struct {
		j    *job.Job
		want bool
		why  string
	}{
		{
			j:    withHeartbeat("stale-heartbeat", 30*time.Minute, 20*time.Minute),
			want: true,
			why:  "abandoned by a worker that stopped reporting",
		},
		{
			// Old claim, current heartbeat: pins that heartbeat_at wins
			// over started_at rather than both needing to be fresh.
			j:    withHeartbeat("fresh-heartbeat", 30*time.Minute, 0),
			want: false,
			why:  "still reporting, so it belongs to a healthy worker",
		},
		{
			j:    runningJob("no-heartbeat-old-start", 20*time.Minute),
			want: true,
			why:  "claimed long ago and never heartbeated: died before its first beat",
		},
		{
			j:    runningJob("no-heartbeat-fresh-start", 0),
			want: false,
			why:  "just claimed; its first heartbeat is not due yet",
		},
	}

	live := runningJob("live-lease", time.Minute)
	until := time.Now().UTC().Add(10 * time.Minute)
	live.LeaseExpiresAt = &until
	live.LeaseEpoch = 1
	ageless := runningJob("no-times", 0)
	for _, extra := range []struct {
		j    *job.Job
		want bool
		why  string
	}{
		{live, false, "holds a lease that has not lapsed"},
		{ageless, false, "no timestamp to establish age from"},
	} {
		cases = append(cases, extra)
	}

	for _, c := range cases {
		if err := s.EnqueueJob(ctx, c.j); err != nil {
			t.Fatalf("enqueue %s: %v", c.j.Name, err)
		}
	}

	// The ABSENT-key shape, on a row that must still be adopted. Enqueue
	// wrote an explicit null for every row above; this is the only way to
	// produce the other shape, and a $exists-based filter would miss it.
	if _, err := col.UpdateOne(ctx,
		bson.M{"_id": cases[2].j.ID.String()},
		bson.M{"$unset": bson.M{"lease_expires_at": ""}},
	); err != nil {
		t.Fatalf("unset lease_expires_at: %v", err)
	}
	if _, err := col.UpdateOne(ctx,
		bson.M{"_id": ageless.ID.String()},
		bson.M{"$unset": bson.M{"started_at": "", "heartbeat_at": ""}},
	); err != nil {
		t.Fatalf("unset timestamps: %v", err)
	}

	reclaimed, err := s.ReclaimExpiredLeases(ctx, 100)
	if err != nil {
		t.Fatalf("ReclaimExpiredLeases: %v", err)
	}

	for _, c := range cases {
		got := storetest.Contains(reclaimed, c.j.ID)
		if got == c.want {
			continue
		}
		if c.want {
			t.Errorf("%s was not reclaimed but should have been: %s", c.j.Name, c.why)
		} else {
			t.Errorf("%s was reclaimed but must not be: %s", c.j.Name, c.why)
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

func TestDLQConformance(t *testing.T) {
	uri := startMongo(t)

	storetest.RunDLQSuite(t, func(t *testing.T) storetest.DLQStore {
		t.Helper()

		return openStore(t, uri)
	})
}
