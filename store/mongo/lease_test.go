package mongo_test

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/storetest"
)

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

	// The two cases below need their lease fields set up before they can be
	// listed alongside the rest.
	live := runningJob("live-lease", time.Minute)
	until := time.Now().UTC().Add(10 * time.Minute)
	live.LeaseExpiresAt = &until
	live.LeaseEpoch = 1
	ageless := runningJob("no-times", 0)

	type leaseCase struct {
		j    *job.Job
		want bool
		why  string
	}

	cases := []leaseCase{
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
		{j: live, want: false, why: "holds a lease that has not lapsed"},
		{j: ageless, want: false, why: "no timestamp to establish age from"},
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
