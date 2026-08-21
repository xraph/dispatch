package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/store/storetest"
)

// findJob returns the job with the given ID from jobs, or nil.
//
// ReclaimExpiredLeases is not queue-scoped (see storetest.Contains), so a
// test asserting on a specific job must find it by ID rather than assume
// it is alone in the result.
func findJob(jobs []*job.Job, jobID id.JobID) *job.Job {
	for _, j := range jobs {
		if j.ID == jobID {
			return j
		}
	}

	return nil
}

func TestLeaseConformance(t *testing.T) {
	storetest.RunLeaseSuite(t, func(t *testing.T) storetest.LeaseStore {
		t.Helper()

		return memory.New()
	})
}

// TestLeaseStoreDoesNotAliasResourceMap covers the same class of bug as
// TestMemoryStoreDoesNotAliasResourceMap (resource_test.go), but for the
// lease-granting paths: the leased claim and ReclaimExpiredLeases both
// used to hand back a job built from a shallow struct copy, aliasing
// Resources, ResourceLimits, Payload, and ArtifactBindings against the
// stored job. A worker mutating its leased job's Resources would silently
// rewrite the stored requirement.
//
// The shared lease conformance suite in store/storetest/lease.go does not
// check this — it runs against backends where a shallow struct copy isn't
// even the mechanism, so this case lives here instead.
func TestLeaseStoreDoesNotAliasResourceMap(t *testing.T) {
	t.Run("DequeueJobsWithGrant", func(t *testing.T) {
		st := memory.New()
		ctx := context.Background()
		worker := id.NewWorkerID()
		const queue = "lease-alias-dequeue"

		j := storetest.PendingJob("alias-dequeue", queue, 0)
		j.Resources = resource.Set{resource.CPU: 1000, resource.Memory: 8 << 30}
		if err := st.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("EnqueueJob() error = %v", err)
		}

		got, err := st.DequeueJobs(ctx, job.DequeueOpts{
			Queues:     []string{queue},
			Limit:      1,
			WorkerID:   worker,
			LeaseUntil: time.Now().UTC().Add(time.Minute),
		})
		if err != nil {
			t.Fatalf("DequeueJobs() error = %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("DequeueJobs() returned %d jobs, want 1", len(got))
		}

		// A caller mutating what the claim handed back must not rewrite the
		// stored job.
		got[0].Resources[resource.Memory] = 1

		stored, err := st.GetJob(ctx, j.ID)
		if err != nil {
			t.Fatalf("GetJob() error = %v", err)
		}
		if stored.Resources[resource.Memory] != 8<<30 {
			t.Fatalf("the leased claim aliased the stored map: memory = %d",
				stored.Resources[resource.Memory])
		}
	})

	t.Run("ReclaimExpiredLeases", func(t *testing.T) {
		st := memory.New()
		ctx := context.Background()
		const queue = "lease-alias-reclaim"

		j := storetest.RunningJob("alias-reclaim", queue, 0)
		j.Resources = resource.Set{resource.CPU: 1000, resource.Memory: 8 << 30}
		if err := st.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("EnqueueJob() error = %v", err)
		}

		reclaimed, err := st.ReclaimExpiredLeases(ctx, 100)
		if err != nil {
			t.Fatalf("ReclaimExpiredLeases() error = %v", err)
		}
		if !storetest.Contains(reclaimed, j.ID) {
			t.Fatalf("reclaimed set does not contain %s", j.ID)
		}

		target := findJob(reclaimed, j.ID)
		if target == nil {
			t.Fatalf("could not find reclaimed job %s in result", j.ID)
		}

		// A caller mutating what ReclaimExpiredLeases handed back must not
		// rewrite the stored job.
		target.Resources[resource.Memory] = 1

		stored, err := st.GetJob(ctx, j.ID)
		if err != nil {
			t.Fatalf("GetJob() error = %v", err)
		}
		if stored.Resources[resource.Memory] != 8<<30 {
			t.Fatalf("ReclaimExpiredLeases aliased the stored map: memory = %d",
				stored.Resources[resource.Memory])
		}
	})
}

// TestReclaimAdoptsRunningJobsWithoutLease covers a running job carrying
// no lease at all, matching the rule the four persistent backends apply.
//
// Memory has no upgrade to survive, since it loses every row on restart,
// so the pre-lease-build half of the problem cannot reach it. The other
// half can: DequeueOpts.Grants() is false whenever LeaseUntil is zero, so
// a caller claiming through job.Store without lease options holds a
// running job with no lease, and if that caller stops without completing
// the job, nothing reclaims it for the life of the process. Reclamation
// skips a zero expiry and dequeue claims only pending and retrying rows.
//
// The negative cases carry the safety argument: a null expiry does not by
// itself mean the job was abandoned, so silence is what separates a dead
// claim from a live one.
func TestReclaimAdoptsRunningJobsWithoutLease(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	now := time.Now().UTC()

	// unleased builds a running job with no lease fields at all, the shape
	// a claim through DequeueJobs without lease options leaves behind.
	unleased := func(name string, started, beat *time.Time) *job.Job {
		return &job.Job{
			ID:          id.NewJobID(),
			Name:        name,
			Queue:       "default",
			Payload:     []byte(`{}`),
			State:       job.StateRunning,
			MaxRetries:  3,
			RunAt:       now,
			StartedAt:   started,
			HeartbeatAt: beat,
		}
	}

	at := func(d time.Duration) *time.Time {
		t := now.Add(-d)

		return &t
	}

	silent := job.UnleasedReclaimGrace + time.Minute

	cases := []struct {
		j    *job.Job
		want bool
		why  string
	}{
		{
			j:    unleased("silent-heartbeat", at(2*silent), at(silent)),
			want: true,
			why:  "abandoned by a caller that stopped reporting",
		},
		{
			// Old claim, current heartbeat: pins that heartbeat wins over
			// started_at rather than both needing to be fresh.
			j:    unleased("fresh-heartbeat", at(2*silent), at(0)),
			want: false,
			why:  "still reporting, so it belongs to a live caller",
		},
		{
			j:    unleased("no-heartbeat-old-start", at(silent), nil),
			want: true,
			why:  "claimed long ago and never heartbeated",
		},
		{
			j:    unleased("no-heartbeat-fresh-start", at(0), nil),
			want: false,
			why:  "just claimed; its first heartbeat is not due yet",
		},
		{
			j:    unleased("no-times", nil, nil),
			want: false,
			why:  "no timestamp to establish age from",
		},
	}

	for _, c := range cases {
		if err := s.EnqueueJob(ctx, c.j); err != nil {
			t.Fatalf("enqueue %s: %v", c.j.Name, err)
		}
	}

	reclaimed, err := s.ReclaimExpiredLeases(ctx, 100)
	if err != nil {
		t.Fatalf("ReclaimExpiredLeases: %v", err)
	}

	for _, c := range cases {
		got := findJob(reclaimed, c.j.ID) != nil
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

// TestClearOwnershipStopsTheRequeueLivelock reproduces what a stale lease
// column does to a job returned to pending from outside the lease
// machinery, which is the shape api.retryJob wrote before it called
// job.Job.ClearOwnership.
//
// UpdateJob writes the whole row on every backend, lease columns included,
// so a failed job put back to pending keeps the LeaseExpiresAt of its
// failed run. Nothing notices while it sits pending. It goes wrong at the
// next claim that grants no lease, which is a supported call
// (DequeueOpts.Grants() is false whenever LeaseUntil is zero): that claim
// writes state and worker but never touches the expiry, so the job enters
// running already holding a lapsed lease and the next sweep takes it
// straight back. Claimed, reclaimed, claimed again, forever.
//
// The memory store is used because the bug is in the shared job row rather
// than in any one backend's SQL, and this keeps the reproduction in-process.
func TestClearOwnershipStopsTheRequeueLivelock(t *testing.T) {
	ctx := context.Background()

	// requeued builds the row a retry path produces, with or without the
	// ownership reset, and returns it after one no-lease claim.
	requeued := func(t *testing.T, resetOwnership bool) (*memory.Store, *job.Job) {
		t.Helper()

		s := memory.New()
		lapsed := time.Now().UTC().Add(-time.Hour)
		started := lapsed.Add(-time.Minute)
		j := &job.Job{
			ID:             id.NewJobID(),
			Name:           "retried",
			Queue:          "default",
			Payload:        []byte(`{}`),
			State:          job.StateFailed,
			MaxRetries:     3,
			WorkerID:       id.NewWorkerID(),
			StartedAt:      &started,
			HeartbeatAt:    &lapsed,
			LeaseExpiresAt: &lapsed,
			LeaseEpoch:     4,
		}
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("enqueue: %v", err)
		}

		// What api.retryJob does to a failed job.
		j.State = job.StatePending
		j.RetryCount = 0
		j.LastError = ""
		j.RunAt = time.Now().UTC()
		j.CompletedAt = nil
		if resetOwnership {
			j.ClearOwnership()
		} else {
			j.StartedAt = nil // the old code cleared only this
		}
		if err := s.UpdateJob(ctx, j); err != nil {
			t.Fatalf("update: %v", err)
		}

		// A claim that grants no lease: legal, and it never writes the
		// lease columns.
		claimed, err := s.DequeueJobs(ctx, job.DequeueOpts{
			Queues: []string{"default"},
			Limit:  1,
		})
		if err != nil {
			t.Fatalf("dequeue: %v", err)
		}
		if len(claimed) != 1 {
			t.Fatalf("claimed %d jobs, want 1", len(claimed))
		}

		return s, j
	}

	t.Run("without the reset the claim is immediately reclaimed", func(t *testing.T) {
		s, j := requeued(t, false)

		reclaimed, err := s.ReclaimExpiredLeases(ctx, 10)
		if err != nil {
			t.Fatalf("ReclaimExpiredLeases: %v", err)
		}
		if findJob(reclaimed, j.ID) == nil {
			t.Skip("the stale expiry no longer reaches running; this reproduction is obsolete")
		}
	})

	t.Run("with the reset the claim survives", func(t *testing.T) {
		s, j := requeued(t, true)

		reclaimed, err := s.ReclaimExpiredLeases(ctx, 10)
		if err != nil {
			t.Fatalf("ReclaimExpiredLeases: %v", err)
		}
		if findJob(reclaimed, j.ID) != nil {
			t.Fatal("a freshly claimed job was reclaimed: the retry path left a lapsed " +
				"lease on the row, so it can never run to completion")
		}

		got, err := s.GetJob(ctx, j.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.State != job.StateRunning {
			t.Errorf("State = %s, want running", got.State)
		}
		// The fencing token must not have been rolled back by the reset.
		if got.LeaseEpoch < 4 {
			t.Errorf("LeaseEpoch = %d, want >= 4: a fencing token must never move backwards",
				got.LeaseEpoch)
		}
	})
}

func TestDLQConformance(t *testing.T) {
	storetest.RunDLQSuite(t, func(t *testing.T) storetest.DLQStore {
		t.Helper()

		return memory.New()
	})
}
