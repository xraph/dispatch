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

// TestReclaimExpiredLeasesNonPositiveLimitReclaimsNothing pins the unified
// non-positive-limit contract for job.LeaseStore.ReclaimExpiredLeases: a
// limit <= 0 claims nothing and returns (nil, nil), and — critically —
// leaves the expired job still reclaimable, so a later call with a
// positive limit still returns it.
func TestReclaimExpiredLeasesNonPositiveLimitReclaimsNothing(t *testing.T) {
	ctx := context.Background()

	for _, limit := range []int{0, -1} {
		s := memory.New()

		j := storetest.RunningJob("expired", "reclaim-nonpositive", 0)
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
