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
// lease-granting paths: DequeueLeased and ReclaimExpiredLeases both used to
// hand back a job built from a shallow struct copy, aliasing Resources,
// ResourceLimits, Payload, and ArtifactBindings against the stored job. A
// worker mutating its leased job's Resources would silently rewrite the
// stored requirement.
//
// The shared lease conformance suite in store/storetest/lease.go does not
// check this — it runs against backends where a shallow struct copy isn't
// even the mechanism, so this case lives here instead.
func TestLeaseStoreDoesNotAliasResourceMap(t *testing.T) {
	t.Run("DequeueLeased", func(t *testing.T) {
		st := memory.New()
		ctx := context.Background()
		worker := id.NewWorkerID()
		const queue = "lease-alias-dequeue"

		j := storetest.PendingJob("alias-dequeue", queue, 0)
		j.Resources = resource.Set{resource.CPU: 1000, resource.Memory: 8 << 30}
		if err := st.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("EnqueueJob() error = %v", err)
		}

		got, err := st.DequeueLeased(ctx, []string{queue}, 1, worker, time.Now().UTC().Add(time.Minute))
		if err != nil {
			t.Fatalf("DequeueLeased() error = %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("DequeueLeased() returned %d jobs, want 1", len(got))
		}

		// A caller mutating what DequeueLeased handed back must not rewrite
		// the stored job.
		got[0].Resources[resource.Memory] = 1

		stored, err := st.GetJob(ctx, j.ID)
		if err != nil {
			t.Fatalf("GetJob() error = %v", err)
		}
		if stored.Resources[resource.Memory] != 8<<30 {
			t.Fatalf("DequeueLeased aliased the stored map: memory = %d",
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
