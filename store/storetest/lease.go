package storetest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

// RunLeaseSuite runs the lease conformance suite against a backend.
//
// newStore is called once per subtest. It may return the same underlying
// store every time: each case enqueues onto its own queue and asserts on
// the jobs it created, so cases do not interfere. That matters because
// starting a fresh Postgres or Redis container per subtest would dominate
// the runtime of the whole suite.
//
// The lease is GRANTED by job.Store.DequeueJobs, through
// job.DequeueOpts.WorkerID and LeaseUntil — there is no separate leased
// dequeue. That is why every case below claims with DequeueJobs: a
// backend cannot pass this suite with a grant path that skips the fit
// predicate, because there is only one path.
func RunLeaseSuite(t *testing.T, newStore func(t *testing.T) LeaseStore) {
	t.Helper()

	t.Run("DequeueGrantsLeaseAndBumpsEpoch", func(t *testing.T) {
		testDequeueGrantsLeaseAndBumpsEpoch(t, newStore(t))
	})
	t.Run("DequeueGrantsNoLeaseWhenLeaseUntilZero", func(t *testing.T) {
		testDequeueGrantsNoLeaseWhenLeaseUntilZero(t, newStore(t))
	})
	t.Run("DequeueRejectsLeaseWithoutWorker", func(t *testing.T) {
		testDequeueRejectsLeaseWithoutWorker(t, newStore(t))
	})
	t.Run("DequeueComposesBudgetAndLeaseGrant", func(t *testing.T) {
		testDequeueComposesBudgetAndLeaseGrant(t, newStore(t))
	})
	t.Run("RenewLeaseExtends", func(t *testing.T) {
		testRenewLeaseExtends(t, newStore(t))
	})
	t.Run("RenewLeaseRejectsStaleEpoch", func(t *testing.T) {
		testRenewLeaseRejectsStaleEpoch(t, newStore(t))
	})
	t.Run("RenewLeaseRejectsWrongWorker", func(t *testing.T) {
		testRenewLeaseRejectsWrongWorker(t, newStore(t))
	})
	t.Run("RenewLeaseRejectsMissingJob", func(t *testing.T) {
		testRenewLeaseRejectsMissingJob(t, newStore(t))
	})
	t.Run("ReclaimExpiredLeases", func(t *testing.T) {
		testReclaimExpiredLeases(t, newStore(t))
	})
	t.Run("ReclaimSkipsLiveLease", func(t *testing.T) {
		testReclaimSkipsLiveLease(t, newStore(t))
	})
	t.Run("ReclaimFencesPreviousHolder", func(t *testing.T) {
		testReclaimFencesPreviousHolder(t, newStore(t))
	})
	t.Run("ReclaimPreservesRetryCount", func(t *testing.T) {
		testReclaimPreservesRetryCount(t, newStore(t))
	})
	t.Run("ReclaimIsExclusive", func(t *testing.T) {
		testReclaimIsExclusive(t, newStore(t))
	})
	t.Run("ReclaimIsExclusiveUnderConcurrency", func(t *testing.T) {
		testReclaimIsExclusiveUnderConcurrency(t, newStore(t))
	})
	t.Run("LeaseTTLRoundTrips", func(t *testing.T) {
		testLeaseTTLRoundTrips(t, newStore(t))
	})
	t.Run("UpdateLeasedJobAppliesAtHeldEpoch", func(t *testing.T) {
		testUpdateLeasedJobAppliesAtHeldEpoch(t, newStore(t))
	})
	t.Run("UpdateLeasedJobRejectsStaleEpoch", func(t *testing.T) {
		testUpdateLeasedJobRejectsStaleEpoch(t, newStore(t))
	})
	t.Run("UpdateLeasedJobRejectsWrongWorker", func(t *testing.T) {
		testUpdateLeasedJobRejectsWrongWorker(t, newStore(t))
	})
	t.Run("UpdateLeasedJobRejectsWhenNoLongerRunning", func(t *testing.T) {
		testUpdateLeasedJobRejectsWhenNoLongerRunning(t, newStore(t))
	})
	t.Run("UpdateLeasedJobPreservesLeaseColumnsAgainstAStaleSnapshot", func(t *testing.T) {
		testUpdateLeasedJobPreservesLeaseColumnsAgainstAStaleSnapshot(t, newStore(t))
	})
	t.Run("UpdateLeasedJobRunnableWriteIsDequeueable", func(t *testing.T) {
		testUpdateLeasedJobRunnableWriteIsDequeueable(t, newStore(t))
	})
}

func testDequeueGrantsLeaseAndBumpsEpoch(t *testing.T, s LeaseStore) {
	ctx := context.Background()
	worker := id.NewWorkerID()
	until := time.Now().UTC().Add(time.Minute)
	const queue = "lease-grant"

	j := PendingJob("grant", queue, 0)
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:     []string{queue},
		Limit:      10,
		WorkerID:   worker,
		LeaseUntil: until,
	})
	if err != nil {
		t.Fatalf("DequeueJobs: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("DequeueJobs returned %d jobs, want 1", len(got))
	}

	d := got[0]
	if d.State != job.StateRunning {
		t.Errorf("State = %s, want %s", d.State, job.StateRunning)
	}
	if d.LeaseEpoch != 1 {
		t.Errorf("LeaseEpoch = %d, want 1", d.LeaseEpoch)
	}
	if d.WorkerID != worker {
		t.Errorf("WorkerID = %s, want %s", d.WorkerID, worker)
	}
	if d.LeaseExpiresAt == nil {
		t.Fatal("LeaseExpiresAt = nil, want the granted expiry")
	}
	if diff := d.LeaseExpiresAt.Sub(until); diff > time.Second || diff < -time.Second {
		t.Errorf("LeaseExpiresAt = %v, want within 1s of %v", d.LeaseExpiresAt, until)
	}
	if d.StartedAt == nil {
		t.Error("StartedAt = nil, want it set at dequeue")
	}

	// The grant must have been PERSISTED by the claim, not merely decorated
	// onto the returned copy. A backend that granted as a follow-up write
	// would still pass every assertion above; this is the one that fails if
	// the row itself is running with no lease — the state no reclaimer can
	// see, because every backend ignores a null expiry, and therefore the
	// state nothing recovers from. See job.DequeueOpts.LeaseUntil.
	stored, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if stored.LeaseEpoch != d.LeaseEpoch {
		t.Errorf("stored LeaseEpoch = %d, want %d", stored.LeaseEpoch, d.LeaseEpoch)
	}
	if stored.WorkerID != worker {
		t.Errorf("stored WorkerID = %s, want %s", stored.WorkerID, worker)
	}
	if stored.LeaseExpiresAt == nil {
		t.Error("stored LeaseExpiresAt = nil, want the granted expiry")
	}
}

// testDequeueGrantsNoLeaseWhenLeaseUntilZero pins the backward-
// compatibility guarantee: an opt-out caller must see exactly the
// behaviour DequeueJobs had before leases existed.
//
// Without this case a backend that granted unconditionally — writing the
// zero worker and bumping the epoch on every claim — passes every other
// case in this file, and every caller that never asked for a lease would
// have its jobs reclaimed out from under it on the next sweep.
func testDequeueGrantsNoLeaseWhenLeaseUntilZero(t *testing.T, s LeaseStore) {
	ctx := context.Background()
	const queue = "lease-not-granted"

	j := PendingJob("not-granted", queue, 0)
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Read the baseline back from the store rather than trusting the
	// in-memory job: a backend that defaults lease_epoch differently at
	// insert would otherwise look like it bumped.
	before, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("get before claim: %v", err)
	}

	// No WorkerID and no LeaseUntil — the opts a pool sends today.
	got, err := s.DequeueJobs(ctx, job.DequeueOpts{Queues: []string{queue}, Limit: 1})
	if err != nil {
		t.Fatalf("DequeueJobs: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("DequeueJobs returned %d jobs, want 1", len(got))
	}

	d := got[0]
	if d.State != job.StateRunning {
		t.Errorf("State = %s, want %s", d.State, job.StateRunning)
	}
	if d.LeaseEpoch != before.LeaseEpoch {
		t.Errorf("LeaseEpoch = %d, want it unchanged at %d", d.LeaseEpoch, before.LeaseEpoch)
	}
	if d.LeaseExpiresAt != nil {
		t.Errorf("LeaseExpiresAt = %v, want nil — no lease was asked for", d.LeaseExpiresAt)
	}
	if !d.WorkerID.IsNil() {
		t.Errorf("WorkerID = %s, want it unset — no lease was asked for", d.WorkerID)
	}

	stored, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("get after claim: %v", err)
	}
	if stored.LeaseEpoch != before.LeaseEpoch {
		t.Errorf("stored LeaseEpoch = %d, want it unchanged at %d",
			stored.LeaseEpoch, before.LeaseEpoch)
	}
	if stored.LeaseExpiresAt != nil {
		t.Errorf("stored LeaseExpiresAt = %v, want nil", stored.LeaseExpiresAt)
	}
	if !stored.WorkerID.IsNil() {
		t.Errorf("stored WorkerID = %s, want it unset", stored.WorkerID)
	}
}

// testDequeueRejectsLeaseWithoutWorker covers the one incoherent request:
// a grant with no holder.
//
// RenewLease matches on worker ID, so a lease held by the zero worker can
// never be renewed — the job would be claimed, expire, be reclaimed, and
// go round again forever. That presents as a queue that never drains
// rather than as an error, so the store refuses the claim instead.
func testDequeueRejectsLeaseWithoutWorker(t *testing.T, s LeaseStore) {
	ctx := context.Background()
	const queue = "lease-no-worker"

	j := PendingJob("no-worker", queue, 0)
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues: []string{queue},
		Limit:  1,
		// WorkerID deliberately unset.
		LeaseUntil: time.Now().UTC().Add(time.Minute),
	})
	if !errors.Is(err, job.ErrLeaseWithoutWorker) {
		t.Fatalf("DequeueJobs with LeaseUntil and no WorkerID = %v, want %v",
			err, job.ErrLeaseWithoutWorker)
	}
	if len(got) != 0 {
		t.Errorf("DequeueJobs returned %d jobs, want 0 — a refused claim must claim nothing",
			len(got))
	}

	// The refusal must come before any write: the job is still there for a
	// correctly configured worker to take.
	after, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if after.State != job.StatePending {
		t.Errorf("State = %s, want %s — the refused claim wrote to the job",
			after.State, job.StatePending)
	}
}

// testDequeueComposesBudgetAndLeaseGrant proves the two halves of
// DequeueOpts a production caller always sends together actually work
// together. worker/pool.go's fetchLoop sends Budget, CustomKeys,
// WorkerID, and LeaseUntil in ONE DequeueOpts on every call — there is no
// path that grants a lease without also carrying whatever budget the
// worker has — but RunDequeueSuite (job/store.go's resource-aware fit
// predicate) and the rest of this suite (the lease grant) had exercised
// those two contracts only apart. A backend could satisfy both suites
// while still getting the composed shape wrong, for example by granting
// the lease before the fit predicate ran, or by having the two features
// implemented against different code paths that silently disagree once
// both parameters are non-zero.
func testDequeueComposesBudgetAndLeaseGrant(t *testing.T, s LeaseStore) {
	const queue = "lease-and-budget"

	fits := newFitJob("fits", queue, resource.Set{resource.Memory: GiB}, withRunAtOffset(0))
	exceeds := newFitJob("exceeds", queue,
		resource.Set{resource.Memory: 8 * GiB}, withRunAtOffset(time.Minute))

	mustEnqueue(t, s, fits, exceeds)

	worker := id.NewWorkerID()
	until := time.Now().UTC().Add(time.Minute)

	got := mustDequeue(t, s, job.DequeueOpts{
		Queues:     []string{queue},
		Limit:      10,
		Budget:     resource.Set{resource.Memory: 4 * GiB},
		WorkerID:   worker,
		LeaseUntil: until,
	})

	// The fit predicate ran: only "fits" was claimed.
	wantExactly(t, got, "fits")

	d := got[0]
	if d.State != job.StateRunning {
		t.Errorf("State = %s, want %s", d.State, job.StateRunning)
	}
	if d.WorkerID != worker {
		t.Errorf("WorkerID = %s, want %s", d.WorkerID, worker)
	}
	if d.LeaseEpoch != 1 {
		t.Errorf("LeaseEpoch = %d, want 1", d.LeaseEpoch)
	}
	if d.LeaseExpiresAt == nil {
		t.Fatal("LeaseExpiresAt = nil, want the granted expiry")
	}
	if diff := d.LeaseExpiresAt.Sub(until); diff > time.Second || diff < -time.Second {
		t.Errorf("LeaseExpiresAt = %v, want within 1s of %v", d.LeaseExpiresAt, until)
	}

	// The lease grant did not bypass the fit predicate for the oversized
	// job: it stays fully pending, not merely unleased.
	wantStillClaimable(t, s, queue, "exceeds")
}

func testRenewLeaseExtends(t *testing.T, s LeaseStore) {
	ctx := context.Background()
	worker := id.NewWorkerID()
	now := time.Now().UTC()
	const queue = "lease-renew"

	j := PendingJob("renew", queue, 0)
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:     []string{queue},
		Limit:      1,
		WorkerID:   worker,
		LeaseUntil: now.Add(30 * time.Second),
	})
	if err != nil || len(got) != 1 {
		t.Fatalf("DequeueJobs: %v (n=%d)", err, len(got))
	}

	extended := now.Add(10 * time.Minute)
	if renewErr := s.RenewLease(ctx, got[0].ID, worker, got[0].LeaseEpoch, extended); renewErr != nil {
		t.Fatalf("RenewLease: %v", renewErr)
	}

	after, err := s.GetJob(ctx, got[0].ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if after.LeaseExpiresAt == nil {
		t.Fatal("LeaseExpiresAt = nil after renewal")
	}
	if diff := after.LeaseExpiresAt.Sub(extended); diff > time.Second || diff < -time.Second {
		t.Errorf("LeaseExpiresAt = %v, want within 1s of %v", after.LeaseExpiresAt, extended)
	}
	// Renewal must not bump the epoch — only grant and reclaim do. If it
	// did, the holder's own next renewal would fence itself.
	if after.LeaseEpoch != got[0].LeaseEpoch {
		t.Errorf("LeaseEpoch = %d after renewal, want it unchanged at %d",
			after.LeaseEpoch, got[0].LeaseEpoch)
	}
}

func testRenewLeaseRejectsStaleEpoch(t *testing.T, s LeaseStore) {
	ctx := context.Background()
	worker := id.NewWorkerID()
	now := time.Now().UTC()

	const queue = "lease-stale-epoch"

	j := PendingJob("stale-epoch", queue, 0)
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:     []string{queue},
		Limit:      1,
		WorkerID:   worker,
		LeaseUntil: now.Add(time.Minute),
	})
	if err != nil || len(got) != 1 {
		t.Fatalf("DequeueJobs: %v (n=%d)", err, len(got))
	}

	err = s.RenewLease(ctx, got[0].ID, worker, got[0].LeaseEpoch-1, now.Add(time.Hour))
	if !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("RenewLease with stale epoch = %v, want %v", err, job.ErrLeaseLost)
	}
}

func testRenewLeaseRejectsWrongWorker(t *testing.T, s LeaseStore) {
	ctx := context.Background()
	worker := id.NewWorkerID()
	other := id.NewWorkerID()
	now := time.Now().UTC()

	const queue = "lease-wrong-worker"

	j := PendingJob("wrong-worker", queue, 0)
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:     []string{queue},
		Limit:      1,
		WorkerID:   worker,
		LeaseUntil: now.Add(time.Minute),
	})
	if err != nil || len(got) != 1 {
		t.Fatalf("DequeueJobs: %v (n=%d)", err, len(got))
	}

	err = s.RenewLease(ctx, got[0].ID, other, got[0].LeaseEpoch, now.Add(time.Hour))
	if !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("RenewLease from another worker = %v, want %v", err, job.ErrLeaseLost)
	}
}

func testRenewLeaseRejectsMissingJob(t *testing.T, s LeaseStore) {
	ctx := context.Background()

	err := s.RenewLease(ctx, id.NewJobID(), id.NewWorkerID(), 1, time.Now().UTC().Add(time.Hour))
	if !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("RenewLease on a missing job = %v, want %v", err, job.ErrLeaseLost)
	}
}

func testReclaimExpiredLeases(t *testing.T, s LeaseStore) {
	ctx := context.Background()

	j := RunningJob("expired", "lease-reclaim", 0)
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, err := s.ReclaimExpiredLeases(ctx, 100)
	if err != nil {
		t.Fatalf("ReclaimExpiredLeases: %v", err)
	}
	if !Contains(got, j.ID) {
		t.Fatalf("reclaimed set does not contain %s", j.ID)
	}

	after, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if after.State != job.StatePending {
		t.Errorf("State = %s, want %s", after.State, job.StatePending)
	}
	if after.EvictCount != j.EvictCount+1 {
		t.Errorf("EvictCount = %d, want %d", after.EvictCount, j.EvictCount+1)
	}
	if !after.WorkerID.IsNil() {
		t.Errorf("WorkerID = %s, want it cleared", after.WorkerID)
	}
	if after.StartedAt != nil {
		t.Errorf("StartedAt = %v, want nil", after.StartedAt)
	}
	if after.HeartbeatAt != nil {
		t.Errorf("HeartbeatAt = %v, want nil", after.HeartbeatAt)
	}
	if after.LeaseExpiresAt != nil {
		t.Errorf("LeaseExpiresAt = %v, want nil", after.LeaseExpiresAt)
	}
}

func testReclaimSkipsLiveLease(t *testing.T, s LeaseStore) {
	ctx := context.Background()
	worker := id.NewWorkerID()
	const queue = "lease-live"

	j := PendingJob("live", queue, 0)
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:     []string{queue},
		Limit:      1,
		WorkerID:   worker,
		LeaseUntil: time.Now().UTC().Add(time.Hour),
	}); err != nil {
		t.Fatalf("DequeueJobs: %v", err)
	}

	got, err := s.ReclaimExpiredLeases(ctx, 100)
	if err != nil {
		t.Fatalf("ReclaimExpiredLeases: %v", err)
	}
	if Contains(got, j.ID) {
		t.Fatal("a live lease was reclaimed")
	}
}

func testReclaimFencesPreviousHolder(t *testing.T, s LeaseStore) {
	ctx := context.Background()
	worker := id.NewWorkerID()
	now := time.Now().UTC()

	// This is the split-brain case the whole phase exists to close. A
	// worker holds a lease, the lease expires while the worker is paused,
	// the reaper reclaims it — and the worker then wakes and tries to
	// renew. It must be refused.
	const queue = "lease-fenced"

	j := PendingJob("fenced", queue, 0)
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:     []string{queue},
		Limit:      1,
		WorkerID:   worker,
		LeaseUntil: now.Add(-time.Second),
	})
	if err != nil || len(got) != 1 {
		t.Fatalf("DequeueJobs: %v (n=%d)", err, len(got))
	}
	heldEpoch := got[0].LeaseEpoch

	reclaimed, err := s.ReclaimExpiredLeases(ctx, 100)
	if err != nil {
		t.Fatalf("ReclaimExpiredLeases: %v", err)
	}
	if !Contains(reclaimed, j.ID) {
		t.Fatalf("reclaimed set does not contain %s", j.ID)
	}

	afterReclaim, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if afterReclaim.LeaseEpoch <= heldEpoch {
		t.Errorf("LeaseEpoch = %d after reclaim, want > %d", afterReclaim.LeaseEpoch, heldEpoch)
	}

	// The zombie wakes up.
	err = s.RenewLease(ctx, j.ID, worker, heldEpoch, now.Add(time.Hour))
	if !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("zombie RenewLease = %v, want %v", err, job.ErrLeaseLost)
	}
}

func testReclaimPreservesRetryCount(t *testing.T, s LeaseStore) {
	ctx := context.Background()

	j := RunningJob("retry-untouched", "lease-retry", 0)
	j.RetryCount = 2
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if _, err := s.ReclaimExpiredLeases(ctx, 100); err != nil {
		t.Fatalf("ReclaimExpiredLeases: %v", err)
	}

	after, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	// Losing a lease is infrastructure, not a handler failure. Charging it
	// to the retry budget would DLQ a job that never once errored.
	if after.RetryCount != 2 {
		t.Errorf("RetryCount = %d, want it unchanged at 2", after.RetryCount)
	}
}

func testReclaimIsExclusive(t *testing.T, s LeaseStore) {
	ctx := context.Background()

	j := RunningJob("exclusive", "lease-exclusive", 0)
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Two pools reclaiming concurrently must not both take the job, or two
	// workers would run it. Sequential calls prove the same invariant: the
	// second call cannot see this job, because the first cleared its lease.
	first, err := s.ReclaimExpiredLeases(ctx, 100)
	if err != nil {
		t.Fatalf("first ReclaimExpiredLeases: %v", err)
	}
	second, err := s.ReclaimExpiredLeases(ctx, 100)
	if err != nil {
		t.Fatalf("second ReclaimExpiredLeases: %v", err)
	}

	if !Contains(first, j.ID) {
		t.Errorf("first reclaim did not take %s", j.ID)
	}
	if Contains(second, j.ID) {
		t.Errorf("second reclaim took %s again — reclamation is not exclusive", j.ID)
	}
}

func testReclaimIsExclusiveUnderConcurrency(t *testing.T, s LeaseStore) {
	ctx := context.Background()
	const (
		queue      = "lease-concurrent"
		jobCount   = 20
		reclaimers = 4

		// Deliberately not jobCount. Reclamation is not queue-scoped (see
		// Contains), and newStore may hand this case a store other cases
		// have already left expired jobs in. A limit of jobCount would
		// let four reclaimers fill their quotas with those instead, leave
		// some of `mine` unclaimed, and fail the n == 0 branch below for
		// a reason that has nothing to do with exclusivity. The limit
		// only has to be too large to ever be the binding constraint;
		// what this case measures is double-claiming, not throughput.
		reclaimLimit = 10_000
	)

	mine := make(map[id.JobID]bool, jobCount)
	for i := range jobCount {
		j := RunningJob(fmt.Sprintf("concurrent-%d", i), queue, 0)
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		mine[j.ID] = true
	}

	var (
		mu     sync.Mutex
		claims = make(map[id.JobID]int)
		wg     sync.WaitGroup
	)
	errCh := make(chan error, reclaimers)

	for range reclaimers {
		wg.Add(1)
		go func() {
			defer wg.Done()

			got, err := s.ReclaimExpiredLeases(ctx, reclaimLimit)
			if err != nil {
				errCh <- err

				return
			}

			mu.Lock()
			defer mu.Unlock()
			for _, j := range got {
				claims[j.ID]++
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent ReclaimExpiredLeases: %v", err)
	}

	// The invariant, not a timing guess: a job handed to two reclaimers
	// would be run by two workers. A correct backend never violates this,
	// so a correct backend never flakes here. A select-then-update backend
	// violates it whenever two scans overlap.
	for jobID := range mine {
		switch n := claims[jobID]; {
		case n == 0:
			t.Errorf("job %s was never claimed", jobID)
		case n > 1:
			t.Errorf("job %s claimed %d times, want exactly 1 — reclamation is not atomic", jobID, n)
		}
	}
}

func testLeaseTTLRoundTrips(t *testing.T, s LeaseStore) {
	ctx := context.Background()

	j := PendingJob("ttl", "lease-ttl", 6*time.Hour)
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	// The pool reads LeaseTTL off the dequeued row to compute each
	// renewal's expiry, so a backend that drops it silently reverts every
	// job to the default.
	if got.LeaseTTL != 6*time.Hour {
		t.Errorf("LeaseTTL = %v, want %v", got.LeaseTTL, 6*time.Hour)
	}
}

// timeEqual compares two *time.Time fields the way GetJob round-trips
// them: both nil, or both non-nil and equal instants. Plain == on the
// pointers would compare addresses, and reflect.DeepEqual on a bare
// time.Time can trip over a monotonic reading that a store round-trip
// already strips — Equal is the contract these fields actually promise.
func timeEqual(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}

	return a.Equal(*b)
}

// testUpdateLeasedJobAppliesAtHeldEpoch is the positive case: a worker
// that still holds the epoch it was granted at claim time gets its
// terminal write applied.
func testUpdateLeasedJobAppliesAtHeldEpoch(t *testing.T, s LeaseStore) {
	ctx := context.Background()
	worker := id.NewWorkerID()
	now := time.Now().UTC()
	const queue = "lease-update-applies"

	j := PendingJob("update-applies", queue, 0)
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:     []string{queue},
		Limit:      1,
		WorkerID:   worker,
		LeaseUntil: now.Add(time.Minute),
	})
	if err != nil || len(got) != 1 {
		t.Fatalf("DequeueJobs: %v (n=%d)", err, len(got))
	}

	claimed := got[0]
	claimed.State = job.StateCompleted
	completedAt := now
	claimed.CompletedAt = &completedAt
	claimed.LastError = "" // a business field, round-tripped like any other

	if updErr := s.UpdateLeasedJob(ctx, claimed, worker, claimed.LeaseEpoch); updErr != nil {
		t.Fatalf("UpdateLeasedJob: %v", updErr)
	}

	after, err := s.GetJob(ctx, claimed.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if after.State != job.StateCompleted {
		t.Errorf("State = %s, want %s", after.State, job.StateCompleted)
	}
	if after.CompletedAt == nil {
		t.Fatal("CompletedAt = nil, want it set")
	}
	// The write must have gone through at the epoch the caller named —
	// UpdateLeasedJob never bumps it, only the grant and reclaim do.
	if after.LeaseEpoch != claimed.LeaseEpoch {
		t.Errorf("LeaseEpoch = %d after the write, want it unchanged at %d",
			after.LeaseEpoch, claimed.LeaseEpoch)
	}
}

// testUpdateLeasedJobRejectsStaleEpoch covers a worker presenting an
// epoch older than the one the row currently holds. The row must be
// byte-identical afterwards — a refused fenced write is not merely
// "did not apply the intended change," it is "touched nothing at all."
func testUpdateLeasedJobRejectsStaleEpoch(t *testing.T, s LeaseStore) {
	ctx := context.Background()
	worker := id.NewWorkerID()
	now := time.Now().UTC()
	const queue = "lease-update-stale-epoch"

	j := PendingJob("update-stale-epoch", queue, 0)
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:     []string{queue},
		Limit:      1,
		WorkerID:   worker,
		LeaseUntil: now.Add(time.Minute),
	})
	if err != nil || len(got) != 1 {
		t.Fatalf("DequeueJobs: %v (n=%d)", err, len(got))
	}
	claimed := got[0]

	before, err := s.GetJob(ctx, claimed.ID)
	if err != nil {
		t.Fatalf("get before: %v", err)
	}

	attempt := *claimed
	attempt.State = job.StateCompleted

	err = s.UpdateLeasedJob(ctx, &attempt, worker, claimed.LeaseEpoch-1)
	if !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("UpdateLeasedJob with stale epoch = %v, want %v", err, job.ErrLeaseLost)
	}

	after, err := s.GetJob(ctx, claimed.ID)
	if err != nil {
		t.Fatalf("get after: %v", err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Errorf("row changed after a refused fenced write:\nbefore = %+v\nafter  = %+v", before, after)
	}
}

// testUpdateLeasedJobRejectsWrongWorker covers a worker presenting the
// held epoch but the wrong worker ID — the same epoch reused by a
// process that never legitimately held it, or a claim-time snapshot
// misattributed to the wrong caller.
func testUpdateLeasedJobRejectsWrongWorker(t *testing.T, s LeaseStore) {
	ctx := context.Background()
	worker := id.NewWorkerID()
	other := id.NewWorkerID()
	now := time.Now().UTC()
	const queue = "lease-update-wrong-worker"

	j := PendingJob("update-wrong-worker", queue, 0)
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:     []string{queue},
		Limit:      1,
		WorkerID:   worker,
		LeaseUntil: now.Add(time.Minute),
	})
	if err != nil || len(got) != 1 {
		t.Fatalf("DequeueJobs: %v (n=%d)", err, len(got))
	}
	claimed := got[0]

	before, err := s.GetJob(ctx, claimed.ID)
	if err != nil {
		t.Fatalf("get before: %v", err)
	}

	attempt := *claimed
	attempt.State = job.StateCompleted

	err = s.UpdateLeasedJob(ctx, &attempt, other, claimed.LeaseEpoch)
	if !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("UpdateLeasedJob from another worker = %v, want %v", err, job.ErrLeaseLost)
	}

	after, err := s.GetJob(ctx, claimed.ID)
	if err != nil {
		t.Fatalf("get after: %v", err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Errorf("row changed after a refused fenced write:\nbefore = %+v\nafter  = %+v", before, after)
	}
}

// testUpdateLeasedJobRejectsWhenNoLongerRunning covers a job that has
// already left the running state — completion does not bump lease_epoch
// or reassign worker_id, so a naive predicate that checked only those two
// would let a second write land on an already-terminal row.
func testUpdateLeasedJobRejectsWhenNoLongerRunning(t *testing.T, s LeaseStore) {
	ctx := context.Background()
	worker := id.NewWorkerID()
	now := time.Now().UTC()
	const queue = "lease-update-not-running"

	j := PendingJob("update-not-running", queue, 0)
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:     []string{queue},
		Limit:      1,
		WorkerID:   worker,
		LeaseUntil: now.Add(time.Minute),
	})
	if err != nil || len(got) != 1 {
		t.Fatalf("DequeueJobs: %v (n=%d)", err, len(got))
	}
	claimed := got[0]
	epoch := claimed.LeaseEpoch

	claimed.State = job.StateCompleted
	if updErr := s.UpdateLeasedJob(ctx, claimed, worker, epoch); updErr != nil {
		t.Fatalf("first UpdateLeasedJob: %v", updErr)
	}

	// The job is completed now: still assigned to worker, still at
	// epoch — completion touches neither — but no longer running. A
	// second fenced write must be refused on state alone.
	retry := *claimed
	retry.LastError = "a second write attempt"

	err = s.UpdateLeasedJob(ctx, &retry, worker, epoch)
	if !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("UpdateLeasedJob on a non-running job = %v, want %v", err, job.ErrLeaseLost)
	}
}

// testUpdateLeasedJobPreservesLeaseColumnsAgainstAStaleSnapshot is the
// case a naive whole-row-plus-predicate implementation fails. The lease
// is granted, renewed several times so lease_expires_at moves well
// ahead of the claim-time value, and then the fenced write is issued
// with the ORIGINAL stale snapshot — exactly what worker/runner.go does,
// since j is captured once at claim time and never refreshed. The
// passing epoch predicate must not be mistaken for permission to write
// j's copy of the lease columns back: lease_expires_at, lease_epoch,
// worker_id, and heartbeat_at must come out exactly as they were right
// before this call, not rolled back to what the stale snapshot said,
// while the business column this call actually changed must have moved.
func testUpdateLeasedJobPreservesLeaseColumnsAgainstAStaleSnapshot(t *testing.T, s LeaseStore) {
	ctx := context.Background()
	worker := id.NewWorkerID()
	now := time.Now().UTC()
	const queue = "lease-update-stale-expiry"

	j := PendingJob("update-stale-expiry", queue, 0)
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Grant properly through DequeueJobs rather than hand-constructing
	// the divergence — hand-constructing would test the fixture, not
	// this method.
	got, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:     []string{queue},
		Limit:      1,
		WorkerID:   worker,
		LeaseUntil: now.Add(30 * time.Second),
	})
	if err != nil || len(got) != 1 {
		t.Fatalf("DequeueJobs: %v (n=%d)", err, len(got))
	}
	staleSnapshot := got[0]
	epoch := staleSnapshot.LeaseEpoch

	// Renew several times so the store's lease_expires_at (and
	// heartbeat_at, which every backend's RenewLease also advances)
	// moves well past what staleSnapshot remembers.
	extended := now
	for range 3 {
		extended = extended.Add(time.Hour)
		if renewErr := s.RenewLease(ctx, staleSnapshot.ID, worker, epoch, extended); renewErr != nil {
			t.Fatalf("RenewLease: %v", renewErr)
		}
	}

	beforeWrite, err := s.GetJob(ctx, staleSnapshot.ID)
	if err != nil {
		t.Fatalf("get before write: %v", err)
	}

	// The worker finishes and hands the fenced write its ORIGINAL,
	// now-stale claim-time snapshot.
	stale := *staleSnapshot
	stale.State = job.StateCompleted
	completedAt := now
	stale.CompletedAt = &completedAt

	if updErr := s.UpdateLeasedJob(ctx, &stale, worker, epoch); updErr != nil {
		t.Fatalf("UpdateLeasedJob: %v", updErr)
	}

	after, err := s.GetJob(ctx, staleSnapshot.ID)
	if err != nil {
		t.Fatalf("get after: %v", err)
	}

	// The business column this call actually changed moved.
	if after.State != job.StateCompleted {
		t.Errorf("State = %s, want %s", after.State, job.StateCompleted)
	}

	// Every lease-owned column is exactly what it was right before the
	// fenced write — not what the stale claim-time snapshot said.
	if !timeEqual(after.LeaseExpiresAt, beforeWrite.LeaseExpiresAt) {
		t.Errorf("LeaseExpiresAt = %v, want it unchanged at %v (not rolled back to the claim-time %v)",
			after.LeaseExpiresAt, beforeWrite.LeaseExpiresAt, staleSnapshot.LeaseExpiresAt)
	}
	if after.LeaseEpoch != beforeWrite.LeaseEpoch {
		t.Errorf("LeaseEpoch = %d, want it unchanged at %d", after.LeaseEpoch, beforeWrite.LeaseEpoch)
	}
	if after.WorkerID != beforeWrite.WorkerID {
		t.Errorf("WorkerID = %s, want it unchanged at %s", after.WorkerID, beforeWrite.WorkerID)
	}
	if !timeEqual(after.HeartbeatAt, beforeWrite.HeartbeatAt) {
		t.Errorf("HeartbeatAt = %v, want it unchanged at %v", after.HeartbeatAt, beforeWrite.HeartbeatAt)
	}
}

// testUpdateLeasedJobRunnableWriteIsDequeueable covers the case every
// prior UpdateLeasedJob case missed: all of them write a TERMINAL state,
// and a backend whose fenced write persists the row's own storage without
// also restoring whatever index the original claim removed the job from
// would pass every one of them while still stranding a retried job
// forever. scheduleRetry is the call site that writes a runnable state
// (StateRetrying) through this method, so this claims a job, fences a
// write back to StateRetrying with a due RunAt, and asserts a second
// DequeueJobs on the SAME queue returns it. Dequeue is queue-scoped, so
// counting its result is safe per the suite's own rule even though this
// runs against a store shared with every other case.
func testUpdateLeasedJobRunnableWriteIsDequeueable(t *testing.T, s LeaseStore) {
	ctx := context.Background()
	worker := id.NewWorkerID()
	now := time.Now().UTC()
	const queue = "lease-update-runnable"

	j := PendingJob("update-runnable", queue, 0)
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:     []string{queue},
		Limit:      1,
		WorkerID:   worker,
		LeaseUntil: now.Add(time.Minute),
	})
	if err != nil || len(got) != 1 {
		t.Fatalf("DequeueJobs: %v (n=%d)", err, len(got))
	}
	claimed := got[0]

	claimed.State = job.StateRetrying
	claimed.RunAt = now.Add(-time.Second) // already due
	claimed.LastError = "transient, retrying"

	if updErr := s.UpdateLeasedJob(ctx, claimed, worker, claimed.LeaseEpoch); updErr != nil {
		t.Fatalf("UpdateLeasedJob: %v", updErr)
	}

	// A row report alone is not enough — GetJob would show StateRetrying
	// whether or not the backend's own claim index (a Redis ZSET; nothing
	// analogous on the SQL/Mongo backends, which re-derive candidacy from
	// the row) still points at it. Only a second dequeue proves the job
	// is actually reachable again.
	again, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:     []string{queue},
		Limit:      1,
		WorkerID:   id.NewWorkerID(),
		LeaseUntil: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("second DequeueJobs: %v", err)
	}
	if len(again) != 1 {
		t.Fatalf("second DequeueJobs returned %d jobs, want 1 — the retried job is unreachable", len(again))
	}
	if again[0].ID != claimed.ID {
		t.Errorf("second DequeueJobs returned job %s, want %s", again[0].ID, claimed.ID)
	}
}
