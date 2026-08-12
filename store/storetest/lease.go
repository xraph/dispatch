package storetest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// RunLeaseSuite runs the lease conformance suite against a backend.
//
// newStore is called once per subtest. It may return the same underlying
// store every time: each case enqueues onto its own queue and asserts on
// the jobs it created, so cases do not interfere. That matters because
// starting a fresh Postgres or Redis container per subtest would dominate
// the runtime of the whole suite.
func RunLeaseSuite(t *testing.T, newStore func(t *testing.T) LeaseStore) {
	t.Helper()

	t.Run("DequeueLeasedGrantsAndBumpsEpoch", func(t *testing.T) {
		testDequeueLeasedGrantsAndBumpsEpoch(t, newStore(t))
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
}

func testDequeueLeasedGrantsAndBumpsEpoch(t *testing.T, s LeaseStore) {
	ctx := context.Background()
	worker := id.NewWorkerID()
	until := time.Now().UTC().Add(time.Minute)
	const queue = "lease-grant"

	j := PendingJob("grant", queue, 0)
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, err := s.DequeueLeased(ctx, []string{queue}, 10, worker, until)
	if err != nil {
		t.Fatalf("DequeueLeased: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("DequeueLeased returned %d jobs, want 1", len(got))
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
	got, err := s.DequeueLeased(ctx, []string{queue}, 1, worker, now.Add(30*time.Second))
	if err != nil || len(got) != 1 {
		t.Fatalf("DequeueLeased: %v (n=%d)", err, len(got))
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
	got, err := s.DequeueLeased(ctx, []string{queue}, 1, worker, now.Add(time.Minute))
	if err != nil || len(got) != 1 {
		t.Fatalf("DequeueLeased: %v (n=%d)", err, len(got))
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
	got, err := s.DequeueLeased(ctx, []string{queue}, 1, worker, now.Add(time.Minute))
	if err != nil || len(got) != 1 {
		t.Fatalf("DequeueLeased: %v (n=%d)", err, len(got))
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
	if _, err := s.DequeueLeased(ctx, []string{queue}, 1, worker,
		time.Now().UTC().Add(time.Hour)); err != nil {
		t.Fatalf("DequeueLeased: %v", err)
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
	got, err := s.DequeueLeased(ctx, []string{queue}, 1, worker, now.Add(-time.Second))
	if err != nil || len(got) != 1 {
		t.Fatalf("DequeueLeased: %v (n=%d)", err, len(got))
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

			got, err := s.ReclaimExpiredLeases(ctx, jobCount)
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
