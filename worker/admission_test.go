package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/middleware"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/resource/resourcetest"
	"github.com/xraph/dispatch/store/memory"
)

const gib = int64(1) << 30

// TestDequeueBudgetUsesFreePlusReclaimableForDiskOnly pins the single
// asymmetry the whole admission model rests on.
//
// Disk that a cache holds but no running job leases is available to a new
// job, because staging evicts to make room; counting only Free() there
// would let a warm cache stop the worker claiming anything. Memory held
// by a running job is available to nobody, whatever is registered against
// it; counting Reclaimable() there would admit work the box cannot run.
//
// Both halves are asserted against ONE manager with a reclaimer on BOTH
// keys, so the test fails if the rule is ever applied by key-agnostic
// arithmetic in either direction.
func TestDequeueBudgetUsesFreePlusReclaimableForDiskOnly(t *testing.T) {
	mgr := resource.NewManager(resource.Set{
		resource.Memory: 8 * gib,
		resource.Disk:   100 * gib,
	})

	// A staging cache holding 40 GiB across four evictable entries.
	diskCache, err := resourcetest.NewFakeReclaimer(mgr, resource.Disk, 10*gib, 4)
	if err != nil {
		t.Fatalf("disk reclaimer: %v", err)
	}

	mgr.RegisterReclaimer(resource.Disk, diskCache)

	// A reclaimer on memory, which must make no difference whatsoever.
	// Registering one is legal — Manager.Acquire will use it, because it
	// can afford to wait for the eviction — but the dequeue budget must
	// not, because a non-blocking claim cannot.
	memCache, err := resourcetest.NewFakeReclaimer(mgr, resource.Memory, gib, 2)
	if err != nil {
		t.Fatalf("memory reclaimer: %v", err)
	}

	mgr.RegisterReclaimer(resource.Memory, memCache)

	// One job already running on this worker.
	running, ok := mgr.TryAcquire("job-running", resource.Set{
		resource.Memory: 4 * gib,
		resource.Disk:   20 * gib,
	})
	if !ok {
		t.Fatal("TryAcquire for the running job did not fit")
	}

	defer running.Release()

	free := mgr.Free()
	if got, want := free[resource.Disk], 40*gib; got != want {
		t.Fatalf("setup: free disk = %d, want %d", got, want)
	}

	if got, want := free[resource.Memory], 2*gib; got != want {
		t.Fatalf("setup: free memory = %d, want %d", got, want)
	}

	p := &Pool{resources: mgr}
	budget := p.dequeueBudget()

	// 40 GiB free + 40 GiB the cache can evict.
	if got, want := budget[resource.Disk], 80*gib; got != want {
		t.Errorf("budget disk = %d, want %d (free + reclaimable)", got, want)
	}

	// 2 GiB free. The 2 GiB the memory reclaimer holds is NOT offered.
	if got, want := budget[resource.Memory], 2*gib; got != want {
		t.Errorf("budget memory = %d, want %d (free only, never reclaimable)", got, want)
	}
}

// TestDequeueBudgetWithoutReclaimerIsFree is the control for the disk
// rule: with nothing registered to evict, disk is plain free capacity.
func TestDequeueBudgetWithoutReclaimerIsFree(t *testing.T) {
	mgr := resource.NewManager(resource.Set{resource.Disk: 100 * gib})

	held, ok := mgr.TryAcquire("job-running", resource.Set{resource.Disk: 30 * gib})
	if !ok {
		t.Fatal("TryAcquire did not fit")
	}

	defer held.Release()

	p := &Pool{resources: mgr}

	if got, want := p.dequeueBudget()[resource.Disk], 70*gib; got != want {
		t.Errorf("budget disk = %d, want %d", got, want)
	}
}

// TestOfferedCustomKeysDerivesFromCapacity pins the default: a worker
// advertises exactly the custom keys its manager meters, and an explicit
// option overrides that.
func TestOfferedCustomKeysDerivesFromCapacity(t *testing.T) {
	mgr := resource.NewManager(resource.Set{
		resource.Memory: gib,
		"fpga":          2,
		"nvme":          1,
	})

	p := &Pool{resources: mgr}

	got := p.offeredCustomKeys()
	if len(got) != 2 || got[0] != "fpga" || got[1] != "nvme" {
		t.Errorf("offeredCustomKeys() = %v, want [fpga nvme]", got)
	}

	p.customKeys = []string{"fpga"}

	if got := p.offeredCustomKeys(); len(got) != 1 || got[0] != "fpga" {
		t.Errorf("offeredCustomKeys() with override = %v, want [fpga]", got)
	}
}

// TestLeaseReleasedAfterExecution proves capacity comes back on every
// exit path an attempt has: success, handler error, and panic.
func TestLeaseReleasedAfterExecution(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		assertLeaseReturned(t, nil)
	})

	t.Run("handler error", func(t *testing.T) {
		assertLeaseReturned(t, errBoom)
	})

	// The panic case drives runJob directly rather than through a started
	// pool. A handler that panics past a pool with no Recover middleware
	// takes the process with it — there is no surviving pool to observe
	// afterwards — but the defer that returns the lease is the same one
	// either way, and this recovers the panic at the boundary so the
	// ledger can be read on the other side of it.
	t.Run("panic", func(t *testing.T) {
		h := newLeaseHarness(t, false)

		job.RegisterDefinition(h.registry, job.NewDefinition("panicker",
			func(_ context.Context, _ struct{}) error {
				panic("handler exploded")
			}))

		j := newResourceJob("panicker", resource.Set{resource.Memory: gib})

		lease, err := h.admitOne(j)
		if err != nil {
			t.Fatalf("admit refused a job that fits: %v", err)
		}

		// Pre-condition, not the assertion: the lease is genuinely held
		// going in, so the drained check afterwards means something.
		if got := h.manager.Free()[resource.Memory]; got != 3*gib {
			t.Fatalf("free memory while admitted = %d, want %d", got, 3*gib)
		}

		var panicked bool

		func() {
			defer func() {
				if r := recover(); r != nil {
					panicked = true
				}
			}()

			h.pool.runJob(admitted{job: j, lease: lease})
		}()

		if !panicked {
			t.Fatal("expected the handler panic to propagate out of runJob")
		}

		h.assertDrained()
	})
}

// assertLeaseReturned runs one job end to end through a started pool and
// asserts the manager is back at full capacity afterwards.
func assertLeaseReturned(t *testing.T, handlerErr error) {
	t.Helper()

	h := newLeaseHarness(t, true)

	var ran atomic.Bool

	job.RegisterDefinition(h.registry, job.NewDefinition("leased",
		func(_ context.Context, _ struct{}) error {
			ran.Store(true)

			return handlerErr
		}))

	j := newResourceJob("leased", resource.Set{resource.Memory: gib})
	if err := h.store.EnqueueJob(context.Background(), j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	h.start()
	waitFor(t, "job to run", ran.Load)
	h.stop()

	if got := h.manager.Free()[resource.Memory]; got != 4*gib {
		t.Errorf("free memory after execution = %d, want %d", got, 4*gib)
	}

	h.assertDrained()
}

// TestAdmitReclaimsDiskBeforeRefusing closes the loop between the two
// halves of the disk rule.
//
// dequeueBudget offers disk as free PLUS what the cache can evict, so
// admission has to be able to evict, or the worker promises the store
// capacity it will then refuse to honour — claiming the job, bouncing it,
// and doing it again on the next poll forever. Nothing registers a
// reclaimer on the shared manager yet, which is the only reason that is
// not already happening in production.
func TestAdmitReclaimsDiskBeforeRefusing(t *testing.T) {
	mgr := resource.NewManager(resource.Set{resource.Disk: 100 * gib})

	// A staging cache holding 60 GiB across six evictable entries, so 40
	// GiB is free and 60 GiB is reclaimable.
	cache, err := resourcetest.NewFakeReclaimer(mgr, resource.Disk, 10*gib, 6)
	if err != nil {
		t.Fatalf("disk reclaimer: %v", err)
	}

	mgr.RegisterReclaimer(resource.Disk, cache)

	h := newHarness(t, mgr, true)

	if got := h.pool.dequeueBudget()[resource.Disk]; got != 100*gib {
		t.Fatalf("setup: budget disk = %d, want %d", got, 100*gib)
	}

	if free := mgr.Free()[resource.Disk]; free != 40*gib {
		t.Fatalf("setup: free disk = %d, want %d", free, 40*gib)
	}

	// A job sized to the budget the store was given. TryAcquire would
	// refuse this against the 40 GiB that is free right now.
	j := newResourceJob("staging-hog", resource.Set{resource.Disk: 100 * gib})

	lease, err := h.admitOne(j)
	if err != nil {
		t.Fatalf("admit refused a job the dequeue budget promised: %v", err)
	}

	if cache.Calls() == 0 {
		t.Error("admit took the lease without reclaiming; the budget was redeemed by luck, not eviction")
	}

	if got := mgr.Free()[resource.Disk]; got != 0 {
		t.Errorf("free disk while the job holds everything = %d, want 0", got)
	}

	lease.Release()

	if got := mgr.Free()[resource.Disk]; got != 100*gib {
		t.Errorf("free disk after release = %d, want %d", got, 100*gib)
	}
}

// TestAdmitRefusesLargerThanCapacityImmediately covers the cheap
// refusal: a want bigger than the whole worker short-circuits in Acquire
// before it waits at all, so the permanently-impossible case never costs
// the admission deadline.
//
// Note what this does NOT prove: because it short-circuits, it never
// reaches the deadline path. TestAdmitRefusalIsBounded is the guard for
// that.
func TestAdmitRefusesLargerThanCapacityImmediately(t *testing.T) {
	mgr := resource.NewManager(resource.Set{resource.Disk: 10 * gib})
	h := newHarness(t, mgr, true, WithPollInterval(2*time.Second))

	j := newResourceJob("too-big", resource.Set{resource.Disk: 40 * gib})

	start := time.Now()

	if _, err := h.admitOne(j); err == nil {
		t.Fatal("admit accepted a job larger than total capacity")
	}

	// The deadline is 2s here precisely so that waiting would be obvious.
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("admit blocked for %v on an impossible job; want an immediate refusal", elapsed)
	}
}

// TestAdmitRefusalIsBounded exercises the deadline itself: a want that
// fits the worker's capacity but not its free capacity, with nothing
// reclaimable to cover the gap, must give up at the deadline instead of
// waiting for a running job to finish.
//
// The reclaimer is wedged rather than absent, so the refusal goes the
// long way round — reclaim is attempted, frees nothing, and the wait is
// what ends it.
func TestAdmitRefusalIsBounded(t *testing.T) {
	mgr := resource.NewManager(resource.Set{resource.Disk: 100 * gib})

	// 80 GiB held by something that is not a reclaimer: a running job.
	// Nothing can take this back within the deadline.
	running, ok := mgr.TryAcquire("job-running", resource.Set{resource.Disk: 80 * gib})
	if !ok {
		t.Fatal("setup: running job did not fit")
	}

	defer running.Release()

	cache, err := resourcetest.NewFakeReclaimer(mgr, resource.Disk, gib, 5)
	if err != nil {
		t.Fatalf("disk reclaimer: %v", err)
	}

	cache.SetError(errBoom) // the evictor is wedged
	mgr.RegisterReclaimer(resource.Disk, cache)

	const deadline = 120 * time.Millisecond

	h := newHarness(t, mgr, true, WithPollInterval(deadline))

	// 15 GiB free, 5 GiB stuck behind a broken evictor, 30 GiB wanted.
	j := newResourceJob("wont-fit-yet", resource.Set{resource.Disk: 30 * gib})

	start := time.Now()
	_, admitErr := h.admitOne(j)
	elapsed := time.Since(start)

	if admitErr == nil {
		t.Fatal("admit granted a lease no eviction could cover")
	}

	if cache.Calls() == 0 {
		t.Error("admit never attempted reclamation")
	}

	if elapsed < deadline {
		t.Errorf("admit gave up after %v, before the %v deadline; it is not waiting at all", elapsed, deadline)
	}

	if elapsed > 4*deadline {
		t.Errorf("admit blocked for %v against a %v deadline", elapsed, deadline)
	}

	// A refusal must leave nothing behind.
	if free := mgr.Free()[resource.Disk]; free != 15*gib {
		t.Errorf("free disk after a refusal = %d, want %d", free, 15*gib)
	}
}

// TestAdmissionBudgetIsPerBatchNotPerJob pins the fetcher's worst-case
// stall.
//
// Per job, a batch costs batch size × deadline, and the fetcher spends
// that stall holding jobs the store has already marked running and that
// nobody is heartbeating yet. Concurrency 20 with a 5s poll interval
// would be a 100s stall against a 30s stale-job threshold — the reaper
// reclaiming jobs the fetcher still has in hand. One budget for the batch
// makes the worst case the deadline, whatever the batch size.
func TestAdmissionBudgetIsPerBatchNotPerJob(t *testing.T) {
	mgr := resource.NewManager(resource.Set{resource.Disk: 100 * gib})

	running, ok := mgr.TryAcquire("job-running", resource.Set{resource.Disk: 80 * gib})
	if !ok {
		t.Fatal("setup: running job did not fit")
	}

	defer running.Release()

	cache, err := resourcetest.NewFakeReclaimer(mgr, resource.Disk, gib, 5)
	if err != nil {
		t.Fatalf("disk reclaimer: %v", err)
	}

	cache.SetError(errBoom)
	mgr.RegisterReclaimer(resource.Disk, cache)

	const deadline = 120 * time.Millisecond

	h := newHarness(t, mgr, true, WithPollInterval(deadline))

	// Four jobs that each have to wait out the budget.
	batch := make([]*job.Job, 0, 4)
	for range 4 {
		batch = append(batch, newResourceJob("wont-fit-yet", resource.Set{resource.Disk: 30 * gib}))
	}

	ctx, cancel := h.pool.admissionBudget(len(batch))
	defer cancel()

	start := time.Now()

	for i, j := range batch {
		if _, admitErr := h.pool.admit(ctx, j); admitErr == nil {
			t.Fatalf("job %d was admitted against a full worker", i)
		}
	}

	elapsed := time.Since(start)

	t.Logf("4 unadmittable jobs under one %v budget took %v", deadline, elapsed)

	// Per job this is 4 × 120ms = 480ms. Shared, the first job spends the
	// budget and the other three fail on the expired context immediately.
	if elapsed > 2*deadline {
		t.Errorf("batch admission took %v against a %v budget; the deadline is being spent per job",
			elapsed, deadline)
	}

	// The spent budget must not poison the batch: Acquire only consults
	// the context when a request does not fit, so a job with room is still
	// admitted after the budget is gone.
	small := newResourceJob("fits-anyway", resource.Set{resource.Disk: gib})

	lease, admitErr := h.pool.admit(ctx, small)
	if admitErr != nil {
		t.Fatalf("a job that fits was refused on an expired batch budget: %v", admitErr)
	}

	lease.Release()
}

// TestPoolPacesUnfittableBacklog pins the pacing of a batch that
// dispatched nothing.
//
// The "there may be more ready work, poll again immediately" fast path
// was only safe because every returned job went through the blocking
// hand-off, which paced the loop at the rate work completes. A job
// refused locally never touches that channel and returns instantly, so
// reading len(jobs) as productive spins the claim/requeue cycle as fast
// as the store can serve it — each turn costing a dequeue and an
// UpdateJob against a backlog nothing can run.
func TestPoolPacesUnfittableBacklog(t *testing.T) {
	mgr := resource.NewManager(resource.Set{"fpga": 1})
	h := newHarness(t, mgr, true,
		WithPoolConcurrency(2),
		WithMaxPollInterval(50*time.Millisecond),
	)

	job.RegisterDefinition(h.registry, job.NewDefinition("never-fits",
		func(_ context.Context, _ struct{}) error { return nil }))

	// Every one of these passes the store's filter — dequeue matches
	// custom keys, not quantities — and fails admission.
	for range 60 {
		j := newResourceJob("never-fits", resource.Set{"fpga": 4})
		if err := h.store.EnqueueJob(context.Background(), j); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	h.start()
	time.Sleep(500 * time.Millisecond)

	calls := h.store.calls()

	h.stop()

	t.Logf("DequeueJobs calls in 500ms against a 60-job unfittable backlog = %d", calls)

	// One poll per 10ms pollInterval is ~50 in 500ms, and the cadence caps
	// it there by construction. Reverting the fix on this exact test
	// measures 713 — the loop polling as fast as the store can answer.
	if calls > 60 {
		t.Errorf("DequeueJobs calls in 500ms = %d, want <= 60; a batch that dispatched nothing is not being paced", calls)
	}
}

// TestPoolRunsRunnableJobBehindUnfittableBacklog is the other half of the
// pacing contract, and the reason a refused batch is paced at the poll
// interval rather than handed to the idle backoff.
//
// The store returns the queue head, so an unrunnable backlog is re-claimed
// a Limit at a time and only clears by being claimed. Backing off
// exponentially between those claims strands a job the worker CAN run
// behind jobs it cannot, for the whole ramp — measured at 8.5s with a 2s
// cap, against ~94ms at the poll cadence. That is a worse failure than the
// spin it would be fixing.
func TestPoolRunsRunnableJobBehindUnfittableBacklog(t *testing.T) {
	mgr := resource.NewManager(resource.Set{resource.Memory: 8 * gib, "fpga": 1})
	h := newHarness(t, mgr, true,
		WithPoolConcurrency(2),
		WithMaxPollInterval(2*time.Second),
	)

	var ran atomic.Bool

	job.RegisterDefinition(h.registry, job.NewDefinition("never-fits",
		func(_ context.Context, _ struct{}) error { return nil }))
	job.RegisterDefinition(h.registry, job.NewDefinition("fits",
		func(_ context.Context, _ struct{}) error {
			ran.Store(true)

			return nil
		}))

	// A backlog of jobs this worker can never run, all sorting ahead of
	// what comes next by RunAt.
	for range 20 {
		j := newResourceJob("never-fits", resource.Set{"fpga": 4})
		if err := h.store.EnqueueJob(context.Background(), j); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	h.start()
	time.Sleep(200 * time.Millisecond) // let the loop settle into the backlog

	good := newResourceJob("fits", resource.Set{resource.Memory: gib})
	if err := h.store.EnqueueJob(context.Background(), good); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	start := time.Now()
	h.pool.Wake()

	deadline := time.Now().Add(3 * time.Second)
	for !ran.Load() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}

	elapsed := time.Since(start)

	h.stop()

	t.Logf("runnable job ran %v after Wake, behind a 20-job unfittable backlog", elapsed)

	if !ran.Load() {
		t.Fatal("a runnable job never ran; it is stuck behind the unfittable backlog")
	}

	// Draining 20 misfits two at a time at a 10ms cadence is ~100ms. One
	// second is far past that and far under the 2s cap a single backed-off
	// wait would have cost.
	if elapsed > time.Second {
		t.Errorf("runnable job took %v to run; the refused backlog is being paced by the idle backoff", elapsed)
	}
}

// TestPoolRequeuesJobThatDoesNotFitLocally covers the gap the store
// cannot close: dequeue matches custom resources by KEY, never by
// quantity, so a worker offering "fpga" legitimately claims a job wanting
// four of them. That job must go back to pending, not run — and the
// queue/tenant token taken for it must come back too.
func TestPoolRequeuesJobThatDoesNotFitLocally(t *testing.T) {
	mgr := resource.NewManager(resource.Set{"fpga": 1})
	qm := newCountingQueueManager()
	h := newHarness(t, mgr, true, WithQueueManager(qm))

	var ran atomic.Bool

	job.RegisterDefinition(h.registry, job.NewDefinition("needs-four-fpga",
		func(_ context.Context, _ struct{}) error {
			ran.Store(true)

			return nil
		}))

	j := newResourceJob("needs-four-fpga", resource.Set{"fpga": 4})
	beforeRunAt := j.RunAt

	if err := h.store.EnqueueJob(context.Background(), j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	h.start()

	var got *job.Job

	// requeueRateLimited (which requeueLocalMisfit reuses verbatim) now
	// clears the assignment fields on its way back to pending, so
	// StartedAt no longer survives as proof the store handed the job
	// over. RunAt does: requeueRateLimited unconditionally pushes it to
	// time.Now().Add(pollInterval), which nothing else in this test ever
	// touches, so RunAt moving past its enqueue-time value is proof of
	// exactly one thing — a real claim-then-requeue cycle ran.
	waitFor(t, "job to be claimed and returned to pending", func() bool {
		fetched, err := h.store.GetJob(context.Background(), j.ID)
		if err != nil {
			return false
		}

		got = fetched

		return fetched.State == job.StatePending && fetched.RunAt.After(beforeRunAt)
	})

	h.stop()

	// RunAt pushed forward proves the store DID hand the job over — the
	// key filter let it through, exactly as documented — and pending
	// proves the worker refused it locally on quantity.
	if !got.RunAt.After(beforeRunAt) || got.State != job.StatePending {
		t.Fatalf("job state = %q, RunAt = %v (before %v); want pending with RunAt pushed forward after a claim",
			got.State, got.RunAt, beforeRunAt)
	}

	// And the fields that record a worker assignment must NOT survive —
	// this is the property the shared clearJobAssignment helper exists
	// for: a pending job must never look claimed by a worker that no
	// longer holds it.
	if got.StartedAt != nil {
		t.Errorf("StartedAt = %v, want nil — a job returned to pending must not still look claimed", got.StartedAt)
	}
	if !got.WorkerID.IsNil() {
		t.Errorf("WorkerID = %s, want cleared", got.WorkerID)
	}

	if ran.Load() {
		t.Error("handler ran for a job that does not fit local capacity")
	}

	if free := mgr.Free()["fpga"]; free != 1 {
		t.Errorf("free fpga = %d, want 1 (a refused claim must take no lease)", free)
	}

	acquired, released := qm.counts()
	if acquired == 0 {
		t.Fatal("queue manager was never consulted; the misfit path is not being exercised")
	}

	if acquired != released {
		t.Errorf("queue tokens acquired = %d, released = %d; a refused job must give its token back",
			acquired, released)
	}

	h.assertDrained()
}

// TestAbandonReturnsEverything covers the shutdown path, which no
// realistic race can be made to hit on demand: a job claimed by the
// fetcher and cancelled before the hand-off must give back its row, its
// queue token, and its lease.
func TestAbandonReturnsEverything(t *testing.T) {
	mgr := resource.NewManager(resource.Set{resource.Memory: 4 * gib})
	qm := newCountingQueueManager()
	h := newHarness(t, mgr, true, WithQueueManager(qm))

	ctx := context.Background()

	j := newResourceJob("interrupted", resource.Set{resource.Memory: gib})
	if err := h.store.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Walk the fetcher's steps by hand, up to the hand-off it never wins.
	claimed, err := h.store.DequeueJobs(ctx, job.DequeueOpts{Queues: []string{"default"}, Limit: 1})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("dequeue: %v (%d jobs)", err, len(claimed))
	}

	if !h.pool.queueManager.Acquire(claimed[0].Queue, claimed[0].ScopeOrgID) {
		t.Fatal("queue manager refused")
	}

	lease, err := h.admitOne(claimed[0])
	if err != nil {
		t.Fatalf("admit: %v", err)
	}

	h.pool.abandon(admitted{job: claimed[0], lease: lease})

	got, err := h.store.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	if got.State != job.StatePending {
		t.Errorf("job state = %q, want %q", got.State, job.StatePending)
	}

	if got.StartedAt != nil {
		t.Errorf("StartedAt = %v, want nil (the attempt never started)", got.StartedAt)
	}

	if acquired, released := qm.counts(); acquired != released {
		t.Errorf("queue tokens acquired = %d, released = %d", acquired, released)
	}

	if free := mgr.Free()[resource.Memory]; free != 4*gib {
		t.Errorf("free memory = %d, want %d", free, 4*gib)
	}

	h.assertDrained()
}

// TestRequeueLocalMisfitDuringShutdownUsesFreshContext pins the branch
// that keeps a stopping pool from stranding claimed jobs.
//
// Admission is bounded by the pool's own context, so shutdown makes it
// fail for every job the fetcher is holding. Requeueing those through the
// rate-limited path would write through that same cancelled context, the
// UpdateJob would fail, and the job would sit in running with no worker
// until the reaper noticed.
func TestRequeueLocalMisfitDuringShutdownUsesFreshContext(t *testing.T) {
	mgr := resource.NewManager(resource.Set{resource.Memory: 4 * gib})
	h := newHarness(t, mgr, true)

	ctx := context.Background()

	j := newResourceJob("interrupted", resource.Set{resource.Memory: gib})
	if err := h.store.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	claimed, err := h.store.DequeueJobs(ctx, job.DequeueOpts{Queues: []string{"default"}, Limit: 1})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("dequeue: %v (%d jobs)", err, len(claimed))
	}

	// Another job took the whole worker between the budget and the claim,
	// so admission would have to wait — the only way a cancelled context
	// can be observed, since a job that fits is granted outright.
	blocker, ok := mgr.TryAcquire("other-job", resource.Set{resource.Memory: 4 * gib})
	if !ok {
		t.Fatal("setup: blocker did not fit")
	}

	defer blocker.Release()

	// The pool is stopping: its context is dead and every store call made
	// through it would fail.
	h.pool.cancelFunc()

	_, admitErr := h.admitOne(claimed[0])
	if admitErr == nil {
		t.Fatal("admit succeeded against a cancelled pool context with no free capacity")
	}

	h.pool.requeueLocalMisfit(claimed[0], admitErr)

	got, err := h.store.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	if got.State != job.StatePending {
		t.Errorf("job state = %q, want %q; the requeue wrote through the cancelled context",
			got.State, job.StatePending)
	}
}

// TestPoolWithoutManagerPassesZeroBudget is the degradation guarantee: a
// pool with no resource manager sends opts every backend treats as
// unbounded, so it claims exactly what it claimed before this model
// existed.
func TestPoolWithoutManagerPassesZeroBudget(t *testing.T) {
	h := newHarness(t, nil, true)

	h.start()
	waitFor(t, "a dequeue", func() bool { return h.store.calls() > 0 })
	h.stop()

	opts := h.store.lastOpts()

	if !opts.IsUnbounded() {
		t.Errorf("DequeueOpts.IsUnbounded() = false, want true (Budget=%v CustomKeys=%v ReservedFor=%v)",
			opts.Budget, opts.CustomKeys, opts.ReservedFor)
	}

	if opts.Budget != nil {
		t.Errorf("Budget = %v, want nil", opts.Budget)
	}

	if opts.CustomKeys != nil {
		t.Errorf("CustomKeys = %v, want nil", opts.CustomKeys)
	}

	if opts.Limit == 0 {
		t.Error("Limit = 0, want the free slot count (the rest of the opts must still be wired)")
	}
}

// TestPoolWithManagerPassesBoundedBudget is the other side of the
// degradation test: with a manager the opts must actually constrain, or
// the wiring above would be untested by it.
func TestPoolWithManagerPassesBoundedBudget(t *testing.T) {
	mgr := resource.NewManager(resource.Set{resource.Memory: 4 * gib, "fpga": 2})
	h := newHarness(t, mgr, true)

	h.start()
	waitFor(t, "a dequeue", func() bool { return h.store.calls() > 0 })
	h.stop()

	opts := h.store.lastOpts()

	if opts.IsUnbounded() {
		t.Fatal("DequeueOpts.IsUnbounded() = true, want false with a resource manager")
	}

	if got, want := opts.Budget[resource.Memory], 4*gib; got != want {
		t.Errorf("Budget[memory] = %d, want %d", got, want)
	}

	if len(opts.CustomKeys) != 1 || opts.CustomKeys[0] != "fpga" {
		t.Errorf("CustomKeys = %v, want [fpga]", opts.CustomKeys)
	}
}

// ──────────────────────────────────────────────────
// Harness
// ──────────────────────────────────────────────────

var errBoom = errors.New("boom")

// recordingOptsStore is a memory store that remembers the DequeueOpts it
// was called with, which is how the wiring tests read what the fetcher
// built.
type recordingOptsStore struct {
	*memory.Store

	mu    sync.Mutex
	last  job.DequeueOpts
	count int
}

func (r *recordingOptsStore) DequeueJobs(ctx context.Context, opts job.DequeueOpts) ([]*job.Job, error) {
	r.mu.Lock()
	r.last = opts
	r.count++
	r.mu.Unlock()

	return r.Store.DequeueJobs(ctx, opts)
}

func (r *recordingOptsStore) lastOpts() job.DequeueOpts {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.last
}

func (r *recordingOptsStore) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.count
}

// countingQueueManager is a QueueManager that admits everything and
// counts both sides of the token.
//
// It exists because WithQueueManager had no test call site anywhere in
// the repo, so nothing could observe a token that was never released —
// which is how two new release call sites landed in a blind spot.
type countingQueueManager struct {
	acquires atomic.Int64
	releases atomic.Int64
	refuse   atomic.Bool
}

func newCountingQueueManager() *countingQueueManager { return &countingQueueManager{} }

func (q *countingQueueManager) Acquire(_, _ string) bool {
	if q.refuse.Load() {
		return false
	}

	q.acquires.Add(1)

	return true
}

func (q *countingQueueManager) Release(_, _ string) { q.releases.Add(1) }

func (q *countingQueueManager) counts() (acquired, released int64) {
	return q.acquires.Load(), q.releases.Load()
}

type leaseHarness struct {
	t        *testing.T
	pool     *Pool
	store    *recordingOptsStore
	registry *job.Registry
	manager  resource.Manager
	started  bool
}

// newLeaseHarness builds a pool over a 4 GiB memory manager.
func newLeaseHarness(t *testing.T, withRecover bool) *leaseHarness {
	t.Helper()

	return newHarness(t, resource.NewManager(resource.Set{resource.Memory: 4 * gib}), withRecover)
}

// newHarness builds a pool with concurrency 1 over mgr, which may be nil.
// withRecover installs middleware.Recover; the panic test omits it on
// purpose, and then never starts the pool. Options in extra are applied
// last, so they override the defaults here.
func newHarness(t *testing.T, mgr resource.Manager, withRecover bool, extra ...PoolOption) *leaseHarness {
	t.Helper()

	logger := log.NewNoopLogger()
	s := &recordingOptsStore{Store: memory.New()}
	reg := job.NewRegistry()
	extensions := ext.NewRegistry(logger)

	var mws []middleware.Middleware
	if withRecover {
		mws = append(mws, middleware.Recover(logger))
	}

	runner := NewExecutor(reg, extensions, s, dlq.NewService(s, s),
		backoff.NewConstant(10*time.Millisecond), logger, mws...)

	opts := []PoolOption{
		WithPoolConcurrency(1),
		WithPollInterval(10 * time.Millisecond),
		WithMaxPollInterval(10 * time.Millisecond),
		WithPoolQueues([]string{"default"}),
	}
	if mgr != nil {
		opts = append(opts, WithResourceManager(mgr))
	}

	opts = append(opts, extra...)

	h := &leaseHarness{
		t:        t,
		pool:     NewPool(s, runner, extensions, logger, opts...),
		store:    s,
		registry: reg,
		manager:  mgr,
	}

	// The panic test calls runJob without Start, so give it the two
	// fields Start would have set. Start overwrites both, so a harness
	// that is later started is unaffected.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	h.pool.cancelCtx, h.pool.cancelFunc = ctx, cancel
	h.pool.slots = make(chan struct{}, 1)

	t.Cleanup(h.stop)

	return h
}

// admitOne admits a single job under a one-job admission budget, the way
// the fetcher would for a batch of one.
func (h *leaseHarness) admitOne(j *job.Job) (resource.Lease, error) {
	ctx, cancel := h.pool.admissionBudget(1)
	defer cancel()

	return h.pool.admit(ctx, j)
}

func (h *leaseHarness) start() {
	h.t.Helper()

	if err := h.pool.Start(context.Background()); err != nil {
		h.t.Fatalf("start: %v", err)
	}

	h.started = true
}

func (h *leaseHarness) stop() {
	if !h.started {
		return
	}

	h.started = false

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := h.pool.Stop(ctx); err != nil {
		h.t.Errorf("stop: %v", err)
	}
}

// assertDrained checks the manager holds no leases at all, which catches
// a release that returned the right quantity under the wrong lease.
func (h *leaseHarness) assertDrained() {
	h.t.Helper()

	if h.manager == nil {
		return
	}

	if held := h.manager.Leases(); len(held) != 0 {
		h.t.Errorf("manager still holds %d lease(s): %+v", len(held), held)
	}
}

func newResourceJob(name string, req resource.Set) *job.Job {
	now := time.Now().UTC()

	j := &job.Job{
		ID:         id.NewJobID(),
		Name:       name,
		Queue:      "default",
		Payload:    []byte(`{}`),
		State:      job.StatePending,
		MaxRetries: 0,
		RunAt:      now,
		Resources:  req,
	}
	j.CreatedAt = now
	j.UpdatedAt = now

	return j
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.After(5 * time.Second)

	for {
		if cond() {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(5 * time.Millisecond):
		}
	}
}
