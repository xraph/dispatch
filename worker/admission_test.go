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
	// takes the process with it, so there is no way to observe the ledger
	// afterwards from inside a running pool — but the defer that returns
	// the lease is the same one either way, and this recovers the panic at
	// the boundary to read Free() on the other side of it.
	t.Run("panic", func(t *testing.T) {
		h := newLeaseHarness(t, false)

		job.RegisterDefinition(h.registry, job.NewDefinition("panicker",
			func(_ context.Context, _ struct{}) error {
				panic("handler exploded")
			}))

		j := newResourceJob("panicker", resource.Set{resource.Memory: gib})

		lease, fits := h.pool.admit(j)
		if !fits {
			t.Fatal("admit refused a job that fits")
		}

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

// TestPoolRequeuesJobThatDoesNotFitLocally covers the gap the store
// cannot close: dequeue matches custom resources by KEY, never by
// quantity, so a worker offering "fpga" legitimately claims a job wanting
// four of them. That job must go back to pending, not run.
func TestPoolRequeuesJobThatDoesNotFitLocally(t *testing.T) {
	mgr := resource.NewManager(resource.Set{"fpga": 1})
	h := newHarness(t, mgr, true)

	var ran atomic.Bool

	job.RegisterDefinition(h.registry, job.NewDefinition("needs-four-fpga",
		func(_ context.Context, _ struct{}) error {
			ran.Store(true)

			return nil
		}))

	j := newResourceJob("needs-four-fpga", resource.Set{"fpga": 4})
	if err := h.store.EnqueueJob(context.Background(), j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	h.start()

	var got *job.Job

	waitFor(t, "job to be claimed and returned to pending", func() bool {
		fetched, err := h.store.GetJob(context.Background(), j.ID)
		if err != nil {
			return false
		}

		got = fetched

		return fetched.StartedAt != nil && fetched.State == job.StatePending
	})

	h.stop()

	// StartedAt proves the store DID hand the job over — the key filter
	// let it through, exactly as documented — and pending proves the
	// worker refused it locally on quantity.
	if got.StartedAt == nil || got.State != job.StatePending {
		t.Fatalf("job state = %q, StartedAt = %v; want pending after a claim", got.State, got.StartedAt)
	}

	if ran.Load() {
		t.Error("handler ran for a job that does not fit local capacity")
	}

	if free := mgr.Free()["fpga"]; free != 1 {
		t.Errorf("free fpga = %d, want 1 (a refused claim must take no lease)", free)
	}

	h.assertDrained()
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
// purpose, and then never starts the pool.
func newHarness(t *testing.T, mgr resource.Manager, withRecover bool) *leaseHarness {
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
