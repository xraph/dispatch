package resourcetest

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xraph/dispatch/resource"
)

// fakeReclaimerOwner is the lease owner name every FakeReclaimer
// acquires under, so a test inspecting resource.Manager.Leases() can
// tell the fake's holdings apart from the job leases it is competing
// against.
const fakeReclaimerOwner = "resourcetest.FakeReclaimer"

// Clock is a manually advanced time source for resource.WithClock.
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

// NewClock starts a clock at t.
func NewClock(t time.Time) *Clock { return &Clock{now: t} }

// Now returns the current fake time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

// Advance moves the clock forward.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

// FakeReclaimer stands in for a component like the artifact cache: it
// holds real resource.Manager leases for one key and frees capacity by
// releasing them, never by crediting a private counter.
//
// This matters because resource.Manager's invariant is
// used == Σ (held of every live lease); a Reclaimer that does not hold
// a lease has nothing to release, so under the real contract it can
// never actually return units to the ledger. A counter-based fake would
// make Manager.Acquire spin on a predicate that never becomes true once
// the counter is spent, and a test built on it would hang or pass
// without exercising reclamation at all. FakeReclaimer is built the way
// countingReclaimer in resource/manager_test.go is: it acquires the
// pool it represents as real leases up front and releases whole leases
// to satisfy Reclaim, so a test asserting against it is exercising the
// same admission and release path a live reclaimer would.
type FakeReclaimer struct {
	mu      sync.Mutex
	mgr     resource.Manager
	key     string
	entries []resource.Lease
	calls   int
	err     error
}

// NewFakeReclaimer builds a reclaimer for key by acquiring count leases
// of leaseSize units each from m, standing in for count evictable
// entries (an artifact cache would hold one lease per cached object).
// The pool Reclaim can free totals count*leaseSize units.
//
// Splitting the pool into count leases rather than one big one is what
// lets Reclaim satisfy a partial request the way a real evictor does:
// releasing whole entries until the shortfall is covered, rather than
// only being able to free everything or nothing.
//
// It returns an error rather than taking a *testing.T, because a
// construction failure here is a caller setup mistake (asking for more
// than the manager's capacity), not a test assertion — the caller
// decides whether that is fatal.
func NewFakeReclaimer(m resource.Manager, key string, leaseSize int64, count int) (*FakeReclaimer, error) {
	r := &FakeReclaimer{mgr: m, key: key}

	for i := range count {
		lease, ok := m.TryAcquire(fakeReclaimerOwner, resource.Set{key: leaseSize})
		if !ok {
			// Every lease acquired in iterations before this one is real,
			// live capacity taken from m — the only handle to it is
			// r.entries, which is about to be discarded. Release them all
			// before returning, or the caller's manager permanently loses
			// that capacity: indistinguishable from a job that leaked.
			for _, held := range r.entries {
				held.Release()
			}

			return nil, fmt.Errorf("resourcetest: acquire lease %d/%d of %d %s: capacity exhausted",
				i+1, count, leaseSize, key)
		}

		r.entries = append(r.entries, lease)
	}

	return r, nil
}

// SetError makes every subsequent Reclaim fail with err instead of
// releasing leases.
func (r *FakeReclaimer) SetError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.err = err
}

// Refill acquires one more lease of n units from the manager and adds
// it to the pool Reclaim can release. It fails if the manager has no
// room to grant it: refill goes through the same admission check any
// other acquisition does, rather than pretending capacity that is not
// there.
func (r *FakeReclaimer) Refill(n int64) error {
	r.mu.Lock()
	key := r.key
	mgr := r.mgr
	r.mu.Unlock()

	lease, ok := mgr.TryAcquire(fakeReclaimerOwner, resource.Set{key: n})
	if !ok {
		return fmt.Errorf("resourcetest: refill %d %s: capacity exhausted", n, key)
	}

	r.mu.Lock()
	r.entries = append(r.entries, lease)
	r.mu.Unlock()

	return nil
}

// Calls reports how many times Reclaim was called.
func (r *FakeReclaimer) Calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.calls
}

// Reclaim releases whole held leases until at least need units of key
// have been freed, returning the total actually released. Releasing
// the lease is what returns the units to the manager; the return value
// is only the "something changed, re-check" signal Manager.Acquire
// uses, exactly as resource.Reclaimer documents.
//
// A key this reclaimer was not built for frees nothing: it owns no
// leases in that key, so there is nothing honest to release.
func (r *FakeReclaimer) Reclaim(_ context.Context, key string, need int64) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls++

	if r.err != nil {
		return 0, r.err
	}

	if key != r.key {
		return 0, nil
	}

	var freed int64

	for freed < need && len(r.entries) > 0 {
		last := len(r.entries) - 1
		entry := r.entries[last]
		r.entries = r.entries[:last]

		freed += entry.Held()[key]
		entry.Release()
	}

	return freed, nil
}

// Available reports how much this reclaimer could still free without
// blocking: the sum of what its live leases hold. A key it was not
// built for is never available through it.
func (r *FakeReclaimer) Available(key string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	if key != r.key {
		return 0
	}

	var avail int64
	for _, entry := range r.entries {
		avail += entry.Held()[key]
	}

	return avail
}

// FakeEstimator returns a scripted Set and records what it was asked.
type FakeEstimator struct {
	Out   resource.Set
	Err   error
	Calls int
	Last  resource.Request
}

// Estimate returns the scripted result.
func (e *FakeEstimator) Estimate(_ context.Context, r resource.Request) (resource.Set, error) {
	e.Calls++
	e.Last = r

	if e.Err != nil {
		return nil, e.Err
	}

	return e.Out, nil
}

// Compile-time proof the fakes satisfy the interfaces they stand in for.
var (
	_ resource.Reclaimer = (*FakeReclaimer)(nil)
	_ resource.Estimator = (*FakeEstimator)(nil)
)
