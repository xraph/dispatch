package resource

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Reclaimer frees capacity for one key on the manager's behalf.
//
// It exists because the resource dimensions are not symmetric: cached
// bytes on disk can be evicted to make room, while memory held by a
// running job cannot. Registering a reclaimer for a key turns blocking
// into "reclaim, then block only if that was not enough". A key with no
// reclaimer — memory, CPU — can only wait for a release.
//
// A reclaimer holds a Manager lease for what it caches, and returns
// units by releasing that lease — it never credits the ledger directly.
// The artifact cache holds one lease per cached entry, so evicting an
// entry releases that entry's lease and the bytes come back through the
// same path a finished job's would.
type Reclaimer interface {
	// Reclaim frees up to need units of key, returning how many it
	// actually freed. Returning zero means nothing more is reclaimable.
	//
	// The units are returned to the manager by releasing the Lease that
	// held them. The return value is only a "something changed, re-check"
	// signal — the manager does not add it to the ledger, because those
	// units are still recorded against a live lease until that lease is
	// released, and counting them in both places would invent capacity.
	//
	// Reclaim is called with the manager's lock dropped, so releasing a
	// lease from inside it is safe and is the expected implementation.
	Reclaim(ctx context.Context, key string, need int64) (int64, error)
	// Available reports how much could be freed without blocking.
	Available(key string) int64
}

// Lease is a held allocation. Release is idempotent.
type Lease interface {
	Held() Set
	Owner() string
	Release()
}

// LeaseInfo is a point-in-time view of one lease, for the capacity API.
type LeaseInfo struct {
	Owner      string    `json:"owner"`
	Held       Set       `json:"held"`
	AcquiredAt time.Time `json:"acquired_at"`
}

// Manager admits work against a fixed capacity.
//
// It generalizes the artifact cache's single-key disk budget to N keys:
// same mutex and condition variable, same context-bounded wait, same
// reclaim-then-wait loop — but keyed, so memory and CPU are accounted
// by the mechanism that already worked for disk rather than by a
// second one built alongside it.
type Manager interface {
	// Acquire blocks until want fits, reclaiming where a Reclaimer is
	// registered. It is bounded by ctx, so a blocked job can never
	// outlive its deadline. A want larger than total capacity fails
	// immediately: no release or reclamation could ever satisfy it, so
	// waiting would only defer a certain error.
	Acquire(ctx context.Context, owner string, want Set) (Lease, error)
	// TryAcquire is the non-blocking form. It never reclaims, because a
	// caller that cannot wait also cannot afford eviction I/O.
	TryAcquire(owner string, want Set) (Lease, bool)

	// Free is what is immediately available.
	Free() Set
	// Reclaimable is what registered reclaimers could free on top of Free.
	Reclaimable() Set
	// Capacity is the configured total.
	Capacity() Set
	// Leases is a snapshot of what is currently held.
	Leases() []LeaseInfo

	// RegisterReclaimer installs the reclaim policy for one key.
	RegisterReclaimer(key string, r Reclaimer)
}

// ManagerOption configures a manager.
type ManagerOption func(*manager)

// WithClock replaces the time source, for deterministic tests.
func WithClock(now func() time.Time) ManagerOption {
	return func(m *manager) {
		if now != nil {
			m.now = now
		}
	}
}

type manager struct {
	mu   sync.Mutex
	cond *sync.Cond
	now  func() time.Time

	capacity   Set
	used       Set
	reclaimers map[string]Reclaimer

	nextID int64
	leases map[int64]*lease
}

// NewManager builds a manager over a fixed capacity.
func NewManager(capacity Set, opts ...ManagerOption) Manager {
	m := &manager{
		now:        time.Now,
		capacity:   capacity.Clone(),
		used:       make(Set, len(capacity)),
		reclaimers: make(map[string]Reclaimer),
		leases:     make(map[int64]*lease),
	}
	m.cond = sync.NewCond(&m.mu)

	for _, opt := range opts {
		opt(m)
	}

	if m.capacity == nil {
		m.capacity = make(Set)
	}

	return m
}

func (m *manager) RegisterReclaimer(key string, r Reclaimer) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if r == nil {
		delete(m.reclaimers, key)

		return
	}

	m.reclaimers[key] = r
}

func (m *manager) Capacity() Set {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.capacity.Clone()
}

func (m *manager) Free() Set {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.freeLocked()
}

func (m *manager) freeLocked() Set {
	return m.capacity.Sub(m.used)
}

func (m *manager) Reclaimable() Set {
	m.mu.Lock()
	reclaimers := make(map[string]Reclaimer, len(m.reclaimers))
	for k, r := range m.reclaimers {
		reclaimers[k] = r
	}
	m.mu.Unlock()

	// The reclaimer map is snapshotted under our lock and then queried
	// with it dropped. Available takes the reclaimer's own lock, and a
	// reclaimer takes ours whenever it releases a lease to return units —
	// taking both locks in both orders is a deadlock.
	out := make(Set, len(reclaimers))
	for k, r := range reclaimers {
		out[k] = r.Available(k)
	}

	return out
}

func (m *manager) Leases() []LeaseInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]LeaseInfo, 0, len(m.leases))
	for _, l := range m.leases {
		out = append(out, LeaseInfo{
			Owner:      l.owner,
			Held:       l.held.Clone(),
			AcquiredAt: l.acquiredAt,
		})
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].AcquiredAt.Before(out[j].AcquiredAt)
	})

	return out
}

func (m *manager) TryAcquire(owner string, want Set) (Lease, bool) {
	if want.IsZero() {
		return m.grant(owner, want), true
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if !want.Fits(m.freeLocked()) {
		return nil, false
	}

	return m.grantLocked(owner, want), true
}

func (m *manager) Acquire(ctx context.Context, owner string, want Set) (Lease, error) {
	if want.IsZero() {
		return m.grant(owner, want), nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// A request larger than total capacity can never be satisfied.
	if over := want.Exceeds(m.capacity); len(over) > 0 {
		return nil, fmt.Errorf("%w: %v exceeds worker capacity", ErrCapacityExceeded, over)
	}

	stop := m.watchContext(ctx)
	defer stop()

	for !want.Fits(m.freeLocked()) {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("%w: waiting for %v: %w",
				ErrCapacityExceeded, want.Exceeds(m.freeLocked()), err)
		}

		if m.reclaimLocked(ctx, want) {
			continue
		}

		// Nothing reclaimable on any short key. Only a release can help.
		m.cond.Wait()
	}

	return m.grantLocked(owner, want), nil
}

// reclaimLocked asks the registered reclaimer for each short key to free
// the shortfall. It reports whether anything was freed, which is only a
// "something changed, re-check the ledger" signal for the caller's loop.
//
// The freed amount is deliberately not added back here. The manager's
// invariant is used == Σ live lease.held, and release is its only
// mutator: a reclaimer returns units by releasing the lease that held
// them, so by the time Reclaim returns the ledger already reflects the
// change. Crediting the return value on top would count those units
// twice — the lease still records them until it is released, and
// release subtracts held in full — which is how a worker would slowly
// invent capacity it does not have.
//
// The manager's lock is released across the reclaimer call. That is
// what makes the design work rather than an optimization: Reclaim calls
// back into the manager to return the bytes, so holding the lock
// through it would deadlock on the first eviction. It also keeps
// eviction I/O off the admission path for every other caller.
func (m *manager) reclaimLocked(ctx context.Context, want Set) bool {
	free := m.freeLocked()

	var freedAny bool

	for _, key := range want.Exceeds(free) {
		r, ok := m.reclaimers[key]
		if !ok {
			continue
		}

		need := want[key] - free[key]

		m.mu.Unlock()
		freed, err := r.Reclaim(ctx, key, need)
		m.mu.Lock()

		if err == nil && freed > 0 {
			freedAny = true
		}
	}

	return freedAny
}

// watchContext broadcasts when ctx ends so a waiter is interruptible.
func (m *manager) watchContext(ctx context.Context) func() {
	if ctx.Done() == nil {
		return func() {}
	}

	done := make(chan struct{})

	go func() {
		select {
		case <-ctx.Done():
			m.mu.Lock()
			m.cond.Broadcast()
			m.mu.Unlock()
		case <-done:
		}
	}()

	return func() { close(done) }
}

func (m *manager) grant(owner string, want Set) Lease {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.grantLocked(owner, want)
}

func (m *manager) grantLocked(owner string, want Set) *lease {
	m.nextID++

	l := &lease{
		id:         m.nextID,
		mgr:        m,
		owner:      owner,
		held:       want.Clone(),
		acquiredAt: m.now(),
	}

	m.used = m.used.Add(want)
	m.leases[l.id] = l

	return l
}

// release returns a lease's resources. It is the only path that reduces
// used, reclamation included, which is what keeps the invariant
// used == Σ live lease.held true by construction.
//
// Idempotent: releasing twice must not credit the ledger twice, or a
// worker slowly invents capacity. The sync.Once on the lease and the
// liveness check here are belt and braces — either alone would hold for
// the paths that exist today, so both are kept deliberately.
func (m *manager) release(l *lease) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, live := m.leases[l.id]; !live {
		return
	}

	delete(m.leases, l.id)
	m.used = m.used.Sub(l.held)
	m.cond.Broadcast()
}

type lease struct {
	id         int64
	mgr        *manager
	owner      string
	held       Set
	acquiredAt time.Time
	once       sync.Once
}

func (l *lease) Held() Set     { return l.held.Clone() }
func (l *lease) Owner() string { return l.owner }

func (l *lease) Release() {
	l.once.Do(func() { l.mgr.release(l) })
}
