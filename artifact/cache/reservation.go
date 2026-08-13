package cache

import (
	"context"
	"errors"
	"fmt"

	"github.com/xraph/dispatch/resource"
)

// ErrBudgetExceeded means the cache could not free enough space for a
// stage request.
//
// It is returned both when a single artifact is larger than the whole
// budget — which can never succeed and so fails immediately — and when
// every cached entry is currently leased and the caller's deadline
// elapsed while waiting for one to be released. The manager's own
// resource.ErrCapacityExceeded is kept in the chain, so a caller that
// wants the dimension rather than the layer can still find it.
var ErrBudgetExceeded = errors.New("dispatch/artifact/cache: budget exceeded")

// holdOwner is the lease owner every cached entry is admitted under, so
// an operator reading resource.Manager.Leases() can tell staged bytes
// from the jobs they were staged for.
const holdOwner = "artifact-cache"

// hold is the manager capacity backing one cached object.
//
// It is a slice rather than a single lease because a lease's size is
// fixed once granted, and a ref that carried no size — every freshly
// registered artifact — is only sized after the copy. Growing appends;
// evicting releases every lease and the bytes come back through the one
// path that credits the manager's ledger.
type hold struct {
	leases []resource.Lease
	bytes  int64
}

// acquire takes a manager lease for n bytes of disk.
//
// This is the whole admission path for a staged byte. The manager
// reclaims through this cache's own Reclaim before it blocks and wakes
// on any release, which is the evict-then-wait loop the private budget
// used to run for disk alone — now keyed, and shared with the jobs
// competing for the same volume.
func (c *Cache) acquire(ctx context.Context, n int64) (resource.Lease, error) {
	l, err := c.resources.Acquire(ctx, holdOwner, resource.Set{resource.Disk: n})
	if err != nil {
		return nil, fmt.Errorf("%w: reserving %d bytes: %w", ErrBudgetExceeded, n, err)
	}

	c.used.Add(n)

	return l, nil
}

// newHold reserves n bytes for an object about to be written.
func (c *Cache) newHold(ctx context.Context, n int64) (*hold, error) {
	l, err := c.acquire(ctx, n)
	if err != nil {
		return nil, err
	}

	return &hold{leases: []resource.Lease{l}, bytes: n}, nil
}

// tryHold reserves n bytes without blocking, for the startup walk. It
// must not block: nothing is waiting to release anything yet, and
// evicting a file to make room for another file already on the same
// disk would free nothing.
func (c *Cache) tryHold(n int64) (*hold, bool) {
	l, ok := c.resources.TryAcquire(holdOwner, resource.Set{resource.Disk: n})
	if !ok {
		return nil, false
	}

	c.used.Add(n)

	return &hold{leases: []resource.Lease{l}, bytes: n}, true
}

// resize corrects a hold to the bytes that actually landed on disk.
//
// Growing appends a lease rather than replacing one, so the bytes
// already written stay accounted for and only the difference has to be
// admitted. Shrinking releases and re-takes, because a granted lease
// cannot be made smaller; the request that follows is strictly smaller
// than what was just handed back, so it fits unless a concurrent
// acquirer took the difference first — and then it waits like any other.
func (c *Cache) resize(ctx context.Context, h *hold, want int64) error {
	switch {
	case want == h.bytes:
		return nil

	case want > h.bytes:
		l, err := c.acquire(ctx, want-h.bytes)
		if err != nil {
			return err
		}

		h.leases = append(h.leases, l)
		h.bytes = want

		return nil
	}

	c.releaseHold(h)

	l, err := c.acquire(ctx, want)
	if err != nil {
		return err
	}

	h.leases = []resource.Lease{l}
	h.bytes = want

	return nil
}

// releaseHold returns a hold's bytes to the manager.
//
// Releasing the lease is the only thing that credits the ledger — see
// resource.Reclaimer — so this is what makes eviction actually give
// disk back rather than merely delete a file.
func (c *Cache) releaseHold(h *hold) {
	if h == nil {
		return
	}

	for _, l := range h.leases {
		l.Release()
	}

	c.used.Add(-h.bytes)

	h.leases = nil
	h.bytes = 0
}

// wake asks the manager's waiters to re-check after an entry stopped
// being leased.
//
// That entry just became evictable, so an Acquire that already asked
// this cache to reclaim, was told "everything is pinned", and went to
// sleep can now be satisfied. The manager broadcasts when a lease is
// released and on nothing else — it cannot observe a change in what its
// reclaimers are holding — so a zero-unit lease taken and immediately
// released is the smallest honest way to say so: it moves no capacity
// in either direction and costs two turns of the manager's mutex.
//
// Without it, a stager blocked behind a fully leased cache would sleep
// until its deadline even though the space it needs was freed a
// millisecond later, and the job would requeue for no reason.
func (c *Cache) wake() {
	l, ok := c.resources.TryAcquire(holdOwner, nil)
	if !ok {
		return
	}

	l.Release()
}
