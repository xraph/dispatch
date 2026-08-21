package worker

import (
	"context"
	"testing"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
	"github.com/xraph/dispatch/artifact/cache"
	"github.com/xraph/dispatch/resource"
)

const mib = int64(1) << 20

// stageInto puts an object of size bytes in the backend, stages it
// through c, and returns the stager's release func.
func stageInto(t *testing.T, c *cache.Cache, b *artifacttest.Backend, key string, size int64) func() {
	t.Helper()

	b.Put("stage", key, make([]byte, size))

	_, _, release, err := c.Stage(context.Background(), artifact.Ref{
		Backend: b.Name(),
		Bucket:  "stage",
		Key:     key,
		Size:    size,
	})
	if err != nil {
		t.Fatalf("stage %s: %v", key, err)
	}

	return release
}

// TestCacheAndPoolShareOneLedger is the integration this whole wiring
// task exists to get right.
//
// The staging cache and the worker pool must hold the SAME
// resource.Manager. The cache takes a lease per cached entry and
// registers itself as the manager's disk reclaimer; the pool offers disk
// at dequeue as free PLUS what that reclaimer could evict. Wire two
// managers instead of one and every part still works in isolation —
// which is why this needs a test rather than a code review. The cache
// accounts perfectly against its private ledger, the pool accounts
// perfectly against its own, Reclaimable() on the pool's side is
// permanently zero, and a worker with a warm cache quietly stops claiming
// disk-hungry work. No error, no log line, just a worker that went quiet.
//
// So this asserts the coupling end to end, on real bytes: stage through
// the cache, then read the pool's budget.
func TestCacheAndPoolShareOneLedger(t *testing.T) {
	const (
		capacityBytes = 64 * mib
		stagedBytes   = 8 * mib
	)

	mgr := resource.NewManager(resource.Set{
		resource.Memory: 4 * gib,
		resource.Disk:   capacityBytes,
	})

	backend := artifacttest.NewBackend()

	c, err := cache.New(t.TempDir(), backend, cache.WithManager(mgr))
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	p := &Pool{resources: mgr}

	if got := p.dequeueBudget()[resource.Disk]; got != capacityBytes {
		t.Fatalf("empty cache: budget disk = %d, want %d", got, capacityBytes)
	}

	release := stageInto(t, c, backend, "model.bin", stagedBytes)

	// Pinned by a live stager. The bytes are spent — they are neither free
	// nor evictable — so the budget has to shrink by exactly that much.
	// This is the half that proves the cache's lease landed in the
	// manager the pool is reading.
	if got, want := p.dequeueBudget()[resource.Disk], capacityBytes-stagedBytes; got != want {
		t.Fatalf("while pinned: budget disk = %d, want %d", got, want)
	}

	if got, want := mgr.Free()[resource.Disk], capacityBytes-stagedBytes; got != want {
		t.Fatalf("while pinned: manager free disk = %d, want %d", got, want)
	}

	release()

	// Unpinned: still on disk, still leased, but now evictable. The pool
	// must offer it back, because admission can redeem it by reclaiming.
	// A full cache is a healthy cache.
	if got := mgr.Reclaimable()[resource.Disk]; got != stagedBytes {
		t.Fatalf("after release: reclaimable disk = %d, want %d", got, stagedBytes)
	}

	if got := p.dequeueBudget()[resource.Disk]; got != capacityBytes {
		t.Fatalf("after release: budget disk = %d, want %d (free + reclaimable)",
			got, capacityBytes)
	}

	// And the promise is redeemable: a job asking for the whole volume
	// gets it, by evicting.
	lease, aerr := mgr.Acquire(context.Background(), "big-job",
		resource.Set{resource.Disk: capacityBytes})
	if aerr != nil {
		t.Fatalf("acquire the full budget: %v", aerr)
	}

	lease.Release()
}

// TestPrivateCacheManagerIsInvisibleToThePool is the negative control:
// the exact miswiring the test above guards against, asserted to produce
// the exact symptom described.
//
// A cache built without WithManager constructs its own ledger. Everything
// still works — the cache admits, evicts and accounts correctly — but the
// pool's manager never hears about any of it, so staged bytes are neither
// spent nor reclaimable from where admission is looking.
func TestPrivateCacheManagerIsInvisibleToThePool(t *testing.T) {
	const (
		capacityBytes = 64 * mib
		stagedBytes   = 8 * mib
	)

	poolMgr := resource.NewManager(resource.Set{resource.Disk: capacityBytes})

	backend := artifacttest.NewBackend()

	// No WithManager: the cache builds a private one over its own budget.
	c, err := cache.New(t.TempDir(), backend, cache.WithBudget(capacityBytes))
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	release := stageInto(t, c, backend, "model.bin", stagedBytes)
	release()

	p := &Pool{resources: poolMgr}

	if got := p.dequeueBudget()[resource.Disk]; got != capacityBytes {
		t.Fatalf("private manager: budget disk = %d, want %d", got, capacityBytes)
	}

	// The tell: the cache is holding bytes and the pool's ledger reports
	// nothing to reclaim. Free() alone cannot distinguish this from an
	// empty cache, which is why the miswiring is silent.
	if got := poolMgr.Reclaimable()[resource.Disk]; got != 0 {
		t.Fatalf("private manager: reclaimable disk = %d, want 0 — "+
			"the pool's manager cannot see a cache it was not given", got)
	}

	if got := c.Used(); got != stagedBytes {
		t.Fatalf("the cache did hold the bytes: used = %d, want %d", got, stagedBytes)
	}
}
