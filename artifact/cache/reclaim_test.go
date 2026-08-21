package cache_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
	"github.com/xraph/dispatch/artifact/cache"
	"github.com/xraph/dispatch/resource"
)

// The cache is what makes resource.Manager's disk key reclaimable.
// Nothing else in the process holds evictable bytes.
var _ resource.Reclaimer = (*cache.Cache)(nil)

// entrySize is the size of the objects stageAndRelease stages, small
// enough that a capacity of 100 is several whole entries and the
// arithmetic in these tests is readable.
const entrySize = 10

// managedCache builds a cache sharing mgr's ledger, preloaded with
// objects named "a".."a"+n-1 of size bytes each.
func managedCache(t *testing.T, mgr resource.Manager, count, size int) *cache.Cache {
	t.Helper()

	b := artifacttest.NewBackend()

	for i := range count {
		// Distinct bytes per object: identical bytes are one cache
		// entry by design, and these tests are counting entries.
		payload := make([]byte, size)
		for j := range payload {
			payload[j] = byte('a' + i)
		}

		b.Put("m", objectKey(i), payload)
	}

	c, err := cache.New(t.TempDir(), b, cache.WithManager(mgr))
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	t.Cleanup(func() {
		if cerr := c.Close(); cerr != nil {
			t.Errorf("cache close: %v", cerr)
		}
	})

	return c
}

func objectKey(i int) string { return string(rune('a' + i)) }

// stageAndRelease stages object i and immediately releases it, leaving
// it cached and evictable. It returns the staged path.
func stageAndRelease(t *testing.T, c *cache.Cache, i int) string {
	t.Helper()

	path, _, release, err := c.Stage(context.Background(),
		artifact.Ref{Bucket: "m", Key: objectKey(i), Size: entrySize})
	if err != nil {
		t.Fatalf("Stage %s: %v", objectKey(i), err)
	}

	release()

	return path
}

// heldByLeases totals what every live lease says it holds, which the
// manager's own used must equal exactly. Capacity minus Free is used,
// so this compares the ledger against the leases that justify it —
// the invariant a reclaimer crediting the ledger itself would break.
func heldByLeases(t *testing.T, mgr resource.Manager, key string) (used, held int64) {
	t.Helper()

	used = mgr.Capacity()[key] - mgr.Free()[key]

	for _, l := range mgr.Leases() {
		held += l.Held[key]
	}

	return used, held
}

func assertLedgerBalanced(t *testing.T, mgr resource.Manager) {
	t.Helper()

	used, held := heldByLeases(t, mgr, resource.Disk)
	if used != held {
		t.Fatalf("ledger broken: used = %d but live leases hold %d — "+
			"reclamation must return bytes by releasing a lease, never by crediting the ledger",
			used, held)
	}
}

// TestCacheIgnoresNonDiskKeys pins the boundary of what a disk cache may
// speak for. Reporting memory it does not hold would have the manager
// admit work the box cannot run.
func TestCacheIgnoresNonDiskKeys(t *testing.T) {
	mgr := resource.NewManager(resource.Set{resource.Disk: 100, resource.Memory: 100})
	c := managedCache(t, mgr, 2, 10)

	pathA := stageAndRelease(t, c, 0)
	stageAndRelease(t, c, 1)

	if got := c.Available(resource.Memory); got != 0 {
		t.Fatalf("Available(memory) = %d, want 0 — the cache holds bytes on a volume and nothing else", got)
	}

	freed, err := c.Reclaim(context.Background(), resource.Memory, 100)
	if err != nil {
		t.Fatalf("Reclaim(memory): %v", err)
	}

	if freed != 0 {
		t.Fatalf("Reclaim(memory) = %d, want 0", freed)
	}

	if _, serr := os.Stat(pathA); serr != nil {
		t.Fatalf("Reclaim(memory) evicted a cached file: %v", serr)
	}

	if used := c.Used(); used != 20 {
		t.Fatalf("Used() = %d, want 20 — a memory reclaim must not touch disk accounting", used)
	}

	if got := c.Available(resource.Disk); got != 20 {
		t.Fatalf("Available(disk) = %d, want 20", got)
	}
}

// TestStagingSpendsSharedManagerDisk is the connection the whole task
// exists to make: bytes on disk are bytes the shared ledger has spent.
func TestStagingSpendsSharedManagerDisk(t *testing.T) {
	mgr := resource.NewManager(resource.Set{resource.Disk: 100})
	c := managedCache(t, mgr, 1, 30)

	before := mgr.Free()[resource.Disk]

	_, _, release, err := c.Stage(context.Background(),
		artifact.Ref{Bucket: "m", Key: "a", Size: 30})
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}

	defer release()

	if after := mgr.Free()[resource.Disk]; after != before-30 {
		t.Fatalf("Free()[disk] = %d, want %d — staged bytes must be spent from the shared ledger",
			after, before-30)
	}

	if got := mgr.Reclaimable()[resource.Disk]; got != 0 {
		t.Fatalf("Reclaimable()[disk] = %d, want 0 while the entry is leased", got)
	}

	assertLedgerBalanced(t, mgr)
}

// TestReclaimEvictsLRUAndReturnsBytes covers the reclaimer contract
// itself: least-recently-used first, only as much as was asked for, and
// the manager's ledger reflects it because the lease was released.
func TestReclaimEvictsLRUAndReturnsBytes(t *testing.T) {
	mgr := resource.NewManager(resource.Set{resource.Disk: 100})
	c := managedCache(t, mgr, 3, 10)

	// Staged oldest first, so a is the least recently used. The gaps
	// keep the ordering an assertion about LRU rather than about clock
	// resolution.
	pathA := stageAndRelease(t, c, 0)
	time.Sleep(2 * time.Millisecond)
	pathB := stageAndRelease(t, c, 1)
	time.Sleep(2 * time.Millisecond)
	pathC := stageAndRelease(t, c, 2)

	if got := mgr.Reclaimable()[resource.Disk]; got != 30 {
		t.Fatalf("Reclaimable()[disk] = %d, want 30", got)
	}

	freed, err := c.Reclaim(context.Background(), resource.Disk, 15)
	if err != nil {
		t.Fatalf("Reclaim: %v", err)
	}

	if freed != 20 {
		t.Fatalf("Reclaim(15) freed %d, want 20 — whole entries, and no more than the shortfall needs", freed)
	}

	if free := mgr.Free()[resource.Disk]; free != 90 {
		t.Fatalf("Free()[disk] = %d, want 90 — the evicted leases must be released, not merely counted", free)
	}

	assertLedgerBalanced(t, mgr)

	for _, p := range []string{pathA, pathB} {
		if _, serr := os.Stat(p); !os.IsNotExist(serr) {
			t.Fatalf("evicted file %s still on disk: %v", p, serr)
		}
	}

	if _, serr := os.Stat(pathC); serr != nil {
		t.Fatalf("most recently used entry was evicted first: %v", serr)
	}

	if used := c.Used(); used != 10 {
		t.Fatalf("Used() = %d, want 10", used)
	}
}

// TestReclaimNeverEvictsALeasedEntry: a running handler holds the path,
// so those bytes are not the manager's to take back however short it is.
func TestReclaimNeverEvictsALeasedEntry(t *testing.T) {
	mgr := resource.NewManager(resource.Set{resource.Disk: 100})
	c := managedCache(t, mgr, 2, 10)

	pathA, _, releaseA, err := c.Stage(context.Background(),
		artifact.Ref{Bucket: "m", Key: "a", Size: 10})
	if err != nil {
		t.Fatalf("Stage a: %v", err)
	}

	defer releaseA()

	stageAndRelease(t, c, 1)

	freed, err := c.Reclaim(context.Background(), resource.Disk, 1000)
	if err != nil {
		t.Fatalf("Reclaim: %v", err)
	}

	if freed != 10 {
		t.Fatalf("Reclaim(1000) freed %d, want 10 — only the unleased entry was available", freed)
	}

	if _, serr := os.Stat(pathA); serr != nil {
		t.Fatalf("leased entry was evicted: %v", serr)
	}

	if got := c.Available(resource.Disk); got != 0 {
		t.Fatalf("Available(disk) = %d, want 0 — everything left is pinned", got)
	}

	assertLedgerBalanced(t, mgr)
}

// TestPrivateManagerBehavesLikeTheOldBudget is the degradation
// guarantee: a cache constructed the way every existing caller
// constructs one still owns its own allowance and reports it the same
// way.
func TestPrivateManagerBehavesLikeTheOldBudget(t *testing.T) {
	c, b := newCache(t, 64)
	b.Put("m", "a", []byte("0123456789"))

	if got := c.Budget(); got != 64 {
		t.Fatalf("Budget() = %d, want 64", got)
	}

	if got := c.Used(); got != 0 {
		t.Fatalf("Used() = %d, want 0 on a fresh cache", got)
	}

	_, _, release, err := c.Stage(context.Background(),
		artifact.Ref{Bucket: "m", Key: "a", Size: 10})
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}

	if got := c.Used(); got != 10 {
		t.Fatalf("Used() = %d, want 10", got)
	}

	release()

	if got := c.Used(); got != 10 {
		t.Fatalf("Used() = %d after release, want 10 — releasing a lease keeps the bytes cached", got)
	}
}

// probingManager wraps a resource.Manager and signals every time the
// reclaimer registered for a key is ASKED to free something and answers
// "nothing".
//
// That callback is the only externally visible moment in
// resource.Manager.Acquire's wait loop, and it is precisely the moment
// the test needs: reclaimLocked runs with the manager's lock DROPPED,
// immediately before the acquirer checks the reclaim generation and goes
// into cond.Wait. Releasing the cache entry from inside that window is
// the exact race the generation counter exists to survive — a plain
// Broadcast there reaches nobody, because the acquirer is not waiting yet.
type probingManager struct {
	resource.Manager

	refused chan struct{} // "asked to evict, gave nothing"
	proceed chan struct{} // test → reclaimer: you may return now
}

func (m *probingManager) RegisterReclaimer(key string, r resource.Reclaimer) {
	if r == nil {
		m.Manager.RegisterReclaimer(key, nil)

		return
	}

	m.Manager.RegisterReclaimer(key, &probingReclaimer{
		Reclaimer: r,
		refused:   m.refused,
		proceed:   m.proceed,
	})
}

type probingReclaimer struct {
	resource.Reclaimer

	once    sync.Once
	refused chan struct{}
	proceed chan struct{}
}

func (r *probingReclaimer) Reclaim(ctx context.Context, key string, need int64) (int64, error) {
	freed, err := r.Reclaimer.Reclaim(ctx, key, need)

	// Only the FIRST refusal is gated. The acquirer rounds its loop more
	// than once and a second block would never be released.
	//
	// Gating AFTER the wrapped call is what makes it safe: the cache has
	// let go of its entry table by the time it returns, so the test
	// goroutine can release an entry from here without deadlocking
	// against it.
	if freed == 0 {
		r.once.Do(func() {
			r.refused <- struct{}{}
			<-r.proceed
		})
	}

	return freed, err
}

// TestBlockedStageWakesWhenAnEntryIsReleased covers the wake-up the
// manager cannot generate for itself: it broadcasts when a lease is
// released, and a cache lease dropping to zero is not one of those, yet
// it is exactly when the blocked stager's space becomes available.
//
// Without it the stage below sleeps until its deadline and the job
// requeues for no reason — a worker going quiet rather than erroring.
// This is the guard for the permanent-hang bug found during the track, so
// it is synchronised rather than timed: it waits for the stager to be
// observed asking the cache for space and being refused, which is the
// last thing that happens before it sleeps. A sleep would let the release
// land FIRST on a slow or loaded machine, and then the second Stage
// succeeds trivially without the wake-up path running at all — a broken
// generation counter would be invisible and the test would still pass.
//
// Releasing at the refusal point is also deliberately the HARDEST timing,
// not merely a deterministic one: reclaimLocked holds no manager lock
// across the reclaimer call, so the release and its Wake land inside the
// window where the acquirer has already been told "nothing available" and
// has not yet reached cond.Wait. Only the generation counter carries the
// signal across that gap.
//
// Mutation-verified: deleting the reclaimGen re-check in
// resource.Manager.Acquire makes this hang to the 5s deadline and fail.
func TestBlockedStageWakesWhenAnEntryIsReleased(t *testing.T) {
	refused := make(chan struct{})
	proceed := make(chan struct{})
	mgr := &probingManager{
		Manager: resource.NewManager(resource.Set{resource.Disk: 10}),
		refused: refused,
		proceed: proceed,
	}

	c := managedCache(t, mgr, 2, 10)

	_, _, releaseA, err := c.Stage(context.Background(),
		artifact.Ref{Bucket: "m", Key: "a", Size: 10})
	if err != nil {
		t.Fatalf("Stage a: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	done := make(chan error, 1)

	go func() {
		_, _, release, serr := c.Stage(ctx, artifact.Ref{Bucket: "m", Key: "b", Size: 10})
		if release != nil {
			release()
		}

		done <- serr
	}()

	// Wait for the stager to be exactly where this test needs it: it has
	// asked the cache to evict, the cache — whose only entry is still
	// pinned — has answered "nothing", and it is now held inside that
	// call with the manager's lock DROPPED and cond.Wait still ahead of
	// it.
	select {
	case <-refused:
	case <-time.After(5 * time.Second):
		t.Fatal("the second Stage never reached the cache's reclaimer; " +
			"it is not blocked where this test believes it is")
	}

	// Both halves of the wake-up land in that window: the entry loses its
	// last stager, and the cache's Wake bumps the generation and
	// broadcasts to a waiter that is not waiting yet.
	releaseA()

	close(proceed)

	select {
	case serr := <-done:
		if serr != nil {
			t.Fatalf("blocked Stage: %v", serr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stage never woke after the entry it needed was released")
	}

	assertLedgerBalanced(t, mgr)
}

// TestConcurrentStageAndReclaim runs the two paths that touch the entry
// table against each other under -race: stagers pinning, using and
// releasing entries while jobs reclaim the same volume out from under
// them.
//
// It is looking for three things a single-threaded test cannot see. A
// deadlock, because Reclaim takes the table lock and then the manager's
// while Acquire is holding neither and Available must hold only the
// first. A path handed back for a file eviction already unlinked, which
// is what the pin-or-retry loop in Stage exists to prevent. And a
// ledger that stops matching its leases, which is what a reclaimer
// crediting the manager itself would produce.
func TestConcurrentStageAndReclaim(t *testing.T) {
	const (
		objects = 8
		stagers = 6
		rounds  = 40
	)

	mgr := resource.NewManager(resource.Set{resource.Disk: 45})
	c := managedCache(t, mgr, objects, entrySize)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	for s := range stagers {
		wg.Add(1)

		go func(s int) {
			defer wg.Done()

			for r := range rounds {
				ref := artifact.Ref{
					Bucket: "m",
					Key:    objectKey((s + r) % objects),
					Size:   entrySize,
				}

				path, _, release, err := c.Stage(ctx, ref)
				if err != nil {
					t.Errorf("stager %d round %d: %v", s, r, err)

					return
				}

				// The whole point of a lease: while it is held, the file
				// is there. A reclaimer taking it would show up here.
				if _, serr := os.Stat(path); serr != nil {
					t.Errorf("stager %d round %d: staged path unusable: %v", s, r, serr)
				}

				release()
			}
		}(s)
	}

	// A job competing for the same volume, redeeming the disk the cache
	// is holding.
	wg.Add(1)

	go func() {
		defer wg.Done()

		for range rounds {
			lease, err := mgr.Acquire(ctx, "job", resource.Set{resource.Disk: 20})
			if err != nil {
				t.Errorf("job admission: %v", err)

				return
			}

			lease.Release()
		}
	}()

	wg.Wait()

	assertLedgerBalanced(t, mgr)

	if used, avail := c.Used(), c.Available(resource.Disk); avail > used {
		t.Fatalf("Available(disk) = %d exceeds Used() = %d — the cache is offering bytes it does not hold",
			avail, used)
	}
}

// TestCachedBytesAdmitAJobThatWouldNotFit is the property the whole
// task exists for.
//
// The manager's free disk is short, so TryAcquire — what a caller that
// cannot wait would use — refuses. Acquire admits the same job, because
// the cache is registered as the disk reclaimer and evicts to cover the
// shortfall. Everything here is real: a real manager, a real cache, real
// files on disk.
func TestCachedBytesAdmitAJobThatWouldNotFit(t *testing.T) {
	mgr := resource.NewManager(resource.Set{resource.Disk: 100})
	c := managedCache(t, mgr, 6, 10)

	// Sixty bytes of warm, evictable cache.
	for i := range 6 {
		stageAndRelease(t, c, i)
	}

	if free := mgr.Free()[resource.Disk]; free != 40 {
		t.Fatalf("Free()[disk] = %d, want 40", free)
	}

	if got := mgr.Reclaimable()[resource.Disk]; got != 60 {
		t.Fatalf("Reclaimable()[disk] = %d, want 60 — a warm cache is what a worker offers on top of free",
			got)
	}

	want := resource.Set{resource.Disk: 70}

	if _, ok := mgr.TryAcquire("job", want); ok {
		t.Fatal("TryAcquire admitted 70 against 40 free — this test proves nothing if it fits already")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lease, err := mgr.Acquire(ctx, "job", want)
	if err != nil {
		t.Fatalf("Acquire 70 disk with 60 evictable bytes cached: %v — "+
			"the reclaimer path is the whole point of registering the cache", err)
	}

	defer lease.Release()

	if free := mgr.Free()[resource.Disk]; free < 0 {
		t.Fatalf("Free()[disk] = %d — the ledger went negative", free)
	}

	assertLedgerBalanced(t, mgr)

	// Only what the shortfall needed: 30 bytes of cache had to go, which
	// is three ten-byte entries.
	if used := c.Used(); used != 30 {
		t.Fatalf("cache Used() = %d, want 30 — reclamation must stop at the shortfall", used)
	}
}
