package cache

import (
	"sync"
)

// entry is one cached object on disk.
type entry struct {
	// hash is the BLAKE3 content hash, formatted "blake3:<hex>".
	hash string
	// path is the absolute location of the file. On the download path
	// publish fills it in, because the file only has a home once it has
	// been renamed into one, and that happens under the table's lock.
	path string
	// size is the file's byte count, as accounted against the manager.
	size int64
	// hold is the manager capacity backing those bytes. It is set
	// before the entry is published and released when the entry is
	// evicted, so hold.bytes equals size for the whole time an entry is
	// reachable through this table.
	hold *hold
	// leases counts the stagers currently using this entry. An entry with
	// leases > 0 must never be evicted: a running handler holds its path.
	leases int
	// coords are the coordinate aliases resolving to this entry, so
	// eviction can drop them by name instead of scanning every alias in
	// the table.
	coords []string
	// prev, next and evictable thread the eviction list. An entry is on
	// that list exactly while nothing holds it; evictable says so,
	// because a list of one has nil on both sides.
	prev, next *entry
	evictable  bool
}

// entryTable holds the cache's in-memory view of what is on disk.
//
// Two lookup paths matter. byHash is the content-addressed one and is
// authoritative. byCoord maps an artifact's storage coordinates to a hash
// so a ref whose content_hash is still NULL — every freshly registered
// artifact — can hit the cache on its second stage.
//
// Everything here is O(1). Eviction runs on the admission path, where a
// worker has one poll interval to free what a claimed job needs, and a
// scan of the table per victim would put the cost of one eviction in
// proportion to how warm the cache is. A 20 GiB cache of 100 KiB
// objects is 200k entries: scanning made a single eviction cost
// milliseconds, so a batch could free tens of megabytes and the disk a
// worker offered as free-plus-reclaimable stopped being redeemable at
// exactly the size where it mattered.
type entryTable struct {
	mu      sync.Mutex
	byHash  map[string]*entry
	byCoord map[string]string

	// head and tail are the eviction list, most recently released
	// first, so the tail is always the victim and finding it is a
	// pointer read. Only unleased entries are linked, which is what
	// makes that true without a scan past the pinned ones.
	head, tail *entry
	// evictable totals the bytes on that list.
	evictable int64
}

func newEntryTable() *entryTable {
	return &entryTable{
		byHash:  make(map[string]*entry),
		byCoord: make(map[string]string),
	}
}

// coordKey identifies an artifact's storage location.
func coordKey(backend, bucket, key string) string {
	return backend + "\x00" + bucket + "\x00" + key
}

// getByHash returns the entry for a content hash.
func (t *entryTable) getByHash(hash string) (*entry, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	e, ok := t.byHash[hash]

	return e, ok
}

// getByCoord returns the entry cached for an artifact's coordinates.
func (t *entryTable) getByCoord(coord string) (*entry, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	hash, ok := t.byCoord[coord]
	if !ok {
		return nil, false
	}

	e, ok := t.byHash[hash]

	return e, ok
}

// put records an entry and, when coord is non-empty, its coordinate
// alias.
//
// This is the startup path, walking files that are already on disk, so
// it has no losing entry to hand back: one file per hash means the
// collision putLocked guards against cannot happen here. Everything
// that publishes an entry while the cache is live goes through publish
// instead, because it has a file to put on disk and that has to happen
// under this same lock.
func (t *entryTable) put(e *entry, coord string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	_ = t.putLocked(e, coord)
}

// putLocked is put's body, for callers already holding the lock.
//
// Two downloads of different coordinates can produce identical bytes at
// the same moment and both miss the content-address check. Only one of
// them can own the file: overwriting here would strand the other
// entry's hold with no path back to the manager, leaking capacity for
// the life of the process, and would drop an entry other stagers may
// already be holding. The loser is told so and hands its hold back.
func (t *entryTable) putLocked(e *entry, coord string) *entry {
	live, ok := t.byHash[e.hash]
	if !ok {
		live = e
		t.byHash[e.hash] = e
		t.link(e)
	}

	t.aliasLocked(coord, live)

	return live
}

// publish puts a downloaded file into its content-addressed home and
// registers the entry that owns it, both under this lock. place does
// the filesystem half and returns the path it settled on.
//
// The two halves cannot be separated. Eviction unlinks a victim's file
// under this same lock, so serialising against it here is what upholds
// the table's central invariant: a file exists at a hash's path if and
// only if the table holds an entry for that hash. Promote outside the
// lock and a download can stat a victim's file in the window after
// eviction dropped it from the table and before it unlinked it, adopt
// the doomed file, register an entry for it, and hand a live lease on a
// path that is deleted a moment later. Worse, the entry stays in the
// table pointing at nothing, so every later stager of that artifact is
// handed the same corpse until it is evicted again.
//
// Both critical sections are two syscalls rather than scans, so
// eviction stays O(1) in the size of the cache, which is what the
// admission path needs from it.
func (t *entryTable) publish(e *entry, coord string, place func() (string, error)) (*entry, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	path, err := place()
	if err != nil {
		return nil, err
	}

	e.path = path

	return t.putLocked(e, coord), nil
}

// alias points a coordinate at an existing hash.
func (t *entryTable) alias(coord, hash string) {
	if coord == "" {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if e, ok := t.byHash[hash]; ok {
		t.aliasLocked(coord, e)
	}
}

// aliasLocked records a coordinate against an entry, once.
//
// The repeat check is not an optimisation: every cache hit on a
// content-hashed ref re-aliases the same coordinate, and appending each
// time would grow the entry's coords slice for as long as the entry
// lives. A coordinate that moves to a different entry leaves its name
// behind on the old one, which costs nothing — eviction only deletes an
// alias that still resolves to the entry being evicted.
func (t *entryTable) aliasLocked(coord string, e *entry) {
	if coord == "" || t.byCoord[coord] == e.hash {
		return
	}

	t.byCoord[coord] = e.hash
	e.coords = append(e.coords, coord)
}

// lease pins an entry and takes it off the eviction list.
//
// It reports false when the entry is no longer in the table, which
// means eviction took it: the file is gone and its bytes have been
// credited back to the manager, so the caller must go and stage it
// again rather than pin a corpse. Checking membership under the same
// lock that evictLRU removes under is what makes the two mutually
// exclusive — either this pins the entry first and eviction cannot see
// it, or eviction wins and this fails.
func (t *entryTable) lease(e *entry) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.byHash[e.hash] != e {
		return false
	}

	e.leases++

	if e.leases == 1 {
		t.unlink(e)
	}

	return true
}

// release unpins an entry and reports whether that was its last lease.
//
// The last release puts the entry back at the head of the eviction
// list, which is what "recently used" means here: an entry's place in
// the queue is set by when it stopped being used, not by when it was
// picked up. Only that release changes what eviction could free, so
// only that one is worth waking a blocked acquirer for.
func (t *entryTable) release(e *entry) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if e.leases == 0 {
		return false
	}

	e.leases--

	if e.leases > 0 {
		return false
	}

	// An entry only leaves the table by eviction and eviction never
	// takes a leased one, so this one is still there — belt and braces
	// against a future path that is not so careful.
	if t.byHash[e.hash] == e {
		t.link(e)
	}

	return true
}

// evictLRU removes the least recently used unleased entry, unlinks its
// file through remove, and returns it. It returns nil, without calling
// remove, when every entry is leased.
//
// remove runs under the lock on purpose: see publish. Dropping the
// entry and deleting its file have to look like one step to a download
// promoting the same hash, or that download adopts a file that is
// already condemned. remove must not touch the resource manager.
// Crediting the victim's bytes back is the caller's job, once this has
// returned and the lock is gone.
func (t *entryTable) evictLRU(remove func(*entry)) *entry {
	t.mu.Lock()
	defer t.mu.Unlock()

	victim := t.tail
	if victim == nil {
		return nil
	}

	t.forget(victim)
	remove(victim)

	return victim
}

// drain empties the table and returns everything that was in it,
// leased entries included, for the caller to delete and account for.
//
// Taking the entries out under the lock is what makes Purge safe
// against a concurrent eviction: an entry leaves the table exactly
// once, so its hold is released exactly once, and the two paths cannot
// both claim the same bytes.
func (t *entryTable) drain() []*entry {
	t.mu.Lock()
	defer t.mu.Unlock()

	out := make([]*entry, 0, len(t.byHash))
	for _, e := range t.byHash {
		out = append(out, e)
		t.unlink(e)

		e.coords = nil
	}

	t.byHash = make(map[string]*entry)
	t.byCoord = make(map[string]string)

	return out
}

// evictableBytes totals the entries that could be evicted right now.
// It is what the cache reports as reclaimable disk, so it counts only
// what a stager is not holding.
func (t *entryTable) evictableBytes() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.evictable
}

// forget removes an entry from the table entirely: the eviction list,
// the hash index, and every alias that still resolves to it.
func (t *entryTable) forget(e *entry) {
	t.unlink(e)
	delete(t.byHash, e.hash)

	for _, coord := range e.coords {
		if t.byCoord[coord] == e.hash {
			delete(t.byCoord, coord)
		}
	}

	e.coords = nil
}

// link puts an entry at the head of the eviction list.
func (t *entryTable) link(e *entry) {
	if e.evictable {
		return
	}

	e.evictable = true
	e.prev = nil
	e.next = t.head

	if t.head != nil {
		t.head.prev = e
	}

	t.head = e

	if t.tail == nil {
		t.tail = e
	}

	t.evictable += e.size
}

// unlink takes an entry off the eviction list.
func (t *entryTable) unlink(e *entry) {
	if !e.evictable {
		return
	}

	e.evictable = false

	if e.prev != nil {
		e.prev.next = e.next
	} else {
		t.head = e.next
	}

	if e.next != nil {
		e.next.prev = e.prev
	} else {
		t.tail = e.prev
	}

	e.prev, e.next = nil, nil
	t.evictable -= e.size
}
