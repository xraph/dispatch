package cache

import (
	"sync"
	"time"
)

// entry is one cached object on disk.
type entry struct {
	// hash is the BLAKE3 content hash, formatted "blake3:<hex>".
	hash string
	// path is the absolute location of the file.
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
	// lastUsed drives least-recent-use eviction.
	lastUsed time.Time
}

// entryTable holds the cache's in-memory view of what is on disk.
//
// Two lookup paths matter. byHash is the content-addressed one and is
// authoritative. byCoord maps an artifact's storage coordinates to a hash
// so a ref whose content_hash is still NULL — every freshly registered
// artifact — can hit the cache on its second stage.
type entryTable struct {
	mu      sync.Mutex
	byHash  map[string]*entry
	byCoord map[string]string
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
// alias. It returns whichever entry now owns the hash, which is not e
// when one was already there.
//
// Two downloads of different coordinates can produce identical bytes at
// the same moment and both miss the content-address check. Only one of
// them can own the file: overwriting here would strand the other
// entry's hold with no path back to the manager, leaking capacity for
// the life of the process, and would drop an entry other stagers may
// already be holding. The loser is told so and hands its hold back.
func (t *entryTable) put(e *entry, coord string) *entry {
	t.mu.Lock()
	defer t.mu.Unlock()

	live, ok := t.byHash[e.hash]
	if !ok {
		live = e
		t.byHash[e.hash] = e
	}

	if coord != "" {
		t.byCoord[coord] = live.hash
	}

	return live
}

// alias points a coordinate at an existing hash.
func (t *entryTable) alias(coord, hash string) {
	if coord == "" {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.byCoord[coord] = hash
}

// lease pins an entry and marks it recently used.
//
// It reports false when the entry is no longer in the table, which
// means eviction took it: the file is gone and its bytes have been
// credited back to the manager, so the caller must go and stage it
// again rather than pin a corpse. Checking membership under the same
// lock that evictLRU removes under is what makes the two mutually
// exclusive — either this pins the entry first and eviction skips it,
// or eviction wins and this fails.
func (t *entryTable) lease(e *entry, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.byHash[e.hash] != e {
		return false
	}

	e.leases++
	e.lastUsed = now

	return true
}

// release unpins an entry and reports whether that was its last lease.
// Only that release changes what eviction could free, so only that one
// is worth waking a blocked acquirer for.
func (t *entryTable) release(e *entry) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if e.leases == 0 {
		return false
	}

	e.leases--

	return e.leases == 0
}

// evictLRU removes the least recently used unleased entry and returns it.
// It returns nil when every entry is leased.
func (t *entryTable) evictLRU() *entry {
	t.mu.Lock()
	defer t.mu.Unlock()

	var victim *entry

	for _, e := range t.byHash {
		if e.leases > 0 {
			continue
		}

		if victim == nil || e.lastUsed.Before(victim.lastUsed) {
			victim = e
		}
	}

	if victim == nil {
		return nil
	}

	delete(t.byHash, victim.hash)

	for coord, hash := range t.byCoord {
		if hash == victim.hash {
			delete(t.byCoord, coord)
		}
	}

	return victim
}

// evictableBytes totals the entries that could be evicted right now.
// It is what the cache reports as reclaimable disk, so it counts only
// what a stager is not holding.
func (t *entryTable) evictableBytes() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	var total int64

	for _, e := range t.byHash {
		if e.leases == 0 {
			total += e.size
		}
	}

	return total
}

// all returns a snapshot of every entry.
func (t *entryTable) all() []*entry {
	t.mu.Lock()
	defer t.mu.Unlock()

	out := make([]*entry, 0, len(t.byHash))
	for _, e := range t.byHash {
		out = append(out, e)
	}

	return out
}
