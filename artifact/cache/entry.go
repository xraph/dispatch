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
	// size is the file's byte count, as accounted against the budget.
	size int64
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

// put records an entry and, when coord is non-empty, its coordinate alias.
func (t *entryTable) put(e *entry, coord string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.byHash[e.hash] = e

	if coord != "" {
		t.byCoord[coord] = e.hash
	}
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
func (t *entryTable) lease(e *entry, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	e.leases++
	e.lastUsed = now
}

// release unpins an entry.
func (t *entryTable) release(e *entry) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if e.leases > 0 {
		e.leases--
	}
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
