package cache

import (
	"testing"
	"time"
)

// blocked is how long a goroutine has to stay out of the entry table
// before this file believes it is genuinely locked out. It only ever
// costs the suite that long when the invariant holds, and a value this
// side of a second keeps a loaded machine from mattering: the assertions
// below can miss, never misfire, because only the contended path
// completing early fails them.
const blocked = 50 * time.Millisecond

// TestPublishAndEvictionExcludeEachOther pins the invariant the cache's
// staged paths rest on: a file exists at a hash's path if and only if
// the entry table holds an entry for that hash.
//
// Both transitions have to be atomic against each other, so both are
// checked. The second case is the regression. Eviction used to unlink
// after handing the victim back, and in that gap a download of the same
// hash could stat the doomed file, adopt it instead of writing its own,
// and register an entry for a path that was unlinked a moment later.
// Every stager of that artifact was then handed a path with no file
// behind it until the poisoned entry was itself evicted, which is a
// leased entry losing its bytes: exactly what a lease is supposed to
// prevent.
func TestPublishAndEvictionExcludeEachOther(t *testing.T) {
	const (
		hash = "blake3:d0"
		path = "/cache/blake3/d0/d0"
	)

	newTable := func() *entryTable {
		t.Helper()

		tbl := newEntryTable()
		tbl.put(&entry{hash: hash, path: path, size: 1}, "coord")

		return tbl
	}

	t.Run("eviction cannot start while a download is promoting", func(t *testing.T) {
		tbl := newTable()

		promoting := make(chan struct{})
		evicted := make(chan *entry, 1)

		go func() {
			<-promoting

			evicted <- tbl.evictLRU(func(*entry) {})
		}()

		if _, err := tbl.publish(&entry{hash: hash, size: 1}, "coord",
			func() (string, error) {
				// Standing where the rename stands, holding the table.
				close(promoting)

				select {
				case victim := <-evicted:
					t.Errorf("eviction removed %s while a download held the table to promote it; "+
						"the download would adopt a file this eviction is about to unlink", victim.hash)
				case <-time.After(blocked):
				}

				return path, nil
			}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	})

	t.Run("a download cannot promote while eviction is unlinking", func(t *testing.T) {
		tbl := newTable()

		unlinking := make(chan struct{})
		promoted := make(chan struct{})

		go func() {
			<-unlinking

			_, _ = tbl.publish(&entry{hash: hash, size: 1}, "coord",
				func() (string, error) {
					close(promoted)

					return path, nil
				})
		}()

		victim := tbl.evictLRU(func(*entry) {
			// Standing where the unlink stands: the entry has left the
			// table and its file is still on disk.
			close(unlinking)

			select {
			case <-promoted:
				t.Error("a download reached promote while eviction still held the table; " +
					"it would stat the victim's file, adopt it, and hand out a path " +
					"that is unlinked as soon as this returns")
			case <-time.After(blocked):
			}
		})

		if victim == nil {
			t.Fatal("evictLRU returned no victim, but the table held one unleased entry")
		}

		if got, want := victim.hash, hash; got != want {
			t.Fatalf("evicted hash = %q, want %q", got, want)
		}
	})
}

// TestEvictLRUUnlinksBeforeItReturns is the cheap half of the same
// invariant, and the one a refactor is most likely to undo: the removal
// callback is not optional and does not run later.
func TestEvictLRUUnlinksBeforeItReturns(t *testing.T) {
	tbl := newEntryTable()
	tbl.put(&entry{hash: "blake3:d0", path: "/cache/blake3/d0/d0", size: 1}, "coord")

	var removed []string

	victim := tbl.evictLRU(func(e *entry) {
		removed = append(removed, e.path)
	})

	if victim == nil {
		t.Fatal("evictLRU returned no victim, but the table held one unleased entry")
	}

	if got, want := len(removed), 1; got != want {
		t.Fatalf("remove called %d times, want %d", got, want)
	}

	if got, want := removed[0], victim.path; got != want {
		t.Fatalf("removed %q, want the victim's own path %q", got, want)
	}
}

// TestEvictLRUSkipsRemovalWithNothingToEvict: a table whose entries are
// all leased has no victim, and nothing on disk may be touched for one.
func TestEvictLRUSkipsRemovalWithNothingToEvict(t *testing.T) {
	tbl := newEntryTable()

	e := &entry{hash: "blake3:d0", path: "/cache/blake3/d0/d0", size: 1}
	tbl.put(e, "coord")

	if !tbl.lease(e) {
		t.Fatal("lease on a freshly published entry failed")
	}

	called := false

	if victim := tbl.evictLRU(func(*entry) { called = true }); victim != nil {
		t.Fatalf("evictLRU returned %q, want nil while every entry is leased", victim.hash)
	}

	if called {
		t.Fatal("remove ran with no victim to remove")
	}
}
