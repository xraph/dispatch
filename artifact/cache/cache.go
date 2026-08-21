package cache

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/zeebo/blake3"
	"golang.org/x/sync/singleflight"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/resource"
)

// DefaultBudget is the disk allowance when none is configured.
const DefaultBudget int64 = 20 << 30 // 20 GiB

const (
	hashDir = "blake3"
	tmpDir  = "tmp"
	dirPerm = 0o750
)

// hashPrefix labels the digest so the algorithm is visible wherever a
// hash is stored or logged.
const hashPrefix = "blake3:"

// maxStageAttempts bounds Stage's re-download loop. Each attempt means
// an entry was evicted between being staged and being pinned, which
// takes a cache under enough pressure that the next attempt is unlikely
// to fare better.
const maxStageAttempts = 8

// Cache stages artifacts to local disk, content-addressed and bounded by
// a byte budget. It is safe for concurrent use.
//
// Every cached object holds one resource.Manager lease for its bytes,
// and eviction returns them by releasing that lease. That is what lets
// the same ledger that admits jobs see the disk the cache is sitting
// on: without it, staged bytes would be invisible to admission and a
// worker would offer capacity it had already spent.
type Cache struct {
	dir     string
	backend artifact.Backend
	logger  log.Logger

	entries *entryTable
	flight  singleflight.Group

	// resources is the ledger every cached byte is admitted against.
	// With no manager configured this is a private single-key manager
	// over the configured allowance, which is exactly the private disk
	// budget the cache used to own; with one supplied it is the
	// worker's shared ledger.
	resources resource.Manager
	// allowance is the disk capacity of the private manager, used only
	// when no manager is supplied.
	allowance int64
	// used totals the bytes this cache holds against the manager,
	// in-flight downloads included. The manager's own used counts every
	// tenant of the volume, so it cannot answer this question.
	used atomic.Int64

	closeOnce sync.Once
}

// Cache is the manager's disk reclaimer: it is the component that can
// hand bytes back on demand.
var _ resource.Reclaimer = (*Cache)(nil)

// Option configures a Cache.
type Option func(*Cache)

// WithBudget sets the maximum bytes the cache may hold on disk.
//
// It configures the private manager the cache builds for itself, so it
// is ignored when WithManager supplies one: a shared ledger's disk
// capacity is the allowance, and a second ceiling underneath it would
// only be a place for the two to disagree.
func WithBudget(bytes int64) Option {
	return func(c *Cache) {
		if bytes > 0 {
			c.allowance = bytes
		}
	}
}

// WithManager admits cached bytes against a shared resource manager
// rather than a private one.
//
// This is what makes staged bytes visible to job admission. The cache
// registers itself as the manager's disk reclaimer and holds a lease
// per cached entry, so a worker sizing up a job is offered the disk
// that is free plus the disk the cache can evict, and redeems the
// second half by evicting. A nil manager leaves the private one in
// place, so a caller threading an optional dependency through does not
// have to branch.
func WithManager(m resource.Manager) Option {
	return func(c *Cache) {
		if m != nil {
			c.resources = m
		}
	}
}

// WithLogger sets the logger.
func WithLogger(l log.Logger) Option {
	return func(c *Cache) { c.logger = l }
}

// New opens a cache rooted at dir.
//
// Startup wipes the temp directory and rebuilds the entry table by
// walking the hash directories. The walk is the source of truth: there is
// no persisted index to corrupt, so a crash costs at most a re-download.
func New(dir string, backend artifact.Backend, opts ...Option) (*Cache, error) {
	if backend == nil {
		return nil, artifact.ErrNoBackend
	}

	c := &Cache{
		dir:       dir,
		backend:   backend,
		logger:    log.NewNoopLogger(),
		entries:   newEntryTable(),
		allowance: DefaultBudget,
	}

	for _, opt := range opts {
		opt(c)
	}

	if c.resources == nil {
		c.resources = resource.NewManager(resource.Set{resource.Disk: c.allowance})
	}

	if err := os.MkdirAll(filepath.Join(dir, hashDir), dirPerm); err != nil {
		return nil, fmt.Errorf("dispatch/artifact/cache: create hash dir: %w", err)
	}

	if err := c.resetTmp(); err != nil {
		return nil, err
	}

	if err := c.rebuild(); err != nil {
		return nil, err
	}

	// Registered after the walk, so nothing can evict a file whose lease
	// this cache has not taken yet.
	c.resources.RegisterReclaimer(resource.Disk, c)

	return c, nil
}

// Budget returns the configured disk allowance. The engine uses it to
// reject a job definition whose declared inputs could never be staged.
func (c *Cache) Budget() int64 { return c.resources.Capacity()[resource.Disk] }

// Used returns the bytes currently held on disk.
func (c *Cache) Used() int64 { return c.used.Load() }

// Dir returns the cache root.
func (c *Cache) Dir() string { return c.dir }

// resetTmp clears partial downloads left by a previous process.
func (c *Cache) resetTmp() error {
	tmp := filepath.Join(c.dir, tmpDir)

	if err := os.RemoveAll(tmp); err != nil {
		return fmt.Errorf("dispatch/artifact/cache: clear tmp: %w", err)
	}

	if err := os.MkdirAll(tmp, dirPerm); err != nil {
		return fmt.Errorf("dispatch/artifact/cache: create tmp: %w", err)
	}

	return nil
}

// rebuild reconstructs the entry table by walking the hash directories.
//
// Each surviving file takes its own lease, so a restart re-admits what
// is on disk rather than starting from an empty ledger while the volume
// is already full. A file the ledger cannot account for is deleted: the
// allowance has shrunk, or something else on this box now holds the
// volume, and keeping bytes nothing knows about is the exact hole this
// accounting exists to close. The cost is a re-download.
func (c *Cache) rebuild() error {
	root := filepath.Join(c.dir, hashDir)

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		info, ierr := d.Info()
		if ierr != nil {
			return ierr
		}

		h, ok := c.tryHold(info.Size())
		if !ok {
			c.logger.Warn("dispatch/artifact/cache: dropping unaccountable cached file",
				log.String("path", path), log.Int64("bytes", info.Size()))
			c.removeQuietly(path)

			return nil
		}

		// One file per hash on disk, so this never collides.
		c.entries.put(&entry{
			hash: hashPrefix + d.Name(),
			path: path,
			size: info.Size(),
			hold: h,
		}, "")

		return nil
	})
	if err != nil {
		return fmt.Errorf("dispatch/artifact/cache: rebuild index: %w", err)
	}

	return nil
}

// Stage materialises an artifact locally and returns its path, its
// content hash, and a release function the caller must invoke.
//
// The returned path stays valid until release is called. Callers should
// `defer release()` immediately; releasing twice is safe.
func (c *Cache) Stage(ctx context.Context, ref artifact.Ref) (path, hash string, release func(), err error) {
	if ref.Bucket == "" && ref.Key == "" {
		return "", "", nil, errors.New("dispatch/artifact/cache: ref has no storage coordinates")
	}

	backendName := ref.Backend
	if backendName == "" {
		backendName = c.backend.Name()
	}

	coord := coordKey(backendName, ref.Bucket, ref.Key)

	// Pinning is the loop condition, not a step in it. An entry can be
	// evicted between being found and being leased, and handing back a
	// path whose file has just been unlinked — and whose bytes the
	// manager has already credited to someone else — is worse than
	// paying for the download again.
	//
	// Bounded, because the retry is only correct while it is rare. An
	// entry evicted before its own stager can pin it means the cache is
	// thrashing hard enough that this artifact will not survive being
	// fetched however many times we try, and a job that fails saying so
	// beats one that downloads forever under a context with no
	// deadline.
	for range maxStageAttempts {
		// Fast path: a ref that already knows its hash, or coordinates
		// we have staged before.
		if e, ok := c.lookup(ref, coord); ok && c.entries.lease(e) {
			return e.path, e.hash, c.releaseFunc(e), nil
		}

		// Slow path: one download per artifact, however many stagers
		// arrive.
		res, ferr, _ := c.flight.Do(coord, func() (any, error) {
			return c.download(ctx, ref, coord)
		})
		if ferr != nil {
			return "", "", nil, ferr
		}

		e, ok := res.(*entry)
		if !ok {
			return "", "", nil, fmt.Errorf("dispatch/artifact/cache: unexpected flight result %T", res)
		}

		if c.entries.lease(e) {
			return e.path, e.hash, c.releaseFunc(e), nil
		}

		if cerr := ctx.Err(); cerr != nil {
			return "", "", nil, fmt.Errorf("dispatch/artifact/cache: stage %s/%s: %w",
				ref.Bucket, ref.Key, cerr)
		}
	}

	return "", "", nil, fmt.Errorf("%w: %s/%s was evicted before it could be used, %d times running",
		ErrBudgetExceeded, ref.Bucket, ref.Key, maxStageAttempts)
}

// lookup resolves a cached entry by hash, then by coordinates.
func (c *Cache) lookup(ref artifact.Ref, coord string) (*entry, bool) {
	if ref.ContentHash != "" {
		if e, ok := c.entries.getByHash(ref.ContentHash); ok {
			c.entries.alias(coord, e.hash)

			return e, true
		}
	}

	return c.entries.getByCoord(coord)
}

// releaseFunc returns an idempotent release for an entry.
//
// The manager is only nudged when the entry loses its last stager,
// because that is the only release that changes what eviction could
// free.
func (c *Cache) releaseFunc(e *entry) func() {
	var once sync.Once

	return func() {
		once.Do(func() {
			if c.entries.release(e) {
				c.wake()
			}
		})
	}
}

// download fetches an artifact into the cache.
//
// Bytes stream through a BLAKE3 hasher into a temp file, which is then
// renamed into its content-addressed home. Hashing therefore costs
// nothing beyond the read that was happening anyway, which is what lets
// registration skip hashing entirely.
func (c *Cache) download(ctx context.Context, ref artifact.Ref, coord string) (*entry, error) {
	// Re-check under the flight: a concurrent stager may have finished
	// between our miss and our turn here.
	if e, ok := c.lookup(ref, coord); ok {
		return e, nil
	}

	reserved := ref.Size
	if reserved <= 0 {
		// Size unknown. Reserve nothing up front and correct the
		// accounting once the copy reports the real figure.
		reserved = 0
	}

	h, err := c.newHold(ctx, reserved)
	if err != nil {
		return nil, err
	}

	// The hold belongs to whoever ends up owning the bytes. Until an
	// entry takes it, that is nobody, and every path out of here has to
	// give it back.
	committed := false

	defer func() {
		if !committed {
			c.releaseHold(h)
		}
	}()

	rc, err := c.backend.Open(ctx, ref)
	if err != nil {
		if errors.Is(err, dispatch.ErrPermanent) {
			// Preserve the classification: staging an input that is gone,
			// or that we may not read, is permanent, and the executor must
			// fail fast rather than retry. Wrap rather than replace, so the
			// specific sentinel and the backend's message both survive.
			return nil, fmt.Errorf("stage %s/%s: %w", ref.Bucket, ref.Key, err)
		}

		return nil, fmt.Errorf("dispatch/artifact/cache: open %s/%s: %w", ref.Bucket, ref.Key, err)
	}

	defer func() {
		if cerr := rc.Close(); cerr != nil {
			c.logger.Warn("dispatch/artifact/cache: close source",
				log.String("key", ref.Key), log.String("error", cerr.Error()))
		}
	}()

	tmpPath := filepath.Join(c.dir, tmpDir, id.NewArtifactID().String())

	written, sum, err := c.copyAndHash(tmpPath, rc)
	if err != nil {
		c.removeQuietly(tmpPath)

		return nil, err
	}

	// Either the ref carried no size, or it lied. Correct the hold to
	// what actually landed on disk. Growing can block or evict, which is
	// why it is bounded by the caller's context like the first
	// reservation was; failing here costs the download, not the ledger.
	if rerr := c.resize(ctx, h, written); rerr != nil {
		c.removeQuietly(tmpPath)

		return nil, rerr
	}

	final, err := c.shardPath(sum)
	if err != nil {
		c.removeQuietly(tmpPath)

		return nil, err
	}

	e := &entry{
		hash: hashPrefix + sum,
		size: written,
		hold: h,
	}

	// The rename and the registration happen together, under the entry
	// table's lock, because eviction deletes a file under that same lock.
	// Anything less and this download can adopt a file eviction has
	// already condemned.
	live, err := c.entries.publish(e, coord, func() (string, error) {
		return final, c.promote(tmpPath, final)
	})
	if err != nil {
		c.removeQuietly(tmpPath)

		return nil, err
	}

	// A racing download of the same bytes may have registered first,
	// either a different artifact that happens to share them or a retry
	// of this one. It owns the file and the hold that covers it; ours
	// goes back with the deferred release. Content addressing makes that
	// a cache hit rather than a conflict.
	if live != e {
		return live, nil
	}

	committed = true

	return e, nil
}

// copyAndHash streams src into a new file at dst, returning the byte
// count and the hex digest.
func (c *Cache) copyAndHash(dst string, src io.Reader) (written int64, digest string, err error) {
	// #nosec G304 -- dst is a cache-internal temp path, and O_EXCL means this
	// creates a new file rather than opening an existing one.
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, "", fmt.Errorf("dispatch/artifact/cache: create temp file: %w", err)
	}

	hasher := blake3.New()

	written, copyErr := io.Copy(io.MultiWriter(f, hasher), src)

	closeErr := f.Close()

	if copyErr != nil {
		return 0, "", fmt.Errorf("dispatch/artifact/cache: download: %w", copyErr)
	}

	if closeErr != nil {
		return 0, "", fmt.Errorf("dispatch/artifact/cache: close temp file: %w", closeErr)
	}

	return written, hex.EncodeToString(hasher.Sum(nil)), nil
}

// shardPath returns a digest's home, creating its shard directory.
//
// It is separate from promote because only promote has to be ordered
// against eviction, and creating a directory eviction never removes
// does not belong inside that lock.
func (c *Cache) shardPath(sum string) (string, error) {
	dir := filepath.Join(c.dir, hashDir, sum[:2])
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return "", fmt.Errorf("dispatch/artifact/cache: create shard dir: %w", err)
	}

	return filepath.Join(dir, sum), nil
}

// promote moves a completed temp file into its content-addressed home.
//
// It runs under the entry table's lock, so a file it finds already at
// final belongs to an entry that is in the table right now and cannot
// be unlinked while this holds the lock. That is the whole reason the
// caller passes it in rather than calling it first: eviction unlinks
// under the same lock, so without that ordering a stat here can see a
// file whose eviction has already been decided.
func (c *Cache) promote(tmpPath, final string) error {
	// Another stager promoted identical bytes first. Its copy is as good
	// as ours, so drop ours rather than racing the rename.
	if _, err := os.Stat(final); err == nil {
		c.removeQuietly(tmpPath)

		return nil
	}

	if err := os.Rename(tmpPath, final); err != nil {
		return fmt.Errorf("dispatch/artifact/cache: promote: %w", err)
	}

	return nil
}

// evictOne removes the least recently used unleased entry, releasing
// the lease that held its bytes. It reports the bytes reclaimed and
// whether there was anything to evict at all — an empty artifact frees
// zero bytes and is still progress, so the two answers cannot be folded
// into one number without stalling reclamation on a zero-byte file.
//
// The table picks the victim, forgets it and unlinks its file under its
// own lock, and it only ever picks an entry no stager holds. Doing the
// unlink there rather than here is what stops a concurrent download
// adopting the victim's file in the gap: see entryTable.publish.
//
// Releasing the hold stays out here, because that takes the manager's
// lock and the table's lock is still held inside evictLRU.
func (c *Cache) evictOne() (int64, bool) {
	victim := c.entries.evictLRU(func(e *entry) {
		c.removeQuietly(e.path)
	})
	if victim == nil {
		return 0, false
	}

	freed := victim.hold.bytes
	c.releaseHold(victim.hold)

	c.logger.Debug("dispatch/artifact/cache: evicted entry",
		log.String("hash", victim.hash),
		log.Int64("bytes", victim.size),
	)

	return freed, true
}

// Reclaim frees up to need bytes by evicting least recently used
// entries, satisfying resource.Reclaimer.
//
// The bytes return to the manager as each victim's lease is released,
// which is the only path that credits the ledger; the count returned
// here is the manager's "something changed, re-check" signal and is
// deliberately not added to anything.
//
// It reclaims nothing for any key but disk. This cache holds bytes on
// one volume and nothing else, and a reclaimer that answered for memory
// it does not hold would have the manager admit work the box cannot
// run.
//
// This runs on the admission path — Manager.Acquire calls it before it
// blocks, under whatever deadline the caller set — so it does the
// unlinks the shortfall needs and stops, rather than tidying up while a
// fetcher waits.
func (c *Cache) Reclaim(ctx context.Context, key string, need int64) (int64, error) {
	if key != resource.Disk || need <= 0 {
		return 0, nil
	}

	var freed int64

	for freed < need && ctx.Err() == nil {
		bytes, ok := c.evictOne()
		if !ok {
			// Everything left is leased by a running stager.
			break
		}

		freed += bytes
	}

	return freed, nil
}

// Available reports the bytes eviction could free right now, satisfying
// resource.Reclaimer.
//
// It reads the entry table and nothing else. The guarantee this cache
// keeps is that the table lock and the manager's lock are never held at
// the same time — not that one is always taken first; rebuild takes
// them in the opposite sequence and is safe for exactly that reason.
// Reclaim lets go of the table before releasing a lease. Totalling the
// leases here instead of the entry sizes would break the rule on the
// one call the manager makes while a caller is mid-Acquire, and hold
// the table under the manager's lock while Reclaim waits for the table.
// That is the deadlock.
func (c *Cache) Available(key string) int64 {
	if key != resource.Disk {
		return 0
	}

	return c.entries.evictableBytes()
}

// removeQuietly deletes a path, logging rather than failing.
func (c *Cache) removeQuietly(path string) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		c.logger.Warn("dispatch/artifact/cache: remove file",
			log.String("path", path), log.String("error", err.Error()))
	}
}

// Purge removes every cached file and returns its bytes to the manager.
//
// The table is drained under its own lock rather than replaced, so an
// eviction running at the same time cannot pick an entry this loop has
// already released: each entry leaves the table once and its hold goes
// back once. It still assumes no live stagers — Purge deletes files
// out from under anything holding one, which was always its contract.
func (c *Cache) Purge() error {
	for _, e := range c.entries.drain() {
		c.removeQuietly(e.path)
		c.releaseHold(e.hold)
	}

	if err := os.RemoveAll(filepath.Join(c.dir, hashDir)); err != nil {
		return fmt.Errorf("dispatch/artifact/cache: purge: %w", err)
	}

	if err := os.MkdirAll(filepath.Join(c.dir, hashDir), dirPerm); err != nil {
		return fmt.Errorf("dispatch/artifact/cache: recreate hash dir: %w", err)
	}

	return nil
}

// Close releases the cache. Staged files survive so the next process can
// reuse them; only the temp directory is cleared.
func (c *Cache) Close() error {
	var err error

	c.closeOnce.Do(func() {
		err = c.resetTmp()
	})

	return err
}

// TrimHashPrefix returns a digest without its algorithm label.
func TrimHashPrefix(hash string) string {
	return strings.TrimPrefix(hash, hashPrefix)
}
