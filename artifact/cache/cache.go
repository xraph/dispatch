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
	"time"

	"github.com/zeebo/blake3"
	"golang.org/x/sync/singleflight"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
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

// Cache stages artifacts to local disk, content-addressed and bounded by
// a byte budget. It is safe for concurrent use.
type Cache struct {
	dir     string
	backend artifact.Backend
	logger  log.Logger

	entries *entryTable
	budget  *budget
	flight  singleflight.Group

	closeOnce sync.Once
}

// Option configures a Cache.
type Option func(*Cache)

// WithBudget sets the maximum bytes the cache may hold on disk.
func WithBudget(bytes int64) Option {
	return func(c *Cache) {
		if bytes > 0 {
			c.budget = newBudget(bytes)
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
		dir:     dir,
		backend: backend,
		logger:  log.NewNoopLogger(),
		entries: newEntryTable(),
		budget:  newBudget(DefaultBudget),
	}

	for _, opt := range opts {
		opt(c)
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

	c.budget.setEvictor(c.evictOne)

	return c, nil
}

// Budget returns the configured disk allowance. The engine uses it to
// reject a job definition whose declared inputs could never be staged.
func (c *Cache) Budget() int64 { return c.budget.Limit() }

// Used returns the bytes currently held on disk.
func (c *Cache) Used() int64 { return c.budget.Used() }

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
func (c *Cache) rebuild() error {
	root := filepath.Join(c.dir, hashDir)
	now := time.Now()

	var total int64

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

		e := &entry{
			hash:     hashPrefix + d.Name(),
			path:     path,
			size:     info.Size(),
			lastUsed: now,
		}

		c.entries.put(e, "")
		total += info.Size()

		return nil
	})
	if err != nil {
		return fmt.Errorf("dispatch/artifact/cache: rebuild index: %w", err)
	}

	if total > 0 {
		// Account for what is already on disk without blocking: this is
		// recovery, not a new reservation.
		c.budget.Adjust(0, total)
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

	// Fast path: a ref that already knows its hash, or coordinates we
	// have staged before.
	if e, ok := c.lookup(ref, coord); ok {
		c.entries.lease(e, time.Now())

		return e.path, e.hash, c.releaseFunc(e), nil
	}

	// Slow path: one download per artifact, however many stagers arrive.
	res, err, _ := c.flight.Do(coord, func() (any, error) {
		return c.download(ctx, ref, coord)
	})
	if err != nil {
		return "", "", nil, err
	}

	e, ok := res.(*entry)
	if !ok {
		return "", "", nil, fmt.Errorf("dispatch/artifact/cache: unexpected flight result %T", res)
	}

	c.entries.lease(e, time.Now())

	return e.path, e.hash, c.releaseFunc(e), nil
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
func (c *Cache) releaseFunc(e *entry) func() {
	var once sync.Once

	return func() {
		once.Do(func() {
			c.entries.release(e)
			c.budget.Wake()
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

	if err := c.budget.Acquire(ctx, reserved); err != nil {
		return nil, err
	}

	committed := false

	defer func() {
		if !committed {
			c.budget.Release(reserved)
		}
	}()

	rc, err := c.backend.Open(ctx, ref)
	if err != nil {
		if errors.Is(err, artifact.ErrNotFound) {
			// Preserve the sentinel: staging a deleted input is permanent,
			// and the executor must fail fast rather than retry.
			return nil, fmt.Errorf("stage %s/%s: %w", ref.Bucket, ref.Key, artifact.ErrNotFound)
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

	if written != reserved {
		// Either the ref carried no size, or it lied. Correct the budget
		// to what actually landed on disk.
		c.budget.Adjust(reserved, written)
	}

	hash := hashPrefix + sum

	final, err := c.promote(tmpPath, sum)
	if err != nil {
		c.removeQuietly(tmpPath)

		return nil, err
	}

	// A different artifact may share these bytes and have staged them
	// first. Content addressing makes that a cache hit, not a conflict:
	// drop our copy's accounting and reuse the existing entry.
	if existing, ok := c.entries.getByHash(hash); ok && existing.path == final {
		c.budget.Adjust(written, 0)
		c.entries.alias(coord, hash)

		committed = true

		return existing, nil
	}

	e := &entry{
		hash:     hash,
		path:     final,
		size:     written,
		lastUsed: time.Now(),
	}

	c.entries.put(e, coord)

	committed = true

	return e, nil
}

// copyAndHash streams src into a new file at dst, returning the byte
// count and the hex digest.
func (c *Cache) copyAndHash(dst string, src io.Reader) (written int64, digest string, err error) {
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

// promote moves a completed temp file into its content-addressed home.
func (c *Cache) promote(tmpPath, sum string) (string, error) {
	dir := filepath.Join(c.dir, hashDir, sum[:2])
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return "", fmt.Errorf("dispatch/artifact/cache: create shard dir: %w", err)
	}

	final := filepath.Join(dir, sum)

	// Another stager may have promoted identical bytes first. Its copy is
	// as good as ours, so drop ours rather than racing the rename.
	if _, err := os.Stat(final); err == nil {
		c.removeQuietly(tmpPath)

		return final, nil
	}

	if err := os.Rename(tmpPath, final); err != nil {
		return "", fmt.Errorf("dispatch/artifact/cache: promote: %w", err)
	}

	return final, nil
}

// evictOne removes the least recently used unleased entry. It returns the
// bytes reclaimed, or zero when every entry is leased.
func (c *Cache) evictOne() int64 {
	victim := c.entries.evictLRU()
	if victim == nil {
		return 0
	}

	c.removeQuietly(victim.path)

	// Only the file and the table entry are dropped here. The budget
	// subtracts the returned size itself, because it already holds its
	// own mutex when it calls this.
	c.logger.Debug("dispatch/artifact/cache: evicted entry",
		log.String("hash", victim.hash),
		log.Int64("bytes", victim.size),
	)

	return victim.size
}

// removeQuietly deletes a path, logging rather than failing.
func (c *Cache) removeQuietly(path string) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		c.logger.Warn("dispatch/artifact/cache: remove file",
			log.String("path", path), log.String("error", err.Error()))
	}
}

// Purge removes every cached file and resets the accounting.
func (c *Cache) Purge() error {
	for _, e := range c.entries.all() {
		c.removeQuietly(e.path)
	}

	c.entries = newEntryTable()

	if err := os.RemoveAll(filepath.Join(c.dir, hashDir)); err != nil {
		return fmt.Errorf("dispatch/artifact/cache: purge: %w", err)
	}

	if err := os.MkdirAll(filepath.Join(c.dir, hashDir), dirPerm); err != nil {
		return fmt.Errorf("dispatch/artifact/cache: recreate hash dir: %w", err)
	}

	c.budget.Reset()

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
