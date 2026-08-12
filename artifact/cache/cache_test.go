package cache_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
	"github.com/xraph/dispatch/artifact/cache"
)

func newCache(t *testing.T, budget int64) (*cache.Cache, *artifacttest.Backend) {
	t.Helper()

	b := artifacttest.NewBackend()

	c, err := cache.New(t.TempDir(), b, cache.WithBudget(budget))
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	t.Cleanup(func() {
		if cerr := c.Close(); cerr != nil {
			t.Errorf("cache close: %v", cerr)
		}
	})

	return c, b
}

func TestStageDownloadsAndCaches(t *testing.T) {
	ctx := context.Background()
	c, b := newCache(t, 1<<20)
	b.Put("models", "tower.ifc", []byte("hello world"))

	ref := artifact.Ref{Bucket: "models", Key: "tower.ifc", Size: 11}

	path, hash, release, err := c.Stage(ctx, ref)
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", path, err)
	}

	if string(data) != "hello world" {
		t.Fatalf("staged content = %q, want %q", data, "hello world")
	}

	if !strings.HasPrefix(hash, "blake3:") {
		t.Fatalf("hash = %q, want a blake3: prefix — Stage must hash during download", hash)
	}

	release()

	_, hash2, release2, err := c.Stage(ctx, ref)
	if err != nil {
		t.Fatalf("second Stage: %v", err)
	}

	release2()

	if b.Opens() != 1 {
		t.Fatalf("Opens() = %d, want 1 (second Stage must hit the cache)", b.Opens())
	}

	if hash2 != hash {
		t.Fatalf("hash changed between stages: %q then %q", hash, hash2)
	}
}

// TestStageSingleFlight is the reason the cache exists in front of the
// backend rather than beside it: eight jobs on the same model must pay
// for one download, not eight.
func TestStageSingleFlight(t *testing.T) {
	ctx := context.Background()
	c, b := newCache(t, 1<<20)
	b.Put("models", "big.ifc", []byte("payload"))
	b.DelayOpen = 50 * time.Millisecond

	ref := artifact.Ref{Bucket: "models", Key: "big.ifc", Size: 7}

	const n = 8

	var wg sync.WaitGroup

	errs := make([]error, n)
	releases := make([]func(), n)

	for i := range n {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			_, _, release, err := c.Stage(ctx, ref)
			errs[i] = err
			releases[i] = release
		}(i)
	}

	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}

		if releases[i] != nil {
			releases[i]()
		}
	}

	if b.Opens() != 1 {
		t.Fatalf("Opens() = %d, want 1 — %d concurrent stages must share one download", b.Opens(), n)
	}
}

func TestStageMissingObjectIsPermanent(t *testing.T) {
	c, _ := newCache(t, 1<<20)

	_, _, _, err := c.Stage(context.Background(),
		artifact.Ref{Bucket: "models", Key: "absent"})
	if !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("Stage(missing) = %v, want ErrNotFound (permanent, so the job fails fast)", err)
	}
}

// TestStageDeniedObjectIsPermanent covers the permanent failure that is not a
// missing object. The object is present; the backend just will not hand it
// over, and retrying cannot change that.
//
// This pins propagation, not the retry decision: the executor does not yet
// consult the classification (see worker/executor.go handleFailure), so
// nothing fails fast at runtime today. What this guarantees is that the
// classification survives the cache layer intact and is there to act on.
func TestStageDeniedObjectIsPermanent(t *testing.T) {
	c, b := newCache(t, 1<<20)
	b.Put("models", "secret.ifc", []byte("classified"))
	b.DenyOpen = true

	_, _, _, err := c.Stage(context.Background(),
		artifact.Ref{Bucket: "models", Key: "secret.ifc"})

	if !errors.Is(err, dispatch.ErrPermanent) {
		t.Fatalf("Stage(denied) = %v, want ErrPermanent (so the job fails fast)", err)
	}

	if !errors.Is(err, artifact.ErrPermissionDenied) {
		t.Fatalf("Stage(denied) = %v, want ErrPermissionDenied", err)
	}

	// Reporting a forbidden object as missing would send an operator
	// looking for a deleted file that is sitting right there.
	if errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("Stage(denied) reported as not found: %v", err)
	}
}

func TestStageUnknownSizeStillWorks(t *testing.T) {
	ctx := context.Background()
	c, b := newCache(t, 1<<20)
	b.Put("models", "nosize.ifc", []byte("0123456789"))

	// A ref registered but never staged carries no size yet.
	_, _, release, err := c.Stage(ctx, artifact.Ref{Bucket: "models", Key: "nosize.ifc"})
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}

	defer release()

	if used := c.Used(); used != 10 {
		t.Fatalf("Used() = %d, want 10 — accounting must correct itself once the size is known", used)
	}
}

func TestLeaseBlocksEviction(t *testing.T) {
	ctx := context.Background()
	c, b := newCache(t, 20) // room for two 10-byte objects
	b.Put("m", "a", []byte("0123456789"))
	b.Put("m", "b", []byte("0123456789"))
	b.Put("m", "c", []byte("0123456789"))

	pathA, _, releaseA, err := c.Stage(ctx, artifact.Ref{Bucket: "m", Key: "a", Size: 10})
	if err != nil {
		t.Fatalf("Stage a: %v", err)
	}

	_, _, releaseB, err := c.Stage(ctx, artifact.Ref{Bucket: "m", Key: "b", Size: 10})
	if err != nil {
		t.Fatalf("Stage b: %v", err)
	}

	releaseB() // b is now evictable; a is still leased

	_, _, releaseC, err := c.Stage(ctx, artifact.Ref{Bucket: "m", Key: "c", Size: 10})
	if err != nil {
		t.Fatalf("Stage c should have evicted b, got: %v", err)
	}

	// The leased entry must still be on disk: a running handler holds it.
	if _, serr := os.Stat(pathA); serr != nil {
		t.Fatalf("leased entry was evicted: %v", serr)
	}

	releaseC()
	releaseA()
}

func TestBudgetExceededRespectsDeadline(t *testing.T) {
	ctx := context.Background()
	c, b := newCache(t, 10)
	b.Put("m", "a", []byte("0123456789"))
	b.Put("m", "b", []byte("0123456789"))

	_, _, releaseA, err := c.Stage(ctx, artifact.Ref{Bucket: "m", Key: "a", Size: 10})
	if err != nil {
		t.Fatalf("Stage a: %v", err)
	}

	defer releaseA()

	// a is leased and fills the budget, so b cannot be admitted.
	deadlined, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	start := time.Now()

	_, _, _, err = c.Stage(deadlined, artifact.Ref{Bucket: "m", Key: "b", Size: 10})
	if !errors.Is(err, cache.ErrBudgetExceeded) {
		t.Fatalf("Stage under an exhausted budget = %v, want ErrBudgetExceeded", err)
	}

	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Stage waited %v — it must be bounded by the context deadline", elapsed)
	}
}

// TestOversizeRefRejectedImmediately guards against a job that can never
// be staged blocking until its deadline instead of failing at once.
func TestOversizeRefRejectedImmediately(t *testing.T) {
	c, b := newCache(t, 10)
	b.Put("m", "huge", make([]byte, 100))

	start := time.Now()

	_, _, _, err := c.Stage(context.Background(),
		artifact.Ref{Bucket: "m", Key: "huge", Size: 100})
	if !errors.Is(err, cache.ErrBudgetExceeded) {
		t.Fatalf("Stage of a ref larger than the whole budget = %v, want ErrBudgetExceeded", err)
	}

	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("took %v — an impossible request must fail immediately, not wait", elapsed)
	}
}

func TestReleaseIsIdempotent(t *testing.T) {
	ctx := context.Background()
	c, b := newCache(t, 1<<20)
	b.Put("m", "a", []byte("data"))

	_, _, release, err := c.Stage(ctx, artifact.Ref{Bucket: "m", Key: "a", Size: 4})
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}

	release()
	release()
	release()

	// A double release must not drop the lease count below zero and let a
	// still-in-use entry be evicted.
	_, _, release2, err := c.Stage(ctx, artifact.Ref{Bucket: "m", Key: "a", Size: 4})
	if err != nil {
		t.Fatalf("Stage after repeated release: %v", err)
	}

	release2()
}

func TestRecoveryWipesTmpAndReusesFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b := artifacttest.NewBackend()
	b.Put("m", "a", []byte("0123456789"))

	ref := artifact.Ref{Bucket: "m", Key: "a", Size: 10}

	c1, err := cache.New(dir, b, cache.WithBudget(1<<20))
	if err != nil {
		t.Fatalf("first New: %v", err)
	}

	_, hash, release, err := c1.Stage(ctx, ref)
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}

	release()

	if cerr := c1.Close(); cerr != nil {
		t.Fatalf("close: %v", cerr)
	}

	// Simulate a crash mid-download.
	leftover := filepath.Join(dir, "tmp", "leftover")
	if werr := os.WriteFile(leftover, []byte("junk"), 0o600); werr != nil {
		t.Fatalf("write leftover: %v", werr)
	}

	c2, err := cache.New(dir, b, cache.WithBudget(1<<20))
	if err != nil {
		t.Fatalf("second New: %v", err)
	}

	t.Cleanup(func() {
		if cerr := c2.Close(); cerr != nil {
			t.Errorf("close c2: %v", cerr)
		}
	})

	if _, serr := os.Stat(leftover); !os.IsNotExist(serr) {
		t.Fatal("startup must wipe tmp/ — a partial download is never reusable")
	}

	// A ref carrying the hash resolves against the rebuilt table without
	// touching the backend.
	hashed := ref
	hashed.ContentHash = hash

	_, _, release2, err := c2.Stage(ctx, hashed)
	if err != nil {
		t.Fatalf("Stage after recovery: %v", err)
	}

	release2()

	if b.Opens() != 1 {
		t.Fatalf("Opens() = %d, want 1 — the index must be rebuilt from disk, not re-downloaded", b.Opens())
	}
}

func TestDistinctArtifactsWithIdenticalBytesShareOneCopy(t *testing.T) {
	ctx := context.Background()
	c, b := newCache(t, 1<<20)
	b.Put("m", "first.ifc", []byte("identical"))
	b.Put("m", "second.ifc", []byte("identical"))

	pathA, hashA, releaseA, err := c.Stage(ctx, artifact.Ref{Bucket: "m", Key: "first.ifc", Size: 9})
	if err != nil {
		t.Fatalf("Stage first: %v", err)
	}

	defer releaseA()

	pathB, hashB, releaseB, err := c.Stage(ctx, artifact.Ref{Bucket: "m", Key: "second.ifc", Size: 9})
	if err != nil {
		t.Fatalf("Stage second: %v", err)
	}

	defer releaseB()

	if hashA != hashB {
		t.Fatalf("identical bytes hashed differently: %q vs %q", hashA, hashB)
	}

	if pathA != pathB {
		t.Fatalf("content addressing failed: %q vs %q", pathA, pathB)
	}

	if used := c.Used(); used != 9 {
		t.Fatalf("Used() = %d, want 9 — identical bytes must be stored once", used)
	}
}

func TestBudgetReportsConfiguredLimit(t *testing.T) {
	c, _ := newCache(t, 4096)
	if got := c.Budget(); got != 4096 {
		t.Fatalf("Budget() = %d, want 4096", got)
	}
}

func TestNewRejectsNilBackend(t *testing.T) {
	_, err := cache.New(t.TempDir(), nil)
	if !errors.Is(err, artifact.ErrNoBackend) {
		t.Fatalf("New(nil backend) = %v, want ErrNoBackend", err)
	}
}
