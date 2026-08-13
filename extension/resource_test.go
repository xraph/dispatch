package extension_test

import (
	"context"
	"testing"

	forgetesting "github.com/xraph/forge/testing"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
	"github.com/xraph/dispatch/extension"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/store/memory"
)

const mib = int64(1) << 20

// registerWithResources builds an extension with the artifact plane and
// the resource model both on, and registers it against a test app.
func registerWithResources(t *testing.T, opts ...extension.ExtOption) *extension.Extension {
	t.Helper()

	base := []extension.ExtOption{
		extension.WithStore(memory.New()),
		extension.WithArtifactBackend(artifacttest.NewBackend()),
		extension.WithArtifactStore(memory.New()),
		extension.WithArtifactCacheDir(t.TempDir()),
		extension.WithArtifactCacheBudget(64 * mib),
		extension.WithResources(),
		extension.WithDisableRoutes(),
	}

	ext := extension.New(append(base, opts...)...)

	if err := ext.Register(forgetesting.NewTestApp("test-app", "0.1.0")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	return ext
}

// TestCacheAndEngineShareOneManager is the end-to-end form of the
// coupling worker.TestCacheAndPoolShareOneLedger pins in isolation:
// through the real extension wiring, the staging cache and the worker
// pool must end up holding the same resource.Manager.
//
// It asserts behaviourally rather than by pointer, because the symptom of
// getting this wrong is behavioural. Bytes staged through the cache have
// to show up as reclaimable in the ledger the engine handed the pool. If
// the cache ever falls back to the private manager it builds when none is
// supplied, that number stays zero and the worker silently stops claiming
// disk-hungry work.
func TestCacheAndEngineShareOneManager(t *testing.T) {
	ext := registerWithResources(t)

	mgr := ext.Resources()
	if mgr == nil {
		t.Fatal("resource manager is nil after Register with WithResources")
	}

	if got := ext.Engine().Resources(); got != mgr {
		t.Fatal("the engine was given a different manager than the extension built")
	}

	c := ext.ArtifactCache()
	if c == nil {
		t.Fatal("staging cache is nil")
	}

	// The cache's own view of its budget is the shared ledger's disk
	// capacity, which is where the configured cache budget was routed.
	if got, want := c.Budget(), 64*mib; got != want {
		t.Fatalf("cache budget = %d, want %d — the configured budget did not "+
			"reach the shared ledger's disk capacity", got, want)
	}

	if got, want := mgr.Capacity()[resource.Disk], 64*mib; got != want {
		t.Fatalf("ledger disk capacity = %d, want %d", got, want)
	}

	// Stage real bytes and read the ledger the pool reads.
	backend, ok := ext.Artifacts().Backend().(*artifacttest.Backend)
	if !ok {
		t.Fatalf("backend is %T, want *artifacttest.Backend", ext.Artifacts().Backend())
	}

	const staged = 4 * mib

	backend.Put("stage", "model.bin", make([]byte, staged))

	_, _, release, err := c.Stage(context.Background(), artifact.Ref{
		Backend: backend.Name(),
		Bucket:  "stage",
		Key:     "model.bin",
		Size:    staged,
	})
	if err != nil {
		t.Fatalf("stage: %v", err)
	}

	if got, want := mgr.Free()[resource.Disk], 64*mib-staged; got != want {
		t.Fatalf("while pinned: ledger free disk = %d, want %d — "+
			"the cache took its lease against a different manager", got, want)
	}

	release()

	if got := mgr.Reclaimable()[resource.Disk]; got != staged {
		t.Fatalf("after release: ledger reclaimable disk = %d, want %d — "+
			"the cache is not registered as this ledger's disk reclaimer", got, staged)
	}
}

// TestExplicitCapacityDeclaresCustomResources checks the only path a
// custom resource can arrive by. Nothing detects an FPGA.
func TestExplicitCapacityDeclaresCustomResources(t *testing.T) {
	ext := registerWithResources(t,
		extension.WithExplicitCapacity(resource.Set{"fpga": 2}, resource.GPUs(4)),
	)

	capacity := ext.Resources().Capacity()

	if got := capacity["fpga"]; got != 2 {
		t.Errorf("fpga capacity = %d, want 2", got)
	}

	if got := capacity[resource.GPU]; got != 4*resource.MilliScale {
		t.Errorf("gpu capacity = %d, want %d", got, 4*resource.MilliScale)
	}

	// Explicit overrides detection, so an operator pinning memory gets
	// exactly that rather than a fraction of it.
	ext2 := registerWithResources(t,
		extension.WithExplicitCapacity(resource.MemoryBytes(7*mib)),
	)

	if got := ext2.Resources().Capacity()[resource.Memory]; got != 7*mib {
		t.Errorf("explicit memory capacity = %d, want %d", got, 7*mib)
	}
}

// TestResourcesDisabledIsTodaysBehaviour pins the backward-compatibility
// guarantee. Absent config means no ledger anywhere: the pool dequeues
// unbounded, every backend skips its fit predicate, and the staging cache
// keeps the private disk budget it has always had.
func TestResourcesDisabledIsTodaysBehaviour(t *testing.T) {
	ext := extension.New(
		extension.WithStore(memory.New()),
		extension.WithArtifactBackend(artifacttest.NewBackend()),
		extension.WithArtifactStore(memory.New()),
		extension.WithArtifactCacheDir(t.TempDir()),
		extension.WithArtifactCacheBudget(32*mib),
		extension.WithDisableRoutes(),
	)

	if err := ext.Register(forgetesting.NewTestApp("test-app", "0.1.0")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if ext.Resources() != nil {
		t.Fatal("a manager was built without WithResources")
	}

	if ext.Engine().Resources() != nil {
		t.Fatal("the engine has a manager without WithResources")
	}

	// The cache still honours WithBudget, because with no shared ledger
	// its private manager is the only ceiling there is.
	if got, want := ext.ArtifactCache().Budget(), 32*mib; got != want {
		t.Fatalf("cache budget = %d, want %d", got, want)
	}
}

// TestCacheBudgetDefaultsWhenUnset covers the gap that would otherwise
// hand the ledger a zero disk capacity: artifacts on, budget unstated.
// The cache's own default has to be what the ledger advertises, or every
// disk-declaring job becomes unschedulable on a worker that can in fact
// stage 20 GiB.
func TestCacheBudgetDefaultsWhenUnset(t *testing.T) {
	ext := extension.New(
		extension.WithStore(memory.New()),
		extension.WithArtifactBackend(artifacttest.NewBackend()),
		extension.WithArtifactStore(memory.New()),
		extension.WithArtifactCacheDir(t.TempDir()),
		extension.WithResources(),
		extension.WithDisableRoutes(),
	)

	if err := ext.Register(forgetesting.NewTestApp("test-app", "0.1.0")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if got := ext.Resources().Capacity()[resource.Disk]; got <= 0 {
		t.Fatalf("ledger disk capacity = %d, want the cache default", got)
	}

	if got, want := ext.ArtifactCache().Budget(),
		ext.Resources().Capacity()[resource.Disk]; got != want {
		t.Fatalf("cache budget = %d, ledger disk = %d — these must be one number", got, want)
	}
}

// TestNoArtifactPlaneOmitsDisk checks that a worker with no staging cache
// advertises no disk at all, rather than capacity nothing can reclaim.
func TestNoArtifactPlaneOmitsDisk(t *testing.T) {
	ext := extension.New(
		extension.WithStore(memory.New()),
		extension.WithResources(),
		extension.WithDisableRoutes(),
	)

	if err := ext.Register(forgetesting.NewTestApp("test-app", "0.1.0")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	capacity := ext.Resources().Capacity()

	if _, present := capacity[resource.Disk]; present {
		t.Fatalf("disk advertised with no staging cache: %v", capacity)
	}

	// Detection still produced the two keys that always exist.
	if capacity[resource.CPU] <= 0 || capacity[resource.Memory] <= 0 {
		t.Fatalf("detection produced no cpu/memory: %v", capacity)
	}
}

// TestPublishedCapacityMatchesTheLedger pins the other half of the
// wiring: what this worker tells the cluster it can run has to be what
// admission actually enforces, or the enqueue-time unschedulable check
// rejects jobs the worker could run — or worse, admits ones it cannot.
func TestPublishedCapacityMatchesTheLedger(t *testing.T) {
	ext := registerWithResources(t,
		extension.WithExplicitCapacity(resource.Set{"fpga": 3}),
	)

	published := ext.Engine().MaxWorkerCapacity(context.Background())
	ledger := ext.Resources().Capacity()

	for _, k := range ledger.Keys() {
		if published[k] != ledger[k] {
			t.Errorf("published capacity %s = %d, ledger = %d",
				k, published[k], ledger[k])
		}
	}
}
