package engine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/resource/resourcetest"
	"github.com/xraph/dispatch/store/memory"
)

type resPayload struct {
	N int `json:"n"`
}

// newResourceEngine builds a plain engine over the memory store.
//
// Tests that need artifact bindings use newArtifactRig from
// engine/artifact_test.go instead: applyBindings requires a configured
// artifact backend, and both files share package engine_test.
func newResourceEngine(t *testing.T, opts ...engine.Option) *engine.Engine {
	t.Helper()

	d, err := dispatch.New(
		dispatch.WithStore(memory.New()),
		dispatch.WithConcurrency(2),
		dispatch.WithQueues([]string{"default"}),
	)
	if err != nil {
		t.Fatalf("dispatch.New() error = %v", err)
	}

	eng, err := engine.Build(d, opts...)
	if err != nil {
		t.Fatalf("engine.Build() error = %v", err)
	}

	return eng
}

func TestEnqueueResolvesStaticDeclaration(t *testing.T) {
	eng := newResourceEngine(t)

	def := job.NewDefinition("res.static",
		func(context.Context, resPayload) error { return nil },
		job.WithResources(resource.CPUs(4), resource.MemoryGB(16)),
	)
	engine.Register(eng, def)

	j, err := engine.Enqueue(context.Background(), eng, def.Name, resPayload{N: 1})
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	if j.Resources[resource.CPU] != 4000 {
		t.Errorf("cpu = %d, want 4000", j.Resources[resource.CPU])
	}

	if j.Resources[resource.Memory] != 16<<30 {
		t.Errorf("memory = %d, want 16 GiB", j.Resources[resource.Memory])
	}

	if j.ResourceLimits[resource.Memory] != 16<<30 {
		t.Errorf("memory limit should default to the request, got %v", j.ResourceLimits)
	}

	if _, ok := j.ResourceLimits[resource.CPU]; ok {
		t.Errorf("cpu limit should be unset (burstable), got %v", j.ResourceLimits)
	}
}

func TestEnqueueWithoutDeclarationLeavesResourcesZero(t *testing.T) {
	eng := newResourceEngine(t)

	def := job.NewDefinition("res.none",
		func(context.Context, resPayload) error { return nil })
	engine.Register(eng, def)

	j, err := engine.Enqueue(context.Background(), eng, def.Name, resPayload{N: 1})
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	if !j.Resources.IsZero() {
		t.Errorf("Resources = %v; an undeclared job must cost nothing", j.Resources)
	}
}

// TestEnqueueResourceFuncSeesInputBytes is the case the whole track
// exists for: one definition serving a 40 MB model and a 4 GB one.
func TestEnqueueResourceFuncSeesInputBytes(t *testing.T) {
	rig := newArtifactRig(t, 1<<30)

	def := job.NewDefinition("res.dynamic",
		func(context.Context, resPayload) error { return nil },
		job.WithArtifactInputs(
			artifact.Input("small", artifact.Required),
			artifact.Input("large", artifact.Required),
		),
		job.WithResourceFunc(func(_ context.Context, r resource.Request) (resource.Set, error) {
			return resource.MemoryBytes(r.InputBytes * 3), nil
		}),
	)

	if err := engine.RegisterChecked(rig.engine, def); err != nil {
		t.Fatalf("RegisterChecked() error = %v", err)
	}

	ctx := context.Background()

	rig.backend.Put("models", "small.bin", make([]byte, 100))
	rig.backend.Put("models", "large.bin", make([]byte, 200))

	small, err := rig.svc.Register(ctx, "models", "small.bin")
	if err != nil {
		t.Fatalf("Register(small) error = %v", err)
	}

	large, err := rig.svc.Register(ctx, "models", "large.bin")
	if err != nil {
		t.Fatalf("Register(large) error = %v", err)
	}

	got, err := engine.Enqueue(ctx, rig.engine, def.Name,
		resPayload{N: 1},
		engine.Bind("small", small),
		engine.Bind("large", large),
	)
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	if got.InputBytes != 300 {
		t.Errorf("InputBytes = %d, want 300", got.InputBytes)
	}

	if got.Resources[resource.Memory] != 900 {
		t.Errorf("memory = %d, want 900 (3x input)", got.Resources[resource.Memory])
	}

	// PrimaryInputHash may be empty: the artifact plane fills content_hash
	// at first staging, not at registration. Assert on the ref's own hash
	// rather than a literal so this holds either way.
	if got.PrimaryInputHash != large.ContentHash {
		t.Errorf("PrimaryInputHash = %q, want the larger input's hash %q",
			got.PrimaryInputHash, large.ContentHash)
	}
}

func TestEnqueueOverrideBeatsDeclaration(t *testing.T) {
	eng := newResourceEngine(t)

	def := job.NewDefinition("res.override",
		func(context.Context, resPayload) error { return nil },
		job.WithResources(resource.MemoryGB(16)),
	)
	engine.Register(eng, def)

	j, err := engine.Enqueue(context.Background(), eng, def.Name, resPayload{N: 1},
		job.WithResources(resource.MemoryGB(48)),
	)
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	if j.Resources[resource.Memory] != 48<<30 {
		t.Errorf("memory = %d, want the 48 GiB override", j.Resources[resource.Memory])
	}
}

// TestEnqueueResolvesFromEverySource covers one resolution source per
// case, each in isolation.
//
// Isolation is the point. resolveResources skips resolution entirely
// when resourcesInPlay reports nothing constrains the job, so a source
// missing from that predicate would silently enqueue every job of its
// kind with zero requirements. Each case here configures exactly one
// source, so deleting that source's clause from the predicate fails
// this test and nothing else masks it.
func TestEnqueueResolvesFromEverySource(t *testing.T) {
	tests := []struct {
		name       string
		engineOpts []engine.Option
		defOpts    []job.Option
		enqOpts    []job.Option
		wantMem    int64
		wantLimit  int64
		wantClass  string
	}{
		{
			name:    "declaration on the definition",
			defOpts: []job.Option{job.WithResources(resource.MemoryGB(2))},
			wantMem: 2 << 30, wantLimit: 2 << 30,
		},
		{
			name: "configured estimator",
			engineOpts: []engine.Option{
				engine.WithEstimator(&resourcetest.FakeEstimator{Out: resource.MemoryGB(3)}),
			},
			wantMem: 3 << 30, wantLimit: 3 << 30,
		},
		{
			name: "fleet-wide default",
			engineOpts: []engine.Option{
				engine.WithResourceDefaults(resource.MemoryGB(4), nil),
			},
			wantMem: 4 << 30, wantLimit: 4 << 30,
		},
		{
			name: "per-queue default",
			engineOpts: []engine.Option{
				engine.WithResourceDefaults(nil, map[string]resource.Set{
					"default": resource.MemoryGB(5),
				}),
			},
			wantMem: 5 << 30, wantLimit: 5 << 30,
		},
		{
			name:    "override at enqueue",
			enqOpts: []job.Option{job.WithResources(resource.MemoryGB(6))},
			wantMem: 6 << 30, wantLimit: 6 << 30,
		},
		{
			name:      "limits at enqueue with no request",
			enqOpts:   []job.Option{job.WithResourceLimits(resource.MemoryGB(7))},
			wantLimit: 7 << 30,
		},
		{
			name: "resource func at enqueue",
			enqOpts: []job.Option{
				job.WithResourceFunc(func(context.Context, resource.Request) (resource.Set, error) {
					return resource.MemoryGB(8), nil
				}),
			},
			wantMem: 8 << 30, wantLimit: 8 << 30,
		},
		{
			name:      "class at enqueue",
			enqOpts:   []job.Option{job.WithResourceClass("gpu-a100")},
			wantClass: "gpu-a100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eng := newResourceEngine(t, tt.engineOpts...)

			def := job.NewDefinition("res.source",
				func(context.Context, resPayload) error { return nil },
				tt.defOpts...)
			engine.Register(eng, def)

			j, err := engine.Enqueue(context.Background(), eng, def.Name,
				resPayload{N: 1}, tt.enqOpts...)
			if err != nil {
				t.Fatalf("Enqueue() error = %v", err)
			}

			if got := j.Resources[resource.Memory]; got != tt.wantMem {
				t.Errorf("Resources memory = %d, want %d", got, tt.wantMem)
			}

			if got := j.ResourceLimits[resource.Memory]; got != tt.wantLimit {
				t.Errorf("ResourceLimits memory = %d, want %d", got, tt.wantLimit)
			}

			if j.ResourceClass != tt.wantClass {
				t.Errorf("ResourceClass = %q, want %q", j.ResourceClass, tt.wantClass)
			}
		})
	}
}

// TestEnqueueLimitPrecedence pins that an enqueue-time limit beats a
// declared one, which is what OverrideLimits exists for.
func TestEnqueueLimitPrecedence(t *testing.T) {
	eng := newResourceEngine(t)

	def := job.NewDefinition("res.limits",
		func(context.Context, resPayload) error { return nil },
		job.WithResources(resource.MemoryGB(16)),
		job.WithResourceLimits(resource.MemoryGB(20)),
	)
	engine.Register(eng, def)

	ctx := context.Background()

	// The declared limit stands when the enqueue supplies none.
	j, err := engine.Enqueue(ctx, eng, def.Name, resPayload{N: 1})
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	if j.ResourceLimits[resource.Memory] != 20<<30 {
		t.Errorf("declared limit = %d, want 20 GiB", j.ResourceLimits[resource.Memory])
	}

	// An enqueue-time limit replaces it.
	j, err = engine.Enqueue(ctx, eng, def.Name, resPayload{N: 1},
		job.WithResourceLimits(resource.MemoryGB(32)))
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	if j.ResourceLimits[resource.Memory] != 32<<30 {
		t.Errorf("override limit = %d, want the 32 GiB override", j.ResourceLimits[resource.Memory])
	}

	if j.Resources[resource.Memory] != 16<<30 {
		t.Errorf("requests = %d; a limit override must not move the request", j.Resources[resource.Memory])
	}
}

// TestEnqueueFuncAndClassPrecedence pins that the single-valued sources
// are replaced outright by an enqueue-time value rather than merged.
func TestEnqueueFuncAndClassPrecedence(t *testing.T) {
	eng := newResourceEngine(t)

	def := job.NewDefinition("res.precedence",
		func(context.Context, resPayload) error { return nil },
		job.WithResourceClass("cpu-standard"),
		job.WithResourceFunc(func(context.Context, resource.Request) (resource.Set, error) {
			return resource.MemoryGB(2), nil
		}),
	)
	engine.Register(eng, def)

	ctx := context.Background()

	// Declared values apply when the enqueue overrides neither.
	j, err := engine.Enqueue(ctx, eng, def.Name, resPayload{N: 1})
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	if j.Resources[resource.Memory] != 2<<30 || j.ResourceClass != "cpu-standard" {
		t.Errorf("declared func/class not applied: memory = %d, class = %q",
			j.Resources[resource.Memory], j.ResourceClass)
	}

	// Enqueue-time values replace them.
	j, err = engine.Enqueue(ctx, eng, def.Name, resPayload{N: 1},
		job.WithResourceClass("gpu-h100"),
		job.WithResourceFunc(func(context.Context, resource.Request) (resource.Set, error) {
			return resource.MemoryGB(9), nil
		}),
	)
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	if j.Resources[resource.Memory] != 9<<30 {
		t.Errorf("memory = %d, want the enqueue func's 9 GiB", j.Resources[resource.Memory])
	}

	if j.ResourceClass != "gpu-h100" {
		t.Errorf("class = %q, want the enqueue override", j.ResourceClass)
	}
}

// TestEnqueueEstimatorSeesDeclarationAndLosesToOverride places the
// estimator in the precedence chain: above a declaration, below an
// explicit override, and given the declaration so it can defer to it.
func TestEnqueueEstimatorSeesDeclarationAndLosesToOverride(t *testing.T) {
	est := &resourcetest.FakeEstimator{Out: resource.MemoryGB(32)}
	eng := newResourceEngine(t, engine.WithEstimator(est))

	def := job.NewDefinition("res.estimated",
		func(context.Context, resPayload) error { return nil },
		job.WithResources(resource.CPUs(2), resource.MemoryGB(8)),
	)
	engine.Register(eng, def)

	ctx := context.Background()

	j, err := engine.Enqueue(ctx, eng, def.Name, resPayload{N: 1})
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	if j.Resources[resource.Memory] != 32<<30 {
		t.Errorf("memory = %d, want the estimator's 32 GiB", j.Resources[resource.Memory])
	}

	// Per-key overlay: the estimator predicted only memory, so the
	// declared CPU must survive.
	if j.Resources[resource.CPU] != 2000 {
		t.Errorf("cpu = %d, want the declared 2000 to survive a memory-only estimate", j.Resources[resource.CPU])
	}

	if est.Last.Declared[resource.Memory] != 8<<30 {
		t.Errorf("estimator saw Declared = %v, want the 8 GiB declaration", est.Last.Declared)
	}

	// An explicit override outranks the estimator.
	j, err = engine.Enqueue(ctx, eng, def.Name, resPayload{N: 1},
		job.WithResources(resource.MemoryGB(64)))
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	if j.Resources[resource.Memory] != 64<<30 {
		t.Errorf("memory = %d, want the 64 GiB override to beat the estimator", j.Resources[resource.Memory])
	}
}

func TestEnqueueRejectsUnschedulable(t *testing.T) {
	st := memory.New()

	d, err := dispatch.New(
		dispatch.WithStore(st),
		dispatch.WithConcurrency(1),
		dispatch.WithQueues([]string{"default"}),
	)
	if err != nil {
		t.Fatalf("dispatch.New() error = %v", err)
	}

	eng, err := engine.Build(d,
		engine.WithWorkerCapacity(resource.Set{resource.Memory: 8 << 30}))
	if err != nil {
		t.Fatalf("engine.Build() error = %v", err)
	}

	def := job.NewDefinition("res.toobig",
		func(context.Context, resPayload) error { return nil },
		job.WithResources(resource.MemoryGB(64)),
	)
	engine.Register(eng, def)

	_, err = engine.Enqueue(context.Background(), eng, def.Name, resPayload{N: 1})
	if !errors.Is(err, resource.ErrUnschedulable) {
		t.Fatalf("got %v, want ErrUnschedulable", err)
	}

	count, cErr := st.CountJobs(context.Background(), job.CountOpts{})
	if cErr != nil {
		t.Fatalf("CountJobs() error = %v", cErr)
	}

	if count != 0 {
		t.Errorf("an unschedulable job must not be persisted, found %d", count)
	}
}

// TestEnqueueUsesFleetCapacity proves the unschedulable check reads the
// largest capacity in the cluster, not just this process's own: a job too
// big for the local worker still enqueues when a bigger worker exists.
func TestEnqueueUsesFleetCapacity(t *testing.T) {
	st := memory.New()

	d, err := dispatch.New(
		dispatch.WithStore(st),
		dispatch.WithConcurrency(1),
		dispatch.WithQueues([]string{"default"}),
	)
	if err != nil {
		t.Fatalf("dispatch.New() error = %v", err)
	}

	eng, err := engine.Build(d,
		engine.WithWorkerCapacity(resource.Set{resource.Memory: 8 << 30}))
	if err != nil {
		t.Fatalf("engine.Build() error = %v", err)
	}

	ctx := context.Background()

	if rErr := st.RegisterWorker(ctx, &cluster.Worker{
		ID:       id.NewWorkerID(),
		State:    cluster.WorkerActive,
		LastSeen: time.Now().UTC(),
		Capacity: resource.Set{resource.Memory: 128 << 30},
	}); rErr != nil {
		t.Fatalf("RegisterWorker() error = %v", rErr)
	}

	def := job.NewDefinition("res.fleet",
		func(context.Context, resPayload) error { return nil },
		job.WithResources(resource.MemoryGB(64)),
	)
	engine.Register(eng, def)

	if _, err = engine.Enqueue(ctx, eng, def.Name, resPayload{N: 1}); err != nil {
		t.Fatalf("Enqueue() error = %v; a worker in the fleet can run this", err)
	}

	if got := eng.MaxWorkerCapacity(ctx)[resource.Memory]; got != 128<<30 {
		t.Errorf("MaxWorkerCapacity memory = %d, want the fleet maximum 128 GiB", got)
	}
}

// TestMaxWorkerCapacityIgnoresUnusableWorkers keeps a worker that cannot
// actually run anything from admitting jobs nothing can run.
//
// The stale case is the one that matters in production: nothing in
// Dispatch ever writes WorkerDead, so a worker killed by SIGKILL, an OOM
// or a pod eviction stays "active" in the registry with a frozen
// LastSeen until something sweeps the row.
func TestMaxWorkerCapacityIgnoresUnusableWorkers(t *testing.T) {
	tests := []struct {
		name   string
		state  cluster.WorkerState
		seenAt time.Time
	}{
		{
			name:   "crashed worker still marked active",
			state:  cluster.WorkerActive,
			seenAt: time.Now().UTC().Add(-time.Hour),
		},
		{
			name:   "explicitly dead worker",
			state:  cluster.WorkerDead,
			seenAt: time.Now().UTC(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := memory.New()

			d, err := dispatch.New(
				dispatch.WithStore(st),
				dispatch.WithConcurrency(1),
				dispatch.WithQueues([]string{"default"}),
			)
			if err != nil {
				t.Fatalf("dispatch.New() error = %v", err)
			}

			eng, err := engine.Build(d,
				engine.WithWorkerCapacity(resource.Set{resource.Memory: 8 << 30}))
			if err != nil {
				t.Fatalf("engine.Build() error = %v", err)
			}

			ctx := context.Background()

			if rErr := st.RegisterWorker(ctx, &cluster.Worker{
				ID:       id.NewWorkerID(),
				State:    tt.state,
				LastSeen: tt.seenAt,
				Capacity: resource.Set{resource.Memory: 128 << 30},
			}); rErr != nil {
				t.Fatalf("RegisterWorker() error = %v", rErr)
			}

			if got := eng.MaxWorkerCapacity(ctx)[resource.Memory]; got != 8<<30 {
				t.Errorf("MaxWorkerCapacity memory = %d, want 8 GiB; this worker cannot run anything", got)
			}

			// And the capacity it advertised must not admit a job either.
			def := job.NewDefinition("res.unusable."+tt.name,
				func(context.Context, resPayload) error { return nil },
				job.WithResources(resource.MemoryGB(64)),
			)
			engine.Register(eng, def)

			if _, err = engine.Enqueue(ctx, eng, def.Name, resPayload{N: 1}); !errors.Is(err, resource.ErrUnschedulable) {
				t.Errorf("Enqueue() = %v, want ErrUnschedulable", err)
			}
		})
	}
}

// errCluster is a cluster registry whose ListWorkers always fails.
//
// It embeds *memory.Store so it satisfies every store interface
// engine.Build type-asserts, and shadows the single method under test.
type errCluster struct {
	*memory.Store
}

var errListWorkers = errors.New("cluster registry unreachable")

func (errCluster) ListWorkers(context.Context) ([]*cluster.Worker, error) {
	return nil, errListWorkers
}

// TestEnqueueSurvivesClusterStoreFailure pins the documented contract:
// capacity is advisory, so a registry that cannot be read downgrades to
// skipping the unschedulable check rather than failing the enqueue.
func TestEnqueueSurvivesClusterStoreFailure(t *testing.T) {
	d, err := dispatch.New(
		dispatch.WithStore(errCluster{memory.New()}),
		dispatch.WithConcurrency(1),
		dispatch.WithQueues([]string{"default"}),
	)
	if err != nil {
		t.Fatalf("dispatch.New() error = %v", err)
	}

	// Capacity far below the job's requirement: were the registry
	// readable, this enqueue would be rejected.
	eng, err := engine.Build(d,
		engine.WithWorkerCapacity(resource.Set{resource.Memory: 8 << 30}))
	if err != nil {
		t.Fatalf("engine.Build() error = %v", err)
	}

	ctx := context.Background()

	if got := eng.MaxWorkerCapacity(ctx)[resource.Memory]; got != 8<<30 {
		t.Errorf("MaxWorkerCapacity memory = %d; a failed read must still yield the local seed", got)
	}

	def := job.NewDefinition("res.clusterdown",
		func(context.Context, resPayload) error { return nil },
		job.WithResources(resource.MemoryGB(4)),
	)
	engine.Register(eng, def)

	j, err := engine.Enqueue(ctx, eng, def.Name, resPayload{N: 1})
	if err != nil {
		t.Fatalf("Enqueue() error = %v; a cluster read failure must not fail an enqueue", err)
	}

	if j.Resources[resource.Memory] != 4<<30 {
		t.Errorf("memory = %d, want 4 GiB; resolution still runs", j.Resources[resource.Memory])
	}
}

func TestInputSizesTieBreaksDeterministically(t *testing.T) {
	// Two inputs of equal size must always yield the same primary hash,
	// or the same job enqueued twice would advertise different locality.
	bindings := map[string]artifact.Ref{
		"zulu":  {ID: id.NewArtifactID(), Size: 100, ContentHash: "blake3:zz"},
		"alpha": {ID: id.NewArtifactID(), Size: 100, ContentHash: "blake3:aa"},
	}

	for range 20 {
		_, total, primary := engine.InputSizesForTest(bindings)
		if total != 200 {
			t.Fatalf("total = %d, want 200", total)
		}

		if primary != "blake3:aa" {
			t.Fatalf("primary = %q, want the name-ordered winner", primary)
		}
	}
}
