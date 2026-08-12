package engine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
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

// TestMaxWorkerCapacityIgnoresInactiveWorkers keeps a dead worker's
// capacity from admitting jobs nothing can run.
func TestMaxWorkerCapacityIgnoresInactiveWorkers(t *testing.T) {
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
		State:    cluster.WorkerDead,
		Capacity: resource.Set{resource.Memory: 128 << 30},
	}); rErr != nil {
		t.Fatalf("RegisterWorker() error = %v", rErr)
	}

	if got := eng.MaxWorkerCapacity(ctx)[resource.Memory]; got != 8<<30 {
		t.Errorf("MaxWorkerCapacity memory = %d, want 8 GiB; a dead worker's capacity is not usable", got)
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
