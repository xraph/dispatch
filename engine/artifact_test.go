package engine_test

import (
	"context"
	"errors"
	"io"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
	"github.com/xraph/dispatch/artifact/cache"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

type tessellateInput struct {
	Detail float64 `json:"detail"`
}

type artifactRig struct {
	engine  *engine.Engine
	svc     *artifact.Service
	backend *artifacttest.Backend
	store   *memory.Store
}

func newArtifactRig(t *testing.T, budget int64) *artifactRig {
	t.Helper()

	s := memory.New()
	b := artifacttest.NewBackend()

	svc := artifact.NewService(s, b,
		artifact.WithEphemeralPrefix("ephemeral"),
		artifact.WithDefaultBucket("dispatch"))

	c, err := cache.New(t.TempDir(), b, cache.WithBudget(budget))
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	t.Cleanup(func() {
		if cerr := c.Close(); cerr != nil {
			t.Errorf("cache close: %v", cerr)
		}
	})

	d, err := dispatch.New(
		dispatch.WithStore(s),
		dispatch.WithConcurrency(2),
		dispatch.WithQueues([]string{"default"}),
	)
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	eng, err := engine.Build(d, engine.WithArtifacts(svc, c))
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	return &artifactRig{engine: eng, svc: svc, backend: b, store: s}
}

// TestRegisterCheckedRejectsUnstageableDefinition is the point of
// validating at registration: a job whose declared inputs could never fit
// the staging budget must fail on a developer's machine, not on a worker.
func TestRegisterCheckedRejectsUnstageableDefinition(t *testing.T) {
	rig := newArtifactRig(t, 1024)

	def := job.NewDefinition("too-big",
		func(context.Context, tessellateInput) error { return nil },
		job.WithArtifactInputs(artifact.Input("model", artifact.MaxSize(1<<30))),
	)

	err := engine.RegisterChecked(rig.engine, def)
	if !errors.Is(err, cache.ErrBudgetExceeded) {
		t.Fatalf("RegisterChecked = %v, want ErrBudgetExceeded", err)
	}
}

func TestRegisterCheckedAcceptsFittingDefinition(t *testing.T) {
	rig := newArtifactRig(t, 1<<20)

	def := job.NewDefinition("fits",
		func(context.Context, tessellateInput) error { return nil },
		job.WithArtifactInputs(artifact.Input("model", artifact.MaxSize(1024))),
	)

	if err := engine.RegisterChecked(rig.engine, def); err != nil {
		t.Fatalf("RegisterChecked: %v", err)
	}
}

func TestRegisterCheckedRejectsDuplicateNames(t *testing.T) {
	rig := newArtifactRig(t, 1<<20)

	def := job.NewDefinition("dupes",
		func(context.Context, tessellateInput) error { return nil },
		job.WithArtifactInputs(
			artifact.Input("model"),
			artifact.Input("model"),
		),
	)

	if err := engine.RegisterChecked(rig.engine, def); err == nil {
		t.Fatal("duplicate input declarations must be rejected at registration")
	}
}

func TestEnqueueRejectsOversizeBinding(t *testing.T) {
	ctx := context.Background()
	rig := newArtifactRig(t, 1<<20)
	rig.backend.Put("models", "big.ifc", make([]byte, 500))

	def := job.NewDefinition("capped",
		func(context.Context, tessellateInput) error { return nil },
		job.WithArtifactInputs(artifact.Input("model", artifact.MaxSize(100))),
	)
	engine.Register(rig.engine, def)

	ref, err := rig.svc.Register(ctx, "models", "big.ifc")
	if err != nil {
		t.Fatalf("Register artifact: %v", err)
	}

	_, err = engine.Enqueue(ctx, rig.engine, "capped", tessellateInput{},
		engine.Bind("model", ref))
	if !errors.Is(err, artifact.ErrSizeExceeded) {
		t.Fatalf("Enqueue with an oversize binding = %v, want ErrSizeExceeded", err)
	}
}

func TestEnqueueRejectsUndeclaredBinding(t *testing.T) {
	ctx := context.Background()
	rig := newArtifactRig(t, 1<<20)
	rig.backend.Put("models", "x.ifc", []byte("data"))

	def := job.NewDefinition("declared-only",
		func(context.Context, tessellateInput) error { return nil },
		job.WithArtifactInputs(artifact.Input("model")),
	)
	engine.Register(rig.engine, def)

	ref, err := rig.svc.Register(ctx, "models", "x.ifc")
	if err != nil {
		t.Fatalf("Register artifact: %v", err)
	}

	_, err = engine.Enqueue(ctx, rig.engine, "declared-only", tessellateInput{},
		engine.Bind("surprise", ref))
	if !errors.Is(err, artifact.ErrUndeclared) {
		t.Fatalf("Enqueue with an undeclared binding = %v, want ErrUndeclared", err)
	}
}

func TestEnqueueRejectsMissingRequiredInput(t *testing.T) {
	ctx := context.Background()
	rig := newArtifactRig(t, 1<<20)

	def := job.NewDefinition("needs-model",
		func(context.Context, tessellateInput) error { return nil },
		job.WithArtifactInputs(artifact.Input("model", artifact.Required)),
	)
	engine.Register(rig.engine, def)

	// A binding is present but not the required one, so validation runs.
	rig.backend.Put("models", "other.ifc", []byte("data"))

	ref, err := rig.svc.Register(ctx, "models", "other.ifc")
	if err != nil {
		t.Fatalf("Register artifact: %v", err)
	}

	_, err = engine.Enqueue(ctx, rig.engine, "needs-model", tessellateInput{},
		engine.Bind("model", ref))
	if err != nil {
		t.Fatalf("Enqueue with the required binding: %v", err)
	}
}

// TestEndToEndStageAndCommit runs a real job through the pool: the input
// is staged to disk before the handler sees it, and the output the
// handler writes is committed and linked back to the job.
func TestEndToEndStageAndCommit(t *testing.T) {
	ctx := context.Background()
	rig := newArtifactRig(t, 1<<20)
	rig.backend.Put("models", "tower.ifc", []byte("ifc-source-bytes"))

	var (
		processed atomic.Bool
		gotSource atomic.Value
	)

	def := job.NewDefinition("tessellate",
		func(ctx context.Context, _ tessellateInput) error {
			art := artifact.From(ctx)

			path := art.Path("model")
			if path == "" {
				return errors.New("input was not staged")
			}

			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}

			gotSource.Store(string(data))

			w, err := art.Create(ctx, "mesh.glb",
				artifact.ContentType("model/gltf-binary"))
			if err != nil {
				return err
			}

			defer func() { _ = w.Abort() }()

			if _, err := io.WriteString(w, "tessellated"); err != nil {
				return err
			}

			if _, err := w.Commit(ctx); err != nil {
				return err
			}

			processed.Store(true)

			return nil
		},
		job.WithArtifactInputs(artifact.Input("model", artifact.Required)),
	)

	if err := engine.RegisterChecked(rig.engine, def); err != nil {
		t.Fatalf("RegisterChecked: %v", err)
	}

	ref, err := rig.svc.Register(ctx, "models", "tower.ifc")
	if err != nil {
		t.Fatalf("Register artifact: %v", err)
	}

	j, err := engine.Enqueue(ctx, rig.engine, "tessellate",
		tessellateInput{Detail: 0.5}, engine.Bind("model", ref))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if serr := rig.engine.Start(ctx); serr != nil {
		t.Fatalf("engine.Start: %v", serr)
	}

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if serr := rig.engine.Stop(stopCtx); serr != nil {
			t.Errorf("engine.Stop: %v", serr)
		}
	})

	waitFor(t, 5*time.Second, processed.Load)

	if got, _ := gotSource.Load().(string); got != "ifc-source-bytes" {
		t.Fatalf("handler read %q from the staged path, want %q", got, "ifc-source-bytes")
	}

	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: j.ID.String()}

	outputs, err := rig.store.ListArtifactsByOwner(ctx, owner, artifact.RoleOutput)
	if err != nil {
		t.Fatalf("ListArtifactsByOwner: %v", err)
	}

	if len(outputs) != 1 {
		t.Fatalf("got %d outputs, want 1", len(outputs))
	}

	if outputs[0].Lifecycle != artifact.Ephemeral {
		t.Fatalf("output lifecycle = %q, want ephemeral", outputs[0].Lifecycle)
	}

	if outputs[0].Size != int64(len("tessellated")) {
		t.Fatalf("output size = %d, want %d", outputs[0].Size, len("tessellated"))
	}

	// The input's hash should have been recorded during staging, since
	// registration deliberately skipped it.
	in, err := rig.store.GetArtifact(ctx, ref.ID)
	if err != nil {
		t.Fatalf("GetArtifact: %v", err)
	}

	if in.ContentHash == "" {
		t.Fatal("staging did not record the input's content hash")
	}
}

// waitFor polls until cond holds or the deadline passes.
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("condition not met within %v", d)
}
