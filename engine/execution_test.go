package engine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

type execPayload struct {
	Value int `json:"value"`
}

func newTestEngine(t *testing.T) *engine.Engine {
	t.Helper()

	d, err := dispatch.New(dispatch.WithStore(memory.New()))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}
	eng, err := engine.Build(d)
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	return eng
}

func TestEngine_ExecutorsIncludesInProcessByDefault(t *testing.T) {
	eng := newTestEngine(t)

	executors := eng.Executors()
	if executors == nil {
		t.Fatal("Executors() = nil, want a registry")
	}
	def := executors.Default()
	if def == nil {
		t.Fatal("Default() = nil, want the in-process executor")
	}
	if def.Name() != "inprocess" {
		t.Errorf("Default().Name() = %q, want %q", def.Name(), "inprocess")
	}
}

func TestEngine_RegisterRejectsUnsatisfiablePolicy(t *testing.T) {
	// A definition that must be isolated must not silently run
	// unisolated because it was deployed somewhere that cannot isolate.
	eng := newTestEngine(t)

	err := engine.RegisterChecked(eng, job.NewDefinition("needs.sandbox",
		func(context.Context, execPayload) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelSandboxed)),
	))
	if !errors.Is(err, exec.ErrNoExecutor) {
		t.Fatalf("RegisterChecked() = %v, want %v", err, exec.ErrNoExecutor)
	}
}

func TestEngine_RegisterCheckedAllowsExplicitDowngrade(t *testing.T) {
	eng := newTestEngine(t)

	err := engine.RegisterChecked(eng, job.NewDefinition("needs.sandbox.but.ok",
		func(context.Context, execPayload) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelSandboxed), exec.AllowDowngrade()),
	))
	if err != nil {
		t.Fatalf("RegisterChecked() = %v, want nil", err)
	}
}

func TestEngine_RegisterStaysUnchecked(t *testing.T) {
	// Register is the unchecked path by existing convention, and its
	// signature must not change. A policy nothing satisfies is caught by
	// RegisterChecked and by RegisterAll, not here.
	eng := newTestEngine(t)

	engine.Register(eng, job.NewDefinition("unchecked.sandbox",
		func(context.Context, execPayload) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelSandboxed)),
	))

	if _, ok := eng.Registry().Get("unchecked.sandbox"); !ok {
		t.Error("Register did not register the handler")
	}
}

func TestEngine_RegisterAll(t *testing.T) {
	eng := newTestEngine(t)

	defs := []job.Registrable{
		job.NewDefinition("a.job", func(context.Context, execPayload) error { return nil }),
		job.NewDefinition("b.job", func(context.Context, struct{}) error { return nil }),
	}

	if err := engine.RegisterAll(eng, defs...); err != nil {
		t.Fatalf("RegisterAll() = %v, want nil", err)
	}
	for _, name := range []string{"a.job", "b.job"} {
		if _, ok := eng.Registry().Get(name); !ok {
			t.Errorf("handler %q not registered", name)
		}
	}
}

func TestEngine_RegisterAllRejectsWholeSetOnOneFailure(t *testing.T) {
	// RegisterAll validates every definition before registering any of
	// them, so a rejected set leaves the registry as it was rather than
	// half populated.
	eng := newTestEngine(t)

	defs := []job.Registrable{
		job.NewDefinition("good.job", func(context.Context, execPayload) error { return nil }),
		job.NewDefinition("bad.job",
			func(context.Context, execPayload) error { return nil },
			job.WithExecution(exec.Isolate(exec.LevelSandboxed)),
		),
	}

	if err := engine.RegisterAll(eng, defs...); !errors.Is(err, exec.ErrNoExecutor) {
		t.Fatalf("RegisterAll() = %v, want %v", err, exec.ErrNoExecutor)
	}
	if _, ok := eng.Registry().Get("good.job"); ok {
		t.Error("good.job was registered even though the set was rejected")
	}
}
