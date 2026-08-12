package exec_test

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/id"
)

// fakeExecutor is a minimal Executor for registry tests.
type fakeExecutor struct {
	name  string
	level exec.Level
}

func (f fakeExecutor) Name() string      { return f.name }
func (f fakeExecutor) Level() exec.Level { return f.level }

func (f fakeExecutor) Run(context.Context, *exec.Request) (*exec.Result, error) {
	return &exec.Result{Status: exec.StatusOK}, nil
}

func (f fakeExecutor) Reclaim(context.Context, id.WorkerID) error { return nil }
func (f fakeExecutor) Close() error                               { return nil }

func TestRegistry_SelectPicksWeakestSufficient(t *testing.T) {
	r := exec.NewRegistry(fakeExecutor{name: "inprocess", level: exec.LevelNone})
	r.Add(fakeExecutor{name: "subprocess", level: exec.LevelProcess})
	r.Add(fakeExecutor{name: "k8s", level: exec.LevelVM})

	// A job needing process isolation must not be handed the Kubernetes
	// rung when a cheaper sufficient one exists.
	got, err := r.Select(exec.NewPolicy(exec.Isolate(exec.LevelProcess)))
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if got.Name() != "subprocess" {
		t.Errorf("Select() = %q, want %q", got.Name(), "subprocess")
	}
}

func TestRegistry_SelectEscalatesWhenExactRungAbsent(t *testing.T) {
	r := exec.NewRegistry(fakeExecutor{name: "inprocess", level: exec.LevelNone})
	r.Add(fakeExecutor{name: "k8s", level: exec.LevelVM})

	got, err := r.Select(exec.NewPolicy(exec.Isolate(exec.LevelSandboxed)))
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if got.Name() != "k8s" {
		t.Errorf("Select() = %q, want %q", got.Name(), "k8s")
	}
}

func TestRegistry_SelectRefusesSilentDowngrade(t *testing.T) {
	r := exec.NewRegistry(fakeExecutor{name: "inprocess", level: exec.LevelNone})

	_, err := r.Select(exec.NewPolicy(exec.Isolate(exec.LevelSandboxed)))
	if !errors.Is(err, exec.ErrNoExecutor) {
		t.Fatalf("Select() error = %v, want %v", err, exec.ErrNoExecutor)
	}
}

func TestRegistry_SelectAllowsExplicitDowngrade(t *testing.T) {
	r := exec.NewRegistry(fakeExecutor{name: "inprocess", level: exec.LevelNone})

	got, err := r.Select(exec.NewPolicy(
		exec.Isolate(exec.LevelSandboxed),
		exec.AllowDowngrade(),
	))
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if got.Name() != "inprocess" {
		t.Errorf("Select() = %q, want %q", got.Name(), "inprocess")
	}
}

func TestRegistry_SelectDefaultForLevelNone(t *testing.T) {
	r := exec.NewRegistry(fakeExecutor{name: "inprocess", level: exec.LevelNone})
	r.Add(fakeExecutor{name: "subprocess", level: exec.LevelProcess})

	got, err := r.Select(exec.NewPolicy())
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if got.Name() != "inprocess" {
		t.Errorf("Select() = %q, want the default %q", got.Name(), "inprocess")
	}
}

func TestRegistry_AddReplacesSameName(t *testing.T) {
	r := exec.NewRegistry(fakeExecutor{name: "inprocess", level: exec.LevelNone})
	r.Add(fakeExecutor{name: "subprocess", level: exec.LevelProcess})
	r.Add(fakeExecutor{name: "subprocess", level: exec.LevelSandboxed})

	if n := len(r.Executors()); n != 2 {
		t.Fatalf("len(Executors()) = %d, want 2", n)
	}
	got, err := r.Select(exec.NewPolicy(exec.Isolate(exec.LevelSandboxed)))
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if got.Name() != "subprocess" {
		t.Errorf("Select() = %q, want %q", got.Name(), "subprocess")
	}
}
