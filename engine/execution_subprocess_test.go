package engine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/subprocess"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

// TestEngine_SubprocessRungSatisfiesLevelProcess proves the no-silent-
// downgrade rule end to end through the mechanism configuration actually
// drives: engine.WithExecutor. A definition declaring
// exec.Isolate(exec.LevelProcess) must fail registration when nothing
// configured can provide it, and succeed once the subprocess rung is
// registered — exactly what extension.resolveExecutionOptions wires up
// from the "execution.subprocess" YAML block, without engine importing
// extension (that would be a cycle) or extension needing a Forge app just
// to prove this.
func TestEngine_SubprocessRungSatisfiesLevelProcess(t *testing.T) {
	newEngine := func(t *testing.T, opts ...engine.Option) *engine.Engine {
		t.Helper()

		d, err := dispatch.New(dispatch.WithStore(memory.New()))
		if err != nil {
			t.Fatalf("dispatch.New: %v", err)
		}
		eng, err := engine.Build(d, opts...)
		if err != nil {
			t.Fatalf("engine.Build: %v", err)
		}

		return eng
	}

	t.Run("no subprocess rung configured fails registration", func(t *testing.T) {
		eng := newEngine(t)

		err := engine.RegisterChecked(eng, job.NewDefinition("needs.process",
			func(context.Context, execSubprocessPayload) error { return nil },
			job.WithExecution(exec.Isolate(exec.LevelProcess)),
		))
		if !errors.Is(err, exec.ErrNoExecutor) {
			t.Fatalf("RegisterChecked() = %v, want %v", err, exec.ErrNoExecutor)
		}
	})

	t.Run("subprocess rung configured satisfies the policy", func(t *testing.T) {
		eng := newEngine(t, engine.WithExecutor(subprocess.New()))

		err := engine.RegisterChecked(eng, job.NewDefinition("needs.process",
			func(context.Context, execSubprocessPayload) error { return nil },
			job.WithExecution(exec.Isolate(exec.LevelProcess)),
		))
		if err != nil {
			t.Fatalf("RegisterChecked() = %v, want nil", err)
		}

		executors := eng.Executors()
		selected, selectErr := executors.Select(exec.NewPolicy(exec.Isolate(exec.LevelProcess)))
		if selectErr != nil {
			t.Fatalf("Select() = %v, want nil", selectErr)
		}
		if selected.Name() != subprocess.Name {
			t.Errorf("Select().Name() = %q, want %q", selected.Name(), subprocess.Name)
		}
	})

	t.Run("a job declaring no isolation still runs in-process", func(t *testing.T) {
		// Configuring the subprocess rung must not change the DEFAULT: a
		// definition that declares nothing still resolves to the
		// in-process executor, exactly as it does with no execution
		// config at all.
		eng := newEngine(t, engine.WithExecutor(subprocess.New()))

		selected, err := eng.Executors().Select(exec.NewPolicy())
		if err != nil {
			t.Fatalf("Select() = %v, want nil", err)
		}
		if selected.Name() != "inprocess" {
			t.Errorf("Select().Name() = %q, want %q — the default must not change", selected.Name(), "inprocess")
		}
	})
}

type execSubprocessPayload struct{}
