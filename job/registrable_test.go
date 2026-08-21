package job_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/job"
)

type meshPayload struct {
	Detail int `json:"detail"`
}

func TestDefinition_ImplementsRegistrable(t *testing.T) {
	// The whole out-of-process design depends on this compiling: a
	// heterogeneous slice of definitions with different payload types.
	defs := []job.Registrable{
		job.NewDefinition("send-email", func(_ context.Context, _ emailPayload) error { return nil }),
		job.NewDefinition("tessellate", func(_ context.Context, _ meshPayload) error { return nil }),
	}

	r := job.NewRegistry()
	for _, d := range defs {
		d.Register(r)
	}

	for _, want := range []string{"send-email", "tessellate"} {
		if _, ok := r.Get(want); !ok {
			t.Errorf("handler %q not registered", want)
		}
	}
}

func TestDefinition_JobName(t *testing.T) {
	d := job.NewDefinition("tessellate", func(_ context.Context, _ meshPayload) error { return nil })

	if got := d.JobName(); got != "tessellate" {
		t.Errorf("JobName() = %q, want %q", got, "tessellate")
	}
}

func TestWithExecution(t *testing.T) {
	d := job.NewDefinition("tessellate",
		func(_ context.Context, _ meshPayload) error { return nil },
		job.WithExecution(
			exec.Isolate(exec.LevelSandboxed),
			exec.GracePeriod(90*time.Second),
		),
	)

	if d.Opts.Execution.Level != exec.LevelSandboxed {
		t.Errorf("Level = %v, want %v", d.Opts.Execution.Level, exec.LevelSandboxed)
	}
	if d.Opts.Execution.GracePeriod != 90*time.Second {
		t.Errorf("GracePeriod = %v, want %v", d.Opts.Execution.GracePeriod, 90*time.Second)
	}
}

func TestDefaultOptions_HasUsableExecutionPolicy(t *testing.T) {
	// A definition that says nothing about execution must still carry a
	// usable grace period, or later rungs would kill instantly.
	d := job.NewDefinition("plain", func(_ context.Context, _ meshPayload) error { return nil })

	if d.Opts.Execution.Level != exec.LevelNone {
		t.Errorf("Level = %v, want %v", d.Opts.Execution.Level, exec.LevelNone)
	}
	if d.Opts.Execution.GracePeriod != exec.DefaultGracePeriod {
		t.Errorf("GracePeriod = %v, want %v", d.Opts.Execution.GracePeriod, exec.DefaultGracePeriod)
	}
}

func TestRegistry_Policy(t *testing.T) {
	r := job.NewRegistry()
	d := job.NewDefinition("tessellate",
		func(_ context.Context, _ meshPayload) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelVM)),
	)
	d.Register(r)

	if got := r.Policy("tessellate").Level; got != exec.LevelVM {
		t.Errorf("Policy(tessellate).Level = %v, want %v", got, exec.LevelVM)
	}
	// An unregistered name yields the zero policy with usable defaults.
	if got := r.Policy("absent").Level; got != exec.LevelNone {
		t.Errorf("Policy(absent).Level = %v, want %v", got, exec.LevelNone)
	}
	if got := r.Policy("absent").GracePeriod; got != exec.DefaultGracePeriod {
		t.Errorf("Policy(absent).GracePeriod = %v, want %v", got, exec.DefaultGracePeriod)
	}
}
