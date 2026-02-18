package ext_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// ──────────────────────────────────────────────────
// Test extensions
// ──────────────────────────────────────────────────

// allHooksExt implements every lifecycle hook for testing.
type allHooksExt struct {
	calls []string
}

func (e *allHooksExt) Name() string { return "all-hooks" }

func (e *allHooksExt) OnJobEnqueued(_ context.Context, _ *job.Job) error {
	e.calls = append(e.calls, "OnJobEnqueued")
	return nil
}

func (e *allHooksExt) OnJobStarted(_ context.Context, _ *job.Job) error {
	e.calls = append(e.calls, "OnJobStarted")
	return nil
}

func (e *allHooksExt) OnJobCompleted(_ context.Context, _ *job.Job, _ time.Duration) error {
	e.calls = append(e.calls, "OnJobCompleted")
	return nil
}

func (e *allHooksExt) OnJobFailed(_ context.Context, _ *job.Job, _ error) error {
	e.calls = append(e.calls, "OnJobFailed")
	return nil
}

func (e *allHooksExt) OnJobRetrying(_ context.Context, _ *job.Job, _ int, _ time.Time) error {
	e.calls = append(e.calls, "OnJobRetrying")
	return nil
}

func (e *allHooksExt) OnJobDLQ(_ context.Context, _ *job.Job, _ error) error {
	e.calls = append(e.calls, "OnJobDLQ")
	return nil
}

func (e *allHooksExt) OnWorkflowStarted(_ context.Context, _ *workflow.Run) error {
	e.calls = append(e.calls, "OnWorkflowStarted")
	return nil
}

func (e *allHooksExt) OnWorkflowStepCompleted(_ context.Context, _ *workflow.Run, _ string, _ time.Duration) error {
	e.calls = append(e.calls, "OnWorkflowStepCompleted")
	return nil
}

func (e *allHooksExt) OnWorkflowStepFailed(_ context.Context, _ *workflow.Run, _ string, _ error) error {
	e.calls = append(e.calls, "OnWorkflowStepFailed")
	return nil
}

func (e *allHooksExt) OnWorkflowCompleted(_ context.Context, _ *workflow.Run, _ time.Duration) error {
	e.calls = append(e.calls, "OnWorkflowCompleted")
	return nil
}

func (e *allHooksExt) OnWorkflowFailed(_ context.Context, _ *workflow.Run, _ error) error {
	e.calls = append(e.calls, "OnWorkflowFailed")
	return nil
}

func (e *allHooksExt) OnCronFired(_ context.Context, _ string, _ id.JobID) error {
	e.calls = append(e.calls, "OnCronFired")
	return nil
}

func (e *allHooksExt) OnShutdown(_ context.Context) error {
	e.calls = append(e.calls, "OnShutdown")
	return nil
}

// jobOnlyExt only implements job-related hooks.
type jobOnlyExt struct {
	calls []string
}

func (e *jobOnlyExt) Name() string { return "job-only" }

func (e *jobOnlyExt) OnJobEnqueued(_ context.Context, _ *job.Job) error {
	e.calls = append(e.calls, "OnJobEnqueued")
	return nil
}

func (e *jobOnlyExt) OnJobCompleted(_ context.Context, _ *job.Job, _ time.Duration) error {
	e.calls = append(e.calls, "OnJobCompleted")
	return nil
}

// failingExt returns errors from hooks.
type failingExt struct{}

func (e *failingExt) Name() string { return "failing" }

func (e *failingExt) OnJobEnqueued(_ context.Context, _ *job.Job) error {
	return errors.New("boom")
}

func (e *failingExt) OnShutdown(_ context.Context) error {
	return errors.New("shutdown boom")
}

// ──────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────

func TestRegistry_RegisterDiscoversInterfaces(t *testing.T) {
	r := ext.NewRegistry(slog.Default())
	all := &allHooksExt{}
	r.Register(all)

	if got := len(r.Extensions()); got != 1 {
		t.Fatalf("expected 1 extension, got %d", got)
	}
	if got := r.Extensions()[0].Name(); got != "all-hooks" {
		t.Fatalf("expected name 'all-hooks', got %q", got)
	}
}

func TestRegistry_EmitFiresOnlyImplementors(t *testing.T) {
	r := ext.NewRegistry(slog.Default())
	all := &allHooksExt{}
	jo := &jobOnlyExt{}
	r.Register(all)
	r.Register(jo)

	ctx := context.Background()
	j := &job.Job{Name: "test-job"}

	// Both implement OnJobEnqueued → both called.
	r.EmitJobEnqueued(ctx, j)
	if len(all.calls) != 1 || all.calls[0] != "OnJobEnqueued" {
		t.Fatalf("all: expected [OnJobEnqueued], got %v", all.calls)
	}
	if len(jo.calls) != 1 || jo.calls[0] != "OnJobEnqueued" {
		t.Fatalf("jo: expected [OnJobEnqueued], got %v", jo.calls)
	}

	// Only all implements OnJobStarted → jo not called.
	r.EmitJobStarted(ctx, j)
	if len(all.calls) != 2 || all.calls[1] != "OnJobStarted" {
		t.Fatalf("all: expected OnJobStarted as 2nd, got %v", all.calls)
	}
	if len(jo.calls) != 1 {
		t.Fatalf("jo: should still have 1 call, got %v", jo.calls)
	}
}

func TestRegistry_AllJobHooksFire(t *testing.T) {
	r := ext.NewRegistry(slog.Default())
	all := &allHooksExt{}
	r.Register(all)

	ctx := context.Background()
	j := &job.Job{Name: "test-job"}

	r.EmitJobEnqueued(ctx, j)
	r.EmitJobStarted(ctx, j)
	r.EmitJobCompleted(ctx, j, time.Second)
	r.EmitJobFailed(ctx, j, errors.New("fail"))
	r.EmitJobRetrying(ctx, j, 1, time.Now())
	r.EmitJobDLQ(ctx, j, errors.New("dlq"))

	expected := []string{
		"OnJobEnqueued", "OnJobStarted", "OnJobCompleted",
		"OnJobFailed", "OnJobRetrying", "OnJobDLQ",
	}
	if len(all.calls) != len(expected) {
		t.Fatalf("expected %d calls, got %d: %v", len(expected), len(all.calls), all.calls)
	}
	for i, want := range expected {
		if all.calls[i] != want {
			t.Errorf("call[%d] = %q, want %q", i, all.calls[i], want)
		}
	}
}

func TestRegistry_AllWorkflowHooksFire(t *testing.T) {
	r := ext.NewRegistry(slog.Default())
	all := &allHooksExt{}
	r.Register(all)

	ctx := context.Background()
	run := &workflow.Run{Name: "test-wf"}

	r.EmitWorkflowStarted(ctx, run)
	r.EmitWorkflowStepCompleted(ctx, run, "step1", time.Second)
	r.EmitWorkflowStepFailed(ctx, run, "step2", errors.New("step fail"))
	r.EmitWorkflowCompleted(ctx, run, 2*time.Second)
	r.EmitWorkflowFailed(ctx, run, errors.New("wf fail"))

	expected := []string{
		"OnWorkflowStarted", "OnWorkflowStepCompleted",
		"OnWorkflowStepFailed", "OnWorkflowCompleted", "OnWorkflowFailed",
	}
	if len(all.calls) != len(expected) {
		t.Fatalf("expected %d calls, got %d: %v", len(expected), len(all.calls), all.calls)
	}
	for i, want := range expected {
		if all.calls[i] != want {
			t.Errorf("call[%d] = %q, want %q", i, all.calls[i], want)
		}
	}
}

func TestRegistry_CronAndShutdownHooksFire(t *testing.T) {
	r := ext.NewRegistry(slog.Default())
	all := &allHooksExt{}
	r.Register(all)

	ctx := context.Background()
	r.EmitCronFired(ctx, "daily-report", id.NewJobID())
	r.EmitShutdown(ctx)

	if len(all.calls) != 2 {
		t.Fatalf("expected 2 calls, got %d: %v", len(all.calls), all.calls)
	}
	if all.calls[0] != "OnCronFired" {
		t.Errorf("call[0] = %q, want OnCronFired", all.calls[0])
	}
	if all.calls[1] != "OnShutdown" {
		t.Errorf("call[1] = %q, want OnShutdown", all.calls[1])
	}
}

func TestRegistry_HookErrorsLoggedNotPropagated(t *testing.T) {
	r := ext.NewRegistry(slog.Default())
	failing := &failingExt{}
	all := &allHooksExt{}

	// Register failing first, then all-hooks. Both should be called.
	r.Register(failing)
	r.Register(all)

	ctx := context.Background()
	j := &job.Job{Name: "test-job"}

	// No panic, no error propagation. allHooksExt should still fire.
	r.EmitJobEnqueued(ctx, j)

	if len(all.calls) != 1 || all.calls[0] != "OnJobEnqueued" {
		t.Fatalf("all: expected [OnJobEnqueued] despite failing ext, got %v", all.calls)
	}
}

func TestRegistry_EmptyRegistryNoOp(_ *testing.T) {
	r := ext.NewRegistry(slog.Default())
	ctx := context.Background()

	// None of these should panic or error.
	r.EmitJobEnqueued(ctx, &job.Job{})
	r.EmitJobStarted(ctx, &job.Job{})
	r.EmitJobCompleted(ctx, &job.Job{}, time.Second)
	r.EmitJobFailed(ctx, &job.Job{}, errors.New("x"))
	r.EmitJobRetrying(ctx, &job.Job{}, 1, time.Now())
	r.EmitJobDLQ(ctx, &job.Job{}, errors.New("x"))
	r.EmitWorkflowStarted(ctx, &workflow.Run{})
	r.EmitWorkflowStepCompleted(ctx, &workflow.Run{}, "s", time.Second)
	r.EmitWorkflowStepFailed(ctx, &workflow.Run{}, "s", errors.New("x"))
	r.EmitWorkflowCompleted(ctx, &workflow.Run{}, time.Second)
	r.EmitWorkflowFailed(ctx, &workflow.Run{}, errors.New("x"))
	r.EmitCronFired(ctx, "test", id.NewJobID())
	r.EmitShutdown(ctx)
}

func TestRegistry_MultipleExtensionsOrderPreserved(t *testing.T) {
	r := ext.NewRegistry(slog.Default())
	ext1 := &allHooksExt{}
	ext2 := &allHooksExt{}
	r.Register(ext1)
	r.Register(ext2)

	ctx := context.Background()
	r.EmitJobEnqueued(ctx, &job.Job{})

	// Both should be called.
	if len(ext1.calls) != 1 {
		t.Errorf("ext1: expected 1 call, got %d", len(ext1.calls))
	}
	if len(ext2.calls) != 1 {
		t.Errorf("ext2: expected 1 call, got %d", len(ext2.calls))
	}
}
