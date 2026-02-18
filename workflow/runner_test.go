package workflow_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/workflow"
)

// noopEmitter implements workflow.RunEmitter with no-ops.
type noopEmitter struct{}

func (noopEmitter) EmitStepCompleted(_ context.Context, _ *workflow.Run, _ string, _ time.Duration) {
}
func (noopEmitter) EmitStepFailed(_ context.Context, _ *workflow.Run, _ string, _ error) {}
func (noopEmitter) EmitWorkflowStarted(_ context.Context, _ *workflow.Run)               {}
func (noopEmitter) EmitWorkflowCompleted(_ context.Context, _ *workflow.Run, _ time.Duration) {
}
func (noopEmitter) EmitWorkflowFailed(_ context.Context, _ *workflow.Run, _ error) {}

func newTestRunner() (*workflow.Runner, *workflow.Registry, *memory.Store) {
	s := memory.New()
	reg := workflow.NewRegistry()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := workflow.NewRunner(reg, s, s, noopEmitter{}, logger)
	return runner, reg, s
}

func TestRunner_StartAndComplete(t *testing.T) {
	runner, reg, s := newTestRunner()

	var gotInput orderInput
	def := workflow.NewWorkflow("order-wf", func(_ *workflow.Workflow, input orderInput) error {
		gotInput = input
		return nil
	})
	workflow.RegisterDefinition(reg, def)

	run, err := workflow.Start(context.Background(), runner, "order-wf", orderInput{
		OrderID: "ord_99",
		Amount:  500,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if run.State != workflow.RunStateCompleted {
		t.Errorf("run state = %q, want %q", run.State, workflow.RunStateCompleted)
	}
	if run.CompletedAt == nil {
		t.Error("expected CompletedAt to be set")
	}
	if gotInput.OrderID != "ord_99" {
		t.Errorf("OrderID = %q, want %q", gotInput.OrderID, "ord_99")
	}

	// Verify in store.
	stored, err := s.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if stored.State != workflow.RunStateCompleted {
		t.Errorf("stored state = %q, want %q", stored.State, workflow.RunStateCompleted)
	}
}

func TestRunner_StartAndFail(t *testing.T) {
	runner, reg, s := newTestRunner()

	workflow.RegisterDefinition(reg, workflow.NewWorkflow("fail-wf", func(_ *workflow.Workflow, _ struct{}) error {
		return errors.New("intentional failure")
	}))

	run, err := workflow.Start(context.Background(), runner, "fail-wf", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if run.State != workflow.RunStateFailed {
		t.Errorf("run state = %q, want %q", run.State, workflow.RunStateFailed)
	}
	if run.Error != "intentional failure" {
		t.Errorf("run error = %q, want %q", run.Error, "intentional failure")
	}

	stored, err := s.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if stored.State != workflow.RunStateFailed {
		t.Errorf("stored state = %q, want %q", stored.State, workflow.RunStateFailed)
	}
}

func TestRunner_StartUnknownWorkflow(t *testing.T) {
	runner, _, _ := newTestRunner()

	_, err := workflow.Start(context.Background(), runner, "nonexistent", struct{}{})
	if err == nil {
		t.Fatal("expected error for unknown workflow")
	}
}

func TestRunner_ResumeFromCheckpoint(t *testing.T) {
	s := memory.New()
	reg := workflow.NewRegistry()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := workflow.NewRunner(reg, s, s, noopEmitter{}, logger)

	var step1Calls, step2Calls int
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("resume-wf", func(wf *workflow.Workflow, _ struct{}) error {
		if stepErr := wf.Step("step-1", func(_ context.Context) error {
			step1Calls++
			return nil
		}); stepErr != nil {
			return stepErr
		}
		return wf.Step("step-2", func(_ context.Context) error {
			step2Calls++
			return nil
		})
	}))

	// First run — both steps execute.
	run, err := workflow.Start(context.Background(), runner, "resume-wf", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if step1Calls != 1 {
		t.Errorf("step1Calls = %d, want 1", step1Calls)
	}
	if step2Calls != 1 {
		t.Errorf("step2Calls = %d, want 1", step2Calls)
	}

	// Reset counters and change state to running (simulate crash).
	step1Calls = 0
	step2Calls = 0
	run.State = workflow.RunStateRunning
	run.CompletedAt = nil
	if updateErr := s.UpdateRun(context.Background(), run); updateErr != nil {
		t.Fatalf("UpdateRun: %v", updateErr)
	}

	// Resume — step-1 should be skipped (has checkpoint), step-2 should also be skipped.
	if resumeErr := runner.Resume(context.Background(), run.ID); resumeErr != nil {
		t.Fatalf("Resume: %v", resumeErr)
	}

	if step1Calls != 0 {
		t.Errorf("step1Calls after resume = %d, want 0 (checkpointed)", step1Calls)
	}
	if step2Calls != 0 {
		t.Errorf("step2Calls after resume = %d, want 0 (checkpointed)", step2Calls)
	}
}

func TestRunner_ResumeAll(t *testing.T) {
	s := memory.New()
	reg := workflow.NewRegistry()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := workflow.NewRunner(reg, s, s, noopEmitter{}, logger)

	var calls int
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("resumeall-wf", func(wf *workflow.Workflow, _ struct{}) error {
		return wf.Step("step-1", func(_ context.Context) error {
			calls++
			return nil
		})
	}))

	// Create two runs in "running" state.
	run1, err := workflow.Start(context.Background(), runner, "resumeall-wf", struct{}{})
	if err != nil {
		t.Fatalf("Start 1: %v", err)
	}
	run2, err := workflow.Start(context.Background(), runner, "resumeall-wf", struct{}{})
	if err != nil {
		t.Fatalf("Start 2: %v", err)
	}

	// Set both to running (simulate crash mid-execution).
	for _, run := range []*workflow.Run{run1, run2} {
		run.State = workflow.RunStateRunning
		run.CompletedAt = nil
		if updateErr := s.UpdateRun(context.Background(), run); updateErr != nil {
			t.Fatalf("UpdateRun: %v", updateErr)
		}
	}

	calls = 0
	if resumeErr := runner.ResumeAll(context.Background()); resumeErr != nil {
		t.Fatalf("ResumeAll: %v", resumeErr)
	}

	// Both runs were already checkpointed, so steps are skipped.
	if calls != 0 {
		t.Errorf("step calls after ResumeAll = %d, want 0 (all checkpointed)", calls)
	}
}

func TestRunner_ResumeNotRunning(t *testing.T) {
	runner, reg, _ := newTestRunner()

	workflow.RegisterDefinition(reg, workflow.NewWorkflow("completed-wf", func(_ *workflow.Workflow, _ struct{}) error {
		return nil
	}))

	run, err := workflow.Start(context.Background(), runner, "completed-wf", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Run is completed, Resume should fail.
	resumeErr := runner.Resume(context.Background(), run.ID)
	if resumeErr == nil {
		t.Fatal("expected error resuming completed run")
	}
}
