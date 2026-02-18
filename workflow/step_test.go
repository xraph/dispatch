package workflow_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/workflow"
)

// trackingEmitter records step lifecycle events for test assertions.
type trackingEmitter struct {
	noopEmitter
	stepCompletedCount atomic.Int32
	stepFailedCount    atomic.Int32
}

func (e *trackingEmitter) EmitStepCompleted(_ context.Context, _ *workflow.Run, _ string, _ time.Duration) {
	e.stepCompletedCount.Add(1)
}

func (e *trackingEmitter) EmitStepFailed(_ context.Context, _ *workflow.Run, _ string, _ error) {
	e.stepFailedCount.Add(1)
}

func TestStep_HappyPath(t *testing.T) {
	s := memory.New()
	reg := workflow.NewRegistry()
	emitter := &trackingEmitter{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := workflow.NewRunner(reg, s, s, emitter, logger)

	var step1Done, step2Done bool
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("step-test", func(wf *workflow.Workflow, _ struct{}) error {
		if err := wf.Step("step-1", func(_ context.Context) error {
			step1Done = true
			return nil
		}); err != nil {
			return err
		}
		return wf.Step("step-2", func(_ context.Context) error {
			step2Done = true
			return nil
		})
	}))

	run, err := workflow.Start(context.Background(), runner, "step-test", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if !step1Done {
		t.Error("step-1 did not execute")
	}
	if !step2Done {
		t.Error("step-2 did not execute")
	}
	if run.State != workflow.RunStateCompleted {
		t.Errorf("state = %q, want %q", run.State, workflow.RunStateCompleted)
	}
	if emitter.stepCompletedCount.Load() != 2 {
		t.Errorf("step completed events = %d, want 2", emitter.stepCompletedCount.Load())
	}
}

func TestStep_CheckpointSkip(t *testing.T) {
	s := memory.New()
	reg := workflow.NewRegistry()
	emitter := &trackingEmitter{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := workflow.NewRunner(reg, s, s, emitter, logger)

	var calls int
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("checkpoint-test", func(wf *workflow.Workflow, _ struct{}) error {
		return wf.Step("idempotent-step", func(_ context.Context) error {
			calls++
			return nil
		})
	}))

	// First run.
	run, err := workflow.Start(context.Background(), runner, "checkpoint-test", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}

	// Simulate crash: reset to running.
	run.State = workflow.RunStateRunning
	run.CompletedAt = nil
	if updateErr := s.UpdateRun(context.Background(), run); updateErr != nil {
		t.Fatalf("UpdateRun: %v", updateErr)
	}

	// Resume — step should be skipped.
	if resumeErr := runner.Resume(context.Background(), run.ID); resumeErr != nil {
		t.Fatalf("Resume: %v", resumeErr)
	}
	if calls != 1 {
		t.Errorf("calls after resume = %d, want 1 (step should be skipped)", calls)
	}
}

func TestStep_Failure(t *testing.T) {
	s := memory.New()
	reg := workflow.NewRegistry()
	emitter := &trackingEmitter{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := workflow.NewRunner(reg, s, s, emitter, logger)

	workflow.RegisterDefinition(reg, workflow.NewWorkflow("fail-step-test", func(wf *workflow.Workflow, _ struct{}) error {
		return wf.Step("bad-step", func(_ context.Context) error {
			return errors.New("step failed")
		})
	}))

	run, err := workflow.Start(context.Background(), runner, "fail-step-test", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if run.State != workflow.RunStateFailed {
		t.Errorf("state = %q, want %q", run.State, workflow.RunStateFailed)
	}
	if emitter.stepFailedCount.Load() != 1 {
		t.Errorf("step failed events = %d, want 1", emitter.stepFailedCount.Load())
	}
}

func TestStepWithResult_RoundTrip(t *testing.T) {
	s := memory.New()
	reg := workflow.NewRegistry()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := workflow.NewRunner(reg, s, s, noopEmitter{}, logger)

	type result struct {
		Value string
		Count int
	}

	var gotResult result
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("result-test", func(wf *workflow.Workflow, _ struct{}) error {
		r, err := workflow.StepWithResult[result](wf, "compute", func(_ context.Context) (result, error) {
			return result{Value: "hello", Count: 42}, nil
		})
		if err != nil {
			return err
		}
		gotResult = r
		return nil
	}))

	run, err := workflow.Start(context.Background(), runner, "result-test", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if run.State != workflow.RunStateCompleted {
		t.Errorf("state = %q, want %q", run.State, workflow.RunStateCompleted)
	}
	if gotResult.Value != "hello" {
		t.Errorf("Value = %q, want %q", gotResult.Value, "hello")
	}
	if gotResult.Count != 42 {
		t.Errorf("Count = %d, want %d", gotResult.Count, 42)
	}
}

func TestStepWithResult_CheckpointResume(t *testing.T) {
	s := memory.New()
	reg := workflow.NewRegistry()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := workflow.NewRunner(reg, s, s, noopEmitter{}, logger)

	var computeCalls int
	var gotResult int
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("result-resume", func(wf *workflow.Workflow, _ struct{}) error {
		r, err := workflow.StepWithResult[int](wf, "compute", func(_ context.Context) (int, error) {
			computeCalls++
			return 999, nil
		})
		if err != nil {
			return err
		}
		gotResult = r
		return nil
	}))

	// First run.
	run, err := workflow.Start(context.Background(), runner, "result-resume", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if computeCalls != 1 {
		t.Fatalf("computeCalls = %d, want 1", computeCalls)
	}
	if gotResult != 999 {
		t.Fatalf("gotResult = %d, want 999", gotResult)
	}

	// Reset for resume.
	computeCalls = 0
	gotResult = 0
	run.State = workflow.RunStateRunning
	run.CompletedAt = nil
	if updateErr := s.UpdateRun(context.Background(), run); updateErr != nil {
		t.Fatalf("UpdateRun: %v", updateErr)
	}

	if resumeErr := runner.Resume(context.Background(), run.ID); resumeErr != nil {
		t.Fatalf("Resume: %v", resumeErr)
	}

	if computeCalls != 0 {
		t.Errorf("computeCalls after resume = %d, want 0 (checkpointed)", computeCalls)
	}
	if gotResult != 999 {
		t.Errorf("gotResult after resume = %d, want 999", gotResult)
	}
}

func TestParallel_AllSucceed(t *testing.T) {
	s := memory.New()
	reg := workflow.NewRegistry()
	emitter := &trackingEmitter{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := workflow.NewRunner(reg, s, s, emitter, logger)

	var a, b, c atomic.Bool
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("parallel-test", func(wf *workflow.Workflow, _ struct{}) error {
		return wf.Parallel("group-1",
			func(_ context.Context) error { a.Store(true); return nil },
			func(_ context.Context) error { b.Store(true); return nil },
			func(_ context.Context) error { c.Store(true); return nil },
		)
	}))

	run, err := workflow.Start(context.Background(), runner, "parallel-test", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if run.State != workflow.RunStateCompleted {
		t.Errorf("state = %q, want %q", run.State, workflow.RunStateCompleted)
	}
	if !a.Load() || !b.Load() || !c.Load() {
		t.Errorf("parallel steps: a=%v b=%v c=%v, want all true", a.Load(), b.Load(), c.Load())
	}
}

func TestParallel_Failure(t *testing.T) {
	s := memory.New()
	reg := workflow.NewRegistry()
	emitter := &trackingEmitter{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := workflow.NewRunner(reg, s, s, emitter, logger)

	workflow.RegisterDefinition(reg, workflow.NewWorkflow("parallel-fail", func(wf *workflow.Workflow, _ struct{}) error {
		return wf.Parallel("failing-group",
			func(_ context.Context) error { return nil },
			func(_ context.Context) error { return errors.New("step 2 failed") },
			func(_ context.Context) error { return nil },
		)
	}))

	run, err := workflow.Start(context.Background(), runner, "parallel-fail", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if run.State != workflow.RunStateFailed {
		t.Errorf("state = %q, want %q", run.State, workflow.RunStateFailed)
	}
}

func TestParallel_CheckpointSkip(t *testing.T) {
	s := memory.New()
	reg := workflow.NewRegistry()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := workflow.NewRunner(reg, s, s, noopEmitter{}, logger)

	var calls atomic.Int32
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("parallel-resume", func(wf *workflow.Workflow, _ struct{}) error {
		return wf.Parallel("group",
			func(_ context.Context) error { calls.Add(1); return nil },
			func(_ context.Context) error { calls.Add(1); return nil },
		)
	}))

	// First run.
	run, err := workflow.Start(context.Background(), runner, "parallel-resume", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2", calls.Load())
	}

	// Reset for resume.
	calls.Store(0)
	run.State = workflow.RunStateRunning
	run.CompletedAt = nil
	if updateErr := s.UpdateRun(context.Background(), run); updateErr != nil {
		t.Fatalf("UpdateRun: %v", updateErr)
	}

	if resumeErr := runner.Resume(context.Background(), run.ID); resumeErr != nil {
		t.Fatalf("Resume: %v", resumeErr)
	}
	if calls.Load() != 0 {
		t.Errorf("calls after resume = %d, want 0 (group checkpointed)", calls.Load())
	}
}

func TestSleep_CheckpointSkip(t *testing.T) {
	s := memory.New()
	reg := workflow.NewRegistry()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := workflow.NewRunner(reg, s, s, noopEmitter{}, logger)

	workflow.RegisterDefinition(reg, workflow.NewWorkflow("sleep-test", func(wf *workflow.Workflow, _ struct{}) error {
		// Very short sleep for testing.
		return wf.Sleep("brief", 1*time.Millisecond)
	}))

	// First run.
	run, err := workflow.Start(context.Background(), runner, "sleep-test", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if run.State != workflow.RunStateCompleted {
		t.Errorf("state = %q, want %q", run.State, workflow.RunStateCompleted)
	}

	// Verify checkpoint exists.
	data, chkErr := s.GetCheckpoint(context.Background(), run.ID, "sleep:brief")
	if chkErr != nil {
		t.Fatalf("GetCheckpoint: %v", chkErr)
	}
	if data == nil {
		t.Fatal("expected sleep checkpoint to exist")
	}

	// Resume should skip sleep.
	run.State = workflow.RunStateRunning
	run.CompletedAt = nil
	if updateErr := s.UpdateRun(context.Background(), run); updateErr != nil {
		t.Fatalf("UpdateRun: %v", updateErr)
	}

	start := time.Now()
	if resumeErr := runner.Resume(context.Background(), run.ID); resumeErr != nil {
		t.Fatalf("Resume: %v", resumeErr)
	}
	elapsed := time.Since(start)
	// On resume, sleep should be skipped instantly (well under 100ms).
	if elapsed > 100*time.Millisecond {
		t.Errorf("resumed sleep took %v, expected near-instant", elapsed)
	}
}
