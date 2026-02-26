package workflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/workflow"
)

// ── Saga Compensations ──────────────────────────────

func TestStepWithCompensation_NoFailure(t *testing.T) {
	runner, reg, _ := newTestRunner()

	var comp1, comp2 atomic.Bool
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("saga-ok", func(wf *workflow.Workflow, _ struct{}) error {
		if err := wf.StepWithCompensation("step-1",
			func(_ context.Context) error { return nil },
			func(_ context.Context) error { comp1.Store(true); return nil },
		); err != nil {
			return err
		}
		return wf.StepWithCompensation("step-2",
			func(_ context.Context) error { return nil },
			func(_ context.Context) error { comp2.Store(true); return nil },
		)
	}))

	run, err := workflow.Start(context.Background(), runner, "saga-ok", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if run.State != workflow.RunStateCompleted {
		t.Errorf("state = %q, want %q", run.State, workflow.RunStateCompleted)
	}
	// Compensations should NOT have run since workflow succeeded.
	if comp1.Load() {
		t.Error("compensation 1 should not run on success")
	}
	if comp2.Load() {
		t.Error("compensation 2 should not run on success")
	}
}

func TestStepWithCompensation_ReverseOrder(t *testing.T) {
	runner, reg, _ := newTestRunner()

	var order []string
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("saga-reverse", func(wf *workflow.Workflow, _ struct{}) error {
		if err := wf.StepWithCompensation("reserve-inventory",
			func(_ context.Context) error { return nil },
			func(_ context.Context) error { order = append(order, "undo-inventory"); return nil },
		); err != nil {
			return err
		}
		if err := wf.StepWithCompensation("charge-payment",
			func(_ context.Context) error { return nil },
			func(_ context.Context) error { order = append(order, "undo-payment"); return nil },
		); err != nil {
			return err
		}
		// Third step fails, triggering compensations.
		return wf.StepWithCompensation("ship-order",
			func(_ context.Context) error { return errors.New("shipping unavailable") },
			func(_ context.Context) error { order = append(order, "undo-shipping"); return nil },
		)
	}))

	run, err := workflow.Start(context.Background(), runner, "saga-reverse", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if run.State != workflow.RunStateFailed {
		t.Errorf("state = %q, want %q", run.State, workflow.RunStateFailed)
	}

	// Compensations should run in reverse order.
	// Step 3 failed so its compensation is NOT registered.
	// Only steps 1 and 2 succeeded and are on the compensation stack.
	// Reverse order: step 2 first, then step 1.
	if len(order) != 2 {
		t.Fatalf("compensations executed = %d, want 2: %v", len(order), order)
	}
	if order[0] != "undo-payment" {
		t.Errorf("order[0] = %q, want %q", order[0], "undo-payment")
	}
	if order[1] != "undo-inventory" {
		t.Errorf("order[1] = %q, want %q", order[1], "undo-inventory")
	}
}

func TestStepWithResultAndCompensation(t *testing.T) {
	runner, reg, _ := newTestRunner()

	var compensated atomic.Bool
	var gotResult int
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("saga-result", func(wf *workflow.Workflow, _ struct{}) error {
		r, err := workflow.StepWithResultAndCompensation[int](wf, "compute",
			func(_ context.Context) (int, error) { return 42, nil },
			func(_ context.Context) error { compensated.Store(true); return nil },
		)
		if err != nil {
			return err
		}
		gotResult = r

		// Fail after the compensable step.
		return errors.New("later failure")
	}))

	run, err := workflow.Start(context.Background(), runner, "saga-result", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if run.State != workflow.RunStateFailed {
		t.Errorf("state = %q, want %q", run.State, workflow.RunStateFailed)
	}
	if gotResult != 42 {
		t.Errorf("result = %d, want 42", gotResult)
	}
	if !compensated.Load() {
		t.Error("expected compensation to run after later failure")
	}
}

func TestCompensation_CheckpointResume(t *testing.T) {
	s := memory.New()
	runner, reg := newTestRunnerWithStore(s)

	var compCalls atomic.Int32
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("saga-resume", func(wf *workflow.Workflow, _ struct{}) error {
		if err := wf.StepWithCompensation("step-1",
			func(_ context.Context) error { return nil },
			func(_ context.Context) error {
				compCalls.Add(1)
				return nil
			},
		); err != nil {
			return err
		}
		return errors.New("forced failure")
	}))

	// First run — compensation executes.
	run, err := workflow.Start(context.Background(), runner, "saga-resume", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if run.State != workflow.RunStateFailed {
		t.Fatalf("state = %q, want failed", run.State)
	}
	if compCalls.Load() != 1 {
		t.Fatalf("compCalls = %d, want 1", compCalls.Load())
	}

	// The compensation checkpoint should exist ("compensate:step-1").
	data, chkErr := s.GetCheckpoint(context.Background(), run.ID, "compensate:step-1")
	if chkErr != nil {
		t.Fatalf("GetCheckpoint: %v", chkErr)
	}
	if data == nil {
		t.Fatal("expected compensation checkpoint to exist")
	}
}

func TestCompensation_BestEffort(t *testing.T) {
	runner, reg, _ := newTestRunner()

	var comp1, comp2 atomic.Bool
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("saga-best-effort", func(wf *workflow.Workflow, _ struct{}) error {
		if err := wf.StepWithCompensation("step-1",
			func(_ context.Context) error { return nil },
			func(_ context.Context) error {
				comp1.Store(true)
				return nil
			},
		); err != nil {
			return err
		}
		if err := wf.StepWithCompensation("step-2",
			func(_ context.Context) error { return nil },
			func(_ context.Context) error {
				comp2.Store(true)
				return errors.New("compensation 2 failed")
			},
		); err != nil {
			return err
		}
		return errors.New("trigger compensations")
	}))

	run, err := workflow.Start(context.Background(), runner, "saga-best-effort", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if run.State != workflow.RunStateFailed {
		t.Errorf("state = %q, want %q", run.State, workflow.RunStateFailed)
	}

	// Both compensations should execute even if one fails (best effort).
	// Reverse order: comp2 (fails), then comp1 (succeeds).
	if !comp2.Load() {
		t.Error("compensation 2 should have run (even though it failed)")
	}
	if !comp1.Load() {
		t.Error("compensation 1 should have run (best effort continues after comp2 failure)")
	}
}
