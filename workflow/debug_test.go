package workflow_test

import (
	"bytes"
	"context"
	"encoding/gob"
	"testing"

	"github.com/xraph/dispatch/workflow"
)

// ── Time-Travel Debugging ───────────────────────────

func TestGetTimeline_OrderedSteps(t *testing.T) {
	runner, reg, _ := newTestRunner()

	workflow.RegisterDefinition(reg, workflow.NewWorkflow("timeline-wf", func(wf *workflow.Workflow, _ struct{}) error {
		if err := wf.Step("step-a", func(_ context.Context) error { return nil }); err != nil {
			return err
		}
		if err := wf.Step("step-b", func(_ context.Context) error { return nil }); err != nil {
			return err
		}
		return wf.Step("step-c", func(_ context.Context) error { return nil })
	}))

	run, err := workflow.Start(context.Background(), runner, "timeline-wf", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if run.State != workflow.RunStateCompleted {
		t.Fatalf("state = %q, want completed", run.State)
	}

	timeline, tlErr := runner.GetTimeline(context.Background(), run.ID)
	if tlErr != nil {
		t.Fatalf("GetTimeline: %v", tlErr)
	}
	if len(timeline) != 3 {
		t.Fatalf("timeline entries = %d, want 3", len(timeline))
	}

	// Verify ordering by step name (they ran sequentially so timestamps are ordered).
	expectedNames := []string{"step-a", "step-b", "step-c"}
	for i, entry := range timeline {
		if entry.StepName != expectedNames[i] {
			t.Errorf("timeline[%d].StepName = %q, want %q", i, entry.StepName, expectedNames[i])
		}
		if entry.CreatedAt.IsZero() {
			t.Errorf("timeline[%d].CreatedAt is zero", i)
		}
	}

	// Verify temporal ordering.
	for i := 1; i < len(timeline); i++ {
		if timeline[i].CreatedAt.Before(timeline[i-1].CreatedAt) {
			t.Errorf("timeline[%d] (%v) is before timeline[%d] (%v)",
				i, timeline[i].CreatedAt, i-1, timeline[i-1].CreatedAt)
		}
	}
}

func TestInspectStep_ReturnsCheckpointData(t *testing.T) {
	runner, reg, _ := newTestRunner()

	type computeResult struct {
		Score int
		Label string
	}

	workflow.RegisterDefinition(reg, workflow.NewWorkflow("inspect-wf", func(wf *workflow.Workflow, _ struct{}) error {
		_, err := workflow.StepWithResult[computeResult](wf, "compute", func(_ context.Context) (computeResult, error) {
			return computeResult{Score: 100, Label: "perfect"}, nil
		})
		return err
	}))

	run, err := workflow.Start(context.Background(), runner, "inspect-wf", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	data, inspErr := runner.InspectStep(context.Background(), run.ID, "compute")
	if inspErr != nil {
		t.Fatalf("InspectStep: %v", inspErr)
	}
	if data == nil {
		t.Fatal("expected non-nil checkpoint data")
	}

	// Decode the gob-encoded result.
	var result computeResult
	dec := gob.NewDecoder(bytes.NewReader(data))
	if decErr := dec.Decode(&result); decErr != nil {
		t.Fatalf("gob decode: %v", decErr)
	}
	if result.Score != 100 {
		t.Errorf("Score = %d, want 100", result.Score)
	}
	if result.Label != "perfect" {
		t.Errorf("Label = %q, want %q", result.Label, "perfect")
	}
}

func TestInspectStep_NotFound(t *testing.T) {
	runner, reg, _ := newTestRunner()

	workflow.RegisterDefinition(reg, workflow.NewWorkflow("inspect-missing", func(_ *workflow.Workflow, _ struct{}) error {
		return nil
	}))

	run, err := workflow.Start(context.Background(), runner, "inspect-missing", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	_, inspErr := runner.InspectStep(context.Background(), run.ID, "nonexistent")
	if inspErr == nil {
		t.Fatal("expected error for missing step")
	}
}
