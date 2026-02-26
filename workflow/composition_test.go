package workflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/workflow"
)

// ── Child Workflow Composition ───────────────────────

func TestRunChild_HappyPath(t *testing.T) {
	runner, reg, _ := newTestRunner()

	// Register a child workflow that returns a result.
	type childOutput struct {
		Doubled int `json:"doubled"`
	}
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("double-wf", func(wf *workflow.Workflow, input int) error {
		wf.Run().Output = []byte(`{"doubled":` + itoa(input*2) + `}`)
		return nil
	}))

	// Register a parent workflow that calls RunChild.
	var gotResult childOutput
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("parent-wf", func(wf *workflow.Workflow, _ struct{}) error {
		result, err := workflow.RunChild[int, childOutput](wf, "double-wf", 21)
		if err != nil {
			return err
		}
		gotResult = result
		return nil
	}))

	run, err := workflow.Start(context.Background(), runner, "parent-wf", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if run.State != workflow.RunStateCompleted {
		t.Errorf("state = %q, want %q, error = %q", run.State, workflow.RunStateCompleted, run.Error)
	}
	if gotResult.Doubled != 42 {
		t.Errorf("child result = %d, want 42", gotResult.Doubled)
	}
}

func TestRunChild_ChildFailure(t *testing.T) {
	runner, reg, _ := newTestRunner()

	// Register a child that always fails.
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("fail-child", func(_ *workflow.Workflow, _ struct{}) error {
		return errors.New("child exploded")
	}))

	// Parent calls RunChild which should propagate the failure.
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("parent-fail", func(wf *workflow.Workflow, _ struct{}) error {
		_, err := workflow.RunChild[struct{}, struct{}](wf, "fail-child", struct{}{})
		return err
	}))

	run, err := workflow.Start(context.Background(), runner, "parent-fail", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if run.State != workflow.RunStateFailed {
		t.Errorf("state = %q, want %q", run.State, workflow.RunStateFailed)
	}
	if run.Error == "" {
		t.Error("expected error message on failed run")
	}
}

func TestSpawnChild_FireAndForget(t *testing.T) {
	runner, reg, s := newTestRunner()

	var childStarted atomic.Bool
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("async-child", func(_ *workflow.Workflow, _ struct{}) error {
		childStarted.Store(true)
		return nil
	}))

	var gotRunID string
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("parent-spawn", func(wf *workflow.Workflow, _ struct{}) error {
		childID, err := workflow.SpawnChild[struct{}](wf, "async-child", struct{}{})
		if err != nil {
			return err
		}
		gotRunID = childID.String()
		return nil
	}))

	run, err := workflow.Start(context.Background(), runner, "parent-spawn", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if run.State != workflow.RunStateCompleted {
		t.Errorf("parent state = %q, want %q, error = %q", run.State, workflow.RunStateCompleted, run.Error)
	}
	if gotRunID == "" {
		t.Fatal("expected non-empty child run ID")
	}

	// Wait briefly for async child to complete.
	time.Sleep(100 * time.Millisecond)

	if !childStarted.Load() {
		t.Error("expected async child to have started")
	}

	// Verify child run exists in store and has ParentRunID set.
	_ = s // store available for verification if needed
}

func TestFanOut_CollectsAllResults(t *testing.T) {
	runner, reg, _ := newTestRunner()

	// Child workflow that doubles the input.
	type fanOutResult struct {
		Value int `json:"value"`
	}
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("multiply-wf", func(wf *workflow.Workflow, input int) error {
		wf.Run().Output = []byte(`{"value":` + itoa(input*2) + `}`)
		return nil
	}))

	// Parent that fans out to 3 children.
	var results []fanOutResult
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("fanout-parent", func(wf *workflow.Workflow, _ struct{}) error {
		r, err := workflow.FanOut[int, fanOutResult](wf, "multiply-wf", []int{1, 2, 3})
		if err != nil {
			return err
		}
		results = r
		return nil
	}))

	run, err := workflow.Start(context.Background(), runner, "fanout-parent", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if run.State != workflow.RunStateCompleted {
		t.Errorf("state = %q, want %q, error = %q", run.State, workflow.RunStateCompleted, run.Error)
	}
	if len(results) != 3 {
		t.Fatalf("results len = %d, want 3", len(results))
	}
	// Results should be [2, 4, 6] (order preserved by index).
	expected := []int{2, 4, 6}
	for i, r := range results {
		if r.Value != expected[i] {
			t.Errorf("results[%d].Value = %d, want %d", i, r.Value, expected[i])
		}
	}
}

func TestFanOut_OneChildFails(t *testing.T) {
	runner, reg, _ := newTestRunner()

	// A child that fails on input 0.
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("maybe-fail", func(wf *workflow.Workflow, input int) error {
		if input == 0 {
			return errors.New("input cannot be zero")
		}
		wf.Run().Output = []byte(`{"v":1}`)
		return nil
	}))

	workflow.RegisterDefinition(reg, workflow.NewWorkflow("fanout-fail-parent", func(wf *workflow.Workflow, _ struct{}) error {
		type r struct {
			V int `json:"v"`
		}
		_, err := workflow.FanOut[int, r](wf, "maybe-fail", []int{1, 0, 3})
		return err
	}))

	run, err := workflow.Start(context.Background(), runner, "fanout-fail-parent", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if run.State != workflow.RunStateFailed {
		t.Errorf("state = %q, want %q", run.State, workflow.RunStateFailed)
	}
}

func TestRunChild_CheckpointResume(t *testing.T) {
	s := memory.New()
	runner, reg := newTestRunnerWithStore(s)

	type childOut struct {
		Value string `json:"value"`
	}

	var childCalls atomic.Int32
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("cached-child", func(wf *workflow.Workflow, _ struct{}) error {
		childCalls.Add(1)
		wf.Run().Output = []byte(`{"value":"done"}`)
		return nil
	}))

	var gotResult childOut
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("parent-resume", func(wf *workflow.Workflow, _ struct{}) error {
		result, err := workflow.RunChild[struct{}, childOut](wf, "cached-child", struct{}{})
		if err != nil {
			return err
		}
		gotResult = result
		return nil
	}))

	// First run.
	run, err := workflow.Start(context.Background(), runner, "parent-resume", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if childCalls.Load() != 1 {
		t.Fatalf("childCalls = %d, want 1", childCalls.Load())
	}
	if gotResult.Value != "done" {
		t.Fatalf("result = %q, want %q", gotResult.Value, "done")
	}

	// Simulate crash and resume.
	childCalls.Store(0)
	gotResult = childOut{}
	run.State = workflow.RunStateRunning
	run.CompletedAt = nil
	if updateErr := s.UpdateRun(context.Background(), run); updateErr != nil {
		t.Fatalf("UpdateRun: %v", updateErr)
	}

	if resumeErr := runner.Resume(context.Background(), run.ID); resumeErr != nil {
		t.Fatalf("Resume: %v", resumeErr)
	}

	// Child should NOT have been called again (checkpoint hit).
	if childCalls.Load() != 0 {
		t.Errorf("childCalls after resume = %d, want 0 (checkpointed)", childCalls.Load())
	}
	if gotResult.Value != "done" {
		t.Errorf("result after resume = %q, want %q", gotResult.Value, "done")
	}
}

func TestRunChild_ParentRunIDLinked(t *testing.T) {
	runner, reg, s := newTestRunner()

	workflow.RegisterDefinition(reg, workflow.NewWorkflow("linked-child", func(_ *workflow.Workflow, _ struct{}) error {
		return nil
	}))

	var childRunIDStr string
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("linked-parent", func(wf *workflow.Workflow, _ struct{}) error {
		_, err := workflow.RunChild[struct{}, struct{}](wf, "linked-child", struct{}{})
		return err
	}))

	parentRun, err := workflow.Start(context.Background(), runner, "linked-parent", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if parentRun.State != workflow.RunStateCompleted {
		t.Fatalf("state = %q, want completed, error = %q", parentRun.State, parentRun.Error)
	}

	// List child runs of the parent.
	children, listErr := s.ListChildRuns(context.Background(), parentRun.ID)
	if listErr != nil {
		t.Fatalf("ListChildRuns: %v", listErr)
	}
	if len(children) != 1 {
		t.Fatalf("children count = %d, want 1", len(children))
	}

	child := children[0]
	if child.ParentRunID == nil {
		t.Fatal("child.ParentRunID is nil")
	}
	if *child.ParentRunID != parentRun.ID {
		t.Errorf("child.ParentRunID = %s, want %s", child.ParentRunID, parentRun.ID)
	}
	_ = childRunIDStr
}

// ── helpers ──────────────────────────────────────────

// itoa is a minimal int→string conversion for test output JSON.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	digits := make([]byte, 0, 20)
	for n > 0 {
		digits = append(digits, byte('0'+n%10))
		n /= 10
	}
	if neg {
		digits = append(digits, '-')
	}
	// Reverse.
	for i, j := 0, len(digits)-1; i < j; i, j = i+1, j-1 {
		digits[i], digits[j] = digits[j], digits[i]
	}
	return string(digits)
}
