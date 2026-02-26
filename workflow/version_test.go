package workflow_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/workflow"
)

// ── Workflow Versioning ─────────────────────────────

func TestVersionedRegistration(t *testing.T) {
	reg := workflow.NewRegistry()

	// Register v1.
	workflow.RegisterDefinition(reg, workflow.NewWorkflowV("versioned-wf", 1,
		func(_ *workflow.Workflow, _ struct{}) error { return nil },
	))

	// Register v2.
	workflow.RegisterDefinition(reg, workflow.NewWorkflowV("versioned-wf", 2,
		func(_ *workflow.Workflow, _ struct{}) error { return nil },
	))

	// Latest version should be 2.
	if v := reg.LatestVersion("versioned-wf"); v != 2 {
		t.Errorf("LatestVersion = %d, want 2", v)
	}

	// Get() returns latest (v2).
	r, ok := reg.Get("versioned-wf")
	if !ok {
		t.Fatal("expected Get to return runner")
	}
	if r == nil {
		t.Fatal("expected non-nil runner")
	}

	// GetVersion(v1) returns v1 runner.
	rv1, ok := reg.GetVersion("versioned-wf", 1)
	if !ok {
		t.Fatal("expected GetVersion(1) to return runner")
	}
	if rv1 == nil {
		t.Fatal("expected non-nil v1 runner")
	}

	// GetVersion(v2) returns v2 runner.
	rv2, ok := reg.GetVersion("versioned-wf", 2)
	if !ok {
		t.Fatal("expected GetVersion(2) to return runner")
	}
	if rv2 == nil {
		t.Fatal("expected non-nil v2 runner")
	}

	// GetVersion(v3) returns false.
	_, ok = reg.GetVersion("versioned-wf", 3)
	if ok {
		t.Error("expected GetVersion(3) to return false")
	}
}

func TestNewRunUsesLatestVersion(t *testing.T) {
	runner, reg, _ := newTestRunner()

	// Register v1.
	workflow.RegisterDefinition(reg, workflow.NewWorkflowV("auto-latest", 1,
		func(_ *workflow.Workflow, _ struct{}) error { return nil },
	))

	// Register v2.
	workflow.RegisterDefinition(reg, workflow.NewWorkflowV("auto-latest", 2,
		func(_ *workflow.Workflow, _ struct{}) error { return nil },
	))

	run, err := workflow.Start(context.Background(), runner, "auto-latest", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if run.Version != 2 {
		t.Errorf("run.Version = %d, want 2", run.Version)
	}
	if run.State != workflow.RunStateCompleted {
		t.Errorf("state = %q, want completed", run.State)
	}
}

func TestResumePreservesVersion(t *testing.T) {
	s := memory.New()
	runner, reg := newTestRunnerWithStore(s)

	var v1Calls, v2Calls atomic.Int32

	// Register v1.
	workflow.RegisterDefinition(reg, workflow.NewWorkflowV("resume-ver", 1,
		func(wf *workflow.Workflow, _ struct{}) error {
			v1Calls.Add(1)
			return wf.Step("s1", func(_ context.Context) error { return nil })
		},
	))

	// Start a run on v1.
	run, err := workflow.Start(context.Background(), runner, "resume-ver", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if run.Version != 1 {
		t.Fatalf("run.Version = %d, want 1", run.Version)
	}
	if v1Calls.Load() != 1 {
		t.Fatalf("v1Calls = %d, want 1", v1Calls.Load())
	}

	// Now register v2 (v1 still exists).
	workflow.RegisterDefinition(reg, workflow.NewWorkflowV("resume-ver", 2,
		func(wf *workflow.Workflow, _ struct{}) error {
			v2Calls.Add(1)
			return wf.Step("s1", func(_ context.Context) error { return nil })
		},
	))

	// Simulate crash: reset run state.
	v1Calls.Store(0)
	v2Calls.Store(0)
	run.State = workflow.RunStateRunning
	run.CompletedAt = nil
	if updateErr := s.UpdateRun(context.Background(), run); updateErr != nil {
		t.Fatalf("UpdateRun: %v", updateErr)
	}

	// Resume should use v1 (the version stamped on the run).
	if resumeErr := runner.Resume(context.Background(), run.ID); resumeErr != nil {
		t.Fatalf("Resume: %v", resumeErr)
	}

	if v1Calls.Load() != 1 {
		t.Errorf("v1Calls after resume = %d, want 1 (should use v1)", v1Calls.Load())
	}
	if v2Calls.Load() != 0 {
		t.Errorf("v2Calls after resume = %d, want 0 (should NOT use v2)", v2Calls.Load())
	}
}

func TestMigrateRun(t *testing.T) {
	s := memory.New()
	runner, reg := newTestRunnerWithStore(s)

	var v1Calls, v2Calls atomic.Int32

	// Register ONLY v1 first — so Start picks v1 as the latest.
	workflow.RegisterDefinition(reg, workflow.NewWorkflowV("migrate-wf", 1,
		func(wf *workflow.Workflow, _ struct{}) error {
			v1Calls.Add(1)
			return wf.Step("s1", func(_ context.Context) error { return nil })
		},
	))

	// Start on v1 (only version registered).
	run, err := workflow.Start(context.Background(), runner, "migrate-wf", struct{}{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if run.Version != 1 {
		t.Fatalf("run.Version = %d, want 1", run.Version)
	}
	if v1Calls.Load() != 1 {
		t.Fatalf("v1Calls = %d, want 1", v1Calls.Load())
	}

	// Now register v2 with steps "s1" + "s2-new".
	workflow.RegisterDefinition(reg, workflow.NewWorkflowV("migrate-wf", 2,
		func(wf *workflow.Workflow, _ struct{}) error {
			v2Calls.Add(1)
			if err := wf.Step("s1", func(_ context.Context) error { return nil }); err != nil {
				return err
			}
			return wf.Step("s2-new", func(_ context.Context) error { return nil })
		},
	))

	// Reset counters.
	v1Calls.Store(0)
	v2Calls.Store(0)

	// Migrate to v2 — checkpoints for "s1" are preserved (skip),
	// but "s2-new" is a new step that will execute.
	if migrateErr := runner.MigrateRun(context.Background(), run.ID, 2); migrateErr != nil {
		t.Fatalf("MigrateRun: %v", migrateErr)
	}

	// Verify v2 handler was used.
	if v2Calls.Load() != 1 {
		t.Errorf("v2Calls after migrate = %d, want 1", v2Calls.Load())
	}
	if v1Calls.Load() != 0 {
		t.Errorf("v1Calls after migrate = %d, want 0", v1Calls.Load())
	}

	// Verify run version updated.
	migrated, getErr := s.GetRun(context.Background(), run.ID)
	if getErr != nil {
		t.Fatalf("GetRun: %v", getErr)
	}
	if migrated.Version != 2 {
		t.Errorf("migrated.Version = %d, want 2", migrated.Version)
	}
	if migrated.State != workflow.RunStateCompleted {
		t.Errorf("migrated.State = %q, want completed", migrated.State)
	}
}

func TestDefaultVersionIsOne(t *testing.T) {
	reg := workflow.NewRegistry()

	// Register without explicit version (Version=0 in Definition).
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("default-ver",
		func(_ *workflow.Workflow, _ struct{}) error { return nil },
	))

	// Latest version should be 1 (default 0 → 1).
	if v := reg.LatestVersion("default-ver"); v != 1 {
		t.Errorf("LatestVersion = %d, want 1", v)
	}

	// GetVersion(1) should work.
	_, ok := reg.GetVersion("default-ver", 1)
	if !ok {
		t.Error("expected GetVersion(1) to return runner for default version")
	}
}
