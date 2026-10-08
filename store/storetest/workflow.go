package storetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/workflow"
)

// WorkflowStore is what the workflow suite requires: the base workflow
// store plus the reopen claim replay-from-step is built on.
type WorkflowStore interface {
	workflow.Store
	workflow.Reopener
}

// concurrentReopeners is how many goroutines race to reopen one run.
const concurrentReopeners = 16

// RunWorkflowSuite pins ReopenRun, the claim that makes replay-from-step
// safe, and the parent link child runs are found by.
//
// Run it with `go test -race`. ReopenRunConcurrentExactlyOneWinner proves
// the claim is atomic, and an unguarded read-then-write only loses that
// race reliably under the detector.
//
// newStore may return a shared store, so every case creates its own runs
// and never lists or counts anything but its own parent's children.
func RunWorkflowSuite(t *testing.T, newStore func(t *testing.T) WorkflowStore) {
	t.Helper()

	cases := []struct {
		name string
		fn   func(t *testing.T, s WorkflowStore)
	}{
		{"ReopenFailedRunClearsErrorAndCompletion", testReopenFailedRun},
		{"ReopenCompletedRun", testReopenCompletedRun},
		{"ReopenRunningRunIsRefused", testReopenRunningRun},
		{"ReopenRunUnknown", testReopenUnknownRun},
		{"ReopenRunConcurrentExactlyOneWinner", testReopenRunConcurrent},
		{"ParentRunIDRoundTripsAndListChildRuns", testParentRunIDAndChildren},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.fn(t, newStore(t))
		})
	}
}

// createSuiteRun stores a run in the given state with every field the
// reopen must leave alone set to something distinct. A finished run also
// gets an error and a completion time, the two fields reopen clears.
func createSuiteRun(t *testing.T, s WorkflowStore, state workflow.RunState) *workflow.Run {
	t.Helper()

	now := time.Now().UTC().Truncate(time.Millisecond)
	r := &workflow.Run{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewRunID(),
		Name:       "workflow-suite",
		State:      state,
		Input:      []byte(`{"order":7}`),
		ScopeAppID: "app_wf",
		ScopeOrgID: "org_wf",
		StartedAt:  now.Add(-time.Minute),
		Version:    3,
	}
	if state != workflow.RunStateRunning {
		completed := now
		r.Output = []byte(`{"partial":true}`)
		r.Error = "step charge failed"
		r.CompletedAt = &completed
	}

	if err := s.CreateRun(context.Background(), r); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	return mustGetRun(t, s, r.ID)
}

// mustGetRun reads a run and returns a copy of it, so a store that hands
// out its own pointer cannot change a snapshot behind the test.
func mustGetRun(t *testing.T, s WorkflowStore, runID id.RunID) *workflow.Run {
	t.Helper()

	got, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun(%s): %v", runID, err)
	}
	snapshot := *got

	return &snapshot
}

// assertReopened checks the three fields reopen writes and every field it
// must leave alone.
func assertReopened(t *testing.T, before, after *workflow.Run) {
	t.Helper()

	if after.State != workflow.RunStateRunning {
		t.Errorf("State = %s, want running", after.State)
	}
	if after.Error != "" {
		t.Errorf("Error = %q, want it cleared", after.Error)
	}
	if after.CompletedAt != nil {
		t.Errorf("CompletedAt = %v, want nil", *after.CompletedAt)
	}

	if after.Name != before.Name {
		t.Errorf("Name = %q, want %q", after.Name, before.Name)
	}
	if !bytes.Equal(after.Input, before.Input) {
		t.Errorf("Input = %q, want %q", after.Input, before.Input)
	}
	if !bytes.Equal(after.Output, before.Output) {
		t.Errorf("Output = %q, want %q", after.Output, before.Output)
	}
	if after.ScopeAppID != before.ScopeAppID || after.ScopeOrgID != before.ScopeOrgID {
		t.Errorf("scope = %q/%q, want %q/%q",
			after.ScopeAppID, after.ScopeOrgID, before.ScopeAppID, before.ScopeOrgID)
	}
	if !after.StartedAt.Equal(before.StartedAt) {
		t.Errorf("StartedAt = %v, want %v", after.StartedAt, before.StartedAt)
	}
	if after.Version != before.Version {
		t.Errorf("Version = %d, want %d", after.Version, before.Version)
	}
	if !after.CreatedAt.Equal(before.CreatedAt) {
		t.Errorf("CreatedAt = %v, want %v", after.CreatedAt, before.CreatedAt)
	}
}

func testReopenFailedRun(t *testing.T, s WorkflowStore) {
	before := createSuiteRun(t, s, workflow.RunStateFailed)

	if err := s.ReopenRun(context.Background(), before.ID); err != nil {
		t.Fatalf("ReopenRun(failed): %v", err)
	}

	assertReopened(t, before, mustGetRun(t, s, before.ID))
}

func testReopenCompletedRun(t *testing.T, s WorkflowStore) {
	before := createSuiteRun(t, s, workflow.RunStateCompleted)

	if err := s.ReopenRun(context.Background(), before.ID); err != nil {
		t.Fatalf("ReopenRun(completed): %v", err)
	}

	assertReopened(t, before, mustGetRun(t, s, before.ID))
}

func testReopenRunningRun(t *testing.T, s WorkflowStore) {
	before := createSuiteRun(t, s, workflow.RunStateRunning)

	err := s.ReopenRun(context.Background(), before.ID)
	if !errors.Is(err, dispatch.ErrInvalidState) {
		t.Fatalf("ReopenRun(running) error = %v, want one wrapping ErrInvalidState", err)
	}

	after := mustGetRun(t, s, before.ID)
	if after.State != workflow.RunStateRunning {
		t.Errorf("State = %s after a refused reopen, want running", after.State)
	}
}

func testReopenUnknownRun(t *testing.T, s WorkflowStore) {
	err := s.ReopenRun(context.Background(), id.NewRunID())
	if !errors.Is(err, dispatch.ErrRunNotFound) {
		t.Fatalf("ReopenRun(unknown) error = %v, want ErrRunNotFound", err)
	}
}

func testReopenRunConcurrent(t *testing.T, s WorkflowStore) {
	ctx := context.Background()

	for round := range concurrentRounds {
		run := createSuiteRun(t, s, workflow.RunStateFailed)

		res := raceAttempts(concurrentReopeners, dispatch.ErrInvalidState, func(int) error {
			return s.ReopenRun(ctx, run.ID)
		})
		res.assertOneWinner(t, fmt.Sprintf("round %d: concurrent ReopenRun", round))

		if got := mustGetRun(t, s, run.ID); got.State != workflow.RunStateRunning {
			t.Errorf("round %d: State = %s after the race, want running", round, got.State)
		}
	}
}

func testParentRunIDAndChildren(t *testing.T, s WorkflowStore) {
	ctx := context.Background()
	parent := createSuiteRun(t, s, workflow.RunStateRunning)

	parentID := parent.ID
	child := &workflow.Run{
		Entity:      dispatch.NewEntity(),
		ID:          id.NewRunID(),
		Name:        "workflow-suite-child",
		State:       workflow.RunStateRunning,
		StartedAt:   time.Now().UTC().Truncate(time.Millisecond),
		ParentRunID: &parentID,
	}
	if err := s.CreateRun(ctx, child); err != nil {
		t.Fatalf("CreateRun(child): %v", err)
	}

	if got := mustGetRun(t, s, parent.ID); got.ParentRunID != nil {
		t.Errorf("top-level run ParentRunID = %s, want nil", *got.ParentRunID)
	}

	got := mustGetRun(t, s, child.ID)
	if got.ParentRunID == nil {
		t.Fatalf("child ParentRunID = nil, want %s: the parent link was not stored", parent.ID)
	}
	if *got.ParentRunID != parent.ID {
		t.Errorf("child ParentRunID = %s, want %s", *got.ParentRunID, parent.ID)
	}

	children, err := s.ListChildRuns(ctx, parent.ID)
	if err != nil {
		t.Fatalf("ListChildRuns: %v", err)
	}
	if len(children) != 1 || children[0].ID != child.ID {
		ids := make([]string, len(children))
		for i, c := range children {
			ids[i] = c.ID.String()
		}
		t.Fatalf("ListChildRuns(parent) = %v, want only %s", ids, child.ID)
	}

	grandchildren, err := s.ListChildRuns(ctx, child.ID)
	if err != nil {
		t.Fatalf("ListChildRuns(child): %v", err)
	}
	if len(grandchildren) != 0 {
		t.Errorf("ListChildRuns(child) = %d runs, want none", len(grandchildren))
	}
}
