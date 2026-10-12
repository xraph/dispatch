package workflow_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/workflow"
)

// endEmitter reports every run that finishes, so a test can wait for a
// replay running on the runner's background launcher.
type endEmitter struct {
	noopEmitter
	ends chan workflow.RunState
}

func (e endEmitter) EmitWorkflowCompleted(_ context.Context, _ *workflow.Run, _ time.Duration) {
	e.ends <- workflow.RunStateCompleted
}

func (e endEmitter) EmitWorkflowFailed(_ context.Context, _ *workflow.Run, _ error) {
	e.ends <- workflow.RunStateFailed
}

// newReplayRunner builds a runner on store st whose finished runs arrive
// on the returned channel. The runner is shut down when the test ends.
func newReplayRunner(t *testing.T, st workflow.Store, es *memory.Store) (*workflow.Runner, *workflow.Registry, chan workflow.RunState) {
	t.Helper()
	ends := make(chan workflow.RunState, 16)
	reg := workflow.NewRegistry()
	runner := workflow.NewRunner(reg, st, es, endEmitter{ends: ends}, testLogger())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runner.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})
	return runner, reg, ends
}

// waitEnd returns the end state of the next run to finish.
func waitEnd(t *testing.T, ends chan workflow.RunState) workflow.RunState {
	t.Helper()
	select {
	case st := <-ends:
		return st
	case <-time.After(5 * time.Second):
		t.Fatal("no run finished within 5s")
		return ""
	}
}

// stepCounts counts invocations of the three steps of the replay workflow.
type stepCounts struct {
	s1, s2, s3 atomic.Int32
}

// tick spaces the lifecycle fixture's checkpoints apart. Deterministic
// timestamp ties are covered by the checkpoint ordering suites.
func tick() { time.Sleep(time.Millisecond) }

// registerThreeSteps registers "replay-three" at version, with steps
// step-1..step-3. step-3 fails while failStep3 is set; step-2 blocks on
// gate when gate is non-nil.
func registerThreeSteps(reg *workflow.Registry, version int, c *stepCounts, failStep3 *atomic.Bool, gate chan struct{}) {
	workflow.RegisterDefinition(reg, workflow.NewWorkflowV("replay-three", version, func(wf *workflow.Workflow, _ struct{}) error {
		if err := wf.Step("step-1", func(_ context.Context) error { c.s1.Add(1); tick(); return nil }); err != nil {
			return err
		}
		if err := wf.Step("step-2", func(_ context.Context) error {
			c.s2.Add(1)
			if gate != nil {
				<-gate
			}
			tick()
			return nil
		}); err != nil {
			return err
		}
		return wf.Step("step-3", func(_ context.Context) error {
			c.s3.Add(1)
			tick()
			if failStep3.Load() {
				return errors.New("step-3 boom")
			}
			return nil
		})
	}))
}

// failedRun starts "replay-three" with step-3 failing, so the run ends
// failed with step-1 and step-2 checkpointed.
func failedRun(t *testing.T, runner *workflow.Runner, ends chan workflow.RunState, failStep3 *atomic.Bool) *workflow.Run {
	t.Helper()
	failStep3.Store(true)
	run, err := runner.StartRaw(context.Background(), "replay-three", []byte(`{}`))
	if err != nil {
		t.Fatalf("StartRaw: %v", err)
	}
	if st := waitEnd(t, ends); st != workflow.RunStateFailed {
		t.Fatalf("first run ended %q, want failed", st)
	}
	return run
}

func TestReplayFrom_RerunsOnlyLaterSteps(t *testing.T) {
	s := memory.New()
	runner, reg, ends := newReplayRunner(t, s, s)
	var c stepCounts
	var failStep3 atomic.Bool
	registerThreeSteps(reg, 1, &c, &failStep3, nil)
	run := failedRun(t, runner, ends, &failStep3)

	plan, err := runner.PlanReplay(context.Background(), run.ID, "step-1")
	if err != nil {
		t.Fatalf("PlanReplay: %v", err)
	}
	if plan.RunID != run.ID || plan.FromStep != "step-1" || plan.Version != 1 || plan.State != workflow.RunStateFailed {
		t.Fatalf("plan = %+v, want run %s from step-1 on version 1, state failed", plan, run.ID)
	}
	if !slices.Equal(plan.Reruns, []string{"step-2"}) {
		t.Fatalf("plan.Reruns = %v, want [step-2]", plan.Reruns)
	}

	failStep3.Store(false)
	got, err := runner.ReplayFrom(context.Background(), run.ID, "step-1")
	if err != nil {
		t.Fatalf("ReplayFrom: %v", err)
	}
	if !slices.Equal(got.Reruns, []string{"step-2"}) || got.Version != 1 {
		t.Fatalf("ReplayFrom plan = %+v, want reruns [step-2] on version 1", got)
	}
	if st := waitEnd(t, ends); st != workflow.RunStateCompleted {
		t.Fatalf("replay ended %q, want completed", st)
	}

	if c.s1.Load() != 1 || c.s2.Load() != 2 || c.s3.Load() != 2 {
		t.Errorf("step calls s1=%d s2=%d s3=%d, want 1/2/2 (step-1 kept, later steps re-run)",
			c.s1.Load(), c.s2.Load(), c.s3.Load())
	}
	after, err := s.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if after.State != workflow.RunStateCompleted || after.Error != "" || after.CompletedAt == nil {
		t.Errorf("run after replay: state=%q error=%q completed_at=%v, want completed, no error, set",
			after.State, after.Error, after.CompletedAt)
	}
}

func TestReplayFrom_RefusesRunningRun(t *testing.T) {
	s := memory.New()
	runner, reg, _ := newReplayRunner(t, s, s)
	var c stepCounts
	var failStep3 atomic.Bool
	registerThreeSteps(reg, 1, &c, &failStep3, nil)

	ctx := context.Background()
	run := &workflow.Run{
		Entity:    dispatch.NewEntity(),
		ID:        id.NewRunID(),
		Name:      "replay-three",
		State:     workflow.RunStateRunning,
		Version:   1,
		StartedAt: time.Now().UTC(),
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if err := s.SaveCheckpoint(ctx, run.ID, "step-1", []byte{}); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}

	plan, err := runner.PlanReplay(ctx, run.ID, "step-1")
	if err != nil {
		t.Fatalf("PlanReplay on a running run: %v (planning is read-only and should succeed)", err)
	}
	if plan.State != workflow.RunStateRunning {
		t.Errorf("plan.State = %q, want running", plan.State)
	}

	_, err = runner.ReplayFrom(ctx, run.ID, "step-1")
	if !errors.Is(err, dispatch.ErrInvalidState) {
		t.Fatalf("ReplayFrom on a running run: err = %v, want ErrInvalidState", err)
	}
	if !strings.Contains(err.Error(), "running") {
		t.Errorf("error %q does not name the state", err)
	}
	if c.s1.Load()+c.s2.Load()+c.s3.Load() != 0 {
		t.Errorf("a refused replay ran steps: s1=%d s2=%d s3=%d", c.s1.Load(), c.s2.Load(), c.s3.Load())
	}
}

func TestReplayFrom_ConcurrentCallsStartOnce(t *testing.T) {
	s := memory.New()
	runner, reg, ends := newReplayRunner(t, s, s)
	var c stepCounts
	var failStep3 atomic.Bool
	gate := make(chan struct{})
	registerThreeSteps(reg, 1, &c, &failStep3, gate)

	// The first run must get past step-2, so open the gate for it.
	close(gate)
	run := failedRun(t, runner, ends, &failStep3)

	// A fresh gate holds the winning replay inside step-2 while the
	// others race, so a loser can never find the run finished again.
	gate2 := make(chan struct{})
	var c2 stepCounts
	registerThreeSteps(reg, 1, &c2, &failStep3, gate2)
	failStep3.Store(false)

	const callers = 8
	var wg sync.WaitGroup
	var wins, refused atomic.Int32
	start := make(chan struct{})
	for range callers {
		wg.Go(func() {
			<-start
			_, err := runner.ReplayFrom(context.Background(), run.ID, "step-1")
			switch {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, dispatch.ErrInvalidState):
				refused.Add(1)
			default:
				t.Errorf("ReplayFrom: unexpected error %v", err)
			}
		})
	}
	close(start)
	wg.Wait()
	close(gate2)

	if wins.Load() != 1 || refused.Load() != callers-1 {
		t.Fatalf("wins=%d refused=%d, want 1 and %d", wins.Load(), refused.Load(), callers-1)
	}
	if st := waitEnd(t, ends); st != workflow.RunStateCompleted {
		t.Fatalf("replay ended %q, want completed", st)
	}
	if c2.s2.Load() != 1 || c2.s3.Load() != 1 {
		t.Errorf("replayed steps ran s2=%d s3=%d, want exactly once each", c2.s2.Load(), c2.s3.Load())
	}
}

func TestReplayFrom_UsesStampedVersion(t *testing.T) {
	s := memory.New()
	runner, reg, ends := newReplayRunner(t, s, s)
	var v1 stepCounts
	var failStep3 atomic.Bool
	registerThreeSteps(reg, 1, &v1, &failStep3, nil)
	run := failedRun(t, runner, ends, &failStep3)
	if run.Version != 1 {
		t.Fatalf("run.Version = %d, want 1", run.Version)
	}

	// v2 registers after the run started; the replay must stay on v1.
	var v2 stepCounts
	registerThreeSteps(reg, 2, &v2, &failStep3, nil)
	failStep3.Store(false)

	plan, err := runner.ReplayFrom(context.Background(), run.ID, "step-1")
	if err != nil {
		t.Fatalf("ReplayFrom: %v", err)
	}
	if plan.Version != 1 {
		t.Errorf("plan.Version = %d, want 1", plan.Version)
	}
	if st := waitEnd(t, ends); st != workflow.RunStateCompleted {
		t.Fatalf("replay ended %q, want completed", st)
	}
	if v1.s2.Load() != 2 || v1.s3.Load() != 2 {
		t.Errorf("v1 steps s2=%d s3=%d, want 2/2 (replay on v1)", v1.s2.Load(), v1.s3.Load())
	}
	if v2.s1.Load()+v2.s2.Load()+v2.s3.Load() != 0 {
		t.Errorf("v2 handler ran: s1=%d s2=%d s3=%d, want none", v2.s1.Load(), v2.s2.Load(), v2.s3.Load())
	}
}

// storedRun writes a finished run of "replay-three" stamped with version,
// with a checkpoint for step-1, straight to the store.
func storedRun(t *testing.T, s *memory.Store, version int) *workflow.Run {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	run := &workflow.Run{
		Entity:      dispatch.NewEntity(),
		ID:          id.NewRunID(),
		Name:        "replay-three",
		State:       workflow.RunStateFailed,
		Error:       "earlier failure",
		Version:     version,
		StartedAt:   now,
		CompletedAt: &now,
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if err := s.SaveCheckpoint(ctx, run.ID, "step-1", []byte{}); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	return run
}

func TestReplayFrom_RefusesUnregisteredVersion(t *testing.T) {
	s := memory.New()
	runner, reg, _ := newReplayRunner(t, s, s)
	var c stepCounts
	var failStep3 atomic.Bool
	registerThreeSteps(reg, 1, &c, &failStep3, nil)
	run := storedRun(t, s, 3)

	for name, call := range map[string]func() error{
		"PlanReplay": func() error {
			_, err := runner.PlanReplay(context.Background(), run.ID, "step-1")
			return err
		},
		"ReplayFrom": func() error {
			_, err := runner.ReplayFrom(context.Background(), run.ID, "step-1")
			return err
		},
	} {
		err := call()
		if !errors.Is(err, dispatch.ErrInvalidState) {
			t.Fatalf("%s: err = %v, want ErrInvalidState", name, err)
		}
		if !strings.Contains(err.Error(), "version 3") {
			t.Errorf("%s: error %q does not name the version", name, err)
		}
	}

	after, err := s.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if after.State != workflow.RunStateFailed {
		t.Errorf("refused replay moved the run to %q", after.State)
	}
}

func TestReplayFrom_UnversionedRunUsesVersionOne(t *testing.T) {
	s := memory.New()
	runner, reg, ends := newReplayRunner(t, s, s)
	var v1, v2 stepCounts
	var failStep3 atomic.Bool
	registerThreeSteps(reg, 1, &v1, &failStep3, nil)
	registerThreeSteps(reg, 2, &v2, &failStep3, nil)
	run := storedRun(t, s, 0)

	plan, err := runner.ReplayFrom(context.Background(), run.ID, "step-1")
	if err != nil {
		t.Fatalf("ReplayFrom: %v", err)
	}
	if plan.Version != 1 {
		t.Errorf("plan.Version = %d, want 1 (Run.Version 0 means version 1)", plan.Version)
	}
	if st := waitEnd(t, ends); st != workflow.RunStateCompleted {
		t.Fatalf("replay ended %q, want completed", st)
	}
	if v1.s2.Load() != 1 || v2.s2.Load() != 0 {
		t.Errorf("v1 s2=%d v2 s2=%d, want the replay on v1 only", v1.s2.Load(), v2.s2.Load())
	}
}

func TestReplayFrom_RefusesStepWithoutCheckpoint(t *testing.T) {
	s := memory.New()
	runner, reg, ends := newReplayRunner(t, s, s)
	var c stepCounts
	var failStep3 atomic.Bool
	registerThreeSteps(reg, 1, &c, &failStep3, nil)
	run := failedRun(t, runner, ends, &failStep3)

	// step-3 failed, so it has no checkpoint; "nope" never existed.
	for _, step := range []string{"step-3", "nope"} {
		_, err := runner.PlanReplay(context.Background(), run.ID, step)
		if !errors.Is(err, dispatch.ErrInvalidState) {
			t.Fatalf("PlanReplay(%q): err = %v, want ErrInvalidState", step, err)
		}
		if !strings.Contains(err.Error(), step) {
			t.Errorf("PlanReplay(%q): error %q does not name the step", step, err)
		}
		if _, err := runner.ReplayFrom(context.Background(), run.ID, step); !errors.Is(err, dispatch.ErrInvalidState) {
			t.Fatalf("ReplayFrom(%q): err = %v, want ErrInvalidState", step, err)
		}
	}

	_, err := runner.PlanReplay(context.Background(), id.NewRunID(), "step-1")
	if !errors.Is(err, dispatch.ErrRunNotFound) {
		t.Errorf("PlanReplay on an unknown run: err = %v, want ErrRunNotFound", err)
	}

	cps, err := s.ListCheckpoints(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("ListCheckpoints: %v", err)
	}
	if len(cps) != 2 {
		t.Errorf("checkpoints after refused replays = %d, want 2 (untouched)", len(cps))
	}
}

func TestShutdown_WaitsForInFlightReplay(t *testing.T) {
	s := memory.New()
	runner, reg, ends := newReplayRunner(t, s, s)
	var c stepCounts
	var failStep3 atomic.Bool
	gate := make(chan struct{})
	registerThreeSteps(reg, 1, &c, &failStep3, gate)
	close(gate)
	run := failedRun(t, runner, ends, &failStep3)

	held := make(chan struct{})
	var held2 stepCounts
	registerThreeSteps(reg, 1, &held2, &failStep3, held)
	failStep3.Store(false)

	if _, err := runner.ReplayFrom(context.Background(), run.ID, "step-1"); err != nil {
		t.Fatalf("ReplayFrom: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for held2.s2.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("replay never reached step-2")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// A Shutdown whose context expires first reports it and returns.
	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := runner.Shutdown(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown with a short deadline: err = %v, want DeadlineExceeded", err)
	}

	done := make(chan error, 1)
	go func() { done <- runner.Shutdown(context.Background()) }()
	select {
	case err := <-done:
		t.Fatalf("Shutdown returned (%v) while a replay was still running", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(held)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not return after the replay finished")
	}
	if st := waitEnd(t, ends); st != workflow.RunStateCompleted {
		t.Fatalf("replay ended %q, want completed", st)
	}

	// After Shutdown a replay is refused before it touches the run.
	_, err := runner.ReplayFrom(context.Background(), run.ID, "step-1")
	if !errors.Is(err, workflow.ErrRunnerShutdown) {
		t.Fatalf("ReplayFrom after Shutdown: err = %v, want ErrRunnerShutdown", err)
	}
	after, err := s.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if after.State != workflow.RunStateCompleted {
		t.Errorf("refused replay moved the run to %q", after.State)
	}
	cps, err := s.ListCheckpoints(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("ListCheckpoints: %v", err)
	}
	if len(cps) != 3 {
		t.Errorf("checkpoints after refused replay = %d, want 3 (untouched)", len(cps))
	}
}

func TestShutdown_CancelsReplayContext(t *testing.T) {
	s := memory.New()
	runner, reg, _ := newReplayRunner(t, s, s)
	run := storedRun(t, s, 1)

	entered := make(chan struct{})
	var sawCancel atomic.Bool
	workflow.RegisterDefinition(reg, workflow.NewWorkflowV("replay-three", 1, func(wf *workflow.Workflow, _ struct{}) error {
		if err := wf.Step("step-1", func(_ context.Context) error { return nil }); err != nil {
			return err
		}
		return wf.Step("step-2", func(ctx context.Context) error {
			close(entered)
			<-ctx.Done()
			sawCancel.Store(true)
			return ctx.Err()
		})
	}))

	if _, err := runner.ReplayFrom(context.Background(), run.ID, "step-1"); err != nil {
		t.Fatalf("ReplayFrom: %v", err)
	}
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runner.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if !sawCancel.Load() {
		t.Error("the replay's step did not see its context cancelled by Shutdown")
	}
}

// failingDeleteStore is a memory store whose DeleteCheckpointsAfter fails,
// which strands a run ReplayFrom has already reopened.
type failingDeleteStore struct {
	*memory.Store
}

var errDeleteFailed = errors.New("delete checkpoints: disk on fire")

func (failingDeleteStore) DeleteCheckpointsAfter(_ context.Context, _ id.RunID, _ string) error {
	return errDeleteFailed
}

func TestReplayFrom_DeleteFailureFailsRunAgain(t *testing.T) {
	s := memory.New()
	runner, reg, _ := newReplayRunner(t, failingDeleteStore{s}, s)
	var c stepCounts
	var failStep3 atomic.Bool
	registerThreeSteps(reg, 1, &c, &failStep3, nil)
	run := storedRun(t, s, 1)

	_, err := runner.ReplayFrom(context.Background(), run.ID, "step-1")
	if !errors.Is(err, errDeleteFailed) {
		t.Fatalf("ReplayFrom: err = %v, want the delete failure", err)
	}

	after, err := s.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if after.State != workflow.RunStateFailed {
		t.Fatalf("run state = %q, want failed (not stranded in running)", after.State)
	}
	if !strings.Contains(after.Error, "replay not started") || !strings.Contains(after.Error, errDeleteFailed.Error()) {
		t.Errorf("run error = %q, want the replay failure recorded", after.Error)
	}
	if after.CompletedAt == nil {
		t.Error("run CompletedAt is nil, want it set again")
	}
	if c.s1.Load()+c.s2.Load()+c.s3.Load() != 0 {
		t.Errorf("steps ran after a failed replay: s1=%d s2=%d s3=%d", c.s1.Load(), c.s2.Load(), c.s3.Load())
	}
}
