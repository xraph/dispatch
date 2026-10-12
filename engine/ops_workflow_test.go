package engine_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/workflow"
)

// wfReplayRecorder records operator actions and reports finished
// workflow runs on ends.
type wfReplayRecorder struct {
	mu      sync.Mutex
	actions []ext.Action
	ends    chan workflow.RunState
}

func (r *wfReplayRecorder) Name() string { return "wf-replay-recorder" }

func (r *wfReplayRecorder) OnOperatorAction(_ context.Context, a ext.Action) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions = append(r.actions, a)
	return nil
}

func (r *wfReplayRecorder) OnWorkflowCompleted(_ context.Context, _ *workflow.Run, _ time.Duration) error {
	r.ends <- workflow.RunStateCompleted
	return nil
}

func (r *wfReplayRecorder) OnWorkflowFailed(_ context.Context, _ *workflow.Run, _ error) error {
	r.ends <- workflow.RunStateFailed
	return nil
}

func (r *wfReplayRecorder) recorded() []ext.Action {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.actions)
}

func (r *wfReplayRecorder) waitEnd(t *testing.T) workflow.RunState {
	t.Helper()
	select {
	case st := <-r.ends:
		return st
	case <-time.After(5 * time.Second):
		t.Fatal("no workflow run finished within 5s")
		return ""
	}
}

// replayEngine builds an engine on a memory store with a recorder and a
// two-step workflow "eng-replay" whose step-2 fails while fail is set
// and blocks on gate while gate is non-nil.
func replayEngine(t *testing.T, fail *atomic.Bool, gate chan struct{}, step2Calls *atomic.Int32) (*engine.Engine, *memory.Store, *wfReplayRecorder) {
	t.Helper()
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}
	rec := &wfReplayRecorder{ends: make(chan workflow.RunState, 16)}
	eng, err := engine.Build(d, engine.WithExtension(rec))
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}
	engine.RegisterWorkflow(eng, workflow.NewWorkflow("eng-replay", func(wf *workflow.Workflow, _ struct{}) error {
		if err := wf.Step("step-1", func(_ context.Context) error {
			time.Sleep(time.Millisecond) // keep checkpoint times apart
			return nil
		}); err != nil {
			return err
		}
		return wf.Step("step-2", func(_ context.Context) error {
			step2Calls.Add(1)
			if gate != nil {
				<-gate
			}
			if fail.Load() {
				return errors.New("step-2 boom")
			}
			return nil
		})
	}))
	return eng, s, rec
}

func TestEngine_ReplayWorkflowFrom_EmitsOneAction(t *testing.T) {
	var fail atomic.Bool
	var step2 atomic.Int32
	eng, s, rec := replayEngine(t, &fail, nil, &step2)
	t.Cleanup(func() { _ = eng.Stop(context.Background()) })

	fail.Store(true)
	run, err := engine.StartWorkflow(context.Background(), eng, "eng-replay", struct{}{})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	if st := rec.waitEnd(t); st != workflow.RunStateFailed {
		t.Fatalf("first run ended %q, want failed", st)
	}

	plan, err := eng.PlanWorkflowReplay(context.Background(), run.ID, "step-1")
	if err != nil {
		t.Fatalf("PlanWorkflowReplay: %v", err)
	}
	if plan.Version != 1 || plan.State != workflow.RunStateFailed || len(plan.Reruns) != 0 {
		t.Fatalf("plan = %+v, want version 1, state failed, no checkpointed reruns", plan)
	}
	if got := rec.recorded(); len(got) != 0 {
		t.Fatalf("PlanWorkflowReplay emitted %d actions, want 0", len(got))
	}

	fail.Store(false)
	ctx := ext.WithActor(context.Background(), "user_7")
	got, err := eng.ReplayWorkflowFrom(ctx, run.ID, "step-1")
	if err != nil {
		t.Fatalf("ReplayWorkflowFrom: %v", err)
	}
	if got.RunID != run.ID || got.FromStep != "step-1" {
		t.Errorf("plan = %+v, want run %s from step-1", got, run.ID)
	}
	if st := rec.waitEnd(t); st != workflow.RunStateCompleted {
		t.Fatalf("replay ended %q, want completed", st)
	}
	if step2.Load() != 2 {
		t.Errorf("step-2 calls = %d, want 2", step2.Load())
	}

	actions := rec.recorded()
	if len(actions) != 1 {
		t.Fatalf("actions = %d, want 1: %+v", len(actions), actions)
	}
	a := actions[0]
	if a.Kind != ext.ActionWorkflowReplayed || a.RunID != run.ID || a.Step != "step-1" || a.Actor != "user_7" || a.At.IsZero() {
		t.Errorf("action = %+v, want workflow.replayed of %s from step-1 by user_7 with a time", a, run.ID)
	}

	after, err := s.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if after.State != workflow.RunStateCompleted {
		t.Errorf("run state = %q, want completed", after.State)
	}
}

func TestEngine_ReplayWorkflowFrom_RefusalEmitsNothing(t *testing.T) {
	var fail atomic.Bool
	var step2 atomic.Int32
	eng, _, rec := replayEngine(t, &fail, nil, &step2)
	t.Cleanup(func() { _ = eng.Stop(context.Background()) })

	fail.Store(true)
	run, err := engine.StartWorkflow(context.Background(), eng, "eng-replay", struct{}{})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	rec.waitEnd(t)

	if _, err := eng.ReplayWorkflowFrom(context.Background(), run.ID, "step-2"); !errors.Is(err, dispatch.ErrInvalidState) {
		t.Fatalf("replay from an uncheckpointed step: err = %v, want ErrInvalidState", err)
	}
	if got := rec.recorded(); len(got) != 0 {
		t.Errorf("a refused replay emitted %d actions, want 0", len(got))
	}
}

func TestEngine_StopWaitsForReplay(t *testing.T) {
	var fail atomic.Bool
	var step2 atomic.Int32
	gate := make(chan struct{})
	eng, s, rec := replayEngine(t, &fail, gate, &step2)

	fail.Store(true)
	close(gate)
	run, err := engine.StartWorkflow(context.Background(), eng, "eng-replay", struct{}{})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	rec.waitEnd(t)

	// Hold the replay inside step-2.
	held := make(chan struct{})
	var heldCalls atomic.Int32
	engine.RegisterWorkflow(eng, workflow.NewWorkflow("eng-replay", func(wf *workflow.Workflow, _ struct{}) error {
		if stepErr := wf.Step("step-1", func(_ context.Context) error { return nil }); stepErr != nil {
			return stepErr
		}
		return wf.Step("step-2", func(_ context.Context) error {
			heldCalls.Add(1)
			<-held
			return nil
		})
	}))
	if _, replayErr := eng.ReplayWorkflowFrom(context.Background(), run.ID, "step-1"); replayErr != nil {
		t.Fatalf("ReplayWorkflowFrom: %v", replayErr)
	}
	deadline := time.Now().Add(5 * time.Second)
	for heldCalls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("replay never reached step-2")
		}
		time.Sleep(5 * time.Millisecond)
	}

	stopped := make(chan error, 1)
	go func() { stopped <- eng.Stop(context.Background()) }()
	select {
	case early := <-stopped:
		t.Fatalf("Stop returned (%v) while a replay was in flight", early)
	case <-time.After(100 * time.Millisecond):
	}

	close(held)
	select {
	case stopErr := <-stopped:
		if stopErr != nil {
			t.Fatalf("Stop: %v", stopErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return after the replay finished")
	}
	if st := rec.waitEnd(t); st != workflow.RunStateCompleted {
		t.Fatalf("replay ended %q, want completed", st)
	}

	// After Stop a replay is refused and nothing is emitted for it.
	before := len(rec.recorded())
	if _, lateErr := eng.ReplayWorkflowFrom(context.Background(), run.ID, "step-1"); !errors.Is(lateErr, workflow.ErrRunnerShutdown) {
		t.Fatalf("ReplayWorkflowFrom after Stop: err = %v, want ErrRunnerShutdown", lateErr)
	}
	if got := len(rec.recorded()); got != before {
		t.Errorf("a refused replay emitted an action (%d -> %d)", before, got)
	}
	after, err := s.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if after.State != workflow.RunStateCompleted {
		t.Errorf("run state = %q, want completed", after.State)
	}
}
