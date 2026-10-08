package api_test

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/workflow"
)

// replayFixture registers "api-replay": step-1 always passes, step-2
// fails while fail is set and waits on gate while gate is open.
type replayFixture struct {
	*fixture
	fail atomic.Bool
	gate atomic.Pointer[chan struct{}]
}

func newReplayFixture(t *testing.T) *replayFixture {
	t.Helper()

	rf := &replayFixture{fixture: newFixture(t)}
	engine.RegisterWorkflow(rf.eng, workflow.NewWorkflow("api-replay", func(wf *workflow.Workflow, _ struct{}) error {
		if err := wf.Step("step-1", func(_ context.Context) error {
			time.Sleep(time.Millisecond) // keep checkpoint times apart
			return nil
		}); err != nil {
			return err
		}
		return wf.Step("step-2", func(_ context.Context) error {
			if g := rf.gate.Load(); g != nil {
				<-*g
			}
			if rf.fail.Load() {
				return errors.New("step-2 boom")
			}
			return nil
		})
	}))

	return rf
}

// failedRun starts a run whose step-2 fails and waits for it to finish.
func (rf *replayFixture) failedRun(t *testing.T) *workflow.Run {
	t.Helper()

	rf.fail.Store(true)
	run, err := engine.StartWorkflow(context.Background(), rf.eng, "api-replay", struct{}{})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	rf.waitState(t, run.ID, workflow.RunStateFailed)
	rf.fail.Store(false)

	return run
}

func (rf *replayFixture) waitState(t *testing.T, runID id.RunID, want workflow.RunState) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for {
		run, err := rf.s.GetRun(context.Background(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if run.State == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s is %s after 5s, want %s", runID, run.State, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestWorkflowReplayPlan(t *testing.T) {
	rf := newReplayFixture(t)
	run := rf.failedRun(t)
	path := "/v1/workflows/runs/" + run.ID.String() + "/replay"

	rec := rf.do(t, http.MethodGet, path+"?step=step-1", "")
	wantStatus(t, rec, http.StatusOK)
	plan := decode[workflow.ReplayPlan](t, rec)
	if plan.RunID != run.ID || plan.FromStep != "step-1" || plan.Version != 1 || plan.State != workflow.RunStateFailed || len(plan.Reruns) != 0 {
		t.Errorf("plan = %+v, want run %s from step-1, version 1, failed, no reruns", plan, run.ID)
	}

	// A plan changes nothing.
	got, err := rf.s.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.State != workflow.RunStateFailed {
		t.Errorf("planning moved the run to %s", got.State)
	}

	wantStatus(t, rf.do(t, http.MethodGet, path, ""), http.StatusBadRequest)
	wantStatus(t, rf.do(t, http.MethodGet, path+"?step=step-2", ""), http.StatusConflict) // never checkpointed
	wantStatus(t, rf.do(t, http.MethodGet, "/v1/workflows/runs/"+id.NewRunID().String()+"/replay?step=step-1", ""), http.StatusNotFound)
	wantStatus(t, rf.do(t, http.MethodGet, "/v1/workflows/runs/nope/replay?step=step-1", ""), http.StatusBadRequest)
}

func TestWorkflowReplay(t *testing.T) {
	rf := newReplayFixture(t)
	run := rf.failedRun(t)
	path := "/v1/workflows/runs/" + run.ID.String() + "/replay"

	wantStatus(t, rf.do(t, http.MethodPost, path, ""), http.StatusBadRequest)
	wantStatus(t, rf.do(t, http.MethodPost, path, `{"step":""}`), http.StatusBadRequest)
	wantStatus(t, rf.do(t, http.MethodPost, path, `{"step":"step-2"}`), http.StatusConflict)
	wantStatus(t, rf.do(t, http.MethodPost, "/v1/workflows/runs/"+id.NewRunID().String()+"/replay", `{"step":"step-1"}`), http.StatusNotFound)

	rec := rf.do(t, http.MethodPost, path, `{"step":"step-1"}`)
	wantStatus(t, rec, http.StatusAccepted)
	if plan := decode[workflow.ReplayPlan](t, rec); plan.RunID != run.ID || plan.FromStep != "step-1" {
		t.Errorf("plan = %+v, want run %s from step-1", plan, run.ID)
	}
	rf.waitState(t, run.ID, workflow.RunStateCompleted)
}

func TestWorkflowReplay_RunningRunIsRefused(t *testing.T) {
	rf := newReplayFixture(t)
	run := rf.failedRun(t)
	path := "/v1/workflows/runs/" + run.ID.String() + "/replay"

	// Hold the first replay inside step-2, so the run is running.
	gate := make(chan struct{})
	rf.gate.Store(&gate)
	wantStatus(t, rf.do(t, http.MethodPost, path, `{"step":"step-1"}`), http.StatusAccepted)
	rf.waitState(t, run.ID, workflow.RunStateRunning)

	wantStatus(t, rf.do(t, http.MethodPost, path, `{"step":"step-1"}`), http.StatusConflict)

	close(gate)
	rf.waitState(t, run.ID, workflow.RunStateCompleted)
}

func TestWorkflowReplay_AfterStop(t *testing.T) {
	rf := newReplayFixture(t)
	run := rf.failedRun(t)

	if err := rf.eng.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	rec := rf.do(t, http.MethodPost, "/v1/workflows/runs/"+run.ID.String()+"/replay", `{"step":"step-1"}`)
	wantStatus(t, rec, http.StatusServiceUnavailable)
}
