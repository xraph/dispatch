package engine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/store/memory"
)

func TestTerminalStopObservesActiveDrainWithoutCancellingWork(t *testing.T) {
	s := &strictLifecycleStore{base: memory.New()}
	d, err := dispatch.New(dispatch.WithStore(s), dispatch.WithConcurrency(1), dispatch.WithPollInterval(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	o := drt.Options{Namespace: "drain", Queue: "work", BuildID: "v1", Owner: "worker", PollInterval: time.Millisecond,
		Workflows: map[string]drt.WorkflowFunc{"order": func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.Activity("work", "work", "", nil).Get() }},
		Activities: map[string]drt.ActivityFunc{"work": func(ctx context.Context, _ drt.ActivityInfo, _ []byte) ([]byte, error) {
			close(entered)
			select {
			case <-release:
				return []byte("done"), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}}}
	eng, err := engine.Build(d, engine.WithDurableWorkflows(o))
	if err != nil {
		t.Fatal(err)
	}
	if status, e := eng.DurableStatus(); e != nil || status.Ready || status.State != drt.WorkerNotStarted {
		t.Fatalf("pre-start readiness: %+v %v", status, e)
	}
	if readiness, readErr := eng.DurableReadiness(t.Context()); readErr != nil || readiness.Ready || readiness.RetirementEnabled || readiness.RetirementStatus != "unenrolled" {
		t.Fatalf("engine readiness: %+v %v", readiness, readErr)
	}
	key := durable.Key{Namespace: o.Namespace, WorkflowID: "order", RunID: "run"}
	if _, err = eng.StartDurableWorkflow(t.Context(), durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "order", BuildID: o.BuildID, Queue: o.Queue}); err != nil {
		t.Fatal(err)
	}
	if err = eng.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	awaitLifecycle(t, entered)
	h, err := eng.BeginDurableDrain(t.Context(), drt.DrainRequest{OperationID: "drain", Deadline: time.Now().Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	observer, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	err = eng.Stop(observer)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stop observer: %v", err)
	}
	s.assertState(t, 0, 0)
	if status, e := eng.DurableStatus(); e != nil || status.Ready || status.InFlight != 1 || status.State != drt.WorkerDraining {
		t.Fatalf("drain status: %+v %v", status, e)
	}
	close(release)
	if result, e := eng.WaitDurableDrain(t.Context(), h); e != nil || !result.Complete {
		t.Fatalf("drain interrupted by observer: %+v %v", result, e)
	}
	if _, e := s.GetExecution(t.Context(), key); e != nil {
		t.Fatal(e)
	}
	if err = eng.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	s.assertState(t, 1, 1)
}

func TestStopWorkersEscalatesWhileTerminalStopWaits(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s), dispatch.WithConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	o := drt.Options{Namespace: "escalate", Queue: "work", BuildID: "v1", Owner: "worker", PollInterval: time.Millisecond,
		Workflows: map[string]drt.WorkflowFunc{"order": func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.Activity("work", "work", "", nil).Get() }},
		Activities: map[string]drt.ActivityFunc{"work": func(ctx context.Context, _ drt.ActivityInfo, _ []byte) ([]byte, error) {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		}}}
	eng, err := engine.Build(d, engine.WithDurableWorkflows(o))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = eng.StartDurableWorkflow(t.Context(), durable.StartRequest{Key: durable.Key{Namespace: o.Namespace, WorkflowID: "order", RunID: "run"}, RequestID: "start", WorkflowType: "order", BuildID: o.BuildID, Queue: o.Queue}); err != nil {
		t.Fatal(err)
	}
	if err = eng.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	awaitLifecycle(t, entered)
	h, err := eng.BeginDurableDrain(t.Context(), drt.DrainRequest{OperationID: "drain", Deadline: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	terminal := make(chan error, 1)
	go func() { terminal <- eng.Stop(t.Context()) }()
	force, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err = eng.StopWorkers(force); !errors.Is(err, drt.ErrDrainIncomplete) {
		t.Fatalf("escalation: %v", err)
	}
	select {
	case err = <-terminal:
		if !errors.Is(err, drt.ErrDrainIncomplete) {
			t.Fatalf("terminal outcome: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("terminal stop retained grace lock")
	}
	if result, e := eng.WaitDurableDrain(t.Context(), h); !errors.Is(e, drt.ErrDrainIncomplete) || result.Complete || !result.Quiescent || result.DeadlineExpired {
		t.Fatalf("force drain result: %+v %v", result, e)
	}
}
