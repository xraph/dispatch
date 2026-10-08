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
	"github.com/xraph/dispatch/store"
	"github.com/xraph/dispatch/store/memory"
)

// Embedding only the aggregate interface hides optional durable capabilities.
type checkpointOnlyStore struct{ store.Store }

func TestDurableRuntimeRequiresExplicitSupportedStore(t *testing.T) {
	d, err := dispatch.New(dispatch.WithStore(checkpointOnlyStore{memory.New()}))
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := engine.Build(d)
	if err != nil || legacy.DurableWorker() != nil {
		t.Fatalf("legacy setup changed: %v", err)
	}
	_, err = engine.Build(d, engine.WithDurableWorkflows(drt.Options{}))
	if !errors.Is(err, engine.ErrDurableUnsupported) {
		t.Fatalf("unsupported store did not fail configuration: %v", err)
	}
	if _, err = legacy.StartDurableWorkflow(t.Context(), durable.StartRequest{}); !errors.Is(err, engine.ErrDurableDisabled) {
		t.Fatalf("disabled runtime silently accepted a workflow: %v", err)
	}
}

func buildDurableEngine(t *testing.T, handler drt.WorkflowFunc) (*engine.Engine, *memory.Store, durable.StartRequest) {
	t.Helper()
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatal(err)
	}
	options := drt.Options{Namespace: t.Name(), Queue: "durable", BuildID: "v1", Owner: "worker",
		PollInterval: time.Millisecond, Workflows: map[string]drt.WorkflowFunc{"order": handler},
		Activities: map[string]drt.ActivityFunc{"charge": func(_ context.Context, _ drt.ActivityInfo, _ []byte) ([]byte, error) { return []byte("paid"), nil }}}
	eng, err := engine.Build(d, engine.WithDurableWorkflows(options))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = eng.Stop(ctx)
	})
	request := durable.StartRequest{Key: durable.Key{Namespace: options.Namespace, WorkflowID: "order", RunID: "run"},
		RequestID: "start", WorkflowType: "order", BuildID: options.BuildID, Queue: options.Queue}
	return eng, s, request
}

func TestDurableEngineRunsActivitiesAndTimers(t *testing.T) {
	eng, s, request := buildDurableEngine(t, func(w *drt.Workflow, _ []byte) ([]byte, error) {
		output, err := w.Activity("charge", "charge", "", nil).Get()
		if err != nil {
			return nil, err
		}
		if _, err = w.Timer("delay", time.Millisecond).Get(); err != nil {
			return nil, err
		}
		return output, nil
	})
	if _, err := eng.StartDurableWorkflow(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		execution, err := s.GetExecution(t.Context(), request.Key)
		if err != nil {
			t.Fatal(err)
		}
		if execution.State == durable.StateCompleted {
			if string(execution.Output) != "paid" {
				t.Fatalf("output: %q", execution.Output)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("engine did not complete durable workflow")
}

func TestDurableEngineSurfacesWorkerFailure(t *testing.T) {
	eng, s, request := buildDurableEngine(t, func(_ *drt.Workflow, _ []byte) ([]byte, error) { panic("bad workflow") })
	if _, err := eng.StartDurableWorkflow(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := eng.Health(t.Context()); errors.Is(err, drt.ErrWorkflowPanic) {
			execution, getErr := s.GetExecution(t.Context(), request.Key)
			if getErr != nil || execution.State != durable.StateRunning {
				t.Fatalf("task error closed execution: %+v, %v", execution, getErr)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("engine health hid durable worker failure")
}

func TestDurableEngineCannotStartAfterStop(t *testing.T) {
	eng, _, _ := buildDurableEngine(t, func(_ *drt.Workflow, _ []byte) ([]byte, error) { return nil, nil })
	if err := eng.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(t.Context()); !errors.Is(err, durable.ErrClosed) {
		t.Fatalf("stopped durable worker restarted: %v", err)
	}
}
