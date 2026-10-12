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

func TestDurableEngineAsyncCallbacks(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatal(err)
	}
	disabled, err := engine.Build(d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = disabled.CompleteDurableActivity(t.Context(), drt.AsyncCompletionRequest{}); !errors.Is(err, engine.ErrDurableDisabled) {
		t.Fatalf("disabled completion: %v", err)
	}
	if _, err = disabled.HeartbeatDurableActivity(t.Context(), drt.AsyncHeartbeatRequest{}); !errors.Is(err, engine.ErrDurableDisabled) {
		t.Fatalf("disabled heartbeat: %v", err)
	}
	var handle drt.AsyncActivityHandle
	options := drt.Options{Namespace: t.Name(), Queue: "orders", BuildID: "v1", Owner: "worker",
		Workflows: map[string]drt.WorkflowFunc{"order": func(w *drt.Workflow, _ []byte) ([]byte, error) {
			return w.ActivityWithOptions("charge", "charge", "", nil, drt.ActivityOptions{StartToCloseTimeout: time.Minute}).Get()
		}},
		Activities: map[string]drt.ActivityFunc{"charge": func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
			var callErr error
			handle, callErr = info.DeferCompletion(ctx)
			return nil, callErr
		}}}
	eng, err := engine.Build(d, engine.WithDurableWorkflows(options))
	if err != nil {
		t.Fatal(err)
	}
	key := durable.Key{Namespace: options.Namespace, WorkflowID: "order", RunID: "run"}
	if _, err = eng.StartDurableWorkflow(t.Context(), durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "order", BuildID: options.BuildID, Queue: options.Queue}); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []durable.TaskKind{durable.TaskWorkflow, durable.TaskActivity} {
		if worked, workErr := eng.DurableWorker().RunOnce(t.Context(), kind); workErr != nil || !worked {
			t.Fatalf("run %s: %v %v", kind, worked, workErr)
		}
	}
	if _, err = eng.HeartbeatDurableActivity(t.Context(), drt.AsyncHeartbeatRequest{Handle: handle, RequestID: "progress", Sequence: 1, Details: []byte("approved")}); err != nil {
		t.Fatal(err)
	}
	request := drt.AsyncCompletionRequest{Handle: handle, RequestID: "result", Output: []byte("paid")}
	receipt, err := eng.CompleteDurableActivity(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if worked, workErr := eng.DurableWorker().RunOnce(t.Context(), durable.TaskWorkflow); workErr != nil || !worked {
		t.Fatalf("finish: %v %v", worked, workErr)
	}
	execution, err := s.GetExecution(t.Context(), key)
	if err != nil || execution.State != durable.StateCompleted || string(execution.Output) != "paid" {
		t.Fatalf("async engine result: %+v %v", execution, err)
	}
	if got, repeatErr := eng.CompleteDurableActivity(t.Context(), request); repeatErr != nil || got != receipt {
		t.Fatalf("engine receipt: %+v %v", got, repeatErr)
	}
}
