package engine_test

import (
	"errors"
	"testing"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/store/memory"
)

func TestDurableEngineQuery(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatal(err)
	}
	disabled, err := engine.Build(d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = disabled.QueryDurableWorkflow(t.Context(), drt.QueryRequest{}); !errors.Is(err, engine.ErrDurableDisabled) {
		t.Fatalf("disabled query: %v", err)
	}
	options := drt.Options{Namespace: t.Name(), Queue: "orders", BuildID: "v1", Owner: "worker", Workflows: map[string]drt.WorkflowFunc{"order": func(w *drt.Workflow, _ []byte) ([]byte, error) {
		state := "pending"
		w.SetQueryHandler("status", func(_ []byte) ([]byte, error) { return []byte(state), nil })
		value, receiveErr := w.ReceiveSignal("approval", "approve").Get()
		state = string(value)
		return value, receiveErr
	}}}
	eng, err := engine.Build(d, engine.WithDurableWorkflows(options))
	if err != nil {
		t.Fatal(err)
	}
	key := durable.Key{Namespace: options.Namespace, WorkflowID: "order", RunID: "run"}
	if _, err = eng.StartDurableWorkflow(t.Context(), durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "order", BuildID: options.BuildID, Queue: options.Queue}); err != nil {
		t.Fatal(err)
	}
	request := drt.QueryRequest{Key: key, BuildID: options.BuildID, Name: "status"}
	result, err := eng.QueryDurableWorkflow(t.Context(), request)
	if err != nil || string(result.Output) != "pending" || result.Revision != 1 {
		t.Fatalf("initial query: %+v %v", result, err)
	}
	if _, err = eng.SignalDurableWorkflow(t.Context(), durable.SignalRequest{Key: key, RequestID: "approval", BuildID: options.BuildID, Name: "approve", Input: []byte("approved")}); err != nil {
		t.Fatal(err)
	}
	result, err = eng.QueryDurableWorkflow(t.Context(), request)
	if err != nil || string(result.Output) != "approved" || result.State != durable.StateRunning || result.Revision != 2 {
		t.Fatalf("accepted signal query: %+v %v", result, err)
	}
	if worked, workErr := eng.DurableWorker().RunOnce(t.Context(), durable.TaskWorkflow); workErr != nil || !worked {
		t.Fatalf("workflow: %t %v", worked, workErr)
	}
	result, err = eng.QueryDurableWorkflow(t.Context(), request)
	if err != nil || string(result.Output) != "approved" || result.State != durable.StateCompleted {
		t.Fatalf("completed query: %+v %v", result, err)
	}
}
