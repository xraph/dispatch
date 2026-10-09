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

func TestDurableEngineSignals(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatal(err)
	}
	disabled, err := engine.Build(d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = disabled.SignalDurableWorkflow(t.Context(), durable.SignalRequest{}); !errors.Is(err, engine.ErrDurableDisabled) {
		t.Fatalf("disabled signal: %v", err)
	}
	if _, err = disabled.SignalWithStartDurableWorkflow(t.Context(), durable.SignalWithStartRequest{}); !errors.Is(err, engine.ErrDurableDisabled) {
		t.Fatalf("disabled signal start: %v", err)
	}
	options := drt.Options{Namespace: t.Name(), Queue: "orders", BuildID: "v1", Owner: "worker", Workflows: map[string]drt.WorkflowFunc{"order": func(w *drt.Workflow, _ []byte) ([]byte, error) {
		a, receiveErr := w.ReceiveSignal("first", "approve").Get()
		if receiveErr != nil {
			return nil, receiveErr
		}
		b, receiveErr := w.ReceiveSignal("second", "approve").Get()
		return append(a, b...), receiveErr
	}}}
	eng, err := engine.Build(d, engine.WithDurableWorkflows(options))
	if err != nil {
		t.Fatal(err)
	}
	req := durable.SignalWithStartRequest{Start: durable.StartRequest{Key: durable.Key{Namespace: options.Namespace, WorkflowID: "order", RunID: "run"}, RequestID: "start", WorkflowType: "order", BuildID: options.BuildID, Queue: options.Queue}, Name: "approve", Input: []byte("A")}
	first, err := eng.SignalWithStartDurableWorkflow(t.Context(), req)
	if err != nil || !first.Started {
		t.Fatalf("start: %+v %v", first, err)
	}
	if _, err = eng.SignalDurableWorkflow(t.Context(), durable.SignalRequest{Key: first.Key, RequestID: "second", BuildID: options.BuildID, Name: "approve", Input: []byte("B")}); err != nil {
		t.Fatal(err)
	}
	if worked, workErr := eng.DurableWorker().RunOnce(t.Context(), durable.TaskWorkflow); workErr != nil || !worked {
		t.Fatalf("run: %t %v", worked, workErr)
	}
	execution, err := s.GetExecution(t.Context(), first.Key)
	if err != nil || string(execution.Output) != "AB" || execution.State != durable.StateCompleted {
		t.Fatalf("engine signal result: %+v %v", execution, err)
	}
}
