package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/store/memory"
)

func TestDurableEngineContinuationLifecycle(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatal(err)
	}
	options := drt.Options{Namespace: t.Name(), Queue: "orders", BuildID: "v1", Owner: "engine", PollInterval: time.Millisecond, Workflows: map[string]drt.WorkflowFunc{
		"parent": func(w *drt.Workflow, _ []byte) ([]byte, error) {
			return w.ChildWorkflow("child", "child", nil, drt.ChildOptions{}).Get()
		},
		"child": func(w *drt.Workflow, input []byte) ([]byte, error) {
			w.SetQueryHandler("input", func([]byte) ([]byte, error) { return input, nil })
			if len(input) == 0 {
				return nil, w.ContinueAsNew([]byte("middle"), drt.ContinueOptions{})
			}
			if string(input) == "middle" {
				return nil, w.ContinueAsNew([]byte("last"), drt.ContinueOptions{})
			}
			return []byte("child result"), nil
		},
	}}
	eng, err := engine.Build(d, engine.WithDurableWorkflows(options))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err = eng.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
		defer stopCancel()
		if stopErr := eng.Stop(stopCtx); stopErr != nil {
			t.Error(stopErr)
		}
	}()
	start := durable.StartRequest{Key: durable.Key{Namespace: options.Namespace, WorkflowID: "parent", RunID: "run"}, RequestID: "start", WorkflowType: "parent", Queue: options.Queue, BuildID: options.BuildID}
	if _, err = eng.StartDurableWorkflow(ctx, start); err != nil {
		t.Fatal(err)
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		e, readErr := s.GetExecution(ctx, start.Key)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if e.State == durable.StateCompleted {
			if string(e.Output) != "child result" {
				t.Fatalf("wrong output: %q", e.Output)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
	link, err := s.GetChildExecution(ctx, start.Key, "child")
	if err != nil || link.State != durable.StateCompleted || link.CurrentKey == link.Start.Key {
		t.Fatalf("engine child: %+v %v", link, err)
	}
	for _, target := range []durable.Key{link.Start.Key, link.CurrentKey} {
		q, queryErr := eng.QueryDurableWorkflow(ctx, drt.QueryRequest{Key: target, BuildID: options.BuildID, Name: "input"})
		want, state := "", durable.StateContinuedAsNew
		if target == link.CurrentKey {
			want, state = "last", durable.StateCompleted
		}
		if queryErr != nil || q.State != state || string(q.Output) != want {
			t.Fatalf("chain query: %+v %v", q, queryErr)
		}
	}
	deliveries, err := s.ListChildDeliveries(ctx, link.CurrentKey, "", 10)
	if err != nil || len(deliveries) != 1 || !deliveries[0].Done || deliveries[0].Disposition != durable.ChildDeliveryApplied {
		t.Fatalf("engine did not apply result: %+v %v", deliveries, err)
	}
}
