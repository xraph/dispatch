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

func TestDurableEngineChildLifecycle(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatal(err)
	}
	options := drt.Options{Namespace: t.Name(), Queue: "orders", BuildID: "v1", Owner: "engine", PollInterval: time.Millisecond, Workflows: map[string]drt.WorkflowFunc{
		"parent": func(w *drt.Workflow, _ []byte) ([]byte, error) {
			return w.ChildWorkflow("child", "child", nil, drt.ChildOptions{}).Get()
		},
		"child": func(*drt.Workflow, []byte) ([]byte, error) { return []byte("child result"), nil },
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
	if err != nil || link.State != durable.StateCompleted {
		t.Fatalf("engine child: %+v %v", link, err)
	}
	deliveries, err := s.ListChildDeliveries(ctx, link.Start.Key, "", 10)
	if err != nil || len(deliveries) != 1 || !deliveries[0].Done || deliveries[0].Disposition != durable.ChildDeliveryApplied {
		t.Fatalf("engine did not apply result: %+v %v", deliveries, err)
	}
}
