//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

type lostChildRuntimeCreation struct{ durable.Store }

func (s lostChildRuntimeCreation) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
	receipt, err := s.Store.CommitTransition(ctx, r)
	if err == nil && len(r.Children) != 0 {
		return durable.Receipt{}, errChildCreationLost
	}
	return receipt, err
}

type lostChildRuntimeResult struct {
	durable.Store
	request durable.ChildDeliveryRequest
	receipt durable.ChildDeliveryReceipt
}

func (s *lostChildRuntimeResult) ApplyChildDelivery(ctx context.Context, r durable.ChildDeliveryRequest) (durable.ChildDeliveryReceipt, error) {
	receipt, err := s.Store.ApplyChildDelivery(ctx, r)
	if err != nil {
		return receipt, err
	}
	s.request, s.receipt = r, receipt
	return durable.ChildDeliveryReceipt{}, errChildDeliveryLost
}

func childRuntimeOptions(t *testing.T) drt.Options {
	t.Helper()
	return drt.Options{Namespace: t.Name(), Queue: "parents", BuildID: "parent-v1", Owner: "parent", LeaseDuration: 2 * time.Second, StoreTimeout: 500 * time.Millisecond, PollInterval: time.Millisecond, Workflows: map[string]drt.WorkflowFunc{
		"parent": func(w *drt.Workflow, _ []byte) ([]byte, error) {
			child := w.ChildWorkflow("child", "child", nil, drt.ChildOptions{Queue: "children", BuildID: "child-v1"})
			if _, err := child.Started(); err != nil {
				return nil, err
			}
			return child.Get()
		},
		"child": func(w *drt.Workflow, _ []byte) ([]byte, error) {
			w.SetQueryHandler("state", func([]byte) ([]byte, error) { return []byte("done"), nil })
			return []byte("child result"), nil
		},
	}}
}

func newChildRuntimeWorker(t *testing.T, s durable.Store, options drt.Options) *drt.Worker {
	t.Helper()
	w, err := drt.NewWorker(s, options)
	if err != nil {
		t.Fatal(err)
	}
	return w
}
func runChildRuntimeTask(t *testing.T, w *drt.Worker, kind durable.TaskKind) {
	t.Helper()
	if worked, err := w.RunOnce(t.Context(), kind); err != nil || !worked {
		t.Fatalf("run %s: %t %v", kind, worked, err)
	}
}

func TestDurableRuntimeChildReplacement(t *testing.T) {
	s, dsn := setupTestStoreConnection(t)
	options := childRuntimeOptions(t)
	parent := newChildRuntimeWorker(t, lostChildRuntimeCreation{Store: s}, options)
	start := durable.StartRequest{Key: durable.Key{Namespace: options.Namespace, WorkflowID: "parent", RunID: "run"}, RequestID: "start", WorkflowType: "parent", Queue: options.Queue, BuildID: options.BuildID}
	if _, err := parent.StartExecution(t.Context(), start); err != nil {
		t.Fatal(err)
	}
	if worked, err := parent.RunOnce(t.Context(), durable.TaskWorkflow); !worked || !errors.Is(err, errChildCreationLost) {
		t.Fatalf("creation response loss: %t %v", worked, err)
	}
	link, err := s.GetChildExecution(t.Context(), start.Key, "child")
	if err != nil {
		t.Fatal(err)
	}
	s = reopenAsyncStore(t, s, dsn)
	options.Owner = "replacement"
	parent = newChildRuntimeWorker(t, s, options)
	runChildRuntimeTask(t, parent, durable.TaskWorkflow)
	childOptions := options
	childOptions.Queue, childOptions.BuildID, childOptions.Owner = "children", "child-v1", "child"
	child := newChildRuntimeWorker(t, s, childOptions)
	runChildRuntimeTask(t, child, durable.TaskWorkflow)
	s = reopenAsyncStore(t, s, dsn)
	lossy := &lostChildRuntimeResult{Store: s}
	parent = newChildRuntimeWorker(t, lossy, options)
	if worked, deliveryErr := parent.RunOnce(t.Context(), drt.TaskChildDelivery); !worked || !errors.Is(deliveryErr, errChildDeliveryLost) {
		t.Fatalf("result response loss: %t %v", worked, deliveryErr)
	}
	s = reopenAsyncStore(t, s, dsn)
	parent = newChildRuntimeWorker(t, s, options)
	runChildRuntimeTask(t, parent, durable.TaskWorkflow)
	e, err := s.GetExecution(t.Context(), start.Key)
	if err != nil || e.State != durable.StateCompleted || string(e.Output) != "child result" {
		t.Fatalf("replacement result: %+v %v", e, err)
	}
	if receipt, retryErr := s.ApplyChildDelivery(t.Context(), lossy.request); retryErr != nil || receipt != lossy.receipt {
		t.Fatalf("receipt after replacement/closure: %+v %v", receipt, retryErr)
	}
	again, err := s.GetChildExecution(t.Context(), start.Key, "child")
	if err != nil || again.Start.Key != link.Start.Key || again.State != durable.StateCompleted {
		t.Fatalf("original child changed: %+v %v", again, err)
	}
	child = newChildRuntimeWorker(t, s, childOptions)
	q, err := child.QueryExecution(t.Context(), drt.QueryRequest{Key: link.Start.Key, BuildID: childOptions.BuildID, Name: "state"})
	if err != nil || q.State != durable.StateCompleted || string(q.Output) != "done" {
		t.Fatalf("child query after restart: %+v %v", q, err)
	}
	events, err := s.ReadHistory(t.Context(), start.Key, 0, 100)
	starts, results := 0, 0
	for _, event := range events {
		if event.Type == durable.EventChildStarted {
			starts++
		}
		if event.Type == durable.EventChildCompleted {
			results++
		}
	}
	if err != nil || starts != 1 || results != 1 {
		t.Fatalf("duplicate child events: starts=%d results=%d %v", starts, results, err)
	}
}

func TestDurableRuntimeChildPollers(t *testing.T) {
	s := setupTestStore(t)
	options := childRuntimeOptions(t)
	parent := newChildRuntimeWorker(t, s, options)
	childOptions := options
	childOptions.Queue, childOptions.BuildID, childOptions.Owner = "children", "child-v1", "child"
	child := newChildRuntimeWorker(t, s, childOptions)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	done := make(chan error, 2)
	for _, w := range []*drt.Worker{parent, child} {
		go func() { done <- w.Run(ctx) }()
	}
	start := durable.StartRequest{Key: durable.Key{Namespace: options.Namespace, WorkflowID: "parent", RunID: "run"}, RequestID: "start", WorkflowType: "parent", Queue: options.Queue, BuildID: options.BuildID}
	if _, err := parent.StartExecution(ctx, start); err != nil {
		t.Fatal(err)
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		e, err := s.GetExecution(ctx, start.Key)
		if err != nil {
			t.Fatal(err)
		}
		if e.State == durable.StateCompleted {
			if string(e.Output) != "child result" {
				t.Fatalf("wrong output: %q", e.Output)
			}
			break
		}
		select {
		case err = <-done:
			t.Fatalf("worker stopped early: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
	cancel()
	for range 2 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("child poller did not stop")
		}
	}
}
