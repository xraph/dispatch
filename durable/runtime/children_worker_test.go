package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

func childWorkerOptions(t *testing.T, parent drt.Options) drt.Options {
	t.Helper()
	options := workerOptions(t)
	options.Namespace = parent.Namespace
	options.Queue = "children"
	options.BuildID = "child-v1"
	options.Workflows["child"] = func(*drt.Workflow, []byte) ([]byte, error) { return []byte("child result"), nil }
	return options
}

func TestWorkerChildReplacement(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = func(w *drt.Workflow, input []byte) ([]byte, error) {
		child := w.ChildWorkflow("child", "child", input, drt.ChildOptions{BuildID: "child-v1", Queue: "children"})
		if _, err := child.Started(); err != nil {
			return nil, err
		}
		return child.Get()
	}
	parent := newWorker(t, s, options)
	key := startWorkerRun(t, parent, options)
	runTask(t, parent, durable.TaskWorkflow)
	link, err := s.GetChildExecution(t.Context(), key, "child")
	if err != nil {
		t.Fatal(err)
	}
	options.Owner = "replacement"
	parent = newWorker(t, s, options)
	runTask(t, parent, durable.TaskWorkflow)
	child := newWorker(t, s, childWorkerOptions(t, options))
	runTask(t, child, durable.TaskWorkflow)
	runTask(t, parent, drt.TaskChildDelivery)
	runTask(t, parent, durable.TaskWorkflow)
	e, err := s.GetExecution(t.Context(), key)
	if err != nil || e.State != durable.StateCompleted || string(e.Output) != "child result" {
		t.Fatalf("parent result: %+v %v", e, err)
	}
	again, err := s.GetChildExecution(t.Context(), key, "child")
	if err != nil || again.Start.Key != link.Start.Key || again.State != durable.StateCompleted {
		t.Fatalf("child identity changed: %+v %v", again, err)
	}
}

func TestWorkerChildParentClosePolicies(t *testing.T) {
	for _, policy := range []durable.ParentClosePolicy{durable.ParentCloseTerminate, durable.ParentCloseRequestCancel, durable.ParentCloseAbandon} {
		t.Run(string(policy), func(t *testing.T) {
			s := memory.New()
			options := workerOptions(t)
			options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
				_, err := w.ChildWorkflow("child", "child", nil, drt.ChildOptions{Queue: "children", BuildID: "child-v1", ParentClosePolicy: policy}).Started()
				return nil, err
			}
			parent := newWorker(t, s, options)
			key := startWorkerRun(t, parent, options)
			runTask(t, parent, durable.TaskWorkflow)
			runTask(t, parent, durable.TaskWorkflow)
			link, err := s.GetChildExecution(t.Context(), key, "child")
			if err != nil {
				t.Fatal(err)
			}
			childOptions := childWorkerOptions(t, options)
			childOptions.Workflows["child"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
				w.SetQueryHandler("status", func([]byte) ([]byte, error) { return []byte("waiting"), nil })
				w.SetCancellationHandler(func(*drt.Workflow, durable.ExecutionCancellation) ([]byte, error) {
					return nil, drt.ErrWorkflowCancelled
				})
				return w.ReceiveSignal("hold", "hold").Get()
			}
			child := newWorker(t, s, childOptions)
			if policy == durable.ParentCloseAbandon {
				if worked, pollErr := child.RunOnce(t.Context(), drt.TaskChildDelivery); pollErr != nil || worked {
					t.Fatalf("abandon sent close: %t %v", worked, pollErr)
				}
			} else {
				runTask(t, child, drt.TaskChildDelivery)
				if policy == durable.ParentCloseRequestCancel {
					runTask(t, child, durable.TaskWorkflow)
					runTask(t, child, durable.TaskWorkflow)
				}
			}
			e, err := s.GetExecution(t.Context(), link.Start.Key)
			want := durable.StateTerminated
			if policy == durable.ParentCloseAbandon {
				want = durable.StateRunning
			}
			if policy == durable.ParentCloseRequestCancel {
				want = durable.StateCancelled
			}
			if err != nil || e.State != want {
				t.Fatalf("policy outcome: %+v %v", e, err)
			}
			if policy == durable.ParentCloseTerminate {
				q, queryErr := child.QueryExecution(t.Context(), drt.QueryRequest{Key: link.Start.Key, BuildID: childOptions.BuildID, Name: "status"})
				if queryErr != nil || q.State != want || string(q.Output) != "waiting" {
					t.Fatalf("terminated child query: %+v %v", q, queryErr)
				}
			}
		})
	}
}

func TestWorkerChildUnacknowledgedTerminal(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		w.ChildWorkflow("child", "child", nil, drt.ChildOptions{ParentClosePolicy: durable.ParentCloseAbandon})
		return nil, nil
	}
	worker := newWorker(t, s, options)
	key := startWorkerRun(t, worker, options)
	runTask(t, worker, durable.TaskWorkflow)
	if _, err := s.GetChildExecution(t.Context(), key, "child"); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("terminal decision created child: %v", err)
	}
}

func TestWorkerChildCreationConflictAndCancellation(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	occupied := durable.StartRequest{Key: durable.Key{Namespace: options.Namespace, WorkflowID: "occupied", RunID: "external"}, RequestID: "external", WorkflowType: "external", BuildID: "external", Queue: "external"}
	if _, err := s.StartExecution(t.Context(), occupied); err != nil {
		t.Fatal(err)
	}
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		failed := w.ChildWorkflow("conflict", "child", nil, drt.ChildOptions{WorkflowID: occupied.WorkflowID, Queue: "children", BuildID: "child-v1"})
		successful := w.ChildWorkflow("success", "child", nil, drt.ChildOptions{Queue: "children", BuildID: "child-v1"})
		cancellation := w.CancelChild("cancel", failed)
		if _, err := failed.Get(); !errors.Is(err, drt.ErrChildStart) {
			return nil, errors.New("missing child start failure")
		}
		if _, err := cancellation.Get(); !errors.Is(err, drt.ErrChildStart) {
			return nil, errors.New("missing cancellation failure")
		}
		return successful.Get()
	}
	parent := newWorker(t, s, options)
	key := startWorkerRun(t, parent, options)
	runTask(t, parent, durable.TaskWorkflow)
	if _, err := s.GetChildExecution(t.Context(), key, "conflict"); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("adopted occupied child: %v", err)
	}
	if _, err := s.GetChildExecution(t.Context(), key, "success"); err != nil {
		t.Fatal(err)
	}
	runTask(t, parent, durable.TaskWorkflow)
	child := newWorker(t, s, childWorkerOptions(t, options))
	runTask(t, child, durable.TaskWorkflow)
	runTask(t, parent, drt.TaskChildDelivery)
	runTask(t, parent, durable.TaskWorkflow)
	e, err := s.GetExecution(t.Context(), key)
	if err != nil || e.State != durable.StateCompleted || string(e.Output) != "child result" {
		t.Fatalf("conflict recovery: %+v %v", e, err)
	}
	messages, err := s.ListChildDeliveries(t.Context(), key, "", 10)
	if err != nil || len(messages) != 0 {
		t.Fatalf("fabricated cancellation acceptance: %+v %v", messages, err)
	}
}

func TestWorkerChildExplicitCancellation(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		phase := "waiting"
		w.SetQueryHandler("phase", func([]byte) ([]byte, error) { return []byte(phase), nil })
		child := w.ChildWorkflow("child", "child", nil, drt.ChildOptions{Queue: "children", BuildID: "child-v1"})
		if _, err := child.Started(); err != nil {
			return nil, err
		}
		if _, err := w.CancelChild("cancel", child).Get(); err != nil {
			return nil, err
		}
		phase = "accepted"
		if _, err := child.Get(); !errors.Is(err, drt.ErrChildCancelled) {
			return nil, errors.New("expected actual child cancellation")
		}
		return []byte("cancelled"), nil
	}
	parent := newWorker(t, s, options)
	key := startWorkerRun(t, parent, options)
	runTask(t, parent, durable.TaskWorkflow)
	runTask(t, parent, durable.TaskWorkflow)
	childOptions := childWorkerOptions(t, options)
	childOptions.Workflows["child"] = func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.ReceiveSignal("hold", "hold").Get() }
	child := newWorker(t, s, childOptions)
	runTask(t, child, drt.TaskChildDelivery)
	runTask(t, parent, drt.TaskChildDelivery)
	runTask(t, parent, durable.TaskWorkflow)
	q, err := parent.QueryExecution(t.Context(), drt.QueryRequest{Key: key, BuildID: options.BuildID, Name: "phase"})
	if err != nil || q.State != durable.StateRunning || string(q.Output) != "accepted" {
		t.Fatalf("acceptance phase: %+v %v", q, err)
	}
	link, err := s.GetChildExecution(t.Context(), key, "child")
	if err != nil || link.State != durable.StateRunning {
		t.Fatalf("acceptance closed child: %+v %v", link, err)
	}
	runTask(t, child, durable.TaskWorkflow)
	runTask(t, child, durable.TaskWorkflow)
	runTask(t, parent, drt.TaskChildDelivery)
	runTask(t, parent, durable.TaskWorkflow)
	e, err := s.GetExecution(t.Context(), key)
	if err != nil || e.State != durable.StateCompleted || string(e.Output) != "cancelled" {
		t.Fatalf("child cancellation result: %+v %v", e, err)
	}
}

func TestWorkerChildParentCleanupWaits(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		var child *drt.Future
		w.SetCancellationHandler(func(*drt.Workflow, durable.ExecutionCancellation) ([]byte, error) {
			if _, err := child.Get(); !errors.Is(err, drt.ErrChildCancelled) {
				return nil, errors.New("child still needed to cancel")
			}
			return nil, drt.ErrWorkflowCancelled
		})
		child = w.ChildWorkflow("child", "child", nil, drt.ChildOptions{Queue: "children", BuildID: "child-v1"})
		return child.Get()
	}
	parent := newWorker(t, s, options)
	key := startWorkerRun(t, parent, options)
	runTask(t, parent, durable.TaskWorkflow)
	runTask(t, parent, durable.TaskWorkflow)
	if _, err := parent.RequestCancelExecution(t.Context(), durable.CancelExecutionRequest{Key: key, RequestID: "cancel-parent", BuildID: options.BuildID}); err != nil {
		t.Fatal(err)
	}
	runTask(t, parent, durable.TaskWorkflow)
	runTask(t, parent, durable.TaskWorkflow)
	before, err := s.GetExecution(t.Context(), key)
	if err != nil || before.State != durable.StateRunning {
		t.Fatalf("cleanup did not await child: %+v %v", before, err)
	}
	childOptions := childWorkerOptions(t, options)
	childOptions.Workflows["child"] = func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.ReceiveSignal("hold", "hold").Get() }
	child := newWorker(t, s, childOptions)
	runTask(t, child, drt.TaskChildDelivery)
	runTask(t, child, durable.TaskWorkflow)
	runTask(t, child, durable.TaskWorkflow)
	runTask(t, parent, drt.TaskChildDelivery)
	runTask(t, parent, durable.TaskWorkflow)
	after, err := s.GetExecution(t.Context(), key)
	if err != nil || after.State != durable.StateCancelled {
		t.Fatalf("cleanup result: %+v %v", after, err)
	}
}

type lostChildRuntimeDelivery struct {
	durable.Store
	calls   int
	request durable.ChildDeliveryRequest
}

func (s *lostChildRuntimeDelivery) ApplyChildDelivery(ctx context.Context, r durable.ChildDeliveryRequest) (durable.ChildDeliveryReceipt, error) {
	if s.calls == 0 {
		s.request = r
	} else if s.request != r {
		return durable.ChildDeliveryReceipt{}, errors.New("delivery retry changed request")
	}
	receipt, err := s.Store.ApplyChildDelivery(ctx, r)
	if err != nil {
		return receipt, err
	}
	s.calls++
	if s.calls < 3 {
		return durable.ChildDeliveryReceipt{}, errors.New("response lost")
	}
	return receipt, nil
}

func TestWorkerChildDeliveryRetriesExactRequest(t *testing.T) {
	backend := memory.New()
	lossy := &lostChildRuntimeDelivery{Store: backend}
	options := workerOptions(t)
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		return w.ChildWorkflow("child", "child", nil, drt.ChildOptions{BuildID: "child-v1", Queue: "children"}).Get()
	}
	parent := newWorker(t, lossy, options)
	key := startWorkerRun(t, parent, options)
	runTask(t, parent, durable.TaskWorkflow)
	runTask(t, parent, durable.TaskWorkflow)
	child := newWorker(t, backend, childWorkerOptions(t, options))
	runTask(t, child, durable.TaskWorkflow)
	runTask(t, parent, drt.TaskChildDelivery)
	runTask(t, parent, durable.TaskWorkflow)
	if lossy.calls != 3 {
		t.Fatalf("lost responses not retried: %d", lossy.calls)
	}
	events, err := backend.ReadHistory(t.Context(), key, 0, 100)
	count := 0
	for _, event := range events {
		if event.Type == durable.EventChildCompleted {
			count++
		}
	}
	if err != nil || count != 1 {
		t.Fatalf("duplicate delivered outcome: %d %v", count, err)
	}
}

func TestWorkerChildManyCreationConflicts(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	for i := range 20 {
		r := durable.StartRequest{Key: durable.Key{Namespace: options.Namespace, WorkflowID: fmt.Sprintf("occupied-%d", i), RunID: "external"}, RequestID: "start", WorkflowType: "external", Queue: "external", BuildID: "external"}
		if _, err := s.StartExecution(t.Context(), r); err != nil {
			t.Fatal(err)
		}
	}
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		children := make([]*drt.Future, 20)
		for i := range children {
			children[i] = w.ChildWorkflow(fmt.Sprintf("child-%d", i), "child", nil, drt.ChildOptions{WorkflowID: fmt.Sprintf("occupied-%d", i)})
		}
		for _, child := range children {
			if _, err := child.Get(); !errors.Is(err, drt.ErrChildStart) {
				return nil, errors.New("expected creation conflict")
			}
		}
		return []byte("handled"), nil
	}
	worker := newWorker(t, s, options)
	key := startWorkerRun(t, worker, options)
	runTask(t, worker, durable.TaskWorkflow)
	runTask(t, worker, durable.TaskWorkflow)
	e, err := s.GetExecution(t.Context(), key)
	if err != nil || e.State != durable.StateCompleted || string(e.Output) != "handled" {
		t.Fatalf("large conflict batch stuck: %+v %v", e, err)
	}
}

func TestWorkerChildCancelPreviouslyFailedStart(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	if _, err := s.StartExecution(t.Context(), durable.StartRequest{Key: durable.Key{Namespace: options.Namespace, WorkflowID: "occupied", RunID: "external"}, RequestID: "start", WorkflowType: "external", Queue: "external", BuildID: "external"}); err != nil {
		t.Fatal(err)
	}
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		child := w.ChildWorkflow("child", "child", nil, drt.ChildOptions{WorkflowID: "occupied"})
		if _, err := child.Started(); !errors.Is(err, drt.ErrChildStart) {
			return nil, errors.New("expected start conflict")
		}
		if _, err := w.CancelChild("cancel", child).Get(); !errors.Is(err, drt.ErrChildStart) {
			return nil, errors.New("expected original start failure")
		}
		return []byte("handled"), nil
	}
	worker := newWorker(t, s, options)
	key := startWorkerRun(t, worker, options)
	for range 3 {
		runTask(t, worker, durable.TaskWorkflow)
	}
	e, err := s.GetExecution(t.Context(), key)
	if err != nil || e.State != durable.StateCompleted || string(e.Output) != "handled" {
		t.Fatalf("previous failure cancellation stuck: %+v %v", e, err)
	}
}

type rejectedChildDeliveryStore struct{ durable.Store }

func (s rejectedChildDeliveryStore) ApplyChildDelivery(context.Context, durable.ChildDeliveryRequest) (durable.ChildDeliveryReceipt, error) {
	return durable.ChildDeliveryReceipt{}, durable.ErrInvalid
}

func TestWorkerChildPollerFailureSurfaces(t *testing.T) {
	s := rejectedChildDeliveryStore{Store: memory.New()}
	options := workerOptions(t)
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		return w.ChildWorkflow("child", "child", nil, drt.ChildOptions{}).Get()
	}
	options.Workflows["child"] = func(*drt.Workflow, []byte) ([]byte, error) { return nil, nil }
	worker := newWorker(t, s, options)
	startWorkerRun(t, worker, options)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := worker.Run(ctx); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("delivery failure hidden: %v", err)
	}
}
