//go:build integration

package postgres_test

import (
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

func TestDurableRuntimeExecutionTimeoutReplacement(t *testing.T) {
	for _, phase := range []string{"initial", "normal", "accepted", "cleanup"} {
		t.Run(phase, func(t *testing.T) {
			s, dsn := setupTestStoreConnection(t)
			options := childRuntimeOptions(t)
			options.Workflows = map[string]drt.WorkflowFunc{"order": func(w *drt.Workflow, _ []byte) ([]byte, error) {
				state := "normal"
				w.SetQueryHandler("status", func([]byte) ([]byte, error) { return []byte(state), nil })
				w.SetCancellationHandler(func(cleanup *drt.Workflow, _ durable.ExecutionCancellation) ([]byte, error) {
					state = "cleanup"
					return cleanup.Timer("cleanup", time.Hour).Get()
				})
				return w.Timer("wait", time.Hour).Get()
			}}
			worker := newChildRuntimeWorker(t, s, options)
			start := durable.StartRequest{Key: durable.Key{Namespace: options.Namespace, WorkflowID: "order", RunID: "run"}, RequestID: "start", WorkflowType: "order", BuildID: options.BuildID, Queue: options.Queue, RunTimeout: 2 * time.Second}
			if phase == "initial" {
				start.RunTimeout = time.Microsecond
			}
			if _, err := worker.StartExecution(t.Context(), start); err != nil {
				t.Fatal(err)
			}
			if phase != "initial" {
				runQueryTask(t, worker, durable.TaskWorkflow)
			}
			if phase == "accepted" || phase == "cleanup" {
				if _, err := worker.RequestCancelExecution(t.Context(), durable.CancelExecutionRequest{Key: start.Key, RequestID: "cancel", BuildID: start.BuildID, Reason: "stop"}); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "cleanup" {
				runQueryTask(t, worker, durable.TaskWorkflow)
				runQueryTask(t, worker, durable.TaskWorkflow)
			}
			before, err := s.GetExecution(t.Context(), start.Key)
			if err != nil {
				t.Fatal(err)
			}
			waitDurableStoreTime(t, s, before.RunDeadlineAt)
			s = reopenAsyncStore(t, s, dsn)
			replacement := options
			replacement.BuildID = "new-build"
			replacement.Queue = "other-queue"
			replacement.Owner = "replacement"
			replacement.Workflows = nil
			processor := newChildRuntimeWorker(t, s, replacement)
			runQueryTask(t, processor, drt.TaskExecutionTimeout)
			if worked, runErr := processor.RunOnce(t.Context(), drt.TaskExecutionTimeout); runErr != nil || worked {
				t.Fatalf("duplicate closure: %t %v", worked, runErr)
			}
			s = reopenAsyncStore(t, s, dsn)
			worker = newChildRuntimeWorker(t, s, options)
			want := "normal"
			if phase == "cleanup" {
				want = "cleanup"
			}
			checkPostgresQuery(t, s, worker, drt.QueryRequest{Key: start.Key, BuildID: start.BuildID, Name: "status"}, want, durable.StateTimedOut)
			events, err := s.ReadHistory(t.Context(), start.Key, 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, event := range events {
				if event.Type == durable.EventWorkflowTimedOut {
					count++
				}
			}
			if count != 1 {
				t.Fatalf("timeout events: %d", count)
			}
		})
	}
}

func TestDurableRuntimeChildExecutionTimeoutReplacement(t *testing.T) {
	s, dsn := setupTestStoreConnection(t)
	options := childRuntimeOptions(t)
	options.Workflows["parent"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		_, err := w.ChildWorkflow("child", "child", nil, drt.ChildOptions{BuildID: "retired", Queue: "retired", RunTimeout: time.Microsecond, ExecutionTimeout: time.Hour}).Get()
		var failure *drt.ChildWorkflowError
		if errors.As(err, &failure) && errors.Is(err, drt.ErrChildTimedOut) && failure.Timeout != nil && failure.Timeout.Kind == durable.TimeoutRun {
			return []byte("child timed out"), nil
		}
		return nil, err
	}
	parent := newChildRuntimeWorker(t, s, options)
	start := durable.StartRequest{Key: durable.Key{Namespace: options.Namespace, WorkflowID: "parent", RunID: "run"}, RequestID: "start", WorkflowType: "parent", Queue: options.Queue, BuildID: options.BuildID}
	if _, err := parent.StartExecution(t.Context(), start); err != nil {
		t.Fatal(err)
	}
	runChildRuntimeTask(t, parent, durable.TaskWorkflow)
	link, err := s.GetChildExecution(t.Context(), start.Key, "child")
	if err != nil {
		t.Fatal(err)
	}
	if link.Start.RunTimeout != time.Microsecond || link.Start.ExecutionTimeout != time.Hour {
		t.Fatalf("child timeout policy: %+v", link.Start)
	}
	s = reopenAsyncStore(t, s, dsn)
	options.Owner = "replacement"
	parent = newChildRuntimeWorker(t, s, options)
	runChildRuntimeTask(t, parent, drt.TaskExecutionTimeout)
	s = reopenAsyncStore(t, s, dsn)
	parent = newChildRuntimeWorker(t, s, options)
	runChildRuntimeTask(t, parent, drt.TaskChildDelivery)
	runChildRuntimeTask(t, parent, durable.TaskWorkflow)
	e, err := s.GetExecution(t.Context(), start.Key)
	if err != nil || e.State != durable.StateCompleted || string(e.Output) != "child timed out" {
		t.Fatalf("parent timeout result: %+v %v", e, err)
	}
	childOptions := options
	childOptions.BuildID = link.Start.BuildID
	childOptions.Queue = link.Start.Queue
	child := newChildRuntimeWorker(t, s, childOptions)
	checkPostgresQuery(t, s, child, drt.QueryRequest{Key: link.Start.Key, BuildID: link.Start.BuildID, Name: "state"}, "done", durable.StateTimedOut)
	deliveries, err := s.ListChildDeliveries(t.Context(), link.Start.Key, "", 10)
	if err != nil || len(deliveries) != 1 || !deliveries[0].Done || deliveries[0].Disposition != durable.ChildDeliveryApplied {
		t.Fatalf("child result delivery: %+v %v", deliveries, err)
	}
}
