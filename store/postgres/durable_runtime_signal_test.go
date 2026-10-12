//go:build integration

package postgres_test

import (
	"errors"
	"testing"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

func TestDurableRuntimeSignalRecovery(t *testing.T) {
	for _, mode := range []string{"signal", "with_start"} {
		t.Run(mode, func(t *testing.T) {
			s, dsn := setupTestStoreConnection(t)
			lost := &lostSignalResponseStore{Store: s}
			options := drt.Options{Namespace: t.Name(), Queue: "orders", BuildID: "v1", Owner: "first", Workflows: map[string]drt.WorkflowFunc{"order": func(w *drt.Workflow, _ []byte) ([]byte, error) {
				first, err := w.ReceiveSignal("first", "approve").Get()
				if err != nil {
					return nil, err
				}
				second, err := w.ReceiveSignal("second", "approve").Get()
				return append(first, second...), err
			}}}
			worker, err := drt.NewWorker(lost, options)
			if err != nil {
				t.Fatal(err)
			}
			start := signalStartRequest(t)
			signal := durable.SignalRequest{Key: start.Key, RequestID: "first-message", BuildID: start.BuildID, Name: "approve", Input: []byte("A")}
			signal.RunID = ""
			withStart := durable.SignalWithStartRequest{Start: start, Name: "approve", Input: []byte("A")}
			if mode == "signal" {
				if _, err = worker.StartExecution(t.Context(), start); err != nil {
					t.Fatal(err)
				}
				runSignalWorkflow(t, worker)
				if _, err = s.GetTask(t.Context(), start.Key, "command:1"); !errors.Is(err, durable.ErrNotFound) {
					t.Fatalf("signal wait created task: %v", err)
				}
				_, err = worker.SignalExecution(t.Context(), signal)
			} else {
				_, err = worker.SignalWithStart(t.Context(), withStart)
			}
			if !errors.Is(err, errSignalResponseLost) {
				t.Fatalf("test did not lose acceptance response: %v", err)
			}
			accepted := lost.accepted
			reopened := reopenAsyncStore(t, s, dsn)
			options.Owner = "replacement"
			worker, err = drt.NewWorker(reopened, options)
			if err != nil {
				t.Fatal(err)
			}
			runSignalWorkflow(t, worker)
			callbackOptions := options
			callbackOptions.Queue = "callbacks"
			callbackOptions.Workflows = nil
			callback, err := drt.NewWorker(reopened, callbackOptions)
			if err != nil {
				t.Fatal(err)
			}
			proposed := start
			proposed.RunID = "unused"
			proposed.RequestID = "second-message"
			proposed.Queue = "unused-queue"
			proposed.WorkflowType = "unused-type"
			second, err := callback.SignalWithStart(t.Context(), durable.SignalWithStartRequest{Start: proposed, Name: "approve", Input: []byte("B")})
			if err != nil || second.Started || second.Key != accepted.Key {
				t.Fatalf("existing run selection: %+v %v", second, err)
			}
			if worked, callErr := callback.RunOnce(t.Context(), durable.TaskWorkflow); callErr != nil || worked {
				t.Fatalf("callback stole routed task: %t %v", worked, callErr)
			}
			runSignalWorkflow(t, worker)
			execution, err := reopened.GetExecution(t.Context(), accepted.Key)
			if err != nil || execution.State != durable.StateCompleted || string(execution.Output) != "AB" {
				t.Fatalf("recovery: %+v %v", execution, err)
			}
			events, err := reopened.ReadHistory(t.Context(), accepted.Key, 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			received, consumed := 0, 0
			for _, event := range events {
				if event.Type == drt.EventSignalReceived {
					received++
				}
				if event.Type == drt.EventSignalConsumed {
					consumed++
				}
			}
			if received != 2 || consumed != 2 {
				t.Fatalf("duplicate signals: %d/%d", received, consumed)
			}
			next := start
			next.RequestID = "next"
			next.RunID = "next"
			if _, err = worker.StartExecution(t.Context(), next); err != nil {
				t.Fatal(err)
			}
			var receipt durable.SignalReceipt
			if mode == "signal" {
				receipt, err = callback.SignalExecution(t.Context(), signal)
			} else {
				receipt, err = callback.SignalWithStart(t.Context(), withStart)
			}
			if err != nil || receipt != accepted {
				t.Fatalf("retry followed replacement: %+v %v", receipt, err)
			}
			events, err = reopened.ReadHistory(t.Context(), next.Key, 0, 100)
			if err != nil || len(events) != 1 {
				t.Fatalf("retry changed next run: %d %v", len(events), err)
			}
		})
	}
}

func runSignalWorkflow(t *testing.T, worker *drt.Worker) {
	t.Helper()
	if worked, err := worker.RunOnce(t.Context(), durable.TaskWorkflow); err != nil || !worked {
		t.Fatalf("workflow task: %t %v", worked, err)
	}
}
