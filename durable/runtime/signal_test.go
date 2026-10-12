package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

func TestSignalWorkflowBeforeAndAfterWait(t *testing.T) {
	for _, before := range []bool{true, false} {
		t.Run(fmt.Sprint(before), func(t *testing.T) {
			s := memory.New()
			options := workerOptions(t)
			options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
				future := w.ReceiveSignal("approval", "approve")
				value, err := future.Get()
				if err != nil {
					return nil, err
				}
				value[0] = 'X'
				value, err = future.Get()
				if err != nil {
					return nil, err
				}
				return w.ActivityWithOptions("charge", "charge", "", value, drt.ActivityOptions{StartToCloseTimeout: time.Minute}).Get()
			}
			calls := 0
			options.Activities["charge"] = func(_ context.Context, _ drt.ActivityInfo, input []byte) ([]byte, error) { calls++; return input, nil }
			worker := newWorker(t, s, options)
			key := startWorkerRun(t, worker, options)
			if !before {
				runTask(t, worker, durable.TaskWorkflow)
				if _, err := s.GetTask(t.Context(), key, "command:1"); !errors.Is(err, durable.ErrNotFound) {
					t.Fatalf("receive created a polled task: %v", err)
				}
			}
			callbackOptions := options
			callbackOptions.Queue = "callbacks"
			callbackOptions.Workflows = nil
			callbackOptions.Activities = nil
			callback := newWorker(t, s, callbackOptions)
			request := durable.SignalRequest{Key: key, RequestID: "approval", BuildID: options.BuildID, Name: "approve", Input: []byte("approved")}
			receipt, err := callback.SignalExecution(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			request.Input[0] = 'Y'
			runTask(t, worker, durable.TaskWorkflow)
			runTask(t, worker, durable.TaskActivity)
			runTask(t, newWorker(t, s, options), durable.TaskWorkflow)
			execution, err := s.GetExecution(t.Context(), key)
			if err != nil || execution.State != durable.StateCompleted || string(execution.Output) != "approved" || calls != 1 {
				t.Fatalf("signal result: %+v calls=%d %v", execution, calls, err)
			}
			request.Input = []byte("approved")
			next := durable.StartRequest{Key: key, RequestID: "next", WorkflowType: "order", BuildID: options.BuildID, Queue: options.Queue}
			next.RunID = "next"
			if _, err = worker.StartExecution(t.Context(), next); err != nil {
				t.Fatal(err)
			}
			if got, retryErr := callback.SignalExecution(t.Context(), request); retryErr != nil || got != receipt {
				t.Fatalf("recovery after closure: %+v %v", got, retryErr)
			}
			events, err := s.ReadHistory(t.Context(), key, 0, 100)
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
			if received != 1 || consumed != 1 {
				t.Fatalf("duplicate message or consumption: %d/%d", received, consumed)
			}
		})
	}
}

func TestSignalWithStartClientRouting(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.BuildID = strings.Repeat("b", 512)
	options.Queue = "callbacks"
	worker := newWorker(t, s, options)
	req := durable.SignalWithStartRequest{Start: durable.StartRequest{Key: durable.Key{Namespace: options.Namespace, WorkflowID: "order", RunID: "first"}, RequestID: strings.Repeat("s", 512), WorkflowType: "order", BuildID: options.BuildID, Queue: "orders"}, Name: "approve", Input: []byte("one")}
	first, err := worker.SignalWithStart(t.Context(), req)
	if err != nil || !first.Started {
		t.Fatalf("start: %+v %v", first, err)
	}
	second := req
	second.Start.RunID = "unused"
	second.Start.RequestID = "second"
	second.Start.Queue = "different"
	got, err := worker.SignalWithStart(t.Context(), second)
	if err != nil || got.Started || got.Key != first.Key {
		t.Fatalf("reuse: %+v %v", got, err)
	}
	task, err := s.GetTask(t.Context(), first.Key, "workflow:signal:2")
	if err != nil || task.Queue != "orders" {
		t.Fatalf("routing: %+v %v", task, err)
	}
	for _, mode := range []string{"namespace", "build"} {
		bad := req
		bad.Start.RequestID = mode
		signal := durable.SignalRequest{Key: first.Key, RequestID: mode, BuildID: options.BuildID, Name: "approve"}
		if mode == "namespace" {
			bad.Start.Namespace = "foreign"
			signal.Namespace = "foreign"
		} else {
			bad.Start.BuildID = "foreign"
			signal.BuildID = "foreign"
		}
		if _, err = worker.SignalWithStart(t.Context(), bad); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("invalid start routing: %v", err)
		}
		if _, err = worker.SignalExecution(t.Context(), signal); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("invalid signal routing: %v", err)
		}
	}
}

func TestSignalWithStartRuntimeLimits(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	worker := newWorker(t, s, options)
	for _, field := range []string{"queue", "type"} {
		request := durable.SignalWithStartRequest{Start: durable.StartRequest{Key: durable.Key{Namespace: options.Namespace, WorkflowID: field, RunID: "run"}, RequestID: "start", WorkflowType: "order", BuildID: options.BuildID, Queue: "orders"}, Name: "approve"}
		if field == "queue" {
			request.Start.Queue = strings.Repeat("q", 201)
		} else {
			request.Start.WorkflowType = strings.Repeat("w", 201)
		}
		if _, err := worker.SignalWithStart(t.Context(), request); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("unusable runtime routing accepted: %v", err)
		}
		if _, err := s.GetExecution(t.Context(), request.Start.Key); !errors.Is(err, durable.ErrNotFound) {
			t.Fatalf("invalid routing created run: %v", err)
		}
	}
}
