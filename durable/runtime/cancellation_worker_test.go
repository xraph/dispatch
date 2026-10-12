package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

func cancelTargetWorkflow(kind string, wait bool) drt.WorkflowFunc {
	return func(w *drt.Workflow, _ []byte) ([]byte, error) {
		state := "pending"
		w.SetQueryHandler("status", func(_ []byte) ([]byte, error) { return []byte(state), nil })
		var target *drt.Future
		switch kind {
		case "timer":
			target = w.Timer("target", time.Hour)
		case "signal":
			target = w.ReceiveSignal("target", "approve")
		case "v2":
			target = w.ActivityWithOptions("target", "work", "", nil, asyncOptions(2))
		default:
			target = w.Activity("target", "work", "", nil)
		}
		if wait {
			if _, err := w.ReceiveSignal("go", "cancel").Get(); err != nil {
				return nil, err
			}
		}
		if _, err := w.Cancel("stop", target).Get(); err != nil {
			return nil, err
		}
		value, err := target.Get()
		var cancelled *drt.CancelledError
		if errors.As(err, &cancelled) {
			state = "cancelled"
			if cancelled.Heartbeat != nil {
				state += ":" + string(cancelled.Heartbeat.Details)
			}
			return []byte(state), nil
		}
		if err != nil {
			return nil, err
		}
		state = string(value)
		return value, nil
	}
}

func sendCancelSignal(t *testing.T, w *drt.Worker, key durable.Key, build string) {
	t.Helper()
	if _, err := w.SignalExecution(t.Context(), durable.SignalRequest{Key: key, RequestID: "cancel", BuildID: build, Name: "cancel"}); err != nil {
		t.Fatal(err)
	}
}

func checkCancelledExecution(t *testing.T, s durable.Store, w *drt.Worker, key durable.Key, build, want string) {
	t.Helper()
	e, err := s.GetExecution(t.Context(), key)
	if err != nil || e.State != durable.StateCompleted || string(e.Output) != want {
		t.Fatalf("execution: %+v %v", e, err)
	}
	q, err := w.QueryExecution(t.Context(), drt.QueryRequest{Key: key, BuildID: build, Name: "status"})
	if err != nil || string(q.Output) != want {
		t.Fatalf("cancellation query: %+v %v", q, err)
	}
	events, err := s.ReadHistory(t.Context(), key, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.Type == drt.EventFutureCancelled {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("cancellation event count: %d", count)
	}
}

func TestCancelWorkerNewAndQueuedTargets(t *testing.T) {
	for _, wait := range []bool{false, true} {
		for _, kind := range []string{"activity", "timer", "signal"} {
			t.Run(kind+map[bool]string{false: "_new", true: "_queued"}[wait], func(t *testing.T) {
				s := memory.New()
				options := workerOptions(t)
				options.Workflows["order"] = cancelTargetWorkflow(kind, wait)
				options.Activities["work"] = func(_ context.Context, _ drt.ActivityInfo, _ []byte) ([]byte, error) {
					t.Error("canceled work escaped")
					return nil, nil
				}
				worker := newWorker(t, s, options)
				key := startWorkerRun(t, worker, options)
				runTask(t, worker, durable.TaskWorkflow)
				if wait {
					sendCancelSignal(t, worker, key, options.BuildID)
					runTask(t, worker, durable.TaskWorkflow)
				}
				task, err := s.GetTask(t.Context(), key, "command:1")
				if !wait || kind == "signal" {
					if !errors.Is(err, durable.ErrNotFound) {
						t.Fatalf("new canceled task published: %+v %v", task, err)
					}
				} else if err != nil || !task.Done {
					t.Fatalf("queued task not fenced: %+v %v", task, err)
				}
				if worked, err := worker.RunOnce(t.Context(), durable.TaskActivity); err != nil || worked {
					t.Fatalf("canceled activity polled: %t %v", worked, err)
				}
				options.Owner = "replacement"
				worker = newWorker(t, s, options)
				runTask(t, worker, durable.TaskWorkflow)
				checkCancelledExecution(t, s, worker, key, options.BuildID, "cancelled")
			})
		}
	}
}

func TestCancelWorkerRetryAndAsync(t *testing.T) {
	for _, mode := range []string{"retry", "async", "completed"} {
		t.Run(mode, func(t *testing.T) {
			s := memory.New()
			options := workerOptions(t)
			options.Workflows["order"] = cancelTargetWorkflow("v2", true)
			var handle drt.AsyncActivityHandle
			options.Activities["work"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
				if err := info.Heartbeat(ctx, []byte("progress")); err != nil {
					return nil, err
				}
				if mode == "retry" {
					return nil, &drt.ApplicationError{Type: "retry", Message: "retry"}
				}
				var err error
				handle, err = info.DeferCompletion(ctx)
				return nil, err
			}
			worker := newWorker(t, s, options)
			key := startWorkerRun(t, worker, options)
			runTask(t, worker, durable.TaskWorkflow)
			runTask(t, worker, durable.TaskActivity)
			request := drt.AsyncCompletionRequest{Handle: handle, RequestID: "result", Output: []byte("finished")}
			var receipt durable.Receipt
			var err error
			if mode == "completed" {
				receipt, err = worker.CompleteAsyncActivity(t.Context(), request)
				if err != nil {
					t.Fatal(err)
				}
			}
			sendCancelSignal(t, worker, key, options.BuildID)
			runTask(t, worker, durable.TaskWorkflow)
			task, err := s.GetTask(t.Context(), key, "command:1")
			if err != nil || !task.Done {
				t.Fatalf("target not done: %+v %v", task, err)
			}
			if mode == "async" {
				if _, err = worker.CompleteAsyncActivity(t.Context(), request); !errors.Is(err, durable.ErrLeaseLost) {
					t.Fatalf("stale callback accepted: %v", err)
				}
			}
			runTask(t, worker, durable.TaskWorkflow)
			want := "cancelled:progress"
			if mode == "completed" {
				want = "finished"
			}
			checkCancelledExecution(t, s, worker, key, options.BuildID, want)
			if mode == "completed" {
				if again, err := worker.CompleteAsyncActivity(t.Context(), request); err != nil || again != receipt {
					t.Fatalf("accepted callback receipt lost: %+v %v", again, err)
				}
			}
		})
	}
}

func TestCancelWorkerFireAndForget(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		w.Cancel("stop", w.Timer("target", time.Hour))
		return []byte("done"), nil
	}
	worker := newWorker(t, s, options)
	key := startWorkerRun(t, worker, options)
	runTask(t, worker, durable.TaskWorkflow)
	e, err := s.GetExecution(t.Context(), key)
	if err != nil || string(e.Output) != "done" {
		t.Fatalf("fire and forget: %+v %v", e, err)
	}
	events, err := s.ReadHistory(t.Context(), key, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = drt.Evaluate(e, events, options.Workflows["order"]); err != nil {
		t.Fatalf("closed cancellation replay: %v", err)
	}
	if _, err = s.GetTask(t.Context(), key, "command:1"); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("terminal canceled target published: %v", err)
	}
}

func TestCancelWorkerRepeatedRequestsAndSignalConsumption(t *testing.T) {
	for _, consume := range []bool{false, true} {
		t.Run(map[bool]string{false: "fenced", true: "consumed"}[consume], func(t *testing.T) {
			s := memory.New()
			options := workerOptions(t)
			options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
				target := w.ReceiveSignal("target", "approve")
				first := w.Cancel("first", target)
				if consume {
					if _, err := target.Get(); err != nil {
						return nil, err
					}
				}
				second := w.Cancel("second", target)
				if _, err := first.Get(); err != nil {
					return nil, err
				}
				if _, err := second.Get(); err != nil {
					return nil, err
				}
				value, err := target.Get()
				if !consume && errors.Is(err, drt.ErrCancelled) {
					return w.ReceiveSignal("saved", "approve").Get()
				}
				return value, err
			}
			worker := newWorker(t, s, options)
			key := startWorkerRun(t, worker, options)
			if _, err := worker.SignalExecution(t.Context(), durable.SignalRequest{Key: key, RequestID: "approval", BuildID: options.BuildID, Name: "approve", Input: []byte("saved")}); err != nil {
				t.Fatal(err)
			}
			runTask(t, worker, durable.TaskWorkflow)
			runTask(t, worker, durable.TaskWorkflow)
			e, err := s.GetExecution(t.Context(), key)
			if err != nil || e.State != durable.StateCompleted || string(e.Output) != "saved" {
				t.Fatalf("repeated cancel: %+v %v", e, err)
			}
			events, err := s.ReadHistory(t.Context(), key, 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = drt.Evaluate(e, events, options.Workflows["order"]); err != nil {
				t.Fatal(err)
			}
		})
	}
}
