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

type workflowCancellationFaultStore struct {
	durable.Store
	acceptanceLost bool
	accepted       durable.CancelExecutionReceipt
	commits        map[string]string
	retries        int
}

func (s *workflowCancellationFaultStore) RequestCancelExecution(ctx context.Context, r durable.CancelExecutionRequest) (durable.CancelExecutionReceipt, error) {
	receipt, err := s.Store.RequestCancelExecution(ctx, r)
	if err != nil {
		return receipt, err
	}
	if !s.acceptanceLost {
		s.acceptanceLost = true
		s.accepted = receipt
		return durable.CancelExecutionReceipt{}, errors.New("accepted cancellation response lost")
	}
	return receipt, nil
}
func (s *workflowCancellationFaultStore) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
	phase := ""
	for _, event := range r.Events {
		switch event.Type {
		case drt.EventCancellationStarted, drt.EventWorkflowCancelled, drt.EventWorkflowCompleted, drt.EventWorkflowFailed:
			phase = event.Type
		}
	}
	if phase == "" {
		return s.Store.CommitTransition(ctx, r)
	}
	digest, err := durable.Fingerprint("workflow-cancel", r)
	if err != nil {
		return durable.Receipt{}, err
	}
	if prior, ok := s.commits[phase]; ok {
		if prior != digest {
			return durable.Receipt{}, durable.ErrRequestConflict
		}
		s.retries++
	}
	receipt, err := s.Store.CommitTransition(ctx, r)
	if err == nil && s.commits[phase] == "" {
		s.commits[phase] = digest
		return durable.Receipt{}, errors.New("workflow cancellation phase response lost")
	}
	return receipt, err
}

func workflowCancelHandler(mode string) drt.WorkflowFunc {
	return func(w *drt.Workflow, _ []byte) ([]byte, error) {
		state := "waiting"
		w.SetQueryHandler("status", func(_ []byte) ([]byte, error) { return []byte(state), nil })
		if mode != "default" {
			w.SetCancellationHandler(func(cleanup *drt.Workflow, request durable.ExecutionCancellation) ([]byte, error) {
				state = "cleaning:" + request.Reason
				if _, err := cleanup.Activity("cleanup", "cleanup", "", nil).Get(); err != nil {
					return nil, err
				}
				state = "cleaned:" + request.Reason
				switch mode {
				case "success":
					return []byte(state), nil
				case "failure":
					return nil, &drt.ApplicationError{Type: "cleanup_failed", Message: "cleanup failed"}
				default:
					return nil, drt.ErrWorkflowCancelled
				}
			})
		}
		return w.Timer("normal", time.Hour).Get()
	}
}

func TestWorkflowCancelWorkerPhases(t *testing.T) {
	for _, early := range []bool{false, true} {
		for _, mode := range []string{"default", "success", "failure", "cancelled"} {
			t.Run(fmt.Sprintf("%s/early=%t", mode, early), func(t *testing.T) {
				base := memory.New()
				s := &workflowCancellationFaultStore{Store: base, commits: make(map[string]string)}
				options := workerOptions(t)
				options.Workflows["order"] = workflowCancelHandler(mode)
				calls := 0
				options.Activities["cleanup"] = func(_ context.Context, _ drt.ActivityInfo, _ []byte) ([]byte, error) { calls++; return nil, nil }
				worker := newWorker(t, s, options)
				key := startWorkerRun(t, worker, options)
				if !early {
					runTask(t, worker, durable.TaskWorkflow)
				}
				request := durable.CancelExecutionRequest{Key: key, RequestID: "cancel", BuildID: options.BuildID, Reason: "first"}
				receipt, err := worker.RequestCancelExecution(t.Context(), request)
				if err != nil || receipt != s.accepted {
					t.Fatalf("accepted request: %+v %v", receipt, err)
				}
				request.RequestID, request.Reason = "again", "second"
				if _, err = worker.RequestCancelExecution(t.Context(), request); err != nil {
					t.Fatal(err)
				}
				runTask(t, worker, durable.TaskWorkflow)
				cleanupIndex := 2
				if early {
					cleanupIndex = 1
				}
				if _, err = base.GetTask(t.Context(), key, fmt.Sprintf("command:%d", cleanupIndex)); !errors.Is(err, durable.ErrNotFound) {
					t.Fatalf("cleanup published before fence acknowledgment: %v", err)
				}
				if !early {
					target, readErr := base.GetTask(t.Context(), key, "command:1")
					if readErr != nil || !target.Done {
						t.Fatalf("normal task not fenced: %+v %v", target, readErr)
					}
				}
				options.Owner = "replacement"
				worker = newWorker(t, s, options)
				runTask(t, worker, durable.TaskWorkflow)
				if mode != "default" {
					runTask(t, worker, durable.TaskActivity)
					options.Owner = "after-cleanup"
					worker = newWorker(t, s, options)
					runTask(t, worker, durable.TaskWorkflow)
				}
				wantState := durable.StateCancelled
				want := "cleaned:first"
				switch mode {
				case "success":
					wantState = durable.StateCompleted
				case "failure":
					wantState = durable.StateFailed
				case "default":
					want = "waiting"
				}
				q, err := worker.QueryExecution(t.Context(), drt.QueryRequest{Key: key, BuildID: options.BuildID, Name: "status"})
				if err != nil || q.State != wantState || string(q.Output) != want {
					t.Fatalf("terminal query: %+v %v", q, err)
				}
				wantCalls := 1
				if mode == "default" {
					wantCalls = 0
				}
				if calls != wantCalls || s.retries != 2 {
					t.Fatalf("calls=%d retries=%d", calls, s.retries)
				}
				request.RequestID, request.Reason = "cancel", "first"
				if again, retryErr := worker.RequestCancelExecution(t.Context(), request); retryErr != nil || again != receipt {
					t.Fatalf("closed receipt: %+v %v", again, retryErr)
				}
			})
		}
	}
}

func TestWorkflowCancelClientRouting(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = workflowCancelHandler("default")
	worker := newWorker(t, s, options)
	key := startWorkerRun(t, worker, options)
	request := durable.CancelExecutionRequest{Key: key, RequestID: "cancel", BuildID: options.BuildID}
	options.Namespace = "foreign"
	foreign := newWorker(t, s, options)
	if _, err := foreign.RequestCancelExecution(t.Context(), request); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("foreign namespace: %v", err)
	}
	options.Namespace = key.Namespace
	options.BuildID = "other"
	foreign = newWorker(t, s, options)
	if _, err := foreign.RequestCancelExecution(t.Context(), request); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("foreign build: %v", err)
	}
	e, err := s.GetExecution(t.Context(), key)
	if err != nil || e.Revision != 1 {
		t.Fatalf("rejected client changed state: %+v %v", e, err)
	}
}

func TestWorkflowCancelAsyncBoundary(t *testing.T) {
	for _, completed := range []bool{false, true} {
		t.Run(fmt.Sprint(completed), func(t *testing.T) {
			s := memory.New()
			options := workerOptions(t)
			options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
				state := "waiting"
				var work *drt.Future
				w.SetCancellationHandler(func(_ *drt.Workflow, _ durable.ExecutionCancellation) ([]byte, error) {
					result, err := work.Get()
					if errors.Is(err, drt.ErrWorkflowCancelled) {
						result = []byte("cancelled")
					} else if err != nil {
						return nil, err
					}
					return []byte(state + ":" + string(result)), nil
				})
				work = w.ActivityWithOptions("work", "work", "", nil, asyncOptions(2))
				value, err := work.Get()
				if err != nil {
					return nil, err
				}
				state = string(value)
				return w.Timer("later", time.Hour).Get()
			}
			var handle drt.AsyncActivityHandle
			options.Activities["work"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
				var err error
				handle, err = info.DeferCompletion(ctx)
				return nil, err
			}
			worker := newWorker(t, s, options)
			key := startWorkerRun(t, worker, options)
			runTask(t, worker, durable.TaskWorkflow)
			runTask(t, worker, durable.TaskActivity)
			progress := drt.AsyncHeartbeatRequest{Handle: handle, RequestID: "progress", Sequence: handle.InitialHeartbeatSequence + 1, Details: []byte("saved")}
			hb, err := worker.HeartbeatAsyncActivity(t.Context(), progress)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = worker.RequestCancelExecution(t.Context(), durable.CancelExecutionRequest{Key: key, BuildID: options.BuildID, RequestID: "cancel"}); err != nil {
				t.Fatal(err)
			}
			callback := drt.AsyncCompletionRequest{Handle: handle, RequestID: "callback", Output: []byte("done")}
			var receipt durable.Receipt
			if completed {
				receipt, err = worker.CompleteAsyncActivity(t.Context(), callback)
				if err != nil {
					t.Fatal(err)
				}
			}
			runTask(t, worker, durable.TaskWorkflow)
			lateProgress := progress
			lateProgress.RequestID, lateProgress.Sequence = "fenced", progress.Sequence+1
			if _, err = worker.HeartbeatAsyncActivity(t.Context(), lateProgress); !errors.Is(err, durable.ErrLeaseLost) {
				t.Fatalf("heartbeat escaped live fence: %v", err)
			}
			// A second accepted request after the fence must not restart cleanup.
			if _, err = worker.RequestCancelExecution(t.Context(), durable.CancelExecutionRequest{Key: key, BuildID: options.BuildID, RequestID: "again"}); err != nil {
				t.Fatal(err)
			}
			runTask(t, worker, durable.TaskWorkflow)
			e, err := s.GetExecution(t.Context(), key)
			want := "waiting:cancelled"
			if completed {
				want = "waiting:done"
			}
			if err != nil || e.State != durable.StateCompleted || string(e.Output) != want {
				t.Fatalf("frozen state: %+v %v", e, err)
			}
			if got, replayErr := worker.HeartbeatAsyncActivity(t.Context(), progress); replayErr != nil || got != hb {
				t.Fatalf("heartbeat receipt: %+v %v", got, replayErr)
			}
			got, err := worker.CompleteAsyncActivity(t.Context(), callback)
			if completed {
				if err != nil || got != receipt {
					t.Fatalf("callback receipt: %+v %v", got, err)
				}
			} else if !errors.Is(err, durable.ErrLeaseLost) {
				t.Fatalf("callback escaped fence: %v", err)
			}
			progress.RequestID, progress.Sequence = "late", progress.Sequence+1
			if _, err = worker.HeartbeatAsyncActivity(t.Context(), progress); !errors.Is(err, durable.ErrClosed) {
				t.Fatalf("heartbeat after closure: %v", err)
			}
		})
	}
}

func TestWorkflowCancelActiveHandler(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		return w.ActivityWithOptions("work", "work", "", nil, asyncOptions(2)).Get()
	}
	started := make(chan struct{})
	options.Activities["work"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		if err := info.Heartbeat(ctx, []byte("progress")); err != nil {
			return nil, err
		}
		close(started)
		<-ctx.Done()
		return []byte("stale"), nil
	}
	worker := newWorker(t, s, options)
	key := startWorkerRun(t, worker, options)
	runTask(t, worker, durable.TaskWorkflow)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := worker.RunOnce(ctx, durable.TaskActivity); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("handler did not start")
	}
	if _, err := worker.RequestCancelExecution(t.Context(), durable.CancelExecutionRequest{Key: key, BuildID: options.BuildID, RequestID: "cancel"}); err != nil {
		t.Fatal(err)
	}
	runTask(t, worker, durable.TaskWorkflow)
	select {
	case err := <-done:
		if !errors.Is(err, durable.ErrLeaseLost) {
			t.Fatalf("active handler fence: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("handler did not observe fence")
	}
	runTask(t, worker, durable.TaskWorkflow)
	events, err := s.ReadHistory(t.Context(), key, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Type == drt.EventActivityCompleted {
			t.Fatal("stale result committed")
		}
	}
	e, err := s.GetExecution(t.Context(), key)
	if err != nil || e.State != durable.StateCancelled {
		t.Fatalf("terminal: %+v %v", e, err)
	}
}

func TestWorkflowCancelCleanupCancelsFutures(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		var root *drt.Future
		w.SetCancellationHandler(func(cleanup *drt.Workflow, _ durable.ExecutionCancellation) ([]byte, error) {
			if _, err := cleanup.Cancel("stop-root-again", root).Get(); err != nil {
				return nil, err
			}
			if _, err := root.Get(); !errors.Is(err, drt.ErrWorkflowCancelled) {
				return nil, errors.New("root cancellation changed")
			}
			target := cleanup.Timer("cleanup-timer", time.Hour)
			if _, err := cleanup.Cancel("stop-cleanup", target).Get(); err != nil {
				return nil, err
			}
			if _, err := target.Get(); !errors.Is(err, drt.ErrCancelled) {
				return nil, errors.New("cleanup cancellation missing")
			}
			return nil, drt.ErrWorkflowCancelled
		})
		root = w.Timer("root", time.Hour)
		return root.Get()
	}
	worker := newWorker(t, s, options)
	key := startWorkerRun(t, worker, options)
	runTask(t, worker, durable.TaskWorkflow)
	if _, err := worker.RequestCancelExecution(t.Context(), durable.CancelExecutionRequest{Key: key, BuildID: options.BuildID, RequestID: "cancel"}); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		runTask(t, worker, durable.TaskWorkflow)
	}
	e, err := s.GetExecution(t.Context(), key)
	if err != nil || e.State != durable.StateCancelled {
		t.Fatalf("cleanup cancellation: %+v %v", e, err)
	}
	events, err := s.ReadHistory(t.Context(), key, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = drt.Evaluate(e, events, options.Workflows["order"]); err != nil {
		t.Fatal(err)
	}
}
