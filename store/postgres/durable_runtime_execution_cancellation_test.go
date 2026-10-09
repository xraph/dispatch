//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

type workflowCancellationRecoveryStore struct {
	durable.Store
	accepted       durable.CancelExecutionReceipt
	lostAcceptance bool
	phases         map[string]string
	retries        int
	conflicts      int
	beforeFence    func()
}

func (s *workflowCancellationRecoveryStore) RequestCancelExecution(ctx context.Context, r durable.CancelExecutionRequest) (durable.CancelExecutionReceipt, error) {
	receipt, err := s.Store.RequestCancelExecution(ctx, r)
	if err == nil && !s.lostAcceptance {
		s.lostAcceptance, s.accepted = true, receipt
		return durable.CancelExecutionReceipt{}, errors.New("accepted cancellation response lost")
	}
	return receipt, err
}

func (s *workflowCancellationRecoveryStore) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
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
	if phase == drt.EventCancellationStarted && s.beforeFence != nil {
		before := s.beforeFence
		s.beforeFence = nil
		before()
	}
	digest, err := durable.Fingerprint("workflow-cancel", r)
	if err != nil {
		return durable.Receipt{}, err
	}
	if prior := s.phases[phase]; prior != "" {
		if prior != digest {
			return durable.Receipt{}, durable.ErrRequestConflict
		}
		s.retries++
	}
	receipt, err := s.Store.CommitTransition(ctx, r)
	if errors.Is(err, durable.ErrRevisionConflict) {
		s.conflicts++
	}
	if err == nil && s.phases[phase] == "" {
		s.phases[phase] = digest
		return durable.Receipt{}, errors.New("cancellation phase response lost")
	}
	return receipt, err
}

func postgresWorkflowCancellation(mode string) drt.WorkflowFunc {
	return func(w *drt.Workflow, _ []byte) ([]byte, error) {
		state := "pending"
		var root *drt.Future
		w.SetQueryHandler("status", func(_ []byte) ([]byte, error) { return []byte(state), nil })
		if mode != "default" {
			w.SetCancellationHandler(func(cleanup *drt.Workflow, _ durable.ExecutionCancellation) ([]byte, error) {
				value, err := root.Get()
				if errors.Is(err, drt.ErrWorkflowCancelled) {
					value = []byte("cancelled")
				} else if err != nil {
					return nil, err
				}
				state += ":" + string(value)
				base := state
				state = base + ":cleaning"
				if _, err = cleanup.Activity("cleanup", "cleanup", "", nil).Get(); err != nil {
					return nil, err
				}
				state = base + ":cleaned"
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
		root = w.ActivityWithOptions("root", "work", "", nil, drt.ActivityOptions{StartToCloseTimeout: time.Minute})
		value, err := root.Get()
		if err != nil {
			return nil, err
		}
		state = string(value)
		return w.Timer("later", time.Hour).Get()
	}
}

func TestDurableRuntimeWorkflowCancellationRecovery(t *testing.T) {
	for _, mode := range []string{"default", "success", "failure", "cancelled", "async", "callback_race", "active"} {
		t.Run(mode, func(t *testing.T) {
			s, dsn := setupTestStoreConnection(t)
			lost := &workflowCancellationRecoveryStore{Store: s, phases: make(map[string]string)}
			options := drt.Options{Namespace: t.Name(), Queue: "orders", BuildID: "whole-cancel-v1", Owner: "initial", LeaseDuration: time.Second, StoreTimeout: 200 * time.Millisecond, Workflows: map[string]drt.WorkflowFunc{"order": postgresWorkflowCancellation(mode)}, Activities: make(map[string]drt.ActivityFunc)}
			var handle drt.AsyncActivityHandle
			started := make(chan struct{})
			options.Activities["work"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
				if err := info.Heartbeat(ctx, []byte("progress")); err != nil {
					return nil, err
				}
				if mode == "active" {
					close(started)
					<-ctx.Done()
					return []byte("stale"), nil
				}
				var err error
				handle, err = info.DeferCompletion(ctx)
				return nil, err
			}
			cleanupCalls := 0
			options.Activities["cleanup"] = func(_ context.Context, _ drt.ActivityInfo, _ []byte) ([]byte, error) { cleanupCalls++; return nil, nil }
			newWorker := func() *drt.Worker {
				w, err := drt.NewWorker(lost, options)
				if err != nil {
					t.Fatal(err)
				}
				return w
			}
			worker := newWorker()
			key := durable.Key{Namespace: options.Namespace, WorkflowID: "order", RunID: "run"}
			if _, err := worker.StartExecution(t.Context(), durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "order", BuildID: options.BuildID, Queue: options.Queue}); err != nil {
				t.Fatal(err)
			}
			runQueryTask(t, worker, durable.TaskWorkflow)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			done := make(chan error, 1)
			if mode == "active" {
				go func() { _, err := worker.RunOnce(ctx, durable.TaskActivity); done <- err }()
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("activity did not start")
				}
			} else if mode == "async" || mode == "callback_race" {
				runQueryTask(t, worker, durable.TaskActivity)
			}
			progress := drt.AsyncHeartbeatRequest{Handle: handle, RequestID: "progress", Sequence: handle.InitialHeartbeatSequence + 1, Details: []byte("saved")}
			var heartbeat durable.Receipt
			if mode == "async" || mode == "callback_race" {
				var err error
				heartbeat, err = worker.HeartbeatAsyncActivity(t.Context(), progress)
				if err != nil {
					t.Fatal(err)
				}
			}
			request := durable.CancelExecutionRequest{Key: key, RequestID: "cancel", BuildID: options.BuildID, Reason: "requested"}
			receipt, err := worker.RequestCancelExecution(t.Context(), request)
			if err != nil || receipt != lost.accepted {
				t.Fatalf("accepted receipt: %+v %v", receipt, err)
			}
			query := drt.QueryRequest{Key: key, BuildID: options.BuildID, Name: "status"}
			checkPostgresQuery(t, s, worker, query, "pending", durable.StateRunning)
			if mode != "active" {
				s = reopenAsyncStore(t, s, dsn)
				lost.Store = s
				options.Owner = "before-fence"
				worker = newWorker()
			}
			callback := drt.AsyncCompletionRequest{Handle: handle, RequestID: "result", Output: []byte("finished")}
			var callbackReceipt durable.Receipt
			if mode == "callback_race" {
				rival, newErr := drt.NewWorker(s, options)
				if newErr != nil {
					t.Fatal(newErr)
				}
				lost.beforeFence = func() {
					var callbackErr error
					callbackReceipt, callbackErr = rival.CompleteAsyncActivity(t.Context(), callback)
					if callbackErr != nil {
						t.Fatal(callbackErr)
					}
				}
			}
			runQueryTask(t, worker, durable.TaskWorkflow)
			root, err := s.GetTask(t.Context(), key, "command:1")
			if err != nil || !root.Done {
				t.Fatalf("root fence: %+v %v", root, err)
			}
			if _, err = s.GetTask(t.Context(), key, "command:2"); !errors.Is(err, durable.ErrNotFound) {
				t.Fatalf("speculative cleanup task: %v", err)
			}
			if mode == "active" {
				select {
				case err = <-done:
					if !errors.Is(err, durable.ErrLeaseLost) {
						t.Fatalf("handler fence: %v", err)
					}
				case <-ctx.Done():
					t.Fatal("activity did not stop")
				}
			}
			s = reopenAsyncStore(t, s, dsn)
			lost.Store = s
			options.Owner = "after-fence"
			worker = newWorker()
			want := "pending:cancelled"
			if mode == "callback_race" {
				want = "pending:finished"
			}
			if mode != "default" {
				checkPostgresQuery(t, s, worker, query, want+":cleaning", durable.StateRunning)
			}
			if mode == "async" || mode == "callback_race" {
				lateProgress := progress
				lateProgress.RequestID, lateProgress.Sequence = "fenced", progress.Sequence+1
				if _, err = worker.HeartbeatAsyncActivity(t.Context(), lateProgress); !errors.Is(err, durable.ErrLeaseLost) {
					t.Fatalf("heartbeat escaped live fence: %v", err)
				}
			}
			second := request
			second.RequestID, second.Reason = "second", "cannot replace first"
			if _, err = worker.RequestCancelExecution(t.Context(), second); err != nil {
				t.Fatal(err)
			}
			runQueryTask(t, worker, durable.TaskWorkflow)
			state := durable.StateCancelled
			if mode != "default" {
				checkPostgresQuery(t, s, worker, query, want+":cleaning", durable.StateRunning)
				runQueryTask(t, worker, durable.TaskActivity)
				s = reopenAsyncStore(t, s, dsn)
				lost.Store = s
				options.Owner = "after-cleanup"
				worker = newWorker()
				runQueryTask(t, worker, durable.TaskWorkflow)
				want += ":cleaned"
				if mode == "success" {
					state = durable.StateCompleted
				}
				if mode == "failure" {
					state = durable.StateFailed
				}
			} else {
				want = "pending"
			}
			checkPostgresQuery(t, s, worker, query, want, state)
			wantCalls := 1
			if mode == "default" {
				wantCalls = 0
			}
			if cleanupCalls != wantCalls || lost.retries != 2 {
				t.Fatalf("calls/retries: %d/%d", cleanupCalls, lost.retries)
			}
			wantConflicts := 0
			if mode == "callback_race" {
				wantConflicts = 1
			}
			if lost.conflicts != wantConflicts {
				t.Fatalf("fence conflicts: %d", lost.conflicts)
			}
			if got, retryErr := worker.RequestCancelExecution(t.Context(), request); retryErr != nil || got != receipt {
				t.Fatalf("closed acceptance receipt: %+v %v", got, retryErr)
			}
			if mode == "async" || mode == "callback_race" {
				if got, retryErr := worker.HeartbeatAsyncActivity(t.Context(), progress); retryErr != nil || got != heartbeat {
					t.Fatalf("saved heartbeat: %+v %v", got, retryErr)
				}
				got, callbackErr := worker.CompleteAsyncActivity(t.Context(), callback)
				if mode == "callback_race" {
					if callbackErr != nil || got != callbackReceipt {
						t.Fatalf("saved callback: %+v %v", got, callbackErr)
					}
				} else if !errors.Is(callbackErr, durable.ErrLeaseLost) {
					t.Fatalf("late callback: %v", callbackErr)
				}
				progress.RequestID, progress.Sequence = "late", progress.Sequence+1
				if _, err = worker.HeartbeatAsyncActivity(t.Context(), progress); !errors.Is(err, durable.ErrClosed) {
					t.Fatalf("late heartbeat: %v", err)
				}
			}
			events, err := s.ReadHistory(t.Context(), key, 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			fences := 0
			for _, event := range events {
				if event.Type == drt.EventCancellationStarted {
					fences++
				}
				if mode == "active" && event.Type == drt.EventActivityCompleted {
					var outcome drt.Outcome
					if err = json.Unmarshal(event.Payload, &outcome); err != nil {
						t.Fatal(err)
					}
					if outcome.CommandID == "root" {
						t.Fatal("stale root result committed")
					}
				}
			}
			if fences != 1 {
				t.Fatalf("repeated cleanup fence: %d", fences)
			}
		})
	}
}
