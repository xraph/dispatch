//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

type lostCancellationResponseStore struct {
	durable.Store
	before    func()
	lost      bool
	retries   int
	conflicts int
	digest    string
}

func (s *lostCancellationResponseStore) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
	cancel := false
	for _, event := range r.Events {
		if event.Type == drt.EventFutureCancelled {
			cancel = true
		}
	}
	if cancel && s.before != nil {
		before := s.before
		s.before = nil
		before()
	}
	if cancel && s.lost {
		digest, err := durable.Fingerprint("cancellation", r)
		if err != nil {
			return durable.Receipt{}, err
		}
		if digest != s.digest {
			return durable.Receipt{}, durable.ErrRequestConflict
		}
		s.retries++
	}
	receipt, err := s.Store.CommitTransition(ctx, r)
	if cancel && (errors.Is(err, durable.ErrRevisionConflict) || errors.Is(err, durable.ErrTaskConflict)) {
		s.conflicts++
	}
	if err == nil && cancel && !s.lost {
		s.lost = true
		s.digest, err = durable.Fingerprint("cancellation", r)
		if err != nil {
			return durable.Receipt{}, err
		}
		return durable.Receipt{}, errors.New("PostgreSQL cancellation acknowledgment lost")
	}
	return receipt, err
}

func postgresCancelWorkflow(mode string) drt.WorkflowFunc {
	return func(w *drt.Workflow, _ []byte) ([]byte, error) {
		state := "pending"
		w.SetQueryHandler("status", func(_ []byte) ([]byte, error) { return []byte(state), nil })
		var target *drt.Future
		switch mode {
		case "new_timer", "queued_timer":
			target = w.Timer("target", time.Hour)
		case "signal":
			target = w.ReceiveSignal("target", "approve")
		default:
			target = w.ActivityWithOptions("target", "work", "", nil, drt.ActivityOptions{StartToCloseTimeout: time.Minute})
		}
		if !strings.HasPrefix(mode, "new_") {
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
			if mode == "signal" {
				saved, readErr := w.ReceiveSignal("saved", "approve").Get()
				if readErr != nil {
					return nil, readErr
				}
				state += ":" + string(saved)
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

func TestDurableRuntimeCancelRecovery(t *testing.T) {
	for _, mode := range []string{"new_timer", "new_activity", "queued_timer", "signal", "async", "completion_race", "heartbeat_race", "active"} {
		t.Run(mode, func(t *testing.T) {
			s, dsn := setupTestStoreConnection(t)
			lost := &lostCancellationResponseStore{Store: s}
			options := drt.Options{Namespace: t.Name(), Queue: "orders", BuildID: "cancel-v1", Owner: "first", Workflows: map[string]drt.WorkflowFunc{"order": postgresCancelWorkflow(mode)}, Activities: make(map[string]drt.ActivityFunc)}
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
			if mode == "active" {
				options.LeaseDuration = time.Second
				options.StoreTimeout = 200 * time.Millisecond
			}
			worker, err := drt.NewWorker(lost, options)
			if err != nil {
				t.Fatal(err)
			}
			key := durable.Key{Namespace: options.Namespace, WorkflowID: "order", RunID: "run"}
			if _, err = worker.StartExecution(t.Context(), durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "order", BuildID: options.BuildID, Queue: options.Queue}); err != nil {
				t.Fatal(err)
			}
			runQueryTask(t, worker, durable.TaskWorkflow)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			if mode == "active" {
				go func() { _, runErr := worker.RunOnce(ctx, durable.TaskActivity); done <- runErr }()
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("activity did not start")
				}
			} else if mode == "async" || strings.HasSuffix(mode, "_race") {
				runQueryTask(t, worker, durable.TaskActivity)
			}
			callback := drt.AsyncCompletionRequest{Handle: handle, RequestID: "result", Output: []byte("finished")}
			var accepted durable.Receipt
			rival, err := drt.NewWorker(s, options)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "completion_race" {
				lost.before = func() {
					var callbackErr error
					accepted, callbackErr = rival.CompleteAsyncActivity(t.Context(), callback)
					if callbackErr != nil {
						t.Fatal(callbackErr)
					}
				}
			}
			if mode == "heartbeat_race" {
				lost.before = func() {
					if _, heartbeatErr := rival.HeartbeatAsyncActivity(t.Context(), drt.AsyncHeartbeatRequest{Handle: handle, RequestID: "progress", Sequence: handle.InitialHeartbeatSequence + 1, Details: []byte("new")}); heartbeatErr != nil {
						t.Fatal(heartbeatErr)
					}
				}
			}
			if !strings.HasPrefix(mode, "new_") {
				if mode == "signal" {
					if _, err = worker.SignalExecution(t.Context(), durable.SignalRequest{Key: key, RequestID: "approval", BuildID: options.BuildID, Name: "approve", Input: []byte("saved")}); err != nil {
						t.Fatal(err)
					}
				}
				if _, err = worker.SignalExecution(t.Context(), durable.SignalRequest{Key: key, RequestID: "cancel", BuildID: options.BuildID, Name: "cancel"}); err != nil {
					t.Fatal(err)
				}
				runQueryTask(t, worker, durable.TaskWorkflow)
			}
			if !lost.lost || lost.retries != 1 {
				t.Fatalf("lost acknowledgment: lost=%t retries=%d", lost.lost, lost.retries)
			}
			expectedConflicts := 0
			if strings.HasSuffix(mode, "_race") {
				expectedConflicts = 1
			}
			if lost.conflicts != expectedConflicts {
				t.Fatalf("conflicts: %d want %d", lost.conflicts, expectedConflicts)
			}
			target, err := s.GetTask(t.Context(), key, "command:1")
			if strings.HasPrefix(mode, "new_") || mode == "signal" {
				if !errors.Is(err, durable.ErrNotFound) {
					t.Fatalf("canceled task escaped: %+v %v", target, err)
				}
			} else if err != nil || !target.Done {
				t.Fatalf("target not fenced: %+v %v", target, err)
			}
			if mode == "active" {
				select {
				case runErr := <-done:
					if !errors.Is(runErr, durable.ErrLeaseLost) {
						t.Fatalf("active cancellation: %v", runErr)
					}
				case <-ctx.Done():
					t.Fatal("activity did not stop")
				}
			}
			s = reopenAsyncStore(t, s, dsn)
			options.Owner = "replacement"
			worker, err = drt.NewWorker(s, options)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "async" || mode == "heartbeat_race" {
				if _, err = worker.CompleteAsyncActivity(t.Context(), callback); !errors.Is(err, durable.ErrLeaseLost) {
					t.Fatalf("stale callback accepted: %v", err)
				}
			}
			runQueryTask(t, worker, durable.TaskWorkflow)
			want := "cancelled"
			switch mode {
			case "async", "active":
				want += ":progress"
			case "heartbeat_race":
				want += ":new"
			case "completion_race":
				want = "finished"
			case "signal":
				want += ":saved"
			}
			checkPostgresQuery(t, s, worker, drt.QueryRequest{Key: key, BuildID: options.BuildID, Name: "status"}, want, durable.StateCompleted)
			if mode == "completion_race" {
				if again, callbackErr := worker.CompleteAsyncActivity(t.Context(), callback); callbackErr != nil || again != accepted {
					t.Fatalf("accepted callback receipt: %+v %v", again, callbackErr)
				}
			}
			events, err := s.ReadHistory(t.Context(), key, 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			acknowledgments := 0
			for _, event := range events {
				if event.Type == drt.EventFutureCancelled {
					acknowledgments++
				}
				if mode == "active" && event.Type == drt.EventActivityCompleted {
					t.Fatal("stale handler result committed")
				}
			}
			if acknowledgments != 1 {
				t.Fatalf("acknowledgments: %d", acknowledgments)
			}
		})
	}
}
