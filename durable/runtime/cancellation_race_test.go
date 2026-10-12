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

type cancellationFaultStore struct {
	durable.Store
	before    func()
	conflicts int
	lost      bool
	digest    string
	retries   int
}

func (s *cancellationFaultStore) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
	cancellation := false
	for _, event := range r.Events {
		if event.Type == drt.EventFutureCancelled {
			cancellation = true
		}
	}
	if cancellation && s.before != nil {
		before := s.before
		s.before = nil
		before()
	}
	if cancellation && s.lost {
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
	if cancellation && (errors.Is(err, durable.ErrRevisionConflict) || errors.Is(err, durable.ErrTaskConflict)) {
		s.conflicts++
	}
	if cancellation && err == nil && !s.lost {
		s.lost = true
		s.digest, err = durable.Fingerprint("cancellation", r)
		if err != nil {
			return durable.Receipt{}, err
		}
		return durable.Receipt{}, errors.New("cancellation response lost after commit")
	}
	return receipt, err
}

func TestCancelWorkerRace(t *testing.T) {
	for _, mode := range []string{"claim", "completion", "heartbeat", "retry_activation"} {
		t.Run(mode, func(t *testing.T) {
			base := memory.New()
			s := &cancellationFaultStore{Store: base}
			options := workerOptions(t)
			kind := "activity"
			if mode == "heartbeat" || mode == "retry_activation" {
				kind = "v2"
			}
			options.Workflows["order"] = cancelTargetWorkflow(kind, true)
			var handle drt.AsyncActivityHandle
			options.Activities["work"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
				if mode == "completion" {
					return []byte("finished"), nil
				}
				if mode == "retry_activation" && info.Attempt == 1 {
					return nil, &drt.ApplicationError{Type: "retry"}
				}
				if err := info.Heartbeat(ctx, []byte("progress")); err != nil {
					return nil, err
				}
				var err error
				handle, err = info.DeferCompletion(ctx)
				return nil, err
			}
			worker := newWorker(t, s, options)
			rival := newWorker(t, base, options)
			key := startWorkerRun(t, worker, options)
			runTask(t, worker, durable.TaskWorkflow)
			if mode == "heartbeat" || mode == "retry_activation" {
				runTask(t, rival, durable.TaskActivity)
			}
			var progress drt.AsyncHeartbeatRequest
			var heartbeatReceipt durable.Receipt
			s.before = func() {
				switch mode {
				case "claim":
					_, err := base.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: options.Namespace, BuildID: options.BuildID, Queue: options.Queue, Kind: durable.TaskActivity, Owner: "racing", LeaseDuration: time.Second})
					if err != nil {
						t.Fatal(err)
					}
				case "completion":
					runTask(t, rival, durable.TaskActivity)
				case "retry_activation":
					task, err := base.GetTask(t.Context(), key, "command:1")
					if err != nil {
						t.Fatal(err)
					}
					time.Sleep(max(0, time.Until(task.AvailableAt)) + time.Millisecond)
					runTask(t, rival, durable.TaskActivity)
				case "heartbeat":
					progress = drt.AsyncHeartbeatRequest{Handle: handle, RequestID: "progress", Sequence: handle.InitialHeartbeatSequence + 1, Details: []byte("new")}
					var err error
					heartbeatReceipt, err = rival.HeartbeatAsyncActivity(t.Context(), progress)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			sendCancelSignal(t, worker, key, options.BuildID)
			runTask(t, worker, durable.TaskWorkflow)
			if s.conflicts != 1 || !s.lost || s.retries != 1 {
				t.Fatalf("faults: conflicts=%d lost=%t retries=%d", s.conflicts, s.lost, s.retries)
			}
			if mode == "heartbeat" {
				if got, err := rival.HeartbeatAsyncActivity(t.Context(), progress); err != nil || got != heartbeatReceipt {
					t.Fatalf("heartbeat receipt lost: %+v %v", got, err)
				}
				progress.RequestID = "late"
				progress.Sequence++
				if _, err := rival.HeartbeatAsyncActivity(t.Context(), progress); !errors.Is(err, durable.ErrLeaseLost) {
					t.Fatalf("late heartbeat accepted: %v", err)
				}
			}
			runTask(t, worker, durable.TaskWorkflow)
			want := map[string]string{"claim": "cancelled", "completion": "finished", "heartbeat": "cancelled:new", "retry_activation": "cancelled:progress"}[mode]
			checkCancelledExecution(t, s, worker, key, options.BuildID, want)
		})
	}
}

func TestCancelWorkerActiveContext(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.LeaseDuration = 300 * time.Millisecond
	options.StoreTimeout = 50 * time.Millisecond
	options.Workflows["order"] = cancelTargetWorkflow("v2", true)
	started := make(chan struct{})
	stopped := make(chan error, 1)
	options.Activities["work"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		if err := info.Heartbeat(ctx, []byte("progress")); err != nil {
			return nil, err
		}
		close(started)
		<-ctx.Done()
		stopped <- context.Cause(ctx)
		return []byte("stale"), nil
	}
	worker := newWorker(t, s, options)
	key := startWorkerRun(t, worker, options)
	runTask(t, worker, durable.TaskWorkflow)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := worker.RunOnce(ctx, durable.TaskActivity); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("activity did not start")
	}
	sendCancelSignal(t, worker, key, options.BuildID)
	runTask(t, worker, durable.TaskWorkflow)
	select {
	case err := <-stopped:
		if !errors.Is(err, durable.ErrLeaseLost) {
			t.Fatalf("handler cancellation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("handler did not observe fencing")
	}
	if err := <-done; !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("activity result: %v", err)
	}
	runTask(t, worker, durable.TaskWorkflow)
	checkCancelledExecution(t, s, worker, key, options.BuildID, "cancelled:progress")
	events, err := s.ReadHistory(t.Context(), key, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Type == drt.EventActivityCompleted {
			t.Fatal("stale handler outcome committed")
		}
	}
}

type cancellationObservationStore struct {
	durable.Store
	before func()
}

func (s *cancellationObservationStore) GetTask(ctx context.Context, key durable.Key, id string) (durable.Task, error) {
	if id == "command:1" && s.before != nil {
		before := s.before
		s.before = nil
		before()
	}
	return s.Store.GetTask(ctx, key, id)
}

func TestCancelWorkerSnapshotRace(t *testing.T) {
	base := memory.New()
	s := &cancellationObservationStore{Store: base}
	options := workerOptions(t)
	options.Workflows["order"] = cancelTargetWorkflow("v2", true)
	var handle drt.AsyncActivityHandle
	options.Activities["work"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		if err := info.Heartbeat(ctx, []byte("progress")); err != nil {
			return nil, err
		}
		var err error
		handle, err = info.DeferCompletion(ctx)
		return nil, err
	}
	worker := newWorker(t, s, options)
	rival := newWorker(t, base, options)
	key := startWorkerRun(t, worker, options)
	runTask(t, worker, durable.TaskWorkflow)
	runTask(t, rival, durable.TaskActivity)
	s.before = func() {
		if _, err := rival.CompleteAsyncActivity(t.Context(), drt.AsyncCompletionRequest{Handle: handle, RequestID: "retry", Failure: &drt.ApplicationError{Type: "retry"}}); err != nil {
			t.Fatal(err)
		}
		task, err := base.GetTask(t.Context(), key, "command:1")
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(max(0, time.Until(task.AvailableAt)) + time.Millisecond)
		runTask(t, rival, durable.TaskActivity)
	}
	sendCancelSignal(t, worker, key, options.BuildID)
	runTask(t, worker, durable.TaskWorkflow)
	runTask(t, worker, durable.TaskWorkflow)
	checkCancelledExecution(t, s, worker, key, options.BuildID, "cancelled:progress")
}
