package runtime_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

func TestActivityHeartbeatProgressRecoversOnRetry(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = retryWorkflow(retryOptions(2))
	var retained drt.ActivityInfo
	options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		retained = info
		if info.Attempt == 1 {
			if err := info.Heartbeat(ctx, []byte("offset:42")); err != nil {
				return nil, err
			}
			return nil, errors.New("retry")
		}
		details := info.HeartbeatDetails()
		if string(details) != "offset:42" {
			return nil, errors.New("heartbeat progress was lost")
		}
		details[0] = 'X'
		if string(info.HeartbeatDetails()) != "offset:42" {
			return nil, errors.New("heartbeat details alias activity")
		}
		return []byte("paid"), nil
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	runTask(t, w, durable.TaskActivity)
	task, err := s.GetTask(t.Context(), key, "command:1")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(task.AvailableAt) + time.Millisecond)
	runTask(t, w, durable.TaskActivity)
	if err = retained.Heartbeat(context.Background(), []byte("late")); !errors.Is(err, context.Canceled) {
		t.Fatalf("retained heartbeat callback remained usable: %v", err)
	}
	runTask(t, w, durable.TaskWorkflow)
	execution, err := s.GetExecution(t.Context(), key)
	if err != nil || execution.State != durable.StateCompleted || string(execution.Output) != "paid" {
		t.Fatalf("heartbeat recovery: %+v %v", execution, err)
	}
	outcome := lastActivityOutcome(t, s, key)
	if outcome.Heartbeat == nil || string(outcome.Heartbeat.Details) != "offset:42" || outcome.Heartbeat.Sequence != 0 {
		t.Fatalf("missing inherited checkpoint: %+v", outcome)
	}
}

type uncertainHeartbeatStore struct {
	durable.Store
	mu       sync.Mutex
	requests []durable.HeartbeatRequest
}

func (s *uncertainHeartbeatStore) RecordHeartbeat(ctx context.Context, r durable.HeartbeatRequest) (durable.Receipt, error) {
	s.mu.Lock()
	s.requests = append(s.requests, r)
	n := len(s.requests)
	s.mu.Unlock()
	receipt, err := s.Store.RecordHeartbeat(ctx, r)
	if err == nil && n <= 3 {
		return durable.Receipt{}, errors.New("response lost")
	}
	return receipt, err
}

func TestActivityHeartbeatResolvesUncertaintyBeforeConcurrentProgress(t *testing.T) {
	s := &uncertainHeartbeatStore{Store: memory.New()}
	options := workerOptions(t)
	options.Workflows["order"] = retryWorkflow(retryOptions(1))
	options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if err := info.Heartbeat(cancelled, []byte("cancelled")); !errors.Is(err, context.Canceled) {
			return nil, errors.New("caller cancellation was ignored")
		}
		if err := info.Heartbeat(ctx, []byte("first")); err == nil {
			return nil, errors.New("test did not lose heartbeat responses")
		}
		if err := info.Heartbeat(ctx, []byte("second")); err != nil {
			return nil, err
		}
		var group sync.WaitGroup
		failures := make(chan error, 8)
		for range 8 {
			group.Go(func() { failures <- info.Heartbeat(ctx, []byte("concurrent")) })
		}
		group.Wait()
		close(failures)
		for err := range failures {
			if err != nil {
				return nil, err
			}
		}
		return []byte("paid"), nil
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	runTask(t, w, durable.TaskActivity)
	runTask(t, w, durable.TaskWorkflow)
	outcome := lastActivityOutcome(t, s, key)
	if outcome.Failure != nil || outcome.Heartbeat == nil || outcome.Heartbeat.Sequence != 10 || string(outcome.Heartbeat.Details) != "concurrent" {
		t.Fatalf("heartbeat ordering lost progress: %+v", outcome)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range 4 {
		request := s.requests[i]
		if request.Sequence != 1 || request.RequestID != s.requests[0].RequestID || string(request.Progress) != "first" {
			t.Fatalf("unknown request changed before resolution: %+v", request)
		}
	}
}

type delayedHeartbeatWriteStore struct {
	durable.Store
	entered chan struct{}
	release chan struct{}
}

func (s *delayedHeartbeatWriteStore) RecordHeartbeat(ctx context.Context, r durable.HeartbeatRequest) (durable.Receipt, error) {
	close(s.entered)
	<-s.release
	// Model a server finishing an in-flight write after client cancellation.
	return s.Store.RecordHeartbeat(context.WithoutCancel(ctx), r)
}

func TestActivityResultDrainsInflightHeartbeat(t *testing.T) {
	s := &delayedHeartbeatWriteStore{Store: memory.New(), entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(s.release) }) }
	defer release()
	options := workerOptions(t)
	options.Workflows["order"] = retryWorkflow(retryOptions(1))
	heartbeatDone := make(chan error, 1)
	options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		go func() { heartbeatDone <- info.Heartbeat(ctx, []byte("final progress")) }()
		<-s.entered
		return []byte("paid"), nil
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	finished := make(chan error, 1)
	go func() { _, err := w.RunOnce(t.Context(), durable.TaskActivity); finished <- err }()
	select {
	case <-s.entered:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not enter persistence")
	}
	select {
	case err := <-finished:
		t.Fatalf("result published before heartbeat settled: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	release()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("result did not finish after heartbeat settled")
	}
	select {
	case err := <-heartbeatDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("heartbeat callback did not finish")
	}
	outcome := lastActivityOutcome(t, s, key)
	if outcome.Heartbeat == nil || outcome.Heartbeat.Sequence != 1 || string(outcome.Heartbeat.Details) != "final progress" {
		t.Fatalf("result missed final persisted heartbeat: %+v", outcome)
	}
	runTask(t, w, durable.TaskWorkflow)
}

func TestActivityHeartbeatTimeoutRetriesAndRecoversProgress(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	policy := retryOptions(2)
	policy.HeartbeatTimeout = 80 * time.Millisecond
	policy.StartToCloseTimeout = time.Second
	options.Workflows["order"] = retryWorkflow(policy)
	var calls atomic.Int64
	options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		if calls.Add(1) == 1 {
			if err := info.Heartbeat(ctx, []byte("offset:42")); err != nil {
				return nil, err
			}
			<-ctx.Done()
			return nil, context.Cause(ctx)
		}
		if info.Attempt != 2 || string(info.HeartbeatDetails()) != "offset:42" {
			return nil, errors.New("heartbeat timeout lost progress or attempt identity")
		}
		return []byte("paid"), nil
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runHeartbeatWorkerToState(t, w, s, key, durable.StateCompleted)
	outcome := lastActivityOutcome(t, s, key)
	if calls.Load() != 2 || string(outcome.Output) != "paid" || outcome.Heartbeat == nil || string(outcome.Heartbeat.Details) != "offset:42" {
		t.Fatalf("heartbeat timeout recovery: calls=%d %+v", calls.Load(), outcome)
	}
}

func TestActivityHeartbeatsCannotExtendHardDeadlines(t *testing.T) {
	for _, kind := range []drt.ActivityTimeoutKind{drt.TimeoutStartToClose, drt.TimeoutScheduleToClose} {
		t.Run(string(kind), func(t *testing.T) {
			s := memory.New()
			options := workerOptions(t)
			policy := retryOptions(1)
			policy.HeartbeatTimeout = 150 * time.Millisecond
			if kind == drt.TimeoutStartToClose {
				policy.StartToCloseTimeout = 100 * time.Millisecond
			} else {
				policy.ScheduleToCloseTimeout = 100 * time.Millisecond
			}
			options.Workflows["order"] = retryWorkflow(policy)
			options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
				ticker := time.NewTicker(10 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return nil, context.Cause(ctx)
					case <-ticker.C:
						if err := info.Heartbeat(ctx, []byte("still working")); err != nil {
							return nil, err
						}
					}
				}
			}
			w := newWorker(t, s, options)
			key := startWorkerRun(t, w, options)
			runHeartbeatWorkerToState(t, w, s, key, durable.StateFailed)
			outcome := lastActivityOutcome(t, s, key)
			if outcome.Timeout != kind || outcome.Heartbeat == nil || outcome.Heartbeat.Sequence < 2 {
				t.Fatalf("heartbeats changed hard deadline: %+v", outcome)
			}
		})
	}
}

func runHeartbeatWorkerToState(t *testing.T, w *drt.Worker, s durable.Store, key durable.Key, state durable.State) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	limit := time.Now().Add(2 * time.Second)
	for {
		select {
		case err := <-done:
			t.Fatalf("worker stopped before expected state: %v", err)
		default:
		}
		execution, err := s.GetExecution(t.Context(), key)
		if err != nil {
			t.Fatal(err)
		}
		if execution.State == state {
			break
		}
		if time.Now().After(limit) {
			t.Fatalf("worker did not reach %s: %+v", state, execution)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
