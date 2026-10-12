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

func workerOptions(t *testing.T) drt.Options {
	t.Helper()
	return drt.Options{Namespace: t.Name(), Queue: "orders", BuildID: "v1", Owner: "worker",
		LeaseDuration: time.Second, PollInterval: time.Millisecond, StoreTimeout: 100 * time.Millisecond,
		Workflows: make(map[string]drt.WorkflowFunc), Activities: make(map[string]drt.ActivityFunc)}
}

func newWorker(t *testing.T, s durable.Store, options drt.Options) *drt.Worker {
	t.Helper()
	w, err := drt.NewWorker(s, options)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func startWorkerRun(t *testing.T, w *drt.Worker, options drt.Options) durable.Key {
	t.Helper()
	key := durable.Key{Namespace: options.Namespace, WorkflowID: "order", RunID: "run"}
	_, err := w.StartExecution(t.Context(), durable.StartRequest{Key: key,
		RequestID: "start", WorkflowType: "order", BuildID: options.BuildID, Queue: options.Queue})
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func runTask(t *testing.T, w *drt.Worker, kind durable.TaskKind) {
	t.Helper()
	if worked, err := w.RunOnce(t.Context(), kind); err != nil || !worked {
		t.Fatalf("run %s task: worked=%t error=%v", kind, worked, err)
	}
}

func TestWorkerReplacementReplaysCompletedEffects(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		result, err := w.Activity("charge", "charge", "", nil).Get()
		if err != nil {
			return nil, err
		}
		if _, err = w.Timer("delay", time.Millisecond).Get(); err != nil {
			return nil, err
		}
		return result, nil
	}
	var calls atomic.Int64
	options.Activities["charge"] = func(_ context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		calls.Add(1)
		if info.CommandID != "charge" || info.Key.Namespace != options.Namespace || info.IdempotencyKey() == "" {
			return nil, errors.New("missing stable activity identity")
		}
		return []byte("paid"), nil
	}
	first := newWorker(t, s, options)
	key := startWorkerRun(t, first, options)
	runTask(t, first, durable.TaskWorkflow)
	runTask(t, first, durable.TaskActivity)
	options.Owner = "replacement"
	replacement := newWorker(t, s, options)
	runTask(t, replacement, durable.TaskWorkflow)
	// The deadline can pass while no worker is running.
	time.Sleep(3 * time.Millisecond)
	runTask(t, replacement, durable.TaskTimer)
	runTask(t, replacement, durable.TaskWorkflow)
	execution, err := s.GetExecution(t.Context(), key)
	if err != nil || execution.State != durable.StateCompleted || string(execution.Output) != "paid" || calls.Load() != 1 {
		t.Fatalf("replacement result: %+v calls=%d err=%v", execution, calls.Load(), err)
	}
}

func TestConcurrentActivityResultsSurviveRevisionConflicts(t *testing.T) {
	s := &revisionRaceStore{Store: memory.New(), release: make(chan struct{})}
	options := workerOptions(t)
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		a := w.Activity("a", "lookup", "", []byte("A"))
		b := w.Activity("b", "lookup", "", []byte("B"))
		x, err := a.Get()
		if err != nil {
			return nil, err
		}
		y, err := b.Get()
		return append(x, y...), err
	}
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	options.Activities["lookup"] = func(ctx context.Context, _ drt.ActivityInfo, input []byte) ([]byte, error) {
		ready <- struct{}{}
		select {
		case <-release:
			return input, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	results := make(chan error, 2)
	for range 2 {
		go func() { _, err := w.RunOnce(t.Context(), durable.TaskActivity); results <- err }()
	}
	for range 2 {
		select {
		case <-ready:
		case <-time.After(time.Second):
			t.Fatal("activity did not start")
		}
	}
	close(release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if s.conflicts.Load() < 1 {
		t.Fatal("test did not force a conflicting result commit")
	}
	runTask(t, w, durable.TaskWorkflow)
	execution, err := s.GetExecution(t.Context(), key)
	if err != nil || string(execution.Output) != "AB" {
		t.Fatalf("lost concurrent result: %+v, %v", execution, err)
	}
	if worked, pollErr := w.RunOnce(t.Context(), durable.TaskWorkflow); pollErr != nil || worked {
		t.Fatalf("terminal run left wakeup tasks: %t, %v", worked, pollErr)
	}
}

type lostResponseStore struct {
	durable.Store
	lost atomic.Bool
}

func (s *lostResponseStore) CommitTransition(ctx context.Context, request durable.CommitRequest) (durable.Receipt, error) {
	receipt, err := s.Store.CommitTransition(ctx, request)
	if err == nil && s.lost.CompareAndSwap(false, true) {
		return durable.Receipt{}, errors.New("connection dropped after commit")
	}
	return receipt, err
}

func TestWorkerRetriesLostCommitResponse(t *testing.T) {
	s := &lostResponseStore{Store: memory.New()}
	options := workerOptions(t)
	options.Workflows["order"] = func(_ *drt.Workflow, _ []byte) ([]byte, error) { return []byte("done"), nil }
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	events, err := s.ReadHistory(t.Context(), key, 0, 100)
	if err != nil || len(events) != 2 {
		t.Fatalf("lost response duplicated completion: %+v, %v", events, err)
	}
}

type leaseFailureStore struct {
	durable.Store
	fail atomic.Bool
}

func (s *leaseFailureStore) RenewTask(ctx context.Context, key durable.Key, token durable.TaskToken, ttl time.Duration) (time.Time, error) {
	if s.fail.Load() {
		return time.Time{}, durable.ErrLeaseLost
	}
	return s.Store.RenewTask(ctx, key, token, ttl)
}

func TestLeaseLossCancelsActivityWithoutPublishing(t *testing.T) {
	s := &leaseFailureStore{Store: memory.New()}
	options := workerOptions(t)
	options.LeaseDuration, options.StoreTimeout = 90*time.Millisecond, 20*time.Millisecond
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.Activity("a", "wait", "", nil).Get() }
	options.Activities["wait"] = func(ctx context.Context, _ drt.ActivityInfo, _ []byte) ([]byte, error) {
		<-ctx.Done()
		return []byte("late result"), nil
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	s.fail.Store(true)
	_, err := w.RunOnce(t.Context(), durable.TaskActivity)
	if !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("lease loss not returned: %v", err)
	}
	events, err := s.ReadHistory(t.Context(), key, 0, 100)
	if err != nil || events[len(events)-1].Type != drt.EventWorkflowWaiting {
		t.Fatalf("expired owner published result: %+v, %v", events, err)
	}
}

func TestWorkerRenewsActivityLeaseAndStops(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.LeaseDuration, options.StoreTimeout = 90*time.Millisecond, 20*time.Millisecond
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.Activity("a", "wait", "", nil).Get() }
	started := make(chan struct{})
	var once sync.Once
	options.Activities["wait"] = func(ctx context.Context, _ drt.ActivityInfo, _ []byte) ([]byte, error) {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return nil, ctx.Err()
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not dispatch activity")
	}
	time.Sleep(3 * options.LeaseDuration)
	if task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: options.Namespace, Queue: options.Queue, Kind: durable.TaskActivity, Owner: "contender", LeaseDuration: time.Second}); err != nil || task != nil {
		t.Fatalf("live activity lease was not renewed: %+v, %v", task, err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker failed to stop")
	}
	execution, err := s.GetExecution(t.Context(), key)
	if err != nil || execution.State != durable.StateRunning {
		t.Fatalf("shutdown closed workflow: %+v, %v", execution, err)
	}
}

func TestWorkerBuildAndNamespaceBoundaries(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = func(_ *drt.Workflow, _ []byte) ([]byte, error) { return nil, nil }
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	wrong := options
	wrong.BuildID = "v2"
	if worked, err := newWorker(t, s, wrong).RunOnce(t.Context(), durable.TaskWorkflow); err != nil || worked {
		t.Fatalf("wrong build executed workflow: %t, %v", worked, err)
	}
	_, err := w.StartExecution(t.Context(), durable.StartRequest{Key: durable.Key{Namespace: "another", WorkflowID: "id", RunID: "run"}, RequestID: "start", WorkflowType: "order", BuildID: "v1", Queue: options.Queue})
	if !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("foreign namespace start: %v", err)
	}
	runTask(t, w, durable.TaskWorkflow)
	if execution, getErr := s.GetExecution(t.Context(), key); getErr != nil || execution.State != durable.StateCompleted {
		t.Fatalf("matching build did not finish: %+v, %v", execution, getErr)
	}
}

// closeRaceStore models a task that was claimed just before another task closed
// its execution. Run should keep serving unrelated runs after this normal race.
type closeRaceStore struct {
	durable.Store
	closed atomic.Bool
}

func (s *closeRaceStore) GetExecution(ctx context.Context, key durable.Key) (durable.Execution, error) {
	if s.closed.CompareAndSwap(false, true) {
		return durable.Execution{State: durable.StateCompleted}, nil
	}
	return s.Store.GetExecution(ctx, key)
}

func TestWorkerRunContinuesAfterConcurrentClosure(t *testing.T) {
	s := &closeRaceStore{Store: memory.New()}
	options := workerOptions(t)
	options.Workflows["order"] = func(_ *drt.Workflow, _ []byte) ([]byte, error) { return nil, nil }
	w := newWorker(t, s, options)
	startWorkerRun(t, w, options)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if err := w.Run(ctx); err != nil {
		t.Fatalf("normal concurrent closure killed pollers: %v", err)
	}
}

func TestCrossQueueActivityWakesOriginalWorkflowQueue(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		return w.Activity("charge", "charge", "billing", nil).Get()
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	billing := workerOptions(t)
	billing.Queue = "billing"
	billing.Activities["charge"] = func(_ context.Context, _ drt.ActivityInfo, _ []byte) ([]byte, error) { return []byte("paid"), nil }
	runTask(t, newWorker(t, s, billing), durable.TaskActivity)
	runTask(t, w, durable.TaskWorkflow)
	if execution, err := s.GetExecution(t.Context(), key); err != nil || string(execution.Output) != "paid" {
		t.Fatalf("cross-queue wakeup: %+v, %v", execution, err)
	}
}

type revisionRaceStore struct {
	durable.Store
	arrivals  atomic.Int64
	conflicts atomic.Int64
	release   chan struct{}
}

func (s *revisionRaceStore) CommitTransition(ctx context.Context, request durable.CommitRequest) (durable.Receipt, error) {
	if len(request.Events) == 1 && request.Events[0].Type == drt.EventActivityCompleted {
		arrival := s.arrivals.Add(1)
		if arrival == 2 {
			close(s.release)
		}
		if arrival <= 2 {
			select {
			case <-s.release:
			case <-ctx.Done():
				return durable.Receipt{}, ctx.Err()
			}
		}
	}
	receipt, err := s.Store.CommitTransition(ctx, request)
	if errors.Is(err, durable.ErrRevisionConflict) {
		s.conflicts.Add(1)
	}
	return receipt, err
}
