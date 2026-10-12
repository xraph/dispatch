package runtime_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

// Hold renewal responses so the old worker cannot observe ownership loss before
// its handler returns. The underlying store still enforces the real deadline.
type delayedActivityRenewalStore struct {
	durable.Store
}

type delayedActivityClaimStore struct {
	durable.Store
	claimed chan struct{}
	release chan struct{}
}

func (s *delayedActivityClaimStore) ClaimTask(ctx context.Context, request durable.ClaimRequest) (*durable.Task, error) {
	task, err := s.Store.ClaimTask(ctx, request)
	if err == nil && task != nil && request.Kind == durable.TaskActivity && request.Owner == "delayed" {
		close(s.claimed)
		// Model a response buffered by transport before its caller is scheduled.
		<-s.release
	}
	return task, err
}

func TestDelayedActivityClaimDoesNotTreatReplacementAsCorruptHistory(t *testing.T) {
	s := &delayedActivityClaimStore{Store: memory.New(), claimed: make(chan struct{}), release: make(chan struct{})}
	options := workerOptions(t)
	options.Owner = "delayed"
	options.LeaseDuration, options.StoreTimeout = 60*time.Millisecond, 15*time.Millisecond
	options.Workflows["order"] = retryWorkflow(retryOptions(2))
	started, release := make(chan struct{}), make(chan struct{})
	var claimRelease, handlerRelease sync.Once
	defer claimRelease.Do(func() { close(s.release) })
	defer handlerRelease.Do(func() { close(release) })
	options.Activities["charge"] = func(context.Context, drt.ActivityInfo, []byte) ([]byte, error) {
		close(started)
		<-release
		return []byte("paid"), nil
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	stale := make(chan error, 1)
	go func() { _, err := w.RunOnce(t.Context(), durable.TaskActivity); stale <- err }()
	select {
	case <-s.claimed:
	case err := <-stale:
		t.Fatalf("delayed worker did not claim activity: %v", err)
	case <-time.After(time.Second):
		t.Fatal("activity claim did not arrive")
	}
	task, err := s.GetTask(t.Context(), key, "command:1")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(task.LeaseUntil) + time.Millisecond)
	options.Owner = "replacement"
	replacement := newWorker(t, s, options)
	finished := make(chan error, 1)
	go func() { _, runErr := replacement.RunOnce(t.Context(), durable.TaskActivity); finished <- runErr }()
	select {
	case <-started:
	case err = <-finished:
		t.Fatalf("replacement did not start activity: %v", err)
	case <-time.After(time.Second):
		t.Fatal("replacement activity did not start")
	}
	claimRelease.Do(func() { close(s.release) })
	var staleErr error
	select {
	case staleErr = <-stale:
	case <-time.After(time.Second):
		t.Fatal("delayed claim did not finish")
	}
	handlerRelease.Do(func() { close(release) })
	select {
	case err = <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement activity did not finish")
	}
	if !errors.Is(staleErr, durable.ErrLeaseLost) {
		t.Fatalf("delayed claim did not report ownership loss: %v", staleErr)
	}
	runTask(t, replacement, durable.TaskWorkflow)
	execution, err := s.GetExecution(t.Context(), key)
	if err != nil || execution.State != durable.StateCompleted || string(execution.Output) != "paid" {
		t.Fatalf("replacement result changed: %+v %v", execution, err)
	}
}

func (s *delayedActivityRenewalStore) RenewTask(ctx context.Context, key durable.Key, token durable.TaskToken, ttl time.Duration) (time.Time, error) {
	if key.WorkflowID == "order" && token.TaskID == "command:1" && token.LeaseKind == "" {
		<-ctx.Done()
		return time.Time{}, ctx.Err()
	}
	return s.Store.RenewTask(ctx, key, token, ttl)
}

func TestWorkerContinuesAfterActivityTimeoutWinsResultRace(t *testing.T) {
	s := &delayedActivityRenewalStore{Store: memory.New()}
	options := workerOptions(t)
	options.LeaseDuration, options.StoreTimeout = 6*time.Second, 2*time.Second
	policy := retryOptions(2)
	policy.ScheduleToCloseTimeout = 100 * time.Millisecond
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		if _, err := w.ActivityWithOptions("charge", "charge", "", nil, policy).Get(); err != nil {
			// Keep this run open after timeout so its late handler must read the
			// committed result instead of taking the closed-execution shortcut.
			return w.Timer("after-timeout", time.Hour).Get()
		}
		return nil, nil
	}
	options.Workflows["unrelated"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		return w.Activity("unrelated", "quick", "", nil).Get()
	}
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseHandler()
	options.Activities["charge"] = func(context.Context, drt.ActivityInfo, []byte) ([]byte, error) {
		close(started)
		<-release
		return []byte("late"), nil
	}
	options.Activities["quick"] = func(context.Context, drt.ActivityInfo, []byte) ([]byte, error) {
		return []byte("unrelated completed"), nil
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case <-started:
	case err := <-done:
		t.Fatalf("worker stopped before activity start: %v", err)
	case <-time.After(time.Second):
		t.Fatal("activity did not start")
	}
	limit := time.Now().Add(time.Second)
	for {
		task, err := s.GetTask(t.Context(), key, "command:1")
		if err != nil {
			t.Fatal(err)
		}
		if task.Done {
			break
		}
		if time.Now().After(limit) {
			t.Fatal("activity did not time out")
		}
		time.Sleep(time.Millisecond)
	}
	if outcome := lastActivityOutcome(t, s, key); outcome.Timeout != drt.TimeoutScheduleToClose {
		t.Fatalf("unexpected result before releasing handler: %+v", outcome)
	}
	unrelated := durable.Key{Namespace: options.Namespace, WorkflowID: "unrelated", RunID: "run"}
	if _, err := w.StartExecution(t.Context(), durable.StartRequest{Key: unrelated, RequestID: "start", WorkflowType: "unrelated", BuildID: options.BuildID, Queue: options.Queue}); err != nil {
		t.Fatal(err)
	}
	releaseHandler()
	limit = time.Now().Add(time.Second)
	for {
		select {
		case err := <-done:
			t.Fatalf("late activity result stopped worker: %v", err)
		default:
		}
		execution, err := s.GetExecution(t.Context(), unrelated)
		if err != nil {
			t.Fatal(err)
		}
		if execution.State == durable.StateCompleted && string(execution.Output) == "unrelated completed" {
			break
		}
		if time.Now().After(limit) {
			t.Fatalf("worker did not complete unrelated activity: %+v", execution)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if outcome := lastActivityOutcome(t, s, key); outcome.Timeout != drt.TimeoutScheduleToClose || len(outcome.Output) != 0 {
		t.Fatalf("late result replaced timeout: %+v", outcome)
	}
}
