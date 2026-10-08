package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

func retryOptions(maximum int64) drt.ActivityOptions {
	return drt.ActivityOptions{RetryPolicy: &drt.RetryPolicy{InitialInterval: 30 * time.Millisecond, MaximumInterval: 50 * time.Millisecond, BackoffCoefficient: 2, MaximumAttempts: maximum}}
}

func retryWorkflow(options drt.ActivityOptions) drt.WorkflowFunc {
	return func(w *drt.Workflow, _ []byte) ([]byte, error) {
		return w.ActivityWithOptions("charge", "charge", "", nil, options).Get()
	}
}

func TestActivityRetryPersistsAttemptAndDelay(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = retryWorkflow(retryOptions(3))
	var identities []drt.ActivityInfo
	options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		events, err := s.ReadHistory(ctx, info.Key, 0, 100)
		if err != nil || events[len(events)-1].Type != drt.EventActivityAttemptStarted {
			t.Fatalf("handler before persisted start: %+v %v", events, err)
		}
		identities = append(identities, info)
		if info.Attempt < 3 {
			return nil, errors.New("temporary")
		}
		return []byte("paid"), nil
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	for attempt := int64(1); attempt <= 3; attempt++ {
		runTask(t, w, durable.TaskActivity)
		if attempt == 3 {
			break
		}
		task, err := s.GetTask(t.Context(), key, "command:1")
		if err != nil || task.Owner != "" || task.Done {
			t.Fatalf("retry not released: %+v %v", task, err)
		}
		events, _ := s.ReadHistory(t.Context(), key, 0, 100)
		last := events[len(events)-1]
		var failed drt.ActivityAttempt
		if err = json.Unmarshal(last.Payload, &failed); err != nil {
			t.Fatal(err)
		}
		expected := time.Duration(30) * time.Millisecond
		if attempt == 2 {
			expected = 50 * time.Millisecond
		}
		if last.Type != drt.EventActivityAttemptFailed || failed.RetryAfter != expected || !task.AvailableAt.Equal(last.Time.Add(expected)) {
			t.Fatalf("retry timing: %+v %+v", failed, task)
		}
		if worked, pollErr := w.RunOnce(t.Context(), durable.TaskActivity); pollErr != nil || worked {
			t.Fatalf("early retry: %t %v", worked, pollErr)
		}
		if worked, pollErr := w.RunOnce(t.Context(), durable.TaskWorkflow); pollErr != nil || worked {
			t.Fatalf("intermediate failure woke workflow: %t %v", worked, pollErr)
		}
		time.Sleep(time.Until(task.AvailableAt) + 5*time.Millisecond)
		options.Owner = "replacement"
		w = newWorker(t, s, options)
	}
	runTask(t, w, durable.TaskWorkflow)
	execution, err := s.GetExecution(t.Context(), key)
	if err != nil || execution.State != durable.StateCompleted || string(execution.Output) != "paid" || len(identities) != 3 {
		t.Fatalf("retry result: %+v calls=%d %v", execution, len(identities), err)
	}
	for i, info := range identities {
		if info.Attempt != int64(i+1) || info.IdempotencyKey() != identities[0].IdempotencyKey() {
			t.Fatalf("unstable attempt identity: %+v", identities)
		}
	}
}

func TestActivityRetryStopsAtPolicyBoundary(t *testing.T) {
	for _, mode := range []string{"limit", "flag", "type"} {
		t.Run(mode, func(t *testing.T) {
			s := memory.New()
			options := workerOptions(t)
			policy := retryOptions(2)
			failure := &drt.ApplicationError{Type: "invalid", Message: "bad input"}
			if mode == "flag" {
				failure.NonRetryable = true
			}
			if mode == "type" {
				policy.RetryPolicy.NonRetryableTypes = []string{"invalid"}
			}
			options.Workflows["order"] = retryWorkflow(policy)
			var calls int
			options.Activities["charge"] = func(context.Context, drt.ActivityInfo, []byte) ([]byte, error) { calls++; return nil, failure }
			w := newWorker(t, s, options)
			key := startWorkerRun(t, w, options)
			runTask(t, w, durable.TaskWorkflow)
			runTask(t, w, durable.TaskActivity)
			if mode == "limit" {
				task, _ := s.GetTask(t.Context(), key, "command:1")
				time.Sleep(time.Until(task.AvailableAt) + 5*time.Millisecond)
				runTask(t, w, durable.TaskActivity)
			}
			runTask(t, w, durable.TaskWorkflow)
			execution, err := s.GetExecution(t.Context(), key)
			want := 1
			if mode == "limit" {
				want = 2
			}
			if err != nil || execution.State != durable.StateFailed || calls != want {
				t.Fatalf("failure policy: %+v calls=%d %v", execution, calls, err)
			}
		})
	}
}

type activityResponseStore struct {
	durable.Store
	target string
	lost   atomic.Bool
}

func (s *activityResponseStore) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
	receipt, err := s.Store.CommitTransition(ctx, r)
	for _, event := range r.Events {
		if err == nil && event.Type == s.target && s.lost.CompareAndSwap(false, true) {
			return durable.Receipt{}, errors.New("response lost after commit")
		}
	}
	return receipt, err
}

func TestActivityRetriesAmbiguousAttemptWrites(t *testing.T) {
	for _, event := range []string{drt.EventActivityAttemptStarted, drt.EventActivityAttemptFailed, drt.EventActivityCompleted} {
		t.Run(event, func(t *testing.T) {
			s := &activityResponseStore{Store: memory.New(), target: event}
			options := workerOptions(t)
			options.Workflows["order"] = retryWorkflow(retryOptions(2))
			calls := 0
			options.Activities["charge"] = func(context.Context, drt.ActivityInfo, []byte) ([]byte, error) {
				calls++
				if calls == 1 {
					return nil, errors.New("temporary")
				}
				return []byte("paid"), nil
			}
			w := newWorker(t, s, options)
			key := startWorkerRun(t, w, options)
			runTask(t, w, durable.TaskWorkflow)
			runTask(t, w, durable.TaskActivity)
			task, _ := s.GetTask(t.Context(), key, "command:1")
			time.Sleep(time.Until(task.AvailableAt) + 5*time.Millisecond)
			runTask(t, w, durable.TaskActivity)
			runTask(t, w, durable.TaskWorkflow)
			events, err := s.ReadHistory(t.Context(), key, 0, 100)
			counts := map[string]int{}
			for _, record := range events {
				counts[record.Type]++
			}
			if err != nil || !s.lost.Load() || calls != 2 || counts[drt.EventActivityAttemptStarted] != 2 || counts[drt.EventActivityAttemptFailed] != 1 || counts[drt.EventActivityCompleted] != 1 {
				t.Fatalf("ambiguous activity writes: calls=%d counts=%v %v", calls, counts, err)
			}
		})
	}
}

func TestActivityReclaimsInterruptedAttemptThroughPolicy(t *testing.T) {
	for _, maximum := range []int64{1, 2} {
		t.Run(time.Duration(maximum).String(), func(t *testing.T) {
			s := memory.New()
			options := workerOptions(t)
			options.LeaseDuration = 60 * time.Millisecond
			options.StoreTimeout = 10 * time.Millisecond
			options.Workflows["order"] = retryWorkflow(retryOptions(maximum))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
				calls++
				if calls == 1 {
					cancel()
					<-ctx.Done()
					return nil, ctx.Err()
				}
				if info.Attempt != 2 {
					t.Fatalf("logical attempt: %d", info.Attempt)
				}
				return []byte("paid"), nil
			}
			w := newWorker(t, s, options)
			key := startWorkerRun(t, w, options)
			runTask(t, w, durable.TaskWorkflow)
			if _, err := w.RunOnce(ctx, durable.TaskActivity); !errors.Is(err, context.Canceled) {
				t.Fatalf("interruption: %v", err)
			}
			task, _ := s.GetTask(t.Context(), key, "command:1")
			time.Sleep(time.Until(task.LeaseUntil) + 5*time.Millisecond)
			options.Owner = "replacement"
			w = newWorker(t, s, options)
			runTask(t, w, durable.TaskActivity)
			if calls != 1 {
				t.Fatalf("replacement skipped abandoned-attempt policy: %d", calls)
			}
			if maximum == 2 {
				task, _ = s.GetTask(t.Context(), key, "command:1")
				time.Sleep(time.Until(task.AvailableAt) + 5*time.Millisecond)
				runTask(t, w, durable.TaskActivity)
			}
			runTask(t, w, durable.TaskWorkflow)
			execution, err := s.GetExecution(t.Context(), key)
			expected := durable.StateFailed
			if maximum == 2 {
				expected = durable.StateCompleted
			}
			if err != nil || execution.State != expected || int64(calls) != maximum {
				t.Fatalf("interruption policy result: %+v calls=%d %v", execution, calls, err)
			}
		})
	}
}

func TestConcurrentRetryActivitiesPreserveAttempts(t *testing.T) {
	s := &revisionRaceStore{Store: memory.New(), release: make(chan struct{})}
	options := workerOptions(t)
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		a := w.ActivityWithOptions("a", "charge", "", []byte("A"), retryOptions(2))
		b := w.ActivityWithOptions("b", "charge", "", []byte("B"), retryOptions(2))
		first, err := a.Get()
		if err != nil {
			return nil, err
		}
		second, err := b.Get()
		return append(first, second...), err
	}
	var calls atomic.Int64
	options.Activities["charge"] = func(_ context.Context, info drt.ActivityInfo, input []byte) ([]byte, error) {
		calls.Add(1)
		if info.Attempt != 1 {
			return nil, errors.New("unexpected repeated attempt")
		}
		return input, nil
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	results := make(chan error, 2)
	for range 2 {
		go func() { _, err := w.RunOnce(t.Context(), durable.TaskActivity); results <- err }()
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	runTask(t, w, durable.TaskWorkflow)
	execution, err := s.GetExecution(t.Context(), key)
	if err != nil || string(execution.Output) != "AB" || calls.Load() != 2 || s.conflicts.Load() < 1 {
		t.Fatalf("concurrent retry activities: %+v calls=%d conflicts=%d %v", execution, calls.Load(), s.conflicts.Load(), err)
	}
}

type unacknowledgedStartStore struct{ durable.Store }

func (s *unacknowledgedStartStore) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
	receipt, err := s.Store.CommitTransition(ctx, r)
	if err == nil && len(r.Events) == 1 && r.Events[0].Type == drt.EventActivityAttemptStarted {
		return durable.Receipt{}, errors.New("all start acknowledgements lost")
	}
	return receipt, err
}

func TestActivityDoesNotRunWithoutAcknowledgedStart(t *testing.T) {
	s := &unacknowledgedStartStore{Store: memory.New()}
	options := workerOptions(t)
	options.Workflows["order"] = retryWorkflow(retryOptions(2))
	calls := 0
	options.Activities["charge"] = func(context.Context, drt.ActivityInfo, []byte) ([]byte, error) { calls++; return nil, nil }
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	if _, err := w.RunOnce(t.Context(), durable.TaskActivity); err == nil || calls != 0 {
		t.Fatalf("unacknowledged start invoked handler: calls=%d %v", calls, err)
	}
	events, err := s.ReadHistory(t.Context(), key, 0, 100)
	starts := 0
	for _, event := range events {
		if event.Type == drt.EventActivityAttemptStarted {
			starts++
		}
	}
	if err != nil || starts != 1 {
		t.Fatalf("ambiguous start duplicated: starts=%d %v", starts, err)
	}
}

func TestUnstartedClaimDoesNotConsumeActivityAttempt(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = retryWorkflow(retryOptions(1))
	attempt := int64(0)
	options.Activities["charge"] = func(_ context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		attempt = info.Attempt
		return []byte("paid"), nil
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	claimed, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: options.Namespace, Queue: options.Queue, BuildID: options.BuildID, Kind: durable.TaskActivity, Owner: "crashed", LeaseDuration: 30 * time.Millisecond})
	if err != nil || claimed == nil {
		t.Fatalf("unstarted claim: %+v %v", claimed, err)
	}
	time.Sleep(time.Until(claimed.LeaseUntil) + 5*time.Millisecond)
	runTask(t, w, durable.TaskActivity)
	runTask(t, w, durable.TaskWorkflow)
	execution, err := s.GetExecution(t.Context(), key)
	if err != nil || execution.State != durable.StateCompleted || attempt != 1 {
		t.Fatalf("claim consumed attempt: %+v attempt=%d %v", execution, attempt, err)
	}
}
