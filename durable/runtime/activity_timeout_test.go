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

func TestActivityQueueTimeoutWithoutActivityWorker(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	policy := retryOptions(10)
	policy.ScheduleToStartTimeout = 40 * time.Millisecond
	policy.StartToCloseTimeout = time.Second
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		return w.ActivityWithOptions("charge", "charge", "offline", nil, policy).Get()
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	limit := time.Now().Add(time.Second)
	for {
		execution, err := s.GetExecution(t.Context(), key)
		if err != nil {
			t.Fatal(err)
		}
		if execution.State == durable.StateFailed {
			break
		}
		if time.Now().After(limit) {
			t.Fatalf("offline queue did not time out: %+v", execution)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	outcome := lastActivityOutcome(t, s, key)
	if outcome.Timeout != drt.TimeoutScheduleToStart || outcome.Attempt != 0 || outcome.Failure == nil || !outcome.Failure.NonRetryable || outcome.Failure.Type != "activity_schedule_to_start_timeout" {
		t.Fatalf("queue timeout outcome: %+v", outcome)
	}
}

func lastActivityOutcome(t *testing.T, s durable.Store, key durable.Key) drt.Outcome {
	t.Helper()
	events, err := s.ReadHistory(t.Context(), key, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Type == drt.EventActivityCompleted {
			var outcome drt.Outcome
			if err = json.Unmarshal(events[i].Payload, &outcome); err != nil {
				t.Fatal(err)
			}
			return outcome
		}
	}
	t.Fatal("missing final activity outcome")
	return drt.Outcome{}
}

func TestActivityAttemptTimeoutRejectsLateSuccessAndRetries(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	policy := retryOptions(2)
	policy.StartToCloseTimeout = 60 * time.Millisecond
	options.Workflows["order"] = retryWorkflow(policy)
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	options.Activities["charge"] = func(_ context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		call := calls.Add(1)
		if info.Attempt != call {
			return nil, errors.New("attempt identity changed")
		}
		if call == 1 {
			close(started)
			<-release
			return []byte("late"), nil
		}
		return []byte("paid"), nil
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	finished := make(chan error, 1)
	go func() { _, err := w.RunOnce(t.Context(), durable.TaskActivity); finished <- err }()
	<-started
	task, err := s.GetTask(t.Context(), key, "command:1")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(task.DeadlineAt) + 10*time.Millisecond)
	runTask(t, w, drt.TaskTimeout)
	close(release)
	if err = <-finished; !errors.Is(err, durable.ErrTaskDeadline) && !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("late handler result accepted: %v", err)
	}
	retried, err := s.GetTask(t.Context(), key, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if retried.Owner != "" || retried.Done || retried.LeaseKind != "" {
		t.Fatalf("timeout retry not released: %+v", retried)
	}
	time.Sleep(time.Until(retried.AvailableAt) + 5*time.Millisecond)
	runTask(t, w, durable.TaskActivity)
	runTask(t, w, durable.TaskWorkflow)
	execution, err := s.GetExecution(t.Context(), key)
	if err != nil || execution.State != durable.StateCompleted || string(execution.Output) != "paid" || calls.Load() != 2 {
		t.Fatalf("timeout retry result: %+v calls=%d %v", execution, calls.Load(), err)
	}
	outcome := lastActivityOutcome(t, s, key)
	if string(outcome.Output) != "paid" || outcome.Attempt != 2 {
		t.Fatalf("late success won: %+v", outcome)
	}
}

func TestActivityOverallTimeoutDuringBackoff(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	policy := retryOptions(10)
	policy.RetryPolicy.InitialInterval = time.Second
	policy.RetryPolicy.MaximumInterval = time.Second
	policy.ScheduleToCloseTimeout = 80 * time.Millisecond
	policy.StartToCloseTimeout = time.Second
	policy.ScheduleToStartTimeout = time.Second
	options.Workflows["order"] = retryWorkflow(policy)
	calls := 0
	options.Activities["charge"] = func(context.Context, drt.ActivityInfo, []byte) ([]byte, error) {
		calls++
		return nil, errors.New("temporary")
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	scheduled, err := s.GetTask(t.Context(), key, "command:1")
	if err != nil {
		t.Fatal(err)
	}
	runTask(t, w, durable.TaskActivity)
	retried, err := s.GetTask(t.Context(), key, scheduled.ID)
	if err != nil || !retried.DeadlineAt.Equal(scheduled.DeadlineAt) || !retried.AvailableAt.After(retried.DeadlineAt) {
		t.Fatalf("backoff extended total timeout: %+v %v", retried, err)
	}
	time.Sleep(time.Until(retried.DeadlineAt) + 10*time.Millisecond)
	results := make(chan bool, 2)
	failures := make(chan error, 2)
	for range 2 {
		go func() {
			worked, runErr := w.RunOnce(t.Context(), drt.TaskTimeout)
			results <- worked
			failures <- runErr
		}()
	}
	winners := 0
	for range 2 {
		if <-results {
			winners++
		}
		if runErr := <-failures; runErr != nil {
			t.Fatal(runErr)
		}
	}
	if winners != 1 {
		t.Fatalf("competing processors both claimed timeout: %d", winners)
	}
	runTask(t, w, durable.TaskWorkflow)
	outcome := lastActivityOutcome(t, s, key)
	if calls != 1 || outcome.Timeout != drt.TimeoutScheduleToClose || outcome.Attempt != 1 || outcome.Failure == nil || !outcome.Failure.NonRetryable {
		t.Fatalf("overall timeout during backoff: calls=%d %+v", calls, outcome)
	}
}

func TestActivityOverallDeadlineCancelsRenewingHandler(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	policy := retryOptions(5)
	policy.ScheduleToCloseTimeout = 80 * time.Millisecond
	policy.StartToCloseTimeout = time.Second
	options.Workflows["order"] = retryWorkflow(policy)
	options.Activities["charge"] = func(ctx context.Context, _ drt.ActivityInfo, _ []byte) ([]byte, error) {
		<-ctx.Done()
		return nil, context.Cause(ctx)
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	done := make(chan error, 1)
	go func() { _, err := w.RunOnce(t.Context(), durable.TaskActivity); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, durable.ErrTaskDeadline) {
			t.Fatalf("renewing handler cancellation: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("renewals extended activity timeout or cancellation lagged")
	}
	runTask(t, w, drt.TaskTimeout)
	runTask(t, w, durable.TaskWorkflow)
	outcome := lastActivityOutcome(t, s, key)
	if outcome.Timeout != drt.TimeoutScheduleToClose || outcome.Attempt != 1 {
		t.Fatalf("overall execution timeout: %+v", outcome)
	}
}

func TestActivityTimeoutOptionsAndHistoryReplay(t *testing.T) {
	f := newHistory()
	policy := retryOptions(2)
	policy.ScheduleToStartTimeout = time.Second
	policy.StartToCloseTimeout = 2 * time.Second
	policy.ScheduleToCloseTimeout = 5 * time.Second
	handler := retryWorkflow(policy)
	first, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil {
		t.Fatal(err)
	}
	f.commands(t, first.Commands)
	changed := policy
	changed.StartToCloseTimeout = time.Minute
	if _, err = drt.Evaluate(f.execution, f.events, retryWorkflow(changed)); !errors.Is(err, drt.ErrNondeterministic) {
		t.Fatalf("changed timeout policy accepted: %v", err)
	}
	for _, kind := range []drt.ActivityTimeoutKind{drt.TimeoutScheduleToStart, drt.TimeoutStartToClose, drt.TimeoutScheduleToClose} {
		corrupted := *f
		corrupted.events = append([]durable.Event(nil), f.events...)
		corrupted.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 2, CommandID: "charge", Timeout: kind, Failure: &drt.ApplicationError{Type: "activity_" + string(kind) + "_timeout", Message: "expired", NonRetryable: kind != drt.TimeoutStartToClose}}), f.execution.CreatedAt.Add(500*time.Millisecond))
		if _, err = drt.Evaluate(corrupted.execution, corrupted.events, handler); !errors.Is(err, drt.ErrHistory) {
			t.Fatalf("early or wrong-class timeout accepted: %s %v", kind, err)
		}
	}
	invalid := policy
	invalid.StartToCloseTimeout = -1
	empty := newHistory()
	if _, err = drt.Evaluate(empty.execution, empty.events, retryWorkflow(invalid)); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("negative timeout accepted: %v", err)
	}
}

func TestReplayRejectsExecutionAfterActivityDeadline(t *testing.T) {
	for _, mode := range []string{"start", "success", "failure"} {
		t.Run(mode, func(t *testing.T) {
			f := newHistory()
			policy := retryOptions(2)
			policy.ScheduleToStartTimeout = time.Second
			policy.StartToCloseTimeout = time.Second
			handler := retryWorkflow(policy)
			first, err := drt.Evaluate(f.execution, f.events, handler)
			if err != nil {
				t.Fatal(err)
			}
			f.commands(t, first.Commands)
			attempt := drt.ActivityAttempt{Version: 1, CommandID: "charge", Attempt: 1, Epoch: 1}
			at := f.execution.CreatedAt
			if mode == "start" {
				at = at.Add(time.Second)
			}
			f.append(drt.EventActivityAttemptStarted, encode(t, attempt), at)
			if mode == "success" {
				f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 2, CommandID: "charge", Attempt: 1, Output: []byte("late")}), at.Add(time.Second))
			}
			if mode == "failure" {
				attempt.Failure = &drt.ApplicationError{Type: "temporary", Message: "late"}
				attempt.RetryAfter = 30 * time.Millisecond
				f.append(drt.EventActivityAttemptFailed, encode(t, attempt), at.Add(time.Second))
			}
			if _, err = drt.Evaluate(f.execution, f.events, handler); !errors.Is(err, drt.ErrHistory) {
				t.Fatalf("late %s accepted by replay: %v", mode, err)
			}
		})
	}
}

func TestStartingActivityReplacesQueueDeadline(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	policy := retryOptions(1)
	policy.ScheduleToStartTimeout = 40 * time.Millisecond
	policy.StartToCloseTimeout = 300 * time.Millisecond
	options.Workflows["order"] = retryWorkflow(policy)
	started := make(chan struct{})
	options.Activities["charge"] = func(ctx context.Context, _ drt.ActivityInfo, _ []byte) ([]byte, error) {
		close(started)
		select {
		case <-time.After(150 * time.Millisecond):
			return []byte("paid"), nil
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	queued, err := s.GetTask(t.Context(), key, "command:1")
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, runErr := w.RunOnce(t.Context(), durable.TaskActivity); result <- runErr }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("activity did not start")
	}
	time.Sleep(time.Until(queued.DeadlineAt) + 10*time.Millisecond)
	if worked, runErr := w.RunOnce(t.Context(), drt.TaskTimeout); runErr != nil || worked {
		t.Fatalf("old queue deadline expired a running attempt: %t %v", worked, runErr)
	}
	if err = <-result; err != nil {
		t.Fatalf("queue deadline cut short an active grant: %v", err)
	}
	runTask(t, w, durable.TaskWorkflow)
	execution, err := s.GetExecution(t.Context(), key)
	if err != nil || string(execution.Output) != "paid" {
		t.Fatalf("started activity: %+v %v", execution, err)
	}
}
