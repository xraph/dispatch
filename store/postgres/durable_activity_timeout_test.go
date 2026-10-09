//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/postgres"
)

func TestDurableActivityTimeoutRecovery(t *testing.T) {
	for _, kind := range []drt.ActivityTimeoutKind{drt.TimeoutScheduleToStart, drt.TimeoutStartToClose, drt.TimeoutScheduleToClose} {
		t.Run(string(kind), func(t *testing.T) { testDurableActivityTimeoutRecovery(t, kind) })
	}
}

func testDurableActivityTimeoutRecovery(t *testing.T, kind drt.ActivityTimeoutKind) {
	t.Helper()
	s, dsn := setupTestStoreConnection(t)
	policy := drt.ActivityOptions{StartToCloseTimeout: 200 * time.Millisecond, RetryPolicy: &drt.RetryPolicy{InitialInterval: 20 * time.Millisecond, MaximumAttempts: 2}}
	queue := "orders"
	if kind == drt.TimeoutScheduleToStart {
		queue = "offline"
		policy.ScheduleToStartTimeout = 50 * time.Millisecond
	}
	if kind == drt.TimeoutScheduleToClose {
		policy.ScheduleToCloseTimeout = 150 * time.Millisecond
		policy.RetryPolicy.InitialInterval = time.Second
	}
	options := drt.Options{Namespace: t.Name(), Queue: "orders", BuildID: "v2", Owner: "first", Workflows: map[string]drt.WorkflowFunc{"order": func(w *drt.Workflow, _ []byte) ([]byte, error) {
		return w.ActivityWithOptions("charge", "charge", queue, nil, policy).Get()
	}}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	options.Activities = map[string]drt.ActivityFunc{"charge": func(_ context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		calls++
		if info.Attempt != int64(calls) {
			t.Errorf("logical attempt=%d calls=%d", info.Attempt, calls)
		}
		if calls == 1 {
			if kind == drt.TimeoutStartToClose {
				cancel()
				return nil, context.Canceled
			}
			return nil, errors.New("temporary")
		}
		return []byte("paid"), nil
	}}
	worker, err := drt.NewWorker(s, options)
	if err != nil {
		t.Fatal(err)
	}
	key := durable.Key{Namespace: options.Namespace, WorkflowID: "order", RunID: "run"}
	if _, err = worker.StartExecution(t.Context(), durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "order", BuildID: options.BuildID, Queue: options.Queue}); err != nil {
		t.Fatal(err)
	}
	if worked, workErr := worker.RunOnce(t.Context(), durable.TaskWorkflow); workErr != nil || !worked {
		t.Fatalf("workflow: %t %v", worked, workErr)
	}
	if kind != drt.TimeoutScheduleToStart {
		_, err = worker.RunOnce(ctx, durable.TaskActivity)
		if (kind == drt.TimeoutStartToClose && !errors.Is(err, context.Canceled)) || (kind != drt.TimeoutStartToClose && err != nil) {
			t.Fatalf("first activity: %v", err)
		}
	}
	saved, err := s.GetTask(t.Context(), key, "command:1")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.DB().Close(); err != nil {
		t.Fatal(err)
	}
	drv := pgdriver.New()
	if err = drv.Open(t.Context(), dsn); err != nil {
		t.Fatal(err)
	}
	db, err := grove.Open(drv)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	reopened := postgres.New(db)
	coordinatorOptions := options
	coordinatorOptions.Owner = "coordinator"
	coordinatorOptions.Queue = "coordinator-only"
	coordinatorOptions.Activities = nil
	coordinator, err := drt.NewWorker(reopened, coordinatorOptions)
	if err != nil {
		t.Fatal(err)
	}
	waitDurableStoreTime(t, reopened, saved.DeadlineAt)
	if worked, workErr := coordinator.RunOnce(t.Context(), drt.TaskTimeout); workErr != nil || !worked {
		t.Fatalf("timeout recovery: %t %v", worked, workErr)
	}
	options.Owner = "replacement"
	worker, err = drt.NewWorker(reopened, options)
	if err != nil {
		t.Fatal(err)
	}
	if kind == drt.TimeoutStartToClose {
		retry, readErr := reopened.GetTask(t.Context(), key, saved.ID)
		if readErr != nil {
			t.Fatal(readErr)
		}
		waitDurableStoreTime(t, reopened, retry.AvailableAt)
		if worked, workErr := worker.RunOnce(t.Context(), durable.TaskActivity); workErr != nil || !worked {
			t.Fatalf("retry after timeout: %t %v", worked, workErr)
		}
	}
	if worked, workErr := worker.RunOnce(t.Context(), durable.TaskWorkflow); workErr != nil || !worked {
		t.Fatalf("timeout workflow outcome: %t %v", worked, workErr)
	}
	execution, err := reopened.GetExecution(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	expected := durable.StateFailed
	expectedCalls := 0
	if kind == drt.TimeoutStartToClose {
		expected = durable.StateCompleted
		expectedCalls = 2
		if string(execution.Output) != "paid" {
			t.Fatalf("retry output: %q", execution.Output)
		}
	}
	if kind == drt.TimeoutScheduleToClose {
		expectedCalls = 1
	}
	if execution.State != expected || calls != expectedCalls {
		t.Fatalf("timeout recovery %s: %+v calls=%d", kind, execution, calls)
	}
}
