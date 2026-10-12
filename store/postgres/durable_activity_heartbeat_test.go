//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/postgres"
)

func TestDurableActivityHeartbeatRecovery(t *testing.T) {
	for _, mode := range []string{"failure", "heartbeat_timeout", "worker_loss"} {
		t.Run(mode, func(t *testing.T) { testDurableActivityHeartbeatRecovery(t, mode) })
	}
}

func testDurableActivityHeartbeatRecovery(t *testing.T, mode string) {
	t.Helper()
	s, dsn := setupTestStoreConnection(t)
	policy := drt.ActivityOptions{StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &drt.RetryPolicy{InitialInterval: 50 * time.Millisecond, MaximumAttempts: 2}}
	if mode == "heartbeat_timeout" {
		policy.HeartbeatTimeout = 300 * time.Millisecond
	}
	options := drt.Options{Namespace: t.Name(), Queue: "orders", BuildID: "heartbeat-v1", Owner: "first",
		LeaseDuration: 2 * time.Second, StoreTimeout: 500 * time.Millisecond,
		Workflows: map[string]drt.WorkflowFunc{"order": func(w *drt.Workflow, _ []byte) ([]byte, error) {
			return w.ActivityWithOptions("charge", "charge", "", nil, policy).Get()
		}}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var identity string
	var calls int
	options.Activities = map[string]drt.ActivityFunc{"charge": func(activityCtx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		calls++
		if info.Attempt == 1 {
			identity = info.IdempotencyKey()
			if err := info.Heartbeat(activityCtx, []byte("offset:42")); err != nil {
				return nil, err
			}
			if mode != "failure" {
				cancel()
				return nil, context.Canceled
			}
			return nil, errors.New("retry")
		}
		if info.Attempt != 2 || info.IdempotencyKey() != identity || string(info.HeartbeatDetails()) != "offset:42" {
			return nil, errors.New("heartbeat recovery changed progress or identity")
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
		t.Fatalf("schedule: %t %v", worked, workErr)
	}
	_, err = worker.RunOnce(ctx, durable.TaskActivity)
	if (mode == "failure" && err != nil) || (mode != "failure" && !errors.Is(err, context.Canceled)) {
		t.Fatalf("first heartbeat activity: %v", err)
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
	options.Owner = "replacement"
	worker, err = drt.NewWorker(reopened, options)
	if err != nil {
		t.Fatal(err)
	}
	task, err := reopened.GetTask(t.Context(), key, "command:1")
	if err != nil || string(task.Progress) != "offset:42" {
		t.Fatalf("persisted heartbeat disappeared: %+v %v", task, err)
	}
	switch mode {
	case "heartbeat_timeout":
		waitDurableStoreTime(t, reopened, task.DeadlineAt)
		coordinator, newErr := drt.NewWorker(reopened, drt.Options{Namespace: options.Namespace, Queue: "coordinator-only", BuildID: options.BuildID, Owner: "coordinator"})
		if newErr != nil {
			t.Fatal(newErr)
		}
		if worked, workErr := coordinator.RunOnce(t.Context(), drt.TaskTimeout); workErr != nil || !worked {
			t.Fatalf("heartbeat timeout coordinator: %t %v", worked, workErr)
		}
	case "worker_loss":
		waitDurableStoreTime(t, reopened, task.LeaseUntil)
		if worked, workErr := worker.RunOnce(t.Context(), durable.TaskActivity); workErr != nil || !worked {
			t.Fatalf("adjudicate lost worker: %t %v", worked, workErr)
		}
	}
	task, err = reopened.GetTask(t.Context(), key, "command:1")
	if err != nil {
		t.Fatal(err)
	}
	waitDurableStoreTime(t, reopened, task.AvailableAt)
	for _, kind := range []durable.TaskKind{durable.TaskActivity, durable.TaskWorkflow} {
		if worked, workErr := worker.RunOnce(t.Context(), kind); workErr != nil || !worked {
			t.Fatalf("resume %s: %t %v", kind, worked, workErr)
		}
	}
	execution, err := reopened.GetExecution(t.Context(), key)
	if err != nil || execution.State != durable.StateCompleted || string(execution.Output) != "paid" || calls != 2 {
		t.Fatalf("heartbeat recovery result: %+v calls=%d %v", execution, calls, err)
	}
	events, err := reopened.ReadHistory(t.Context(), key, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	failures := 0
	for _, event := range events {
		if event.Type != drt.EventActivityAttemptFailed {
			continue
		}
		var failed drt.ActivityAttempt
		failures++
		if err = json.Unmarshal(event.Payload, &failed); err != nil || failed.Heartbeat == nil || string(failed.Heartbeat.Details) != "offset:42" || failed.Heartbeat.Sequence != 1 {
			t.Fatalf("persisted failure checkpoint: %+v %v", failed, err)
		}
		if mode == "heartbeat_timeout" && failed.Timeout != drt.TimeoutHeartbeat {
			t.Fatalf("wrong persisted timeout: %+v", failed)
		}
		if mode == "worker_loss" && failed.Failure.Type != drt.FailureWorkerLost {
			t.Fatalf("wrong persisted ownership failure: %+v", failed)
		}
	}
	if failures != 1 {
		t.Fatalf("expected one persisted failed attempt, got %d", failures)
	}
}
