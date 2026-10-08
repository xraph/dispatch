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

func TestDurableActivityRetryRecovery(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		name := "failure"
		if interrupted {
			name = "interrupted"
		}
		t.Run(name, func(t *testing.T) { testDurableActivityRetryRecovery(t, interrupted) })
	}
}

func testDurableActivityRetryRecovery(t *testing.T, interrupted bool) {
	t.Helper()
	s, dsn := setupTestStoreConnection(t)
	options := drt.Options{Namespace: t.Name(), Queue: "orders", BuildID: "v2", Owner: "first", LeaseDuration: 200 * time.Millisecond, StoreTimeout: 50 * time.Millisecond,
		Workflows: map[string]drt.WorkflowFunc{"order": func(w *drt.Workflow, _ []byte) ([]byte, error) {
			return w.ActivityWithOptions("charge", "charge", "", nil, drt.ActivityOptions{RetryPolicy: &drt.RetryPolicy{InitialInterval: time.Second, MaximumAttempts: 2}}).Get()
		}}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var calls int
	var identity string
	options.Activities = map[string]drt.ActivityFunc{"charge": func(_ context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		calls++
		if info.Attempt != int64(calls) {
			t.Errorf("logical attempt %d differs from handler call %d", info.Attempt, calls)
		}
		if calls == 1 {
			identity = info.IdempotencyKey()
			if interrupted {
				cancel()
				return nil, context.Canceled
			}
			return nil, errors.New("temporary")
		}
		if info.IdempotencyKey() != identity {
			t.Error("idempotency key changed across replacement")
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
	if (interrupted && !errors.Is(err, context.Canceled)) || (!interrupted && err != nil) {
		t.Fatalf("first activity: %v", err)
	}
	before, err := s.GetTask(t.Context(), key, "command:1")
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
	options.Owner = "replacement"
	worker, err = drt.NewWorker(reopened, options)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := reopened.GetTask(t.Context(), key, "command:1")
	if err != nil || !saved.AvailableAt.Equal(before.AvailableAt) || saved.Version != before.Version {
		t.Fatalf("task changed across reopen: %+v %+v %v", before, saved, err)
	}
	if interrupted {
		time.Sleep(time.Until(saved.LeaseUntil) + 10*time.Millisecond)
		if worked, workErr := worker.RunOnce(t.Context(), durable.TaskActivity); workErr != nil || !worked {
			t.Fatalf("adjudicate interruption: %t %v", worked, workErr)
		}
		if calls != 1 {
			t.Fatal("replacement invoked activity before applying interrupted attempt policy")
		}
		saved, err = reopened.GetTask(t.Context(), key, "command:1")
		if err != nil {
			t.Fatal(err)
		}
	}
	if saved.Done || saved.Owner != "" {
		t.Fatalf("retry not released: %+v", saved)
	}
	if worked, workErr := worker.RunOnce(t.Context(), durable.TaskActivity); workErr != nil || worked {
		t.Fatalf("retry delay lost after reopen: %t %v", worked, workErr)
	}
	time.Sleep(time.Until(saved.AvailableAt) + 10*time.Millisecond)
	for _, kind := range []durable.TaskKind{durable.TaskActivity, durable.TaskWorkflow} {
		if worked, workErr := worker.RunOnce(t.Context(), kind); workErr != nil || !worked {
			t.Fatalf("replacement %s: %t %v", kind, worked, workErr)
		}
	}
	execution, err := reopened.GetExecution(t.Context(), key)
	if err != nil || execution.State != durable.StateCompleted || string(execution.Output) != "paid" || calls != 2 {
		t.Fatalf("retry recovery: %+v calls=%d %v", execution, calls, err)
	}
	events, err := reopened.ReadHistory(t.Context(), key, 0, 100)
	counts := map[string]int{}
	for _, event := range events {
		counts[event.Type]++
	}
	if err != nil || counts[drt.EventActivityAttemptStarted] != 2 || counts[drt.EventActivityAttemptFailed] != 1 || counts[drt.EventActivityCompleted] != 1 {
		t.Fatalf("retry recovery history: %v %v", counts, err)
	}
}
