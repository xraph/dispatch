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
	"github.com/xraph/dispatch/durable/durabletest"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/postgres"
)

func TestDurable(t *testing.T) {
	s := setupTestStore(t)
	durabletest.Run(t, s)
}

func TestDurableRenewalChecksExpiryAfterLockWait(t *testing.T) {
	s := setupTestStore(t)
	r := durable.StartRequest{Key: durable.Key{Namespace: "lock-expiry", WorkflowID: "order", RunID: "run"},
		RequestID: "start", WorkflowType: "order", BuildID: "v1", Queue: "orders"}
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: r.Queue,
		Kind: durable.TaskWorkflow, Owner: "worker", LeaseDuration: time.Second})
	if err != nil || task == nil {
		t.Fatalf("claim: %+v, %v", task, err)
	}
	pg := pgdriver.Unwrap(s.DB())
	tx, err := pg.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec(t.Context(), `SELECT 1 FROM dispatch_execution_tasks
        WHERE namespace=$1 FOR UPDATE`, r.Namespace); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, renewErr := s.RenewTask(t.Context(), r.Key, task.Token(), time.Minute)
		result <- renewErr
	}()
	deadline := time.Now().Add(5 * time.Second)
	blocked := false
	for time.Now().Before(deadline) {
		if err = pg.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
            WHERE wait_event_type='Lock' AND query LIKE '%dispatch_execution_tasks%')`).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !blocked {
		t.Fatal("renewal did not wait for the task lock")
	}
	for {
		var expired bool
		if err = pg.QueryRow(t.Context(), `SELECT clock_timestamp() >= $1`, task.LeaseUntil).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lease did not expire")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	select {
	case renewErr := <-result:
		if !errors.Is(renewErr, durable.ErrLeaseLost) {
			t.Fatalf("renewal revived expired lease after lock wait: %v", renewErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("renewal stayed blocked after rollback")
	}
}

func TestDurableReopen(t *testing.T) {
	s, dsn := setupTestStoreConnection(t)
	r := durable.StartRequest{Key: durable.Key{Namespace: "restart", WorkflowID: "order", RunID: "run"},
		RequestID: "start", WorkflowType: "order", BuildID: "v1", Queue: "orders"}
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: r.Queue,
		Kind: durable.TaskWorkflow, Owner: "worker", LeaseDuration: time.Minute})
	if err != nil || claimed == nil {
		t.Fatalf("claim: %+v, %v", claimed, err)
	}
	req := durable.CommitRequest{Key: r.Key, RequestID: "timer", ExpectedRevision: 1, Token: claimed.Token(),
		Events: []durable.EventInput{{Type: "timer.scheduled"}},
		Tasks:  []durable.TaskSpec{{ID: "timer", Kind: durable.TaskTimer, Queue: "timers", AvailableAt: time.Now().Add(-time.Second)}}}
	receipt, err := s.CommitTransition(t.Context(), req)
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
	retry, err := reopened.CommitTransition(t.Context(), req)
	if err != nil || retry != receipt {
		t.Fatalf("receipt after reopen: %+v, %v", retry, err)
	}
	timer, err := reopened.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: "timers",
		Kind: durable.TaskTimer, Owner: "replacement", LeaseDuration: time.Minute})
	if err != nil || timer == nil || timer.ID != "timer" {
		t.Fatalf("timer after reopen: %+v, %v", timer, err)
	}
	history, err := reopened.ReadHistory(t.Context(), r.Key, 0, 100)
	if err != nil || len(history) != 2 || history[1].Type != "timer.scheduled" {
		t.Fatalf("history after reopen: %+v, %v", history, err)
	}
}

func TestDurableMigrationRetry(t *testing.T) {
	s := setupTestStore(t)
	r := durable.StartRequest{Key: durable.Key{Namespace: "migration", WorkflowID: "order", RunID: "run"},
		RequestID: "start", WorkflowType: "order", BuildID: "v1", Queue: "orders"}
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	// Model a crash after schema creation but before the migration receipt.
	_, err := pgdriver.Unwrap(s.DB()).Exec(t.Context(),
		`DELETE FROM grove_migrations WHERE version=$1`, "20261010120000")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Migrate(t.Context()); err != nil {
		t.Fatalf("retry applied schema: %v", err)
	}
	if execution, getErr := s.GetExecution(t.Context(), r.Key); getErr != nil || execution.Revision != 1 {
		t.Fatalf("migration retry changed execution: %+v, %v", execution, getErr)
	}
}

func TestDurableRuntimeRecovery(t *testing.T) {
	s, dsn := setupTestStoreConnection(t)
	options := drt.Options{Namespace: t.Name(), Queue: "orders", BuildID: "v1", Owner: "worker",
		Workflows: map[string]drt.WorkflowFunc{"order": func(w *drt.Workflow, _ []byte) ([]byte, error) {
			result, err := w.Activity("charge", "charge", "", nil).Get()
			if err != nil {
				return nil, err
			}
			if _, err = w.Timer("delay", time.Millisecond).Get(); err != nil {
				return nil, err
			}
			return result, nil
		}}}
	calls := 0
	options.Activities = map[string]drt.ActivityFunc{"charge": func(_ context.Context, _ drt.ActivityInfo, _ []byte) ([]byte, error) {
		calls++
		return []byte("paid"), nil
	}}
	worker, err := drt.NewWorker(s, options)
	if err != nil {
		t.Fatal(err)
	}
	key := durable.Key{Namespace: options.Namespace, WorkflowID: "order", RunID: "run"}
	if _, err = worker.StartExecution(t.Context(), durable.StartRequest{Key: key, RequestID: "start",
		WorkflowType: "order", BuildID: options.BuildID, Queue: options.Queue}); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []durable.TaskKind{durable.TaskWorkflow, durable.TaskActivity, durable.TaskWorkflow} {
		if worked, workErr := worker.RunOnce(t.Context(), kind); workErr != nil || !worked {
			t.Fatalf("initial task %s: %t, %v", kind, worked, workErr)
		}
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
	for _, kind := range []durable.TaskKind{durable.TaskTimer, durable.TaskWorkflow} {
		if worked, workErr := worker.RunOnce(t.Context(), kind); workErr != nil || !worked {
			t.Fatalf("recovery task %s: %t, %v", kind, worked, workErr)
		}
	}
	execution, err := reopened.GetExecution(t.Context(), key)
	if err != nil || execution.State != durable.StateCompleted || string(execution.Output) != "paid" || calls != 1 {
		t.Fatalf("recovered execution: %+v, activity calls=%d, %v", execution, calls, err)
	}
}
