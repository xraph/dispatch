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
	progress := []byte("offset:12")
	timeout := 2 * time.Hour
	req := durable.CommitRequest{Key: r.Key, RequestID: "timer", ExpectedRevision: 1, Token: claimed.Token(),
		TaskUpdate: &durable.TaskUpdate{Action: durable.TaskRetry, RetryAfter: time.Hour, DeadlineAfter: &timeout, Progress: &progress},
		Events:     []durable.EventInput{{Type: "timer.scheduled"}},
		Tasks:      []durable.TaskSpec{{ID: "timer", Kind: durable.TaskTimer, Queue: "timers", AvailableAt: time.Now().Add(-time.Second)}}}
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
	pending, err := reopened.GetTask(t.Context(), r.Key, claimed.ID)
	if err != nil || pending.Done || pending.Owner != "" || !pending.LeaseUntil.IsZero() || string(pending.Progress) != "offset:12" ||
		pending.Version != claimed.Version+1 || !pending.DeadlineAt.Equal(pending.AvailableAt.Add(timeout)) {
		t.Fatalf("retry state after pool replacement: %+v, %v", pending, err)
	}
	if _, err = reopened.RenewTask(t.Context(), r.Key, claimed.Token(), time.Minute); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("old token after pool replacement: %v", err)
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
		`DELETE FROM grove_migrations WHERE version IN ($1, $2, $3, $4)`, "20261010120000", "20261011120000", "20261012120000", "20261013120000")
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

func TestDurableDeadlineCheckedAfterTaskLock(t *testing.T) {
	t.Run("condition", func(t *testing.T) { testDurableDeadlineCheckedAfterTaskLock(t, false) })
	t.Run("terminal", func(t *testing.T) { testDurableDeadlineCheckedAfterTaskLock(t, true) })
}

func testDurableDeadlineCheckedAfterTaskLock(t *testing.T, terminal bool) {
	t.Helper()
	s := setupTestStore(t)
	r := durable.StartRequest{Key: durable.Key{Namespace: t.Name(), WorkflowID: "order", RunID: "run"},
		RequestID: "start", WorkflowType: "order", BuildID: "v1", Queue: "orders"}
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: r.Queue,
		Kind: durable.TaskWorkflow, Owner: "worker", LeaseDuration: time.Minute})
	if err != nil || task == nil {
		t.Fatalf("claim: %+v, %v", task, err)
	}
	timeout := time.Second
	_, err = s.CommitTransition(t.Context(), durable.CommitRequest{Key: r.Key, RequestID: "begin", ExpectedRevision: 1,
		Token: task.Token(), Events: []durable.EventInput{{Type: "attempt.started"}},
		TaskUpdate: &durable.TaskUpdate{Action: durable.TaskKeep, DeadlineAfter: &timeout},
		Tasks:      []durable.TaskSpec{{ID: "target", Kind: durable.TaskActivity, Queue: "effects"}}})
	if err != nil {
		t.Fatal(err)
	}
	source, err := s.GetTask(t.Context(), r.Key, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	target, err := s.GetTask(t.Context(), r.Key, "target")
	if err != nil {
		t.Fatal(err)
	}
	pg := pgdriver.Unwrap(s.DB())
	tx, err := pg.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec(t.Context(), `SELECT 1 FROM dispatch_execution_tasks
        WHERE namespace=$1 AND task_id=$2 FOR UPDATE`, r.Namespace, target.ID); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		request := durable.CommitRequest{Key: r.Key, RequestID: "finish", ExpectedRevision: 2,
			Token: task.Token(), Events: []durable.EventInput{{Type: "attempt.completed"}}}
		if terminal {
			request.State = durable.StateCompleted
		} else {
			request.Conditions = []durable.TaskCondition{{TaskID: target.ID, Version: target.Version}}
			request.CancelTasks = []string{target.ID}
		}
		_, commitErr := s.CommitTransition(t.Context(), request)
		result <- commitErr
	}()
	limit := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		if err = pg.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
            WHERE wait_event_type='Lock' AND query LIKE '%dispatch_execution_tasks%')`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(limit) {
			t.Fatal("completion did not wait for target lock")
		}
		time.Sleep(5 * time.Millisecond)
	}
	for {
		var expired bool
		if err = pg.QueryRow(t.Context(), `SELECT clock_timestamp() >= $1`, source.DeadlineAt).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			break
		}
		if time.Now().After(limit) {
			t.Fatal("source deadline did not expire")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	select {
	case commitErr := <-result:
		if !errors.Is(commitErr, durable.ErrTaskDeadline) {
			t.Fatalf("completion used pre-lock deadline check: %v", commitErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("completion stayed blocked")
	}
	execution, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || execution.Revision != 2 {
		t.Fatalf("expired completion advanced state: %+v, %v", execution, err)
	}
}

func TestDurableTaskControlRollbackOnReceiptFailure(t *testing.T) {
	s := setupTestStore(t)
	r := durable.StartRequest{Key: durable.Key{Namespace: t.Name(), WorkflowID: "order", RunID: "run"},
		RequestID: "start", WorkflowType: "order", BuildID: "v1", Queue: "orders"}
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: r.Queue,
		Kind: durable.TaskWorkflow, Owner: "worker", LeaseDuration: time.Minute})
	if err != nil || task == nil {
		t.Fatalf("claim: %+v, %v", task, err)
	}
	_, err = s.CommitTransition(t.Context(), durable.CommitRequest{Key: r.Key, RequestID: "begin", ExpectedRevision: 1,
		Token: task.Token(), Events: []durable.EventInput{{Type: "attempt.started"}}, TaskUpdate: &durable.TaskUpdate{Action: durable.TaskKeep},
		Tasks: []durable.TaskSpec{{ID: "target", Kind: durable.TaskActivity, Queue: "effects"}}})
	if err != nil {
		t.Fatal(err)
	}
	target, err := s.GetTask(t.Context(), r.Key, "target")
	if err != nil {
		t.Fatal(err)
	}
	progress := []byte("offset:42")
	request := durable.CommitRequest{Key: r.Key, RequestID: "failing-receipt", ExpectedRevision: 2, Token: task.Token(),
		Events:     []durable.EventInput{{Type: "attempt.progress"}},
		TaskUpdate: &durable.TaskUpdate{Action: durable.TaskKeep, Progress: &progress},
		Tasks:      []durable.TaskSpec{{ID: "next", Kind: durable.TaskActivity, Queue: "effects"}},
		Conditions: []durable.TaskCondition{{TaskID: target.ID, Version: target.Version}}, CancelTasks: []string{target.ID}}
	pg := pgdriver.Unwrap(s.DB())
	_, err = pg.Exec(t.Context(), `CREATE FUNCTION reject_execution_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.request_id = 'failing-receipt' THEN RAISE EXCEPTION 'injected receipt failure'; END IF;
          RETURN NEW;
        END $$;
        CREATE TRIGGER reject_execution_receipt BEFORE INSERT ON dispatch_execution_receipts
        FOR EACH ROW EXECUTE FUNCTION reject_execution_receipt()`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CommitTransition(t.Context(), request); err == nil {
		t.Fatal("receipt failure was hidden")
	}
	source, err := s.GetTask(t.Context(), r.Key, task.ID)
	if err != nil || source.Done || len(source.Progress) != 0 || source.Version != task.Version+1 {
		t.Fatalf("receipt failure leaked source update: %+v, %v", source, err)
	}
	other, err := s.GetTask(t.Context(), r.Key, target.ID)
	if err != nil || other.Done || other.Version != target.Version {
		t.Fatalf("receipt failure leaked cancellation: %+v, %v", other, err)
	}
	if _, err = s.GetTask(t.Context(), r.Key, "next"); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("receipt failure leaked next task: %v", err)
	}
	execution, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || execution.Revision != 2 || execution.LastSequence != 2 {
		t.Fatalf("receipt failure leaked history: %+v, %v", execution, err)
	}
	if _, err = pg.Exec(t.Context(), `DROP TRIGGER reject_execution_receipt ON dispatch_execution_receipts`); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CommitTransition(t.Context(), request); err != nil {
		t.Fatalf("failed transaction persisted its receipt: %v", err)
	}
}
