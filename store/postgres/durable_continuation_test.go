//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/grove/migrate"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/store/postgres"
)

func postgresContinueRequest(t *testing.T, s *postgres.Store, r durable.StartRequest, next string) durable.CommitRequest {
	t.Helper()
	task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: r.Queue, Kind: durable.TaskWorkflow, BuildID: r.BuildID, Owner: "continue", LeaseDuration: time.Minute})
	if err != nil || task == nil || task.Key != r.Key {
		t.Fatalf("continuation task: %+v %v", task, err)
	}
	e, err := s.GetExecution(t.Context(), r.Key)
	if err != nil {
		t.Fatal(err)
	}
	return durable.CommitRequest{Key: r.Key, RequestID: "continue", ExpectedRevision: e.Revision, Token: task.Token(), State: durable.StateContinuedAsNew, Events: []durable.EventInput{{Type: "workflow.waiting"}}, Continuation: &durable.ContinueSpec{RunID: next, WorkflowType: r.WorkflowType, BuildID: r.BuildID, Queue: r.Queue, Input: []byte("next"), RunTimeout: r.RunTimeout}}
}

func TestDurableContinuationRollback(t *testing.T) {
	for _, boundary := range []struct{ name, table, operation, condition string }{
		{"outbox", "dispatch_child_deliveries", "INSERT", "true"},
		{"source_event", "dispatch_execution_events", "INSERT", "NEW.type='workflow.continued_as_new'"},
		{"source_projection", "dispatch_executions", "UPDATE", "NEW.state='continued_as_new'"},
		{"source_task", "dispatch_execution_tasks", "UPDATE", "NEW.done"},
		{"receipt", "dispatch_execution_receipts", "INSERT", "NEW.request_id='continue'"},
		{"successor", "dispatch_executions", "INSERT", "NEW.run_id='next'"},
		{"head", "dispatch_execution_heads", "UPDATE", "NEW.run_id='next'"},
		{"successor_event", "dispatch_execution_events", "INSERT", "NEW.run_id='next' AND NEW.sequence=1"},
		{"lineage", "dispatch_execution_events", "INSERT", "NEW.type='workflow.run_started'"},
		{"carry", "dispatch_execution_events", "INSERT", "NEW.type='workflow.signal_carried'"},
		{"successor_task", "dispatch_execution_tasks", "INSERT", "NEW.run_id='next'"},
		{"commit", "dispatch_execution_receipts", "INSERT", "NEW.request_id='continue'"},
	} {
		t.Run(boundary.name, func(t *testing.T) {
			s := setupTestStore(t)
			parent, _ := deliveryPair(t, s)
			if _, err := s.SignalExecution(t.Context(), durable.SignalRequest{Key: parent.Key, RequestID: "pending", BuildID: parent.BuildID, Name: "message", Input: []byte("carry")}); err != nil {
				t.Fatal(err)
			}
			r := postgresContinueRequest(t, s, parent, "next")
			before, err := s.GetExecution(t.Context(), parent.Key)
			if err != nil {
				t.Fatal(err)
			}
			pg := pgdriver.Unwrap(s.DB())
			trigger := fmt.Sprintf(`CREATE TRIGGER reject_continuation_write BEFORE %s ON %s FOR EACH ROW EXECUTE FUNCTION reject_continuation_write()`, boundary.operation, boundary.table)
			if boundary.name == "commit" {
				trigger = `CREATE CONSTRAINT TRIGGER reject_continuation_write AFTER INSERT ON dispatch_execution_receipts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_continuation_write()`
			}
			_, err = pg.Exec(t.Context(), fmt.Sprintf(`CREATE FUNCTION reject_continuation_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF %s THEN RAISE EXCEPTION 'injected continuation failure'; END IF; RETURN NEW; END $$; %s`, boundary.condition, trigger))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.CommitTransition(t.Context(), r); err == nil || !strings.Contains(err.Error(), "injected continuation failure") {
				t.Fatalf("fault not reached: %v", err)
			}
			after, err := s.GetExecution(t.Context(), parent.Key)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("partial source: %+v %v", after, err)
			}
			key := parent.Key
			key.RunID = "next"
			if _, err = s.GetExecution(t.Context(), key); !errors.Is(err, durable.ErrNotFound) {
				t.Fatalf("partial successor: %v", err)
			}
			target, err := s.ResolveExecution(t.Context(), latestTarget(parent.Key))
			if err != nil || target.Key != parent.Key {
				t.Fatalf("partial head: %+v %v", target, err)
			}
			history, err := s.ReadHistory(t.Context(), parent.Key, 0, 1000)
			if err != nil || int64(len(history)) != before.LastSequence {
				t.Fatalf("partial history: %d %v", len(history), err)
			}
			outbox, err := s.ListChildDeliveries(t.Context(), parent.Key, "", 100)
			if err != nil || len(outbox) != 0 {
				t.Fatalf("partial child close: %+v %v", outbox, err)
			}
			task, err := s.GetTask(t.Context(), parent.Key, r.Token.TaskID)
			if err != nil || task.Done {
				t.Fatalf("partial grant: %+v %v", task, err)
			}
			var accepted bool
			if err = pg.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM dispatch_execution_receipts WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND request_id=$4)`, r.Namespace, r.WorkflowID, r.RunID, r.RequestID).Scan(&accepted); err != nil || accepted {
				t.Fatalf("partial continuation receipt: %t %v", accepted, err)
			}
			if _, err = pg.Exec(t.Context(), fmt.Sprintf(`DROP TRIGGER reject_continuation_write ON %s; DROP FUNCTION reject_continuation_write()`, boundary.table)); err != nil {
				t.Fatal(err)
			}
			if _, err = s.CommitTransition(t.Context(), r); err != nil {
				t.Fatalf("retry after rollback: %v", err)
			}
			created, err := s.GetExecution(t.Context(), key)
			if err != nil || created.PreviousRunID != r.RunID || created.State != durable.StateRunning {
				t.Fatalf("retry did not create successor: %+v %v", created, err)
			}
		})
	}
}

func continuationMigration(t *testing.T, s *postgres.Store) (*migrate.Migration, migrate.Executor) {
	t.Helper()
	exec, err := migrate.NewExecutorFor(pgdriver.Unwrap(s.DB()))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range postgres.Migrations.Migrations() {
		if m.Version == "20261024120000" {
			return m, exec
		}
	}
	t.Fatal("continuation migration missing")
	return nil, nil
}

func TestDurableContinuationRecovery(t *testing.T) {
	s, dsn := setupTestStoreConnection(t)
	migration, exec := continuationMigration(t, s)
	for range 2 {
		if err := migration.Down(t.Context(), exec); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if err := migration.Up(t.Context(), exec); err != nil {
			t.Fatal(err)
		}
	}
	r := signalStartRequest(t)
	r.RunTimeout = time.Minute
	r.ExecutionTimeout = time.Hour
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	request := postgresContinueRequest(t, s, r, "next")
	// Simulate the guard installed by the preceding schema release.
	pg := pgdriver.Unwrap(s.DB())
	_, err := pg.Exec(t.Context(), `CREATE OR REPLACE FUNCTION dispatch_guard_run_chain() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.next_run_id IS DISTINCT FROM OLD.next_run_id THEN RAISE EXCEPTION USING ERRCODE='DX004',MESSAGE='run lineage is immutable'; END IF; RETURN NEW; END $$`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CommitTransition(t.Context(), request); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("old guard fixture: %v", err)
	}
	if err = migration.Up(t.Context(), exec); err != nil {
		t.Fatal(err)
	}
	receipt, err := s.CommitTransition(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	base, baseExec := runChainMigration(t, s)
	if err = base.Up(t.Context(), baseExec); err != nil {
		t.Fatal(err)
	}
	if err = migration.Up(t.Context(), exec); err != nil {
		t.Fatal(err)
	}
	if err = migration.Down(t.Context(), exec); err == nil || !strings.Contains(err.Error(), "retained run chains") {
		t.Fatalf("destructive downgrade: %v", err)
	}
	s = reopenAsyncStore(t, s, dsn)
	next := r
	next.RunID = "next"
	second := postgresContinueRequest(t, s, next, "third")
	if _, err = s.CommitTransition(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	s = reopenAsyncStore(t, s, dsn)
	pg = pgdriver.Unwrap(s.DB())
	lock, err := pg.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback() }()
	if _, err = lock.Exec(t.Context(), `SELECT 1 FROM dispatch_executions WHERE namespace=$1 FOR UPDATE`, r.Namespace); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	if got, retryErr := s.CommitTransition(ctx, request); retryErr != nil || got != receipt {
		t.Fatalf("receipt waited on later ownership: %+v %v", got, retryErr)
	}
}

func TestDurableContinuationPairIntegrity(t *testing.T) {
	s := setupTestStore(t)
	r := signalStartRequest(t)
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	pg := pgdriver.Unwrap(s.DB())
	if _, err := pg.Exec(t.Context(), `UPDATE dispatch_executions SET state='continued_as_new',revision=revision+1,last_sequence=last_sequence+1,next_run_id='missing' WHERE namespace=$1`, r.Namespace); err == nil || !strings.Contains(err.Error(), "DX004") {
		t.Fatalf("dangling source link: %v", err)
	}
	e, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || e.State != durable.StateRunning || e.NextRunID != "" {
		t.Fatalf("pair rollback: %+v %v", e, err)
	}
	request := postgresContinueRequest(t, s, r, "next")
	if _, err = s.CommitTransition(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"next_run_id=''", "previous_run_id='other'", "first_run_id='other'", "created_at=created_at+interval '1 second'"} {
		if _, err = pg.Exec(t.Context(), `UPDATE dispatch_executions SET `+mutation+` WHERE namespace=$1 AND run_id=$2`, r.Namespace, r.RunID); err == nil || !strings.Contains(err.Error(), "DX004") {
			t.Fatalf("changed linked metadata %s: %v", mutation, err)
		}
	}
}

func TestDurableContinuationLockExpiry(t *testing.T) {
	for _, kind := range []string{"execution", "task"} {
		t.Run(kind, func(t *testing.T) {
			s := setupTestStore(t)
			r := signalStartRequest(t)
			r.RunTimeout = 300 * time.Millisecond
			if _, err := s.StartExecution(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			request := postgresContinueRequest(t, s, r, "next")
			before, err := s.GetExecution(t.Context(), r.Key)
			if err != nil {
				t.Fatal(err)
			}
			pg := pgdriver.Unwrap(s.DB())
			lock, err := pg.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = lock.Rollback() }()
			table := "dispatch_executions"
			if kind == "task" {
				table = "dispatch_execution_tasks"
			}
			if _, err = lock.Exec(t.Context(), `SELECT 1 FROM `+table+` WHERE namespace=$1 FOR UPDATE`, r.Namespace); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { _, callErr := s.CommitTransition(ctx, request); done <- callErr }()
			for {
				var waiting bool
				if err = pg.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%dispatch_execution%' AND query LIKE '%FOR UPDATE%')`).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				select {
				case early := <-done:
					t.Fatalf("did not wait: %v", early)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(5 * time.Millisecond):
				}
			}
			waitDurableStoreTime(t, s, before.RunDeadlineAt)
			if err = lock.Commit(); err != nil {
				t.Fatal(err)
			}
			if err = <-done; !errors.Is(err, durable.ErrExecutionDeadline) {
				t.Fatalf("expired handoff: %v", err)
			}
			after, err := s.GetExecution(t.Context(), r.Key)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("expired handoff changed state: %+v %v", after, err)
			}
		})
	}
}
