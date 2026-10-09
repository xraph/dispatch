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

func workflowRetryStart(t *testing.T) durable.StartRequest {
	t.Helper()
	r := signalStartRequest(t)
	r.RunTimeout, r.ExecutionTimeout = time.Hour, 3*time.Hour
	r.RetryPolicy = &durable.WorkflowRetryPolicy{InitialInterval: 100 * time.Millisecond, MaximumAttempts: 3}
	return r
}

func postgresFailureRequest(t *testing.T, s *postgres.Store, r durable.StartRequest) durable.CommitRequest {
	t.Helper()
	request := childCloseRequest(t, s, r)
	request.State, request.Output = durable.StateFailed, nil
	request.Events = []durable.EventInput{{Type: "workflow.failed", Payload: []byte(`{"type":"temporary","message":"retry"}`)}}
	return request
}

func TestDurableWorkflowRetryRollback(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		for _, boundary := range []struct{ name, table, operation, condition string }{
			{"outbox", "dispatch_child_deliveries", "INSERT", "true"},
			{"retry_event", "dispatch_execution_events", "INSERT", "NEW.type='workflow.retry_scheduled'"},
			{"terminal", "dispatch_execution_events", "INSERT", "NEW.type IN ('workflow.failed','workflow.timed_out')"},
			{"source_projection", "dispatch_executions", "UPDATE", "NEW.next_run_id<>''"},
			{"source_task", "dispatch_execution_tasks", "UPDATE", "NEW.done"},
			{"receipt", "dispatch_execution_receipts", "INSERT", "true"},
			{"successor", "dispatch_executions", "INSERT", "NEW.retry_attempt=2"},
			{"head", "dispatch_execution_heads", "UPDATE", "NEW.run_id LIKE 'retry-%'"},
			{"successor_event", "dispatch_execution_events", "INSERT", "NEW.run_id LIKE 'retry-%' AND NEW.sequence=1"},
			{"lineage", "dispatch_execution_events", "INSERT", "NEW.type='workflow.run_started'"},
			{"carry", "dispatch_execution_events", "INSERT", "NEW.type='workflow.signal_carried'"},
			{"successor_task", "dispatch_execution_tasks", "INSERT", "NEW.run_id LIKE 'retry-%'"},
			{"commit", "dispatch_execution_receipts", "INSERT", "true"},
		} {
			t.Run(fmt.Sprintf("timeout_%t/%s", timeout, boundary.name), func(t *testing.T) {
				s := setupTestStore(t)
				r := workflowRetryStart(t)
				if timeout {
					r.RunTimeout = 500 * time.Millisecond
				}
				if _, err := s.StartExecution(t.Context(), r); err != nil {
					t.Fatal(err)
				}
				if boundary.name == "outbox" {
					create := childCloseRequest(t, s, r)
					create.RequestID, create.State, create.Output = "children", durable.StateRunning, nil
					create.Events = []durable.EventInput{{Type: "workflow.waiting"}}
					create.Tasks = []durable.TaskSpec{{ID: "parent-next", Kind: durable.TaskWorkflow, Queue: r.Queue}}
					child := r.Clone()
					child.WorkflowID, child.RunID, child.RetryPolicy = "child", "child-run", nil
					child.RunTimeout = 0
					create.Children = []durable.ChildStartSpec{{CommandID: "child", Start: child, ParentQueue: r.Queue, ParentClosePolicy: durable.ParentCloseTerminate}}
					if _, err := s.CommitTransition(t.Context(), create); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := s.SignalExecution(t.Context(), durable.SignalRequest{Key: r.Key, RequestID: "pending", BuildID: r.BuildID, Name: "items", Input: []byte("carry")}); err != nil {
					t.Fatal(err)
				}
				before, err := s.GetExecution(t.Context(), r.Key)
				if err != nil {
					t.Fatal(err)
				}
				var apply func() (durable.Receipt, error)
				requestID, taskID := "close", "workflow:1"
				if boundary.name == "outbox" {
					taskID = "parent-next"
				}
				if timeout {
					waitDurableStoreTime(t, s, before.RunDeadlineAt)
					_, request := timeoutGrant(t, s, r, time.Minute)
					requestID = request.RequestID
					apply = func() (durable.Receipt, error) { return s.ApplyExecutionTimeout(t.Context(), request) }
				} else {
					request := postgresFailureRequest(t, s, r)
					taskID = request.Token.TaskID
					apply = func() (durable.Receipt, error) { return s.CommitTransition(t.Context(), request) }
				}
				pg := pgdriver.Unwrap(s.DB())
				trigger := fmt.Sprintf(`CREATE TRIGGER reject_workflow_retry BEFORE %s ON %s FOR EACH ROW EXECUTE FUNCTION reject_workflow_retry()`, boundary.operation, boundary.table)
				if boundary.name == "commit" {
					trigger = `CREATE CONSTRAINT TRIGGER reject_workflow_retry AFTER INSERT ON dispatch_execution_receipts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_workflow_retry()`
				}
				if _, err := pg.Exec(t.Context(), fmt.Sprintf(`CREATE FUNCTION reject_workflow_retry() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF %s THEN RAISE EXCEPTION 'injected workflow retry failure'; END IF; RETURN NEW; END $$; %s`, boundary.condition, trigger)); err != nil {
					t.Fatal(err)
				}
				if _, err := apply(); err == nil || !strings.Contains(err.Error(), "injected workflow retry failure") {
					t.Fatalf("fault not reached: %v", err)
				}
				after, err := s.GetExecution(t.Context(), r.Key)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("partial source: %+v %v", after, err)
				}
				identity, err := durable.Fingerprint("workflow-retry", r.Key)
				if err != nil {
					t.Fatal(err)
				}
				key := r.Key
				key.RunID = "retry-" + identity
				if _, err := s.GetExecution(t.Context(), key); !errors.Is(err, durable.ErrNotFound) {
					t.Fatalf("partial successor: %v", err)
				}
				latest, err := s.ResolveExecution(t.Context(), latestTarget(r.Key))
				if err != nil || latest.Key != r.Key {
					t.Fatalf("partial head: %+v %v", latest, err)
				}
				events, err := s.ReadHistory(t.Context(), r.Key, 0, 1000)
				if err != nil || int64(len(events)) != before.LastSequence {
					t.Fatalf("partial history: %+v %v", events, err)
				}
				outbox, err := s.ListChildDeliveries(t.Context(), r.Key, "", 100)
				if err != nil || len(outbox) != 0 {
					t.Fatalf("partial outbox: %+v %v", outbox, err)
				}
				task, err := s.GetTask(t.Context(), r.Key, taskID)
				if err != nil || task.Done {
					t.Fatalf("partial task: %+v %v", task, err)
				}
				var accepted bool
				if err := pg.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM dispatch_execution_receipts WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND request_id=$4)`, r.Namespace, r.WorkflowID, r.RunID, requestID).Scan(&accepted); err != nil || accepted {
					t.Fatalf("partial receipt: %t %v", accepted, err)
				}
				if _, err := pg.Exec(t.Context(), fmt.Sprintf(`DROP TRIGGER reject_workflow_retry ON %s; DROP FUNCTION reject_workflow_retry()`, boundary.table)); err != nil {
					t.Fatal(err)
				}
				receipt, err := apply()
				if err != nil {
					t.Fatalf("retry after rollback: %v", err)
				}
				if again, err := apply(); err != nil || again != receipt {
					t.Fatalf("exact retry: %+v %v", again, err)
				}
				created, err := s.GetExecution(t.Context(), key)
				if err != nil || created.PreviousRunID != r.RunID || created.WorkflowAttempt() != 2 {
					t.Fatalf("successor missing: %+v %v", created, err)
				}
			})
		}
	}
}

func workflowRetryMigration(t *testing.T, s *postgres.Store) (*migrate.Migration, migrate.Executor) {
	t.Helper()
	exec, err := migrate.NewExecutorFor(pgdriver.Unwrap(s.DB()))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range postgres.Migrations.Migrations() {
		if m.Version == "20261025120000" {
			return m, exec
		}
	}
	t.Fatal("workflow retry migration missing")
	return nil, nil
}

func TestDurableWorkflowRetryMigration(t *testing.T) {
	s, dsn := setupTestStoreConnection(t)
	m, exec := workflowRetryMigration(t, s)
	for range 2 {
		if err := m.Down(t.Context(), exec); err != nil {
			t.Fatal(err)
		}
	}
	// An older writer creates a root without any new retry columns.
	pg := pgdriver.Unwrap(s.DB())
	if _, err := pg.Exec(t.Context(), `INSERT INTO dispatch_executions(namespace,workflow_id,run_id,workflow_type,build_id,state,revision,last_sequence,input,output,created_at,updated_at) VALUES('old-writer','workflow','root','order','v1','running',1,1,''::bytea,''::bytea,clock_timestamp(),clock_timestamp())`); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := m.Up(t.Context(), exec); err != nil {
			t.Fatal(err)
		}
	}
	old, err := s.GetExecution(t.Context(), durable.Key{Namespace: "old-writer", WorkflowID: "workflow", RunID: "root"})
	if err != nil || old.RetryPolicy != nil || old.RetryAttempt != 1 || !old.RunAvailableAt.Equal(old.CreatedAt) {
		t.Fatalf("backfill: %+v %v", old, err)
	}
	r := workflowRetryStart(t)
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"retry_attempt=2", "retry_policy=NULL", "run_available_at=run_available_at+interval '1 second'"} {
		if _, err := pg.Exec(t.Context(), `UPDATE dispatch_executions SET `+mutation+` WHERE namespace=$1`, r.Namespace); err == nil || !strings.Contains(err.Error(), "DX004") {
			t.Fatalf("immutable retry metadata changed: %s %v", mutation, err)
		}
	}
	r.WorkflowID, r.RunTimeout = "timeout", time.Microsecond
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	_, request := timeoutGrant(t, s, r, time.Minute)
	for _, get := range []func(*testing.T, *postgres.Store) (*migrate.Migration, migrate.Executor){deadlineMigration, timeoutMigration, runChainMigration, continuationMigration, workflowRetryMigration} {
		prior, executor := get(t, s)
		if err := prior.Up(t.Context(), executor); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pg.Exec(t.Context(), `CREATE OR REPLACE FUNCTION dispatch_execution_timeout_update_allowed(old_run dispatch_executions,new_run dispatch_executions) RETURNS boolean LANGUAGE sql AS $$ SELECT FALSE $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyExecutionTimeout(t.Context(), request); !errors.Is(err, durable.ErrExecutionDeadline) {
		t.Fatalf("preceding guard fixture did not reject retry: %v", err)
	}
	if err := m.Up(t.Context(), exec); err != nil {
		t.Fatal(err)
	}
	if err := m.Down(t.Context(), exec); err == nil || !strings.Contains(err.Error(), "retained workflow retry metadata") {
		t.Fatalf("destructive retry downgrade: %v", err)
	}
	s = reopenAsyncStore(t, s, dsn)
	receipt, err := s.ApplyExecutionTimeout(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	e, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || e.NextRunID == "" {
		t.Fatalf("timeout after migration retries: %+v %v", e, err)
	}
	s = reopenAsyncStore(t, s, dsn)
	pg = pgdriver.Unwrap(s.DB())
	lock, err := pg.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback() }()
	if _, err := lock.Exec(t.Context(), `SELECT 1 FROM dispatch_executions WHERE namespace=$1 FOR UPDATE`, r.Namespace); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	if again, err := s.ApplyExecutionTimeout(ctx, request); err != nil || again != receipt {
		t.Fatalf("exact timeout recovery waited on chain: %+v %v", again, err)
	}
}

func TestDurableWorkflowRetryLockExpiry(t *testing.T) {
	for _, mode := range []string{"failure_lease", "failure_deadline", "timeout_lease", "timeout_deadline"} {
		t.Run(mode, func(t *testing.T) {
			s := setupTestStore(t)
			r := workflowRetryStart(t)
			if strings.HasPrefix(mode, "timeout") {
				r.RunTimeout = time.Microsecond
			}
			if strings.HasSuffix(mode, "deadline") {
				r.ExecutionTimeout = 300 * time.Millisecond
			}
			if _, err := s.StartExecution(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			var apply func() error
			var until time.Time
			if strings.HasPrefix(mode, "timeout") {
				ttl := 250 * time.Millisecond
				if mode == "timeout_deadline" {
					ttl = time.Minute
				}
				grant, request := timeoutGrant(t, s, r, ttl)
				until = grant.LeaseUntil
				apply = func() error { _, err := s.ApplyExecutionTimeout(ctx, request); return err }
			} else {
				ttl := time.Minute
				if mode == "failure_lease" {
					ttl = 250 * time.Millisecond
				}
				task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: r.Queue, BuildID: r.BuildID, Kind: durable.TaskWorkflow, Owner: "failure", LeaseDuration: ttl})
				if err != nil || task == nil {
					t.Fatalf("claim: %+v %v", task, err)
				}
				until = task.LeaseUntil
				request := durable.CommitRequest{Key: r.Key, RequestID: "failure", ExpectedRevision: 1, Token: task.Token(), State: durable.StateFailed, Events: []durable.EventInput{{Type: "workflow.failed", Payload: []byte(`{"type":"temporary","message":"retry"}`)}}}
				apply = func() error { _, err := s.CommitTransition(ctx, request); return err }
			}
			e, err := s.GetExecution(t.Context(), r.Key)
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasSuffix(mode, "deadline") {
				until = e.ExecutionDeadlineAt
			}
			pg := pgdriver.Unwrap(s.DB())
			lock, err := pg.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = lock.Rollback() }()
			if _, err := lock.Exec(t.Context(), `SELECT 1 FROM dispatch_execution_tasks WHERE namespace=$1 FOR UPDATE`, r.Namespace); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- apply() }()
			for {
				var waiting bool
				if err := pg.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%dispatch_execution_tasks%' AND query LIKE '%FOR UPDATE%')`).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				select {
				case err := <-done:
					t.Fatalf("did not wait: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(5 * time.Millisecond):
				}
			}
			waitDurableStoreTime(t, s, until)
			if err := lock.Commit(); err != nil {
				t.Fatal(err)
			}
			err = <-done
			want := durable.ErrLeaseLost
			if mode == "failure_deadline" {
				want = durable.ErrExecutionDeadline
			}
			if mode == "timeout_deadline" {
				want = nil
			}
			if !errors.Is(err, want) {
				t.Fatalf("closure after wait: %v want %v", err, want)
			}
			after, err := s.GetExecution(t.Context(), r.Key)
			if err != nil || after.NextRunID != "" {
				t.Fatalf("expired retry created successor: %+v %v", after, err)
			}
			if want == nil && after.State != durable.StateTimedOut {
				t.Fatalf("expired chain not closed: %+v", after)
			}
		})
	}
}

func TestDurableWorkflowRetryBackfillRollback(t *testing.T) {
	s := setupTestStore(t)
	m, exec := workflowRetryMigration(t, s)
	if err := m.Down(t.Context(), exec); err != nil {
		t.Fatal(err)
	}
	pg := pgdriver.Unwrap(s.DB())
	if _, err := pg.Exec(t.Context(), `INSERT INTO dispatch_executions(namespace,workflow_id,run_id,workflow_type,build_id,state,revision,last_sequence,input,output,created_at,updated_at) VALUES('old-writer','workflow','root','order','v1','running',1,1,''::bytea,''::bytea,clock_timestamp(),clock_timestamp());
 CREATE FUNCTION reject_retry_backfill() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected retry backfill failure'; END $$;
 CREATE TRIGGER reject_retry_backfill BEFORE UPDATE ON dispatch_executions FOR EACH ROW EXECUTE FUNCTION reject_retry_backfill()`); err != nil {
		t.Fatal(err)
	}
	if err := m.Up(t.Context(), exec); err == nil || !strings.Contains(err.Error(), "injected retry backfill failure") {
		t.Fatalf("backfill fault missing: %v", err)
	}
	var added, guarded bool
	if err := pg.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='dispatch_executions'::regclass AND attname='retry_attempt' AND NOT attisdropped), EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='dispatch_executions'::regclass AND tgname='dispatch_execution_deadline_update')`).Scan(&added, &guarded); err != nil || added || !guarded {
		t.Fatalf("partial migration: added=%t guarded=%t %v", added, guarded, err)
	}
	if _, err := pg.Exec(t.Context(), `DROP TRIGGER reject_retry_backfill ON dispatch_executions; DROP FUNCTION reject_retry_backfill()`); err != nil {
		t.Fatal(err)
	}
	if err := m.Up(t.Context(), exec); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.Exec(t.Context(), `INSERT INTO dispatch_executions(namespace,workflow_id,run_id,workflow_type,build_id,state,revision,last_sequence,input,output,created_at,updated_at) VALUES('old-writer','new-workflow','root','order','v1','running',1,1,''::bytea,''::bytea,clock_timestamp(),clock_timestamp())`); err != nil {
		t.Fatal(err)
	}
	for _, workflow := range []string{"workflow", "new-workflow"} {
		e, err := s.GetExecution(t.Context(), durable.Key{Namespace: "old-writer", WorkflowID: workflow, RunID: "root"})
		if err != nil || e.RetryAttempt != 1 || e.RetryPolicy != nil || !e.RunAvailableAt.Equal(e.CreatedAt) {
			t.Fatalf("old root backfill/default: %+v %v", e, err)
		}
	}
}

func TestDurableWorkflowRetryCapacityMigration(t *testing.T) {
	s, dsn := setupTestStoreConnection(t)
	pg := pgdriver.Unwrap(s.DB())
	var migration *migrate.Migration
	for _, m := range postgres.Migrations.Migrations() {
		if m.Name == "repair_durable_retry_capacity_closure" {
			migration = m
		}
	}
	if migration == nil {
		t.Fatal("capacity closure migration missing")
	}
	_, exec := workflowRetryMigration(t, s)
	for range 2 {
		if err := migration.Up(t.Context(), exec); err != nil {
			t.Fatal(err)
		}
	}
	r := workflowRetryStart(t)
	r.RunTimeout = time.Second
	r.ExecutionTimeout = 0
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		if _, err := s.SignalExecution(t.Context(), durable.SignalRequest{Key: r.Key, RequestID: fmt.Sprint(i), BuildID: r.BuildID, Name: "large", Input: make([]byte, 1<<20)}); err != nil {
			t.Fatal(err)
		}
	}
	e, err := s.GetExecution(t.Context(), r.Key)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(max(time.Until(e.RunDeadlineAt)+time.Millisecond, 0))
	_, request := timeoutGrant(t, s, r, time.Minute)
	if _, err := pg.Exec(t.Context(), `CREATE OR REPLACE FUNCTION dispatch_execution_timeout_update_allowed(old_run dispatch_executions,new_run dispatch_executions) RETURNS boolean LANGUAGE sql AS $$ SELECT FALSE $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyExecutionTimeout(t.Context(), request); !errors.Is(err, durable.ErrExecutionDeadline) {
		t.Fatalf("old guard accepted suppression: %v", err)
	}
	after, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || after.State != durable.StateRunning || after.LastSequence != e.LastSequence || after.Revision != e.Revision {
		t.Fatalf("rejected suppression leaked state: %+v %v", after, err)
	}
	tail, err := s.ReadHistory(t.Context(), r.Key, e.LastSequence, 1000)
	if err != nil || len(tail) != 0 {
		t.Fatalf("rejected suppression leaked history: %+v %v", tail, err)
	}
	for range 2 {
		if err := migration.Up(t.Context(), exec); err != nil {
			t.Fatal(err)
		}
	}
	receipt, err := s.ApplyExecutionTimeout(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	s = reopenAsyncStore(t, s, dsn)
	if again, err := s.ApplyExecutionTimeout(t.Context(), request); err != nil || again != receipt {
		t.Fatalf("suppression recovery: %+v %v", again, err)
	}
	if err := migration.Down(t.Context(), exec); err == nil {
		t.Fatal("downgrade discarded retained suppression support")
	}
}
