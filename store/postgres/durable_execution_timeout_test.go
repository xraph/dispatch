//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/grove/migrate"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/store/postgres"
)

func timeoutGrant(t *testing.T, s *postgres.Store, r durable.StartRequest, ttl time.Duration) (*durable.ExecutionTimeoutTask, durable.ExecutionTimeoutRequest) {
	t.Helper()
	grant, err := s.ClaimExecutionTimeout(t.Context(), durable.ExecutionTimeoutClaimRequest{Namespace: r.Namespace, Owner: "expiry", LeaseDuration: ttl})
	if err != nil || grant == nil {
		t.Fatalf("grant: %+v %v", grant, err)
	}
	return grant, durable.ExecutionTimeoutRequest{Key: grant.Key, RequestID: "timeout", Owner: grant.Owner, Epoch: grant.Epoch}
}
func timeoutMigration(t *testing.T, s *postgres.Store) (*migrate.Migration, migrate.Executor) {
	t.Helper()
	exec, err := migrate.NewExecutorFor(pgdriver.Unwrap(s.DB()))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range postgres.Migrations.Migrations() {
		if m.Version == "20261022120000" {
			return m, exec
		}
	}
	t.Fatal("timeout migration missing")
	return nil, nil
}
func TestDurableExecutionTimeoutRecovery(t *testing.T) {
	s, dsn := setupTestStoreConnection(t)
	migration, exec := timeoutMigration(t, s)
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
	r.ExecutionTimeout = time.Microsecond
	r.BuildID = "retired"
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	grant, request := timeoutGrant(t, s, r, time.Minute)
	// Retry both migrations after a grant exists. The old guard must not disable closure.
	prior, oldExec := deadlineMigration(t, s)
	if err := prior.Up(t.Context(), oldExec); err != nil {
		t.Fatal(err)
	}
	if err := migration.Up(t.Context(), exec); err != nil {
		t.Fatal(err)
	}
	if err := migration.Down(t.Context(), exec); err == nil || !strings.Contains(err.Error(), "retained timeout grants") {
		t.Fatalf("unsafe downgrade: %v", err)
	}
	s = reopenAsyncStore(t, s, dsn)
	receipt, err := s.ApplyExecutionTimeout(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	before, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || before.State != durable.StateTimedOut || !before.ExecutionDeadlineAt.Equal(grant.DeadlineAt) {
		t.Fatalf("closure: %+v %v", before, err)
	}
	s = reopenAsyncStore(t, s, dsn)
	// Treat the first response as lost. Recovery must read its receipt before any row wait.
	pg := pgdriver.Unwrap(s.DB())
	lock, err := pg.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback() }()
	if _, err = lock.Exec(t.Context(), `SELECT 1 FROM dispatch_executions WHERE namespace=$1 FOR UPDATE`, r.Namespace); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	if again, e := s.ApplyExecutionTimeout(ctx, request); e != nil || again != receipt {
		t.Fatalf("receipt recovery: %+v %v", again, e)
	}
	after, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("receipt mutated state: %+v %v", after, err)
	}
}
func TestDurableExecutionTimeoutRollback(t *testing.T) {
	for _, table := range []string{"dispatch_execution_events", "dispatch_executions", "dispatch_execution_tasks", "dispatch_child_deliveries", "dispatch_execution_receipts"} {
		t.Run(table, func(t *testing.T) {
			s := setupTestStore(t)
			_, _, creation := childCreationRequest(t, s, time.Minute)
			child := creation.Children[0].Start
			creation.Children[0].Start.RunTimeout = time.Microsecond
			if _, err := s.CommitTransition(t.Context(), creation); err != nil {
				t.Fatal(err)
			}
			_, request := timeoutGrant(t, s, child, time.Minute)
			before, err := s.GetExecution(t.Context(), child.Key)
			if err != nil {
				t.Fatal(err)
			}
			pg := pgdriver.Unwrap(s.DB())
			_, err = pg.Exec(t.Context(), `CREATE FUNCTION reject_execution_timeout() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected timeout failure'; END $$;
 CREATE TRIGGER reject_execution_timeout BEFORE INSERT OR UPDATE ON `+table+` FOR EACH ROW EXECUTE FUNCTION reject_execution_timeout()`)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.ApplyExecutionTimeout(t.Context(), request); err == nil || !strings.Contains(err.Error(), "injected timeout failure") {
				t.Fatalf("fault missing: %v", err)
			}
			after, err := s.GetExecution(t.Context(), child.Key)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("partial projection: %+v %v", after, err)
			}
			events, err := s.ReadHistory(t.Context(), child.Key, 0, 100)
			if err != nil || len(events) != 1 {
				t.Fatalf("partial history: %+v %v", events, err)
			}
			task, err := s.GetTask(t.Context(), child.Key, "workflow:1")
			if err != nil || task.Done {
				t.Fatalf("partial task: %+v %v", task, err)
			}
			messages, err := s.ListChildDeliveries(t.Context(), child.Key, "", 100)
			if err != nil || len(messages) != 0 {
				t.Fatalf("partial delivery: %+v %v", messages, err)
			}
			if _, err = pg.Exec(t.Context(), `DROP TRIGGER reject_execution_timeout ON `+table); err != nil {
				t.Fatal(err)
			}
			if _, err = s.ApplyExecutionTimeout(t.Context(), request); err != nil {
				t.Fatalf("retry: %v", err)
			}
		})
	}
}
func TestDurableExecutionTimeoutLockExpiry(t *testing.T) {
	s := setupTestStore(t)
	r := signalStartRequest(t)
	r.RunTimeout = time.Microsecond
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	grant, request := timeoutGrant(t, s, r, 250*time.Millisecond)
	pg := pgdriver.Unwrap(s.DB())
	lock, err := pg.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback() }()
	if _, err = lock.Exec(t.Context(), `SELECT 1 FROM dispatch_execution_tasks WHERE namespace=$1 FOR UPDATE`, r.Namespace); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := s.ApplyExecutionTimeout(ctx, request); done <- e }()
	for {
		var waiting bool
		if err = pg.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%dispatch_execution_tasks%' AND query LIKE '%FOR UPDATE%')`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case e := <-done:
			t.Fatalf("did not wait: %v", e)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
	waitDurableStoreTime(t, s, grant.LeaseUntil)
	if err = lock.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = <-done; !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("lease after pending-task lock: %v", err)
	}
	next, retry := timeoutGrant(t, s, r, time.Minute)
	if next.Epoch != grant.Epoch+1 {
		t.Fatal("grant did not advance")
	}
	if _, err = s.ApplyExecutionTimeout(t.Context(), retry); err != nil {
		t.Fatal(err)
	}
}

func TestDurableExecutionTimeoutOldWriterCannotUseGrant(t *testing.T) {
	s := setupTestStore(t)
	r := signalStartRequest(t)
	r.RunTimeout = time.Microsecond
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	_, request := timeoutGrant(t, s, r, time.Minute)
	pg := pgdriver.Unwrap(s.DB())
	// A legacy commit knows neither the timeout grant nor its explicit consumption.
	_, err := pg.Exec(t.Context(), `UPDATE dispatch_executions SET state='timed_out',revision=revision+1,last_sequence=last_sequence+1,output=''::bytea,updated_at=clock_timestamp() WHERE namespace=$1`, r.Namespace)
	if err == nil || !strings.Contains(err.Error(), "DX001") {
		t.Fatalf("old source consumed independent timeout grant: %v", err)
	}
	if _, err = s.ApplyExecutionTimeout(t.Context(), request); err != nil {
		t.Fatal(err)
	}
}

func TestDurableExecutionTimeoutLeaseBoundary(t *testing.T) {
	s := setupTestStore(t)
	r := signalStartRequest(t)
	r.RunTimeout = time.Microsecond
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	_, request := timeoutGrant(t, s, r, 250*time.Millisecond)
	pg := pgdriver.Unwrap(s.DB())
	_, err := pg.Exec(t.Context(), `CREATE FUNCTION delay_timeout_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.4); RETURN NEW; END $$;
 CREATE TRIGGER delay_timeout_event BEFORE INSERT ON dispatch_execution_events FOR EACH ROW EXECUTE FUNCTION delay_timeout_event()`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ApplyExecutionTimeout(t.Context(), request); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("lease crossed during history write: %v", err)
	}
	e, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || e.State != durable.StateRunning || e.Revision != 1 {
		t.Fatalf("partial closure: %+v %v", e, err)
	}
	history, err := s.ReadHistory(t.Context(), r.Key, 0, 100)
	if err != nil || len(history) != 1 {
		t.Fatalf("partial history: %+v %v", history, err)
	}
}
