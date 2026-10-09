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

func deadlineMigration(t *testing.T, s *postgres.Store) (*migrate.Migration, migrate.Executor) {
	t.Helper()
	exec, err := migrate.NewExecutorFor(pgdriver.Unwrap(s.DB()))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range postgres.Migrations.Migrations() {
		if m.Version == "20261021120000" {
			return m, exec
		}
	}
	t.Fatal("deadline migration missing")
	return nil, nil
}

func TestDurableExecutionDeadlineOldWriter(t *testing.T) {
	s := setupTestStore(t)
	r := signalStartRequest(t)
	r.RunTimeout = 100 * time.Millisecond
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	e, err := s.GetExecution(t.Context(), r.Key)
	if err != nil {
		t.Fatal(err)
	}
	waitDurableStoreTime(t, s, e.RunDeadlineAt)
	pg := pgdriver.Unwrap(s.DB())
	for _, query := range []string{
		`UPDATE dispatch_executions SET revision=revision+1,last_sequence=last_sequence+1 WHERE namespace=$1`,
		`UPDATE dispatch_executions SET state='completed' WHERE namespace=$1`,
		`UPDATE dispatch_execution_tasks SET owner='older-worker',epoch=epoch+1 WHERE namespace=$1`,
		`UPDATE dispatch_execution_tasks SET lease_until=clock_timestamp()+interval '1 hour' WHERE namespace=$1`,
		`UPDATE dispatch_execution_tasks SET heartbeat_sequence=heartbeat_sequence+1,progress='late' WHERE namespace=$1`,
	} {
		if _, err = pg.Exec(t.Context(), query, r.Namespace); err == nil || !strings.Contains(err.Error(), "DX001") {
			t.Fatalf("old writer escaped deadline: %s: %v", query, err)
		}
	}
	if _, err = pg.Exec(t.Context(), `UPDATE dispatch_executions SET run_deadline_at=NULL WHERE namespace=$1`, r.Namespace); err == nil || !strings.Contains(err.Error(), "DX002") {
		t.Fatalf("deadline extension: %v", err)
	}
	after, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || !reflect.DeepEqual(e, after) {
		t.Fatalf("partial old write: %+v %v", after, err)
	}
}

func TestDurableExecutionDeadlineMigrationRecovery(t *testing.T) {
	s, dsn := setupTestStoreConnection(t)
	migration, exec := deadlineMigration(t, s)
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
	r.RunTimeout = time.Microsecond
	r.ExecutionTimeout = time.Hour
	receipt, err := s.StartExecution(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	before, err := s.GetExecution(t.Context(), r.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err = migration.Up(t.Context(), exec); err != nil {
		t.Fatal(err)
	}
	if err = migration.Down(t.Context(), exec); err == nil || !strings.Contains(err.Error(), "retained workflow deadlines") {
		t.Fatalf("destructive downgrade: %v", err)
	}
	s = reopenAsyncStore(t, s, dsn)
	after, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("deadline lost: %+v %v", after, err)
	}
	if again, e := s.StartExecution(t.Context(), r); e != nil || again != receipt {
		t.Fatalf("recovered start: %+v %v", again, e)
	}
	selected, err := s.ResolveExecution(t.Context(), latestTarget(r.Key))
	if err != nil || !reflect.DeepEqual(before, selected) {
		t.Fatalf("selected deadline lost: %+v %v", selected, err)
	}
	if _, err = s.SignalExecution(t.Context(), durable.SignalRequest{Key: r.Key, BuildID: r.BuildID, RequestID: "late", Name: "go"}); !errors.Is(err, durable.ErrExecutionDeadline) {
		t.Fatalf("reopened expiry: %v", err)
	}
}

func TestDurableExecutionDeadlineBoundaryRollback(t *testing.T) {
	s := setupTestStore(t)
	r := signalStartRequest(t)
	r.RunTimeout = 250 * time.Millisecond
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	request := childCloseRequest(t, s, r)
	before, err := s.GetExecution(t.Context(), r.Key)
	if err != nil {
		t.Fatal(err)
	}
	pg := pgdriver.Unwrap(s.DB())
	// Force expiry after Go's acceptance check and before the projection write.
	_, err = pg.Exec(t.Context(), `CREATE FUNCTION delay_deadline_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 PERFORM pg_sleep(0.4); RETURN NEW; END $$;
 CREATE TRIGGER delay_deadline_event BEFORE INSERT ON dispatch_execution_events FOR EACH ROW EXECUTE FUNCTION delay_deadline_event()`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CommitTransition(t.Context(), request); !errors.Is(err, durable.ErrExecutionDeadline) {
		t.Fatalf("late commit: %v", err)
	}
	after, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("projection escaped rollback: %+v %v", after, err)
	}
	history, err := s.ReadHistory(t.Context(), r.Key, 0, 100)
	if err != nil || len(history) != 1 {
		t.Fatalf("history escaped rollback: %+v %v", history, err)
	}
	var count int
	if err = pg.QueryRow(t.Context(), `SELECT count(*) FROM dispatch_execution_receipts WHERE namespace=$1 AND request_id=$2`, r.Namespace, request.RequestID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("receipt escaped rollback: %d %v", count, err)
	}
}

func TestDurableExecutionDeadlineLockWait(t *testing.T) {
	for _, op := range []string{"renew", "commit", "signal", "cancel"} {
		t.Run(op, func(t *testing.T) {
			s := setupTestStore(t)
			r := signalStartRequest(t)
			r.RunTimeout = 300 * time.Millisecond
			if _, err := s.StartExecution(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			commit := childCloseRequest(t, s, r)
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
			if _, err = lock.Exec(t.Context(), `SELECT 1 FROM dispatch_executions WHERE namespace=$1 FOR UPDATE`, r.Namespace); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var e error
				switch op {
				case "renew":
					_, e = s.RenewTask(ctx, r.Key, commit.Token, time.Minute)
				case "commit":
					_, e = s.CommitTransition(ctx, commit)
				case "signal":
					_, e = s.SignalExecution(ctx, durable.SignalRequest{Key: r.Key, RequestID: "signal", Name: "go", BuildID: r.BuildID})
				case "cancel":
					_, e = s.RequestCancelExecution(ctx, durable.CancelExecutionRequest{Key: r.Key, RequestID: "cancel", BuildID: r.BuildID})
				}
				done <- e
			}()
			for {
				var waiting bool
				if err = pg.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%dispatch_executions%' AND query LIKE '%FOR UPDATE%')`).Scan(&waiting); err != nil {
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
				t.Fatalf("expired while waiting: %v", err)
			}
			after, err := s.GetExecution(t.Context(), r.Key)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("late write: %+v %v", after, err)
			}
		})
	}
}
