//go:build integration

package postgres_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/grove/migrate"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/store/postgres"
)

func latestTarget(key durable.Key) durable.ExecutionTarget {
	key.RunID = ""
	return durable.ExecutionTarget{Key: key, Selection: durable.RunLatest}
}
func checkLatest(t *testing.T, s *postgres.Store, key, want durable.Key) {
	t.Helper()
	got, err := s.ResolveExecution(t.Context(), latestTarget(key))
	if err != nil || got.Key != want {
		t.Fatalf("latest: %+v %v", got, err)
	}
}

func TestDurableExecutionTargetRecovery(t *testing.T) {
	s, dsn := setupTestStoreConnection(t)
	first := signalStartRequest(t)
	receipt, err := s.StartExecution(t.Context(), first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CommitTransition(t.Context(), childCloseRequest(t, s, first)); err != nil {
		t.Fatal(err)
	}
	next := first
	next.RunID = "replacement"
	next.RequestID = "replacement"
	if _, err = s.StartExecution(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	s = reopenAsyncStore(t, s, dsn)
	if got, retryErr := s.StartExecution(t.Context(), first); retryErr != nil || got != receipt {
		t.Fatalf("receipt: %+v %v", got, retryErr)
	}
	checkLatest(t, s, first.Key, next.Key)
}

func TestDurableExecutionTargetRollback(t *testing.T) {
	for _, kind := range []string{"start", "signal", "child", "start_event", "signal_event", "child_event"} {
		t.Run(kind, func(t *testing.T) {
			s := setupTestStore(t)
			start := signalStartRequest(t)
			var parent durable.StartRequest
			var commit durable.CommitRequest
			if strings.HasPrefix(kind, "child") {
				parent, _, commit = childCreationRequest(t, s, time.Minute)
				start = commit.Children[0].Start
			}
			prior := start
			prior.RunID = "prior"
			prior.RequestID = "prior"
			if _, err := s.StartExecution(t.Context(), prior); err != nil {
				t.Fatal(err)
			}
			if _, err := s.CommitTransition(t.Context(), childCloseRequest(t, s, prior)); err != nil {
				t.Fatal(err)
			}
			table := "dispatch_execution_heads"
			if strings.HasSuffix(kind, "_event") {
				table = "dispatch_execution_events"
			}
			pg := pgdriver.Unwrap(s.DB())
			_, err := pg.Exec(t.Context(), `CREATE FUNCTION reject_head() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected head failure'; END $$;
 CREATE TRIGGER reject_head BEFORE INSERT OR UPDATE ON `+table+` FOR EACH ROW EXECUTE FUNCTION reject_head()`)
			if err != nil {
				t.Fatal(err)
			}
			create := func() error {
				switch strings.TrimSuffix(kind, "_event") {
				case "signal":
					_, e := s.SignalWithStart(t.Context(), durable.SignalWithStartRequest{Start: start, Name: "approval"})
					return e
				case "child":
					_, e := s.CommitTransition(t.Context(), commit)
					return e
				default:
					_, e := s.StartExecution(t.Context(), start)
					return e
				}
			}
			if err = create(); err == nil || !strings.Contains(err.Error(), "injected head failure") {
				t.Fatalf("fault: %v", err)
			}
			if _, err = s.GetExecution(t.Context(), start.Key); !errors.Is(err, durable.ErrNotFound) {
				t.Fatalf("partial execution: %v", err)
			}
			checkLatest(t, s, start.Key, prior.Key)
			if strings.HasPrefix(kind, "child") {
				e, eErr := s.GetExecution(t.Context(), parent.Key)
				if eErr != nil || e.Revision != 1 || e.LastSequence != 1 {
					t.Fatalf("partial parent: %+v %v", e, eErr)
				}
				checkLatest(t, s, parent.Key, parent.Key)
			}
			for _, table := range []string{"dispatch_execution_events", "dispatch_execution_tasks", "dispatch_execution_receipts"} {
				var count int
				if err = pg.QueryRow(t.Context(), "SELECT count(*) FROM "+table+" WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3", start.Namespace, start.WorkflowID, start.RunID).Scan(&count); err != nil || count != 0 {
					t.Fatalf("partial %s: %d %v", table, count, err)
				}
			}
			if _, err = pg.Exec(t.Context(), "DROP TRIGGER reject_head ON "+table); err != nil {
				t.Fatal(err)
			}
			if err = create(); err != nil {
				t.Fatal(err)
			}
			checkLatest(t, s, start.Key, start.Key)
		})
	}
}

func headMigration(t *testing.T, s *postgres.Store) (*migrate.Migration, migrate.Executor) {
	t.Helper()
	executor, err := migrate.NewExecutorFor(pgdriver.Unwrap(s.DB()))
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range postgres.Migrations.Migrations() {
		if candidate.Version == "20261020120000" {
			return candidate, executor
		}
	}
	t.Fatal("head migration missing")
	return nil, nil
}

func TestDurableExecutionTargetMigration(t *testing.T) {
	s := setupTestStore(t)
	migration, executor := headMigration(t, s)
	pg := pgdriver.Unwrap(s.DB())
	for range 2 {
		if err := migration.Down(t.Context(), executor); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if err := migration.Up(t.Context(), executor); err != nil {
			t.Fatal(err)
		}
	}
	first := signalStartRequest(t)
	if _, err := s.StartExecution(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitTransition(t.Context(), childCloseRequest(t, s, first)); err != nil {
		t.Fatal(err)
	}
	second := first
	second.RunID = "second"
	second.RequestID = "second"
	if _, err := s.StartExecution(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	// Emulate an older database with no recorded head for this workflow.
	if _, err := pg.Exec(t.Context(), `DELETE FROM dispatch_execution_heads WHERE namespace=$1`, first.Namespace); err != nil {
		t.Fatal(err)
	}
	if err := migration.Up(t.Context(), executor); err != nil {
		t.Fatal(err)
	}
	checkLatest(t, s, first.Key, second.Key) // unique running run is provably latest
	if _, err := s.CommitTransition(t.Context(), childCloseRequest(t, s, second)); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.Exec(t.Context(), `DELETE FROM dispatch_execution_heads WHERE namespace=$1`, first.Namespace); err != nil {
		t.Fatal(err)
	}
	if err := migration.Up(t.Context(), executor); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResolveExecution(t.Context(), latestTarget(first.Key)); !errors.Is(err, durable.ErrAmbiguousRun) {
		t.Fatalf("guessed historical order: %v", err)
	}
	explicit, err := s.ResolveExecution(t.Context(), durable.ExecutionTarget{Key: first.Key})
	if err != nil || explicit.Key != first.Key {
		t.Fatalf("explicit: %+v %v", explicit, err)
	}
	third := first
	third.RunID = "third"
	third.RequestID = "third"
	if _, err = s.StartExecution(t.Context(), third); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CommitTransition(t.Context(), childCloseRequest(t, s, third)); err != nil {
		t.Fatal(err)
	}
	if err = migration.Up(t.Context(), executor); err != nil {
		t.Fatal(err)
	}
	checkLatest(t, s, first.Key, third.Key) // retry never replaces known pointer with NULL
	if err = migration.Down(t.Context(), executor); err == nil || !strings.Contains(err.Error(), "retained execution heads") {
		t.Fatalf("unsafe downgrade: %v", err)
	}
	checkLatest(t, s, first.Key, third.Key)
	// A lone historical closed run has an unambiguous head.
	lone := first
	lone.WorkflowID = "lone"
	if _, err = s.StartExecution(t.Context(), lone); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CommitTransition(t.Context(), childCloseRequest(t, s, lone)); err != nil {
		t.Fatal(err)
	}
	if _, err = pg.Exec(t.Context(), `DELETE FROM dispatch_execution_heads WHERE namespace=$1 AND workflow_id=$2`, lone.Namespace, lone.WorkflowID); err != nil {
		t.Fatal(err)
	}
	if err = migration.Up(t.Context(), executor); err != nil {
		t.Fatal(err)
	}
	checkLatest(t, s, lone.Key, lone.Key)
}

func TestDurableExecutionTargetOlderWriter(t *testing.T) {
	s := setupTestStore(t)
	first := signalStartRequest(t)
	if _, err := s.StartExecution(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitTransition(t.Context(), childCloseRequest(t, s, first)); err != nil {
		t.Fatal(err)
	}
	next := first.Key
	next.RunID = "older-writer"
	// An older process knows the execution schema but not the new head table.
	// Its clock is behind the first run. Latest must follow creation order.
	_, err := pgdriver.Unwrap(s.DB()).Exec(t.Context(), `INSERT INTO dispatch_executions(namespace,workflow_id,run_id,workflow_type,build_id,state,revision,last_sequence,input,output,created_at,updated_at)
 SELECT namespace,workflow_id,$4,workflow_type,build_id,'running',1,1,input,''::bytea,created_at-interval '1 day',created_at-interval '1 day'
 FROM dispatch_executions WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3`, first.Namespace, first.WorkflowID, first.RunID, next.RunID)
	if err != nil {
		t.Fatal(err)
	}
	checkLatest(t, s, first.Key, next)
}
