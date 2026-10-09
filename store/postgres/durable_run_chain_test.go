//go:build integration

package postgres_test

import (
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

func runChainMigration(t *testing.T, s *postgres.Store) (*migrate.Migration, migrate.Executor) {
	t.Helper()
	exec, err := migrate.NewExecutorFor(pgdriver.Unwrap(s.DB()))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range postgres.Migrations.Migrations() {
		if m.Version == "20261023120000" {
			return m, exec
		}
	}
	t.Fatal("run chain migration missing")
	return nil, nil
}

func TestDurableRunChainOlderRoots(t *testing.T) {
	s := setupTestStore(t)
	pg := pgdriver.Unwrap(s.DB())
	now := durable.Timestamp(time.Now())
	for _, run := range []string{"older", "newer"} {
		_, err := pg.Exec(t.Context(), `INSERT INTO dispatch_executions(namespace,workflow_id,run_id,workflow_type,build_id,state,revision,last_sequence,input,output,created_at,updated_at,run_deadline_at)
 VALUES($1,'order',$2,'order','v1','completed',1,1,'input','',$3,$3,$4)`, t.Name(), run, now, now.Add(61*time.Microsecond))
		if err != nil {
			t.Fatal(err)
		}
		e, err := s.GetExecution(t.Context(), durable.Key{Namespace: t.Name(), WorkflowID: "order", RunID: run})
		if err != nil || e.FirstRunID != run || e.RunNumber != 1 || e.PreviousRunID != "" || e.NextRunID != "" || e.RunTimeout != 61*time.Microsecond || !e.FirstStartedAt.Equal(now) {
			t.Fatalf("older root: %+v %v", e, err)
		}
		if err = durable.ValidateRunMetadata(e); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDurableRunChainMetadataImmutable(t *testing.T) {
	s := setupTestStore(t)
	r := signalStartRequest(t)
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	before, err := s.GetExecution(t.Context(), r.Key)
	if err != nil {
		t.Fatal(err)
	}
	pg := pgdriver.Unwrap(s.DB())
	for _, update := range []string{"first_run_id='different'", "previous_run_id='previous'", "next_run_id='next'", "run_number=2", "first_started_at=first_started_at-interval '1 second'", "run_timeout=1000", "created_at=created_at+interval '1 microsecond'"} {
		if _, err = pg.Exec(t.Context(), `UPDATE dispatch_executions SET `+update+` WHERE namespace=$1`, r.Namespace); err == nil || !strings.Contains(err.Error(), "DX004") {
			t.Fatalf("mutable lineage %s: %v", update, err)
		}
	}
	after, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("lineage changed: %+v %v", after, err)
	}
	if _, err = s.SignalExecution(t.Context(), durable.SignalRequest{Key: r.Key, RequestID: "signal", BuildID: r.BuildID, Name: "go"}); err != nil {
		t.Fatalf("ordinary progress rejected: %v", err)
	}
}

func TestDurableRunChainMigrationRecovery(t *testing.T) {
	s, dsn := setupTestStoreConnection(t)
	migration, exec := runChainMigration(t, s)
	r := signalStartRequest(t)
	r.RunTimeout = time.Microsecond + 234*time.Nanosecond
	r.ExecutionTimeout = time.Hour
	receipt, err := s.StartExecution(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	before, err := s.GetExecution(t.Context(), r.Key)
	if err != nil {
		t.Fatal(err)
	}
	waitDurableStoreTime(t, s, before.RunDeadlineAt)
	for range 2 {
		if err = migration.Down(t.Context(), exec); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if err = migration.Up(t.Context(), exec); err != nil {
			t.Fatal(err)
		}
	}
	// Retrying earlier migrations must preserve the independent lineage guard.
	old, oldExec := deadlineMigration(t, s)
	if err = old.Up(t.Context(), oldExec); err != nil {
		t.Fatal(err)
	}
	s = reopenAsyncStore(t, s, dsn)
	after, err := s.GetExecution(t.Context(), r.Key)
	before.RunTimeout = time.Microsecond
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("lineage backfill/recovery: %+v != %+v: %v", after, before, err)
	}
	if got, retryErr := s.StartExecution(t.Context(), r); retryErr != nil || got != receipt {
		t.Fatalf("old receipt changed: %+v %v", got, retryErr)
	}
	if _, err = s.SignalExecution(t.Context(), durable.SignalRequest{Key: r.Key, RequestID: "late", BuildID: r.BuildID, Name: "go"}); !errors.Is(err, durable.ErrExecutionDeadline) {
		t.Fatalf("migration removed expiry fence: %v", err)
	}
	if _, err = pgdriver.Unwrap(s.DB()).Exec(t.Context(), `UPDATE dispatch_executions SET first_run_id='different' WHERE namespace=$1`, r.Namespace); err == nil || !strings.Contains(err.Error(), "DX004") {
		t.Fatalf("migration removed lineage guard: %v", err)
	}
}

func TestDurableRunChainDowngradeGuard(t *testing.T) {
	s := setupTestStore(t)
	migration, exec := runChainMigration(t, s)
	now := durable.Timestamp(time.Now())
	_, err := pgdriver.Unwrap(s.DB()).Exec(t.Context(), `INSERT INTO dispatch_executions(namespace,workflow_id,run_id,workflow_type,build_id,state,revision,last_sequence,input,output,created_at,updated_at,next_run_id)
 VALUES($1,'order','first','order','v1','continued_as_new',1,1,'','',$2,$2,'next')`, t.Name(), now)
	if err != nil {
		t.Fatal(err)
	}
	if err = migration.Down(t.Context(), exec); err == nil || !strings.Contains(err.Error(), "retained run chains") {
		t.Fatalf("destructive lineage downgrade: %v", err)
	}
	if _, err = s.GetExecution(t.Context(), durable.Key{Namespace: t.Name(), WorkflowID: "order", RunID: "first"}); err != nil {
		t.Fatalf("failed downgrade damaged schema: %v", err)
	}
}

func TestDurableRunChainBackfillRollback(t *testing.T) {
	s := setupTestStore(t)
	migration, exec := runChainMigration(t, s)
	r := signalStartRequest(t)
	r.RunTimeout = time.Microsecond
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	if err := migration.Down(t.Context(), exec); err != nil {
		t.Fatal(err)
	}
	pg := pgdriver.Unwrap(s.DB())
	_, err := pg.Exec(t.Context(), `CREATE FUNCTION reject_run_chain_backfill() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected lineage backfill failure'; END $$;
 CREATE TRIGGER reject_run_chain_backfill BEFORE UPDATE ON dispatch_executions FOR EACH ROW EXECUTE FUNCTION reject_run_chain_backfill()`)
	if err != nil {
		t.Fatal(err)
	}
	if err = migration.Up(t.Context(), exec); err == nil || !strings.Contains(err.Error(), "injected lineage backfill failure") {
		t.Fatalf("backfill fault: %v", err)
	}
	var columnExists bool
	err = pg.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='dispatch_executions'::regclass AND attname='first_run_id' AND NOT attisdropped)`).Scan(&columnExists)
	if err != nil || columnExists {
		t.Fatalf("partial lineage schema: %t %v", columnExists, err)
	}
	if _, err = pg.Exec(t.Context(), `UPDATE dispatch_executions SET revision=revision+1 WHERE namespace=$1`, r.Namespace); err == nil || !strings.Contains(err.Error(), "DX001") {
		t.Fatalf("failed backfill removed deadline guard: %v", err)
	}
	if _, err = pg.Exec(t.Context(), `DROP TRIGGER reject_run_chain_backfill ON dispatch_executions; DROP FUNCTION reject_run_chain_backfill()`); err != nil {
		t.Fatal(err)
	}
	if err = migration.Up(t.Context(), exec); err != nil {
		t.Fatal(err)
	}
	e, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || e.FirstRunID != r.RunID || e.RunNumber != 1 {
		t.Fatalf("backfill recovery: %+v %v", e, err)
	}
}
