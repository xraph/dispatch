//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/grove/migrate"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/store/postgres"
)

func intentCompletion(t *testing.T, s *postgres.Store) durable.CommitRequest {
	t.Helper()
	key, task := enableTestHeartbeat(t, s, time.Minute)
	if _, err := s.CommitTransition(t.Context(), asyncHandoffRequest(t, key, task)); err != nil {
		t.Fatal(err)
	}
	current, err := s.GetTask(t.Context(), key, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	r := asyncCompletionRequest(key, current)
	r.IntentDigest, err = durable.Fingerprint("callback.complete", struct {
		Key    durable.Key
		Token  durable.TaskToken
		Secret string
		Output []byte
	}{key, current.Token(), r.AsyncSecret, r.Output})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func intentLookup(r durable.CommitRequest) durable.ReceiptRequest {
	return durable.ReceiptRequest{Key: r.Key, RequestID: r.RequestID, IntentDigest: r.IntentDigest}
}

func replayLegacyHandoff(t *testing.T, s *postgres.Store, r durable.CommitRequest) {
	t.Helper()
	prior := durable.Task{Key: r.Key, TaskSpec: durable.TaskSpec{ID: r.Token.TaskID}, Owner: r.Token.Owner, Epoch: r.Token.Epoch}
	if got, err := s.CommitTransition(t.Context(), asyncHandoffRequest(t, r.Key, prior)); err != nil || got != (durable.Receipt{Revision: 4, FirstSequence: 4, LastSequence: 4}) {
		t.Fatalf("legacy receipt changed on migration: %+v %v", got, err)
	}
}

func TestDurableIntentReopenAndMigration(t *testing.T) {
	s, dsn := setupTestStoreConnection(t)
	r := intentCompletion(t, s)
	if _, err := (lostAsyncResponseStore{Store: s}).CommitTransition(t.Context(), r); !errors.Is(err, errAsyncAcknowledgementLost) {
		t.Fatalf("completion did not lose acknowledgement: %v", err)
	}
	pg := pgdriver.Unwrap(s.DB())
	if _, err := pg.Exec(t.Context(), `DELETE FROM grove_migrations WHERE version=$1`, "20261015120000"); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	var storedIntent, exactDigest string
	if err := pg.QueryRow(t.Context(), `SELECT intent_digest,digest FROM dispatch_execution_receipts WHERE namespace=$1 AND request_id=$2`, r.Namespace, r.RequestID).Scan(&storedIntent, &exactDigest); err != nil {
		t.Fatal(err)
	}
	if storedIntent != r.IntentDigest || exactDigest == storedIntent || strings.Contains(storedIntent, r.AsyncSecret) {
		t.Fatal("receipt did not retain distinct hashes")
	}
	if err := s.DB().Close(); err != nil {
		t.Fatal(err)
	}
	drv := pgdriver.New()
	if err := drv.Open(t.Context(), dsn); err != nil {
		t.Fatal(err)
	}
	db, err := grove.Open(drv)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	reopened := postgres.New(db)
	replayLegacyHandoff(t, reopened, r)
	receipt, found, err := reopened.LookupReceipt(t.Context(), intentLookup(r))
	if err != nil || !found || receipt != (durable.Receipt{Revision: 5, FirstSequence: 5, LastSequence: 5}) {
		t.Fatalf("lookup after reopen: %+v %v %v", receipt, found, err)
	}
	if replay, replayErr := reopened.CommitTransition(t.Context(), r); replayErr != nil || replay != receipt {
		t.Fatalf("exact replay after reopen: %+v %v", replay, replayErr)
	}
	changed := r
	changed.ExpectedRevision++
	if _, err = reopened.CommitTransition(t.Context(), changed); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("changed derived request accepted: %v", err)
	}
	query := intentLookup(r)
	query.RequestID = "await"
	if _, found, err = reopened.LookupReceipt(t.Context(), query); !errors.Is(err, durable.ErrRequestConflict) || found {
		t.Fatalf("migration granted legacy intent: %v %v", found, err)
	}
	var count int
	if err = pgdriver.Unwrap(db).QueryRow(t.Context(), `SELECT count(*) FROM dispatch_execution_events WHERE namespace=$1`, r.Namespace).Scan(&count); err != nil || count != 5 {
		t.Fatalf("duplicate completion history: %d %v", count, err)
	}
}

func TestDurableIntentRollbackOnReceiptFailure(t *testing.T) {
	s := setupTestStore(t)
	r := intentCompletion(t, s)
	pg := pgdriver.Unwrap(s.DB())
	if _, err := pg.Exec(t.Context(), `CREATE FUNCTION reject_intent_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.intent_digest <> '' THEN RAISE EXCEPTION 'injected intent receipt failure'; END IF; RETURN NEW; END $$;
 CREATE TRIGGER reject_intent_receipt BEFORE INSERT ON dispatch_execution_receipts FOR EACH ROW EXECUTE FUNCTION reject_intent_receipt()`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitTransition(t.Context(), r); err == nil || !strings.Contains(err.Error(), "injected intent receipt failure") {
		t.Fatalf("receipt failure not injected: %v", err)
	}
	if got, found, err := s.LookupReceipt(t.Context(), intentLookup(r)); err != nil || found || got != (durable.Receipt{}) {
		t.Fatalf("rolled-back receipt visible: %+v %v %v", got, found, err)
	}
	execution, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || execution.State != durable.StateRunning || execution.Revision != 4 || execution.LastSequence != 4 || len(execution.Output) != 0 {
		t.Fatalf("rolled-back state: %+v %v", execution, err)
	}
	task, err := s.GetTask(t.Context(), r.Key, r.Token.TaskID)
	if err != nil || task.Done || task.LeaseKind != durable.LeaseAsync {
		t.Fatalf("rolled-back task: %+v %v", task, err)
	}
	if _, err = pg.Exec(t.Context(), `DROP TRIGGER reject_intent_receipt ON dispatch_execution_receipts`); err != nil {
		t.Fatal(err)
	}
	receipt, err := s.CommitTransition(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	if got, found, lookupErr := s.LookupReceipt(t.Context(), intentLookup(r)); lookupErr != nil || !found || got != receipt {
		t.Fatalf("retry after rollback: %+v %v %v", got, found, lookupErr)
	}
}

func TestDurableIntentLookupDuringTransaction(t *testing.T) {
	for _, commit := range []bool{false, true} {
		name := "rollback"
		if commit {
			name = "commit"
		}
		t.Run(name, func(t *testing.T) {
			s := setupTestStore(t)
			r := intentCompletion(t, s)
			pg := pgdriver.Unwrap(s.DB())
			function := `CREATE FUNCTION gate_intent_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.intent_digest <> '' THEN PERFORM pg_advisory_xact_lock(816343); `
			if !commit {
				function += `RAISE EXCEPTION 'injected pending intent rollback'; `
			}
			function += `END IF; RETURN NEW; END $$;
 CREATE TRIGGER gate_intent_receipt BEFORE INSERT ON dispatch_execution_receipts FOR EACH ROW EXECUTE FUNCTION gate_intent_receipt()`
			if _, err := pg.Exec(t.Context(), function); err != nil {
				t.Fatal(err)
			}
			gate, err := pg.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer gate.Rollback()
			if _, err = gate.Exec(t.Context(), `SELECT pg_advisory_xact_lock(816343)`); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, writeErr := s.CommitTransition(ctx, r)
				result <- writeErr
			}()
			for {
				var blocked bool
				if err = pg.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event='advisory' AND query LIKE '%dispatch_execution_receipts%')`).Scan(&blocked); err != nil {
					t.Fatal(err)
				}
				if blocked {
					break
				}
				time.Sleep(time.Millisecond)
			}
			lookupCtx, lookupCancel := context.WithTimeout(ctx, time.Second)
			defer lookupCancel()
			if _, found, lookupErr := s.LookupReceipt(lookupCtx, intentLookup(r)); lookupErr != nil || found {
				t.Fatalf("uncommitted receipt visible or lookup blocked: %v %v", found, lookupErr)
			}
			before, err := s.GetExecution(ctx, r.Key)
			if err != nil || before.Revision != 4 || before.State != durable.StateRunning {
				t.Fatalf("uncommitted execution visible: %+v %v", before, err)
			}
			if err = gate.Rollback(); err != nil {
				t.Fatal(err)
			}
			select {
			case writeErr := <-result:
				if commit && writeErr != nil {
					t.Fatal(writeErr)
				}
				if !commit && (writeErr == nil || !strings.Contains(writeErr.Error(), "injected pending intent rollback")) {
					t.Fatalf("rollback not injected: %v", writeErr)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			got, found, err := s.LookupReceipt(t.Context(), intentLookup(r))
			if err != nil || found != commit {
				t.Fatalf("transaction visibility: %+v %v %v", got, found, err)
			}
			if commit && got != (durable.Receipt{Revision: 5, FirstSequence: 5, LastSequence: 5}) {
				t.Fatalf("wrong receipt: %+v", got)
			}
			if !commit && got != (durable.Receipt{}) {
				t.Fatalf("rolled-back receipt: %+v", got)
			}
		})
	}
}

func TestDurableIntentDowngrade(t *testing.T) {
	s := setupTestStore(t)
	r := intentCompletion(t, s)
	executor, err := migrate.NewExecutorFor(pgdriver.Unwrap(s.DB()))
	if err != nil {
		t.Fatal(err)
	}
	var migration *migrate.Migration
	for _, candidate := range postgres.Migrations.Migrations() {
		if candidate.Version == "20261015120000" {
			migration = candidate
		}
	}
	if migration == nil {
		t.Fatal("intent migration not registered")
	}
	// Old receipts alone must not block downgrade, and upgrade restores lookup.
	if err = migration.Down(t.Context(), executor); err != nil {
		t.Fatal(err)
	}
	if err = migration.Up(t.Context(), executor); err != nil {
		t.Fatal(err)
	}
	replayLegacyHandoff(t, s, r)
	receipt, err := s.CommitTransition(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	if err = migration.Down(t.Context(), executor); err == nil || !strings.Contains(err.Error(), "retained intent receipts prevent downgrade") {
		t.Fatalf("downgrade discarded proof: %v", err)
	}
	if got, found, lookupErr := s.LookupReceipt(t.Context(), intentLookup(r)); lookupErr != nil || !found || got != receipt {
		t.Fatalf("failed downgrade changed receipt: %+v %v %v", got, found, lookupErr)
	}
}
