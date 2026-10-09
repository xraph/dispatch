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

func signalStartRequest(t *testing.T) durable.StartRequest {
	t.Helper()
	return durable.StartRequest{Key: durable.Key{Namespace: t.Name(), WorkflowID: "order", RunID: "first"}, RequestID: "approval", WorkflowType: "order", BuildID: "v1", Queue: "orders", Input: []byte("start")}
}

type lostSignalResponseStore struct {
	durable.Store
	accepted durable.SignalReceipt
}

var errSignalResponseLost = errors.New("signal acknowledgement lost after commit")

func (s *lostSignalResponseStore) SignalExecution(ctx context.Context, r durable.SignalRequest) (durable.SignalReceipt, error) {
	receipt, err := s.Store.SignalExecution(ctx, r)
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	s.accepted = receipt
	return durable.SignalReceipt{}, errSignalResponseLost
}
func (s *lostSignalResponseStore) SignalWithStart(ctx context.Context, r durable.SignalWithStartRequest) (durable.SignalReceipt, error) {
	receipt, err := s.Store.SignalWithStart(ctx, r)
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	s.accepted = receipt
	return durable.SignalReceipt{}, errSignalResponseLost
}

func TestDurableSignalReopenReceipts(t *testing.T) {
	for _, mode := range []string{"signal", "with_start"} {
		t.Run(mode, func(t *testing.T) {
			s, dsn := setupTestStoreConnection(t)
			start := signalStartRequest(t)
			signal := durable.SignalRequest{Key: start.Key, RequestID: "approval", BuildID: start.BuildID, Name: "approve", Input: []byte("yes")}
			signal.RunID = ""
			withStart := durable.SignalWithStartRequest{Start: start, Name: "approve", Input: []byte("yes")}
			lost := &lostSignalResponseStore{Store: s}
			var err error
			if mode == "signal" {
				if _, err = s.StartExecution(t.Context(), start); err != nil {
					t.Fatal(err)
				}
				_, err = lost.SignalExecution(t.Context(), signal)
			} else {
				_, err = lost.SignalWithStart(t.Context(), withStart)
			}
			if !errors.Is(err, errSignalResponseLost) {
				t.Fatalf("test did not lose accepted signal: %v", err)
			}
			task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: start.Namespace, Queue: start.Queue, BuildID: start.BuildID, Kind: durable.TaskWorkflow, Owner: "worker", LeaseDuration: time.Minute})
			if err != nil || task == nil {
				t.Fatalf("claim: %+v %v", task, err)
			}
			if _, err = s.CommitTransition(t.Context(), durable.CommitRequest{Key: start.Key, RequestID: "close", ExpectedRevision: lost.accepted.Revision, Token: task.Token(), Events: []durable.EventInput{{Type: "workflow.completed"}}, State: durable.StateCompleted}); err != nil {
				t.Fatal(err)
			}
			next := start
			next.RunID, next.RequestID = "next", "next-start"
			if _, err = s.StartExecution(t.Context(), next); err != nil {
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
			var receipt durable.SignalReceipt
			if mode == "signal" {
				receipt, err = reopened.SignalExecution(t.Context(), signal)
			} else {
				receipt, err = reopened.SignalWithStart(t.Context(), withStart)
			}
			if err != nil || receipt != lost.accepted {
				t.Fatalf("reopened receipt followed replacement: %+v %v", receipt, err)
			}
			events, err := reopened.ReadHistory(t.Context(), next.Key, 0, 100)
			if err != nil || len(events) != 1 {
				t.Fatalf("retry changed replacement: %d %v", len(events), err)
			}
		})
	}
}

func TestDurableSignalReceiptFailureRollsBack(t *testing.T) {
	for _, mode := range []string{"signal", "new_start", "existing_start"} {
		t.Run(mode, func(t *testing.T) {
			s := setupTestStore(t)
			start := signalStartRequest(t)
			if mode != "new_start" {
				if _, err := s.StartExecution(t.Context(), start); err != nil {
					t.Fatal(err)
				}
			}
			pg := pgdriver.Unwrap(s.DB())
			_, err := pg.Exec(t.Context(), `CREATE FUNCTION reject_signal_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected signal receipt failure'; END $$;
CREATE TRIGGER reject_signal_receipt BEFORE INSERT ON dispatch_signal_receipts FOR EACH ROW EXECUTE FUNCTION reject_signal_receipt()`)
			if err != nil {
				t.Fatal(err)
			}
			accept := func() (durable.SignalReceipt, error) {
				if mode == "signal" {
					return s.SignalExecution(t.Context(), durable.SignalRequest{Key: start.Key, RequestID: "message", BuildID: start.BuildID, Name: "approve"})
				}
				proposed := start
				if mode == "existing_start" {
					proposed.RunID = "unused"
				}
				return s.SignalWithStart(t.Context(), durable.SignalWithStartRequest{Start: proposed, Name: "approve"})
			}
			if _, err = accept(); err == nil || !strings.Contains(err.Error(), "injected signal receipt failure") {
				t.Fatalf("test did not fail receipt insertion: %v", err)
			}
			var executions, events, tasks, receipts int
			err = pg.QueryRow(t.Context(), `SELECT
(SELECT count(*) FROM dispatch_executions),(SELECT count(*) FROM dispatch_execution_events),
(SELECT count(*) FROM dispatch_execution_tasks),(SELECT count(*) FROM dispatch_signal_receipts)`).Scan(&executions, &events, &tasks, &receipts)
			want := 1
			if mode == "new_start" {
				want = 0
			}
			if err != nil || executions != want || events != want || tasks != want || receipts != 0 {
				t.Fatalf("partial signal acceptance: executions=%d events=%d tasks=%d receipts=%d %v", executions, events, tasks, receipts, err)
			}
			if mode != "new_start" {
				execution, readErr := s.GetExecution(t.Context(), start.Key)
				if readErr != nil || execution.Revision != 1 || execution.LastSequence != 1 {
					t.Fatalf("failed signal changed projection: %+v %v", execution, readErr)
				}
			}
			if _, err = pg.Exec(t.Context(), `DROP TRIGGER reject_signal_receipt ON dispatch_signal_receipts`); err != nil {
				t.Fatal(err)
			}
			if _, err = accept(); err != nil {
				t.Fatalf("retry after rollback: %v", err)
			}
		})
	}
}

func TestDurableSignalMigrationRetryAndDowngrade(t *testing.T) {
	s := setupTestStore(t)
	pg := pgdriver.Unwrap(s.DB())
	executor, err := migrate.NewExecutorFor(pg)
	if err != nil {
		t.Fatal(err)
	}
	var migration *migrate.Migration
	for _, candidate := range postgres.Migrations.Migrations() {
		if candidate.Version == "20261016120000" {
			migration = candidate
		}
	}
	if migration == nil {
		t.Fatal("signal migration missing")
	}
	if err = migration.Down(t.Context(), executor); err != nil {
		t.Fatal(err)
	}
	if err = migration.Down(t.Context(), executor); err != nil {
		t.Fatal(err)
	}
	if err = migration.Up(t.Context(), executor); err != nil {
		t.Fatal(err)
	}
	request := durable.SignalWithStartRequest{Start: signalStartRequest(t), Name: "approve", Input: []byte("yes")}
	receipt, err := s.SignalWithStart(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pg.Exec(t.Context(), `DELETE FROM grove_migrations WHERE version=$1`, "20261016120000"); err != nil {
		t.Fatal(err)
	}
	if err = s.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = migration.Down(t.Context(), executor); err == nil || !strings.Contains(err.Error(), "retained signal receipts prevent downgrade") {
		t.Fatalf("downgrade discarded routing receipt: %v", err)
	}
	if got, replayErr := s.SignalWithStart(t.Context(), request); replayErr != nil || got != receipt {
		t.Fatalf("migration lost receipt: %+v %v", got, replayErr)
	}
}
