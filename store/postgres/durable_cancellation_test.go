//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/grove/migrate"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/store/postgres"
)

type lostCancellationAcceptanceStore struct {
	durable.Store
	accepted durable.CancelExecutionReceipt
}

var errCancellationAcceptanceLost = errors.New("cancellation acceptance response lost")

func (s *lostCancellationAcceptanceStore) RequestCancelExecution(ctx context.Context, r durable.CancelExecutionRequest) (durable.CancelExecutionReceipt, error) {
	receipt, err := s.Store.RequestCancelExecution(ctx, r)
	if err != nil {
		return receipt, err
	}
	s.accepted = receipt
	return durable.CancelExecutionReceipt{}, errCancellationAcceptanceLost
}

func TestDurableCancellationReceiptRecovery(t *testing.T) {
	s, dsn := setupTestStoreConnection(t)
	start := signalStartRequest(t)
	if _, err := s.StartExecution(t.Context(), start); err != nil {
		t.Fatal(err)
	}
	request := durable.CancelExecutionRequest{Key: start.Key, RequestID: "cancel", BuildID: start.BuildID, Reason: "requested"}
	request.RunID = ""
	lost := &lostCancellationAcceptanceStore{Store: s}
	if _, err := lost.RequestCancelExecution(t.Context(), request); !errors.Is(err, errCancellationAcceptanceLost) {
		t.Fatalf("loss: %v", err)
	}
	task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: start.Namespace, Queue: start.Queue, Kind: durable.TaskWorkflow, Owner: "worker", LeaseDuration: time.Minute})
	if err != nil || task == nil {
		t.Fatalf("claim: %+v %v", task, err)
	}
	if _, err = s.CommitTransition(t.Context(), durable.CommitRequest{Key: start.Key, RequestID: "close", Token: task.Token(), ExpectedRevision: 2, State: durable.StateCancelled, Events: []durable.EventInput{{Type: "workflow.cancelled"}}}); err != nil {
		t.Fatal(err)
	}
	next := start
	next.RunID = "next"
	next.RequestID = "next-start"
	if _, err = s.StartExecution(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	s = reopenAsyncStore(t, s, dsn)
	if again, retryErr := s.RequestCancelExecution(t.Context(), request); retryErr != nil || again != lost.accepted {
		t.Fatalf("reopened receipt: %+v %v", again, retryErr)
	}
	events, err := s.ReadHistory(t.Context(), next.Key, 0, 100)
	if err != nil || len(events) != 1 {
		t.Fatalf("replacement changed: %+v %v", events, err)
	}
}

func TestDurableCancellationRollback(t *testing.T) {
	s := setupTestStore(t)
	start := signalStartRequest(t)
	if _, err := s.StartExecution(t.Context(), start); err != nil {
		t.Fatal(err)
	}
	pg := pgdriver.Unwrap(s.DB())
	_, err := pg.Exec(t.Context(), `CREATE FUNCTION reject_cancel_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected cancellation receipt failure'; END $$;
 CREATE TRIGGER reject_cancel_receipt BEFORE INSERT ON dispatch_cancellation_receipts FOR EACH ROW EXECUTE FUNCTION reject_cancel_receipt()`)
	if err != nil {
		t.Fatal(err)
	}
	request := durable.CancelExecutionRequest{Key: start.Key, RequestID: "cancel", BuildID: start.BuildID}
	if _, err = s.RequestCancelExecution(t.Context(), request); err == nil || !strings.Contains(err.Error(), "injected cancellation receipt failure") {
		t.Fatalf("missing fault: %v", err)
	}
	execution, err := s.GetExecution(t.Context(), start.Key)
	if err != nil || execution.Revision != 1 || execution.LastSequence != 1 {
		t.Fatalf("partial acceptance: %+v %v", execution, err)
	}
	if _, err = s.GetTask(t.Context(), start.Key, "workflow:cancel-request:2"); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("partial wakeup: %v", err)
	}
	if _, err = pg.Exec(t.Context(), `DROP TRIGGER reject_cancel_receipt ON dispatch_cancellation_receipts`); err != nil {
		t.Fatal(err)
	}
	if _, err = s.RequestCancelExecution(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	source, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: start.Namespace, Queue: start.Queue, Kind: durable.TaskWorkflow, Owner: "worker", LeaseDuration: time.Minute})
	if err != nil || source == nil {
		t.Fatalf("claim: %+v %v", source, err)
	}
	seed := durable.CommitRequest{Key: start.Key, RequestID: "seed", Token: source.Token(), ExpectedRevision: 2, Events: []durable.EventInput{{Type: "seed"}}, TaskUpdate: &durable.TaskUpdate{Action: durable.TaskKeep}, Tasks: []durable.TaskSpec{{ID: "activity", Kind: durable.TaskActivity, Queue: start.Queue}}}
	if _, err = s.CommitTransition(t.Context(), seed); err != nil {
		t.Fatal(err)
	}
	target, err := s.GetTask(t.Context(), start.Key, "activity")
	if err != nil {
		t.Fatal(err)
	}
	_, err = pg.Exec(t.Context(), `CREATE FUNCTION reject_fence_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected fencing receipt failure'; END $$;
 CREATE TRIGGER reject_fence_receipt BEFORE INSERT ON dispatch_execution_receipts FOR EACH ROW EXECUTE FUNCTION reject_fence_receipt()`)
	if err != nil {
		t.Fatal(err)
	}
	fence := durable.CommitRequest{Key: start.Key, RequestID: "fence", Token: source.Token(), ExpectedRevision: 3, Events: []durable.EventInput{{Type: "fence"}}, CancelPendingTasks: true, Tasks: []durable.TaskSpec{{ID: "cleanup", Kind: durable.TaskWorkflow, Queue: start.Queue}}}
	if _, err = s.CommitTransition(t.Context(), fence); err == nil || !strings.Contains(err.Error(), "injected fencing receipt failure") {
		t.Fatalf("missing fence fault: %v", err)
	}
	after, err := s.GetTask(t.Context(), start.Key, "activity")
	if err != nil || after.Done || after.Version != target.Version {
		t.Fatalf("partial target fence: %+v %v", after, err)
	}
	sourceAfter, err := s.GetTask(t.Context(), start.Key, source.ID)
	if err != nil || sourceAfter.Done || sourceAfter.Version != source.Version+1 {
		t.Fatalf("partial source fence: %+v %v", sourceAfter, err)
	}
	execution, err = s.GetExecution(t.Context(), start.Key)
	if err != nil || execution.Revision != 3 || execution.LastSequence != 3 {
		t.Fatalf("partial fence history: %+v %v", execution, err)
	}
	if _, err = s.GetTask(t.Context(), start.Key, "cleanup"); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("partial cleanup: %v", err)
	}
	if _, err = pg.Exec(t.Context(), `DROP TRIGGER reject_fence_receipt ON dispatch_execution_receipts`); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CommitTransition(t.Context(), fence); err != nil {
		t.Fatalf("fence retry: %v", err)
	}
}

func TestDurableCancellationMigration(t *testing.T) {
	s := setupTestStore(t)
	pg := pgdriver.Unwrap(s.DB())
	executor, err := migrate.NewExecutorFor(pg)
	if err != nil {
		t.Fatal(err)
	}
	var migration *migrate.Migration
	for _, candidate := range postgres.Migrations.Migrations() {
		if candidate.Version == "20261017120000" {
			migration = candidate
		}
	}
	if migration == nil {
		t.Fatal("migration missing")
	}
	for range 2 {
		if err = migration.Down(t.Context(), executor); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if err = migration.Up(t.Context(), executor); err != nil {
			t.Fatal(err)
		}
	}
	start := signalStartRequest(t)
	if _, err = s.StartExecution(t.Context(), start); err != nil {
		t.Fatal(err)
	}
	request := durable.CancelExecutionRequest{Key: start.Key, RequestID: "cancel", BuildID: start.BuildID}
	receipt, err := s.RequestCancelExecution(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pg.Exec(t.Context(), `DELETE FROM grove_migrations WHERE version=$1`, "20261017120000"); err != nil {
		t.Fatal(err)
	}
	if err = s.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = migration.Down(t.Context(), executor); err == nil || !strings.Contains(err.Error(), "retained cancellation receipts prevent downgrade") {
		t.Fatalf("unsafe downgrade: %v", err)
	}
	if again, retryErr := s.RequestCancelExecution(t.Context(), request); retryErr != nil || again != receipt {
		t.Fatalf("migration receipt: %+v %v", again, retryErr)
	}
}

func TestDurableCancellationFenceLockWait(t *testing.T) {
	testDurableDeadlineCheckedAfterTaskLock(t, "pending")
}
