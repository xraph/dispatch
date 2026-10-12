//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/grove/migrate"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/store/postgres"
)

func deliveryPair(t *testing.T, s *postgres.Store) (durable.StartRequest, durable.ChildStartSpec) {
	t.Helper()
	parent, _, create := childCreationRequest(t, s, time.Minute)
	if _, err := s.CommitTransition(t.Context(), create); err != nil {
		t.Fatal(err)
	}
	return parent, create.Children[0]
}

func childCloseRequest(t *testing.T, s *postgres.Store, start durable.StartRequest) durable.CommitRequest {
	t.Helper()
	task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: start.Namespace, Queue: start.Queue, BuildID: start.BuildID, Kind: durable.TaskWorkflow, Owner: "closer", LeaseDuration: time.Minute})
	if err != nil || task == nil || task.Key != start.Key {
		t.Fatalf("close claim: %+v %v", task, err)
	}
	e, err := s.GetExecution(t.Context(), start.Key)
	if err != nil {
		t.Fatal(err)
	}
	return durable.CommitRequest{Key: start.Key, Token: task.Token(), ExpectedRevision: e.Revision, RequestID: "close", State: durable.StateCompleted, Output: []byte("result"), Events: []durable.EventInput{{Type: "workflow.completed", Payload: []byte("result")}}}
}

func pollChildDelivery(t *testing.T, s *postgres.Store, target durable.StartRequest, ttl time.Duration) (*durable.ChildDelivery, durable.ChildDeliveryRequest) {
	t.Helper()
	d, err := s.ClaimChildDelivery(t.Context(), durable.ChildDeliveryClaimRequest{Namespace: target.Namespace, BuildID: target.BuildID, Owner: "delivery", LeaseDuration: ttl})
	if err != nil || d == nil {
		t.Fatalf("delivery claim: %+v %v", d, err)
	}
	return d, durable.ChildDeliveryRequest{Source: d.Source, DeliveryID: d.ID, RequestID: "apply", Owner: d.Owner, Epoch: d.Epoch}
}

func TestDurableChildDeliveryRollback(t *testing.T) {
	for _, failure := range []string{"source_outbox", "target_event", "target_receipt", "termination_result", "cancel_ack"} {
		t.Run(failure, func(t *testing.T) {
			s := setupTestStore(t)
			parent, child := deliveryPair(t, s)
			table := "dispatch_child_deliveries"
			var commit *durable.CommitRequest
			var apply *durable.ChildDeliveryRequest
			target := parent.Key
			if failure == "source_outbox" {
				request := childCloseRequest(t, s, child.Start)
				commit = &request
				target = child.Start.Key
			} else {
				source := child.Start
				if failure == "termination_result" {
					source = parent
					target = child.Start.Key
				}
				request := childCloseRequest(t, s, source)
				if failure == "cancel_ack" {
					request = childCloseRequest(t, s, parent)
					target = child.Start.Key
					request.State = ""
					request.Output = nil
					request.CancelChildren = []durable.ChildCancellationSpec{{CommandID: "cancel", TargetID: child.CommandID}}
				}
				if _, err := s.CommitTransition(t.Context(), request); err != nil {
					t.Fatal(err)
				}
				destination := parent
				if target == child.Start.Key {
					destination = child.Start
				}
				_, delivery := pollChildDelivery(t, s, destination, time.Minute)
				apply = &delivery
				switch failure {
				case "target_event":
					table = "dispatch_execution_events"
				case "target_receipt":
					table = "dispatch_child_delivery_receipts"
				}
			}
			before, err := s.GetExecution(t.Context(), target)
			if err != nil {
				t.Fatal(err)
			}
			pg := pgdriver.Unwrap(s.DB())
			_, err = pg.Exec(t.Context(), fmt.Sprintf(`CREATE FUNCTION reject_delivery_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected delivery write failure'; END $$;
CREATE TRIGGER reject_delivery_write BEFORE INSERT ON %s FOR EACH ROW EXECUTE FUNCTION reject_delivery_write()`, table))
			if err != nil {
				t.Fatal(err)
			}
			if commit != nil {
				_, err = s.CommitTransition(t.Context(), *commit)
			} else {
				_, err = s.ApplyChildDelivery(t.Context(), *apply)
			}
			if err == nil || !strings.Contains(err.Error(), "injected delivery write failure") {
				t.Fatalf("fault not reached: %v", err)
			}
			after, err := s.GetExecution(t.Context(), target)
			if err != nil || after.Revision != before.Revision || after.LastSequence != before.LastSequence || after.State != before.State || string(after.Output) != string(before.Output) {
				t.Fatalf("partial target: before=%+v after=%+v err=%v", before, after, err)
			}
			if apply != nil {
				d, readErr := s.GetChildDelivery(t.Context(), apply.Source, apply.DeliveryID)
				if readErr != nil || d.Done {
					t.Fatalf("partial delivery completion: %+v %v", d, readErr)
				}
			}
			messages, err := s.ListChildDeliveries(t.Context(), target, "", 100)
			if err != nil || len(messages) != 0 {
				t.Fatalf("partial generated messages: %+v %v", messages, err)
			}
			if failure == "cancel_ack" {
				var receipts int
				if err = pg.QueryRow(t.Context(), `SELECT count(*) FROM dispatch_cancellation_receipts`).Scan(&receipts); err != nil || receipts != 0 {
					t.Fatalf("partial cancellation receipt: %d %v", receipts, err)
				}
			}
			if _, err = pg.Exec(t.Context(), fmt.Sprintf(`DROP TRIGGER reject_delivery_write ON %s; DROP FUNCTION reject_delivery_write()`, table)); err != nil {
				t.Fatal(err)
			}
			if commit != nil {
				_, err = s.CommitTransition(t.Context(), *commit)
			} else {
				_, err = s.ApplyChildDelivery(t.Context(), *apply)
			}
			if err != nil {
				t.Fatalf("retry after rollback: %v", err)
			}
		})
	}
}

type lostChildDeliveryStore struct {
	durable.Store
	receipt durable.ChildDeliveryReceipt
}

var errChildDeliveryLost = errors.New("child delivery response lost")

func (s *lostChildDeliveryStore) ApplyChildDelivery(ctx context.Context, r durable.ChildDeliveryRequest) (durable.ChildDeliveryReceipt, error) {
	receipt, err := s.Store.ApplyChildDelivery(ctx, r)
	if err != nil {
		return receipt, err
	}
	s.receipt = receipt
	return durable.ChildDeliveryReceipt{}, errChildDeliveryLost
}

func TestDurableChildDeliveryRecovery(t *testing.T) {
	s, dsn := setupTestStoreConnection(t)
	parent, child := deliveryPair(t, s)
	if _, err := s.CommitTransition(t.Context(), childCloseRequest(t, s, child.Start)); err != nil {
		t.Fatal(err)
	}
	s = reopenAsyncStore(t, s, dsn)
	_, request := pollChildDelivery(t, s, parent, time.Minute)
	lost := &lostChildDeliveryStore{Store: s}
	if _, err := lost.ApplyChildDelivery(t.Context(), request); !errors.Is(err, errChildDeliveryLost) {
		t.Fatalf("response loss: %v", err)
	}
	s = reopenAsyncStore(t, s, dsn)
	if _, err := s.CommitTransition(t.Context(), childCloseRequest(t, s, parent)); err != nil {
		t.Fatal(err)
	}
	replacement := parent
	replacement.RunID += "-new"
	replacement.RequestID = "replacement"
	if _, err := s.StartExecution(t.Context(), replacement); err != nil {
		t.Fatal(err)
	}
	s = reopenAsyncStore(t, s, dsn)
	pg := pgdriver.Unwrap(s.DB())
	lock, err := pg.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback() }()
	if _, err = lock.Exec(t.Context(), `SELECT 1 FROM dispatch_executions WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 FOR UPDATE`, parent.Namespace, parent.WorkflowID, parent.RunID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	if got, retryErr := s.ApplyChildDelivery(ctx, request); retryErr != nil || got != lost.receipt {
		t.Fatalf("receipt recovery waited or retargeted: %+v %v", got, retryErr)
	}
	e, err := s.GetExecution(t.Context(), replacement.Key)
	if err != nil || e.Revision != 1 {
		t.Fatalf("replacement received old result: %+v %v", e, err)
	}
}

func TestDurableChildDeliveryLockExpiry(t *testing.T) {
	s := setupTestStore(t)
	parent, child := deliveryPair(t, s)
	if _, err := s.CommitTransition(t.Context(), childCloseRequest(t, s, child.Start)); err != nil {
		t.Fatal(err)
	}
	message, request := pollChildDelivery(t, s, parent, 300*time.Millisecond)
	pg := pgdriver.Unwrap(s.DB())
	lock, err := pg.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback() }()
	if _, err = lock.Exec(t.Context(), `SELECT 1 FROM dispatch_executions WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 FOR UPDATE`, parent.Namespace, parent.WorkflowID, parent.RunID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, applyErr := s.ApplyChildDelivery(ctx, request); done <- applyErr }()
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
	waitDurableStoreTime(t, s, message.LeaseUntil)
	if err = lock.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = <-done; !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("expired after target lock: %v", err)
	}
	e, err := s.GetExecution(t.Context(), parent.Key)
	if err != nil || e.Revision != 2 || e.LastSequence != 3 {
		t.Fatalf("expired delivery mutated: %+v %v", e, err)
	}
	_, request = pollChildDelivery(t, s, parent, time.Minute)
	if _, err = s.ApplyChildDelivery(t.Context(), request); err != nil {
		t.Fatal(err)
	}
}

func TestDurableChildDeliveryClosureLocks(t *testing.T) {
	for _, lockedChild := range []bool{false, true} {
		t.Run(fmt.Sprint(lockedChild), func(t *testing.T) {
			s := setupTestStore(t)
			parent, child := deliveryPair(t, s)
			parentClose := childCloseRequest(t, s, parent)
			childClose := childCloseRequest(t, s, child.Start)
			locked, closing := parent, childClose
			if lockedChild {
				locked, closing = child.Start, parentClose
			}
			pg := pgdriver.Unwrap(s.DB())
			lock, err := pg.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = lock.Rollback() }()
			if _, err = lock.Exec(t.Context(), `SELECT 1 FROM dispatch_executions WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 FOR UPDATE`, locked.Namespace, locked.WorkflowID, locked.RunID); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			defer cancel()
			if _, err = s.CommitTransition(ctx, closing); err != nil {
				t.Fatalf("source closure waited on target execution: %v", err)
			}
			if err = lock.Commit(); err != nil {
				t.Fatal(err)
			}
			remaining := parentClose
			if lockedChild {
				remaining = childClose
			}
			if _, err = s.CommitTransition(t.Context(), remaining); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDurableChildDeliveryMigration(t *testing.T) {
	s := setupTestStore(t)
	pg := pgdriver.Unwrap(s.DB())
	executor, err := migrate.NewExecutorFor(pg)
	if err != nil {
		t.Fatal(err)
	}
	var migration *migrate.Migration
	for _, candidate := range postgres.Migrations.Migrations() {
		if candidate.Version == "20261019120000" {
			migration = candidate
		}
	}
	if migration == nil {
		t.Fatal("delivery migration missing")
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
	parent, child := deliveryPair(t, s)
	if _, err = s.CommitTransition(t.Context(), childCloseRequest(t, s, child.Start)); err != nil {
		t.Fatal(err)
	}
	if err = migration.Down(t.Context(), executor); err == nil || !strings.Contains(err.Error(), "retained child deliveries prevent downgrade") {
		t.Fatalf("unsafe pending downgrade: %v", err)
	}
	_, request := pollChildDelivery(t, s, parent, time.Minute)
	receipt, err := s.ApplyChildDelivery(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err = migration.Down(t.Context(), executor); err == nil || !strings.Contains(err.Error(), "retained child delivery receipts prevent downgrade") {
		t.Fatalf("unsafe completed downgrade: %v", err)
	}
	if _, err = pg.Exec(t.Context(), `DELETE FROM grove_migrations WHERE version=$1`, migration.Version); err != nil {
		t.Fatal(err)
	}
	if err = s.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, retryErr := s.ApplyChildDelivery(t.Context(), request); retryErr != nil || got != receipt {
		t.Fatalf("migration receipt: %+v %v", got, retryErr)
	}
}
