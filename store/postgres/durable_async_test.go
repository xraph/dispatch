//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/store/postgres"
)

const asyncTestSecret = "0202020202020202020202020202020202020202020202020202020202020202"

func asyncHandoffRequest(t *testing.T, key durable.Key, task durable.Task) durable.CommitRequest {
	t.Helper()
	hash, err := durable.HashAsyncSecret(asyncTestSecret)
	if err != nil {
		t.Fatal(err)
	}
	return durable.CommitRequest{Key: key, RequestID: "await", ExpectedRevision: 3, Token: task.Token(), Events: []durable.EventInput{{Type: "activity.awaited"}}, TaskUpdate: &durable.TaskUpdate{Action: durable.TaskAwait, AsyncKeyHash: hash}}
}

func asyncCompletionRequest(key durable.Key, task durable.Task) durable.CommitRequest {
	return durable.CommitRequest{Key: key, RequestID: "complete", ExpectedRevision: 4, Token: task.Token(), AsyncSecret: asyncTestSecret, Events: []durable.EventInput{{Type: "activity.completed"}}, State: durable.StateCompleted, Output: []byte("paid")}
}

type lostAsyncResponseStore struct{ durable.Store }

var errAsyncAcknowledgementLost = errors.New("async acknowledgement lost after commit")

func (s lostAsyncResponseStore) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
	if _, err := s.Store.CommitTransition(ctx, r); err != nil {
		return durable.Receipt{}, err
	}
	return durable.Receipt{}, errAsyncAcknowledgementLost
}

func (s lostAsyncResponseStore) RecordHeartbeat(ctx context.Context, r durable.HeartbeatRequest) (durable.Receipt, error) {
	if _, err := s.Store.RecordHeartbeat(ctx, r); err != nil {
		return durable.Receipt{}, err
	}
	return durable.Receipt{}, errAsyncAcknowledgementLost
}

func TestDurableAsyncReopenAndReceipts(t *testing.T) {
	s, dsn := setupTestStoreConnection(t)
	key, task := enableTestHeartbeat(t, s, time.Minute)
	handoff := asyncHandoffRequest(t, key, task)
	if _, err := (lostAsyncResponseStore{Store: s}).CommitTransition(t.Context(), handoff); !errors.Is(err, errAsyncAcknowledgementLost) {
		t.Fatalf("test did not lose handoff response: %v", err)
	}
	task, err := s.GetTask(t.Context(), key, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	heartbeat := durable.HeartbeatRequest{Key: key, RequestID: "heartbeat", Token: task.Token(), AsyncSecret: asyncTestSecret, Sequence: 1, Progress: []byte("offset:42")}
	if _, err = (lostAsyncResponseStore{Store: s}).RecordHeartbeat(t.Context(), heartbeat); !errors.Is(err, errAsyncAcknowledgementLost) {
		t.Fatalf("test did not lose heartbeat response: %v", err)
	}
	saved, err := s.GetTask(t.Context(), key, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pgdriver.Unwrap(s.DB()).Exec(t.Context(), `DELETE FROM grove_migrations WHERE version=$1`, "20261014120000"); err != nil {
		t.Fatal(err)
	}
	if err = s.Migrate(t.Context()); err != nil {
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
	current, err := reopened.GetTask(t.Context(), key, task.ID)
	if err != nil || current.LeaseKind != durable.LeaseAsync || !current.LeaseUntil.Equal(current.DeadlineAt) || current.AsyncKeyHash != handoff.TaskUpdate.AsyncKeyHash || current.Version != saved.Version || !current.DeadlineAt.Equal(saved.DeadlineAt) || string(current.Progress) != "offset:42" {
		t.Fatalf("async grant lost on reopen: %+v %v", current, err)
	}
	if receipt, replayErr := reopened.CommitTransition(t.Context(), handoff); replayErr != nil || receipt.Revision != 4 || receipt.FirstSequence != 4 || receipt.LastSequence != 4 {
		t.Fatalf("handoff receipt: %+v %v", receipt, replayErr)
	}
	if receipt, replayErr := reopened.RecordHeartbeat(t.Context(), heartbeat); replayErr != nil || receipt.Revision != 4 || receipt.FirstSequence != 0 {
		t.Fatalf("heartbeat receipt: %+v %v", receipt, replayErr)
	}
	complete := asyncCompletionRequest(key, current)
	if _, err = (lostAsyncResponseStore{Store: reopened}).CommitTransition(t.Context(), complete); !errors.Is(err, errAsyncAcknowledgementLost) {
		t.Fatalf("test did not lose completion response: %v", err)
	}
	if receipt, replayErr := reopened.CommitTransition(t.Context(), complete); replayErr != nil || receipt.Revision != 5 {
		t.Fatalf("completion receipt: %+v %v", receipt, replayErr)
	}
	execution, err := reopened.GetExecution(t.Context(), key)
	if err != nil || execution.State != durable.StateCompleted || string(execution.Output) != "paid" {
		t.Fatalf("async completion: %+v %v", execution, err)
	}
}

func TestDurableAsyncChecksDeadlineAfterLockWait(t *testing.T) {
	for _, mode := range []string{"handoff", "heartbeat", "completion"} {
		t.Run(mode, func(t *testing.T) {
			s := setupTestStore(t)
			key, task := enableTestHeartbeat(t, s, 500*time.Millisecond)
			handoff := asyncHandoffRequest(t, key, task)
			if mode != "handoff" {
				if _, err := s.CommitTransition(t.Context(), handoff); err != nil {
					t.Fatal(err)
				}
				var err error
				task, err = s.GetTask(t.Context(), key, task.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			pg := pgdriver.Unwrap(s.DB())
			tx, err := pg.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err = tx.Exec(t.Context(), `SELECT 1 FROM dispatch_execution_tasks WHERE namespace=$1 AND task_id=$2 FOR UPDATE`, key.Namespace, task.ID); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var writeErr error
				switch mode {
				case "handoff":
					_, writeErr = s.CommitTransition(ctx, handoff)
				case "heartbeat":
					_, writeErr = s.RecordHeartbeat(ctx, durable.HeartbeatRequest{Key: key, RequestID: "heartbeat", Token: task.Token(), AsyncSecret: asyncTestSecret, Sequence: 1, Progress: []byte("late")})
				case "completion":
					_, writeErr = s.CommitTransition(ctx, asyncCompletionRequest(key, task))
				}
				done <- writeErr
			}()
			for {
				var blocked bool
				if err = pg.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%dispatch_execution_tasks%')`).Scan(&blocked); err != nil {
					t.Fatal(err)
				}
				if blocked {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			for {
				var expired bool
				if err = pg.QueryRow(ctx, `SELECT clock_timestamp() >= $1`, task.DeadlineAt).Scan(&expired); err != nil {
					t.Fatal(err)
				}
				if expired {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			if err = tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			select {
			case err = <-done:
				if !errors.Is(err, durable.ErrTaskDeadline) {
					t.Fatalf("late %s accepted: %v", mode, err)
				}
			case <-ctx.Done():
				t.Fatal("callback did not finish after lock release")
			}
			current, err := s.GetTask(t.Context(), key, task.ID)
			if err != nil || current.Version != task.Version || current.Done || current.HeartbeatSequence != task.HeartbeatSequence || current.LeaseKind != task.LeaseKind || current.AsyncKeyHash != task.AsyncKeyHash {
				t.Fatalf("expired callback changed task: %+v %v", current, err)
			}
			execution, err := s.GetExecution(t.Context(), key)
			expectedRevision := int64(4)
			if mode == "handoff" {
				expectedRevision = 3
			}
			if err != nil || execution.Revision != expectedRevision || execution.State != durable.StateRunning {
				t.Fatalf("expired callback changed history: %+v %v", execution, err)
			}
		})
	}
}

func TestDurableAsyncHandoffRollsBackOnReceiptFailure(t *testing.T) {
	s := setupTestStore(t)
	key, task := enableTestHeartbeat(t, s, time.Minute)
	handoff := asyncHandoffRequest(t, key, task)
	handoff.Tasks = []durable.TaskSpec{{ID: "after", Kind: durable.TaskWorkflow, Queue: "orders"}}
	pg := pgdriver.Unwrap(s.DB())
	_, err := pg.Exec(t.Context(), `CREATE FUNCTION reject_async_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.request_id = 'await' THEN RAISE EXCEPTION 'injected async receipt failure'; END IF; RETURN NEW; END $$;
 CREATE TRIGGER reject_async_receipt BEFORE INSERT ON dispatch_execution_receipts FOR EACH ROW EXECUTE FUNCTION reject_async_receipt()`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CommitTransition(t.Context(), handoff); err == nil || !strings.Contains(err.Error(), "injected async receipt failure") {
		t.Fatalf("test did not inject receipt failure: %v", err)
	}
	current, err := s.GetTask(t.Context(), key, task.ID)
	if err != nil || current.LeaseKind != "" || current.Version != task.Version || current.AsyncKeyHash != "" || !current.LeaseUntil.Equal(task.LeaseUntil) {
		t.Fatalf("partial handoff after rollback: %+v %v", current, err)
	}
	if _, err = s.GetTask(t.Context(), key, "after"); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("partial task after rollback: %v", err)
	}
	execution, err := s.GetExecution(t.Context(), key)
	if err != nil || execution.Revision != 3 || execution.LastSequence != 3 {
		t.Fatalf("partial history after rollback: %+v %v", execution, err)
	}
	if _, err = pg.Exec(t.Context(), `DROP TRIGGER reject_async_receipt ON dispatch_execution_receipts`); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CommitTransition(t.Context(), handoff); err != nil {
		t.Fatalf("retry after rollback: %v", err)
	}
}

func TestDurableAsyncLegacyPollFence(t *testing.T) {
	for _, heartbeat := range []bool{false, true} {
		t.Run(fmt.Sprint(heartbeat), func(t *testing.T) {
			s := setupTestStore(t)
			key, task := enableTestHeartbeat(t, s, time.Minute)
			if _, err := s.CommitTransition(t.Context(), asyncHandoffRequest(t, key, task)); err != nil {
				t.Fatal(err)
			}
			task, err := s.GetTask(t.Context(), key, task.ID)
			if err != nil {
				t.Fatal(err)
			}
			if heartbeat {
				if _, err = s.RecordHeartbeat(t.Context(), durable.HeartbeatRequest{Key: key, RequestID: "heartbeat", Token: task.Token(), AsyncSecret: asyncTestSecret, Sequence: 1}); err != nil {
					t.Fatal(err)
				}
			}
			// This is the previous poller's eligibility predicate, before it understood async grants.
			var claimed string
			err = pgdriver.Unwrap(s.DB()).QueryRow(t.Context(), `WITH candidate AS (
   SELECT t.namespace,t.workflow_id,t.run_id,t.task_id FROM dispatch_execution_tasks t
   JOIN dispatch_executions e USING(namespace,workflow_id,run_id)
   WHERE t.namespace=$1 AND t.kind='activity' AND NOT t.done AND e.state='running'
   AND t.available_at <= clock_timestamp()
   AND (t.lease_until IS NULL OR t.lease_until <= clock_timestamp())
   AND (t.deadline_at IS NULL OR t.deadline_at > clock_timestamp())
   FOR UPDATE OF t SKIP LOCKED LIMIT 1
  ) UPDATE dispatch_execution_tasks t SET owner='old-poller',epoch=t.epoch+1,
    attempt=t.attempt+1,version=t.version+1,lease_kind='',lease_until=clock_timestamp()+interval '1 minute'
   FROM candidate c WHERE t.namespace=c.namespace AND t.workflow_id=c.workflow_id AND t.run_id=c.run_id AND t.task_id=c.task_id
   RETURNING t.task_id`, key.Namespace).Scan(&claimed)
			if !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("older poller reclaimed async grant: %q %v", claimed, err)
			}
		})
	}
}
