//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/store/postgres"
)

func enableTestHeartbeat(t *testing.T, s durable.Store, timeout time.Duration) (durable.Key, durable.Task) {
	t.Helper()
	key := durable.Key{Namespace: t.Name(), WorkflowID: "order", RunID: "run"}
	if _, err := s.StartExecution(t.Context(), durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "order", BuildID: "v1", Queue: "orders"}); err != nil {
		t.Fatal(err)
	}
	workflow, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: key.Namespace, Queue: "orders", Kind: durable.TaskWorkflow, Owner: "worker", LeaseDuration: time.Minute})
	if err != nil || workflow == nil {
		t.Fatalf("workflow claim: %+v %v", workflow, err)
	}
	_, err = s.CommitTransition(t.Context(), durable.CommitRequest{Key: key, RequestID: "schedule", ExpectedRevision: 1, Token: workflow.Token(),
		Events: []durable.EventInput{{Type: "scheduled"}}, Tasks: []durable.TaskSpec{{ID: "activity", Kind: durable.TaskActivity, Queue: "external"}}})
	if err != nil {
		t.Fatal(err)
	}
	task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: key.Namespace, Queue: "external", Kind: durable.TaskActivity, Owner: "worker", LeaseDuration: time.Minute})
	if err != nil || task == nil {
		t.Fatalf("activity claim: %+v %v", task, err)
	}
	hard := time.Minute
	_, err = s.CommitTransition(t.Context(), durable.CommitRequest{Key: key, RequestID: "attempt", ExpectedRevision: 2, Token: task.Token(),
		Events: []durable.EventInput{{Type: "attempt.started"}}, TaskUpdate: &durable.TaskUpdate{Action: durable.TaskKeep, DeadlineAfter: &hard, Heartbeat: &durable.HeartbeatConfig{Timeout: timeout}}})
	if err != nil {
		t.Fatal(err)
	}
	saved, err := s.GetTask(t.Context(), key, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	return key, saved
}

type lostHeartbeatResponseStore struct{ durable.Store }

func (s lostHeartbeatResponseStore) RecordHeartbeat(ctx context.Context, request durable.HeartbeatRequest) (durable.Receipt, error) {
	if _, err := s.Store.RecordHeartbeat(ctx, request); err != nil {
		return durable.Receipt{}, err
	}
	return durable.Receipt{}, errors.New("heartbeat acknowledgement lost after commit")
}

func TestDurableHeartbeatReopenAndLostAcknowledgement(t *testing.T) {
	s, dsn := setupTestStoreConnection(t)
	key, task := enableTestHeartbeat(t, s, 10*time.Second)
	request := durable.HeartbeatRequest{Key: key, RequestID: "heartbeat", Token: task.Token(), Sequence: 1, Progress: []byte("offset:42"), LeaseDuration: time.Minute}
	if _, err := (lostHeartbeatResponseStore{Store: s}).RecordHeartbeat(t.Context(), request); err == nil {
		t.Fatal("test did not lose acknowledgement")
	}
	saved, err := s.GetTask(t.Context(), key, task.ID)
	if err != nil || saved.HeartbeatSequence != 1 {
		t.Fatalf("heartbeat did not persist before lost response: %+v %v", saved, err)
	}
	if _, err = pgdriver.Unwrap(s.DB()).Exec(t.Context(), `DELETE FROM grove_migrations WHERE version=$1`, "20261013120000"); err != nil {
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
	receipt, err := reopened.RecordHeartbeat(t.Context(), request)
	if err != nil || receipt.Revision != 3 || receipt.FirstSequence != 0 || receipt.LastSequence != 0 {
		t.Fatalf("heartbeat receipt after reopen: %+v %v", receipt, err)
	}
	current, err := reopened.GetTask(t.Context(), key, task.ID)
	if err != nil || current.Version != saved.Version || current.HeartbeatSequence != saved.HeartbeatSequence || current.HeartbeatEpoch != task.Epoch ||
		!current.HeartbeatAt.Equal(saved.HeartbeatAt) || !current.HeartbeatLimit.Equal(saved.HeartbeatLimit) || !current.DeadlineAt.Equal(saved.DeadlineAt) ||
		current.HeartbeatTimeout != saved.HeartbeatTimeout || string(current.Progress) != "offset:42" {
		t.Fatalf("migration/reopen/receipt changed heartbeat state: %+v %v", current, err)
	}
}

func TestDurableHeartbeatChecksDeadlineAfterLockWait(t *testing.T) {
	s := setupTestStore(t)
	key, task := enableTestHeartbeat(t, s, 500*time.Millisecond)
	pg := pgdriver.Unwrap(s.DB())
	tx, err := pg.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec(t.Context(), `SELECT 1 FROM dispatch_execution_tasks WHERE namespace=$1 AND task_id=$2 FOR UPDATE`, key.Namespace, task.ID); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, recordErr := s.RecordHeartbeat(t.Context(), durable.HeartbeatRequest{Key: key, RequestID: "heartbeat", Token: task.Token(), Sequence: 1, Progress: []byte("late"), LeaseDuration: time.Minute})
		result <- recordErr
	}()
	limit := time.Now().Add(5 * time.Second)
	for {
		var blocked bool
		if err = pg.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%dispatch_execution_tasks%')`).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		if time.Now().After(limit) {
			t.Fatal("heartbeat did not wait for task lock")
		}
		time.Sleep(5 * time.Millisecond)
	}
	for {
		var expired bool
		if err = pg.QueryRow(t.Context(), `SELECT clock_timestamp() >= $1`, task.DeadlineAt).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			break
		}
		if time.Now().After(limit) {
			t.Fatal("heartbeat deadline did not expire")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-result:
		if !errors.Is(err, durable.ErrTaskDeadline) {
			t.Fatalf("heartbeat revived task after lock wait: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("heartbeat stayed blocked after lock release")
	}
	current, err := s.GetTask(t.Context(), key, task.ID)
	if err != nil || current.Version != task.Version || current.HeartbeatSequence != 0 || len(current.Progress) != 0 {
		t.Fatalf("expired heartbeat left partial progress: %+v %v", current, err)
	}
}
