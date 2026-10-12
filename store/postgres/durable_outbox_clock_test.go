//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/store/postgres"
)

// Wait until the mutation reaches the namespace admission barrier, then keep
// activation uncommitted until its original grant or deadline expires.
func waitAuditNamespaceWriter(ctx context.Context, t *testing.T, pg *pgdriver.PgDB, namespace string, done <-chan error) {
	t.Helper()
	for {
		var blocked bool
		err := pg.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND classid=((dispatch_audit_lock_key($1)>>32)&4294967295)::oid AND objid=(dispatch_audit_lock_key($1)&4294967295)::oid)`, namespace).Scan(&blocked)
		if err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		select {
		case err = <-done:
			t.Fatalf("mutation returned before namespace barrier: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}

func expiringAuditChildDelivery(t *testing.T, s *postgres.Store, ignore bool) *durable.ChildDelivery {
	t.Helper()
	ctx := t.Context()
	key := auditSeed(t, s, t.Name())
	workflow, err := s.ClaimTask(ctx, durable.ClaimRequest{Namespace: key.Namespace, Queue: "q", Kind: durable.TaskWorkflow, Owner: "worker", LeaseDuration: time.Minute})
	if err != nil || workflow == nil {
		t.Fatalf("parent claim: %+v %v", workflow, err)
	}
	child := durable.ChildStartSpec{CommandID: "child", Start: durable.StartRequest{Key: durable.Key{Namespace: key.Namespace, WorkflowID: "child", RunID: "run"}, RequestID: "child-start", WorkflowType: "child", BuildID: "v1", Queue: "children"}, ParentQueue: "q", ParentClosePolicy: durable.ParentCloseTerminate}
	if _, err = s.CommitTransition(ctx, durable.CommitRequest{Key: key, RequestID: "children", ExpectedRevision: 1, Token: workflow.Token(), Events: []durable.EventInput{{Type: "decision"}}, Children: []durable.ChildStartSpec{child}, Tasks: []durable.TaskSpec{{ID: "next", Kind: durable.TaskWorkflow, Queue: "q"}}}); err != nil {
		t.Fatal(err)
	}
	next, err := s.ClaimTask(ctx, durable.ClaimRequest{Namespace: key.Namespace, Queue: "q", Kind: durable.TaskWorkflow, Owner: "worker", LeaseDuration: time.Minute})
	if err != nil || next == nil {
		t.Fatalf("parent next: %+v %v", next, err)
	}
	if _, err = s.CommitTransition(ctx, durable.CommitRequest{Key: key, RequestID: "complete", ExpectedRevision: 2, Token: next.Token(), State: durable.StateCompleted, Events: []durable.EventInput{{Type: "completed"}}}); err != nil {
		t.Fatal(err)
	}
	if ignore {
		task, claimErr := s.ClaimTask(ctx, durable.ClaimRequest{Namespace: key.Namespace, Queue: "children", Kind: durable.TaskWorkflow, Owner: "child", LeaseDuration: time.Minute})
		if claimErr != nil || task == nil {
			t.Fatalf("child claim: %+v %v", task, claimErr)
		}
		if _, commitErr := s.CommitTransition(ctx, durable.CommitRequest{Key: child.Start.Key, RequestID: "complete-child", ExpectedRevision: 1, Token: task.Token(), State: durable.StateCompleted, Events: []durable.EventInput{{Type: "completed"}}}); commitErr != nil {
			t.Fatal(commitErr)
		}
	}
	delivery, err := s.ClaimChildDelivery(ctx, durable.ChildDeliveryClaimRequest{Namespace: key.Namespace, Owner: "delivery-worker", LeaseDuration: 500 * time.Millisecond})
	if err != nil || delivery == nil || delivery.Source != key {
		t.Fatalf("child delivery: %+v %v", delivery, err)
	}
	return delivery
}

func TestDurableOutboxNamespaceWaitExpiry(t *testing.T) {
	s := setupTestStore(t)
	pg := pgdriver.Unwrap(s.DB())
	for _, kind := range []string{"transition_lease", "heartbeat_deadline", "child_lease", "ignored_child_lease"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			var mutate func() error
			var assertUnchanged func()
			var expiry time.Time
			var key durable.Key
			want := durable.ErrLeaseLost
			switch kind {
			case "heartbeat_deadline":
				var task durable.Task
				key, task = enableTestHeartbeat(t, s, 500*time.Millisecond)
				expiry = task.DeadlineAt
				want = durable.ErrTaskDeadline
				mutate = func() error {
					_, err := s.RecordHeartbeat(ctx, durable.HeartbeatRequest{Key: key, RequestID: "expired-heartbeat", Token: task.Token(), Sequence: 1, Progress: []byte("must-not-persist"), LeaseDuration: time.Minute})
					return err
				}
				assertUnchanged = func() {
					after, err := s.GetTask(ctx, key, task.ID)
					if err != nil || !reflect.DeepEqual(after, task) {
						t.Fatalf("expired heartbeat changed task: %+v %v", after, err)
					}
				}
			case "transition_lease":
				key = auditSeed(t, s, t.Name())
				task, err := s.ClaimTask(ctx, durable.ClaimRequest{Namespace: key.Namespace, Queue: "q", Kind: durable.TaskWorkflow, Owner: "worker", LeaseDuration: 500 * time.Millisecond})
				if err != nil || task == nil {
					t.Fatalf("claim: %+v %v", task, err)
				}
				expiry = task.LeaseUntil
				mutate = func() error {
					_, commitErr := s.CommitTransition(ctx, durable.CommitRequest{Key: key, RequestID: "expired-transition", ExpectedRevision: 1, Token: task.Token(), Events: []durable.EventInput{{Type: "must-not-persist"}}})
					return commitErr
				}
				assertUnchanged = func() {
					after, readErr := s.GetTask(ctx, key, task.ID)
					if readErr != nil || !reflect.DeepEqual(after, *task) {
						t.Fatalf("expired transition changed task: %+v %v", after, readErr)
					}
				}
			default:
				delivery := expiringAuditChildDelivery(t, s, kind == "ignored_child_lease")
				key = delivery.Source
				expiry = delivery.LeaseUntil
				deliveryBefore, err := s.GetChildDelivery(ctx, delivery.Source, delivery.ID)
				if err != nil {
					t.Fatal(err)
				}
				targetHistory, historyErr := s.ReadHistory(ctx, delivery.Target, 0, 100)
				if historyErr != nil {
					t.Fatal(historyErr)
				}
				before, err := s.GetExecution(ctx, delivery.Target)
				if err != nil {
					t.Fatal(err)
				}
				mutate = func() error {
					_, applyErr := s.ApplyChildDelivery(ctx, durable.ChildDeliveryRequest{Source: delivery.Source, DeliveryID: delivery.ID, RequestID: "expired-child", Owner: delivery.Owner, Epoch: delivery.Epoch})
					return applyErr
				}
				assertUnchanged = func() {
					history, historyErr := s.ReadHistory(ctx, delivery.Target, 0, 100)
					if historyErr != nil || !reflect.DeepEqual(history, targetHistory) {
						t.Fatalf("target history changed: %+v %v", history, historyErr)
					}
					after, readErr := s.GetExecution(ctx, delivery.Target)
					if readErr != nil || !reflect.DeepEqual(after, before) {
						t.Fatalf("expired child delivery changed target: %+v %v", after, readErr)
					}
					saved, readErr := s.GetChildDelivery(ctx, delivery.Source, delivery.ID)
					if readErr != nil || !reflect.DeepEqual(saved, deliveryBefore) {
						t.Fatalf("expired child delivery consumed: %+v %v", saved, readErr)
					}
				}
			}
			before, err := s.GetExecution(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			sourceHistory, err := s.ReadHistory(ctx, key, 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			activation, err := pg.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = activation.Rollback() }()
			if err = auditInsertCatalog(ctx, activation, key.Namespace); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- mutate() }()
			waitAuditNamespaceWriter(ctx, t, pg, key.Namespace, done)
			waitDurableStoreTime(t, s, expiry)
			if err = activation.Commit(); err != nil {
				t.Fatal(err)
			}
			if err = <-done; !errors.Is(err, want) {
				t.Fatalf("expired %s: got %v, want %v", kind, err, want)
			}
			assertUnchanged()
			history, err := s.ReadHistory(ctx, key, 0, 100)
			if err != nil || !reflect.DeepEqual(history, sourceHistory) {
				t.Fatalf("source history changed: %+v %v", history, err)
			}
			after, err := s.GetExecution(ctx, key)
			if err != nil || !reflect.DeepEqual(after, before) {
				t.Fatalf("source changed: %+v %v", after, err)
			}
			var count int
			if err = pg.QueryRow(ctx, `SELECT count(*) FROM dispatch_durable_outbox WHERE namespace=$1`, key.Namespace).Scan(&count); err != nil || count != 0 {
				t.Fatalf("rejected mutation persisted intents: %d %v", count, err)
			}
			if err = pg.QueryRow(ctx, `SELECT (SELECT count(*) FROM dispatch_execution_receipts WHERE namespace=$1 AND request_id LIKE 'expired-%')+(SELECT count(*) FROM dispatch_child_delivery_receipts WHERE namespace=$1 AND request_id LIKE 'expired-%')`, key.Namespace).Scan(&count); err != nil || count != 0 {
				t.Fatalf("rejected mutation persisted receipt: %d %v", count, err)
			}
		})
	}
}
