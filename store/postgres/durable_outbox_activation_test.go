//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch/durable"
)

func auditConfig(namespace string) durable.NamespaceConfig {
	return durable.NamespaceConfig{InstallationID: "host", Namespace: namespace, AppID: "app", TenantID: "tenant", RequireAudit: true, RequireHooks: true, SchemaVersion: 1}
}
func auditSeed(t *testing.T, s durable.Store, namespace string) durable.Key {
	t.Helper()
	key := durable.Key{Namespace: namespace, WorkflowID: "workflow", RunID: "run"}
	if _, err := s.StartExecution(t.Context(), durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "order", BuildID: "v1", Queue: "q"}); err != nil {
		t.Fatal(err)
	}
	return key
}
func auditOldEvent(ctx context.Context, tx driver.Tx, key durable.Key) error {
	_, err := tx.Exec(ctx, `INSERT INTO dispatch_execution_events(namespace,workflow_id,run_id,sequence,type,payload,occurred_at) VALUES($1,$2,$3,2,'old.writer',$4,'2000-01-01')`, key.Namespace, key.WorkflowID, key.RunID, []byte{})
	return err
}
func auditInsertCatalog(ctx context.Context, tx driver.Tx, namespace string) error {
	_, err := tx.Exec(ctx, `INSERT INTO dispatch_durable_namespaces(namespace,installation_id,app_id,tenant_id,require_audit,require_hooks,schema_version) VALUES($1,'host','app','tenant',true,true,1)`, namespace)
	return err
}
func auditWaitBlocked(t *testing.T, pg *pgdriver.PgDB, pid int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for {
		var blocked bool
		err := pg.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND NOT granted)`, pid).Scan(&blocked)
		if err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("transaction never reached lock barrier")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestDurableOutboxActivation(t *testing.T) {
	s, _ := setupTestStoreConnection(t)
	pg := pgdriver.Unwrap(s.DB())
	ctx := t.Context()
	t.Run("old_writer_before_activation", func(t *testing.T) {
		key := auditSeed(t, s, t.Name())
		writer, err := pg.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = writer.Rollback() }()
		if err = auditOldEvent(ctx, writer, key); err != nil {
			t.Fatal(err)
		}
		registration, err := pg.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = registration.Rollback() }()
		var pid int
		if err = registration.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			e := auditInsertCatalog(ctx, registration, key.Namespace)
			if e == nil {
				e = registration.Commit()
			}
			done <- e
		}()
		auditWaitBlocked(t, pg, pid)
		// A distinct namespace remains writable while registration is blocked.
		auditSeed(t, s, t.Name()+"-unrelated")
		if err = writer.Commit(); err != nil {
			t.Fatal(err)
		}
		if err = <-done; err != nil {
			t.Fatal(err)
		}
	})
	t.Run("activation_before_old_insert", func(t *testing.T) {
		key := auditSeed(t, s, t.Name())
		registration, err := pg.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = registration.Rollback() }()
		if err = auditInsertCatalog(ctx, registration, key.Namespace); err != nil {
			t.Fatal(err)
		}
		writer, err := pg.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = writer.Rollback() }()
		var pid int
		if err = writer.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
			t.Fatal(err)
		}
		// Mutations preceding history must roll back with the old writer.
		if _, err = writer.Exec(ctx, `UPDATE dispatch_execution_tasks SET owner='old' WHERE namespace=$1`, key.Namespace); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			e := auditOldEvent(ctx, writer, key)
			if e == nil {
				e = writer.Commit()
			}
			done <- e
		}()
		auditWaitBlocked(t, pg, pid)
		if err = registration.Commit(); err != nil {
			t.Fatal(err)
		}
		if err = <-done; err == nil {
			t.Fatal("old writer committed without intents")
		}
		var count int
		if err = pg.QueryRow(ctx, `SELECT count(*) FROM dispatch_execution_events WHERE namespace=$1 AND sequence=2`, key.Namespace).Scan(&count); err != nil || count != 0 {
			t.Fatalf("history rollback %d %v", count, err)
		}
		var owner string
		if err = pg.QueryRow(ctx, `SELECT owner FROM dispatch_execution_tasks WHERE namespace=$1`, key.Namespace).Scan(&owner); err != nil || owner != "" {
			t.Fatalf("task rollback %s %v", owner, err)
		}
	})
	t.Run("new_writer_waits_for_activation", func(t *testing.T) {
		key := auditSeed(t, s, t.Name())
		registration, err := pg.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = registration.Rollback() }()
		if err = auditInsertCatalog(ctx, registration, key.Namespace); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, e := s.SignalExecution(ctx, durable.SignalRequest{Key: key, RequestID: "signal", Name: "go", BuildID: "v1"})
			done <- e
		}()
		// Find the waiter on this namespace lock, not a sleep-based ordering guess.
		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		for {
			var exists bool
			err = pg.QueryRow(waitCtx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND classid=((dispatch_audit_lock_key($1)>>32)&4294967295)::oid AND objid=(dispatch_audit_lock_key($1)&4294967295)::oid)`, key.Namespace).Scan(&exists)
			if err != nil {
				t.Fatal(err)
			}
			if exists {
				break
			}
			select {
			case <-waitCtx.Done():
				t.Fatal("new writer never blocked")
			case <-time.After(time.Millisecond):
			}
		}
		if err = registration.Commit(); err != nil {
			t.Fatal(err)
		}
		if err = <-done; err != nil {
			t.Fatal(err)
		}
	})
	for _, isolation := range []string{"REPEATABLE READ", "SERIALIZABLE"} {
		for _, active := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s_active_%v", isolation, active), func(t *testing.T) {
				key := auditSeed(t, s, t.Name())
				tx, err := pg.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback() }()
				if _, err = tx.Exec(ctx, `SET TRANSACTION ISOLATION LEVEL `+isolation); err != nil {
					t.Fatal(err)
				}
				var count int
				if err = tx.QueryRow(ctx, `SELECT count(*) FROM dispatch_durable_namespaces`).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if active {
					if _, err = s.RegisterNamespace(ctx, auditConfig(key.Namespace)); err != nil {
						t.Fatal(err)
					}
				}
				if err = auditOldEvent(ctx, tx, key); err == nil {
					t.Fatal("unsupported isolation accepted")
				}
			})
		}
	}
	t.Run("immediate_and_savepoint", func(t *testing.T) {
		key := auditSeed(t, s, t.Name())
		if _, err := s.RegisterNamespace(ctx, auditConfig(key.Namespace)); err != nil {
			t.Fatal(err)
		}
		tx, err := pg.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		if _, err = tx.Exec(ctx, `SAVEPOINT candidate`); err != nil {
			t.Fatal(err)
		}
		if err = auditOldEvent(ctx, tx, key); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, `SET CONSTRAINTS ALL IMMEDIATE`); err == nil {
			t.Fatal("immediate constraints accepted missing intent")
		}
		if _, err = tx.Exec(ctx, `ROLLBACK TO SAVEPOINT candidate`); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("registration_rollback_and_upgrade", func(t *testing.T) {
		tx, err := pg.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err = auditInsertCatalog(ctx, tx, t.Name()); err != nil {
			t.Fatal(err)
		}
		if err = tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if _, err = s.GetNamespace(ctx, "host", t.Name()); !errors.Is(err, durable.ErrNotFound) {
			t.Fatalf("rolled back ownership: %v", err)
		}
		tx, err = pg.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		if _, err = tx.Exec(ctx, `SELECT dispatch_audit_writer_lock($1)`, t.Name()); err != nil {
			t.Fatal(err)
		}
		if err = auditInsertCatalog(ctx, tx, t.Name()); err == nil {
			t.Fatal("shared lock upgrade accepted")
		}
	})
}

func TestDurableOutboxRollbackAndFencing(t *testing.T) {
	s, _ := setupTestStoreConnection(t)
	pg := pgdriver.Unwrap(s.DB())
	ctx := t.Context()
	t.Run("second_destination_rolls_back_start", func(t *testing.T) {
		n := auditConfig(t.Name())
		if _, err := s.RegisterNamespace(ctx, n); err != nil {
			t.Fatal(err)
		}
		if _, err := pg.Exec(ctx, `CREATE FUNCTION dispatch_test_reject_relay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.destination='relay' THEN RAISE EXCEPTION 'injected relay failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER test_reject_relay BEFORE INSERT ON dispatch_durable_outbox FOR EACH ROW EXECUTE FUNCTION dispatch_test_reject_relay()`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := pg.Exec(ctx, `DROP TRIGGER test_reject_relay ON dispatch_durable_outbox; DROP FUNCTION dispatch_test_reject_relay()`); err != nil {
				t.Error(err)
			}
		}()
		r := durable.StartRequest{Key: durable.Key{Namespace: n.Namespace, WorkflowID: "w", RunID: "r"}, RequestID: "start", WorkflowType: "w", BuildID: "v1", Queue: "q"}
		if _, err := s.StartExecution(ctx, r); err == nil {
			t.Fatal("injected failure absent")
		}
		for _, table := range []string{"dispatch_executions", "dispatch_execution_events", "dispatch_execution_tasks", "dispatch_execution_receipts", "dispatch_durable_outbox"} {
			var count int
			if err := pg.QueryRow(ctx, `SELECT count(*) FROM `+table+` WHERE namespace=$1`, n.Namespace).Scan(&count); err != nil || count != 0 {
				t.Fatalf("%s rollback: %d %v", table, count, err)
			}
		}
	})
	t.Run("heartbeat_receipt_rolls_back_task", func(t *testing.T) {
		if _, err := s.RegisterNamespace(ctx, auditConfig(t.Name())); err != nil {
			t.Fatal(err)
		}
		key, task := enableTestHeartbeat(t, s, time.Minute)
		if _, err := pg.Exec(ctx, `CREATE FUNCTION dispatch_test_reject_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.source_kind='execution_receipt' THEN RAISE EXCEPTION 'injected receipt failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER test_reject_receipt BEFORE INSERT ON dispatch_durable_outbox FOR EACH ROW EXECUTE FUNCTION dispatch_test_reject_receipt()`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := pg.Exec(ctx, `DROP TRIGGER test_reject_receipt ON dispatch_durable_outbox; DROP FUNCTION dispatch_test_reject_receipt()`); err != nil {
				t.Error(err)
			}
		}()
		request := durable.HeartbeatRequest{Key: key, RequestID: "beat", Token: task.Token(), Sequence: 1, Progress: []byte("private"), LeaseDuration: time.Minute}
		if _, err := s.RecordHeartbeat(ctx, request); err == nil {
			t.Fatal("receipt failure absent")
		}
		after, err := s.GetTask(ctx, key, task.ID)
		if err != nil || after.Version != task.Version || after.HeartbeatSequence != task.HeartbeatSequence || string(after.Progress) != string(task.Progress) {
			t.Fatalf("task rollback: %+v %v", after, err)
		}
	})
	t.Run("ack_wait_expiry", func(t *testing.T) {
		n := auditConfig(t.Name())
		if _, err := s.RegisterNamespace(ctx, n); err != nil {
			t.Fatal(err)
		}
		a, err := durable.CaptureSecurityAudit("host", n.Namespace, "read", "denied", "target", durable.AuditMetadata{ActorKind: "anonymous"})
		if err != nil {
			t.Fatal(err)
		}
		d, err := s.AppendSecurityAudit(ctx, a)
		if err != nil {
			t.Fatal(err)
		}
		// Use an independent installation to select exactly this record.
		claims, err := s.ClaimDeliveries(ctx, durable.DeliveryClaim{DeliveryScope: durable.DeliveryScope{InstallationID: "host", Destination: durable.DestinationChronicle}, Owner: "publisher", Limit: 100, LeaseDuration: 100 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		var c durable.DeliveryRecord
		for _, v := range claims {
			if v.Delivery.ID == d.ID {
				c = v
			}
		}
		if c.Delivery.ID == "" {
			t.Fatal("missing claim")
		}
		lock, err := pg.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = lock.Rollback() }()
		if _, err = lock.Exec(ctx, `SELECT id FROM dispatch_durable_outbox WHERE id=$1 FOR UPDATE`, d.ID); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			done <- s.AcknowledgeDelivery(ctx, c.Token(), durable.SinkReceipt{ID: "sink", DeliveryID: d.ID, Destination: d.Destination, SchemaVersion: d.SchemaVersion, Fingerprint: d.Fingerprint})
		}()
		waitDurableStoreTime(t, s, c.LeaseUntil)
		if err = lock.Commit(); err != nil {
			t.Fatal(err)
		}
		if err = <-done; !errors.Is(err, durable.ErrLeaseLost) {
			t.Fatalf("expired ack after lock wait: %v", err)
		}
	})
	t.Run("concurrent_registration", func(t *testing.T) {
		config := auditConfig(t.Name())
		done := make(chan error, 2)
		for range 2 {
			go func() { _, err := s.RegisterNamespace(ctx, config); done <- err }()
		}
		for range 2 {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
		config.TenantID = "other"
		if _, err := s.RegisterNamespace(ctx, config); !errors.Is(err, durable.ErrRequestConflict) {
			t.Fatalf("conflicting registration: %v", err)
		}
	})
}

func TestDurableOutboxActivationComplexWriters(t *testing.T) {
	s, _ := setupTestStoreConnection(t)
	pg := pgdriver.Unwrap(s.DB())
	for _, kind := range []string{"children", "heartbeat", "child_delivery"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			var mutate func() error
			if kind == "heartbeat" {
				key, task := enableTestHeartbeat(t, s, time.Minute)
				mutate = func() error {
					_, err := s.RecordHeartbeat(ctx, durable.HeartbeatRequest{Key: key, RequestID: "heartbeat", Token: task.Token(), Sequence: 1, Progress: []byte("private"), LeaseDuration: time.Minute})
					return err
				}
			} else {
				key := auditSeed(t, s, t.Name())
				task, err := s.ClaimTask(ctx, durable.ClaimRequest{Namespace: key.Namespace, Queue: "q", Kind: durable.TaskWorkflow, Owner: "worker", LeaseDuration: time.Minute})
				if err != nil || task == nil {
					t.Fatal(err)
				}
				child := durable.ChildStartSpec{CommandID: "child", Start: durable.StartRequest{Key: durable.Key{Namespace: key.Namespace, WorkflowID: "child", RunID: "run"}, RequestID: "child-start", WorkflowType: "child", BuildID: "v1", Queue: "children"}, ParentQueue: "q", ParentClosePolicy: durable.ParentCloseTerminate}
				request := durable.CommitRequest{Key: key, RequestID: "children", ExpectedRevision: 1, Token: task.Token(), Events: []durable.EventInput{{Type: "decision"}}, Children: []durable.ChildStartSpec{child}, Tasks: []durable.TaskSpec{{ID: "next", Kind: durable.TaskWorkflow, Queue: "q"}}}
				mutate = func() error { _, commitErr := s.CommitTransition(ctx, request); return commitErr }
				if kind == "child_delivery" {
					if err = mutate(); err != nil {
						t.Fatal(err)
					}
					next, claimErr := s.ClaimTask(ctx, durable.ClaimRequest{Namespace: key.Namespace, Queue: "q", Kind: durable.TaskWorkflow, Owner: "worker", LeaseDuration: time.Minute})
					if claimErr != nil || next == nil {
						t.Fatal(claimErr)
					}
					if _, err = s.CommitTransition(ctx, durable.CommitRequest{Key: key, RequestID: "complete", ExpectedRevision: 2, Token: next.Token(), State: durable.StateCompleted, Events: []durable.EventInput{{Type: "completed"}}}); err != nil {
						t.Fatal(err)
					}
					delivery, claimErr := s.ClaimChildDelivery(ctx, durable.ChildDeliveryClaimRequest{Namespace: key.Namespace, Owner: "worker", LeaseDuration: time.Minute})
					if claimErr != nil || delivery == nil {
						t.Fatal(claimErr)
					}
					mutate = func() error {
						_, e := s.ApplyChildDelivery(ctx, durable.ChildDeliveryRequest{Source: delivery.Source, DeliveryID: delivery.ID, RequestID: "apply", Owner: delivery.Owner, Epoch: delivery.Epoch})
						return e
					}
				}
			}
			registration, err := pg.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = registration.Rollback() }()
			if err = auditInsertCatalog(ctx, registration, t.Name()); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- mutate() }()
			for {
				var waiting bool
				err = pg.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND classid=((dispatch_audit_lock_key($1)>>32)&4294967295)::oid AND objid=(dispatch_audit_lock_key($1)&4294967295)::oid)`, t.Name()).Scan(&waiting)
				if err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				select {
				case e := <-done:
					t.Fatalf("mutation returned before activation: %v", e)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(time.Millisecond):
				}
			}
			if err = registration.Commit(); err != nil {
				t.Fatal(err)
			}
			if err = <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDurableOutboxConflictingRegistrationRace(t *testing.T) {
	s := setupTestStore(t)
	ctx := t.Context()
	a := auditConfig(t.Name())
	b := a
	b.TenantID = "other"
	done := make(chan error, 2)
	for _, config := range []durable.NamespaceConfig{a, b} {
		go func() { _, err := s.RegisterNamespace(ctx, config); done <- err }()
	}
	success, conflict := 0, 0
	for range 2 {
		err := <-done
		switch {
		case err == nil:
			success++
		case errors.Is(err, durable.ErrRequestConflict):
			conflict++
		default:
			t.Fatal(err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("registration winners: %d conflicts: %d", success, conflict)
	}
}
