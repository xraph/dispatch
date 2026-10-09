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

func childCreationRequest(t *testing.T, s *postgres.Store, ttl time.Duration) (durable.StartRequest, *durable.Task, durable.CommitRequest) {
	t.Helper()
	r := signalStartRequest(t)
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	source, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: r.Queue, Kind: durable.TaskWorkflow, Owner: "parent", LeaseDuration: ttl})
	if err != nil || source == nil {
		t.Fatalf("parent task: %+v %v", source, err)
	}
	child := durable.ChildStartSpec{CommandID: "child", Start: durable.StartRequest{Key: durable.Key{Namespace: r.Namespace, WorkflowID: "child", RunID: "child-run"}, RequestID: "child-start", WorkflowType: "child", BuildID: "child-v1", Queue: "children", Input: []byte("input")}, ParentQueue: r.Queue, ParentClosePolicy: durable.ParentCloseTerminate}
	request := durable.CommitRequest{Key: r.Key, RequestID: "children", ExpectedRevision: 1, Token: source.Token(), Events: []durable.EventInput{{Type: "decision"}}, Children: []durable.ChildStartSpec{child}, Tasks: []durable.TaskSpec{{ID: "parent-next", Kind: durable.TaskWorkflow, Queue: r.Queue}}}
	return r, source, request
}

type lostChildCreationStore struct {
	durable.Store
	receipt durable.Receipt
}

var errChildCreationLost = errors.New("child creation response lost")

func (s *lostChildCreationStore) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
	receipt, err := s.Store.CommitTransition(ctx, r)
	if err != nil {
		return receipt, err
	}
	s.receipt = receipt
	return durable.Receipt{}, errChildCreationLost
}

func TestDurableChildReceiptRecovery(t *testing.T) {
	s, dsn := setupTestStoreConnection(t)
	r, _, request := childCreationRequest(t, s, time.Minute)
	lost := &lostChildCreationStore{Store: s}
	if _, err := lost.CommitTransition(t.Context(), request); !errors.Is(err, errChildCreationLost) {
		t.Fatalf("missing loss: %v", err)
	}
	s = reopenAsyncStore(t, s, dsn)
	for _, start := range []durable.StartRequest{r, request.Children[0].Start} {
		task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: start.Namespace, Queue: start.Queue, Kind: durable.TaskWorkflow, BuildID: start.BuildID, Owner: "closer", LeaseDuration: time.Minute})
		if err != nil || task == nil {
			t.Fatalf("close claim: %+v %v", task, err)
		}
		e, err := s.GetExecution(t.Context(), start.Key)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.CommitTransition(t.Context(), durable.CommitRequest{Key: start.Key, RequestID: "close", Token: task.Token(), ExpectedRevision: e.Revision, State: durable.StateCompleted, Events: []durable.EventInput{{Type: "closed"}}}); err != nil {
			t.Fatal(err)
		}
		next := start
		next.RunID += "-replacement"
		next.RequestID = "replacement"
		if _, err = s.StartExecution(t.Context(), next); err != nil {
			t.Fatal(err)
		}
	}
	s = reopenAsyncStore(t, s, dsn)
	if got, err := s.CommitTransition(t.Context(), request); err != nil || got != lost.receipt {
		t.Fatalf("recovered receipt: %+v %v", got, err)
	}
	link, err := s.GetChildExecution(t.Context(), r.Key, "child")
	if err != nil || link.Start.Key != request.Children[0].Start.Key || link.State != durable.StateCompleted {
		t.Fatalf("retargeted child: %+v %v", link, err)
	}
}

func TestDurableChildRollback(t *testing.T) {
	for _, kind := range []string{"relationship", "receipt"} {
		t.Run(kind, func(t *testing.T) {
			s := setupTestStore(t)
			r, source, request := childCreationRequest(t, s, time.Minute)
			pg := pgdriver.Unwrap(s.DB())
			table, condition := "dispatch_child_executions", "TRUE"
			if kind == "receipt" {
				table, condition = "dispatch_execution_receipts", "NEW.request_id='children'"
			}
			_, err := pg.Exec(t.Context(), fmt.Sprintf(`CREATE FUNCTION reject_child_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF %s THEN RAISE EXCEPTION 'injected child write failure'; END IF; RETURN NEW; END $$;
 CREATE TRIGGER reject_child_write BEFORE INSERT ON %s FOR EACH ROW EXECUTE FUNCTION reject_child_write()`, condition, table))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.CommitTransition(t.Context(), request); err == nil || !strings.Contains(err.Error(), "injected child write failure") {
				t.Fatalf("missing rollback fault: %v", err)
			}
			child := request.Children[0].Start.Key
			if _, err = s.GetExecution(t.Context(), child); !errors.Is(err, durable.ErrNotFound) {
				t.Fatalf("partial child: %v", err)
			}
			if _, err = s.GetChildExecution(t.Context(), r.Key, "child"); !errors.Is(err, durable.ErrNotFound) {
				t.Fatalf("partial relationship: %v", err)
			}
			e, err := s.GetExecution(t.Context(), r.Key)
			if err != nil || e.Revision != 1 || e.LastSequence != 1 {
				t.Fatalf("partial parent: %+v %v", e, err)
			}
			after, err := s.GetTask(t.Context(), r.Key, source.ID)
			if err != nil || after.Done || after.Version != source.Version {
				t.Fatalf("partial source: %+v %v", after, err)
			}
			if _, err = s.GetTask(t.Context(), r.Key, "parent-next"); !errors.Is(err, durable.ErrNotFound) {
				t.Fatalf("partial wakeup: %v", err)
			}
			if _, err = pg.Exec(t.Context(), "DROP TRIGGER reject_child_write ON "+table); err != nil {
				t.Fatal(err)
			}
			if _, err = s.CommitTransition(t.Context(), request); err != nil {
				t.Fatalf("retry after rollback: %v", err)
			}
		})
	}
}

func TestDurableChildMigration(t *testing.T) {
	s := setupTestStore(t)
	pg := pgdriver.Unwrap(s.DB())
	executor, err := migrate.NewExecutorFor(pg)
	if err != nil {
		t.Fatal(err)
	}
	var migration *migrate.Migration
	for _, candidate := range postgres.Migrations.Migrations() {
		if candidate.Version == "20261018120000" {
			migration = candidate
		}
	}
	if migration == nil {
		t.Fatal("child migration missing")
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
	r, _, request := childCreationRequest(t, s, time.Minute)
	receipt, err := s.CommitTransition(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pg.Exec(t.Context(), `DELETE FROM grove_migrations WHERE version=$1`, "20261018120000"); err != nil {
		t.Fatal(err)
	}
	if err = s.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = migration.Down(t.Context(), executor); err == nil || !strings.Contains(err.Error(), "retained child relationships prevent downgrade") {
		t.Fatalf("unsafe downgrade: %v", err)
	}
	if _, err = s.GetChildExecution(t.Context(), r.Key, "child"); err != nil {
		t.Fatal(err)
	}
	if again, retryErr := s.CommitTransition(t.Context(), request); retryErr != nil || again != receipt {
		t.Fatalf("migration retry: %+v %v", again, retryErr)
	}
}

func TestDurableChildIdentityLockExpiry(t *testing.T) {
	s := setupTestStore(t)
	r, source, request := childCreationRequest(t, s, 300*time.Millisecond)
	pg := pgdriver.Unwrap(s.DB())
	lock, err := pg.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback() }()
	identity := fmt.Sprintf("dispatch.signal/%q/%q", request.Children[0].Start.Namespace, request.Children[0].Start.WorkflowID)
	if _, err = lock.Exec(t.Context(), `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, identity); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, commitErr := s.CommitTransition(ctx, request); done <- commitErr }()
	for {
		var waiting bool
		if err = pg.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%pg_advisory_xact_lock%')`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case earlyErr := <-done:
			t.Fatalf("did not wait for identity: %v", earlyErr)
		case <-ctx.Done():
			t.Fatal("identity wait not observed")
		case <-time.After(5 * time.Millisecond):
		}
	}
	waitDurableStoreTime(t, s, source.LeaseUntil)
	if err = lock.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if !errors.Is(err, durable.ErrLeaseLost) {
			t.Fatalf("expired source: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("commit did not finish")
	}
	if _, err = s.GetExecution(t.Context(), request.Children[0].Start.Key); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("expired child persisted: %v", err)
	}
	e, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || e.Revision != 1 || e.LastSequence != 1 {
		t.Fatalf("expired parent mutated: %+v %v", e, err)
	}
}

func TestDurableChildReceiptIgnoresTargetLocks(t *testing.T) {
	s := setupTestStore(t)
	_, _, request := childCreationRequest(t, s, time.Minute)
	receipt, err := s.CommitTransition(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	pg := pgdriver.Unwrap(s.DB())
	lock, err := pg.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback() }()
	identity := fmt.Sprintf("dispatch.signal/%q/%q", request.Children[0].Start.Namespace, request.Children[0].Start.WorkflowID)
	if _, err = lock.Exec(t.Context(), `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, identity); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	got, err := s.CommitTransition(ctx, request)
	if err != nil || got != receipt {
		t.Fatalf("accepted receipt waited on unrelated child ownership: %+v %v", got, err)
	}
}
