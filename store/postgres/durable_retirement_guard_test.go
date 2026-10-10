package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xraph/dispatch/durable"
	pgstore "github.com/xraph/dispatch/store/postgres"
)

func retirementFixture(t *testing.T) (*pgstore.Store, string, durable.RetirementEnrollmentRequest, durable.StartRequest) {
	t.Helper()
	dsn := os.Getenv("DISPATCH_LIFECYCLE_TEST_DSN")
	if dsn == "" {
		if os.Getenv("DISPATCH_LIFECYCLE_REQUIRED") == "1" {
			t.Fatal("DISPATCH_LIFECYCLE_TEST_DSN required")
		}
		t.Skip("dedicated PostgreSQL fixture required")
	}
	s := openWakeStore(t, dsn)
	ns := fmt.Sprintf("guard-%d", time.Now().UnixNano())
	n := durable.NamespaceConfig{InstallationID: "i", Namespace: ns, AppID: "a", TenantID: "t", RequireAudit: true, SchemaVersion: 1}
	if _, err := s.RegisterNamespace(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	start := durable.StartRequest{Key: durable.Key{Namespace: ns, WorkflowID: "w", RunID: "r"}, RequestID: "start", WorkflowType: "wf", BuildID: "old", Queue: "q"}
	if _, err := s.StartExecution(t.Context(), start); err != nil {
		t.Fatal(err)
	}
	return s, dsn, durable.RetirementEnrollmentRequest{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: ns}, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}, start
}

func retirementConn(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	c, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}
func retirementSQLState(t *testing.T, err error, want string) {
	t.Helper()
	var state interface{ SQLState() string }
	if !errors.As(err, &state) || state.SQLState() != want {
		t.Fatalf("SQLSTATE want %s: %v", want, err)
	}
}
func retirementWaitLock(t *testing.T, c *pgx.Conn, pid uint32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	for {
		var waiting bool
		if err := c.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND NOT granted)`, pid).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("lock wait not reached")
		case <-time.After(time.Millisecond):
		}
	}
}

// SQL fixtures exercise trigger ordering. They are not native old-binary proof.
func TestRetirementWriterGuardAndMarker(t *testing.T) {
	s, dsn, r, start := retirementFixture(t)
	c := retirementConn(t, dsn)
	if _, err := c.Exec(t.Context(), `UPDATE dispatch_execution_tasks SET version=version+1 WHERE namespace=$1`, r.Namespace); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnrollRetirement(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{"", "0", "malformed", "2"} {
		tx, err := c.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(t.Context(), `SELECT set_config('dispatch.retirement_writer_protocol',$1,TRUE)`, marker); err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(t.Context(), `UPDATE dispatch_execution_tasks SET version=version+1 WHERE namespace=$1`, r.Namespace)
		retirementSQLState(t, err, "DL001")
		_ = tx.Rollback(t.Context())
	}
	for _, commit := range []bool{true, false} {
		tx, err := c.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(t.Context(), `SELECT dispatch_retirement_writer_lock($1,1)`, r.Namespace); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(t.Context(), `UPDATE dispatch_execution_tasks SET version=version+1 WHERE namespace=$1`, r.Namespace); err != nil {
			t.Fatal(err)
		}
		if commit {
			err = tx.Commit(t.Context())
		} else {
			err = tx.Rollback(t.Context())
		}
		if err != nil {
			t.Fatal(err)
		}
		var marker string
		if err = c.QueryRow(t.Context(), `SELECT COALESCE(current_setting('dispatch.retirement_writer_protocol',TRUE),'')`).Scan(&marker); err != nil || marker != "" {
			t.Fatalf("marker leaked: %q %v", marker, err)
		}
	}
	// Capable exact start replay returns its original receipt after activation.
	if receipt, err := s.StartExecution(t.Context(), start); err != nil || receipt.Revision != 1 {
		t.Fatalf("capable replay: %+v %v", receipt, err)
	}
	task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: "q", BuildID: "old", Kind: durable.TaskWorkflow, Owner: "capable", LeaseDuration: time.Second})
	if err != nil || task == nil {
		t.Fatalf("capable claim: %+v %v", task, err)
	}
	if _, err = s.RenewTask(t.Context(), start.Key, task.Token(), time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestRetirementFallbackAvoidsThreePartyDeadlock(t *testing.T) {
	_, dsn, r, _ := retirementFixture(t)
	old, current, coordinator, observe := retirementConn(t, dsn), retirementConn(t, dsn), retirementConn(t, dsn), retirementConn(t, dsn)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	oldtx, err := old.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer oldtx.Rollback(context.Background())
	if _, err = oldtx.Exec(ctx, `SELECT task_id FROM dispatch_execution_tasks WHERE namespace=$1 FOR UPDATE`, r.Namespace); err != nil {
		t.Fatal(err)
	}
	currenttx, err := current.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer currenttx.Rollback(context.Background())
	if _, err = currenttx.Exec(ctx, `SELECT dispatch_retirement_writer_lock($1,1)`, r.Namespace); err != nil {
		t.Fatal(err)
	}
	currentDone := make(chan error, 1)
	go func() {
		_, e := currenttx.Exec(ctx, `UPDATE dispatch_execution_tasks SET version=version+1 WHERE namespace=$1`, r.Namespace)
		currentDone <- e
	}()
	retirementWaitLock(t, observe, current.PgConn().PID())
	coordinatorDone := make(chan error, 1)
	go func() {
		_, e := coordinator.Exec(ctx, `SELECT dispatch_retirement_coordinator_lock($1,1)`, r.Namespace)
		coordinatorDone <- e
	}()
	retirementWaitLock(t, observe, coordinator.PgConn().PID())
	_, err = oldtx.Exec(ctx, `UPDATE dispatch_execution_tasks SET version=version+1 WHERE namespace=$1`, r.Namespace)
	retirementSQLState(t, err, "DL002")
	if err = oldtx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-currentDone; err != nil {
		t.Fatal(err)
	}
	if err = currenttx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-coordinatorDone; err != nil {
		t.Fatal(err)
	}
}

func TestRetirementFallbackReadsFloorAfterOuterStatementSnapshot(t *testing.T) {
	s, dsn, r, _ := retirementFixture(t)
	gate, old, observe := retirementConn(t, dsn), retirementConn(t, dsn), retirementConn(t, dsn)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	// A separate test lock pauses the outer UPDATE in a BEFORE trigger. It has
	// already selected its row and snapshot, but has not run the floor guard.
	if _, err := gate.Exec(ctx, `SELECT pg_advisory_lock(819734019)`); err != nil {
		t.Fatal(err)
	}
	defer gate.Exec(context.Background(), `SELECT pg_advisory_unlock(819734019)`)
	_, err := gate.Exec(ctx, `CREATE FUNCTION dispatch_test_retirement_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.namespace=current_setting('dispatch.test_barrier_namespace',TRUE) THEN PERFORM pg_advisory_xact_lock(819734019); END IF; RETURN NEW; END $$; CREATE TRIGGER a_dispatch_test_retirement_barrier BEFORE UPDATE ON dispatch_execution_tasks FOR EACH ROW EXECUTE FUNCTION dispatch_test_retirement_barrier()`)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Exec(context.Background(), `DROP TRIGGER a_dispatch_test_retirement_barrier ON dispatch_execution_tasks; DROP FUNCTION dispatch_test_retirement_barrier()`)
	if _, err = old.Exec(ctx, `SELECT set_config('dispatch.test_barrier_namespace',$1,FALSE)`, r.Namespace); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, e := old.Exec(ctx, `UPDATE dispatch_execution_tasks SET version=version+1 WHERE namespace=$1`, r.Namespace)
		done <- e
	}()
	retirementWaitLock(t, observe, old.PgConn().PID())
	if _, err = s.EnrollRetirement(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, err = gate.Exec(ctx, `SELECT pg_advisory_unlock(819734019)`); err != nil {
		t.Fatal(err)
	}
	retirementSQLState(t, <-done, "DL001")
}

func TestRetirementEnrollmentRequiredIntentRollback(t *testing.T) {
	s, dsn, r, _ := retirementFixture(t)
	c := retirementConn(t, dsn)
	_, err := c.Exec(t.Context(), `CREATE FUNCTION dispatch_test_lifecycle_intent_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.envelope->>'Action'='dispatch.retirement.enroll' THEN RAISE EXCEPTION 'injected intent failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER dispatch_test_lifecycle_intent_failure BEFORE INSERT ON dispatch_durable_outbox FOR EACH ROW EXECUTE FUNCTION dispatch_test_lifecycle_intent_failure()`)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Exec(context.Background(), `DROP TRIGGER IF EXISTS dispatch_test_lifecycle_intent_failure ON dispatch_durable_outbox; DROP FUNCTION IF EXISTS dispatch_test_lifecycle_intent_failure()`)
	if _, err = s.EnrollRetirement(t.Context(), r); err == nil {
		t.Fatal("injected failure accepted")
	}
	var count int
	if err = c.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM dispatch_retirement_namespaces WHERE namespace=$1)+(SELECT count(*) FROM dispatch_build_lifecycle WHERE namespace=$1)+(SELECT count(*) FROM dispatch_lifecycle_receipts WHERE namespace=$1)`, r.Namespace).Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial enrollment: %d %v", count, err)
	}
	if _, err = c.Exec(t.Context(), `DROP TRIGGER dispatch_test_lifecycle_intent_failure ON dispatch_durable_outbox; DROP FUNCTION dispatch_test_lifecycle_intent_failure()`); err != nil {
		t.Fatal(err)
	}
	if _, err = s.EnrollRetirement(t.Context(), r); err != nil {
		t.Fatal(err)
	}
}

func TestRetirementBuildIntentRollback(t *testing.T) {
	s, dsn, enrollment, _ := retirementFixture(t)
	if _, err := s.EnrollRetirement(t.Context(), enrollment); err != nil {
		t.Fatal(err)
	}
	target := durable.BuildTarget{NamespaceTarget: enrollment.NamespaceTarget, BuildID: "empty"}
	registered, err := s.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: target, RequestID: "register"})
	if err != nil {
		t.Fatal(err)
	}
	c := retirementConn(t, dsn)
	install := func() {
		t.Helper()
		_, installErr := c.Exec(t.Context(), `CREATE FUNCTION dispatch_test_build_intent_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.envelope->>'Action' LIKE 'dispatch.build.%' THEN RAISE EXCEPTION 'injected build intent failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER dispatch_test_build_intent_failure BEFORE INSERT ON dispatch_durable_outbox FOR EACH ROW EXECUTE FUNCTION dispatch_test_build_intent_failure()`)
		if installErr != nil {
			t.Fatal(installErr)
		}
	}
	remove := func() {
		t.Helper()
		if _, removeErr := c.Exec(t.Context(), `DROP TRIGGER IF EXISTS dispatch_test_build_intent_failure ON dispatch_durable_outbox; DROP FUNCTION IF EXISTS dispatch_test_build_intent_failure()`); removeErr != nil {
			t.Fatal(removeErr)
		}
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, cleanupErr := c.Exec(ctx, `DROP TRIGGER IF EXISTS dispatch_test_build_intent_failure ON dispatch_durable_outbox; DROP FUNCTION IF EXISTS dispatch_test_build_intent_failure()`); cleanupErr != nil {
			t.Error(cleanupErr)
		}
	})
	install()
	missing := target
	missing.BuildID = "refused-registration"
	if _, err = s.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: missing, RequestID: "refused"}); err == nil {
		t.Fatal("registration intent failure accepted")
	}
	if _, err = s.InspectBuildLifecycle(t.Context(), missing); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("registration leaked: %v", err)
	}
	begin := durable.BuildRetirementRequest{BuildTarget: target, RequestID: "begin", ExpectedEpoch: 1, ExpectedVersion: 1}
	if _, err = s.BeginBuildRetirement(t.Context(), begin); err == nil {
		t.Fatal("begin intent failure accepted")
	}
	facts, err := s.InspectBuildLifecycle(t.Context(), target)
	if err != nil || facts.Admission != *registered.Build {
		t.Fatalf("begin leaked: %+v %v", facts, err)
	}
	remove()
	retiring, err := s.BeginBuildRetirement(t.Context(), begin)
	if err != nil {
		t.Fatal(err)
	}
	install()
	final := durable.BuildRetirementRequest{BuildTarget: target, RequestID: "final", ExpectedEpoch: 2, ExpectedVersion: 2}
	if _, err = s.FinalizeBuildRetirement(t.Context(), final); err == nil {
		t.Fatal("final intent failure accepted")
	}
	facts, err = s.InspectBuildLifecycle(t.Context(), target)
	if err != nil || facts.Admission != *retiring.Build {
		t.Fatalf("final leaked: %+v %v", facts, err)
	}
	remove()
	if _, err = s.FinalizeBuildRetirement(t.Context(), final); err != nil {
		t.Fatal(err)
	}
}
