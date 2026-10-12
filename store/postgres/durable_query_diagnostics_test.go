package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/grove"
	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/durabletest"
)

func queryDiagnosticFixture(t *testing.T) (*Store, durable.VerifyQueryRuntimeRequest) {
	t.Helper()
	dsn := os.Getenv("DISPATCH_LIFECYCLE_TEST_DSN")
	if dsn == "" {
		if os.Getenv("DISPATCH_LIFECYCLE_REQUIRED") == "1" {
			t.Fatal("dedicated PostgreSQL required")
		}
		t.Skip("dedicated PostgreSQL required")
	}
	drv := pgdriver.New()
	if err := drv.Open(t.Context(), dsn); err != nil {
		t.Fatal("fixture database unavailable")
	}
	db, err := grove.Open(drv)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s := New(db)
	if err = s.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	ns := fmt.Sprintf("diagnostic-%d", time.Now().UnixNano())
	target := durable.NamespaceTarget{InstallationID: "i", Namespace: ns}
	if _, err = s.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: "i", Namespace: ns, AppID: "a", TenantID: "t", RequireAudit: true, SchemaVersion: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: target, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
		t.Fatal(err)
	}
	identity := durable.QueryRuntimeIdentity{QueryRuntimeTarget: durable.QueryRuntimeTarget{BuildTarget: durable.BuildTarget{NamespaceTarget: target, BuildID: "b"}, RuntimeID: "runtime"}, InstanceID: "physical", IdentityVersion: 1, BuildIdentity: durabletest.QueryIdentityFixture()}
	if _, err = s.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: identity.BuildTarget, RequestID: "build", Identity: &identity.BuildIdentity}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.RegisterQueryRuntime(t.Context(), durable.RegisterQueryRuntimeRequest{Identity: identity, RequestID: "register"}); err != nil {
		t.Fatal(err)
	}
	proof := durabletest.QueryProofFixture(identity, "proof")
	proof.VerifiedAt = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	proof.ValidUntil = proof.VerifiedAt.Add(time.Minute)
	return s, durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: identity.QueryRuntimeTarget, RequestID: "verify", ExpectedVersion: 1, Verification: proof}
}
func queryDiagnosticSnapshot(t *testing.T, s *Store, namespace string) string {
	t.Helper()
	var state string
	err := s.pgdb.QueryRow(t.Context(), `SELECT json_build_array((SELECT json_agg(binding ORDER BY runtime_id) FROM dispatch_query_runtimes WHERE namespace=$1),(SELECT count(*) FROM dispatch_lifecycle_receipts WHERE namespace=$1),(SELECT count(*) FROM dispatch_durable_outbox WHERE namespace=$1))::text`, namespace).Scan(&state)
	if err != nil {
		t.Fatal(err)
	}
	return state
}
func fixedQuerySample(at time.Time) queryAcceptanceSample {
	return func(context.Context, driver.Tx) (time.Time, error) { return at, nil }
}

func TestQueryDiagnosticPostgresBoundariesAndReplay(t *testing.T) {
	s, r := queryDiagnosticFixture(t)
	at := r.Verification.VerifiedAt
	before := queryDiagnosticSnapshot(t, s, r.Namespace)
	for _, kind := range []string{"future", "expired", "overlong"} {
		bad := r
		bad.RequestID = kind
		switch kind {
		case "future":
			bad.Verification.VerifiedAt = at.Add(time.Microsecond)
		case "expired":
			bad.Verification.ValidUntil = at
		case "overlong":
			bad.Verification.ValidUntil = at.Add(time.Minute + time.Microsecond)
		}
		_, err := s.recordQueryRuntimeVerification(t.Context(), bad, fixedQuerySample(at))
		d, ok := durable.QueryRejectionDetails(err)
		if !errors.Is(err, durable.ErrQueryRetention) || !ok || d.ObservedAt != at || d.VerifiedAt != bad.Verification.VerifiedAt || d.RequestID != kind {
			t.Fatalf("predicate/sample lost: %+v %v", d, err)
		}
		if queryDiagnosticSnapshot(t, s, r.Namespace) != before {
			t.Fatal("refused proof mutated binding, receipt or required outbox")
		}
	}
	accepted, err := s.recordQueryRuntimeVerification(t.Context(), r, fixedQuerySample(at))
	if err != nil {
		t.Fatal(err)
	}
	// The replay lookup must precede both current sampling and current expiry checks.
	replay, err := s.recordQueryRuntimeVerification(t.Context(), r, func(context.Context, driver.Tx) (time.Time, error) {
		t.Error("accepted replay sampled current clock")
		return at.Add(time.Hour), nil
	})
	if err != nil || !reflect.DeepEqual(accepted, replay) || replay.AcceptedAt != at {
		t.Fatalf("accepted replay changed: %v", err)
	}
}

func TestQueryDiagnosticPostgresExpiryAfterNamespaceWait(t *testing.T) {
	s, r := queryDiagnosticFixture(t)
	before := queryDiagnosticSnapshot(t, s, r.Namespace)
	gate, err := s.pgdb.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = gate.Rollback() }()
	if _, err = gate.Exec(t.Context(), `SELECT dispatch_retirement_coordinator_lock($1,1)`, r.Namespace); err != nil {
		t.Fatal(err)
	}
	var sample atomic.Int64
	sample.Store(r.Verification.VerifiedAt.UnixMicro())
	result := make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		_, e := s.recordQueryRuntimeVerification(ctx, r, func(context.Context, driver.Tx) (time.Time, error) { return time.UnixMicro(sample.Load()).UTC(), nil })
		result <- e
	}()
	joined := false
	defer func() {
		cancel()
		_ = gate.Rollback()
		if !joined {
			<-result
		}
	}()
	wait, stop := context.WithTimeout(t.Context(), 3*time.Second)
	defer stop()
	for {
		var waiting bool
		if err = s.pgdb.QueryRow(wait, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND classid=((dispatch_audit_lock_key($1)>>32)&4294967295)::oid AND objid=(dispatch_audit_lock_key($1)&4294967295)::oid AND objsubid=1)`, r.Namespace).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-wait.Done():
			t.Fatal("namespace wait not observed")
		case <-time.After(time.Millisecond):
		}
	}
	sample.Store(r.Verification.ValidUntil.UnixMicro())
	if err = gate.Commit(); err != nil {
		t.Fatal(err)
	}
	err = <-result
	joined = true
	d, ok := durable.QueryRejectionDetails(err)
	if !errors.Is(err, durable.ErrQueryRetention) || !ok || d.Reason != "expired" || d.ObservedAt != r.Verification.ValidUntil {
		t.Fatalf("post-lock sample not used: %+v %v", d, err)
	}
	if queryDiagnosticSnapshot(t, s, r.Namespace) != before {
		t.Fatal("expired proof changed durable state")
	}
}

func TestQueryDiagnosticPostgresSQLStateBeforeNormalization(t *testing.T) {
	s, r := queryDiagnosticFixture(t)
	before := queryDiagnosticSnapshot(t, s, r.Namespace)
	// A namespace-specific test trigger corrupts the row before the real guard runs.
	if _, err := s.pgdb.Exec(t.Context(), `CREATE OR REPLACE FUNCTION dispatch_test_query_diagnostic() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.namespace=TG_ARGV[0] THEN NEW.instance_id:='other'; END IF; RETURN NEW; END $$`); err != nil {
		t.Fatal(err)
	}
	trigger := "a_query_diagnostic"
	if _, err := s.pgdb.Exec(t.Context(), fmt.Sprintf(`CREATE TRIGGER %s BEFORE INSERT OR UPDATE ON dispatch_query_runtimes FOR EACH ROW EXECUTE FUNCTION dispatch_test_query_diagnostic('%s')`, trigger, r.Namespace)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.pgdb.Exec(context.Background(), `DROP TRIGGER IF EXISTS a_query_diagnostic ON dispatch_query_runtimes`)
		_, _ = s.pgdb.Exec(context.Background(), `DROP FUNCTION IF EXISTS dispatch_test_query_diagnostic()`)
	})
	_, err := s.recordQueryRuntimeVerification(t.Context(), r, fixedQuerySample(r.Verification.VerifiedAt))
	d, ok := durable.QueryRejectionDetails(err)
	if !errors.Is(err, durable.ErrQueryRetention) || !ok || d.SQLState != "DL004" || d.Reason != "binding_row" || d.ObservedAt != r.Verification.VerifiedAt {
		t.Fatalf("guard classification lost: %+v %v", d, err)
	}
	if err.Error() != durable.ErrQueryRetention.Error() {
		t.Fatal("raw database details in public error")
	}
	if queryDiagnosticSnapshot(t, s, r.Namespace) != before {
		t.Fatal("guard refusal changed durable state")
	}
}
