package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xraph/dispatch/durable"

	"github.com/xraph/dispatch/durable/durabletest"
)

func TestQueryRuntimeRetention(t *testing.T) {
	s, _, _, _ := retirementFixture(t)
	durabletest.RunQueryRuntimeRetention(t, s)
}

func TestQueryAbortProofExpiresWhileWaiting(t *testing.T) {
	s, dsn, enroll, start := retirementFixture(t)
	if _, err := s.EnrollRetirement(t.Context(), enroll); err != nil {
		t.Fatal(err)
	}
	target := durable.BuildTarget{NamespaceTarget: enroll.NamespaceTarget, BuildID: start.BuildID}
	identity := durabletest.QueryIdentityFixture()
	if _, err := s.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: target, RequestID: "identity", ExpectedVersion: 1, Identity: &identity}); err != nil {
		t.Fatal(err)
	}
	bindings := make([]durable.QueryRuntimeBinding, 2)
	for i, id := range []string{"a", "b"} {
		runtime := durable.QueryRuntimeIdentity{QueryRuntimeTarget: durable.QueryRuntimeTarget{BuildTarget: target, RuntimeID: id}, InstanceID: "host-" + id, IdentityVersion: 1, BuildIdentity: identity}
		if _, err := s.RegisterQueryRuntime(t.Context(), durable.RegisterQueryRuntimeRequest{Identity: runtime, RequestID: "register-" + id}); err != nil {
			t.Fatal(err)
		}
		receipt, err := s.RecordQueryRuntimeVerification(t.Context(), durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: runtime.QueryRuntimeTarget, RequestID: "verify-" + id, ExpectedVersion: 1, Verification: durabletest.QueryProofFixture(runtime, "proof-"+id)})
		if err != nil {
			t.Fatal(err)
		}
		bindings[i] = *receipt.QueryRuntime
	}
	reservation, err := s.BeginQueryRuntimeRemoval(t.Context(), durable.BeginQueryRemovalRequest{QueryRuntimeTarget: bindings[0].QueryRuntimeTarget, RequestID: "remove", ExpectedVersion: bindings[0].Version})
	if err != nil {
		t.Fatal(err)
	}
	fence := reservation.QueryRuntime.Removal
	settlement := durabletest.QuerySettlementFixture(t, fence)
	proof := durabletest.QueryProofFixture(fence.Candidate, "expires")
	proof.ValidUntil = proof.VerifiedAt.Add(time.Second)
	abort := durable.AbortQueryRemovalRequest{Fence: fence, Settlement: settlement, VerifyQueryRuntimeRequest: durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: fence.Candidate.QueryRuntimeTarget, RequestID: "abort", ExpectedVersion: fence.CandidateStateVersion, Verification: proof}}
	gate, observe := retirementConn(t, dsn), retirementConn(t, dsn)
	tx, err := gate.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(t.Context(), `SELECT dispatch_retirement_coordinator_lock($1,1)`, target.Namespace); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, abortErr := s.AbortQueryRuntimeRemoval(t.Context(), abort); result <- abortErr }()
	queryWaitNamespace(t, observe, target.Namespace)
	if !proof.ValidUntil.After(time.Now()) {
		t.Fatal("fixture proof expired before confirmed coordination wait")
	}
	waitProtocolProof(t, proof.ValidUntil)
	if err = tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = <-result; !errors.Is(err, durable.ErrQueryRetention) {
		t.Fatalf("expired proof accepted after lock wait: %v", err)
	}
	page, err := s.ListQueryRuntimes(t.Context(), durable.QueryRuntimeList{NamespaceTarget: target.NamespaceTarget, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if page.Items[0].State != durable.QueryRuntimeRemoving || page.Items[0].Version != fence.CandidateStateVersion {
		t.Fatal("expired abort changed binding")
	}
	if _, err = s.LookupLifecycleReceipt(t.Context(), durable.LifecycleReceiptLookup{NamespaceTarget: target.NamespaceTarget, Operation: durable.OperationAbortQueryRemoval, RequestID: abort.RequestID, CommandDigest: strings.Repeat("a", 64)}); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("expired abort wrote receipt: %v", err)
	}
}

func queryWaitNamespace(t *testing.T, c *pgx.Conn, ns string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	for {
		var waiting bool
		if err := c.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND classid=((dispatch_audit_lock_key($1)>>32)&4294967295)::oid AND objid=(dispatch_audit_lock_key($1)&4294967295)::oid AND objsubid=1)`, ns).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("namespace lock wait not reached")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestQueryRowGuardRefusesExclusiveUpgrade(t *testing.T) {
	s, dsn, enroll, start := retirementFixture(t)
	if _, err := s.EnrollRetirement(t.Context(), enroll); err != nil {
		t.Fatal(err)
	}
	c := retirementConn(t, dsn)
	tx, err := c.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(t.Context(), `SELECT dispatch_retirement_writer_lock($1,1)`, enroll.Namespace); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(t.Context(), `SELECT 1 FROM dispatch_build_lifecycle WHERE namespace=$1 AND build_id=$2 FOR UPDATE`, enroll.Namespace, start.BuildID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err = tx.Exec(ctx, `UPDATE dispatch_build_lifecycle SET state='retired' WHERE namespace=$1 AND build_id=$2`, enroll.Namespace, start.BuildID)
	retirementSQLState(t, err, "DL002")
}

func TestQueryRequiredIntentRollback(t *testing.T) {
	s, dsn, enroll, start := retirementFixture(t)
	if _, err := s.EnrollRetirement(t.Context(), enroll); err != nil {
		t.Fatal(err)
	}
	target := durable.BuildTarget{NamespaceTarget: enroll.NamespaceTarget, BuildID: start.BuildID}
	identity := durabletest.QueryIdentityFixture()
	if _, err := s.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: target, RequestID: "identity", ExpectedVersion: 1, Identity: &identity}); err != nil {
		t.Fatal(err)
	}
	c := retirementConn(t, dsn)
	_, err := c.Exec(t.Context(), `CREATE FUNCTION dispatch_test_query_intent_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.envelope->>'Action' LIKE 'dispatch.query_runtime.%' THEN RAISE EXCEPTION 'injected query intent failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER dispatch_test_query_intent_failure BEFORE INSERT ON dispatch_durable_outbox FOR EACH ROW EXECUTE FUNCTION dispatch_test_query_intent_failure()`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, cleanupErr := c.Exec(ctx, `DROP TRIGGER IF EXISTS dispatch_test_query_intent_failure ON dispatch_durable_outbox; DROP FUNCTION IF EXISTS dispatch_test_query_intent_failure()`); cleanupErr != nil {
			t.Error(cleanupErr)
		}
	})
	r := durable.RegisterQueryRuntimeRequest{Identity: durable.QueryRuntimeIdentity{QueryRuntimeTarget: durable.QueryRuntimeTarget{BuildTarget: target, RuntimeID: "runtime"}, InstanceID: "host", IdentityVersion: 1, BuildIdentity: identity}, RequestID: "register"}
	if _, err = s.RegisterQueryRuntime(t.Context(), r); err == nil {
		t.Fatal("missing required query intent accepted")
	}
	var count int
	if err = c.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM dispatch_query_runtimes WHERE namespace=$1)+(SELECT count(*) FROM dispatch_lifecycle_receipts WHERE namespace=$1 AND operation='query_runtime.register')`, enroll.Namespace).Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial query registration: %d %v", count, err)
	}
}

func TestQueryEmptyRetiredLastBinding(t *testing.T) {
	s, _, _, _ := retirementFixture(t)
	durabletest.RunQueryEmptyRetiredLastBinding(t, s)
}
