package postgres_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/durabletest"
)

func TestQueryReceiptRecoveryEvidence(t *testing.T) {
	s, dsn, _, _ := retirementFixture(t)
	c := retirementConn(t, dsn)
	durabletest.RunQueryReceiptRecovery(t, s, func(t *testing.T, r durable.LifecycleReceipt) {
		t.Helper()
		data, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		tx, err := c.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(context.Background())
		if _, err = tx.Exec(t.Context(), `SELECT dispatch_retirement_coordinator_lock($1,1)`, r.Namespace); err != nil {
			t.Fatal(err)
		}
		// Deliberately corrupt this dedicated fixture's stored payload. Production
		// receipt immutability stays enabled before and after the atomic replacement.
		if _, err = tx.Exec(t.Context(), `ALTER TABLE dispatch_lifecycle_receipts DISABLE TRIGGER dispatch_lifecycle_immutable`); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(t.Context(), `UPDATE dispatch_lifecycle_receipts SET response=$4 WHERE namespace=$1 AND operation=$2 AND request_id=$3`, r.Namespace, string(r.Operation), r.RequestID, data); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(t.Context(), `ALTER TABLE dispatch_lifecycle_receipts ENABLE TRIGGER dispatch_lifecycle_immutable`); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}

// The mutex establishes Go happens-before ordering around the caller mutation.
// PostgreSQL's observed advisory wait supplies the real coordination barrier.
type queryCaptureContext struct {
	context.Context
	mu            sync.Mutex
	once          sync.Once
	captured      chan struct{}
	changed       bool
	observedAfter bool
}

func (c *queryCaptureContext) touch() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.once.Do(func() { close(c.captured) })
	if c.changed {
		c.observedAfter = true
	}
}
func (c *queryCaptureContext) Err() error            { c.touch(); return c.Context.Err() }
func (c *queryCaptureContext) Done() <-chan struct{} { c.touch(); return c.Context.Done() }
func TestQueryBuildIdentityCapturedBeforeCoordination(t *testing.T) {
	s, dsn, enroll, _ := retirementFixture(t)
	if _, err := s.EnrollRetirement(t.Context(), enroll); err != nil {
		t.Fatal(err)
	}
	target := durable.BuildTarget{NamespaceTarget: enroll.NamespaceTarget, BuildID: "captured"}
	gate, observe := retirementConn(t, dsn), retirementConn(t, dsn)
	tx, err := gate.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(t.Context(), `SELECT dispatch_retirement_coordinator_lock($1,1)`, target.Namespace); err != nil {
		t.Fatal(err)
	}
	original := durabletest.QueryIdentityFixture()
	callerOwned := original
	request := durable.RegisterBuildRequest{BuildTarget: target, RequestID: "capture", Identity: &callerOwned}
	digest, err := durable.Fingerprint(string(durable.OperationRegisterBuild), request)
	if err != nil {
		t.Fatal(err)
	}
	ctx := &queryCaptureContext{Context: t.Context(), captured: make(chan struct{})}
	done := make(chan struct{})
	var receipt durable.LifecycleReceipt
	var callErr error
	go func() { receipt, callErr = s.RegisterBuild(ctx, request); close(done) }()
	<-ctx.captured
	queryWaitNamespace(t, observe, target.Namespace)
	ctx.mu.Lock()
	callerOwned.ArtifactDigest = strings.Repeat("f", 64)
	ctx.changed = true
	ctx.mu.Unlock()
	if err = tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-done
	ctx.mu.Lock()
	observedAfter := ctx.observedAfter
	ctx.mu.Unlock()
	if !observedAfter {
		t.Fatal("missing ordered post-wait context barrier")
	}
	if callErr != nil || receipt.Build == nil || receipt.Build.QueryIdentity != original || receipt.RequestDigest != digest {
		t.Fatalf("caller changed captured identity: %+v %v", receipt, callErr)
	}
	request.Identity = &original
	replay, err := s.RegisterBuild(t.Context(), request)
	if err != nil || !reflect.DeepEqual(replay, receipt) {
		t.Fatalf("original request replay changed: %+v %v", replay, err)
	}
}

func TestQueryMutationIntentRollback(t *testing.T) {
	s, dsn, _, _ := retirementFixture(t)
	c := retirementConn(t, dsn)
	durabletest.RunQueryIntentRollback(t, s, func(t *testing.T, target durable.NamespaceTarget) any {
		t.Helper()
		var snapshot string
		err := c.QueryRow(t.Context(), `SELECT jsonb_build_array(
   (SELECT coalesce(jsonb_agg(to_jsonb(q) ORDER BY runtime_id),'[]') FROM dispatch_query_runtimes q WHERE namespace=$1),
   (SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY operation,request_id),'[]') FROM dispatch_lifecycle_receipts r WHERE namespace=$1),
   (SELECT coalesce(jsonb_agg(to_jsonb(o) ORDER BY id),'[]') FROM dispatch_durable_outbox o WHERE namespace=$1))::text`, target.Namespace).Scan(&snapshot)
		if err != nil {
			t.Fatal(err)
		}
		return snapshot
	}, func(t *testing.T, action string) func() {
		t.Helper()
		_, err := c.Exec(t.Context(), `CREATE FUNCTION dispatch_test_query_mutation_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.envelope->>'Action' = TG_ARGV[0] THEN RAISE EXCEPTION 'injected query intent failure'; END IF; RETURN NEW; END $$`)
		if err != nil {
			t.Fatal(err)
		}
		// The action comes from the closed lifecycle action mapping above.
		_, err = c.Exec(t.Context(), `CREATE TRIGGER dispatch_test_query_mutation_failure BEFORE INSERT ON dispatch_durable_outbox FOR EACH ROW EXECUTE FUNCTION dispatch_test_query_mutation_failure('`+action+`')`)
		if err != nil {
			t.Fatal(err)
		}
		var once sync.Once
		restore := func() {
			once.Do(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if _, err := c.Exec(ctx, `DROP TRIGGER IF EXISTS dispatch_test_query_mutation_failure ON dispatch_durable_outbox; DROP FUNCTION IF EXISTS dispatch_test_query_mutation_failure()`); err != nil {
					t.Error(err)
				}
			})
		}
		t.Cleanup(restore)
		return restore
	})
}
