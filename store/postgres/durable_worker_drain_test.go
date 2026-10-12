package postgres_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"

	"github.com/xraph/dispatch/durable/durabletest"
)

func TestWorkerDrainReceipt(t *testing.T) {
	s, _, _, _ := retirementFixture(t)
	durabletest.RunWorkerDrainReceipt(t, s)
}

func TestWorkerDrainIntentRollback(t *testing.T) {
	s, dsn, _, _ := retirementFixture(t)
	c := retirementConn(t, dsn)
	durabletest.RunWorkerDrainIntentRollback(t, s, func(t *testing.T, target durable.NamespaceTarget) any {
		t.Helper()
		var snapshot string
		err := c.QueryRow(t.Context(), `SELECT jsonb_build_array(
 (SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY operation,request_id),'[]') FROM dispatch_lifecycle_receipts r WHERE namespace=$1),
 (SELECT coalesce(jsonb_agg(to_jsonb(o) ORDER BY id),'[]') FROM dispatch_durable_outbox o WHERE namespace=$1))::text`, target.Namespace).Scan(&snapshot)
		if err != nil {
			t.Fatal(err)
		}
		return snapshot
	}, func(t *testing.T) func() {
		t.Helper()
		_, err := c.Exec(t.Context(), `CREATE FUNCTION dispatch_test_drain_intent_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.envelope->>'Action'='dispatch.worker.drain.request' THEN RAISE EXCEPTION 'injected drain intent failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER dispatch_test_drain_intent_failure BEFORE INSERT ON dispatch_durable_outbox FOR EACH ROW EXECUTE FUNCTION dispatch_test_drain_intent_failure()`)
		if err != nil {
			t.Fatal(err)
		}
		var once sync.Once
		restore := func() {
			once.Do(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if _, dropErr := c.Exec(ctx, `DROP TRIGGER IF EXISTS dispatch_test_drain_intent_failure ON dispatch_durable_outbox; DROP FUNCTION IF EXISTS dispatch_test_drain_intent_failure()`); dropErr != nil {
					t.Error(dropErr)
				}
			})
		}
		t.Cleanup(restore)
		return restore
	})
}
