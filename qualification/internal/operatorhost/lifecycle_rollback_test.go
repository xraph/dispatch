package operatorhost

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch/operator"
	pgstore "github.com/xraph/dispatch/store/postgres"
)

func TestPostgresLifecycleIntentFailureBeforeDrain(t *testing.T) {
	t.Setenv("DISPATCH_OPERATOR_DSN", isolatedPostgresScenario(t))
	store := postgresCommands(t)
	c := newConfiguredCommandClient(t, store, &LifecycleOptions{InstanceID: "intent-rollback"})
	c.command("durable.retirementEnroll", operator.EnrollmentInput{NamespaceLifecycleInput: operator.NamespaceLifecycleInput{Namespace: "production"}, RequestID: "rollback-enroll"}, 200)
	identity := c.host.RuntimeIdentities()[0]
	build := operator.BuildInput{Namespace: identity.Namespace, BuildID: identity.BuildID}
	c.command("durable.buildRegister", operator.RegisterBuildInput{BuildInput: build, RequestID: "rollback-build", ExpectedVersion: "0"}, 200)
	pg := pgdriver.Unwrap(store.(*pgstore.Store).DB())
	count := func(table string) int {
		t.Helper()
		var n int
		if err := pg.QueryRow(t.Context(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	receipts, outbox := count("dispatch_lifecycle_receipts"), count("dispatch_durable_outbox")
	if _, err := pg.Exec(t.Context(), `CREATE FUNCTION reject_lifecycle_intent() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.source_kind='security' AND NEW.envelope->>'Action' IN ('dispatch.build.retire','dispatch.worker.drain.request') THEN RAISE EXCEPTION 'private lifecycle intent failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_lifecycle_intent BEFORE INSERT ON dispatch_durable_outbox FOR EACH ROW EXECUTE FUNCTION reject_lifecycle_intent()`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := pg.Exec(context.Background(), `DROP TRIGGER IF EXISTS reject_lifecycle_intent ON dispatch_durable_outbox; DROP FUNCTION IF EXISTS reject_lifecycle_intent()`); err != nil {
			t.Error(err)
		}
	})
	retire := operator.BuildRetirementInput{BuildInput: build, RequestID: "rollback-retire", ExpectedVersion: "1", ExpectedEpoch: "1"}
	drain := operator.WorkerDrainInput{WorkerInput: operator.WorkerInput{BuildInput: build, RuntimeID: identity.RuntimeID}, RequestID: "rollback-drain", OperationID: "rollback-stop", Deadline: time.Now().UTC().Add(time.Minute)}
	for _, raw := range [][]byte{c.command("durable.buildRetire", retire, 503), c.command("durable.workerDrain", drain, 503)} {
		if bytes.Contains(raw, []byte("private")) {
			t.Fatal("private persistence error disclosed")
		}
	}
	if count("dispatch_lifecycle_receipts") != receipts || count("dispatch_durable_outbox") != outbox {
		t.Fatal("failed lifecycle acceptance persisted receipt or outbox")
	}
	facts := data[operator.BuildLifecycle](t, c.command("durable.build", build, 200))
	if facts.Version != "1" || facts.Epoch != "1" {
		t.Fatalf("retirement mutation survived rollback: %+v", facts)
	}
	status := data[operator.WorkerObservation](t, c.command("durable.workerStatus", drain.WorkerInput, 200))
	if status.AdmissionClosed {
		t.Fatal("failed durable drain acceptance invoked process")
	}
	if _, err := pg.Exec(t.Context(), `DROP TRIGGER reject_lifecycle_intent ON dispatch_durable_outbox; DROP FUNCTION reject_lifecycle_intent()`); err != nil {
		t.Fatal(err)
	}
	c.command("durable.buildRetire", retire, 200)
	accepted := data[operator.WorkerDrainAcceptance](t, c.command("durable.workerDrain", drain, 200))
	if !accepted.Complete || accepted.RuntimeID != identity.RuntimeID {
		t.Fatal("same request did not recover after intent persistence restored")
	}
}
