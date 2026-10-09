package sinkhost

import (
	"encoding/json"
	"testing"

	"github.com/xraph/dispatch/durable"
)

func (r *processRig) receiverOutage() {
	r.run("receiver_process_outage", func(t *testing.T) {
		before := r.count("relay", "SELECT count(*) FROM qualification_received")
		r.stop("receiver", true)
		r.command("receiver-outage", "operator", 202)
		r.eventually(func() bool {
			return r.count("dispatch", "SELECT count(*) FROM dispatch_durable_outbox WHERE delivered_at IS NULL") == 0
		})
		r.eventually(func() bool {
			return r.count("relay", "SELECT count(*) FROM relay_deliveries WHERE state<>'delivered' AND attempt_count>0") > 0
		})
		r.equal(before, r.count("relay", "SELECT count(*) FROM qualification_received"), "offline receiver effects")
		t.Logf("source pending=0; Relay webhook pending=%d while receiver process stopped", r.count("relay", "SELECT count(*) FROM relay_deliveries WHERE state<>'delivered'"))
		r.start("receiver", r.c)
		r.settled()
		r.verify()
	})
}
func (r *processRig) conflicts() {
	for _, role := range []string{"chronicle", "relay"} {
		r.run(role+"_confirmed_conflict", func(t *testing.T) {
			r.stop(role, true)
			r.command("conflict-"+role, "operator", 202)
			r.eventually(func() bool {
				return r.count("dispatch", "SELECT count(*) FROM dispatch_durable_outbox WHERE destination=$1 AND delivered_at IS NULL AND error_category<>'conflict'", role) > 0
			})
			r.stop("dispatch", true)
			original := r.pending(role)
			changed, err := durable.NewDelivery(durable.NamespaceRecord{NamespaceConfig: durable.NamespaceConfig{InstallationID: original.InstallationID, Namespace: original.Namespace, AppID: original.AppID, TenantID: original.TenantID, SchemaVersion: original.SchemaVersion}}, original.Destination, durable.DeliverySource{Key: durable.Key{Namespace: original.Namespace, WorkflowID: original.WorkflowID, RunID: original.RunID}, Kind: original.SourceKind, ID: original.SourceID, Sequence: original.Sequence, OccurredAt: original.OccurredAt, Action: "qualification.conflicting-content", Outcome: original.Outcome, Target: original.Target, Metadata: original.Metadata})
			if err != nil {
				t.Fatal(err)
			}
			if changed.ID != original.ID || changed.Fingerprint == original.Fingerprint {
				t.Fatal("invalid conflict setup")
			}
			r.start(role, r.c)
			r.post(role, "/accept", r.c.Credentials[role].Secret, r.body(role, changed), 200, nil)
			r.seeded[role]++
			r.start("dispatch", r.c)
			r.eventually(func() bool {
				return r.count("dispatch", "SELECT count(*) FROM dispatch_durable_outbox WHERE id=$1 AND error_category='conflict' AND delivered_at IS NULL AND receipt IS NULL", original.ID) == 1
			})
			r.settled()
			r.stop("dispatch", true)
			var attempts int64
			var envelope []byte
			if err = r.db["dispatch"].QueryRow(r.ctx, "SELECT attempts,envelope FROM dispatch_durable_outbox WHERE id=$1", original.ID).Scan(&attempts, &envelope); err != nil {
				t.Fatal(err)
			}
			var retained durable.Delivery
			if err = json.Unmarshal(envelope, &retained); err != nil {
				t.Fatal(err)
			}
			if retained != original {
				t.Fatal("conflict rewrote immutable source")
			}
			r.start("dispatch", r.c)
			r.command("unrelated-"+role, "operator", 202)
			r.settled()
			r.equal(attempts, r.count("dispatch", "SELECT attempts FROM dispatch_durable_outbox WHERE id=$1", original.ID), "blocked delivery reclaimed after restart")
			r.equal(1, r.count("dispatch", "SELECT count(*) FROM dispatch_durable_outbox WHERE id=$1 AND error_category='conflict' AND delivered_at IS NULL", original.ID), "blocked disposition lost")
			r.verify()
			t.Logf("blocked source %s retained after restart; unrelated delivery completed; automatic claims unchanged", original.ID)
		})
	}
}
