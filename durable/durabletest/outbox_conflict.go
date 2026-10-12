package durabletest

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

// RunOutboxConflict checks the persisted conflict fence on each backend.
func RunOutboxConflict(t *testing.T, s AuditStore) {
	t.Helper()
	ctx := t.Context()
	n, err := s.RegisterNamespace(ctx, durable.NamespaceConfig{InstallationID: "conflict-install", Namespace: t.Name() + "-conflict", AppID: "app", TenantID: "tenant", RequireAudit: true, SchemaVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{"a", "b"} {
		_, err = s.AppendSecurityAudit(ctx, durable.SecurityAudit{InstallationID: n.InstallationID, Namespace: n.Namespace, AttemptID: strings.Repeat(source, 64), OccurredAt: time.Now().UTC(), Action: "operator.write", Outcome: "denied", Metadata: durable.AuditMetadata{ActorKind: "user", ActorID: "operator"}})
		if err != nil {
			t.Fatal(err)
		}
	}
	scope := durable.DeliveryScope{InstallationID: n.InstallationID, Destination: durable.DestinationChronicle}
	claim := durable.DeliveryClaim{DeliveryScope: scope, Owner: "old", Limit: 1, LeaseDuration: time.Millisecond}
	rows, err := s.ClaimDeliveries(ctx, claim)
	if err != nil || len(rows) != 1 {
		t.Fatalf("claim: %v %v", rows, err)
	}
	old := rows[0]
	time.Sleep(5 * time.Millisecond)
	if err = s.BlockDelivery(ctx, old.Token()); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("expired block: %v", err)
	}
	claim.Owner = "new"
	claim.LeaseDuration = time.Minute
	claim.Limit = 100
	rows, err = s.ClaimDeliveries(ctx, claim)
	if err != nil || len(rows) != 2 {
		t.Fatalf("reclaim: %v %v", rows, err)
	}
	var blocked durable.DeliveryRecord
	for _, row := range rows {
		if row.Delivery.ID == old.Delivery.ID {
			blocked = row
		} else {
			r := durable.SinkReceipt{ID: "receipt", DeliveryID: row.Delivery.ID, Destination: row.Delivery.Destination, SchemaVersion: 1, Fingerprint: row.Delivery.Fingerprint}
			if err = s.AcknowledgeDelivery(ctx, row.Token(), r); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = s.BlockDelivery(ctx, old.Token()); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("stale block: %v", err)
	}
	if err = s.BlockDelivery(ctx, blocked.Token()); err != nil {
		t.Fatal(err)
	}
	if err = s.RetryDelivery(ctx, durable.DeliveryRetry{Token: blocked.Token(), Category: "unavailable"}); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("blocked retry: %v", err)
	}
	claim.Owner = "restarted"
	rows, err = s.ClaimDeliveries(ctx, claim)
	if err != nil || len(rows) != 0 {
		t.Fatalf("blocked reclaim: %v %v", rows, err)
	}
	status, err := s.DeliveryStatus(ctx, durable.DeliveryStatusRequest{DeliveryScope: scope, Limit: 100})
	if err != nil || status.Pending != 1 || status.Blocked != 1 {
		t.Fatalf("status: %+v %v", status, err)
	}
	for _, row := range status.Records {
		if row.Delivery.ID == blocked.Delivery.ID && (row.Delivery != blocked.Delivery || !row.Blocked() || row.Attempts != blocked.Attempts) {
			t.Fatalf("conflict mutated envelope or attempts: %+v", row)
		}
	}
}
