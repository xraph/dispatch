package durabletest

import (
	"testing"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
)

func verifyLifecycleDelivery(t *testing.T, s durable.Store, accepted durable.LifecycleReceipt) {
	t.Helper()
	outbox, ok := s.(durable.OutboxStore)
	if !ok {
		t.Fatal("outbox capability missing")
	}
	catalog, ok := s.(durable.NamespaceStore)
	if !ok {
		t.Fatal("namespace capability missing")
	}
	n, err := catalog.GetNamespace(t.Context(), accepted.InstallationID, accepted.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	q := durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: accepted.InstallationID, Destination: durable.DestinationChronicle}, Limit: durable.MaxDeliveryBatch}
	for {
		status, readErr := outbox.DeliveryStatus(t.Context(), q)
		if readErr != nil {
			t.Fatal(readErr)
		}
		for _, record := range status.Records {
			d := record.Delivery
			q.After = d.ID
			if d.ID != accepted.DeliveryID {
				continue
			}
			source := durable.LifecycleDeliverySource(durable.WithAuditMetadata(t.Context(), d.Metadata), accepted)
			expected, buildErr := durable.NewDelivery(n, durable.DestinationChronicle, source)
			if buildErr != nil || d.Verify() != nil || d.Fingerprint != expected.Fingerprint {
				t.Fatalf("lifecycle source proof mismatch: %v", buildErr)
			}
			mapped, mapErr := ecosystem.ChronicleRequest(ecosystem.Binding{Producer: "dispatch", InstallationID: n.InstallationID, Namespace: n.Namespace, AppID: n.AppID, TenantID: n.TenantID}, d)
			if mapErr != nil || mapped.SourceKey != accepted.DeliveryID || mapped.SourceFingerprint != d.Fingerprint || mapped.Event.Resource != "dispatch.installation" || mapped.Event.Action != durable.LifecycleAction(accepted.Operation) || mapped.Event.ResourceID != source.Target {
				t.Fatalf("lifecycle Chronicle mapping: %+v %v", mapped, mapErr)
			}
			return
		}
		if len(status.Records) < q.Limit {
			t.Fatal("lifecycle intent missing")
		}
	}
}
