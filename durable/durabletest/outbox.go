package durabletest

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

type AuditStore interface {
	durable.Store
	durable.NamespaceStore
	durable.OutboxStore
}

// RunOutbox exercises the public ownership, acceptance and delivery contract.
func RunOutbox(t *testing.T, s AuditStore) {
	RunOutboxConflict(t, s)
	t.Helper()
	ctx := t.Context()
	config := durable.NamespaceConfig{InstallationID: "installation", Namespace: t.Name(), AppID: "app", TenantID: "tenant", RequireAudit: true, RequireHooks: true, SchemaVersion: 1}
	n, err := s.RegisterNamespace(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.RegisterNamespace(ctx, config)
	if err != nil || !again.CoverageStartedAt.Equal(n.CoverageStartedAt) {
		t.Fatalf("registration replay: %+v %v", again, err)
	}
	other := config
	other.InstallationID = "foreign"
	if _, err = s.RegisterNamespace(ctx, other); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("owner conflict: %v", err)
	}
	other = config
	other.RequireHooks = false
	if _, err = s.RegisterNamespace(ctx, other); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("policy conflict: %v", err)
	}
	if _, err = s.GetNamespace(ctx, "foreign", config.Namespace); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("scope lookup: %v", err)
	}
	list, err := s.ListNamespaces(ctx, durable.NamespaceList{InstallationID: "installation", Limit: 1})
	if err != nil || len(list) != 1 {
		t.Fatalf("list: %v %v", list, err)
	}
	if _, err = s.ListNamespaces(ctx, durable.NamespaceList{Limit: 1}); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("wildcard: %v", err)
	}
	metadata := durable.AuditMetadata{ActorKind: "user", ActorID: "operator", RequestID: "client-correlation", DecisionID: "allowed"}
	ctx = durable.WithAuditMetadata(ctx, metadata)
	request := durable.StartRequest{Key: durable.Key{Namespace: n.Namespace, WorkflowID: "order", RunID: "run"}, RequestID: "start", WorkflowType: "order", BuildID: "v1", Queue: "q", Input: []byte(`{"secret":"never-copy"}`)}
	if _, err = s.StartExecution(ctx, request); err != nil {
		t.Fatal(err)
	}
	if _, err = s.StartExecution(ctx, request); err != nil {
		t.Fatal(err)
	}
	scope := durable.DeliveryScope{InstallationID: n.InstallationID, Destination: durable.DestinationChronicle}
	status, err := s.DeliveryStatus(ctx, durable.DeliveryStatusRequest{DeliveryScope: scope, Limit: 100})
	if err != nil || status.Pending != 2 {
		t.Fatalf("start intents: %+v %v", status, err)
	}
	for _, r := range status.Records {
		if r.Delivery.SourceKind == "execution_receipt" && r.Delivery.Action != "execution.start" {
			t.Fatalf("accepted action lost: %+v", r.Delivery)
		}
		if r.Delivery.Metadata != metadata || r.Delivery.AppID != "app" || r.Delivery.TenantID != "tenant" {
			t.Fatalf("metadata: %+v", r)
		}
		b, marshalErr := json.Marshal(r)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if strings.Contains(string(b), "never-copy") {
			t.Fatal("payload copied")
		}
		if err = r.Delivery.Verify(); err != nil {
			t.Fatal(err)
		}
	}
	foreign := scope
	foreign.InstallationID = "foreign"
	claimed, err := s.ClaimDeliveries(ctx, durable.DeliveryClaim{DeliveryScope: foreign, Owner: "p", Limit: 100, LeaseDuration: time.Minute})
	if err != nil || len(claimed) != 0 {
		t.Fatalf("foreign claim: %v %v", claimed, err)
	}
	claimed, err = s.ClaimDeliveries(ctx, durable.DeliveryClaim{DeliveryScope: scope, Owner: "p", Limit: 1, LeaseDuration: time.Minute})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	c := claimed[0]
	receipt := durable.SinkReceipt{ID: "sink-receipt", DeliveryID: c.Delivery.ID, Destination: c.Delivery.Destination, SchemaVersion: 1, Fingerprint: c.Delivery.Fingerprint}
	bad := receipt
	bad.Fingerprint = "wrong"
	if err = s.AcknowledgeDelivery(ctx, c.Token(), bad); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("receipt validation: %v", err)
	}
	if _, err = s.RenewDelivery(ctx, c.Token(), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err = s.RetryDelivery(ctx, durable.DeliveryRetry{Token: c.Token(), Category: "unavailable"}); err != nil {
		t.Fatal(err)
	}
	if err = s.AcknowledgeDelivery(ctx, c.Token(), receipt); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("released token: %v", err)
	}
	claimed, err = s.ClaimDeliveries(ctx, durable.DeliveryClaim{DeliveryScope: scope, Owner: "next", Limit: 100, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	for _, c = range claimed {
		receipt = durable.SinkReceipt{ID: "sink-" + c.Delivery.ID, DeliveryID: c.Delivery.ID, Destination: c.Delivery.Destination, SchemaVersion: 1, Fingerprint: c.Delivery.Fingerprint}
		if err = s.AcknowledgeDelivery(ctx, c.Token(), receipt); err != nil {
			t.Fatal(err)
		}
	}
	status, err = s.DeliveryStatus(ctx, durable.DeliveryStatusRequest{DeliveryScope: scope, Limit: 100})
	if err != nil || status.Pending != 0 || len(status.Records) != 2 {
		t.Fatalf("ack evidence: %+v %v", status, err)
	}
	audit, err := durable.CaptureSecurityAudit(n.InstallationID, n.Namespace, "execution.read", "denied", "unknown-target", durable.AuditMetadata{ActorKind: "anonymous", CorrelationID: "caller-id"})
	if err != nil {
		t.Fatal(err)
	}
	audit.OccurredAt = time.Date(2026, 1, 2, 3, 4, 5, 987654321, time.FixedZone("offset", 3600))
	d, err := s.AppendSecurityAudit(ctx, audit)
	if err != nil {
		t.Fatal(err)
	}
	audit.OccurredAt = audit.OccurredAt.UTC()
	replayed, err := s.AppendSecurityAudit(ctx, audit)
	if err != nil || d != replayed {
		t.Fatalf("captured retry: %+v %v", replayed, err)
	}
	changed := audit
	changed.Metadata.ActorKind = "system"
	changed.Metadata.ActorID = "forged"
	if _, err = s.AppendSecurityAudit(ctx, changed); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("changed actor: %v", err)
	}
	changed = audit
	changed.Namespace = "unknown"
	if _, err = s.AppendSecurityAudit(ctx, changed); err == nil {
		t.Fatal("unbound security accepted")
	}
	changed = audit
	changed.InstallationID = "foreign"
	if _, err = s.AppendSecurityAudit(ctx, changed); err == nil {
		t.Fatal("foreign audit accepted")
	}
	status, err = s.DeliveryStatus(ctx, durable.DeliveryStatusRequest{DeliveryScope: scope, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range status.Records {
		if err = r.Delivery.Verify(); err != nil {
			t.Fatalf("roundtrip fingerprint: %v", err)
		}
	}
}

// RegisteredAuditStore runs existing conformance paths with both destinations
// required. Database constraint guards prove each accepted event and receipt has
// an intent, including paths that create children or carry continuation history.
type RegisteredAuditStore struct{ AuditStore }

func (s RegisteredAuditStore) register(ctx context.Context, namespace string) error {
	_, err := s.RegisterNamespace(ctx, durable.NamespaceConfig{InstallationID: "conformance", Namespace: namespace, AppID: "app", TenantID: "tenant", RequireAudit: true, RequireHooks: true, SchemaVersion: 1})
	return err
}
func (s RegisteredAuditStore) StartExecution(ctx context.Context, r durable.StartRequest) (durable.Receipt, error) {
	if err := s.register(ctx, r.Namespace); err != nil {
		return durable.Receipt{}, err
	}
	return s.AuditStore.StartExecution(ctx, r)
}
func (s RegisteredAuditStore) SignalWithStart(ctx context.Context, r durable.SignalWithStartRequest) (durable.SignalReceipt, error) {
	if err := s.register(ctx, r.Start.Namespace); err != nil {
		return durable.SignalReceipt{}, err
	}
	return s.AuditStore.SignalWithStart(ctx, r)
}
