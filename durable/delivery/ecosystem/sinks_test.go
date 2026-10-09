package ecosystem_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xraph/chronicle"
	ca "github.com/xraph/chronicle/acceptance"
	cs "github.com/xraph/chronicle/store"
	cm "github.com/xraph/chronicle/store/memory"
	"github.com/xraph/relay"
	ra "github.com/xraph/relay/acceptance"
	rm "github.com/xraph/relay/store/memory"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
)

func envelope(t *testing.T, destination durable.Destination) (ecosystem.Binding, durable.Delivery) {
	t.Helper()
	b := ecosystem.Binding{Producer: "dispatch", InstallationID: "install", Namespace: "ns", AppID: "app", OrgID: "org", TenantID: "tenant"}
	d, err := durable.NewDelivery(durable.NamespaceRecord{NamespaceConfig: durable.NamespaceConfig{InstallationID: b.InstallationID, Namespace: b.Namespace, AppID: b.AppID, TenantID: b.TenantID, SchemaVersion: 1, RequireAudit: true, RequireHooks: true}}, destination, durable.DeliverySource{Key: durable.Key{Namespace: b.Namespace, WorkflowID: "workflow", RunID: "run"}, Kind: "event", ID: "9007199254740993", Sequence: 9007199254740993, OccurredAt: time.Date(2026, 10, 9, 1, 2, 3, 123456000, time.UTC), Action: "execution.completed", Outcome: "accepted", Metadata: durable.AuditMetadata{ActorKind: "user", ActorID: "original-user", DecisionID: "decision"}})
	if err != nil {
		t.Fatal(err)
	}
	return b, d
}

type chronicleFunc func(context.Context, ca.Request) (*ca.Receipt, error)

func (f chronicleFunc) RecordOnce(ctx context.Context, r ca.Request) (*ca.Receipt, error) {
	return f(ctx, r)
}

type relayFunc func(context.Context, ra.Request) (*ra.Receipt, error)

func (f relayFunc) SendReliable(ctx context.Context, r ra.Request) (*ra.Receipt, error) {
	return f(ctx, r)
}

func TestConcreteAcceptanceAndRecovery(t *testing.T) {
	c, err := chronicle.New(chronicle.WithStore(cs.NewAdapter(cm.New())))
	if err != nil {
		t.Fatal(err)
	}
	r, err := relay.New(relay.WithStore(rm.New()))
	if err != nil {
		t.Fatal(err)
	}
	if err = ecosystem.RegisterRelaySchema(t.Context(), r.Catalog(), "app"); err != nil {
		t.Fatal(err)
	}
	for _, dest := range []durable.Destination{durable.DestinationChronicle, durable.DestinationRelay} {
		t.Run(string(dest), func(t *testing.T) {
			b, d := envelope(t, dest)
			var sink delivery.Sink
			var committed durable.SinkReceipt
			lost := false
			if dest == durable.DestinationChronicle {
				sink = ecosystem.Chronicle{Binding: b, Client: chronicleFunc(func(ctx context.Context, req ca.Request) (*ca.Receipt, error) {
					if req.Event.UserID != "original-user" || !req.Event.Timestamp.Equal(d.OccurredAt) || req.Event.Metadata["source_sequence"].(json.Number).String() != "9007199254740993" {
						t.Fatal("mapping lost identity or precision")
					}
					receipt, e := c.RecordOnce(ctx, req)
					if e != nil {
						return nil, e
					}
					if !lost {
						lost = true
						return nil, errors.New("lost ack")
					}
					return receipt, nil
				})}
			} else {
				sink = ecosystem.Relay{Binding: b, Client: relayFunc(func(ctx context.Context, req ra.Request) (*ra.Receipt, error) {
					if !strings.Contains(string(req.Data), "9007199254740993") || !strings.Contains(string(req.Data), "123456Z") {
						t.Fatal("mapping lost precision")
					}
					receipt, e := r.SendReliable(ctx, req)
					if e != nil {
						return nil, e
					}
					if !lost {
						lost = true
						return nil, errors.New("lost ack")
					}
					return receipt, nil
				})}
			}
			if _, err = sink.Accept(t.Context(), d); err == nil || errors.Is(err, delivery.ErrConfirmedConflict) {
				t.Fatalf("unknown outcome: %v", err)
			}
			committed, err = sink.Accept(t.Context(), d)
			if err != nil {
				t.Fatal(err)
			}
			again, e := sink.Accept(t.Context(), d)
			if e != nil || again != committed || committed.Verify(d) != nil || committed.MappingVersion != 1 || committed.SinkFingerprint == d.Fingerprint || !json.Valid([]byte(committed.Evidence)) {
				t.Fatalf("recovery: %+v %+v %v", committed, again, e)
			}
			// A valid different source fingerprint under the same source key conflicts.
			if dest == durable.DestinationChronicle {
				req, _ := ecosystem.ChronicleRequest(b, d)
				req.Event.Action = "changed"
				if _, e = c.RecordOnce(t.Context(), req); !errors.Is(e, ca.ErrConflict) {
					t.Fatalf("conflict: %v", e)
				}
			} else {
				req, _ := ecosystem.RelayRequest(b, d)
				req.Data = []byte(`{}`)
				if _, e = r.SendReliable(t.Context(), req); !errors.Is(e, ra.ErrConflict) {
					t.Fatalf("conflict: %v", e)
				}
			}
		})
	}
}

func TestReceiptTamperingAndConflictClassification(t *testing.T) {
	c, err := chronicle.New(chronicle.WithStore(cs.NewAdapter(cm.New())))
	if err != nil {
		t.Fatal(err)
	}
	r, err := relay.New(relay.WithStore(rm.New()))
	if err != nil {
		t.Fatal(err)
	}
	if err = ecosystem.RegisterRelaySchema(t.Context(), r.Catalog(), "app"); err != nil {
		t.Fatal(err)
	}
	b, d := envelope(t, durable.DestinationChronicle)
	req, _ := ecosystem.ChronicleRequest(b, d)
	good, err := c.RecordOnce(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*ca.Receipt){func(r *ca.Receipt) { r.Fingerprint = d.Fingerprint }, func(r *ca.Receipt) { r.SourceFingerprint = "wrong" }, func(r *ca.Receipt) { r.SourceKey = "wrong" }, func(r *ca.Receipt) { r.Installation = "wrong" }, func(r *ca.Receipt) { r.Producer = "wrong" }, func(r *ca.Receipt) { r.OrgID = "wrong" }, func(r *ca.Receipt) { r.AppID = "wrong" }, func(r *ca.Receipt) { r.TenantID = "wrong" }, func(r *ca.Receipt) { r.Sequence = 0 }} {
		bad := *good
		mutate(&bad)
		sink := ecosystem.Chronicle{Binding: b, Client: chronicleFunc(func(context.Context, ca.Request) (*ca.Receipt, error) { return &bad, nil })}
		if _, err = sink.Accept(t.Context(), d); err == nil || errors.Is(err, delivery.ErrConfirmedConflict) {
			t.Fatalf("tampered receipt must remain retryable: %v", err)
		}
	}
	rb, rd := envelope(t, durable.DestinationRelay)
	rr, _ := ecosystem.RelayRequest(rb, rd)
	rg, err := r.SendReliable(t.Context(), rr)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*ra.Receipt){func(r *ra.Receipt) { r.Version = 2 }, func(r *ra.Receipt) { r.Fingerprint = rd.Fingerprint }, func(r *ra.Receipt) { r.SourceKey = "wrong" }, func(r *ra.Receipt) { r.AppID = "wrong" }, func(r *ra.Receipt) { r.OrgID = "wrong" }, func(r *ra.Receipt) { r.TenantID = "wrong" }, func(r *ra.Receipt) { r.Producer = "wrong" }, func(r *ra.Receipt) { r.InstallationID = "wrong" }} {
		bad := *rg
		mutate(&bad)
		sink := ecosystem.Relay{Binding: rb, Client: relayFunc(func(context.Context, ra.Request) (*ra.Receipt, error) { return &bad, nil })}
		if _, err = sink.Accept(t.Context(), rd); err == nil || errors.Is(err, delivery.ErrConfirmedConflict) {
			t.Fatalf("tampered receipt must remain retryable: %v", err)
		}
	}
	for _, e := range []error{ca.ErrConflict, ca.ErrHeadConflict, context.DeadlineExceeded, errors.New("unavailable")} {
		sink := ecosystem.Chronicle{Binding: b, Client: chronicleFunc(func(context.Context, ca.Request) (*ca.Receipt, error) { return nil, e })}
		_, err = sink.Accept(t.Context(), d)
		if errors.Is(err, delivery.ErrConfirmedConflict) != errors.Is(e, ca.ErrConflict) {
			t.Fatalf("classification: %v -> %v", e, err)
		}
	}
	for _, e := range []error{ra.ErrConflict, ra.ErrInvalid, context.DeadlineExceeded} {
		sink := ecosystem.Relay{Binding: rb, Client: relayFunc(func(context.Context, ra.Request) (*ra.Receipt, error) { return nil, e })}
		_, err = sink.Accept(t.Context(), rd)
		if errors.Is(err, delivery.ErrConfirmedConflict) != errors.Is(e, ra.ErrConflict) {
			t.Fatalf("classification: %v -> %v", e, err)
		}
	}
	for _, mutate := range []func(*durable.Delivery){func(d *durable.Delivery) { d.SchemaVersion = 2 }, func(d *durable.Delivery) { d.AppID = "foreign" }, func(d *durable.Delivery) { d.Namespace = "foreign" }, func(d *durable.Delivery) { d.Sequence++ }} {
		bad := rd
		mutate(&bad)
		if _, err = ecosystem.RelayRequest(rb, bad); err == nil {
			t.Fatal("invalid envelope admitted")
		}
	}
	// The real registered schema rejects a mismatched version before acceptance.
	rr.SourceKey = "new-source"
	rr.Data = []byte(strings.ReplaceAll(string(rr.Data), `"SchemaVersion":1`, `"SchemaVersion":2`))
	if _, err = r.SendReliable(t.Context(), rr); err == nil {
		t.Fatal("schema mismatch accepted")
	}
}

func TestClientCannotChangeExpectedSemantics(t *testing.T) {
	c, err := chronicle.New(chronicle.WithStore(cs.NewAdapter(cm.New())))
	if err != nil {
		t.Fatal(err)
	}
	b, d := envelope(t, durable.DestinationChronicle)
	sink := ecosystem.Chronicle{Binding: b, Client: chronicleFunc(func(ctx context.Context, req ca.Request) (*ca.Receipt, error) {
		req.Event.Action = "different"
		return c.RecordOnce(ctx, req)
	})}
	if _, err = sink.Accept(t.Context(), d); err == nil || errors.Is(err, delivery.ErrConfirmedConflict) {
		t.Fatalf("changed semantics acknowledged: %v", err)
	}
	r, err := relay.New(relay.WithStore(rm.New()))
	if err != nil {
		t.Fatal(err)
	}
	if err = ecosystem.RegisterRelaySchema(t.Context(), r.Catalog(), "app"); err != nil {
		t.Fatal(err)
	}
	rb, rd := envelope(t, durable.DestinationRelay)
	relaySink := ecosystem.Relay{Binding: rb, Client: relayFunc(func(ctx context.Context, req ra.Request) (*ra.Receipt, error) {
		changed := strings.ReplaceAll(string(req.Data), "original-user", "differentuser")
		copy(req.Data, []byte(changed))
		return r.SendReliable(ctx, req)
	})}
	if _, err = relaySink.Accept(t.Context(), rd); err == nil || errors.Is(err, delivery.ErrConfirmedConflict) {
		t.Fatalf("changed semantics acknowledged: %v", err)
	}
}
