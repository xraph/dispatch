package ecosystem_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/chronicle"
	ca "github.com/xraph/chronicle/acceptance"
	"github.com/xraph/chronicle/hash"
	"github.com/xraph/chronicle/keys"
	cs "github.com/xraph/chronicle/store"
	cm "github.com/xraph/chronicle/store/memory"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	"github.com/xraph/dispatch/store/memory"
)

func TestRemoteChronicleExactSequence(t *testing.T) {
	b, d := envelope(t, durable.DestinationChronicle)
	req, err := ecosystem.ChronicleRequest(b, d)
	if err != nil {
		t.Fatal(err)
	}
	for _, sequence := range []uint64{0, 1, 10, 11, 100, 90071992547409930, math.MaxUint64 - 5, math.MaxUint64} {
		t.Run(fmt.Sprint(sequence), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(ca.Receipt{Sequence: sequence})
			}))
			defer server.Close()
			remote, e := ecosystem.NewRemote(server.URL, "private-bearer", time.Second, true)
			if e != nil {
				t.Fatal(e)
			}
			defer remote.Close()
			receipt, e := remote.RecordOnce(t.Context(), req)
			if e != nil {
				t.Fatal(e)
			}
			if receipt.Sequence != sequence {
				t.Fatalf("sequence changed: %d != %d", receipt.Sequence, sequence)
			}
		})
	}
	for _, response := range []string{`{"sequence":18446744073709551616}`, `{"sequence":9007199254740993.1}`, `{"sequence":-1}`, `{"sequence":10,"sequence":11}`, `{"sequence":10,"unknown":0}`, `{"sequence":10,"hash_key_id":"\ud800"}`, "{\"sequence\":10,\"hash_key_id\":\"" + string([]byte{0xff}) + "\"}", `{"sequence":NaN}`, `{"sequence":Infinity}`, `{"sequence":10} {}`, strings.Repeat(" ", ecosystem.MaxResponseBytes+1)} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(response)) }))
		remote, e := ecosystem.NewRemote(server.URL, "private-bearer", time.Second, true)
		if e != nil {
			t.Fatal(e)
		}
		receipt, e := remote.RecordOnce(t.Context(), req)
		remote.Close()
		server.Close()
		if e == nil || receipt != nil || errors.Is(e, ca.ErrConflict) {
			t.Fatalf("invalid response accepted or blocked: %v", e)
		}
	}
}

type receiptKeys struct{}

func (receiptKeys) Current(context.Context, keys.Use) (key []byte, id string, err error) {
	return []byte(strings.Repeat("k", 32)), "receipt-key", nil
}
func (receiptKeys) ByID(_ context.Context, id string) ([]byte, error) {
	if id != "receipt-key" {
		return nil, keys.ErrKeyNotFound
	}
	return []byte(strings.Repeat("k", 32)), nil
}

func TestChronicleReceiptProtectionProvenance(t *testing.T) {
	b, d := envelope(t, durable.DestinationChronicle)
	req, err := ecosystem.ChronicleRequest(b, d)
	if err != nil {
		t.Fatal(err)
	}
	for _, scheme := range []hash.Scheme{hash.SchemePlainV4, hash.SchemeHMACV5} {
		t.Run(string(scheme), func(t *testing.T) {
			engine, e := chronicle.New(chronicle.WithStore(cs.NewAdapter(cm.New())), chronicle.WithDigestScheme(scheme), chronicle.WithKeyProvider(receiptKeys{}))
			if e != nil {
				t.Fatal(e)
			}
			good, e := engine.RecordOnce(t.Context(), req)
			if e != nil {
				t.Fatal(e)
			}
			sink := ecosystem.Chronicle{Binding: b, Client: engine}
			if _, e = sink.Accept(t.Context(), d); e != nil {
				t.Fatalf("valid provenance refused: %v", e)
			}
			cases := map[string]func(*ca.Receipt){
				"unknown-scheme":           func(r *ca.Receipt) { r.HashScheme = "unknown" },
				"missing-scheme":           func(r *ca.Receipt) { r.HashScheme = "" },
				"short-digest":             func(r *ca.Receipt) { r.Hash = "x" },
				"odd-digest":               func(r *ca.Receipt) { r.Hash = strings.Repeat("a", 63) },
				"long-digest":              func(r *ca.Receipt) { r.Hash = strings.Repeat("a", 66) },
				"non-hex-digest":           func(r *ca.Receipt) { r.Hash = strings.Repeat("z", 64) },
				"keyed-without-key":        func(r *ca.Receipt) { r.HashScheme = string(hash.SchemeHMACV5); r.HashKeyID = "" },
				"legacy-keyed-without-key": func(r *ca.Receipt) { r.HashScheme = string(hash.SchemeHMAC); r.HashKeyID = "" },
				"plain-with-key":           func(r *ca.Receipt) { r.HashScheme = string(hash.SchemePlainV4); r.HashKeyID = "unexpected" },
				"legacy-plain-with-key":    func(r *ca.Receipt) { r.HashScheme = string(hash.SchemePlain); r.HashKeyID = "unexpected" },
				"original-with-key":        func(r *ca.Receipt) { r.HashScheme = string(hash.SchemeLegacy); r.HashKeyID = "unexpected" },
			}
			for name, mutate := range cases {
				t.Run(name, func(t *testing.T) {
					bad := *good
					mutate(&bad)
					sink := ecosystem.Chronicle{Binding: b, Client: chronicleFunc(func(context.Context, ca.Request) (*ca.Receipt, error) { return &bad, nil })}
					receipt, acceptErr := sink.Accept(t.Context(), d)
					if acceptErr == nil || errors.Is(acceptErr, delivery.ErrConfirmedConflict) || receipt.ID != "" {
						t.Fatalf("invalid provenance acknowledged or blocked: %v", acceptErr)
					}
				})
			}
			// Known retained schemes describe provenance, not a minimum protection policy.
			for _, retained := range []hash.Scheme{hash.SchemeLegacy, hash.SchemePlain, hash.SchemeHMAC, hash.SchemePlainV4, hash.SchemeHMACV5} {
				r := *good
				r.HashScheme = string(retained)
				r.HashKeyID = ""
				if retained == hash.SchemeHMAC || retained == hash.SchemeHMACV5 {
					r.HashKeyID = "receipt-key"
				}
				sink := ecosystem.Chronicle{Binding: b, Client: chronicleFunc(func(context.Context, ca.Request) (*ca.Receipt, error) { return &r, nil })}
				if _, e = sink.Accept(t.Context(), d); e != nil {
					t.Fatalf("known provenance %s refused: %v", retained, e)
				}
			}
		})
	}
}

func TestInvalidChronicleProvenanceRemainsPendingUntilRecovery(t *testing.T) {
	s := memory.New()
	n := durable.NamespaceConfig{InstallationID: "install", Namespace: "ns", AppID: "app", TenantID: "tenant", RequireAudit: true, SchemaVersion: 1}
	if _, err := s.RegisterNamespace(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	audit, err := durable.CaptureSecurityAudit(n.InstallationID, n.Namespace, "read", "allowed", "", durable.AuditMetadata{ActorKind: "anonymous"})
	if err != nil {
		t.Fatal(err)
	}
	d, err := s.AppendSecurityAudit(t.Context(), audit)
	if err != nil {
		t.Fatal(err)
	}
	b := ecosystem.Binding{Producer: "dispatch", InstallationID: n.InstallationID, Namespace: n.Namespace, AppID: n.AppID, TenantID: n.TenantID}
	engine, err := chronicle.New(chronicle.WithStore(cs.NewAdapter(cm.New())))
	if err != nil {
		t.Fatal(err)
	}
	req, err := ecosystem.ChronicleRequest(b, d)
	if err != nil {
		t.Fatal(err)
	}
	good, err := engine.RecordOnce(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	var repaired atomic.Bool
	sink := ecosystem.Chronicle{Binding: b, Client: chronicleFunc(func(ctx context.Context, r ca.Request) (*ca.Receipt, error) {
		receipt, e := engine.RecordOnce(ctx, r)
		if e != nil {
			return nil, e
		}
		returned := *receipt
		if !repaired.Load() {
			returned.Hash = "x"
			returned.HashScheme = "unknown"
		}
		return &returned, nil
	})}
	cfg := delivery.Config{InstallationID: n.InstallationID, Owner: "provenance", Concurrency: 1, PollInterval: time.Millisecond, CallTimeout: time.Second, StoreTimeout: time.Second, LeaseDuration: 3 * time.Second, RetryMin: time.Millisecond, RetryMax: 5 * time.Millisecond}
	publisher, err := delivery.New(s, cfg, map[durable.Destination]delivery.Sink{durable.DestinationChronicle: sink})
	if err != nil {
		t.Fatal(err)
	}
	if err = publisher.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		repaired.Store(true)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if stopErr := publisher.Stop(ctx); stopErr != nil {
			t.Error(stopErr)
		}
	}()
	statusReq := durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: n.InstallationID, Destination: durable.DestinationChronicle}, Limit: 1}
	deadline := time.Now().Add(3 * time.Second)
	for {
		status, e := s.DeliveryStatus(t.Context(), statusReq)
		if e != nil {
			t.Fatal(e)
		}
		if status.Pending == 0 {
			t.Fatal("invalid Chronicle provenance reached acknowledgement")
		}
		if status.Blocked != 0 {
			t.Fatal("invalid receipt became permanent conflict")
		}
		if len(status.Records) == 1 && status.Records[0].ErrorCategory != "" {
			if status.Records[0].Receipt.ID != "" {
				t.Fatal("invalid receipt persisted")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("invalid receipt was not processed")
		}
		time.Sleep(time.Millisecond)
	}
	repaired.Store(true)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err = publisher.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := s.DeliveryStatus(t.Context(), statusReq)
	if err != nil {
		t.Fatal(err)
	}
	if status.Pending != 0 || status.Blocked != 0 || len(status.Records) != 1 || status.Records[0].Receipt.ID != good.EventID.String() || status.Records[0].Receipt.Verify(d) != nil {
		t.Fatalf("original committed receipt not recovered: %+v", status)
	}
	var evidence ca.Receipt
	if err = json.Unmarshal([]byte(status.Records[0].Receipt.Evidence), &evidence); err != nil {
		t.Fatal(err)
	}
	if evidence != *good {
		t.Fatal("recovery changed accepted Chronicle receipt")
	}
}
