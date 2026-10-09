package extension_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	forgetesting "github.com/xraph/forge/testing"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery"
	"github.com/xraph/dispatch/extension"
	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/store/memory"
)

type acceptingSink struct{}

func (acceptingSink) Accept(_ context.Context, d durable.Delivery) (durable.SinkReceipt, error) {
	return durable.SinkReceipt{ID: "accepted-" + d.ID, DeliveryID: d.ID, Destination: d.Destination, SchemaVersion: d.SchemaVersion, Fingerprint: d.Fingerprint}, nil
}
func auditConfig() extension.DeliveryConfig {
	return extension.DeliveryConfig{AuditNamespace: durable.NamespaceConfig{InstallationID: "installation", Namespace: "operator-audit", AppID: "app", TenantID: "policy-tenant", RequireAudit: true, SchemaVersion: 1}, Publisher: delivery.Config{Owner: "publisher", PollInterval: time.Millisecond}, Sinks: map[durable.Destination]delivery.Sink{durable.DestinationChronicle: acceptingSink{}}}
}
func TestDeliveryRequiresActivationAndExplicitMemory(t *testing.T) {
	for _, mode := range []string{"missing", "production-memory", "mismatch", "test"} {
		t.Run(mode, func(t *testing.T) {
			s := memory.New()
			auth := security.AuthenticatorFunc(func(context.Context, *http.Request) (security.Principal, error) {
				return security.Principal{Subject: "operator", Kind: "user"}, nil
			})
			b := security.Boundary{Resource: security.Resource{InstallationID: "installation", PolicyTenant: "policy-tenant"}, Authorizer: security.AuthorizerFunc(func(context.Context, security.Principal, string, security.Resource) error { return nil })}
			opts := []extension.ExtOption{extension.WithStore(s), extension.WithRemoteSecurity(auth, b)}
			cfg := auditConfig()
			switch mode {
			case "test":
				opts = append(opts, extension.WithMemoryAuditForTesting(cfg))
			case "production-memory":
				opts = append(opts, extension.WithDurableDelivery(cfg))
			case "mismatch":
				cfg.AuditNamespace.TenantID = "foreign"
				opts = append(opts, extension.WithMemoryAuditForTesting(cfg))
			}
			e := extension.New(opts...)
			app := forgetesting.NewTestApp("delivery", "1")
			if err := e.Register(app); err != nil {
				t.Fatal(err)
			}
			read := func() int {
				rec := httptest.NewRecorder()
				app.Router().Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/dispatch/v1/stats", nil))
				return rec.Code
			}
			if code := read(); code != 503 {
				t.Fatal("usable before Start", code)
			}
			if _, err := s.GetNamespace(t.Context(), "installation", "operator-audit"); !errors.Is(err, durable.ErrNotFound) {
				t.Fatal("registered before migration", err)
			}
			err := e.Start(t.Context())
			if mode == "production-memory" || mode == "mismatch" {
				if err == nil {
					t.Fatal("invalid composition started")
				}
				if read() != 503 {
					t.Fatal("failed startup usable")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := 503
			if mode == "test" {
				want = 200
			}
			if read() != want {
				t.Fatal("unexpected admission")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err = e.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			if read() != 503 {
				t.Fatal("stopped audit admitted request")
			}
		})
	}
}
