package operatorhost

import (
	"context"
	"errors"
	"testing"

	aid "github.com/xraph/authsome/id"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	"github.com/xraph/dispatch/qualification/internal/authority"
	"github.com/xraph/dispatch/qualification/internal/sinkhost"
	"github.com/xraph/dispatch/store/memory"
)

func hostChronicleRoutes(t *testing.T, h *Host) []sinkhost.ChronicleRoute {
	t.Helper()
	routes := make([]sinkhost.ChronicleRoute, 0, 3)
	for i, namespace := range []string{"production", "audit", "foreign"} {
		record, err := h.Store.GetNamespace(t.Context(), "operator-host", namespace)
		if err != nil {
			t.Fatal(err)
		}
		routes = append(routes, sinkhost.ChronicleRoute{Binding: ecosystem.Binding{Producer: "dispatch-lifecycle", InstallationID: "operator-host", Namespace: namespace, AppID: record.AppID, TenantID: record.TenantID}, Address: []string{"127.0.0.1:12001", "127.0.0.1:12002", "127.0.0.1:12003"}[i], Credential: authority.Credential{KeyID: aid.NewAPIKeyID(), AccountID: aid.NewServiceAccountID(), Secret: "private"}})
	}
	return routes
}

func TestChroniclePublisherRequiresCompletePersistedBinding(t *testing.T) {
	c := newConfiguredCommandClient(t, memory.New(), &LifecycleOptions{InstanceID: "publisher-binding", SkipSampleExecutions: true})
	routes := hostChronicleRoutes(t, c.host)
	if err := c.host.validateChronicleRoutes(t.Context(), routes); err != nil {
		t.Fatal(err)
	}
	if err := c.host.StartChroniclePublisher(t.Context(), routes[:1]); !errors.Is(err, ErrMissingChronicleRoute) {
		t.Fatal("legacy profile did not fail closed with missing_route")
	}
	routes[1].Binding.TenantID = "foreign"
	if err := c.host.StartChroniclePublisher(t.Context(), routes); !errors.Is(err, ErrChronicleOwnership) {
		t.Fatal("publisher accepted foreign persisted ownership")
	}
	routes = hostChronicleRoutes(t, c.host)
	if _, err := c.host.Store.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: "operator-host", Namespace: "extra", AppID: routes[0].Binding.AppID, TenantID: "tenant-extra", RequireAudit: true, RequireHooks: true, SchemaVersion: 1}); err != nil {
		t.Fatal(err)
	}
	if err := c.host.StartChroniclePublisher(t.Context(), routes); !errors.Is(err, ErrChronicleOwnership) {
		t.Fatal("oversized persisted profile accepted")
	}
}

func TestChronicleRouterRefusesUnknownOwnership(t *testing.T) {
	for _, d := range []durable.Delivery{{InstallationID: "operator-host", Namespace: "audit", AppID: "foreign", TenantID: "tenant-audit"}, {InstallationID: "unknown", Namespace: "production"}} {
		if _, err := (chronicleRouter{}).Accept(context.Background(), d); !errors.Is(err, ErrChronicleOwnership) {
			t.Fatal("unknown route accepted")
		}
	}
}

type cancelNamespaceStore struct {
	Store
	cancel context.CancelFunc
}

func (s *cancelNamespaceStore) ListNamespaces(ctx context.Context, list durable.NamespaceList) ([]durable.NamespaceRecord, error) {
	records, err := s.Store.ListNamespaces(ctx, list)
	s.cancel()
	return records, err
}

func TestChroniclePublisherCancellationDuringValidation(t *testing.T) {
	c := newConfiguredCommandClient(t, memory.New(), &LifecycleOptions{InstanceID: "publisher-cancel", SkipSampleExecutions: true})
	routes := hostChronicleRoutes(t, c.host)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c.host.Store = &cancelNamespaceStore{Store: c.host.Store, cancel: cancel}
	if err := c.host.StartChroniclePublisher(ctx, routes); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled validation started publication")
	}
	if c.host.publisher != nil || len(c.host.chronicleRemotes) != 0 {
		t.Fatal("canceled startup retained publisher resources")
	}
}
