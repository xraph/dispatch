package extension

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery"
	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/store/postgres"
)

// DeliveryConfig is explicit host composition. Sinks must implement durable
// idempotent acceptance. Use durable/delivery/ecosystem for Chronicle and Relay.
type DeliveryConfig struct {
	AuditNamespace              durable.NamespaceConfig
	Namespaces                  []durable.NamespaceConfig
	Publisher                   delivery.Config
	Sinks                       map[durable.Destination]delivery.Sink
	AllowNamespaceAuditFallback bool
	ShutdownTimeout             time.Duration
}

func WithDurableDelivery(config DeliveryConfig) ExtOption {
	config.Namespaces = append([]durable.NamespaceConfig(nil), config.Namespaces...)
	config.Sinks = maps.Clone(config.Sinks)
	return func(e *Extension) {
		clone := config
		clone.Namespaces = append([]durable.NamespaceConfig(nil), config.Namespaces...)
		clone.Sinks = maps.Clone(config.Sinks)
		e.deliveryConfig = &clone
	}
}

// WithMemoryAuditForTesting explicitly enables the memory store composition.
// It is not a production durability mode and still requires real local intents.
func WithMemoryAuditForTesting(config DeliveryConfig) ExtOption {
	option := WithDurableDelivery(config)
	return func(e *Extension) { option(e); e.memoryAuditForTesting = true }
}
func (e *Extension) prepareDelivery(ctx context.Context) error {
	if e.deliveryConfig == nil {
		return nil
	}
	cfg := e.deliveryConfig
	store := e.eng.Dispatcher().Store()
	switch store.(type) {
	case *postgres.Store:
	case *memory.Store:
		if !e.memoryAuditForTesting {
			return errors.New("dispatch: memory audit requires explicit test composition")
		}
	default:
		return errors.New("dispatch: durable delivery requires PostgreSQL")
	}
	outbox, ok := store.(durable.OutboxStore)
	if !ok {
		return errors.New("dispatch: outbox store required")
	}
	catalog, ok := store.(durable.NamespaceStore)
	if !ok {
		return errors.New("dispatch: namespace store required")
	}
	if _, ok = store.(durable.LegacyAuditStore); !ok {
		return errors.New("dispatch: legacy audit store required")
	}
	audit := cfg.AuditNamespace
	if audit.Validate() != nil || !audit.RequireAudit || audit.InstallationID != e.boundary.Resource.InstallationID || audit.TenantID != e.boundary.Resource.PolicyTenant {
		return errors.New("dispatch: audit namespace does not match security resource")
	}
	namespaces := append([]durable.NamespaceConfig{audit}, cfg.Namespaces...)
	if cfg.Publisher.InstallationID == "" {
		cfg.Publisher.InstallationID = audit.InstallationID
	}
	if cfg.Publisher.InstallationID != audit.InstallationID {
		return errors.New("dispatch: publisher installation mismatch")
	}
	for _, n := range namespaces {
		if n.Validate() != nil || n.InstallationID != audit.InstallationID || n.TenantID != audit.TenantID {
			return errors.New("dispatch: invalid delivery namespace ownership")
		}
		if n.RequireAudit && cfg.Sinks[durable.DestinationChronicle] == nil || n.RequireHooks && cfg.Sinks[durable.DestinationRelay] == nil {
			return errors.New("dispatch: required delivery sink is missing")
		}
	}
	// Worker routing must already have immutable catalog ownership.
	workerNamespace := ""
	if e.durable != nil {
		workerNamespace = e.durable.Namespace
	} else if e.config.Durable.Enabled {
		workerNamespace = e.config.Durable.Namespace
	}
	if workerNamespace != "" {
		found := false
		for _, n := range cfg.Namespaces {
			if n.Namespace == workerNamespace && n.RequireAudit {
				found = true
			}
		}
		if !found {
			return errors.New("dispatch: audited worker namespace is required")
		}
	}
	p, err := delivery.New(outbox, cfg.Publisher, cfg.Sinks)
	if err != nil {
		return fmt.Errorf("dispatch: publisher configuration: %w", err)
	}
	for _, n := range namespaces {
		if _, err = catalog.RegisterNamespace(ctx, n); err != nil {
			return fmt.Errorf("dispatch: namespace registration: %w", err)
		}
	}
	// Retained ownership can outlive this process configuration. Required sinks
	// for earlier namespaces must remain available to the same installation.
	cursor := ""
	for {
		records, listErr := catalog.ListNamespaces(ctx, durable.NamespaceList{InstallationID: audit.InstallationID, After: cursor, Limit: durable.MaxDeliveryBatch})
		if listErr != nil {
			return listErr
		}
		for _, record := range records {
			if record.RequireAudit && cfg.Sinks[durable.DestinationChronicle] == nil || record.RequireHooks && cfg.Sinks[durable.DestinationRelay] == nil {
				return errors.New("dispatch: persisted namespace requires missing sink")
			}
		}
		if len(records) < durable.MaxDeliveryBatch {
			break
		}
		cursor = records[len(records)-1].Namespace
	}
	e.publisher = p
	return nil
}
func (e *Extension) activateAudit(ctx context.Context) error {
	if e.deliveryConfig == nil {
		return nil
	}
	store := e.eng.Dispatcher().Store()
	outbox, ok := store.(durable.OutboxStore)
	if !ok {
		return security.ErrUnavailable
	}
	catalog, ok := store.(durable.NamespaceStore)
	if !ok {
		return security.ErrUnavailable
	}
	return e.boundary.Audit.Activate(ctx, outbox, catalog, e.boundary.Resource, e.deliveryConfig.AuditNamespace.Namespace, e.deliveryConfig.AllowNamespaceAuditFallback)
}
func (e *Extension) stopDelivery(ctx context.Context) error {
	e.boundary.Audit.Deactivate()
	if e.publisher == nil {
		return nil
	}
	timeout := e.deliveryConfig.ShutdownTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return e.publisher.Stop(ctx)
}

// DeliveryStatus reports backlog separately from engine execution readiness.
// Sink outage does not fail Health while local acceptance remains available.
func (e *Extension) DeliveryStatus(ctx context.Context, destination durable.Destination) (delivery.Status, error) {
	if e.publisher == nil {
		return delivery.Status{}, errors.New("dispatch: publisher not configured")
	}
	return e.publisher.Status(ctx, destination)
}

// AuditAcceptanceFailures exposes local security audit degradation without
// provider text. Unresolved command evidence is retained in LegacyAuditStore.
func (e *Extension) AuditAcceptanceFailures() uint64 {
	if e.boundary == nil {
		return 0
	}
	return e.boundary.Audit.AcceptanceFailures()
}
