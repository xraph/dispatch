package operatorhost

import (
	"context"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
)

// StartChroniclePublisher binds one trusted namespace to its native sink. It has
// its own lifetime; stopping workflow polling does not stop audit publication.
// Stop the returned publisher before closing the host's store.
func (h *Host) StartChroniclePublisher(ctx context.Context, binding ecosystem.Binding, endpoint, bearer string) (func(context.Context) error, error) {
	if h.lifecycle == nil || binding.Validate() != nil {
		return nil, durable.ErrInvalid
	}
	record, err := h.Store.GetNamespace(ctx, binding.InstallationID, binding.Namespace)
	if err != nil {
		return nil, err
	}
	if record.AppID != binding.AppID || record.TenantID != binding.TenantID || !record.RequireAudit {
		return nil, durable.ErrInvalid
	}
	remote, err := ecosystem.NewRemote(endpoint, bearer, 2*time.Second, true)
	if err != nil {
		return nil, err
	}
	publisher, err := delivery.New(h.Store, delivery.Config{InstallationID: binding.InstallationID, Owner: "operator-lifecycle-publisher", Concurrency: 1, PollInterval: 50 * time.Millisecond, CallTimeout: 3 * time.Second, StoreTimeout: time.Second, LeaseDuration: 5 * time.Second, RetryMin: 100 * time.Millisecond, RetryMax: 500 * time.Millisecond}, map[durable.Destination]delivery.Sink{durable.DestinationChronicle: ecosystem.Chronicle{Binding: binding, Client: remote}})
	if err != nil {
		remote.Close()
		return nil, err
	}
	if err = publisher.Start(); err != nil {
		remote.Close()
		return nil, err
	}
	return func(ctx context.Context) error { err := publisher.Stop(ctx); remote.Close(); return err }, nil
}
