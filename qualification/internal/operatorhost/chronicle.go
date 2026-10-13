package operatorhost

import (
	"context"
	"errors"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	"github.com/xraph/dispatch/qualification/internal/sinkhost"
)

// ErrMissingChronicleRoute means a persisted audited namespace has no explicit route.
var ErrMissingChronicleRoute = errors.New("qualification: missing Chronicle route")

// ErrChronicleOwnership means configured and persisted namespace ownership differ.
var ErrChronicleOwnership = errors.New("qualification: Chronicle ownership conflict")

type chronicleTuple struct{ installation, namespace, app, tenant string }

type chronicleRouter map[chronicleTuple]delivery.Sink

func (r chronicleRouter) Accept(ctx context.Context, d durable.Delivery) (durable.SinkReceipt, error) {
	sink := r[chronicleTuple{d.InstallationID, d.Namespace, d.AppID, d.TenantID}]
	if sink == nil {
		return durable.SinkReceipt{}, ErrChronicleOwnership
	}
	return sink.Accept(ctx, d)
}

func (h *Host) validateChronicleRoutes(ctx context.Context, routes []sinkhost.ChronicleRoute) error {
	if h.lifecycle == nil || sinkhost.ValidateChronicleRoutes(routes) != nil {
		return sinkhost.ErrInvalidProfile
	}
	records, err := h.Store.ListNamespaces(ctx, durable.NamespaceList{InstallationID: "operator-host", Limit: 4})
	if err != nil {
		return err
	}
	if len(records) != 3 {
		return ErrChronicleOwnership
	}
	configured := map[string]sinkhost.ChronicleRoute{}
	for _, route := range routes {
		if route.Binding.InstallationID != "operator-host" || route.Binding.Producer != "dispatch-lifecycle" || route.Binding.OrgID != "" {
			return ErrChronicleOwnership
		}
		configured[route.Binding.Namespace] = route
	}
	for _, record := range records {
		if (record.Namespace != "production" && record.Namespace != "audit" && record.Namespace != "foreign") || !record.RequireAudit || !record.RequireHooks || record.TenantID != "tenant-"+record.Namespace {
			return ErrChronicleOwnership
		}
		route, ok := configured[record.Namespace]
		if !ok {
			return ErrMissingChronicleRoute
		}
		if record.AppID != route.Binding.AppID || record.TenantID != route.Binding.TenantID {
			return ErrChronicleOwnership
		}
		delete(configured, record.Namespace)
	}
	if len(configured) != 0 {
		return ErrChronicleOwnership
	}
	return nil
}

// StartChroniclePublisher validates all persisted ownership before starting delivery.
// The dispatcher owns drain and keeps storage open when drain is incomplete.
func (h *Host) StartChroniclePublisher(ctx context.Context, routes []sinkhost.ChronicleRoute) error {
	if err := h.lockClose(ctx); err != nil {
		return err
	}
	defer func() { <-h.closeGate }()
	if h.closed || h.publisher != nil {
		return sinkhost.ErrInvalidProfile
	}
	started := time.Now()
	err := h.validateChronicleRoutes(ctx, routes)
	if err == nil {
		err = ctx.Err()
	}
	h.diagnose("route_validate", started, err, nil)
	if err != nil {
		return err
	}
	router := chronicleRouter{}
	remotes := []*ecosystem.Remote{}
	for _, route := range routes {
		remote, remoteErr := ecosystem.NewRemote("http://"+route.Address+"/accept", route.Credential.Secret, 2*time.Second, true)
		if remoteErr != nil {
			for _, prior := range remotes {
				prior.Close()
			}
			return remoteErr
		}
		remotes = append(remotes, remote)
		b := route.Binding
		router[chronicleTuple{b.InstallationID, b.Namespace, b.AppID, b.TenantID}] = ecosystem.Chronicle{Binding: b, Client: remote}
	}
	publisherStarted := time.Now()
	publisher, err := delivery.New(h.Store, delivery.Config{InstallationID: "operator-host", Owner: "operator-lifecycle-publisher", Concurrency: 1, PollInterval: 50 * time.Millisecond, CallTimeout: 3 * time.Second, StoreTimeout: time.Second, LeaseDuration: 5 * time.Second, RetryMin: 100 * time.Millisecond, RetryMax: 500 * time.Millisecond}, map[durable.Destination]delivery.Sink{durable.DestinationChronicle: router})
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = publisher.Start()
	}
	h.diagnose("publisher_start", publisherStarted, err, nil)
	if err != nil {
		for _, remote := range remotes {
			remote.Close()
		}
		return err
	}
	h.publisher, h.chronicleRemotes = publisher, remotes
	h.engine.Dispatcher().BeforeStoreClose(h.stopChroniclePublisher)
	return nil
}

func (h *Host) stopChroniclePublisher(ctx context.Context) error {
	started := time.Now()
	var status *delivery.Status
	if snapshot, statusErr := h.publisher.Status(ctx, durable.DestinationChronicle); statusErr == nil {
		status = &snapshot
	}
	err := h.publisher.Stop(ctx)
	// A canceled caller cannot grant a new observation deadline. Unknown counts
	// remain explicit, and a later Close call can retry with a fresh context.
	if ctx.Err() == nil {
		snapshot, statusErr := h.publisher.Status(ctx, durable.DestinationChronicle)
		if statusErr == nil {
			status = &snapshot
		}
	}
	h.diagnose("publisher_drain", started, err, status)
	if err != nil {
		return err
	}
	for _, remote := range h.chronicleRemotes {
		remote.Close()
	}
	h.chronicleRemotes = nil
	return nil
}
