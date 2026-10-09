package sinkhost

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/xraph/forge"
	"github.com/xraph/forge/extensions/auth"
	"github.com/xraph/warden"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/extension"
	"github.com/xraph/dispatch/qualification/internal/authority"
	"github.com/xraph/dispatch/security"
	dpg "github.com/xraph/dispatch/store/postgres"
)

type lostAck struct {
	sink   delivery.Sink
	marker string
	once   sync.Once
}

func (s *lostAck) Accept(ctx context.Context, d durable.Delivery) (durable.SinkReceipt, error) {
	receipt, err := s.sink.Accept(ctx, d)
	if err != nil {
		return receipt, err
	}
	lose := false
	s.once.Do(func() { lose = true })
	if lose {
		raw, e := json.Marshal(struct {
			Delivery durable.Delivery
			Receipt  durable.SinkReceipt
		}{d, receipt})
		if e != nil {
			return durable.SinkReceipt{}, e
		}
		if writeErr := os.WriteFile(s.marker+".tmp", raw, 0o600); writeErr != nil {
			return durable.SinkReceipt{}, writeErr
		}
		if renameErr := os.Rename(s.marker+".tmp", s.marker); renameErr != nil {
			return durable.SinkReceipt{}, renameErr
		}
		<-ctx.Done()
		return durable.SinkReceipt{}, ctx.Err()
	}
	return receipt, nil
}
func serveDispatch(ctx context.Context, app forge.App, registry auth.Registry, w *warden.Engine, provider *authority.Provider, c Config) (returnErr error) {
	db, err := Open(ctx, c.DSNs["dispatch"])
	if err != nil {
		return err
	}
	store := dpg.New(db)
	sinks := map[durable.Destination]delivery.Sink{}
	var remotes []*ecosystem.Remote
	defer func() {
		for _, remote := range remotes {
			remote.Close()
		}
	}()
	for _, destination := range []durable.Destination{durable.DestinationChronicle, durable.DestinationRelay} {
		role := string(destination)
		remote, e := ecosystem.NewRemote("http://"+c.Addresses[role]+"/accept", c.Credentials[role].Secret, 2*time.Second, true)
		if e != nil {
			return e
		}
		remotes = append(remotes, remote)
		if destination == durable.DestinationChronicle {
			sinks[destination] = ecosystem.Chronicle{Binding: c.Binding, Client: remote}
		} else {
			sinks[destination] = ecosystem.Relay{Binding: c.Binding, Client: remote}
		}
		if role == c.LostAckDestination {
			sinks[destination] = &lostAck{sink: sinks[destination], marker: c.LostAckMarker}
		}
	}
	n := durable.NamespaceConfig{InstallationID: c.Binding.InstallationID, Namespace: c.Binding.Namespace, AppID: c.Binding.AppID, TenantID: c.Binding.TenantID, RequireAudit: true, RequireHooks: true, SchemaVersion: 1}
	boundary := security.Boundary{Resource: security.Resource{InstallationID: c.Binding.InstallationID, PolicyTenant: c.PolicyTenant}, Authorizer: &security.WardenAuthorizer{Engine: func() (*warden.Engine, error) { return w, nil }}, Audit: &security.AuditService{}}
	cfg := extension.DeliveryConfig{AuditNamespace: n, Namespaces: []durable.NamespaceConfig{n}, Publisher: delivery.Config{InstallationID: c.Binding.InstallationID, Owner: "process-publisher", Concurrency: 1, PollInterval: 50 * time.Millisecond, CallTimeout: 3 * time.Second, StoreTimeout: time.Second, LeaseDuration: 5 * time.Second, RetryMin: 100 * time.Millisecond, RetryMax: 500 * time.Millisecond}, Sinks: sinks, ShutdownTimeout: 3 * time.Second}
	e := extension.New(extension.WithStore(store), extension.WithDisableRoutes(), extension.WithConcurrency(1), extension.WithPollInterval(100*time.Millisecond), extension.WithRemoteSecurity(security.NewForgeAuthenticator(func() (auth.Registry, error) { return registry, nil }, authority.ProviderName), boundary), extension.WithDurableDelivery(cfg), extension.WithDurableWorkflows(drt.Options{Namespace: n.Namespace, Queue: "work", BuildID: "v1", Owner: "process-worker", LeaseDuration: 3 * time.Second, StoreTimeout: 500 * time.Millisecond, PollInterval: 100 * time.Millisecond, Concurrency: 1, Workflows: map[string]drt.WorkflowFunc{"qualification": func(*drt.Workflow, []byte) ([]byte, error) { return []byte(`"completed"`), nil }}}))
	if err := e.Register(app); err != nil {
		return err
	}
	if err := e.Start(ctx); err != nil {
		return err
	}
	defer func() {
		stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if stopErr := e.Stop(stop); stopErr != nil {
			fmtSafe("dispatch shutdown incomplete")
			returnErr = errors.Join(returnErr, stopErr)
		}
	}()
	if err := boundary.Audit.Activate(ctx, store, store, boundary.Resource, n.Namespace, false); err != nil {
		return err
	}
	guard := func(handler forge.Handler) forge.Handler {
		return registry.MiddlewareWithRequirement(auth.Requirement{Providers: []string{provider.Name()}, Scopes: []string{security.OperatorWrite}})(handler)
	}
	command := guard(func(f forge.Context) error {
		var request struct {
			ID string `json:"id"`
		}
		if decode(f.Request(), &request) != nil || !durable.DeliveryIdentifier(request.ID) {
			return f.String(400, "invalid command")
		}
		identity, ok := auth.GetAuthContext(f)
		if !ok {
			return f.String(401, "authentication required")
		}
		principal := security.Principal{Subject: identity.Subject, Kind: "service_acct"}
		op := security.Operation{Action: security.OperatorWrite, AuditAction: "qualification:start", Target: request.ID}
		if check := boundary.Check(f.Context(), principal, op); check != nil {
			if errors.Is(check, security.ErrForbidden) {
				return f.String(403, "access denied")
			}
			return f.String(503, "command unavailable")
		}
		metadata := durable.AuditMetadata{ActorKind: "service_acct", ActorID: identity.Subject, RequestID: request.ID}
		receipt, startErr := e.Engine().StartDurableWorkflow(durable.WithAuditMetadata(f.Context(), metadata), durable.StartRequest{Key: durable.Key{Namespace: n.Namespace, WorkflowID: request.ID, RunID: "run"}, RequestID: request.ID, WorkflowType: "qualification", BuildID: "v1", Queue: "work", Input: []byte(`{"secret":"qualification-payload-must-not-leak"}`)})
		if startErr != nil {
			return f.String(503, "command unavailable")
		}
		return f.JSON(202, receipt)
	})
	if err := app.Router().POST("/command", command, forge.WithRequiredAuth(authority.ProviderName, security.OperatorWrite)); err != nil {
		return err
	}
	if err := app.Router().POST("/workers/stop", guard(func(f forge.Context) error {
		identity, ok := auth.GetAuthContext(f)
		if !ok {
			return f.String(401, "authentication required")
		}
		if check := boundary.Check(f.Context(), security.Principal{Subject: identity.Subject, Kind: "service_acct"}, security.Operation{Action: security.OperatorWrite, AuditAction: "qualification:stop-workers", Target: "installation"}); check != nil {
			return f.String(403, "access denied")
		}
		stop, cancel := context.WithTimeout(f.Context(), 2*time.Second)
		defer cancel()
		if stopErr := e.Engine().StopWorkers(stop); stopErr != nil {
			return f.String(503, "worker shutdown incomplete")
		}
		return f.String(200, "workers stopped")
	}), forge.WithRequiredAuth(authority.ProviderName, security.OperatorWrite)); err != nil {
		return err
	}
	return listen(ctx, app.Router(), c.Addresses["dispatch"], "dispatch")
}
