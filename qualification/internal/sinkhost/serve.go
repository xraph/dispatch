package sinkhost

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	apg "github.com/xraph/authsome/store/postgres"
	"github.com/xraph/chronicle"
	ca "github.com/xraph/chronicle/acceptance"
	"github.com/xraph/chronicle/hash"
	"github.com/xraph/chronicle/keys"
	cs "github.com/xraph/chronicle/store"
	cpg "github.com/xraph/chronicle/store/postgres"
	"github.com/xraph/forge"
	"github.com/xraph/forge/extensions/auth"
	"github.com/xraph/relay"
	ra "github.com/xraph/relay/acceptance"
	rpg "github.com/xraph/relay/store/postgres"
	"github.com/xraph/warden"
	wpg "github.com/xraph/warden/store/postgres"

	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	"github.com/xraph/dispatch/qualification/internal/authority"
)

type keyProvider struct{ key []byte }

func (p keyProvider) Current(context.Context, keys.Use) (key []byte, id string, err error) {
	return append([]byte(nil), p.key...), "qualification-hmac-1", nil
}
func (p keyProvider) ByID(_ context.Context, id string) ([]byte, error) {
	if id != "qualification-hmac-1" {
		return nil, keys.ErrKeyNotFound
	}
	return append([]byte(nil), p.key...), nil
}

// Serve binds only the configured loopback interface. Every acceptance route
// uses a real Authsome provider and Warden destination check. No default sink,
// Authsome or Warden extension routes are registered.
func Serve(ctx context.Context, role string, c Config) error {
	if err := c.Validate(); err != nil {
		return err
	}
	if role == "receiver" {
		return serveReceiver(ctx, c)
	}
	app := forge.New(forge.WithAppName("dispatch-qualification"), forge.WithAppLogger(forge.NewNoopLogger()))
	registry := auth.NewRegistry(app.Container(), forge.NewNoopLogger())
	authDB, err := Open(ctx, c.DSNs["authsome"])
	if err != nil {
		return err
	}
	defer authDB.Close()
	policyDB, err := Open(ctx, c.DSNs["warden"])
	if err != nil {
		return err
	}
	defer policyDB.Close()
	w, err := warden.NewEngine(warden.WithStore(wpg.New(policyDB)))
	if err != nil {
		return err
	}
	authorityEngine, err := authority.NewEncrypted(apg.New(authDB), w, c.Binding.AppID, registry, c.TokenKey)
	if err != nil {
		return err
	}
	if setupErr := authorityEngine.Start(ctx); setupErr != nil {
		return setupErr
	}
	defer func() {
		if stopErr := authorityEngine.Stop(context.Background()); stopErr != nil {
			fmtSafe("authority shutdown incomplete")
		}
	}()
	credentials := []authority.Credential{c.Credentials[role]}
	if role == "dispatch" {
		credentials = []authority.Credential{c.Credentials["operator"], c.Credentials["denied"]}
	}
	provider := &authority.Provider{Engine: authorityEngine, EnvironmentID: c.EnvironmentID, Binding: c.Binding, Credentials: credentials}
	if setupErr := registry.Register(provider); setupErr != nil {
		return setupErr
	}
	if role == "dispatch" {
		return serveDispatch(ctx, app, registry, w, provider, c)
	}
	if role != "chronicle" && role != "relay" {
		return errors.New("qualification: unsupported role")
	}
	db, err := Open(ctx, c.DSNs[role])
	if err != nil {
		return err
	}
	defer db.Close()
	var accept forge.Handler
	switch role {
	case "chronicle":
		engine, e := chronicle.New(chronicle.WithStore(cs.NewAdapter(cpg.New(db))), chronicle.WithDigestScheme(hash.SchemeHMACV5), chronicle.WithKeyProvider(keyProvider{c.HMACKey}))
		if e != nil {
			return e
		}
		accept = func(f forge.Context) error {
			var req ca.Request
			if e := decode(f.Request(), &req); e != nil || VerifyChronicle(c.Binding, req) != nil {
				return f.String(400, "invalid acceptance")
			}
			receipt, e := engine.RecordOnce(f.Context(), req)
			if errors.Is(e, ca.ErrConflict) {
				fp, _ := ca.Fingerprint(req)
				return f.JSON(409, ecosystem.ConflictResponse{Destination: role, SourceKey: req.SourceKey, SourceFingerprint: req.SourceFingerprint, Fingerprint: fp})
			}
			if e != nil {
				return f.String(503, "acceptance unavailable")
			}
			return f.JSON(200, receipt)
		}
	case "relay":
		engine, e := relay.New(relay.WithStore(rpg.New(db)), relay.WithConcurrency(2), relay.WithPollInterval(100*time.Millisecond), relay.WithMaxPollInterval(time.Second), relay.WithRequestTimeout(2*time.Second), relay.WithRetrySchedule([]time.Duration{100 * time.Millisecond, 200 * time.Millisecond, time.Second}))
		if e != nil {
			return e
		}
		if e := ecosystem.RegisterRelaySchema(ctx, engine.Catalog(), c.Binding.AppID); e != nil {
			return e
		}
		engine.Start(ctx)
		defer engine.Stop(context.Background())
		accept = func(f forge.Context) error {
			var req ra.Request
			if e := decode(f.Request(), &req); e != nil || VerifyRelay(c.Binding, req) != nil {
				return f.String(400, "invalid acceptance")
			}
			receipt, e := engine.SendReliable(f.Context(), req)
			if errors.Is(e, ra.ErrConflict) {
				fp, _ := ra.Fingerprint(req)
				return f.JSON(409, ecosystem.ConflictResponse{Destination: role, SourceKey: req.SourceKey, SourceFingerprint: req.SourceFingerprint, Fingerprint: fp})
			}
			if e != nil {
				return f.String(503, "acceptance unavailable")
			}
			return f.JSON(200, receipt)
		}
	}
	protected := registry.MiddlewareWithRequirement(auth.Requirement{Providers: []string{authority.ProviderName}, Scopes: []string{Scope(role)}})(func(f forge.Context) error {
		identity, ok := auth.GetAuthContext(f)
		if !ok {
			return f.String(401, "authentication required")
		}
		e := authority.Authorize(f.Context(), w, c.Binding, c.PolicyTenant, identity.Subject, role, Scope(role))
		if errors.Is(e, authority.ErrDenied) {
			return f.String(403, "access denied")
		}
		if e != nil {
			return f.String(503, "policy unavailable")
		}
		return accept(f)
	})
	if err := app.Router().POST("/accept", protected, forge.WithRequiredAuth(authority.ProviderName, Scope(role))); err != nil {
		return err
	}
	return listen(ctx, app.Router(), c.Addresses[role], role)
}
func listen(ctx context.Context, router forge.Router, address, role string) error {
	if err := router.GET("/health", func(f forge.Context) error { return f.String(200, "ready") }); err != nil {
		return err
	}
	server := &http.Server{Addr: address, Handler: router.Handler(), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 10 * time.Second, MaxHeaderBytes: 16 << 10}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", address)
	if err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	fmt.Println(role + " ready")
	select {
	case err = <-done:
	case <-ctx.Done():
		stop, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err = server.Shutdown(stop)
		if err == nil {
			err = <-done
		}
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
