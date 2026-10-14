//go:build linux

package launcher

import (
	"context"
	"errors"
	"time"

	"github.com/xraph/authsome/app"
	"github.com/xraph/authsome/environment"
	aid "github.com/xraph/authsome/id"
	apg "github.com/xraph/authsome/store/postgres"
	"github.com/xraph/forge"
	"github.com/xraph/forge/extensions/auth"
	"github.com/xraph/warden"
	wpg "github.com/xraph/warden/store/postgres"

	"github.com/xraph/dispatch/qualification/internal/authority"
)

// QualificationServiceConfiguration names dedicated, disposable PostgreSQL
// databases and root-private directories for the real service enrollment drill.
// The caller installs proxy-ca.pem, proxy-ca-key.pem and upstream-ca.pem first.
type QualificationServiceConfiguration struct {
	ConfigurationDirectory string
	StateDirectory         string
	AuthDSN                string
	PolicyDSN              string
	TokenKey               []byte
	Tenant                 string
}

// OpenQualificationServiceHost migrates dedicated Authsome/Warden databases and
// creates a real application/environment. It enables only service enrollment: no
// outer ledger, artifact certification, launcher delegation or floor admission.
func OpenQualificationServiceHost(ctx context.Context, c QualificationServiceConfiguration) (*Host, func() error, error) {
	if len(c.TokenKey) != 32 || !token(c.Tenant) || c.AuthDSN == "" || c.PolicyDSN == "" || c.ConfigurationDirectory == c.StateDirectory {
		return nil, nil, ErrRefused
	}
	if err := secureDirectory(c.ConfigurationDirectory); err != nil {
		return nil, nil, err
	}
	if err := secureDirectory(c.StateDirectory); err != nil {
		return nil, nil, err
	}
	authDB, err := openGrove(ctx, c.AuthDSN)
	if err != nil {
		return nil, nil, err
	}
	policyDB, err := openGrove(ctx, c.PolicyDSN)
	if err != nil {
		return nil, nil, errors.Join(err, authDB.Close())
	}
	closeDB := func() error { return errors.Join(authDB.Close(), policyDB.Close()) }
	authStore := apg.New(authDB)
	policies := wpg.New(policyDB)
	if err = authStore.Migrate(ctx); err != nil {
		return nil, nil, errors.Join(err, closeDB())
	}
	if err = policies.Migrate(ctx); err != nil {
		return nil, nil, errors.Join(err, closeDB())
	}
	policyEngine, err := warden.NewEngine(warden.WithStore(policies))
	if err != nil {
		return nil, nil, errors.Join(err, closeDB())
	}
	appID := aid.NewAppID()
	now := time.Now().UTC()
	if err = authStore.CreateApp(ctx, &app.App{ID: appID, Name: "Managed service qualification", Slug: "managed-service-qualification", IsPlatform: true, CreatedAt: now, UpdatedAt: now}); err != nil {
		return nil, nil, errors.Join(err, closeDB())
	}
	forgeApp := forge.New(forge.WithAppName("managed-service-qualification"), forge.WithAppLogger(forge.NewNoopLogger()))
	registry := auth.NewRegistry(forgeApp.Container(), forge.NewNoopLogger())
	engine, err := authority.NewEncrypted(authStore, policyEngine, appID.String(), registry, c.TokenKey)
	if err != nil {
		return nil, nil, errors.Join(err, closeDB())
	}
	if err = engine.Start(ctx); err != nil {
		return nil, nil, errors.Join(err, closeDB())
	}
	cleanup := func() error {
		bounded, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return errors.Join(engine.Stop(bounded), closeDB())
	}
	env := &environment.Environment{ID: aid.NewEnvironmentID(), AppID: appID, Name: "Service qualification", Slug: "service-qualification", Type: environment.TypeDevelopment}
	if err = engine.CreateEnvironment(ctx, env); err != nil {
		return nil, nil, errors.Join(err, cleanup())
	}
	return &Host{root: c.StateDirectory, configDirectory: c.ConfigurationDirectory, engine: engine, app: appID, environment: env.ID, tenant: c.Tenant, policies: policies}, cleanup, nil
}
