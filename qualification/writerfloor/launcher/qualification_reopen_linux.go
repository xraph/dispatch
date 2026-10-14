//go:build linux

package launcher

import (
	"context"
	"errors"
	"time"

	aid "github.com/xraph/authsome/id"
	apg "github.com/xraph/authsome/store/postgres"
	"github.com/xraph/forge"
	"github.com/xraph/forge/extensions/auth"
	"github.com/xraph/warden"
	wpg "github.com/xraph/warden/store/postgres"

	"github.com/xraph/dispatch/qualification/internal/authority"
)

// QualificationReopenConfiguration selects an existing qualification application
// and environment. Load it only from the controller's root-private configuration.
type QualificationReopenConfiguration struct {
	QualificationServiceConfiguration
	App         string
	Environment string
}

// ReopenQualificationServiceHost reconstructs authentication against the retained
// PostgreSQL stores. It never migrates, enrolls an app, issues a key or adds policy.
func ReopenQualificationServiceHost(ctx context.Context, c QualificationReopenConfiguration) (*Host, func() error, error) {
	if len(c.TokenKey) != 32 || !token(c.Tenant) || c.AuthDSN == "" || c.PolicyDSN == "" || c.ConfigurationDirectory == c.StateDirectory {
		return nil, nil, ErrRefused
	}
	if err := secureDirectory(c.ConfigurationDirectory); err != nil {
		return nil, nil, err
	}
	if err := secureDirectory(c.StateDirectory); err != nil {
		return nil, nil, err
	}
	appID, err := aid.ParseAppID(c.App)
	if err != nil {
		return nil, nil, ErrRefused
	}
	environmentID, err := aid.ParseEnvironmentID(c.Environment)
	if err != nil {
		return nil, nil, ErrRefused
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
	store := apg.New(authDB)
	app, err := store.GetApp(ctx, appID)
	if err != nil || app == nil || app.ID != appID || !app.IsPlatform || app.Slug != "managed-service-qualification" {
		return nil, nil, errors.Join(ErrRefused, closeDB())
	}
	environment, err := store.GetEnvironment(ctx, environmentID)
	if err != nil || environment == nil || environment.ID != environmentID || environment.AppID != appID || environment.Slug != "service-qualification" {
		return nil, nil, errors.Join(ErrRefused, closeDB())
	}
	policies := wpg.New(policyDB)
	policyEngine, err := warden.NewEngine(warden.WithStore(policies))
	if err != nil {
		return nil, nil, errors.Join(err, closeDB())
	}
	forgeApp := forge.New(forge.WithAppName("managed-service-qualification"), forge.WithAppLogger(forge.NewNoopLogger()))
	registry := auth.NewRegistry(forgeApp.Container(), forge.NewNoopLogger())
	engine, err := authority.NewEncrypted(store, policyEngine, appID.String(), registry, c.TokenKey)
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
	return &Host{root: c.StateDirectory, configDirectory: c.ConfigurationDirectory, engine: engine, app: appID, environment: environmentID, tenant: c.Tenant, policies: policies}, cleanup, nil
}
