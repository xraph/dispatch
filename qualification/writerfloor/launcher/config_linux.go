//go:build linux

package launcher

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // Register the PostgreSQL driver for the separate admission ledger.
	aid "github.com/xraph/authsome/id"
	apg "github.com/xraph/authsome/store/postgres"
	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/forge"
	"github.com/xraph/forge/extensions/auth"
	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/warden"
	wpg "github.com/xraph/warden/store/postgres"

	"github.com/xraph/dispatch/qualification/internal/authority"
	"github.com/xraph/dispatch/qualification/writerfloor/internal/admission"
)

// Configuration is root-only local setup. Launcher ledger/signing credentials
// are never copied into startup. Reviewed role configuration is separately hashed.
type Configuration struct {
	StateDirectory string `json:"state_directory"`
	AuthDSN        string `json:"auth_dsn"`
	PolicyDSN      string `json:"policy_dsn"`
	LedgerDSN      string `json:"ledger_dsn"`
	App            string `json:"app"`
	Tenant         string `json:"tenant"`
	Environment    string `json:"environment"`
	TokenKey       []byte `json:"token_key"`
}

type refuseCutover struct{}

func (refuseCutover) VerifyOpen(context.Context, admission.State, admission.Open) error {
	return managed.ErrUnsupported
}

func openGrove(ctx context.Context, dsn string) (*grove.DB, error) {
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	driver := pgdriver.New()
	if operationErr := driver.Open(bounded, dsn); operationErr != nil {
		return nil, ErrRefused
	}
	return grove.Open(driver)
}

// OpenHost wires published Forge/Authsome/Warden stores without migration or a
// default allow policy. The launcher can bind/revoke workloads, never reopen admission.
func OpenHost(ctx context.Context, configPath string) (*Host, func() error, error) {
	directory := filepath.Dir(configPath)
	if operationErr := secureDirectory(directory); operationErr != nil {
		return nil, nil, operationErr
	}
	raw, e := readPrivate(configPath, MaxManifest)
	if e != nil {
		return nil, nil, e
	}
	var c Configuration
	if canonical(raw, &c, MaxManifest) != nil || len(c.TokenKey) != 32 || c.AuthDSN == "" || c.PolicyDSN == "" || c.LedgerDSN == "" || c.Tenant == "" {
		return nil, nil, ErrRefused
	}
	if operationErr := secureDirectory(c.StateDirectory); operationErr != nil {
		return nil, nil, operationErr
	}
	raw, e = readPrivate(filepath.Join(directory, "manifest.json"), MaxManifest)
	if e != nil {
		return nil, nil, e
	}
	manifest, e := DecodeManifest(raw)
	if e != nil {
		return nil, nil, e
	}
	appID, e := aid.ParseAppID(c.App)
	if e != nil {
		return nil, nil, e
	}
	environmentID, e := aid.ParseEnvironmentID(c.Environment)
	if e != nil {
		return nil, nil, e
	}
	authDB, e := openGrove(ctx, c.AuthDSN)
	if e != nil {
		return nil, nil, e
	}
	policyDB, e := openGrove(ctx, c.PolicyDSN)
	if e != nil {
		_ = authDB.Close()
		return nil, nil, e
	}
	policies := wpg.New(policyDB)
	policy, e := warden.NewEngine(warden.WithStore(policies))
	if e != nil {
		_ = authDB.Close()
		_ = policyDB.Close()
		return nil, nil, e
	}
	app := forge.New(forge.WithAppName("writerfloor-fixed-launcher"), forge.WithAppLogger(forge.NewNoopLogger()))
	registry := auth.NewRegistry(app.Container(), forge.NewNoopLogger())
	engine, e := authority.NewEncrypted(apg.New(authDB), policy, c.App, registry, c.TokenKey)
	if e != nil {
		_ = authDB.Close()
		_ = policyDB.Close()
		return nil, nil, e
	}
	if e = engine.Start(ctx); e != nil {
		_ = authDB.Close()
		_ = policyDB.Close()
		return nil, nil, e
	}
	ledger, e := sql.Open("pgx", c.LedgerDSN)
	if e != nil {
		e = errors.Join(e, engine.Stop(ctx))
		_ = authDB.Close()
		_ = policyDB.Close()
		return nil, nil, e
	}
	ledger.SetMaxOpenConns(2)
	admin, e := admission.NewAdmin(ledger, manifest.Authority, refuseCutover{})
	if e != nil {
		_ = ledger.Close()
		e = errors.Join(e, engine.Stop(ctx))
		_ = authDB.Close()
		_ = policyDB.Close()
		return nil, nil, e
	}
	closeAll := func() error {
		bounded, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return errors.Join(engine.Stop(bounded), ledger.Close(), authDB.Close(), policyDB.Close())
	}
	return &Host{c.StateDirectory, directory, manifest, engine, ledger, admin, appID, environmentID, c.Tenant, policies}, closeAll, nil
}
