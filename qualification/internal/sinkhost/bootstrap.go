package sinkhost

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/xraph/authsome/app"
	"github.com/xraph/authsome/environment"
	aid "github.com/xraph/authsome/id"
	apg "github.com/xraph/authsome/store/postgres"
	cpg "github.com/xraph/chronicle/store/postgres"
	"github.com/xraph/relay"
	"github.com/xraph/relay/endpoint"
	rpg "github.com/xraph/relay/store/postgres"
	"github.com/xraph/warden"
	wid "github.com/xraph/warden/id"
	"github.com/xraph/warden/policy"
	wpg "github.com/xraph/warden/store/postgres"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/qualification/internal/authority"
	"github.com/xraph/dispatch/security"
	dpg "github.com/xraph/dispatch/store/postgres"
)

func Scope(role string) string { return "dispatch.sink." + role + ".accept" }

// Bootstrap uses engine APIs offline. No issuance/admin routes are registered.
func Bootstrap(ctx context.Context, c *Config) error {
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
	st, policies := apg.New(authDB), wpg.New(policyDB)
	if setupErr := st.Migrate(ctx); setupErr != nil {
		return setupErr
	}
	if setupErr := policies.Migrate(ctx); setupErr != nil {
		return setupErr
	}
	appID := aid.NewAppID()
	c.Binding.AppID = appID.String()
	now := time.Now().UTC()
	if createErr := st.CreateApp(ctx, &app.App{ID: appID, Name: "Sink qualification", Slug: "sink-qualification", IsPlatform: true, CreatedAt: now, UpdatedAt: now}); createErr != nil {
		return createErr
	}
	w, err := warden.NewEngine(warden.WithStore(policies))
	if err != nil {
		return err
	}
	c.TokenKey = make([]byte, 32)
	if _, err = rand.Read(c.TokenKey); err != nil {
		return err
	}
	engine, err := authority.NewEncrypted(st, w, c.Binding.AppID, nil, c.TokenKey)
	if err != nil {
		return err
	}
	if setupErr := engine.Start(ctx); setupErr != nil {
		return setupErr
	}
	defer func() {
		if stopErr := engine.Stop(context.Background()); stopErr != nil {
			fmtSafe("authority shutdown incomplete")
		}
	}()
	env := &environment.Environment{ID: aid.NewEnvironmentID(), AppID: appID, Name: "Qualification", Slug: "qualification", Type: environment.TypeDevelopment}
	if setupErr := engine.CreateEnvironment(ctx, env); setupErr != nil {
		return setupErr
	}
	c.EnvironmentID = env.ID.String()
	c.Credentials = map[string]authority.Credential{}
	for _, role := range []string{"chronicle", "relay", "operator", "denied"} {
		scopes := []string{Scope(role)}
		if role == "operator" || role == "denied" {
			scopes = []string{security.OperatorRead, security.OperatorWrite}
		}
		account, e := engine.CreateServiceAccountInEnvironment(ctx, appID, env.ID, role, "qualification", scopes)
		if e != nil {
			return e
		}
		key, secret, e := engine.CreateServiceAccountAPIKey(ctx, account.ID, role, scopes, nil)
		if e != nil {
			return e
		}
		c.Credentials[role] = authority.Credential{KeyID: key.ID, AccountID: account.ID, Secret: secret}
		if role == "denied" {
			continue
		}
		pol := &policy.Policy{ID: wid.NewPolicyID(), AppID: c.Binding.AppID, TenantID: c.PolicyTenant, Name: role, IsActive: true, Effect: policy.EffectAllow, Subjects: []policy.SubjectMatch{{Kind: "service_acct", ID: account.ID.String()}}, Actions: scopes, Resources: []string{"dispatch_installation:" + c.Binding.InstallationID}}
		if role == "operator" {
			pol.AppID = ""
		} // Dispatch's operator boundary uses its policy tenant with an empty app.
		if role == "operator" {
			durablePolicy := &policy.Policy{ID: wid.NewPolicyID(), AppID: c.Binding.AppID, TenantID: c.Binding.TenantID, NamespacePath: c.Binding.Namespace, Name: "durable-callback", IsActive: true, Effect: policy.EffectAllow, Subjects: []policy.SubjectMatch{{Kind: "service_acct", ID: account.ID.String()}}, Actions: []string{operator.StartWorkflow, operator.CompleteActivity, operator.HeartbeatActivity}, Resources: []string{"dispatch_namespace:" + c.Binding.Namespace}, Conditions: []policy.Condition{{Field: "resource.installation_id", Operator: policy.OpEquals, Value: c.Binding.InstallationID}, {Field: "resource.app_id", Operator: policy.OpEquals, Value: c.Binding.AppID}, {Field: "resource.tenant_id", Operator: policy.OpEquals, Value: c.Binding.TenantID}}}
			if e := policies.CreatePolicy(ctx, durablePolicy); e != nil {
				return e
			}
		}
		if e := policies.CreatePolicy(ctx, pol); e != nil {
			return e
		}
	}
	c.HMACKey = make([]byte, 32)
	if _, err = rand.Read(c.HMACKey); err != nil {
		return err
	}
	secret := make([]byte, 32)
	if _, err = rand.Read(secret); err != nil {
		return err
	}
	c.WebhookSecret = hex.EncodeToString(secret)
	db, err := Open(ctx, c.DSNs["dispatch"])
	if err != nil {
		return err
	}
	if setupErr := dpg.New(db).Migrate(ctx); setupErr != nil {
		return setupErr
	}
	record, namespaceErr := dpg.New(db).RegisterNamespace(ctx, durable.NamespaceConfig{InstallationID: c.Binding.InstallationID, Namespace: c.Binding.Namespace, AppID: c.Binding.AppID, TenantID: c.Binding.TenantID, RequireAudit: true, RequireHooks: true, SchemaVersion: 1})
	if namespaceErr != nil {
		return namespaceErr
	}
	if schemaErr := operator.RegisterNamespaceSchema(ctx, policies, record); schemaErr != nil {
		return schemaErr
	}
	if setupErr := db.Close(); setupErr != nil {
		return setupErr
	}
	db, err = Open(ctx, c.DSNs["chronicle"])
	if err != nil {
		return err
	}
	if migrateErr := cpg.New(db).Migrate(ctx); migrateErr != nil {
		return migrateErr
	}
	if setupErr := db.Close(); setupErr != nil {
		return setupErr
	}
	db, err = Open(ctx, c.DSNs["relay"])
	if err != nil {
		return err
	}
	defer db.Close()
	relayStore := rpg.New(db)
	if migrateErr := relayStore.Migrate(ctx); migrateErr != nil {
		return migrateErr
	}
	sender, err := relay.New(relay.WithStore(relayStore))
	if err != nil {
		return err
	}
	if err := ecosystem.RegisterRelaySchema(ctx, sender.Catalog(), c.Binding.AppID); err != nil {
		return err
	}
	for _, path := range []string{"one", "two"} {
		ep, e := sender.Endpoints().Create(ctx, endpoint.Input{TenantID: c.Binding.TenantID, URL: "http://" + c.Addresses["receiver"] + "/" + path, Secret: c.WebhookSecret, EventTypes: []string{ecosystem.RelayEventType}})
		if e != nil {
			return e
		}
		ep.ScopeAppID = c.Binding.AppID
		ep.ScopeOrgID = c.Binding.OrgID
		if e := relayStore.UpdateEndpoint(ctx, ep); e != nil {
			return e
		}
	}
	return nil
}
