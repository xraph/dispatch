package sinkhost

import (
	"context"
	"errors"
	"maps"
	"slices"

	aid "github.com/xraph/authsome/id"
	apg "github.com/xraph/authsome/store/postgres"
	"github.com/xraph/warden"
	wid "github.com/xraph/warden/id"
	"github.com/xraph/warden/policy"
	wpg "github.com/xraph/warden/store/postgres"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/qualification/internal/authority"
	dpg "github.com/xraph/dispatch/store/postgres"
)

// ProvisionChronicleProfile issues a Chronicle credential in the original app and environment.
// Namespace ownership is immutable; this helper never creates another app or environment.
func ProvisionChronicleProfile(ctx context.Context, base Config, binding ecosystem.Binding, address string) (result Config, returnErr error) {
	if base.Validate() != nil || binding.Validate() != nil || !validChronicleAddress(address) || binding.InstallationID != base.Binding.InstallationID || binding.AppID != base.Binding.AppID || binding.OrgID != base.Binding.OrgID || binding.Producer != base.Binding.Producer || binding.Namespace == base.Binding.Namespace || address == base.Addresses["chronicle"] {
		return result, ErrInvalidProfile
	}
	if (binding.Namespace != "audit" && binding.Namespace != "foreign") || binding.TenantID != "tenant-"+binding.Namespace {
		return result, ErrInvalidProfile
	}
	appID, err := aid.ParseAppID(binding.AppID)
	if err != nil {
		return result, ErrInvalidProfile
	}
	envID, err := aid.ParseEnvironmentID(base.EnvironmentID)
	if err != nil {
		return result, ErrInvalidProfile
	}
	authDB, err := Open(ctx, base.DSNs["authsome"])
	if err != nil {
		return result, err
	}
	defer func() { returnErr = errors.Join(returnErr, authDB.Close()) }()
	policyDB, err := Open(ctx, base.DSNs["warden"])
	if err != nil {
		return result, err
	}
	defer func() { returnErr = errors.Join(returnErr, policyDB.Close()) }()
	policies := wpg.New(policyDB)
	w, err := warden.NewEngine(warden.WithStore(policies))
	if err != nil {
		return result, err
	}
	engine, err := authority.NewEncrypted(apg.New(authDB), w, binding.AppID, nil, base.TokenKey)
	if err != nil {
		return result, err
	}
	if startErr := engine.Start(ctx); startErr != nil {
		return result, startErr
	}
	defer func() { returnErr = errors.Join(returnErr, engine.Stop(ctx)) }()
	originalApp, err := engine.GetApp(ctx, appID)
	if err != nil || originalApp == nil || originalApp.ID != appID {
		return result, ErrInvalidProfile
	}
	env, err := engine.GetEnvironment(ctx, envID)
	if err != nil || env == nil || env.AppID != appID {
		return result, ErrInvalidProfile
	}
	db, err := Open(ctx, base.DSNs["dispatch"])
	if err != nil {
		return result, err
	}
	defer func() { returnErr = errors.Join(returnErr, db.Close()) }()
	record, err := dpg.New(db).RegisterNamespace(ctx, durable.NamespaceConfig{InstallationID: binding.InstallationID, Namespace: binding.Namespace, AppID: binding.AppID, TenantID: binding.TenantID, RequireAudit: true, RequireHooks: true, SchemaVersion: 1})
	if err != nil {
		return result, err
	}
	if schemaErr := operator.RegisterNamespaceSchema(ctx, policies, record); schemaErr != nil {
		return result, schemaErr
	}
	scopes := []string{Scope("chronicle")}
	name := "chronicle-" + binding.Namespace
	account, err := engine.CreateServiceAccountInEnvironment(ctx, appID, envID, name, "qualification", scopes)
	if err != nil {
		return result, err
	}
	key, secret, err := engine.CreateServiceAccountAPIKey(ctx, account.ID, name, scopes, nil)
	if err != nil {
		return result, err
	}
	conditions := []policy.Condition{}
	for field, value := range map[string]string{"installation": binding.InstallationID, "app": binding.AppID, "tenant": binding.TenantID, "producer": binding.Producer, "destination": "chronicle"} {
		conditions = append(conditions, policy.Condition{Field: "resource." + field, Operator: policy.OpEquals, Value: value})
	}
	if policyErr := policies.CreatePolicy(ctx, &policy.Policy{ID: wid.NewPolicyID(), AppID: binding.AppID, TenantID: binding.TenantID, Name: name, IsActive: true, Effect: policy.EffectAllow, Subjects: []policy.SubjectMatch{{Kind: "service_acct", ID: account.ID.String()}}, Actions: scopes, Resources: []string{"dispatch_installation:" + binding.InstallationID}, Conditions: conditions}); policyErr != nil {
		return result, policyErr
	}
	result = base
	result.Binding, result.PolicyTenant = binding, binding.TenantID
	result.DSNs, result.Addresses = maps.Clone(base.DSNs), maps.Clone(base.Addresses)
	result.TokenKey, result.HMACKey = slices.Clone(base.TokenKey), slices.Clone(base.HMACKey)
	result.Addresses["chronicle"] = address
	result.Credentials = map[string]authority.Credential{"chronicle": {KeyID: key.ID, AccountID: account.ID, Secret: secret}}
	result.LostAckDestination, result.LostAckMarker, result.CallbackDirectory = "", "", ""
	return result, result.Validate()
}
