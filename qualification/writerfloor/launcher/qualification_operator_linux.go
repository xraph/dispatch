//go:build linux

package launcher

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"reflect"
	"slices"
	"sync/atomic"
	"time"

	aid "github.com/xraph/authsome/id"
	"github.com/xraph/forge/extensions/dashboard"
	dashauth "github.com/xraph/forge/extensions/dashboard/auth"
	fc "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"
	"github.com/xraph/forge/extensions/dashboard/contract/idempotency"
	"github.com/xraph/forge/extensions/dashboard/contract/transport"
	dashsecurity "github.com/xraph/forge/extensions/dashboard/security"
	"github.com/xraph/warden"
	wid "github.com/xraph/warden/id"
	"github.com/xraph/warden/policy"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	"github.com/xraph/dispatch/engine"
	dc "github.com/xraph/dispatch/extension/contract"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/qualification/internal/authority"
	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/store/postgres"
)

// QualificationOperatorRuntime is the configured service host, including its
// authenticated enrollment and live remote worker controls. HTTP cannot choose it.
type QualificationOperatorRuntime interface {
	operator.QueryRuntimeHost
	EnrolledIdentity(target durable.QueryRuntimeTarget) (durable.QueryRuntimeIdentity, error)
	WorkerControl(ctx context.Context, target durable.QueryRuntimeTarget) (operator.WorkerControl, error)
}

// QualificationOperatorOptions limits this controller to one existing native
// namespace and at most sixteen already enrolled service process targets.
type QualificationOperatorOptions struct {
	// ControllerID is fixed host configuration. Empty retains the operator default.
	ControllerID  string
	Installation  string
	Namespace     string
	Targets       []durable.QueryRuntimeTarget
	Runtime       QualificationOperatorRuntime
	GuardVerifier durable.QueryRemovalGuardVerifier
}

// QualificationOperatorCredential is private controller startup material. Never
// include its bearer or TLS key in audit, command evidence or test diagnostics.
type QualificationOperatorCredential struct {
	Account        string
	Key            string
	Bearer         string
	CAPEM          []byte
	CertificatePEM []byte
	KeyPEM         []byte
}

// QualificationOperator serves the published Forge contract with real persisted
// machine authentication, exact TLS-peer binding, Warden and native durable audit.
type QualificationOperator struct {
	Handler    http.Handler
	Credential QualificationOperatorCredential
	host       *Host
	audit      *security.AuditService
	closed     atomic.Bool
}

type qualificationOperatorRecord struct {
	ControllerID string                       `json:"controller_id"`
	Phase        string                       `json:"phase"`
	Account      string                       `json:"account"`
	Key          string                       `json:"key"`
	Peer         string                       `json:"peer"`
	Installation string                       `json:"installation"`
	Namespace    string                       `json:"namespace"`
	Targets      []durable.QueryRuntimeTarget `json:"targets"`
}

func qualificationControllerID(id string) (string, error) {
	if id == "" {
		id = "operator"
	}
	if !token(id) || len(id) > 32 {
		return "", ErrRefused
	}
	return id, nil
}

func qualificationOperatorActions() []string {
	return []string{operator.ReadBuild, operator.RegisterBuild, operator.RetireBuild, operator.FinalizeBuild, operator.ResumeBuild, operator.ReadWorker, operator.DrainWorker, operator.ReadQueryRuntime, operator.RegisterQueryRuntime, operator.VerifyQueryRuntime, operator.RemoveQueryRuntime, operator.FinishQueryRuntime, operator.AbortQueryRuntime, operator.RevalidateQueryRuntime, operator.ReadQueryRevalidation, operator.CommitQueryRemoval, operator.ReadQueryCommit, operator.CloseQueryGuard, operator.ReadQueryGuard}
}
func qualificationNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan:
		return v.IsNil()
	}
	return false
}

// OpenQualificationOperator mounts actual durable contracts for the trusted fixed
// controller. Existing credential issuance is never replayed after an unknown
// outcome. It does not register a substitute worker or a successful guard verifier.
func (h *Host) OpenQualificationOperator(ctx context.Context, store *postgres.Store, options QualificationOperatorOptions) (*QualificationOperator, error) {
	controllerID, err := qualificationControllerID(options.ControllerID)
	if err != nil {
		return nil, err
	}
	if h == nil || h.engine == nil || h.policies == nil || store == nil || qualificationNil(options.Runtime) || len(options.Targets) < 1 || len(options.Targets) > 16 || !durable.DeliveryIdentifier(options.Installation) || !durable.DeliveryIdentifier(options.Namespace) {
		return nil, ErrRefused
	}
	if options.GuardVerifier != nil && qualificationNil(options.GuardVerifier) {
		return nil, ErrRefused
	}
	options.Targets = slices.Clone(options.Targets)
	targets := map[durable.QueryRuntimeTarget]bool{}
	for _, target := range options.Targets {
		if target.Validate() != nil || target.InstallationID != options.Installation || target.Namespace != options.Namespace || targets[target] {
			return nil, ErrRefused
		}
		targets[target] = true
	}
	namespace, err := store.GetNamespace(ctx, options.Installation, options.Namespace)
	if err != nil || namespace.AppID != h.app.String() || namespace.TenantID != h.tenant || !namespace.RequireAudit {
		return nil, ErrRefused
	}
	lock, err := openJournal(h.root, controllerID)
	if err != nil {
		return nil, err
	}
	defer lock.close()
	record := qualificationOperatorRecord{ControllerID: controllerID, Phase: "issuing", Installation: options.Installation, Namespace: options.Namespace, Targets: options.Targets}
	save := func(exclusive bool) error {
		raw, e := json.Marshal(record)
		if e != nil || len(raw) > MaxRecord {
			return ErrRefused
		}
		return writePrivate(h.root, controllerID+".json", raw, exclusive)
	}
	if err = save(true); err != nil {
		return nil, errors.Join(ErrUncertain, err)
	}
	actions := qualificationOperatorActions()
	accountName, policyPrefix := "fixed-native-controller", "fixed-controller"
	if controllerID != "operator" {
		accountName += "-" + controllerID
		policyPrefix += "-" + controllerID
	}
	account, err := h.engine.CreateServiceAccountInEnvironment(ctx, h.app, h.environment, accountName, "", actions)
	if err != nil {
		return nil, errors.Join(ErrUncertain, err)
	}
	record.Account = account.ID.String()
	record.Phase = "account"
	if saveErr := save(false); saveErr != nil {
		return nil, saveErr
	}
	expires := time.Now().UTC().Add(time.Hour)
	key, bearer, err := h.engine.CreateServiceAccountAPIKey(ctx, account.ID, accountName, actions, &expires)
	if err != nil {
		return nil, errors.Join(ErrUncertain, err)
	}
	record.Key = key.ID.String()
	record.Phase = "key"
	if saveErr := save(false); saveErr != nil {
		return nil, saveErr
	}
	ca, err := readPrivate(filepath.Join(h.configDirectory, "proxy-ca.pem"), MaxRecord)
	if err != nil {
		return nil, err
	}
	caKey, err := readPrivate(filepath.Join(h.configDirectory, "proxy-ca-key.pem"), MaxRecord)
	if err != nil {
		return nil, err
	}
	_, peer, err := issueProxy(h.root, controllerID, ca, caKey)
	if err != nil {
		return nil, err
	}
	record.Peer = peer
	certificate, err := readPrivate(filepath.Join(h.root, controllerID+"-proxy-cert.pem"), MaxRecord)
	if err != nil {
		return nil, err
	}
	privateKey, err := readPrivate(filepath.Join(h.root, controllerID+"-proxy-key.pem"), MaxRecord)
	if err != nil {
		return nil, err
	}
	serverCA, err := readPrivate(filepath.Join(h.configDirectory, "upstream-ca.pem"), MaxRecord)
	if err != nil {
		return nil, err
	}
	policyEngine, err := warden.NewEngine(warden.WithStore(h.policies))
	if err != nil {
		return nil, err
	}
	if schemaErr := operator.RegisterNamespaceSchema(ctx, h.policies, namespace); schemaErr != nil {
		return nil, schemaErr
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	buildActions := []string{operator.ReadBuild, operator.RegisterBuild, operator.RetireBuild, operator.FinalizeBuild, operator.ResumeBuild}
	builds := []string{}
	runtimes := []string{}
	for _, target := range options.Targets {
		if !slices.Contains(builds, target.BuildID) {
			builds = append(builds, target.BuildID)
		}
		runtimes = append(runtimes, target.RuntimeID)
	}
	processActions := []string{}
	for _, action := range actions {
		if !slices.Contains(buildActions, action) {
			processActions = append(processActions, action)
		}
	}
	for group, allowed := range [][]string{buildActions, processActions} {
		conditions := []policy.Condition{{Field: "resource.installation_id", Operator: policy.OpEquals, Value: options.Installation}, {Field: "resource.app_id", Operator: policy.OpEquals, Value: h.app.String()}, {Field: "resource.tenant_id", Operator: policy.OpEquals, Value: h.tenant}, {Field: "resource.build_id", Operator: policy.OpIn, Value: builds}}
		if group == 1 {
			conditions = append(conditions, policy.Condition{Field: "resource.runtime_id", Operator: policy.OpIn, Value: runtimes})
		}
		rule := &policy.Policy{ID: wid.NewPolicyID(), AppID: h.app.String(), TenantID: h.tenant, NamespacePath: options.Namespace, Name: policyPrefix + []string{"-builds", "-processes"}[group], IsActive: true, Version: 1, Effect: policy.EffectAllow, Subjects: []policy.SubjectMatch{{Kind: "service_acct", ID: record.Account}}, Actions: allowed, Resources: []string{"dispatch_namespace:" + options.Namespace}, Conditions: conditions, CreatedAt: now, UpdatedAt: now}
		if err = h.policies.CreatePolicy(ctx, rule); err != nil {
			return nil, errors.Join(ErrUncertain, err)
		}
	}
	audit := &security.AuditService{}
	boundary := security.Boundary{Resource: security.Resource{InstallationID: options.Installation, PolicyTenant: h.tenant}, Authorizer: &security.WardenAuthorizer{Engine: func() (*warden.Engine, error) { return policyEngine, nil }}, Audit: audit}
	if auditErr := audit.Activate(ctx, store, store, boundary.Resource, options.Namespace, false); auditErr != nil {
		return nil, auditErr
	}
	cursorKey := make([]byte, 32)
	if _, err = rand.Read(cursorKey); err != nil {
		audit.Deactivate()
		return nil, err
	}
	operators, err := operator.New(operator.Options{Store: store, Reads: store, Catalog: store, InstallationID: options.Installation, Audit: boundary, Authorizer: &operator.WardenAuthorizer{Engine: func() (*warden.Engine, error) { return policyEngine, nil }}, CursorKeys: operator.CursorKeys{Active: "fixed-controller", Keys: map[string][]byte{"fixed-controller": cursorKey}}, QueryHost: options.Runtime, QueryRemovalGuardVerifier: options.GuardVerifier, WorkerControl: func(ctx context.Context, target durable.QueryRuntimeTarget) (operator.WorkerControl, error) {
		if !targets[target] {
			return nil, operator.ErrRuntimeUnavailable
		}
		return options.Runtime.WorkerControl(ctx, target)
	}, BuildIdentity: func(ctx context.Context, build durable.BuildTarget) (durable.BuildQueryIdentity, error) {
		if contextErr := ctx.Err(); contextErr != nil {
			return durable.BuildQueryIdentity{}, contextErr
		}
		var identity durable.BuildQueryIdentity
		found := false
		for _, target := range options.Targets {
			if target.BuildTarget != build {
				continue
			}
			current, e := options.Runtime.EnrolledIdentity(target)
			if e != nil {
				return identity, e
			}
			if found && current.BuildIdentity != identity {
				return identity, ErrRefused
			}
			identity = current.BuildIdentity
			found = true
		}
		if !found {
			return identity, ErrRefused
		}
		return identity, nil
	}, QueryRegistrationPolicy: func(ctx context.Context, request durable.RegisterQueryRuntimeRequest) error {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		if !targets[request.Identity.QueryRuntimeTarget] {
			return ErrRefused
		}
		identity, e := options.Runtime.EnrolledIdentity(request.Identity.QueryRuntimeTarget)
		if e != nil {
			return e
		}
		if identity != request.Identity {
			return ErrRefused
		}
		return nil
	}})
	if err != nil {
		audit.Deactivate()
		return nil, err
	}
	core, err := dispatch.New(dispatch.WithStore(store))
	if err != nil {
		audit.Deactivate()
		return nil, err
	}
	execution, err := engine.Build(core)
	if err != nil {
		audit.Deactivate()
		return nil, err
	}
	registry, wardens := fc.NewRegistry(), fc.NewWardenRegistry()
	dispatchers := dispatcher.NewWithOptions(nil, dispatcher.WithIdempotencyStore(dashboard.AdaptIdempotencyStore(idempotency.NewInMemoryStore())))
	if err = dc.Register(dispatchers, registry, wardens, dc.Deps{Engine: execution, Store: store, Security: boundary, Durable: operators}); err != nil {
		audit.Deactivate()
		return nil, err
	}
	csrf := dashsecurity.NewCSRFManager()
	contract := transport.NewHandlerWithCSRF(registry, wardens, dispatchers, nil, csrf)
	csrfHandler := transport.NewCSRFTokenHandler(csrf, 10*time.Minute)
	provider := &authority.Provider{Engine: h.engine, EnvironmentID: h.environment.String(), Binding: ecosystem.Binding{InstallationID: options.Installation, Namespace: options.Namespace, AppID: h.app.String(), TenantID: h.tenant, Producer: "dispatch"}, Credentials: []authority.Credential{{KeyID: key.ID, AccountID: account.ID}}}
	out := &QualificationOperator{host: h, audit: audit, Credential: QualificationOperatorCredential{Account: record.Account, Key: record.Key, Bearer: bearer, CAPEM: serverCA, CertificatePEM: certificate, KeyPEM: privateKey}}
	out.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if out.closed.Load() {
			http.Error(w, "closed", http.StatusForbidden)
			return
		}
		peer, e := readyPeer(r)
		if e != nil || peer != record.Peer {
			http.Error(w, "refused", http.StatusForbidden)
			return
		}
		principal, e := provider.Authenticate(r.Context(), r)
		if e != nil || principal == nil || principal.Subject != record.Account || !slices.Equal(principal.Scopes, actions) {
			http.Error(w, "refused", http.StatusUnauthorized)
			return
		}
		verified := dashauth.WithUser(r.Context(), &dashauth.UserInfo{Subject: principal.Subject, Claims: principal.Claims, Scopes: slices.Clone(principal.Scopes), ProviderName: authority.ProviderName, Metadata: principal.Metadata})
		verified = dashauth.WithHTTP(verified, w, r)
		r = r.WithContext(verified)
		switch {
		case r.URL.Path == "/api/dashboard/v1/csrf" && r.Method == http.MethodGet:
			csrfHandler.ServeHTTP(w, r)
		case r.URL.Path == "/api/dashboard/v1" && r.Method == http.MethodPost:
			contract.ServeHTTP(w, r)
		default:
			http.NotFound(w, r)
		}
	})
	record.Phase = "ready"
	if err = save(false); err != nil {
		audit.Deactivate()
		return nil, err
	}
	return out, nil
}

// Close refuses further controller requests and revokes its exact saved API key.
// It leaves the shared native store and service-process credentials untouched.
func (o *QualificationOperator) Close(ctx context.Context) error {
	if o == nil {
		return nil
	}
	o.closed.Store(true)
	o.audit.Deactivate()
	keyID, err := aid.ParseAPIKeyID(o.Credential.Key)
	if err != nil {
		return err
	}
	key, err := o.host.engine.APIKeyStore().GetAPIKey(ctx, keyID)
	if err != nil {
		return err
	}
	if key == nil || key.ServiceAccountID.String() != o.Credential.Account || key.AppID != o.host.app || key.EnvID != o.host.environment {
		return ErrRefused
	}
	if key.Revoked {
		return nil
	}
	key.Revoked = true
	return o.host.engine.APIKeyStore().UpdateAPIKey(ctx, key)
}
