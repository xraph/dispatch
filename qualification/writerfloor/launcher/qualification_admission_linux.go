//go:build linux

package launcher

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"time"

	aid "github.com/xraph/authsome/id"
	"github.com/xraph/forge"
	"github.com/xraph/warden"
	wid "github.com/xraph/warden/id"
	"github.com/xraph/warden/policy"

	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	"github.com/xraph/dispatch/qualification/internal/authority"
	"github.com/xraph/dispatch/qualification/writerfloor/internal/admission"
	admissionhost "github.com/xraph/dispatch/qualification/writerfloor/internal/host"
)

// QualificationAdmission exposes the real admission route and its private client
// startup material. It neither certifies artifacts nor opens the ledger.
type QualificationAdmission struct {
	Handler    http.Handler
	Credential QualificationOperatorCredential
	PeerDigest string
	host       *Host
	closed     atomic.Bool
}

// OpenQualificationAdmission issues one exact qualification controller identity.
// The private journal refuses another issuance after success or an unknown result.
// All runtime requests use the actual Authsome, Warden and Forge boundary.
func (h *Host) OpenQualificationAdmission(ctx context.Context, runtime *admission.Runtime) (*QualificationAdmission, error) {
	if h == nil || h.engine == nil || h.policies == nil || runtime == nil || !token(runtime.AuthorityID()) {
		return nil, ErrRefused
	}
	lock, err := openJournal(h.root, "admission")
	if err != nil {
		return nil, err
	}
	defer lock.close()
	if err = writePrivate(h.root, "admission-issued.json", []byte(`{"phase":"issuing"}`), true); err != nil {
		return nil, errors.Join(ErrUncertain, err)
	}
	actions := []string{"writerfloor.delegate"}
	account, err := h.engine.CreateServiceAccountInEnvironment(ctx, h.app, h.environment, "qualification-admission", "", actions)
	if err != nil {
		return nil, errors.Join(ErrUncertain, err)
	}
	expires := time.Now().UTC().Add(time.Hour)
	key, bearer, err := h.engine.CreateServiceAccountAPIKey(ctx, account.ID, "qualification-admission", actions, &expires)
	if err != nil {
		return nil, errors.Join(ErrUncertain, err)
	}
	ca, err := readPrivate(filepath.Join(h.configDirectory, "proxy-ca.pem"), MaxRecord)
	if err != nil {
		return nil, err
	}
	caKey, err := readPrivate(filepath.Join(h.configDirectory, "proxy-ca-key.pem"), MaxRecord)
	if err != nil {
		return nil, err
	}
	_, peer, err := issueProxy(h.root, "admission", ca, caKey)
	if err != nil {
		return nil, err
	}
	certificate, err := readPrivate(filepath.Join(h.root, "admission-proxy-cert.pem"), MaxRecord)
	if err != nil {
		return nil, err
	}
	privateKey, err := readPrivate(filepath.Join(h.root, "admission-proxy-key.pem"), MaxRecord)
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
	now := time.Now().UTC().Truncate(time.Microsecond)
	rule := &policy.Policy{ID: wid.NewPolicyID(), AppID: h.app.String(), TenantID: h.tenant, Name: "qualification-admission", IsActive: true, Version: 1, Effect: policy.EffectAllow, Subjects: []policy.SubjectMatch{{Kind: "service_acct", ID: account.ID.String()}}, Actions: actions, Resources: []string{"writerfloor_authority:" + runtime.AuthorityID()}, CreatedAt: now, UpdatedAt: now}
	if err = h.policies.CreatePolicy(ctx, rule); err != nil {
		return nil, errors.Join(ErrUncertain, err)
	}
	provider := &authority.Provider{Engine: h.engine, EnvironmentID: h.environment.String(), Binding: ecosystem.Binding{InstallationID: "service-installation", Namespace: "qualification-admission", AppID: h.app.String(), TenantID: h.tenant, Producer: "dispatch"}, Credentials: []authority.Credential{{KeyID: key.ID, AccountID: account.ID}}}
	boundary, err := admissionhost.New(provider, policyEngine, runtime, runtime.AuthorityID())
	if err != nil {
		return nil, err
	}
	router := forge.NewRouter(forge.WithLogger(forge.NewNoopLogger()))
	if routeErr := router.POST("/writerfloor/delegate", boundary.Handler()); routeErr != nil {
		return nil, routeErr
	}
	out := &QualificationAdmission{host: h, PeerDigest: peer, Credential: QualificationOperatorCredential{Account: account.ID.String(), Key: key.ID.String(), Bearer: bearer, CAPEM: serverCA, CertificatePEM: certificate, KeyPEM: privateKey}}
	out.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if out.closed.Load() {
			http.Error(w, "closed", http.StatusForbidden)
			return
		}
		actual, peerErr := readyPeer(r)
		if peerErr != nil || actual != peer {
			http.Error(w, "refused", http.StatusForbidden)
			return
		}
		router.ServeHTTP(w, r)
	})
	return out, nil
}

// Close stops admission and revokes only this controller's exact API key.
func (a *QualificationAdmission) Close(ctx context.Context) error {
	if a == nil {
		return nil
	}
	a.closed.Store(true)
	keyID, err := aid.ParseAPIKeyID(a.Credential.Key)
	if err != nil {
		return err
	}
	key, err := a.host.engine.APIKeyStore().GetAPIKey(ctx, keyID)
	if err != nil {
		return err
	}
	if key == nil || key.ServiceAccountID.String() != a.Credential.Account || key.AppID != a.host.app || key.EnvID != a.host.environment {
		return ErrRefused
	}
	if key.Revoked {
		return nil
	}
	key.Revoked = true
	return a.host.engine.APIKeyStore().UpdateAPIKey(ctx, key)
}
