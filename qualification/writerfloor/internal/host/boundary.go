// Package host binds real machine authentication to a verified per-workload TLS channel.
package host

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"slices"
	"time"

	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/forge"
	"github.com/xraph/warden"

	"github.com/xraph/dispatch/qualification/internal/authority"
	"github.com/xraph/dispatch/qualification/writerfloor/internal/admission"
)

// Boundary consumes an already started real Authsome provider. Bootstrap it with
// authority.NewEncrypted and the configured persisted Authsome and Warden stores.
// The listener must use TLSConfig directly, without a header-based identity proxy.
type Boundary struct {
	provider    *authority.Provider
	policy      authority.Checker
	runtime     *admission.Runtime
	authorityID string
}

func New(provider *authority.Provider, policy authority.Checker, runtime *admission.Runtime, authorityID string) (*Boundary, error) {
	if provider == nil || provider.Engine == nil || policy == nil || runtime == nil || authorityID == "" || runtime.AuthorityID() != authorityID {
		return nil, admission.ErrDenied
	}
	v := reflect.ValueOf(policy)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan:
		if v.IsNil() {
			return nil, admission.ErrDenied
		}
	}
	// Copy mutable configuration; callers retain no slice ownership.
	copyProvider := *provider
	copyProvider.Credentials = slices.Clone(provider.Credentials)
	return &Boundary{&copyProvider, policy, runtime, authorityID}, nil
}

// TLSConfig requires a configured private client CA. No insecure fallback exists.
func TLSConfig(server tls.Certificate, clientCA *x509.CertPool) (*tls.Config, error) {
	if len(server.Certificate) == 0 || server.PrivateKey == nil || clientCA == nil {
		return nil, admission.ErrDenied
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{server}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientCA.Clone()}, nil
}
func peerDigest(r *http.Request) (string, error) {
	if r.TLS == nil || !r.TLS.HandshakeComplete || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 {
		return "", admission.ErrDenied
	}
	leaf := r.TLS.PeerCertificates[0]
	if leaf == nil || len(leaf.Raw) == 0 {
		return "", admission.ErrDenied
	}
	matched := false
	for _, chain := range r.TLS.VerifiedChains {
		if len(chain) > 0 && chain[0] != nil && chain[0].Equal(leaf) {
			matched = true
			break
		}
	}
	if !matched {
		return "", admission.ErrDenied
	}
	return managed.Digest(leaf.Raw), nil
}

// Authenticate uses the actual Authsome key strategy and persisted account, then
// Warden. The admitted executor bearer still carries its proxy's fingerprint and fails the
// immutable executor binding in Runtime.Delegate. Upstream client authentication
// is a separate executor boundary. Request headers never select a workload.
func (b *Boundary) Authenticate(ctx context.Context, r *http.Request, inv admission.Invocation) (admission.Peer, error) {
	fingerprint, e := peerDigest(r)
	if e != nil {
		return admission.Peer{}, e
	}
	identity, e := b.provider.Authenticate(ctx, r)
	if e != nil || identity == nil || !slices.Contains(identity.Scopes, "writerfloor.delegate") {
		return admission.Peer{}, admission.ErrDenied
	}
	if inv.Tenant != b.provider.Binding.TenantID {
		return admission.Peer{}, admission.ErrDenied
	}
	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	checkCtx = warden.WithNamespace(warden.WithTenant(checkCtx, b.provider.Binding.AppID, inv.Tenant), "")
	result, e := b.policy.Check(checkCtx, &warden.CheckRequest{
		Subject:  warden.Subject{Kind: warden.SubjectServiceAcct, ID: identity.Subject},
		Action:   warden.Action{Name: "writerfloor.delegate"},
		Resource: warden.Resource{Type: "writerfloor_authority", ID: b.authorityID, Attributes: map[string]any{"app": b.provider.Binding.AppID, "tenant": inv.Tenant, "target": inv.Target, "operation": inv.Operation, "kind": inv.Kind}}, TenantID: inv.Tenant,
	})
	if e != nil || result == nil || !result.Allowed || len(result.Obligations) != 0 {
		return admission.Peer{}, admission.ErrDenied
	}
	return admission.Peer{Principal: identity.Subject, CertificateDigest: fingerprint}, nil
}

// Handler is mounted by the trusted Forge host on its mutually authenticated
// listener. There are no administration, certification or arbitrary send routes.
func (b *Boundary) Handler() forge.Handler {
	return func(c forge.Context) error {
		r := c.Request()
		if r.Method != http.MethodPost {
			return c.String(http.StatusMethodNotAllowed, "POST required")
		}
		raw, e := io.ReadAll(io.LimitReader(r.Body, admission.MaxRecord+1))
		if e != nil || len(raw) > admission.MaxRecord {
			return c.String(http.StatusBadRequest, "invalid invocation")
		}
		var inv admission.Invocation
		if e = json.Unmarshal(raw, &inv); e != nil {
			return c.String(http.StatusBadRequest, "invalid invocation")
		}
		// Canonical input also refuses duplicate fields, unknown keys and trailing data.
		canonical, e := json.Marshal(inv)
		if e != nil || !bytes.Equal(canonical, raw) {
			return c.String(http.StatusBadRequest, "canonical invocation required")
		}
		peer, e := b.Authenticate(c.Context(), r, inv)
		if e != nil {
			return c.String(http.StatusForbidden, "admission denied")
		}
		record, e := b.runtime.Delegate(c.Context(), peer, inv)
		if e != nil {
			if errors.Is(e, admission.ErrDenied) || errors.Is(e, admission.ErrConflict) {
				return c.String(http.StatusForbidden, "admission denied")
			}
			return c.String(http.StatusServiceUnavailable, "inspect retained delegation; do not resend")
		}
		return c.JSON(http.StatusOK, record)
	}
}
