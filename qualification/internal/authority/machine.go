package authority

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/xraph/authsome"
	"github.com/xraph/authsome/apikey"
	aid "github.com/xraph/authsome/id"
	"github.com/xraph/authsome/principal"
	"github.com/xraph/forge"
	"github.com/xraph/forge/extensions/auth"
	"github.com/xraph/warden"

	"github.com/xraph/dispatch/durable/delivery/ecosystem"
)

const ProviderName = "dispatch-sink-machine"

var ErrDenied = errors.New("qualification: machine identity refused")

type Credential struct {
	KeyID     aid.APIKeyID         `json:"key_id"`
	AccountID aid.ServiceAccountID `json:"account_id"`
	Secret    string               `json:"secret"`
}

// Provider checks an explicit bearer against both the actual API-key strategy
// and configured persisted key/account bindings. The request cannot pick an app.
type Provider struct {
	Engine        *authsome.Engine
	EnvironmentID string
	Binding       ecosystem.Binding
	Credentials   []Credential
}

func (*Provider) Name() string                  { return ProviderName }
func (*Provider) Type() auth.SecuritySchemeType { return auth.SecurityTypeHTTP }
func (*Provider) OpenAPIScheme() auth.SecurityScheme {
	return auth.SecurityScheme{Type: "http", Scheme: "bearer"}
}
func (*Provider) Middleware() forge.Middleware { return nil }
func (p *Provider) Authenticate(ctx context.Context, r *http.Request) (*auth.AuthContext, error) {
	headers := r.Header.Values("Authorization")
	if p.Engine == nil || len(headers) != 1 {
		return nil, ErrDenied
	}
	parts := strings.Fields(headers[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return nil, ErrDenied
	}
	for header, want := range map[string]string{"X-App-ID": p.Binding.AppID, "X-Environment-ID": p.EnvironmentID, "X-Tenant-ID": p.Binding.TenantID, "X-Org-ID": p.Binding.OrgID, "X-Installation-ID": p.Binding.InstallationID} {
		if got := r.Header.Get(header); got != "" && got != want {
			return nil, ErrDenied
		}
	}
	if app := r.URL.Query().Get("app_id"); app != "" && app != p.Binding.AppID {
		return nil, ErrDenied
	}
	clone := r.Clone(ctx)
	clone.Header = r.Header.Clone()
	clone.Header.Del("Cookie")
	clone.Header.Del("X-API-Key")
	clone.Header.Set("X-App-ID", p.Binding.AppID)
	u := *r.URL
	query := u.Query()
	query.Del("app_id")
	u.RawQuery = query.Encode()
	clone.URL = &u
	strategy, ok := p.Engine.Strategies().Get("apikey")
	if !ok {
		return nil, ErrDenied
	}
	result, err := strategy.Authenticate(ctx, clone)
	if err != nil || result == nil || result.User != nil || result.Session == nil {
		return nil, ErrDenied
	}
	for _, configured := range p.Credentials {
		key, e := p.Engine.APIKeyStore().GetAPIKey(ctx, configured.KeyID)
		if e != nil || key == nil {
			return nil, ErrDenied
		}
		if !apikey.VerifyKey(parts[1], key.KeyHash) {
			continue
		}
		if !key.IsValid() || (key.EnvID.IsNil() || key.EnvID.String() != p.EnvironmentID) || !key.UserID.IsNil() || key.AppID.String() != p.Binding.AppID || key.ServiceAccountID != configured.AccountID || result.Session.ServiceAccountID != configured.AccountID || result.Session.AppID != key.AppID || result.Session.EnvID != key.EnvID || result.Session.PrincipalKind != principal.KindService {
			return nil, ErrDenied
		}
		resolved, e := p.Engine.ResolvePrincipal(ctx, principal.Ref{Kind: principal.KindService, ID: configured.AccountID.String()})
		if e != nil || resolved == nil || !resolved.IsActive(time.Now()) || resolved.Kind != principal.KindService || resolved.ID != configured.AccountID.String() || resolved.AppID != key.AppID || (resolved.EnvID.IsNil() || resolved.EnvID.String() != p.EnvironmentID) || (!resolved.OrgID.IsNil() && resolved.OrgID.String() != p.Binding.OrgID) {
			return nil, ErrDenied
		}
		for _, scope := range key.Scopes {
			if !slices.Contains(resolved.Scopes, scope) {
				return nil, ErrDenied
			}
		}
		return &auth.AuthContext{Subject: configured.AccountID.String(), Claims: map[string]any{"principal_kind": "service_account", "principal_id": configured.AccountID.String()}, Scopes: slices.Clone(key.Scopes), Metadata: map[string]any{"credential_scheme": "bearer"}, Data: p.Binding}, nil
	}
	return nil, ErrDenied
}

type Checker interface {
	Check(context.Context, *warden.CheckRequest, ...warden.CallOption) (*warden.CheckResult, error)
}

// Authorize refuses unavailable decisions and unhandled obligations.
func Authorize(ctx context.Context, w Checker, b ecosystem.Binding, tenant, subject, destination, action string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ctx = warden.WithNamespace(warden.WithTenant(ctx, b.AppID, tenant), "")
	result, err := w.Check(ctx, &warden.CheckRequest{Subject: warden.Subject{Kind: warden.SubjectServiceAcct, ID: subject}, Action: warden.Action{Name: action}, Resource: warden.Resource{Type: "dispatch_installation", ID: b.InstallationID, Attributes: map[string]any{"installation": b.InstallationID, "app": b.AppID, "org": b.OrgID, "tenant": b.TenantID, "producer": b.Producer, "destination": destination}}, TenantID: tenant})
	if err != nil || result == nil || len(result.Obligations) > 0 {
		return errors.New("qualification: policy unavailable")
	}
	if !result.Allowed {
		return ErrDenied
	}
	return nil
}
