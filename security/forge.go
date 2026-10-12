package security

import (
	"context"
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/xraph/forge/extensions/auth"
	fc "github.com/xraph/forge/extensions/dashboard/contract"
)

func verifiedPrincipal(subject string, claims map[string]any) (Principal, error) {
	p := Principal{Subject: subject, Kind: "user"}
	if v, ok := claims["principal_kind"]; ok {
		kind, valid := v.(string)
		if !valid {
			return Principal{}, ErrUnauthenticated
		}
		if kind == "service_account" {
			kind = "service_acct"
		}
		p.Kind = kind
	}
	if v, ok := claims["principal_id"]; ok {
		if _, kindPresent := claims["principal_kind"]; !kindPresent {
			return Principal{}, ErrUnauthenticated
		}
		id, valid := v.(string)
		if !valid || id != subject {
			return Principal{}, ErrUnauthenticated
		}
	}
	return p, p.Validate()
}
func FromForge(a *auth.AuthContext) (Principal, error) {
	if a == nil {
		return Principal{}, ErrUnauthenticated
	}
	return verifiedPrincipal(a.Subject, a.Claims)
}
func FromContract(p fc.Principal) (Principal, error) {
	if p.User == nil {
		return Principal{}, ErrUnauthenticated
	}
	claims := maps.Clone(p.User.Claims)
	if claims == nil {
		claims = make(map[string]any)
	}
	for key, value := range p.Claims {
		if key == "principal_kind" || key == "principal_id" {
			if existing, ok := claims[key]; ok {
				oldKind, oldOK := existing.(string)
				newKind, newOK := value.(string)
				if !oldOK || !newOK || oldKind != newKind {
					return Principal{}, ErrUnauthenticated
				}
			}
		}
		claims[key] = value
	}
	return verifiedPrincipal(p.User.Subject, claims)
}

// ForgeAuthenticator resolves providers on every request. Only provider-attested
// explicit credentials are accepted, including after an upstream cookie bridge.
type ForgeAuthenticator struct {
	Registry  func() (auth.Registry, error)
	Providers []string
}

func NewForgeAuthenticator(registry func() (auth.Registry, error), providers ...string) *ForgeAuthenticator {
	if len(providers) == 0 {
		providers = []string{"session"}
	}
	return &ForgeAuthenticator{Registry: registry, Providers: slices.Clone(providers)}
}
func (a *ForgeAuthenticator) Authenticate(ctx context.Context, r *http.Request) (Principal, error) {
	if r == nil || a == nil || a.Registry == nil {
		return Principal{}, ErrUnauthenticated
	}
	fields := strings.Fields(r.Header.Get("Authorization"))
	if len(fields) != 2 || fields[1] == "" || (!strings.EqualFold(fields[0], "bearer") && !strings.EqualFold(fields[0], "dpop")) {
		return Principal{}, ErrUnauthenticated
	}
	ctx, cancel := context.WithTimeout(ctx, CheckTimeout)
	defer cancel()
	reg, err := a.Registry()
	if err != nil || reg == nil {
		return Principal{}, ErrUnauthenticated
	}
	request := r.Clone(ctx)
	request.Header.Del("Cookie")
	for _, name := range a.Providers {
		provider, err := reg.Get(name)
		if err != nil || provider == nil {
			continue
		}
		result, err := provider.Authenticate(ctx, request)
		if err != nil || result == nil {
			continue
		}
		scheme, ok := result.Metadata["credential_scheme"].(string)
		if !ok || (scheme != "bearer" && scheme != "dpop") {
			continue
		}
		if (scheme == "dpop") != strings.EqualFold(fields[0], "dpop") {
			continue
		}
		p, err := FromForge(result)
		if err == nil {
			return p, nil
		}
	}
	return Principal{}, ErrUnauthenticated
}
