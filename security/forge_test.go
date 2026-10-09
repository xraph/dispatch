package security

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xraph/forge"
	"github.com/xraph/forge/extensions/auth"
	dashauth "github.com/xraph/forge/extensions/dashboard/auth"
	fc "github.com/xraph/forge/extensions/dashboard/contract"
)

type testProvider struct {
	fn func(context.Context, *http.Request) (*auth.AuthContext, error)
}

func (p testProvider) Name() string                       { return "session" }
func (p testProvider) Type() auth.SecuritySchemeType      { return auth.SecurityTypeHTTP }
func (p testProvider) OpenAPIScheme() auth.SecurityScheme { return auth.SecurityScheme{} }
func (p testProvider) Middleware() forge.Middleware       { return nil }
func (p testProvider) Authenticate(ctx context.Context, r *http.Request) (*auth.AuthContext, error) {
	return p.fn(ctx, r)
}

type proofKey struct{}

func TestForgeExplicitCredentialProvenanceAndLateRegistration(t *testing.T) {
	reg := auth.NewRegistry(nil, forge.NewNoopLogger())
	adapter := NewForgeAuthenticator(func() (auth.Registry, error) { return reg, nil })
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "https://host/dispatch/v1/jobs/x/cancel", nil)
	request.Header.Set("Authorization", "Bearer token")
	if _, err := adapter.Authenticate(context.Background(), request); err == nil {
		t.Fatal("missing provider allowed")
	}
	scheme := any("bearer")
	empty := false
	err := reg.Register(testProvider{fn: func(ctx context.Context, r *http.Request) (*auth.AuthContext, error) {
		if r.Method != request.Method || r.URL.String() != request.URL.String() || r.Header.Get("DPoP") != request.Header.Get("DPoP") {
			t.Fatal("lost request proof")
		}
		if r.Header.Get("Cookie") != "" {
			t.Fatal("cookie passed")
		}
		if ctx.Value(proofKey{}) != "trusted" {
			t.Fatal("lost context")
		}
		subject := "operator"
		if empty {
			subject = ""
		}
		return &auth.AuthContext{Subject: subject, Metadata: map[string]any{"credential_scheme": scheme}}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(request.Context(), proofKey{}, "trusted")
	request = request.WithContext(ctx)
	request.Header.Set("Cookie", "session=valid")
	request.Header.Set("DPoP", "real-proof")
	for _, provenance := range []any{nil, 1, "cookie", "unknown", "bearer"} {
		scheme = provenance
		_, err := adapter.Authenticate(request.Context(), request)
		if (err == nil) != (provenance == "bearer") {
			t.Fatalf("scheme %v: %v", provenance, err)
		}
	}
	// A bridged cookie header is rejected by the provider's trusted metadata.
	scheme = "cookie"
	if _, err := adapter.Authenticate(request.Context(), request); err == nil {
		t.Fatal("bridge allowed")
	}
	scheme = "dpop"
	if _, err := adapter.Authenticate(request.Context(), request); err == nil {
		t.Fatal("bound downgrade allowed")
	}
	request.Header.Set("Authorization", "DPoP token")
	if _, err := adapter.Authenticate(request.Context(), request); err != nil {
		t.Fatal(err)
	}
	empty = true
	if _, err := adapter.Authenticate(request.Context(), request); err == nil {
		t.Fatal("empty identity allowed")
	}
	request.Header.Del("Authorization")
	request.Header.Set("Origin", "https://foreign.example")
	if _, err := adapter.Authenticate(request.Context(), request); err == nil {
		t.Fatal("cookie only allowed")
	}
}
func TestForgeNilSuccessDenies(t *testing.T) {
	reg := auth.NewRegistry(nil, forge.NewNoopLogger())
	_ = reg.Register(testProvider{fn: func(context.Context, *http.Request) (*auth.AuthContext, error) { return nil, nil }})
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer x")
	adapter := NewForgeAuthenticator(func() (auth.Registry, error) { return reg, nil })
	if _, err := adapter.Authenticate(request.Context(), request); err == nil {
		t.Fatal("nil success allowed")
	}
}

func TestContractIdentityClaimsCannotHideMachineKind(t *testing.T) {
	p := fc.Principal{User: &dashauth.UserInfo{Subject: "machine", Claims: map[string]any{"principal_kind": "agent"}}}
	if _, err := FromContract(p); err == nil {
		t.Fatal("machine kind inferred as human")
	}
	p.Claims = map[string]any{"principal_kind": "user"}
	if _, err := FromContract(p); err == nil {
		t.Fatal("conflicting kinds allowed")
	}
	p.User.Claims = map[string]any{"principal_kind": map[string]any{"kind": "user"}}
	if _, err := FromContract(p); err == nil {
		t.Fatal("malformed kind accepted")
	}
	p.User.Subject = ""
	p.Claims = nil
	p.User.Claims = nil
	if _, err := FromContract(p); err == nil {
		t.Fatal("empty subject accepted")
	}
}
