package dwp

import (
	"context"
	"fmt"
	"strings"
)

// Identity represents an authenticated caller.
type Identity struct {
	// Subject is the authenticated user/service ID.
	Subject string `json:"subject"`

	// AppID scopes to a tenant application.
	AppID string `json:"app_id,omitempty"`

	// OrgID scopes to a tenant organization.
	OrgID string `json:"org_id,omitempty"`

	// Scopes defines what operations are permitted.
	// Examples: "job:write", "workflow:read", "admin", "*"
	Scopes []string `json:"scopes,omitempty"`
}

// HasScope returns true if the identity has the given scope.
// A wildcard "*" scope grants all permissions.
func (id *Identity) HasScope(scope string) bool {
	for _, s := range id.Scopes {
		if s == "*" || s == scope {
			return true
		}
	}
	return false
}

// Authenticator validates credentials and returns an identity.
type Authenticator interface {
	Authenticate(ctx context.Context, token string) (*Identity, error)
}

// ErrUnauthorized indicates authentication failure.
var ErrUnauthorized = fmt.Errorf("dwp: unauthorized")

// ── API Key authenticator ───────────────────────────

// APIKeyEntry maps a token to an identity.
type APIKeyEntry struct {
	Token    string
	Identity Identity
}

// APIKeyAuthenticator validates API keys against a static list.
type APIKeyAuthenticator struct {
	keys map[string]*Identity
}

// NewAPIKeyAuthenticator creates an API key authenticator.
func NewAPIKeyAuthenticator(entries ...APIKeyEntry) *APIKeyAuthenticator {
	keys := make(map[string]*Identity, len(entries))
	for _, e := range entries {
		id := e.Identity
		keys[e.Token] = &id
	}
	return &APIKeyAuthenticator{keys: keys}
}

func (a *APIKeyAuthenticator) Authenticate(_ context.Context, token string) (*Identity, error) {
	id, ok := a.keys[token]
	if !ok {
		return nil, ErrUnauthorized
	}
	return id, nil
}

// ── No-op authenticator ─────────────────────────────

// NoopAuthenticator accepts all tokens with a wildcard identity.
// Use for development only.
type NoopAuthenticator struct{}

func (a *NoopAuthenticator) Authenticate(_ context.Context, _ string) (*Identity, error) {
	return &Identity{
		Subject: "anonymous",
		Scopes:  []string{"*"},
	}, nil
}

// ── Composite authenticator ─────────────────────────

// CompositeAuthenticator tries multiple authenticators in order.
// The first successful authentication wins.
type CompositeAuthenticator struct {
	authenticators []Authenticator
}

// NewCompositeAuthenticator chains multiple authenticators.
func NewCompositeAuthenticator(auths ...Authenticator) *CompositeAuthenticator {
	return &CompositeAuthenticator{authenticators: auths}
}

func (c *CompositeAuthenticator) Authenticate(ctx context.Context, token string) (*Identity, error) {
	for _, auth := range c.authenticators {
		id, err := auth.Authenticate(ctx, token)
		if err == nil {
			return id, nil
		}
	}
	return nil, ErrUnauthorized
}

// ── Scope constants ─────────────────────────────────

const (
	ScopeJobRead       = "job:read"
	ScopeJobWrite      = "job:write"
	ScopeWorkflowRead  = "workflow:read"
	ScopeWorkflowWrite = "workflow:write"
	ScopeEventWrite    = "event:write"
	ScopeCronRead      = "cron:read"
	ScopeDLQRead       = "dlq:read"
	ScopeDLQWrite      = "dlq:write"
	ScopeStatsRead     = "stats:read"
	ScopeSubscribe     = "subscribe"
	ScopeAdmin         = "admin"
	ScopeAll           = "*"
)

// RequiredScope returns the minimum scope required for a DWP method.
func RequiredScope(method string) string {
	switch {
	case method == MethodAuth:
		return "" // No scope needed for auth.
	case strings.HasPrefix(method, "job."):
		if method == MethodJobGet || method == MethodJobList {
			return ScopeJobRead
		}
		return ScopeJobWrite
	case strings.HasPrefix(method, "workflow."):
		if method == MethodWorkflowGet || method == MethodWorkflowTimeline {
			return ScopeWorkflowRead
		}
		return ScopeWorkflowWrite
	case method == MethodSubscribe, method == MethodUnsubscribe:
		return ScopeSubscribe
	case method == MethodCronList:
		return ScopeCronRead
	case strings.HasPrefix(method, "dlq."):
		if method == MethodDLQList {
			return ScopeDLQRead
		}
		return ScopeDLQWrite
	case method == MethodStats:
		return ScopeStatsRead
	case strings.HasPrefix(method, "federation."):
		return ScopeAdmin
	default:
		return ScopeAdmin
	}
}
