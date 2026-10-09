package extension

import (
	"github.com/xraph/forge/extensions/auth"
	"github.com/xraph/vessel"
	"github.com/xraph/warden"

	"github.com/xraph/dispatch/security"
)

// SecurityConfig identifies the host's installation and policy tenant. Request
// filters and identity claims cannot override it.
type SecurityConfig struct {
	InstallationID string   `json:"installation_id" yaml:"installation_id" mapstructure:"installation_id"`
	PolicyTenant   string   `json:"policy_tenant" yaml:"policy_tenant" mapstructure:"policy_tenant"`
	Providers      []string `json:"providers" yaml:"providers" mapstructure:"providers"`
}

// WithOperatorSecurity sets the installation resource and lazy Forge/Warden adapters.
func WithOperatorSecurity(config SecurityConfig) ExtOption {
	config.Providers = append([]string(nil), config.Providers...)
	return func(e *Extension) {
		clone := config
		clone.Providers = append([]string(nil), config.Providers...)
		e.config.Security = clone
	}
}

// WithRemoteSecurity supplies a host-owned verified authenticator and authorizer.
// Custom authenticators must enforce their own cookie, origin and CSRF policy.
func WithRemoteSecurity(authenticator security.Authenticator, boundary security.Boundary) ExtOption {
	return func(e *Extension) { e.remoteAuth = authenticator; e.boundary = &boundary }
}
func (e *Extension) configureSecurity() {
	if e.remoteAuth == nil {
		e.remoteAuth = security.NewForgeAuthenticator(func() (auth.Registry, error) { return vessel.Inject[auth.Registry](e.App().Container()) }, e.config.Security.Providers...)
	}
	if e.boundary == nil {
		e.boundary = &security.Boundary{Resource: security.Resource{InstallationID: e.config.Security.InstallationID, PolicyTenant: e.config.Security.PolicyTenant}, Authorizer: &security.WardenAuthorizer{Engine: func() (*warden.Engine, error) { return vessel.Inject[*warden.Engine](e.App().Container()) }}}
	}
}
