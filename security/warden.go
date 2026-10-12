package security

import (
	"context"

	"github.com/xraph/warden"
)

// WardenAuthorizer resolves the host policy engine lazily. Unhandled obligations
// deny even an otherwise allowed decision.
type WardenAuthorizer struct {
	Engine func() (*warden.Engine, error)
}

func (a *WardenAuthorizer) Authorize(ctx context.Context, p Principal, action string, r Resource) error {
	if a == nil || a.Engine == nil {
		return ErrUnavailable
	}
	engine, err := a.Engine()
	if err != nil || engine == nil {
		return ErrUnavailable
	}
	ctx = warden.WithNamespace(warden.WithTenant(ctx, "", r.PolicyTenant), "")
	result, err := engine.Check(ctx, &warden.CheckRequest{
		TenantID: r.PolicyTenant,
		Subject:  warden.Subject{Kind: warden.SubjectKind(p.Kind), ID: p.Subject, Attributes: map[string]any{"principal_kind": p.Kind}},
		Action:   warden.Action{Name: action},
		Resource: warden.Resource{Type: "dispatch_installation", ID: r.InstallationID, Attributes: map[string]any{"policy_tenant": r.PolicyTenant, "installation_id": r.InstallationID}},
	})
	if err != nil {
		return ErrUnavailable
	}
	if result == nil || !result.Allowed || len(result.Obligations) > 0 {
		return ErrForbidden
	}
	return nil
}
