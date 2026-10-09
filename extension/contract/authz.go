package contract

import (
	"context"
	"errors"

	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch/security"
)

const WardenName = "dispatchInstallationOperator"

func (d Deps) authorize(ctx context.Context, p fc.Principal, intent string) error {
	return d.authorizeOperation(ctx, p, security.ContractOperation(intent))
}
func (d Deps) authorizeOperation(ctx context.Context, p fc.Principal, op security.Operation) error {
	principal, err := security.FromContract(p)
	if err == nil {
		err = d.Security.Check(ctx, principal, op)
	} else {
		err = d.Security.AuthenticationDenied(ctx, op)
	}
	if err == nil {
		return nil
	}
	if errors.Is(err, security.ErrUnauthenticated) {
		return fc.ErrUnauthenticated
	}
	if errors.Is(err, security.ErrForbidden) {
		return fc.ErrPermissionDenied
	}
	return fc.ErrUnavailable
}

type operatorWarden struct{ deps Deps }

func (w operatorWarden) Authorize(ctx context.Context, p fc.Principal, a fc.Action) (fc.Decision, error) {
	op := security.ContractOperation(a.Intent)
	if a.Contributor != ContributorName || op.Action == "" || (op.Action == security.OperatorWrite && a.Kind != fc.KindCommand) || (op.Action == security.OperatorRead && a.Kind != fc.KindQuery) {
		_ = w.deps.Security.Check(ctx, security.Principal{}, security.Operation{}) //nolint:errcheck // Malformed Warden admissions remain denied.
		return fc.Decision{}, fc.ErrPermissionDenied
	}
	op.Target = "unresolved"
	if raw, ok := a.Resource["id"].(string); ok {
		target, targetErr := auditResourceTarget(a.Intent, raw)
		op.Target = target
		if targetErr != nil {
			op.Target = "invalid-target"
		}
	}
	err := w.deps.authorizeOperation(ctx, p, op)
	return fc.Decision{Allow: err == nil}, err
}
