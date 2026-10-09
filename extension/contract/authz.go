package contract

import (
	"context"
	"errors"

	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch/security"
)

const WardenName = "dispatchInstallationOperator"

func (d Deps) authorize(ctx context.Context, p fc.Principal, intent string) error {
	principal, err := security.FromContract(p)
	if err == nil {
		err = d.Security.Check(ctx, principal, security.ContractOperation(intent))
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
		return fc.Decision{}, fc.ErrPermissionDenied
	}
	err := w.deps.authorize(ctx, p, a.Intent)
	return fc.Decision{Allow: err == nil}, err
}
