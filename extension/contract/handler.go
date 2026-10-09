package contract

import (
	"context"
	"errors"
	"time"

	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/security"
)

const queryTimeout = 10 * time.Second
const commandTimeout = 30 * time.Second

// handle bounds store work and maps errors at the contract boundary.
// Commands carry the authenticated subject to engine operator hooks.
func handle[I, O any](deps Deps, intent string, command bool, fn func(context.Context, I, fc.Principal) (O, error)) func(context.Context, I, fc.Principal) (O, error) {
	return func(ctx context.Context, input I, principal fc.Principal) (O, error) {
		timeout := queryTimeout
		if command {
			timeout = commandTimeout
			actor := ""
			if principal.User != nil {
				actor = principal.User.Subject
			}
			ctx = ext.WithActor(ctx, actor)
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		op := security.ContractOperation(intent)
		target, targetErr := auditInputTarget(intent, input)
		op.Target = target
		if err := deps.authorizeOperation(ctx, principal, op); err != nil {
			var zero O
			return zero, err
		}
		if targetErr != nil && command {
			var zero O
			return zero, fc.ErrBadRequest
		}
		verified, identityErr := security.FromContract(principal)
		if identityErr != nil {
			var zero O
			return zero, fc.ErrUnauthenticated
		}
		attempt, err := deps.Security.BeginCommand(ctx, verified, op)
		if err != nil {
			var zero O
			return zero, fc.ErrUnavailable
		}
		ctx = durable.WithAuditMetadata(ctx, security.Metadata(verified))
		output, err := fn(ctx, input, principal)
		if outcomeErr := deps.Security.FinishCommand(ctx, attempt, err == nil); outcomeErr != nil {
			var zero O
			return zero, errors.Join(fc.ErrUnavailable, outcomeErr)
		}
		return output, deps.mapError(intent, err)
	}
}
