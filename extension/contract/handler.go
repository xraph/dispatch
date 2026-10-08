package contract

import (
	"context"
	"time"

	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch/ext"
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
		output, err := fn(ctx, input, principal)
		return output, deps.mapError(intent, err)
	}
}
