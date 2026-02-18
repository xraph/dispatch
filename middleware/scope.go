package middleware

import (
	"context"

	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/scope"
)

// Scope returns middleware that restores multi-tenant scope from the
// job's ScopeAppID/ScopeOrgID fields into the context. This ensures
// handlers see the same forge.Scope as the original enqueue caller.
func Scope() Middleware {
	return func(ctx context.Context, j *job.Job, next Handler) error {
		ctx = scope.Restore(ctx, j.ScopeAppID, j.ScopeOrgID)
		return next(ctx)
	}
}
