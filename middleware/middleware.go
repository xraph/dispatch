// Package middleware provides composable middleware for job execution.
// Middleware wraps handler calls synchronously and can modify execution
// (recover from panics, inject scope, log, add tracing, etc.).
package middleware

import (
	"context"

	"github.com/xraph/dispatch/job"
)

// Handler is the terminal function that executes job logic.
type Handler func(ctx context.Context) error

// Middleware wraps a Handler with cross-cutting logic.
// It receives the current context, the job being executed, and the
// next handler to call. Middleware MUST call next to continue the chain
// (unless short-circuiting on error).
type Middleware func(ctx context.Context, j *job.Job, next Handler) error

// Chain composes multiple middleware into a single Middleware.
// Middleware are applied right-to-left: the first middleware in the
// list is the outermost wrapper.
//
// Example: Chain(logging, recover, scope) executes as:
//
//	logging → recover → scope → handler
func Chain(mws ...Middleware) Middleware {
	return func(ctx context.Context, j *job.Job, next Handler) error {
		// Build the chain from the end backwards.
		h := next
		for i := len(mws) - 1; i >= 0; i-- {
			mw := mws[i]
			prev := h
			h = func(ctx context.Context) error {
				return mw(ctx, j, prev)
			}
		}
		return h(ctx)
	}
}
