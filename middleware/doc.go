// Package middleware provides composable middleware for job execution.
//
// A [Middleware] is a function that wraps a job handler. Middleware are
// composed into a chain using [Chain] and applied before each job executes.
// They are applied right-to-left: the first middleware in the slice is the
// outermost wrapper.
//
//	// logging → recover → handler
//	chain := middleware.Chain(middleware.Logging(logger), middleware.Recover())
//
// # Built-in Middleware
//
//   - [Logging] — logs job name, queue, duration, and outcome at each execution
//   - [Recover] — catches panics and converts them to errors
//   - [Timeout] — cancels the job context after a configured duration
//   - [Tracing] — wraps execution in an OpenTelemetry span
//   - [Metrics] — records per-job duration and outcome counters
//   - [Scope] — extracts Forge app/org IDs from the job and injects them into context
//
// # Writing Custom Middleware
//
//	func MyMiddleware() middleware.Middleware {
//	    return func(ctx context.Context, j *job.Job, next middleware.Handler) error {
//	        // pre-processing
//	        err := next(ctx)
//	        // post-processing
//	        return err
//	    }
//	}
//
// Middleware MUST call next to continue the chain unless intentionally
// short-circuiting (e.g., circuit breaker, rate limiting).
package middleware
