package middleware

import (
	"context"
	"log/slog"

	"github.com/xraph/dispatch/job"
)

// Timeout returns middleware that enforces a per-job execution deadline.
// If the job has a non-zero Timeout, a context.WithTimeout wraps the handler
// call. When the deadline is exceeded the context is cancelled and the
// handler should return context.DeadlineExceeded.
func Timeout(logger *slog.Logger) Middleware {
	return func(ctx context.Context, j *job.Job, next Handler) error {
		if j.Timeout > 0 {
			logger.Debug("job timeout set",
				slog.String("job_id", j.ID.String()),
				slog.Duration("timeout", j.Timeout),
			)
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, j.Timeout)
			defer cancel()
		}
		return next(ctx)
	}
}
