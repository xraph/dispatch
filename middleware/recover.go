package middleware

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/xraph/dispatch/job"
)

// Recover returns middleware that recovers from panics in the handler chain.
// Panics are converted to errors and logged with a stack trace.
func Recover(logger *slog.Logger) Middleware {
	return func(ctx context.Context, j *job.Job, next Handler) (retErr error) {
		defer func() {
			if r := recover(); r != nil {
				stack := string(debug.Stack())
				logger.Error("job handler panicked",
					slog.String("job_name", j.Name),
					slog.String("job_id", j.ID.String()),
					slog.Any("panic", r),
					slog.String("stack", stack),
				)
				retErr = fmt.Errorf("panic in job %s: %v", j.Name, r)
			}
		}()
		return next(ctx)
	}
}
