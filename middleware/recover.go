package middleware

import (
	"context"
	"fmt"
	"runtime/debug"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/job"
)

// Recover returns middleware that recovers from panics in the handler chain.
// Panics are converted to errors and logged with a stack trace.
func Recover(logger log.Logger) Middleware {
	return func(ctx context.Context, j *job.Job, next Handler) (retErr error) {
		defer func() {
			if r := recover(); r != nil {
				stack := string(debug.Stack())
				logger.Error("job handler panicked",
					log.String("job_name", j.Name),
					log.String("job_id", j.ID.String()),
					log.Any("panic", r),
					log.String("stack", stack),
				)
				retErr = fmt.Errorf("panic in job %s: %v", j.Name, r)
			}
		}()
		return next(ctx)
	}
}
