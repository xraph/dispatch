package middleware

import (
	"context"
	"log/slog"
	"time"

	"github.com/xraph/dispatch/job"
)

// Logging returns middleware that logs job start and completion.
func Logging(logger *slog.Logger) Middleware {
	return func(ctx context.Context, j *job.Job, next Handler) error {
		logger.Info("job started",
			slog.String("job_name", j.Name),
			slog.String("job_id", j.ID.String()),
			slog.String("queue", j.Queue),
		)

		start := time.Now()
		err := next(ctx)
		elapsed := time.Since(start)

		if err != nil {
			logger.Error("job failed",
				slog.String("job_name", j.Name),
				slog.String("job_id", j.ID.String()),
				slog.Duration("elapsed", elapsed),
				slog.String("error", err.Error()),
			)
		} else {
			logger.Info("job completed",
				slog.String("job_name", j.Name),
				slog.String("job_id", j.ID.String()),
				slog.Duration("elapsed", elapsed),
			)
		}

		return err
	}
}
