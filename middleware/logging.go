package middleware

import (
	"context"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/job"
)

// Logging returns middleware that logs job start and completion.
func Logging(logger log.Logger) Middleware {
	return func(ctx context.Context, j *job.Job, next Handler) error {
		logger.Info("job started",
			log.String("job_name", j.Name),
			log.String("job_id", j.ID.String()),
			log.String("queue", j.Queue),
		)

		start := time.Now()
		err := next(ctx)
		elapsed := time.Since(start)

		if err != nil {
			logger.Error("job failed",
				log.String("job_name", j.Name),
				log.String("job_id", j.ID.String()),
				log.Duration("elapsed", elapsed),
				log.String("error", err.Error()),
			)
		} else {
			logger.Info("job completed",
				log.String("job_name", j.Name),
				log.String("job_id", j.ID.String()),
				log.Duration("elapsed", elapsed),
			)
		}

		return err
	}
}
