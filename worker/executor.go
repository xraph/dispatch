// Package worker provides the job execution engine — an Executor that
// invokes registered handlers through middleware, and a Pool that
// manages concurrent worker goroutines polling for jobs.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/middleware"
)

// Executor runs a single job through middleware and the registered handler,
// then handles retry logic, DLQ push, state updates, and lifecycle events.
type Executor struct {
	registry   *job.Registry
	extensions *ext.Registry
	store      job.Store
	dlqService *dlq.Service
	backoff    backoff.Strategy
	mw         middleware.Middleware
	logger     *slog.Logger
}

// NewExecutor creates an Executor with the given dependencies.
func NewExecutor(
	registry *job.Registry,
	extensions *ext.Registry,
	store job.Store,
	dlqService *dlq.Service,
	bo backoff.Strategy,
	logger *slog.Logger,
	mws ...middleware.Middleware,
) *Executor {
	return &Executor{
		registry:   registry,
		extensions: extensions,
		store:      store,
		dlqService: dlqService,
		backoff:    bo,
		mw:         middleware.Chain(mws...),
		logger:     logger,
	}
}

// Execute runs a job through the middleware chain and handler.
// On success: marks completed, emits JobCompleted.
// On failure with retries remaining: marks retrying with backoff, emits JobRetrying.
// On failure with retries exhausted: marks failed, pushes to DLQ, emits JobFailed + JobDLQ.
func (e *Executor) Execute(ctx context.Context, j *job.Job) error {
	handler, ok := e.registry.Get(j.Name)
	if !ok {
		return fmt.Errorf("no handler registered for job %q", j.Name)
	}

	start := time.Now()

	// The terminal handler that calls the registered job handler.
	terminal := func(ctx context.Context) error {
		return handler(ctx, j.Payload)
	}

	// Run through middleware chain.
	err := e.mw(ctx, j, terminal)
	elapsed := time.Since(start)

	now := time.Now().UTC()
	j.UpdatedAt = now

	if err != nil {
		return e.handleFailure(ctx, j, err, now)
	}

	return e.handleSuccess(ctx, j, now, elapsed)
}

// handleSuccess marks the job as completed and emits the lifecycle event.
func (e *Executor) handleSuccess(ctx context.Context, j *job.Job, now time.Time, elapsed time.Duration) error {
	j.State = job.StateCompleted
	j.CompletedAt = &now

	if updateErr := e.store.UpdateJob(ctx, j); updateErr != nil {
		e.logger.Error("failed to update job after success",
			slog.String("job_id", j.ID.String()),
			slog.String("job_name", j.Name),
			slog.String("error", updateErr.Error()),
		)
		return updateErr
	}

	e.extensions.EmitJobCompleted(ctx, j, elapsed)
	return nil
}

// handleFailure increments the retry counter and either retries or sends to DLQ.
func (e *Executor) handleFailure(ctx context.Context, j *job.Job, handlerErr error, now time.Time) error {
	j.RetryCount++
	j.LastError = handlerErr.Error()

	if j.RetryCount <= j.MaxRetries {
		return e.scheduleRetry(ctx, j, now)
	}

	return e.sendToDLQ(ctx, j, handlerErr)
}

// scheduleRetry sets the job to StateRetrying with a backoff delay.
func (e *Executor) scheduleRetry(ctx context.Context, j *job.Job, now time.Time) error {
	delay := e.backoff.Delay(j.RetryCount)
	nextRunAt := now.Add(delay)
	j.RunAt = nextRunAt
	j.State = job.StateRetrying

	if updateErr := e.store.UpdateJob(ctx, j); updateErr != nil {
		e.logger.Error("failed to update job for retry",
			slog.String("job_id", j.ID.String()),
			slog.String("error", updateErr.Error()),
		)
		return updateErr
	}

	e.extensions.EmitJobRetrying(ctx, j, j.RetryCount, nextRunAt)

	e.logger.Info("job scheduled for retry",
		slog.String("job_id", j.ID.String()),
		slog.String("job_name", j.Name),
		slog.Int("attempt", j.RetryCount),
		slog.Int("max_retries", j.MaxRetries),
		slog.Duration("delay", delay),
	)

	return fmt.Errorf("job %s retry %d/%d: %w", j.Name, j.RetryCount, j.MaxRetries, fmt.Errorf("%s", j.LastError))
}

// sendToDLQ marks the job as failed, pushes it to the DLQ, and emits events.
func (e *Executor) sendToDLQ(ctx context.Context, j *job.Job, handlerErr error) error {
	j.State = job.StateFailed

	if updateErr := e.store.UpdateJob(ctx, j); updateErr != nil {
		e.logger.Error("failed to update job as failed",
			slog.String("job_id", j.ID.String()),
			slog.String("error", updateErr.Error()),
		)
		return updateErr
	}

	if e.dlqService != nil {
		if dlqErr := e.dlqService.Push(ctx, j, handlerErr); dlqErr != nil {
			e.logger.Error("failed to push job to DLQ",
				slog.String("job_id", j.ID.String()),
				slog.String("error", dlqErr.Error()),
			)
		}
	}

	e.extensions.EmitJobFailed(ctx, j, handlerErr)
	e.extensions.EmitJobDLQ(ctx, j, handlerErr)

	e.logger.Warn("job moved to DLQ after exhausting retries",
		slog.String("job_id", j.ID.String()),
		slog.String("job_name", j.Name),
		slog.Int("retry_count", j.RetryCount),
		slog.String("error", handlerErr.Error()),
	)

	return handlerErr
}
