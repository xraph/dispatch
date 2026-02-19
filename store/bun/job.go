package bunstore

import (
	"context"
	"fmt"
	"time"

	"github.com/uptrace/bun/dialect/pgdialect"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// EnqueueJob persists a new job in pending state.
func (s *Store) EnqueueJob(ctx context.Context, j *job.Job) error {
	m := toJobModel(j)
	_, err := s.db.NewInsert().Model(m).Exec(ctx)
	if err != nil {
		if isDuplicateKey(err) {
			return dispatch.ErrJobAlreadyExists
		}
		return fmt.Errorf("dispatch/bun: enqueue job: %w", err)
	}
	return nil
}

// DequeueJobs atomically claims up to limit pending jobs from the given
// queues, sets them to running, and returns them. Uses SELECT FOR UPDATE
// SKIP LOCKED for concurrent-safe dequeue via raw SQL.
func (s *Store) DequeueJobs(ctx context.Context, queues []string, limit int) ([]*job.Job, error) {
	var models []jobModel
	_, err := s.db.NewRaw(`
		WITH dequeued AS (
			UPDATE dispatch_jobs
			SET state = 'running', started_at = NOW(), updated_at = NOW()
			WHERE id IN (
				SELECT id FROM dispatch_jobs
				WHERE state IN ('pending', 'retrying')
				  AND queue = ANY(?0)
				  AND run_at <= NOW()
				ORDER BY priority DESC, run_at ASC
				FOR UPDATE SKIP LOCKED
				LIMIT ?1
			)
			RETURNING *
		)
		SELECT * FROM dequeued ORDER BY priority DESC, run_at ASC`,
		pgdialect.Array(queues), limit,
	).Exec(ctx, &models)
	if err != nil {
		return nil, fmt.Errorf("dispatch/bun: dequeue jobs: %w", err)
	}

	jobs := make([]*job.Job, 0, len(models))
	for i := range models {
		j, convErr := fromJobModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/bun: dequeue convert: %w", convErr)
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// GetJob retrieves a job by ID.
func (s *Store) GetJob(ctx context.Context, jobID id.JobID) (*job.Job, error) {
	m := new(jobModel)
	err := s.db.NewSelect().Model(m).
		Where("id = ?", jobID.String()).
		Limit(1).
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, dispatch.ErrJobNotFound
		}
		return nil, fmt.Errorf("dispatch/bun: get job: %w", err)
	}
	return fromJobModel(m)
}

// UpdateJob persists changes to an existing job.
func (s *Store) UpdateJob(ctx context.Context, j *job.Job) error {
	m := toJobModel(j)
	m.UpdatedAt = time.Now().UTC()
	res, err := s.db.NewUpdate().Model(m).WherePK().Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/bun: update job: %w", err)
	}
	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		return dispatch.ErrJobNotFound
	}
	return nil
}

// DeleteJob removes a job by ID.
func (s *Store) DeleteJob(ctx context.Context, jobID id.JobID) error {
	res, err := s.db.NewDelete().
		TableExpr("dispatch_jobs").
		Where("id = ?", jobID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/bun: delete job: %w", err)
	}
	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		return dispatch.ErrJobNotFound
	}
	return nil
}

// ListJobsByState returns jobs matching the given state.
func (s *Store) ListJobsByState(ctx context.Context, state job.State, opts job.ListOpts) ([]*job.Job, error) {
	var models []jobModel
	q := s.db.NewSelect().Model(&models).
		Where("state = ?", string(state))

	if opts.Queue != "" {
		q = q.Where("queue = ?", opts.Queue)
	}

	q = q.Order("created_at ASC")

	if opts.Limit > 0 {
		q = q.Limit(opts.Limit)
	}
	if opts.Offset > 0 {
		q = q.Offset(opts.Offset)
	}

	err := q.Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("dispatch/bun: list jobs by state: %w", err)
	}

	jobs := make([]*job.Job, 0, len(models))
	for i := range models {
		j, convErr := fromJobModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/bun: list jobs convert: %w", convErr)
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// HeartbeatJob updates the heartbeat timestamp for a running job.
func (s *Store) HeartbeatJob(ctx context.Context, jobID id.JobID, _ id.WorkerID) error {
	res, err := s.db.NewUpdate().
		TableExpr("dispatch_jobs").
		Set("heartbeat_at = NOW()").
		Set("updated_at = NOW()").
		Where("id = ?", jobID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/bun: heartbeat job: %w", err)
	}
	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		return dispatch.ErrJobNotFound
	}
	return nil
}

// ReapStaleJobs returns running jobs whose last heartbeat is older than
// the given threshold.
func (s *Store) ReapStaleJobs(ctx context.Context, threshold time.Duration) ([]*job.Job, error) {
	var models []jobModel
	err := s.db.NewSelect().Model(&models).
		Where("state = 'running'").
		Where("heartbeat_at IS NOT NULL").
		Where("heartbeat_at < NOW() - ?::interval", threshold.String()).
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("dispatch/bun: reap stale jobs: %w", err)
	}

	jobs := make([]*job.Job, 0, len(models))
	for i := range models {
		j, convErr := fromJobModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/bun: reap stale convert: %w", convErr)
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// CountJobs returns the number of jobs matching the given options.
func (s *Store) CountJobs(ctx context.Context, opts job.CountOpts) (int64, error) {
	q := s.db.NewSelect().TableExpr("dispatch_jobs")

	if opts.Queue != "" {
		q = q.Where("queue = ?", opts.Queue)
	}
	if opts.State != "" {
		q = q.Where("state = ?", string(opts.State))
	}

	count, err := q.Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("dispatch/bun: count jobs: %w", err)
	}
	return int64(count), nil
}
