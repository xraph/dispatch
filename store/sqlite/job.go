package sqlite

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// EnqueueJob persists a new job in pending state.
func (s *Store) EnqueueJob(ctx context.Context, j *job.Job) error {
	m := toJobModel(j)
	_, err := s.sdb.NewInsert(m).Exec(ctx)
	if err != nil {
		if isDuplicateKey(err) {
			return dispatch.ErrJobAlreadyExists
		}
		return fmt.Errorf("dispatch/sqlite: enqueue job: %w", err)
	}
	return nil
}

// DequeueJobs atomically claims up to limit pending jobs from the given
// queues. SQLite doesn't support FOR UPDATE SKIP LOCKED, so we use
// BEGIN IMMEDIATE with a subquery + UPDATE pattern.
func (s *Store) DequeueJobs(ctx context.Context, queues []string, limit int) ([]*job.Job, error) {
	now := time.Now().UTC()

	// Build queue placeholders for raw SQL.
	placeholders := make([]string, len(queues))
	args := make([]any, 0, len(queues)+3)
	args = append(args, now, now) // started_at, updated_at
	for i, q := range queues {
		placeholders[i] = "?"
		args = append(args, q)
	}
	args = append(args, now, limit) // run_at <=, LIMIT

	query := fmt.Sprintf(`
		UPDATE dispatch_jobs
		SET state = 'running', started_at = ?, updated_at = ?
		WHERE id IN (
			SELECT id FROM dispatch_jobs
			WHERE state IN ('pending', 'retrying')
			  AND queue IN (%s)
			  AND run_at <= ?
			ORDER BY priority DESC, run_at ASC
			LIMIT ?
		)
		RETURNING *`,
		strings.Join(placeholders, ","),
	)

	var models []jobModel
	err := s.sdb.NewRaw(query, args...).Scan(ctx, &models)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: dequeue jobs: %w", err)
	}

	jobs := make([]*job.Job, 0, len(models))
	for i := range models {
		j, convErr := fromJobModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/sqlite: dequeue convert: %w", convErr)
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// GetJob retrieves a job by ID.
func (s *Store) GetJob(ctx context.Context, jobID id.JobID) (*job.Job, error) {
	m := new(jobModel)
	err := s.sdb.NewSelect(m).
		Where("id = ?", jobID.String()).
		Limit(1).
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, dispatch.ErrJobNotFound
		}
		return nil, fmt.Errorf("dispatch/sqlite: get job: %w", err)
	}
	return fromJobModel(m)
}

// UpdateJob persists changes to an existing job.
func (s *Store) UpdateJob(ctx context.Context, j *job.Job) error {
	m := toJobModel(j)
	m.UpdatedAt = time.Now().UTC()
	res, err := s.sdb.NewUpdate(m).WherePK().Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: update job: %w", err)
	}
	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		return dispatch.ErrJobNotFound
	}
	return nil
}

// DeleteJob removes a job by ID.
func (s *Store) DeleteJob(ctx context.Context, jobID id.JobID) error {
	res, err := s.sdb.NewDelete((*jobModel)(nil)).
		Where("id = ?", jobID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: delete job: %w", err)
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
	q := s.sdb.NewSelect(&models).
		Where("state = ?", string(state))

	if opts.Queue != "" {
		q = q.Where("queue = ?", opts.Queue)
	}

	q = q.OrderExpr("created_at ASC")

	if opts.Limit > 0 {
		q = q.Limit(opts.Limit)
	}
	if opts.Offset > 0 {
		q = q.Offset(opts.Offset)
	}

	err := q.Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: list jobs by state: %w", err)
	}

	jobs := make([]*job.Job, 0, len(models))
	for i := range models {
		j, convErr := fromJobModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/sqlite: list jobs convert: %w", convErr)
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// HeartbeatJob updates the heartbeat timestamp for a running job.
func (s *Store) HeartbeatJob(ctx context.Context, jobID id.JobID, _ id.WorkerID) error {
	now := time.Now().UTC()
	res, err := s.sdb.NewUpdate((*jobModel)(nil)).
		Set("heartbeat_at = ?", now).
		Set("updated_at = ?", now).
		Where("id = ?", jobID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: heartbeat job: %w", err)
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
	cutoff := time.Now().UTC().Add(-threshold)
	var models []jobModel
	err := s.sdb.NewSelect(&models).
		Where("state = 'running'").
		Where("heartbeat_at IS NOT NULL").
		Where("heartbeat_at < ?", cutoff).
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: reap stale jobs: %w", err)
	}

	jobs := make([]*job.Job, 0, len(models))
	for i := range models {
		j, convErr := fromJobModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/sqlite: reap stale convert: %w", convErr)
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// CountJobs returns the number of jobs matching the given options.
func (s *Store) CountJobs(ctx context.Context, opts job.CountOpts) (int64, error) {
	q := s.sdb.NewSelect((*jobModel)(nil))

	if opts.Queue != "" {
		q = q.Where("queue = ?", opts.Queue)
	}
	if opts.State != "" {
		q = q.Where("state = ?", string(opts.State))
	}

	count, err := q.Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("dispatch/sqlite: count jobs: %w", err)
	}
	return count, nil
}
