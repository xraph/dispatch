package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// EnqueueJob persists a new job in pending state.
func (s *Store) EnqueueJob(ctx context.Context, j *job.Job) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO dispatch_jobs (
			id, name, queue, payload, state, priority, max_retries, retry_count,
			last_error, scope_app_id, scope_org_id, worker_id,
			run_at, started_at, completed_at, heartbeat_at, created_at, updated_at,
			timeout
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8,
			$9, $10, $11, $12,
			$13, $14, $15, $16, $17, $18,
			$19
		)`,
		j.ID.String(), j.Name, j.Queue, j.Payload, string(j.State),
		j.Priority, j.MaxRetries, j.RetryCount,
		j.LastError, j.ScopeAppID, j.ScopeOrgID, j.WorkerID.String(),
		j.RunAt, j.StartedAt, j.CompletedAt, j.HeartbeatAt,
		j.CreatedAt, j.UpdatedAt,
		j.Timeout.Nanoseconds(),
	)
	if err != nil {
		// Check for unique violation (duplicate ID).
		if isDuplicateKey(err) {
			return dispatch.ErrJobAlreadyExists
		}
		return fmt.Errorf("dispatch/postgres: enqueue job: %w", err)
	}
	return nil
}

// DequeueJobs atomically claims up to limit pending jobs from the given
// queues, sets them to running, and returns them. Uses SELECT FOR UPDATE
// SKIP LOCKED for concurrent-safe dequeue.
func (s *Store) DequeueJobs(ctx context.Context, queues []string, limit int) ([]*job.Job, error) {
	rows, err := s.pool.Query(ctx, `
		WITH dequeued AS (
			UPDATE dispatch_jobs
			SET state = 'running', started_at = NOW(), updated_at = NOW()
			WHERE id IN (
				SELECT id FROM dispatch_jobs
				WHERE state IN ('pending', 'retrying')
				  AND queue = ANY($1)
				  AND run_at <= NOW()
				ORDER BY priority DESC, run_at ASC
				FOR UPDATE SKIP LOCKED
				LIMIT $2
			)
			RETURNING
				id, name, queue, payload, state, priority, max_retries, retry_count,
				last_error, scope_app_id, scope_org_id, worker_id,
				run_at, started_at, completed_at, heartbeat_at, created_at, updated_at,
				timeout
		)
		SELECT * FROM dequeued ORDER BY priority DESC, run_at ASC`,
		queues, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("dispatch/postgres: dequeue jobs: %w", err)
	}
	defer rows.Close()

	return collectJobs(rows)
}

// GetJob retrieves a job by ID.
func (s *Store) GetJob(ctx context.Context, jobID id.JobID) (*job.Job, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT
			id, name, queue, payload, state, priority, max_retries, retry_count,
			last_error, scope_app_id, scope_org_id, worker_id,
			run_at, started_at, completed_at, heartbeat_at, created_at, updated_at,
			timeout
		FROM dispatch_jobs
		WHERE id = $1`,
		jobID.String(),
	)

	j, err := scanJob(row)
	if err != nil {
		if isNoRows(err) {
			return nil, dispatch.ErrJobNotFound
		}
		return nil, fmt.Errorf("dispatch/postgres: get job: %w", err)
	}
	return j, nil
}

// UpdateJob persists changes to an existing job.
func (s *Store) UpdateJob(ctx context.Context, j *job.Job) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE dispatch_jobs SET
			name = $2, queue = $3, payload = $4, state = $5,
			priority = $6, max_retries = $7, retry_count = $8,
			last_error = $9, scope_app_id = $10, scope_org_id = $11,
			worker_id = $12, run_at = $13, started_at = $14,
			completed_at = $15, heartbeat_at = $16, timeout = $17,
			updated_at = NOW()
		WHERE id = $1`,
		j.ID.String(), j.Name, j.Queue, j.Payload, string(j.State),
		j.Priority, j.MaxRetries, j.RetryCount,
		j.LastError, j.ScopeAppID, j.ScopeOrgID,
		j.WorkerID.String(), j.RunAt, j.StartedAt,
		j.CompletedAt, j.HeartbeatAt, j.Timeout.Nanoseconds(),
	)
	if err != nil {
		return fmt.Errorf("dispatch/postgres: update job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return dispatch.ErrJobNotFound
	}
	return nil
}

// DeleteJob removes a job by ID.
func (s *Store) DeleteJob(ctx context.Context, jobID id.JobID) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM dispatch_jobs WHERE id = $1`, jobID.String())
	if err != nil {
		return fmt.Errorf("dispatch/postgres: delete job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return dispatch.ErrJobNotFound
	}
	return nil
}

// ListJobsByState returns jobs matching the given state.
func (s *Store) ListJobsByState(ctx context.Context, state job.State, opts job.ListOpts) ([]*job.Job, error) {
	query := `
		SELECT
			id, name, queue, payload, state, priority, max_retries, retry_count,
			last_error, scope_app_id, scope_org_id, worker_id,
			run_at, started_at, completed_at, heartbeat_at, created_at, updated_at,
			timeout
		FROM dispatch_jobs
		WHERE state = $1`
	args := []interface{}{string(state)}
	argIdx := 2

	if opts.Queue != "" {
		query += fmt.Sprintf(" AND queue = $%d", argIdx)
		args = append(args, opts.Queue)
		argIdx++
	}

	query += " ORDER BY created_at ASC"

	if opts.Limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIdx)
		args = append(args, opts.Limit)
		argIdx++
	}
	if opts.Offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", argIdx)
		args = append(args, opts.Offset)
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("dispatch/postgres: list jobs by state: %w", err)
	}
	defer rows.Close()

	return collectJobs(rows)
}

// HeartbeatJob updates the heartbeat timestamp for a running job.
func (s *Store) HeartbeatJob(ctx context.Context, jobID id.JobID, _ id.WorkerID) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE dispatch_jobs SET heartbeat_at = NOW(), updated_at = NOW() WHERE id = $1`,
		jobID.String(),
	)
	if err != nil {
		return fmt.Errorf("dispatch/postgres: heartbeat job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return dispatch.ErrJobNotFound
	}
	return nil
}

// ReapStaleJobs returns running jobs whose last heartbeat is older than
// the given threshold.
func (s *Store) ReapStaleJobs(ctx context.Context, threshold time.Duration) ([]*job.Job, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			id, name, queue, payload, state, priority, max_retries, retry_count,
			last_error, scope_app_id, scope_org_id, worker_id,
			run_at, started_at, completed_at, heartbeat_at, created_at, updated_at,
			timeout
		FROM dispatch_jobs
		WHERE state = 'running'
		  AND heartbeat_at IS NOT NULL
		  AND heartbeat_at < NOW() - $1::interval`,
		threshold.String(),
	)
	if err != nil {
		return nil, fmt.Errorf("dispatch/postgres: reap stale jobs: %w", err)
	}
	defer rows.Close()

	return collectJobs(rows)
}

// CountJobs returns the number of jobs matching the given options.
func (s *Store) CountJobs(ctx context.Context, opts job.CountOpts) (int64, error) {
	query := `SELECT COUNT(*) FROM dispatch_jobs WHERE 1=1`
	args := []interface{}{}
	argIdx := 1

	if opts.Queue != "" {
		query += fmt.Sprintf(" AND queue = $%d", argIdx)
		args = append(args, opts.Queue)
		argIdx++
	}
	if opts.State != "" {
		query += fmt.Sprintf(" AND state = $%d", argIdx)
		args = append(args, string(opts.State))
	}

	var count int64
	err := s.pool.QueryRow(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("dispatch/postgres: count jobs: %w", err)
	}
	return count, nil
}

// scanJob scans a single job row.
func scanJob(row pgx.Row) (*job.Job, error) {
	var (
		j         job.Job
		idStr     string
		stateStr  string
		workerStr string
		timeoutNs int64
	)
	err := row.Scan(
		&idStr, &j.Name, &j.Queue, &j.Payload, &stateStr,
		&j.Priority, &j.MaxRetries, &j.RetryCount,
		&j.LastError, &j.ScopeAppID, &j.ScopeOrgID, &workerStr,
		&j.RunAt, &j.StartedAt, &j.CompletedAt, &j.HeartbeatAt,
		&j.CreatedAt, &j.UpdatedAt,
		&timeoutNs,
	)
	if err != nil {
		return nil, err
	}

	j.State = job.State(stateStr)
	j.Timeout = time.Duration(timeoutNs)

	parsedID, parseErr := id.ParseJobID(idStr)
	if parseErr != nil {
		return nil, fmt.Errorf("dispatch/postgres: parse job id %q: %w", idStr, parseErr)
	}
	j.ID = parsedID

	if workerStr != "" {
		parsedWorker, workerErr := id.ParseWorkerID(workerStr)
		if workerErr == nil {
			j.WorkerID = parsedWorker
		}
	}

	return &j, nil
}

// collectJobs collects all jobs from query rows.
func collectJobs(rows pgx.Rows) ([]*job.Job, error) {
	var jobs []*job.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("dispatch/postgres: scan job row: %w", err)
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dispatch/postgres: iterate job rows: %w", err)
	}
	return jobs, nil
}
