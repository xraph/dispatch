package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
)

// PushDLQ adds a failed job entry to the dead letter queue.
func (s *Store) PushDLQ(ctx context.Context, entry *dlq.Entry) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO dispatch_dlq (
			id, job_id, job_name, queue, payload, error,
			retry_count, max_retries, scope_app_id, scope_org_id,
			failed_at, replayed_at, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		entry.ID.String(), entry.JobID.String(), entry.JobName,
		entry.Queue, entry.Payload, entry.Error,
		entry.RetryCount, entry.MaxRetries, entry.ScopeAppID, entry.ScopeOrgID,
		entry.FailedAt, entry.ReplayedAt, entry.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("dispatch/postgres: push dlq: %w", err)
	}
	return nil
}

// ListDLQ returns DLQ entries matching the given options.
func (s *Store) ListDLQ(ctx context.Context, opts dlq.ListOpts) ([]*dlq.Entry, error) {
	query := `
		SELECT
			id, job_id, job_name, queue, payload, error,
			retry_count, max_retries, scope_app_id, scope_org_id,
			failed_at, replayed_at, created_at
		FROM dispatch_dlq
		WHERE 1=1`
	args := []interface{}{}
	argIdx := 1

	if opts.Queue != "" {
		query += fmt.Sprintf(" AND queue = $%d", argIdx)
		args = append(args, opts.Queue)
		argIdx++
	}

	query += " ORDER BY failed_at ASC"

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
		return nil, fmt.Errorf("dispatch/postgres: list dlq: %w", err)
	}
	defer rows.Close()

	var entries []*dlq.Entry
	for rows.Next() {
		e, scanErr := scanDLQ(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("dispatch/postgres: scan dlq row: %w", scanErr)
		}
		entries = append(entries, e)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("dispatch/postgres: iterate dlq rows: %w", err)
	}
	return entries, nil
}

// GetDLQ retrieves a DLQ entry by ID.
func (s *Store) GetDLQ(ctx context.Context, entryID id.DLQID) (*dlq.Entry, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT
			id, job_id, job_name, queue, payload, error,
			retry_count, max_retries, scope_app_id, scope_org_id,
			failed_at, replayed_at, created_at
		FROM dispatch_dlq
		WHERE id = $1`,
		entryID.String(),
	)

	e, err := scanDLQ(row)
	if err != nil {
		if isNoRows(err) {
			return nil, dispatch.ErrDLQNotFound
		}
		return nil, fmt.Errorf("dispatch/postgres: get dlq: %w", err)
	}
	return e, nil
}

// ReplayDLQ marks a DLQ entry as replayed.
func (s *Store) ReplayDLQ(ctx context.Context, entryID id.DLQID) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE dispatch_dlq SET replayed_at = NOW() WHERE id = $1`,
		entryID.String(),
	)
	if err != nil {
		return fmt.Errorf("dispatch/postgres: replay dlq: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return dispatch.ErrDLQNotFound
	}
	return nil
}

// PurgeDLQ removes DLQ entries with FailedAt before the given time.
// Returns the number of entries removed.
func (s *Store) PurgeDLQ(ctx context.Context, before time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM dispatch_dlq WHERE failed_at < $1`,
		before,
	)
	if err != nil {
		return 0, fmt.Errorf("dispatch/postgres: purge dlq: %w", err)
	}
	return tag.RowsAffected(), nil
}

// CountDLQ returns the total number of entries in the dead letter queue.
func (s *Store) CountDLQ(ctx context.Context) (int64, error) {
	var count int64
	err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM dispatch_dlq`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("dispatch/postgres: count dlq: %w", err)
	}
	return count, nil
}

// scanDLQ scans a single DLQ entry row.
func scanDLQ(row pgx.Row) (*dlq.Entry, error) {
	var (
		e        dlq.Entry
		idStr    string
		jobIDStr string
	)
	err := row.Scan(
		&idStr, &jobIDStr, &e.JobName, &e.Queue, &e.Payload, &e.Error,
		&e.RetryCount, &e.MaxRetries, &e.ScopeAppID, &e.ScopeOrgID,
		&e.FailedAt, &e.ReplayedAt, &e.CreatedAt,
	)
	if err != nil {
		return nil, err
	}

	parsedID, parseErr := id.ParseDLQID(idStr)
	if parseErr != nil {
		return nil, fmt.Errorf("dispatch/postgres: parse dlq id %q: %w", idStr, parseErr)
	}
	e.ID = parsedID

	parsedJobID, jobParseErr := id.ParseJobID(jobIDStr)
	if jobParseErr != nil {
		return nil, fmt.Errorf("dispatch/postgres: parse job id %q: %w", jobIDStr, jobParseErr)
	}
	e.JobID = parsedJobID

	return &e, nil
}
