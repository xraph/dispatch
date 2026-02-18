package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/id"
)

// RegisterCron persists a new cron entry. Returns an error if the name
// already exists.
func (s *Store) RegisterCron(ctx context.Context, entry *cron.Entry) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO dispatch_cron_entries (
			id, name, schedule, job_name, queue, payload,
			scope_app_id, scope_org_id,
			last_run_at, next_run_at, locked_by, locked_until,
			enabled, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
		entry.ID.String(), entry.Name, entry.Schedule, entry.JobName, entry.Queue, entry.Payload,
		entry.ScopeAppID, entry.ScopeOrgID,
		entry.LastRunAt, entry.NextRunAt, nilIfEmpty(entry.LockedBy), entry.LockedUntil,
		entry.Enabled, entry.CreatedAt, entry.UpdatedAt,
	)
	if err != nil {
		if isDuplicateKey(err) {
			return dispatch.ErrDuplicateCron
		}
		return fmt.Errorf("dispatch/postgres: register cron: %w", err)
	}
	return nil
}

// GetCron retrieves a cron entry by ID.
func (s *Store) GetCron(ctx context.Context, entryID id.CronID) (*cron.Entry, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT
			id, name, schedule, job_name, queue, payload,
			scope_app_id, scope_org_id,
			last_run_at, next_run_at, locked_by, locked_until,
			enabled, created_at, updated_at
		FROM dispatch_cron_entries
		WHERE id = $1`,
		entryID.String(),
	)

	e, err := scanCron(row)
	if err != nil {
		if isNoRows(err) {
			return nil, dispatch.ErrCronNotFound
		}
		return nil, fmt.Errorf("dispatch/postgres: get cron: %w", err)
	}
	return e, nil
}

// ListCrons returns all cron entries.
func (s *Store) ListCrons(ctx context.Context) ([]*cron.Entry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			id, name, schedule, job_name, queue, payload,
			scope_app_id, scope_org_id,
			last_run_at, next_run_at, locked_by, locked_until,
			enabled, created_at, updated_at
		FROM dispatch_cron_entries
		ORDER BY created_at ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("dispatch/postgres: list crons: %w", err)
	}
	defer rows.Close()

	var entries []*cron.Entry
	for rows.Next() {
		e, scanErr := scanCron(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("dispatch/postgres: scan cron row: %w", scanErr)
		}
		entries = append(entries, e)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("dispatch/postgres: iterate cron rows: %w", err)
	}
	return entries, nil
}

// AcquireCronLock attempts to acquire a distributed lock for a cron entry.
// Uses row-level locking with FOR UPDATE to prevent race conditions.
func (s *Store) AcquireCronLock(ctx context.Context, entryID id.CronID, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	now := time.Now().UTC()
	until := now.Add(ttl)
	wID := workerID.String()

	// Try to acquire: succeed if no lock, lock expired, or we already hold it.
	tag, err := s.pool.Exec(ctx, `
		UPDATE dispatch_cron_entries
		SET locked_by = $2, locked_until = $3, updated_at = NOW()
		WHERE id = $1
		  AND (locked_by IS NULL OR locked_until < $4 OR locked_by = $2)`,
		entryID.String(), wID, until, now,
	)
	if err != nil {
		return false, fmt.Errorf("dispatch/postgres: acquire cron lock: %w", err)
	}

	if tag.RowsAffected() == 0 {
		// Check if the entry exists at all.
		var exists bool
		existErr := s.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM dispatch_cron_entries WHERE id = $1)`,
			entryID.String(),
		).Scan(&exists)
		if existErr != nil {
			return false, fmt.Errorf("dispatch/postgres: check cron exists: %w", existErr)
		}
		if !exists {
			return false, dispatch.ErrCronNotFound
		}
		// Entry exists but lock is held by someone else.
		return false, nil
	}

	return true, nil
}

// ReleaseCronLock releases the distributed lock for a cron entry.
func (s *Store) ReleaseCronLock(ctx context.Context, entryID id.CronID, workerID id.WorkerID) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE dispatch_cron_entries
		SET locked_by = NULL, locked_until = NULL, updated_at = NOW()
		WHERE id = $1 AND locked_by = $2`,
		entryID.String(), workerID.String(),
	)
	if err != nil {
		return fmt.Errorf("dispatch/postgres: release cron lock: %w", err)
	}
	return nil
}

// UpdateCronLastRun records when a cron entry last fired.
func (s *Store) UpdateCronLastRun(ctx context.Context, entryID id.CronID, at time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE dispatch_cron_entries
		SET last_run_at = $2, updated_at = NOW()
		WHERE id = $1`,
		entryID.String(), at,
	)
	if err != nil {
		return fmt.Errorf("dispatch/postgres: update cron last run: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return dispatch.ErrCronNotFound
	}
	return nil
}

// UpdateCronEntry updates a cron entry (Enabled, NextRunAt, etc.).
func (s *Store) UpdateCronEntry(ctx context.Context, entry *cron.Entry) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE dispatch_cron_entries SET
			name = $2, schedule = $3, job_name = $4, queue = $5, payload = $6,
			scope_app_id = $7, scope_org_id = $8,
			last_run_at = $9, next_run_at = $10,
			locked_by = $11, locked_until = $12,
			enabled = $13, updated_at = NOW()
		WHERE id = $1`,
		entry.ID.String(), entry.Name, entry.Schedule, entry.JobName, entry.Queue, entry.Payload,
		entry.ScopeAppID, entry.ScopeOrgID,
		entry.LastRunAt, entry.NextRunAt,
		nilIfEmpty(entry.LockedBy), entry.LockedUntil,
		entry.Enabled,
	)
	if err != nil {
		return fmt.Errorf("dispatch/postgres: update cron entry: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return dispatch.ErrCronNotFound
	}
	return nil
}

// DeleteCron removes a cron entry by ID.
func (s *Store) DeleteCron(ctx context.Context, entryID id.CronID) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM dispatch_cron_entries WHERE id = $1`, entryID.String())
	if err != nil {
		return fmt.Errorf("dispatch/postgres: delete cron: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return dispatch.ErrCronNotFound
	}
	return nil
}

// scanCron scans a single cron entry row.
func scanCron(row pgx.Row) (*cron.Entry, error) {
	var (
		e      cron.Entry
		idStr  string
		lockBy *string
	)
	err := row.Scan(
		&idStr, &e.Name, &e.Schedule, &e.JobName, &e.Queue, &e.Payload,
		&e.ScopeAppID, &e.ScopeOrgID,
		&e.LastRunAt, &e.NextRunAt, &lockBy, &e.LockedUntil,
		&e.Enabled, &e.CreatedAt, &e.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	parsedID, parseErr := id.ParseCronID(idStr)
	if parseErr != nil {
		return nil, fmt.Errorf("dispatch/postgres: parse cron id %q: %w", idStr, parseErr)
	}
	e.ID = parsedID

	if lockBy != nil {
		e.LockedBy = *lockBy
	}

	return &e, nil
}
