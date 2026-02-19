package bunstore

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/id"
)

// RegisterCron persists a new cron entry. Returns an error if the name
// already exists.
func (s *Store) RegisterCron(ctx context.Context, entry *cron.Entry) error {
	m := toCronModel(entry)
	_, err := s.db.NewInsert().Model(m).Exec(ctx)
	if err != nil {
		if isDuplicateKey(err) {
			return dispatch.ErrDuplicateCron
		}
		return fmt.Errorf("dispatch/bun: register cron: %w", err)
	}
	return nil
}

// GetCron retrieves a cron entry by ID.
func (s *Store) GetCron(ctx context.Context, entryID id.CronID) (*cron.Entry, error) {
	m := new(cronEntryModel)
	err := s.db.NewSelect().Model(m).
		Where("id = ?", entryID.String()).
		Limit(1).
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, dispatch.ErrCronNotFound
		}
		return nil, fmt.Errorf("dispatch/bun: get cron: %w", err)
	}
	return fromCronModel(m)
}

// ListCrons returns all cron entries.
func (s *Store) ListCrons(ctx context.Context) ([]*cron.Entry, error) {
	var models []cronEntryModel
	err := s.db.NewSelect().Model(&models).
		Order("created_at ASC").
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("dispatch/bun: list crons: %w", err)
	}

	entries := make([]*cron.Entry, 0, len(models))
	for i := range models {
		e, convErr := fromCronModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/bun: list crons convert: %w", convErr)
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// AcquireCronLock attempts to acquire a distributed lock for a cron entry.
// Uses row-level locking to prevent race conditions.
func (s *Store) AcquireCronLock(ctx context.Context, entryID id.CronID, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	now := time.Now().UTC()
	until := now.Add(ttl)
	wID := workerID.String()

	// Try to acquire: succeed if no lock, lock expired, or we already hold it.
	res, err := s.db.NewUpdate().
		TableExpr("dispatch_cron_entries").
		Set("locked_by = ?", wID).
		Set("locked_until = ?", until).
		Set("updated_at = NOW()").
		Where("id = ?", entryID.String()).
		Where("(locked_by IS NULL OR locked_until < ? OR locked_by = ?)", now, wID).
		Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("dispatch/bun: acquire cron lock: %w", err)
	}

	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		// Check if the entry exists at all.
		exists, existErr := s.db.NewSelect().
			TableExpr("dispatch_cron_entries").
			Where("id = ?", entryID.String()).
			Exists(ctx)
		if existErr != nil {
			return false, fmt.Errorf("dispatch/bun: check cron exists: %w", existErr)
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
	_, err := s.db.NewUpdate().
		TableExpr("dispatch_cron_entries").
		Set("locked_by = NULL").
		Set("locked_until = NULL").
		Set("updated_at = NOW()").
		Where("id = ?", entryID.String()).
		Where("locked_by = ?", workerID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/bun: release cron lock: %w", err)
	}
	return nil
}

// UpdateCronLastRun records when a cron entry last fired.
func (s *Store) UpdateCronLastRun(ctx context.Context, entryID id.CronID, at time.Time) error {
	res, err := s.db.NewUpdate().
		TableExpr("dispatch_cron_entries").
		Set("last_run_at = ?", at).
		Set("updated_at = NOW()").
		Where("id = ?", entryID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/bun: update cron last run: %w", err)
	}
	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		return dispatch.ErrCronNotFound
	}
	return nil
}

// UpdateCronEntry updates a cron entry (Enabled, NextRunAt, etc.).
func (s *Store) UpdateCronEntry(ctx context.Context, entry *cron.Entry) error {
	m := toCronModel(entry)
	m.UpdatedAt = time.Now().UTC()
	res, err := s.db.NewUpdate().Model(m).WherePK().Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/bun: update cron entry: %w", err)
	}
	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		return dispatch.ErrCronNotFound
	}
	return nil
}

// DeleteCron removes a cron entry by ID.
func (s *Store) DeleteCron(ctx context.Context, entryID id.CronID) error {
	res, err := s.db.NewDelete().
		TableExpr("dispatch_cron_entries").
		Where("id = ?", entryID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/bun: delete cron: %w", err)
	}
	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		return dispatch.ErrCronNotFound
	}
	return nil
}
