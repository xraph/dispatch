package sqlite

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// maxLeaseBusyRetries bounds how many times a lease write retries after
// SQLITE_BUSY before giving up.
//
// Grove's sqlitedriver configures WAL mode but no busy_timeout, and does
// not expose the underlying *sql.DB for this package to configure one
// itself. Two connections attempting a write at the same instant therefore
// have the loser fail immediately with SQLITE_BUSY instead of blocking.
// SQLite still serializes writes at the engine level — retrying closes the
// gap between "not grantable this instant" and "will succeed shortly" that
// ReclaimExpiredLeases's atomicity guarantee depends on under concurrency.
const maxLeaseBusyRetries = 100

// leaseBusyRetryDelay is the pause between retries. It is small because a
// write against this schema completes in well under a millisecond; the
// retry exists to ride out a burst of contention, not to wait out
// something the caller should instead be timed out for.
const leaseBusyRetryDelay = time.Millisecond

// isSQLiteBusy reports whether err is the driver's SQLITE_BUSY, meaning
// another connection currently holds SQLite's single write lock. Matched
// on the error message the same way isDuplicateKey matches its error.
func isSQLiteBusy(err error) bool {
	return err != nil && strings.Contains(err.Error(), "SQLITE_BUSY")
}

// withBusyRetry runs fn, retrying while it returns SQLITE_BUSY, up to
// maxLeaseBusyRetries times or until ctx is done.
func withBusyRetry(ctx context.Context, fn func() error) error {
	var err error
	for range maxLeaseBusyRetries {
		err = fn()
		if err == nil || !isSQLiteBusy(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(leaseBusyRetryDelay):
		}
	}

	return err
}

// The compile-time check that this store provides the lease capability
// lives in store.go alongside the other interface assertions.
//
// The grant itself is not in this file: it travels on job.DequeueOpts and
// is compiled into DequeueJobs' claim statement by buildLeaseGrant, so a
// leased claim carries the fit predicate and the ordering like any other.
// This file holds only what a lease needs afterwards, plus the SQLITE_BUSY
// retry those writes and the claim share.

// RenewLease extends the lease only if the caller still holds it.
func (s *Store) RenewLease(
	ctx context.Context,
	jobID id.JobID,
	workerID id.WorkerID,
	epoch int,
	leaseUntil time.Time,
) error {
	now := time.Now().UTC()

	var rows int64
	err := withBusyRetry(ctx, func() error {
		res, execErr := s.sdb.NewUpdate((*jobModel)(nil)).
			Set("lease_expires_at = ?", leaseUntil.UTC()).
			Set("heartbeat_at = ?", now).
			Set("updated_at = ?", now).
			Where("id = ?", jobID.String()).
			Where("state = 'running'").
			Where("worker_id = ?", workerID.String()).
			Where("lease_epoch = ?", epoch).
			Exec(ctx)
		if execErr != nil {
			return execErr
		}
		rows, _ = res.RowsAffected() //nolint:errcheck // driver always returns nil
		return nil
	})
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: renew lease: %w", err)
	}

	if rows == 0 {
		// Deleted, reclaimed, or reassigned — in every case this worker no
		// longer owns the job and must stop.
		return job.ErrLeaseLost
	}

	return nil
}

// ReclaimExpiredLeases returns expired-lease jobs to pending, fencing
// their previous holders.
func (s *Store) ReclaimExpiredLeases(ctx context.Context, limit int) ([]*job.Job, error) {
	now := time.Now().UTC()

	var models []jobModel
	err := withBusyRetry(ctx, func() error {
		models = nil
		return s.sdb.NewRaw(`
			UPDATE dispatch_jobs
			SET state = 'pending',
			    run_at = ?,
			    worker_id = NULL,
			    started_at = NULL,
			    heartbeat_at = NULL,
			    lease_expires_at = NULL,
			    lease_epoch = lease_epoch + 1,
			    evict_count = evict_count + 1,
			    updated_at = ?
			WHERE id IN (
				SELECT id FROM dispatch_jobs
				WHERE state = 'running'
				  AND lease_expires_at IS NOT NULL
				  AND lease_expires_at <= ?
				ORDER BY lease_expires_at ASC
				LIMIT ?
			)
			RETURNING *`,
			now, now, now, limit,
		).Scan(ctx, &models)
	})
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: reclaim expired leases: %w", err)
	}

	jobs := make([]*job.Job, 0, len(models))
	for i := range models {
		j, convErr := fromJobModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/sqlite: reclaim convert: %w", convErr)
		}
		jobs = append(jobs, j)
	}

	return jobs, nil
}
