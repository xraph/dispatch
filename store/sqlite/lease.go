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
	// A non-positive limit claims nothing. This early return is
	// load-bearing on SQLite rather than a saved round trip: `LIMIT -1`
	// means UNLIMITED here, the exact opposite of Postgres, where it is a
	// runtime error. Without this, a negative limit would reclaim the
	// entire table.
	if limit <= 0 {
		return nil, nil
	}

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

// updateLeasedJobSQL writes every business column UpdateJob writes,
// fenced on the row being still running, still assigned to the caller's
// workerID, and still at the caller's epoch. Same shape as Postgres'
// equivalent statement, character for character in intent.
//
// lease_epoch, lease_expires_at, worker_id, and heartbeat_at are
// deliberately absent from the SET list. j is the caller's stale
// snapshot, and writing j.LeaseExpiresAt back would roll the real expiry
// backwards even behind a passing epoch predicate. Those four columns
// have exactly three writers (the grant in DequeueJobs, RenewLease, and
// ReclaimExpiredLeases); this statement is deliberately not a fourth.
const updateLeasedJobSQL = `
		UPDATE dispatch_jobs
		SET name = ?, queue = ?, payload = ?, state = ?, priority = ?,
		    max_retries = ?, retry_count = ?, last_error = ?,
		    scope_app_id = ?, scope_org_id = ?, run_at = ?,
		    started_at = ?, completed_at = ?, timeout = ?,
		    lease_ttl = ?, evict_count = ?, created_at = ?,
		    updated_at = ?,
		    req_cpu_milli = ?, req_memory_bytes = ?, req_disk_bytes = ?,
		    req_gpu_milli = ?, req_custom_keys = ?,
		    resource_requests = ?, resource_limits = ?,
		    resource_class = ?, input_bytes = ?, primary_input_hash = ?
		WHERE id = ?
		  AND state = 'running'
		  AND worker_id = ?
		  AND lease_epoch = ?`

// UpdateLeasedJob persists j only while the caller still holds the
// lease.
func (s *Store) UpdateLeasedJob(ctx context.Context, j *job.Job, workerID id.WorkerID, epoch int) error {
	m, err := toJobModel(j)
	if err != nil {
		return err
	}

	now := time.Now().UTC()

	var rows int64
	execErr := withBusyRetry(ctx, func() error {
		res, err := s.sdb.NewRaw(updateLeasedJobSQL,
			m.Name, m.Queue, m.Payload, m.State, m.Priority,
			m.MaxRetries, m.RetryCount, m.LastError,
			m.ScopeAppID, m.ScopeOrgID, m.RunAt,
			m.StartedAt, m.CompletedAt, m.Timeout,
			m.LeaseTTL, m.EvictCount, m.CreatedAt,
			now,
			m.ReqCPUMilli, m.ReqMemoryBytes, m.ReqDiskBytes,
			m.ReqGPUMilli, m.ReqCustomKeys,
			m.ResourceRequests, m.ResourceLimits,
			m.ResourceClass, m.InputBytes, m.PrimaryInputHash,
			m.ID, workerID.String(), epoch,
		).Exec(ctx)
		if err != nil {
			return err
		}
		rows, _ = res.RowsAffected() //nolint:errcheck // driver always returns nil
		return nil
	})
	if execErr != nil {
		return fmt.Errorf("dispatch/sqlite: update leased job: %w", execErr)
	}

	if rows > 0 {
		return nil
	}

	// Zero rows means either the fence predicate failed (the lease moved
	// on) or the row is gone. Only the latter is ErrJobNotFound; the
	// former is ErrLeaseLost, the entire point of this method.
	exists := new(jobModel)
	existErr := s.sdb.NewSelect(exists).Where("id = ?", m.ID).Limit(1).Scan(ctx)
	if existErr != nil {
		if isNoRows(existErr) {
			return dispatch.ErrJobNotFound
		}
		return fmt.Errorf("dispatch/sqlite: update leased job existence check: %w", existErr)
	}

	return job.ErrLeaseLost
}
