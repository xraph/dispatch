package sqlite

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// Retry issuance is bounded independently of the driver's busy_timeout. A
// synchronous call can outlast this window; its accepted result stays truthful.
const sqliteBusyRetryWindow = 5 * time.Second
const leaseBusyRetryDelay = time.Millisecond
const maxBusyBackoffMultiplier = 32

// busyRetryDelay spreads writers across half to one and a half milliseconds.
// Capped exponential scaling below keeps the largest pause below 48ms.
func busyRetryDelay() time.Duration {
	half := leaseBusyRetryDelay / 2
	// #nosec G404 -- retry jitter spreads contention; it is not a security token.
	return half + time.Duration(rand.Float64()*float64(leaseBusyRetryDelay))
}

// isSQLiteBusy reports whether err is the driver's SQLITE_BUSY, meaning
// another connection currently holds SQLite's single write lock. Matched
// on the error message the same way isDuplicateKey matches its error.
func isSQLiteBusy(err error) bool {
	return err != nil && strings.Contains(err.Error(), "SQLITE_BUSY")
}

// withBusyRetry retries SQLITE_BUSY for up to five seconds. Earlier caller
// cancellation prevents further calls. The last busy error survives internal
// budget exhaustion; a successful synchronous call is never changed to timeout.
func withBusyRetry(ctx context.Context, fn func() error) error {
	deadline := time.Now().Add(sqliteBusyRetryWindow)
	multiplier := time.Duration(1)
	var lastBusy error
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if lastBusy != nil && !time.Now().Before(deadline) {
			return lastBusy
		}
		err := fn()
		if err == nil || !isSQLiteBusy(err) {
			return err
		}
		lastBusy = err
		if err := ctx.Err(); err != nil {
			return err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return lastBusy
		}
		delay := min(busyRetryDelay()*multiplier, remaining)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		multiplier = min(multiplier*2, maxBusyBackoffMultiplier)
	}
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

	// The reclaim predicate below adds a branch for a running job carrying
	// no lease at all, gated on silence rather than on the null expiry
	// alone — see job.UnleasedReclaimGrace for why a null expiry does not
	// by itself mean the job was abandoned, and why COALESCE returning NULL
	// (neither timestamp set) must not be adopted.
	//
	// silent is a bound time.Time and must stay one. SQLite has no
	// timestamp type, so every comparison here is a string comparison
	// against whatever grove's sqlitedriver wrote, and the driver renders a
	// time.Time with Go's default layout rather than ISO-8601. Formatting
	// this value instead would sort it above every driver-written timestamp
	// ('T' > ' '), which is the bug the migration 008 backfill shipped with
	// before it was removed.
	//
	// Here it is worse than it was there, and in a way that inverts. The
	// backfill compared in the direction that made a formatted value match
	// NOTHING, so the failure was jobs staying stranded: bad, but the same
	// outcome as having no backfill. This predicate compares the other way,
	// so a formatted value is greater than every stored timestamp and
	// matches EVERYTHING, reclaiming healthy running jobs out from under
	// live workers. The same mistake fails open here rather than closed.
	silent := now.Add(-job.UnleasedReclaimGrace)

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
				  AND ( (lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
				     OR (lease_expires_at IS NULL
				         AND COALESCE(heartbeat_at, started_at) IS NOT NULL
				         AND COALESCE(heartbeat_at, started_at) <= ?) )
				ORDER BY lease_expires_at ASC
				LIMIT ?
			)
			RETURNING *`,
			now, now, now, silent, limit,
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
