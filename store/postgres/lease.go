package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// The compile-time check that this store provides the lease capability
// lives in store.go alongside the other interface assertions, matching the
// mongo and sqlite backends.
//
// The grant itself is not here either: it travels on job.DequeueOpts and is
// compiled into DequeueJobs' claim statement by buildLeaseGrant, so a
// leased claim carries the fit predicate and the ordering like any other.
// This file holds only what a lease needs afterwards.

// RenewLease extends the lease only if the caller still holds it.
func (s *Store) RenewLease(
	ctx context.Context,
	jobID id.JobID,
	workerID id.WorkerID,
	epoch int,
	leaseUntil time.Time,
) error {
	res, err := s.pgdb.NewRaw(`
		UPDATE dispatch_jobs
		SET lease_expires_at = $1,
		    heartbeat_at = NOW(),
		    updated_at = NOW()
		WHERE id = $2
		  AND state = 'running'
		  AND worker_id = $3
		  AND lease_epoch = $4`,
		leaseUntil.UTC(), jobID.String(), workerID.String(), epoch,
	).Exec(ctx)
	if err != nil {
		return fmt.Errorf(errPrefix+"renew lease: %w", err)
	}

	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		// Deleted, reclaimed, or reassigned — in every case this worker no
		// longer owns the job and must stop.
		return job.ErrLeaseLost
	}

	return nil
}

// ReclaimExpiredLeases returns expired-lease jobs to pending, fencing
// their previous holders.
//
// The claim and the read are one statement. The old select-then-update
// reaper let two pools both see the same stale job and both reset it.
func (s *Store) ReclaimExpiredLeases(ctx context.Context, limit int) ([]*job.Job, error) {
	// A non-positive limit claims nothing. Postgres would already return
	// nothing for LIMIT 0, but a negative LIMIT is a runtime error rather
	// than an empty result, and neither is worth a round trip.
	if limit <= 0 {
		return nil, nil
	}

	// The first branch is the actual rule: a lease was granted and has
	// lapsed. The second is the narrow exception for a running job carrying
	// no lease at all, gated on silence rather than on the null expiry
	// alone — see job.UnleasedReclaimGrace for why a null expiry does not
	// by itself mean the job was abandoned, and why COALESCE returning NULL
	// (neither timestamp set) must not be adopted.
	//
	// The cutoff is bound rather than computed as NOW() - INTERVAL so that
	// all four backends read the same constant from one place.
	silent := time.Now().UTC().Add(-job.UnleasedReclaimGrace)

	var models []jobModel
	err := s.pgdb.NewRaw(`
		WITH expired AS (
			SELECT id FROM dispatch_jobs
			WHERE state = 'running'
			  AND ( (lease_expires_at IS NOT NULL AND lease_expires_at <= NOW())
			     OR (lease_expires_at IS NULL
			         AND COALESCE(heartbeat_at, started_at) IS NOT NULL
			         AND COALESCE(heartbeat_at, started_at) <= $2) )
			ORDER BY lease_expires_at ASC NULLS FIRST
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE dispatch_jobs
		SET state = 'pending',
		    run_at = NOW(),
		    worker_id = NULL,
		    started_at = NULL,
		    heartbeat_at = NULL,
		    lease_expires_at = NULL,
		    lease_epoch = lease_epoch + 1,
		    evict_count = evict_count + 1,
		    updated_at = NOW()
		WHERE id IN (SELECT id FROM expired)
		RETURNING *`,
		limit, silent,
	).Scan(ctx, &models)
	if err != nil {
		return nil, fmt.Errorf(errPrefix+"reclaim expired leases: %w", err)
	}

	jobs := make([]*job.Job, 0, len(models))
	for i := range models {
		j, convErr := fromJobModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf(errPrefix+"reclaim convert: %w", convErr)
		}
		jobs = append(jobs, j)
	}

	return jobs, nil
}

// updateLeasedJobSQL writes every business column UpdateJob writes,
// fenced on the row being still running, still assigned to the caller's
// workerID, and still at the caller's epoch.
//
// lease_epoch, lease_expires_at, worker_id, and heartbeat_at are
// deliberately absent from the SET list — not merely bound to their old
// values, but never mentioned — because j is the caller's stale
// snapshot. Even behind a passing epoch predicate, writing
// j.LeaseExpiresAt back would roll the real expiry backwards by however
// long the caller ran since it last read the row. Those four columns
// have exactly three writers (the grant in DequeueJobs, RenewLease, and
// ReclaimExpiredLeases); this statement is deliberately not a fourth.
const updateLeasedJobSQL = `
		UPDATE dispatch_jobs
		SET name = $1, queue = $2, payload = $3, state = $4, priority = $5,
		    max_retries = $6, retry_count = $7, last_error = $8,
		    scope_app_id = $9, scope_org_id = $10, run_at = $11,
		    started_at = $12, completed_at = $13, timeout = $14,
		    lease_ttl = $15, evict_count = $16, created_at = $17,
		    updated_at = $18,
		    req_cpu_milli = $19, req_memory_bytes = $20, req_disk_bytes = $21,
		    req_gpu_milli = $22, req_custom_keys = $23,
		    resource_requests = $24, resource_limits = $25,
		    resource_class = $26, input_bytes = $27, primary_input_hash = $28
		WHERE id = $29
		  AND state = 'running'
		  AND worker_id = $30
		  AND lease_epoch = $31`

// UpdateLeasedJob persists j only while the caller still holds the
// lease.
func (s *Store) UpdateLeasedJob(ctx context.Context, j *job.Job, workerID id.WorkerID, epoch int) error {
	m, err := toJobModel(j)
	if err != nil {
		return err
	}

	now := time.Now().UTC()

	res, err := s.pgdb.NewRaw(updateLeasedJobSQL,
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
		return fmt.Errorf(errPrefix+"update leased job: %w", err)
	}

	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows > 0 {
		return nil
	}

	// Zero rows means either the fence predicate failed (the lease moved
	// on) or the row is gone. Only the latter is ErrJobNotFound; the
	// former is ErrLeaseLost, the entire point of this method.
	exists := new(jobModel)
	existErr := s.pgdb.NewSelect(exists).Where("id = ?", m.ID).Limit(1).Scan(ctx)
	if existErr != nil {
		if isNoRows(existErr) {
			return dispatch.ErrJobNotFound
		}
		return fmt.Errorf(errPrefix+"update leased job existence check: %w", existErr)
	}

	return job.ErrLeaseLost
}
