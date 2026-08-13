package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// Compile-time check that the postgres store provides the lease capability.
//
// The grant itself is not here: it travels on job.DequeueOpts and is
// compiled into DequeueJobs' claim statement by buildLeaseGrant, so a
// leased claim carries the fit predicate and the ordering like any other.
// This file holds only what a lease needs afterwards.
var _ job.LeaseStore = (*Store)(nil)

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
	var models []jobModel
	err := s.pgdb.NewRaw(`
		WITH expired AS (
			SELECT id FROM dispatch_jobs
			WHERE state = 'running'
			  AND lease_expires_at IS NOT NULL
			  AND lease_expires_at <= NOW()
			ORDER BY lease_expires_at ASC
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
		limit,
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
