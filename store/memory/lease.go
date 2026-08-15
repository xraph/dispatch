package memory

import (
	"context"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// Compile-time check that the memory store provides the lease capability.
//
// The grant itself is not here: it travels on job.DequeueOpts and is
// applied by DequeueJobs, under the same write lock that performs the
// claim. This file holds only what a lease needs afterwards.
var _ job.LeaseStore = (*Store)(nil)

// RenewLease extends the lease only if the caller still holds it.
func (m *Store) RenewLease(
	_ context.Context,
	jobID id.JobID,
	workerID id.WorkerID,
	epoch int,
	leaseUntil time.Time,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	j, ok := m.jobs[jobID.String()]
	if !ok {
		return job.ErrLeaseLost
	}
	if j.State != job.StateRunning || j.WorkerID != workerID || j.LeaseEpoch != epoch {
		return job.ErrLeaseLost
	}

	now := time.Now().UTC()
	until := leaseUntil
	beat := now

	j.LeaseExpiresAt = &until
	j.HeartbeatAt = &beat
	j.UpdatedAt = now

	return nil
}

// ReclaimExpiredLeases returns expired-lease jobs to pending, fencing
// their previous holders.
//
// A non-positive limit claims nothing, matching DequeueOpts.Limit's
// behavior rather than reading zero or negative as unlimited.
func (m *Store) ReclaimExpiredLeases(_ context.Context, limit int) ([]*job.Job, error) {
	if limit <= 0 {
		return nil, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().UTC()

	reclaimed := make([]*job.Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		if len(reclaimed) >= limit {
			break
		}
		if j.State != job.StateRunning {
			continue
		}
		lease := job.Lease{Epoch: j.LeaseEpoch}
		if j.LeaseExpiresAt != nil {
			lease.ExpiresAt = *j.LeaseExpiresAt
		}
		if !lease.IsExpired(now) {
			continue
		}

		j.State = job.StatePending
		j.RunAt = now
		j.WorkerID = id.WorkerID{}
		j.StartedAt = nil
		j.HeartbeatAt = nil
		j.LeaseExpiresAt = nil
		j.LeaseEpoch++
		j.EvictCount++
		j.UpdatedAt = now

		reclaimed = append(reclaimed, cloneJob(j))
	}

	return reclaimed, nil
}

// UpdateLeasedJob persists j only while the caller still holds the
// lease — still running, still assigned to workerID, still at epoch.
// Otherwise it returns job.ErrLeaseLost and leaves the stored job
// untouched.
//
// Only the business fields move. lease_epoch, lease_expires_at,
// worker_id, and heartbeat_at are copied from the CURRENTLY STORED job,
// never from j: j is the caller's stale snapshot, and every renewal
// since it was taken has pushed the real expiry forward. Overwriting
// that with j's copy would roll the winner's lease backwards even
// though the epoch check passed — see job.LeaseStore.UpdateLeasedJob.
func (m *Store) UpdateLeasedJob(_ context.Context, j *job.Job, workerID id.WorkerID, epoch int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cur, ok := m.jobs[j.ID.String()]
	if !ok {
		return dispatch.ErrJobNotFound
	}
	if cur.State != job.StateRunning || cur.WorkerID != workerID || cur.LeaseEpoch != epoch {
		return job.ErrLeaseLost
	}

	leaseEpoch := cur.LeaseEpoch
	leaseExpiresAt := cur.LeaseExpiresAt
	leaseWorkerID := cur.WorkerID
	heartbeatAt := cur.HeartbeatAt

	cp := cloneJob(j)
	cp.LeaseEpoch = leaseEpoch
	cp.LeaseExpiresAt = leaseExpiresAt
	cp.WorkerID = leaseWorkerID
	cp.HeartbeatAt = heartbeatAt
	cp.UpdatedAt = time.Now().UTC()

	m.jobs[j.ID.String()] = cp

	return nil
}
