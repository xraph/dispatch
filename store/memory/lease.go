package memory

import (
	"context"
	"time"

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
func (m *Store) ReclaimExpiredLeases(_ context.Context, limit int) ([]*job.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().UTC()

	reclaimed := make([]*job.Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		if limit > 0 && len(reclaimed) >= limit {
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
