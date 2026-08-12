package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// DequeueLeased claims up to limit ready jobs and grants each a lease.
//
// Mongo cannot update-and-return many documents atomically, so this loops
// FindOneAndUpdate exactly as DequeueJobs does. Each iteration is its own
// atomic claim, which is what keeps two workers from taking one job.
func (s *Store) DequeueLeased(
	ctx context.Context,
	queues []string,
	limit int,
	workerID id.WorkerID,
	leaseUntil time.Time,
) ([]*job.Job, error) {
	t := now()
	jobs := make([]*job.Job, 0, limit)

	for len(jobs) < limit {
		j, err := s.dequeueOneLeased(ctx, queues, t, workerID, leaseUntil.UTC())
		if err != nil {
			return nil, err
		}
		if j == nil {
			break // nothing ready
		}
		jobs = append(jobs, j)
	}

	return jobs, nil
}

// dequeueOneLeased claims a single ready job and grants it a lease.
func (s *Store) dequeueOneLeased(
	ctx context.Context,
	queues []string,
	t time.Time,
	workerID id.WorkerID,
	leaseUntil time.Time,
) (*job.Job, error) {
	col := s.mdb.Collection(colJobs)
	filter := bson.M{
		"state":  bson.M{"$in": []string{string(job.StatePending), string(job.StateRetrying)}},
		"queue":  bson.M{"$in": queues},
		"run_at": bson.M{"$lte": t},
	}
	update := bson.M{
		"$set": bson.M{
			"state":            string(job.StateRunning),
			"started_at":       t,
			"updated_at":       t,
			"worker_id":        workerID.String(),
			"lease_expires_at": leaseUntil,
		},
		"$inc": bson.M{"lease_epoch": 1},
	}
	opts := options.FindOneAndUpdate().
		SetReturnDocument(options.After).
		SetSort(bson.D{
			{Key: "priority", Value: -1},
			{Key: "run_at", Value: 1},
		})

	var m jobModel
	err := withRetry(ctx, defaultRetry, func(ctx context.Context) error {
		return col.FindOneAndUpdate(ctx, filter, update, opts).Decode(&m)
	})
	if err != nil {
		if isNoDocuments(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("dispatch/mongo: dequeue leased: %w", err)
	}

	j, convErr := fromJobModel(&m)
	if convErr != nil {
		return nil, fmt.Errorf("dispatch/mongo: dequeue leased convert: %w", convErr)
	}

	return j, nil
}

// RenewLease extends the lease only if the caller still holds it.
func (s *Store) RenewLease(
	ctx context.Context,
	jobID id.JobID,
	workerID id.WorkerID,
	epoch int,
	leaseUntil time.Time,
) error {
	t := now()
	col := s.mdb.Collection(colJobs)

	filter := bson.M{
		"_id":         jobID.String(),
		"state":       string(job.StateRunning),
		"worker_id":   workerID.String(),
		"lease_epoch": epoch,
	}
	update := bson.M{"$set": bson.M{
		"lease_expires_at": leaseUntil.UTC(),
		"heartbeat_at":     t,
		"updated_at":       t,
	}}

	var matched int64
	err := withRetry(ctx, defaultRetry, func(ctx context.Context) error {
		r, updErr := col.UpdateOne(ctx, filter, update)
		if updErr != nil {
			return updErr
		}
		matched = r.MatchedCount

		return nil
	})
	if err != nil {
		return fmt.Errorf("dispatch/mongo: renew lease: %w", err)
	}
	if matched == 0 {
		return job.ErrLeaseLost
	}

	return nil
}

// ReclaimExpiredLeases returns expired-lease jobs to pending, fencing
// their previous holders.
//
// Each job is claimed by its own conditional FindOneAndUpdate keyed on the
// epoch it was seen at, so two pools reclaiming concurrently cannot both
// take the same job — the loser's filter no longer matches.
func (s *Store) ReclaimExpiredLeases(ctx context.Context, limit int) ([]*job.Job, error) {
	t := now()
	col := s.mdb.Collection(colJobs)

	filter := bson.M{
		"state":            string(job.StateRunning),
		"lease_expires_at": bson.M{"$ne": nil, "$lte": t},
	}
	update := bson.M{
		"$set": bson.M{
			"state":            string(job.StatePending),
			"run_at":           t,
			"worker_id":        "",
			"started_at":       nil,
			"heartbeat_at":     nil,
			"lease_expires_at": nil,
			"updated_at":       t,
		},
		"$inc": bson.M{
			"lease_epoch": 1,
			"evict_count": 1,
		},
	}
	opts := options.FindOneAndUpdate().
		SetReturnDocument(options.After).
		SetSort(bson.D{{Key: "lease_expires_at", Value: 1}})

	jobs := make([]*job.Job, 0, limit)
	for len(jobs) < limit {
		var m jobModel
		err := withRetry(ctx, defaultRetry, func(ctx context.Context) error {
			return col.FindOneAndUpdate(ctx, filter, update, opts).Decode(&m)
		})
		if err != nil {
			if isNoDocuments(err) {
				break
			}

			return nil, fmt.Errorf("dispatch/mongo: reclaim expired leases: %w", err)
		}

		j, convErr := fromJobModel(&m)
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/mongo: reclaim convert: %w", convErr)
		}
		jobs = append(jobs, j)
	}

	return jobs, nil
}
