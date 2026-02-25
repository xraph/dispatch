package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// EnqueueJob persists a new job in pending state.
func (s *Store) EnqueueJob(ctx context.Context, j *job.Job) error {
	m := toJobModel(j)
	_, err := s.mdb.NewInsert(m).Exec(ctx)
	if err != nil {
		if isDuplicateKey(err) {
			return dispatch.ErrJobAlreadyExists
		}
		return fmt.Errorf("dispatch/mongo: enqueue job: %w", err)
	}
	return nil
}

// DequeueJobs atomically claims up to limit pending jobs from the given
// queues. Uses FindOneAndUpdate for atomic claim to prevent double-delivery.
func (s *Store) DequeueJobs(ctx context.Context, queues []string, limit int) ([]*job.Job, error) {
	t := now()
	col := s.mdb.Collection(colJobs)
	jobs := make([]*job.Job, 0, limit)

	for i := 0; i < limit; i++ {
		filter := bson.M{
			"state":  bson.M{"$in": []string{string(job.StatePending), string(job.StateRetrying)}},
			"queue":  bson.M{"$in": queues},
			"run_at": bson.M{"$lte": t},
		}

		update := bson.M{
			"$set": bson.M{
				"state":      string(job.StateRunning),
				"started_at": t,
				"updated_at": t,
			},
		}

		opts := options.FindOneAndUpdate().
			SetReturnDocument(options.After).
			SetSort(bson.D{
				{Key: "priority", Value: -1},
				{Key: "run_at", Value: 1},
			})

		var m jobModel
		err := col.FindOneAndUpdate(ctx, filter, update, opts).Decode(&m)
		if err != nil {
			if isNoDocuments(err) {
				break
			}
			return nil, fmt.Errorf("dispatch/mongo: dequeue jobs: %w", err)
		}

		j, convErr := fromJobModel(&m)
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/mongo: dequeue convert: %w", convErr)
		}
		jobs = append(jobs, j)
	}

	return jobs, nil
}

// GetJob retrieves a job by ID.
func (s *Store) GetJob(ctx context.Context, jobID id.JobID) (*job.Job, error) {
	col := s.mdb.Collection(colJobs)
	var m jobModel
	err := col.FindOne(ctx, bson.M{"_id": jobID.String()}).Decode(&m)
	if err != nil {
		if isNoDocuments(err) {
			return nil, dispatch.ErrJobNotFound
		}
		return nil, fmt.Errorf("dispatch/mongo: get job: %w", err)
	}
	return fromJobModel(&m)
}

// UpdateJob persists changes to an existing job.
func (s *Store) UpdateJob(ctx context.Context, j *job.Job) error {
	m := toJobModel(j)
	m.UpdatedAt = now()
	col := s.mdb.Collection(colJobs)
	res, err := col.ReplaceOne(ctx, bson.M{"_id": m.ID}, m)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: update job: %w", err)
	}
	if res.MatchedCount == 0 {
		return dispatch.ErrJobNotFound
	}
	return nil
}

// DeleteJob removes a job by ID.
func (s *Store) DeleteJob(ctx context.Context, jobID id.JobID) error {
	col := s.mdb.Collection(colJobs)
	res, err := col.DeleteOne(ctx, bson.M{"_id": jobID.String()})
	if err != nil {
		return fmt.Errorf("dispatch/mongo: delete job: %w", err)
	}
	if res.DeletedCount == 0 {
		return dispatch.ErrJobNotFound
	}
	return nil
}

// ListJobsByState returns jobs matching the given state.
func (s *Store) ListJobsByState(ctx context.Context, state job.State, opts job.ListOpts) ([]*job.Job, error) {
	col := s.mdb.Collection(colJobs)
	filter := bson.M{"state": string(state)}

	if opts.Queue != "" {
		filter["queue"] = opts.Queue
	}

	findOpts := options.Find().SetSort(bson.D{{Key: "created_at", Value: 1}})
	if opts.Limit > 0 {
		findOpts.SetLimit(int64(opts.Limit))
	}
	if opts.Offset > 0 {
		findOpts.SetSkip(int64(opts.Offset))
	}

	cursor, err := col.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, fmt.Errorf("dispatch/mongo: list jobs by state: %w", err)
	}
	defer cursor.Close(ctx)

	var models []jobModel
	if err := cursor.All(ctx, &models); err != nil {
		return nil, fmt.Errorf("dispatch/mongo: list jobs decode: %w", err)
	}

	jobs := make([]*job.Job, 0, len(models))
	for i := range models {
		j, convErr := fromJobModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/mongo: list jobs convert: %w", convErr)
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// HeartbeatJob updates the heartbeat timestamp for a running job.
func (s *Store) HeartbeatJob(ctx context.Context, jobID id.JobID, _ id.WorkerID) error {
	t := now()
	col := s.mdb.Collection(colJobs)
	res, err := col.UpdateOne(ctx,
		bson.M{"_id": jobID.String()},
		bson.M{"$set": bson.M{
			"heartbeat_at": t,
			"updated_at":   t,
		}},
	)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: heartbeat job: %w", err)
	}
	if res.MatchedCount == 0 {
		return dispatch.ErrJobNotFound
	}
	return nil
}

// ReapStaleJobs returns running jobs whose last heartbeat is older than
// the given threshold.
func (s *Store) ReapStaleJobs(ctx context.Context, threshold time.Duration) ([]*job.Job, error) {
	cutoff := now().Add(-threshold)
	col := s.mdb.Collection(colJobs)

	filter := bson.M{
		"state":        string(job.StateRunning),
		"heartbeat_at": bson.M{"$ne": nil, "$lt": cutoff},
	}

	cursor, err := col.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("dispatch/mongo: reap stale jobs: %w", err)
	}
	defer cursor.Close(ctx)

	var models []jobModel
	if err := cursor.All(ctx, &models); err != nil {
		return nil, fmt.Errorf("dispatch/mongo: reap stale decode: %w", err)
	}

	jobs := make([]*job.Job, 0, len(models))
	for i := range models {
		j, convErr := fromJobModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/mongo: reap stale convert: %w", convErr)
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// CountJobs returns the number of jobs matching the given options.
func (s *Store) CountJobs(ctx context.Context, opts job.CountOpts) (int64, error) {
	col := s.mdb.Collection(colJobs)
	filter := bson.M{}

	if opts.Queue != "" {
		filter["queue"] = opts.Queue
	}
	if opts.State != "" {
		filter["state"] = string(opts.State)
	}

	count, err := col.CountDocuments(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("dispatch/mongo: count jobs: %w", err)
	}
	return count, nil
}
