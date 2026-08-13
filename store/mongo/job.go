package mongo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	mongod "go.mongodb.org/mongo-driver/v2/mongo"
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

// maxDequeueRounds bounds the read-then-claim retry below.
//
// A round claims nothing only when every candidate it read was taken by a
// competing claimer in between. Returning an empty batch then would be a
// lie a drain loop believes — storetest's concurrency case has each
// claimer stop on the first empty result — so a contended round is
// retried rather than reported. The bound keeps a pathological loser
// terminating instead of spinning.
const maxDequeueRounds = 8

// DequeueJobs atomically claims up to opts.Limit ready jobs from
// opts.Queues that fit opts, sets them to running, and returns them
// ordered by priority descending, then locality-preferred first, then
// RunAt ascending.
//
// Mongo has no statement that can order, limit, and claim many documents
// in one shot, so the claim is composed of two parts:
//
//   - one ordered candidate read, which applies the fit predicate, the
//     full contract ordering, and the limit — order THEN truncate,
//     server-side, over the whole eligible set;
//   - one FindOneAndUpdate per candidate, keyed by _id and carrying the
//     SAME fit predicate plus the state and run_at guards. findAndModify
//     is atomic per document, so two workers racing for one job produce
//     exactly one winner; the loser's call matches nothing and it simply
//     gets no job, never a second claim of the same one.
//
// The read is also the write gate the previous probe provided: when it
// finds no candidate, not a single write command is sent. A job that does
// not fit is never written to — the predicate is a conjunct of the
// claiming update itself, not a filter over claimed documents.
//
// When opts.Grants() the lease fields are part of that same per-document
// update, never a follow-up write. Per-document atomicity is what makes
// the claim exclusive, and it is also what keeps the grant recoverable:
// ReclaimExpiredLeases matches lease_expires_at {$ne: nil}, so a document
// left running with no expiry is not one at risk of reclamation — it is
// one reclamation can never see. See job.DequeueOpts.LeaseUntil.
func (s *Store) DequeueJobs(ctx context.Context, opts job.DequeueOpts) ([]*job.Job, error) {
	// A worker computing zero free slots must claim zero jobs, never the
	// whole queue. Matches the SQL backends' LIMIT 0.
	if opts.Limit <= 0 {
		return nil, nil
	}

	// Ahead of the empty-Queues guard, so an incoherent grant is reported
	// rather than swallowed by a return that happens to be silent here.
	// A caller that names no queues still deserves to hear that its lease
	// has no holder, and the five backends must agree on which inputs are
	// errors — see job.DequeueOpts.Validate.
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("dispatch/mongo: dequeue jobs: %w", err)
	}

	// An empty queue list is a guard, not a query. The driver marshals a
	// nil []string to BSON null, so {queue: {$in: null}} reaches the
	// server and is rejected outright — "$in needs an array" — which
	// surfaces as a dequeue ERROR every poll, backing the pool off on a
	// configuration that merely names no queues. Postgres, SQLite and
	// Redis all claim nothing for the same input; this matches them
	// rather than inventing an all-queues scan no other persistent
	// backend performs. See job.DequeueOpts.Queues.
	if len(opts.Queues) == 0 {
		return nil, nil
	}

	for range maxDequeueRounds {
		t := now()

		ids, err := s.dequeueCandidates(ctx, opts, t)
		if err != nil {
			return nil, err
		}

		if len(ids) == 0 {
			return nil, nil
		}

		jobs, err := s.claimCandidates(ctx, opts, ids, t)
		if err != nil {
			return nil, err
		}

		if len(jobs) > 0 {
			return jobs, nil
		}
	}

	return nil, nil
}

// dequeueCandidates returns the _ids of the top opts.Limit eligible jobs,
// already in contract order.
//
// Ordering happens BEFORE truncation and on the server, over every
// eligible document — not over an arbitrary slice of them. That is the
// whole reason the candidates are read rather than letting N independent
// FindOneAndUpdates each pick their own document: with a locality term
// they could not, because Mongo's findAndModify sort takes field paths
// only and locality is a computed predicate.
func (s *Store) dequeueCandidates(
	ctx context.Context,
	opts job.DequeueOpts,
	t time.Time,
) ([]string, error) {
	pipeline := mongod.Pipeline{
		bson.D{{Key: "$match", Value: dequeueFilter(opts, t)}},
	}

	sortDoc := bson.D{{Key: "priority", Value: -1}}

	// Locality is applied whenever PreferHashes is non-empty, including
	// on otherwise-unbounded opts: IsUnbounded governs FILTERING only.
	// And it ranks strictly BELOW priority — above it, a steady stream of
	// locally staged low-priority work would starve the high-priority job
	// the pool exists to run first.
	if hashes := opts.PreferredHashes(); len(hashes) > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$addFields", Value: bson.M{
			preferredField: preferredExpr(hashes),
		}}})

		sortDoc = append(sortDoc, bson.E{Key: preferredField, Value: -1})
	}

	sortDoc = append(sortDoc, bson.E{Key: "run_at", Value: 1})

	pipeline = append(pipeline,
		// $sort immediately followed by $limit is coalesced into a
		// bounded top-k sort, so the locality term — which no index can
		// serve — still costs memory proportional to the limit, not to
		// the size of the pending queue.
		bson.D{{Key: "$sort", Value: sortDoc}},
		bson.D{{Key: "$limit", Value: int64(opts.Limit)}},
		bson.D{{Key: "$project", Value: bson.M{"_id": 1}}},
	)

	var rows []struct {
		ID string `bson:"_id"`
	}

	err := withRetry(ctx, defaultRetry, func(ctx context.Context) error {
		cursor, aggErr := s.mdb.Collection(colJobs).Aggregate(ctx, pipeline)
		if aggErr != nil {
			return aggErr
		}
		defer cursor.Close(ctx)

		rows = rows[:0]

		return cursor.All(ctx, &rows)
	})
	if err != nil {
		return nil, fmt.Errorf("dispatch/mongo: dequeue candidates: %w", err)
	}

	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.ID)
	}

	return ids, nil
}

// claimCandidates claims each candidate in parallel and returns the ones
// it won, still in candidate order.
//
// Order is preserved by writing each result into its candidate's slot and
// compacting afterwards, never by appending in completion order — the
// claims race each other, so completion order is arbitrary.
func (s *Store) claimCandidates(
	ctx context.Context,
	opts job.DequeueOpts,
	ids []string,
	t time.Time,
) ([]*job.Job, error) {
	results := make([]*job.Job, len(ids))
	errsCh := make(chan error, len(ids))

	var wg sync.WaitGroup

	for i, jobID := range ids {
		wg.Add(1)

		go func() {
			defer wg.Done()

			j, err := s.claimOne(ctx, opts, jobID, t)
			if err != nil {
				errsCh <- err

				return
			}

			results[i] = j
		}()
	}

	wg.Wait()
	close(errsCh)

	for err := range errsCh {
		if err != nil && !errors.Is(err, context.Canceled) {
			return nil, err
		}
	}

	jobs := make([]*job.Job, 0, len(ids))

	for _, j := range results {
		if j != nil {
			jobs = append(jobs, j)
		}
	}

	return jobs, nil
}

// claimOne atomically claims one candidate. It returns (nil, nil) when
// the document no longer matches — claimed by someone else, or no longer
// eligible — which is a lost race, not an error.
//
// The filter is the FULL dequeue filter with _id pinned, not just the
// _id: the fit predicate must be evaluated as part of the claim, so a job
// that does not fit is never written to even if it somehow reached the
// candidate list.
//
// The lease grant, when opts asks for one, is part of THIS update
// document. findAndModify applies the whole document atomically, so the
// winner of the race is leased in the instant it is claimed and there is
// no moment at which the job is running without a lease.
func (s *Store) claimOne(
	ctx context.Context,
	opts job.DequeueOpts,
	jobID string,
	t time.Time,
) (*job.Job, error) {
	filter := dequeueFilter(opts, t)
	filter["_id"] = jobID

	set := bson.M{
		"state":      string(job.StateRunning),
		"started_at": t,
		"updated_at": t,
	}
	update := bson.M{"$set": set}

	if opts.Grants() {
		set["worker_id"] = opts.WorkerID.String()
		set["lease_expires_at"] = opts.LeaseUntil.UTC()
		// $inc rather than a computed value: the epoch is the fence and
		// must advance from whatever the document currently holds, which
		// only the document knows.
		update["$inc"] = bson.M{"lease_epoch": 1}
	}

	updateOpts := options.FindOneAndUpdate().SetReturnDocument(options.After)

	var m jobModel

	err := withRetry(ctx, defaultRetry, func(ctx context.Context) error {
		return s.mdb.Collection(colJobs).
			FindOneAndUpdate(ctx, filter, update, updateOpts).
			Decode(&m)
	})
	if err != nil {
		if isNoDocuments(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("dispatch/mongo: dequeue jobs: %w", err)
	}

	j, convErr := fromJobModel(&m)
	if convErr != nil {
		return nil, fmt.Errorf("dispatch/mongo: dequeue convert: %w", convErr)
	}

	return j, nil
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

	// Reap on heartbeat age — or, for workers that died before their first
	// heartbeat (heartbeat_at still null), on start-time age.
	filter := bson.M{
		"state": string(job.StateRunning),
		"$or": bson.A{
			bson.M{"heartbeat_at": bson.M{"$ne": nil, "$lt": cutoff}},
			bson.M{
				"heartbeat_at": nil,
				"started_at":   bson.M{"$ne": nil, "$lt": cutoff},
			},
		},
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
