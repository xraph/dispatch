package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/workflow"
)

var (
	_ dlq.ReplayClaimer    = (*Store)(nil)
	_ cron.TargetedUpdater = (*Store)(nil)
	_ workflow.Reopener    = (*Store)(nil)
)

// Each conditional write below is one UpdateOne whose filter carries the
// condition. Mongo evaluates the filter and applies the update to a single
// document atomically, so of two concurrent callers exactly one matches.
// When nothing matched, a second read only decides which error to return;
// it never decides whether the write happens.

// ClaimReplay marks an unreplayed entry replayed by jobID.
//
// "Unreplayed" is replayed_at equal to nil, which matches both shapes an
// unreplayed entry can have: the explicit null grove's insert writes, and
// the absent key a raw-driver write or ReleaseReplay leaves.
func (s *Store) ClaimReplay(ctx context.Context, entryID id.DLQID, jobID id.JobID) error {
	col := s.mdb.Collection(colDLQ)

	res, err := col.UpdateOne(ctx,
		bson.M{"_id": entryID.String(), "replayed_at": nil},
		bson.M{"$set": bson.M{
			"replayed_at":     now(),
			"replayed_job_id": jobID.String(),
		}},
	)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: claim replay: %w", err)
	}
	if res.MatchedCount == 1 {
		return nil
	}

	exists, err := s.dlqExists(ctx, entryID)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: claim replay: %w", err)
	}
	if !exists {
		return dispatch.ErrDLQNotFound
	}

	return dispatch.ErrDLQAlreadyReplayed
}

// ReleaseReplay clears a claim, but only the one jobID made. The filter
// names the job, so a release that lost to another claim matches nothing
// and changes nothing.
func (s *Store) ReleaseReplay(ctx context.Context, entryID id.DLQID, jobID id.JobID) error {
	col := s.mdb.Collection(colDLQ)

	res, err := col.UpdateOne(ctx,
		bson.M{"_id": entryID.String(), "replayed_job_id": jobID.String()},
		bson.M{"$unset": bson.M{"replayed_at": "", "replayed_job_id": ""}},
	)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: release replay: %w", err)
	}
	if res.MatchedCount == 1 {
		return nil
	}

	exists, err := s.dlqExists(ctx, entryID)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: release replay: %w", err)
	}
	if !exists {
		return dispatch.ErrDLQNotFound
	}

	return nil
}

// dlqExists reports whether an entry with this ID is stored.
func (s *Store) dlqExists(ctx context.Context, entryID id.DLQID) (bool, error) {
	n, err := s.mdb.Collection(colDLQ).CountDocuments(ctx,
		bson.M{"_id": entryID.String()},
		options.Count().SetLimit(1),
	)
	if err != nil {
		return false, err
	}

	return n > 0, nil
}

// GetDLQByJobID returns the newest entry, by ID, for a failed job. It
// reads through the {job_id: 1, _id: -1} index Migrate creates, so the
// first document the index yields is the answer.
func (s *Store) GetDLQByJobID(ctx context.Context, jobID id.JobID) (*dlq.Entry, error) {
	col := s.mdb.Collection(colDLQ)

	var m dlqEntryModel
	err := col.FindOne(ctx,
		bson.M{"job_id": jobID.String()},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&m)
	if err != nil {
		if isNoDocuments(err) {
			return nil, dispatch.ErrDLQNotFound
		}
		return nil, fmt.Errorf("dispatch/mongo: get dlq by job id: %w", err)
	}

	return fromDLQModel(&m)
}

// DeleteDLQ removes one entry.
func (s *Store) DeleteDLQ(ctx context.Context, entryID id.DLQID) error {
	res, err := s.mdb.Collection(colDLQ).DeleteOne(ctx, bson.M{"_id": entryID.String()})
	if err != nil {
		return fmt.Errorf("dispatch/mongo: delete dlq: %w", err)
	}
	if res.DeletedCount == 0 {
		return dispatch.ErrDLQNotFound
	}

	return nil
}

// SetCronEnabled sets enabled, and next_run_at when one is given. It
// writes only those fields and updated_at, never the whole document.
func (s *Store) SetCronEnabled(ctx context.Context, entryID id.CronID, enabled bool, nextRunAt *time.Time) error {
	set := bson.M{
		"enabled":    enabled,
		"updated_at": now(),
	}
	if nextRunAt != nil {
		set["next_run_at"] = *nextRunAt
	}

	res, err := s.mdb.Collection(colCronEntries).UpdateOne(ctx,
		bson.M{"_id": entryID.String()},
		bson.M{"$set": set},
	)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: set cron enabled: %w", err)
	}
	if res.MatchedCount == 0 {
		return dispatch.ErrCronNotFound
	}

	return nil
}

// UpdateCronNextRun sets next_run_at and updated_at, and nothing else, so
// the scheduler's write after a fire can never touch enabled.
func (s *Store) UpdateCronNextRun(ctx context.Context, entryID id.CronID, nextRunAt time.Time) error {
	res, err := s.mdb.Collection(colCronEntries).UpdateOne(ctx,
		bson.M{"_id": entryID.String()},
		bson.M{"$set": bson.M{
			"next_run_at": nextRunAt,
			"updated_at":  now(),
		}},
	)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: update cron next run: %w", err)
	}
	if res.MatchedCount == 0 {
		return dispatch.ErrCronNotFound
	}

	return nil
}

// ReopenRun moves a run that is not running back to running, clearing
// its error and completion time. The state condition is in the filter,
// so of two concurrent reopens exactly one matches.
func (s *Store) ReopenRun(ctx context.Context, runID id.RunID, expectedGeneration int64) error {
	col := s.mdb.Collection(colWorkflowRuns)
	filter := runGenerationFilter(runID.String(), expectedGeneration)
	filter["state"] = bson.M{"$ne": string(workflow.RunStateRunning)}

	res, err := col.UpdateOne(ctx,
		filter,
		bson.M{
			"$inc": bson.M{"replay_generation": 1},
			"$set": bson.M{
				"state":      string(workflow.RunStateRunning),
				"error":      "",
				"updated_at": now(),
			},
			"$unset": bson.M{"completed_at": ""},
		},
	)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: reopen run: %w", err)
	}
	if res.MatchedCount == 1 {
		return nil
	}

	var m workflowRunModel
	err = col.FindOne(ctx, bson.M{"_id": runID.String()},
		options.FindOne().SetProjection(bson.M{"state": 1}),
	).Decode(&m)
	if err != nil {
		if isNoDocuments(err) {
			return dispatch.ErrRunNotFound
		}
		return fmt.Errorf("dispatch/mongo: reopen run: %w", err)
	}

	return fmt.Errorf("%w: run %s is %s or its replay generation changed", dispatch.ErrInvalidState, runID, m.State)
}

// Documents written before generations existed participate as generation zero.
func runGenerationFilter(runID string, generation int64) bson.M {
	filter := bson.M{"_id": runID}
	if generation == 0 {
		filter["$or"] = bson.A{bson.M{"replay_generation": 0}, bson.M{"replay_generation": bson.M{"$exists": false}}}
	} else {
		filter["replay_generation"] = generation
	}
	return filter
}
