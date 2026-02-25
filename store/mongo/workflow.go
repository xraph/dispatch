package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/workflow"
)

// CreateRun persists a new workflow run.
func (s *Store) CreateRun(ctx context.Context, run *workflow.Run) error {
	m := toRunModel(run)
	_, err := s.mdb.NewInsert(m).Exec(ctx)
	if err != nil {
		if isDuplicateKey(err) {
			return dispatch.ErrJobAlreadyExists
		}
		return fmt.Errorf("dispatch/mongo: create run: %w", err)
	}
	return nil
}

// GetRun retrieves a workflow run by ID.
func (s *Store) GetRun(ctx context.Context, runID id.RunID) (*workflow.Run, error) {
	col := s.mdb.Collection(colWorkflowRuns)
	var m workflowRunModel
	err := col.FindOne(ctx, bson.M{"_id": runID.String()}).Decode(&m)
	if err != nil {
		if isNoDocuments(err) {
			return nil, dispatch.ErrRunNotFound
		}
		return nil, fmt.Errorf("dispatch/mongo: get run: %w", err)
	}
	return fromRunModel(&m)
}

// UpdateRun persists changes to an existing workflow run.
func (s *Store) UpdateRun(ctx context.Context, run *workflow.Run) error {
	m := toRunModel(run)
	m.UpdatedAt = now()
	col := s.mdb.Collection(colWorkflowRuns)
	res, err := col.ReplaceOne(ctx, bson.M{"_id": m.ID}, m)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: update run: %w", err)
	}
	if res.MatchedCount == 0 {
		return dispatch.ErrRunNotFound
	}
	return nil
}

// ListRuns returns workflow runs matching the given options.
func (s *Store) ListRuns(ctx context.Context, opts workflow.ListOpts) ([]*workflow.Run, error) {
	col := s.mdb.Collection(colWorkflowRuns)
	filter := bson.M{}

	if opts.State != "" {
		filter["state"] = string(opts.State)
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
		return nil, fmt.Errorf("dispatch/mongo: list runs: %w", err)
	}
	defer cursor.Close(ctx)

	var models []workflowRunModel
	if err := cursor.All(ctx, &models); err != nil {
		return nil, fmt.Errorf("dispatch/mongo: list runs decode: %w", err)
	}

	runs := make([]*workflow.Run, 0, len(models))
	for i := range models {
		r, convErr := fromRunModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/mongo: list runs convert: %w", convErr)
		}
		runs = append(runs, r)
	}
	return runs, nil
}

// SaveCheckpoint persists checkpoint data for a workflow step.
// If a checkpoint already exists for the same run/step, it is replaced.
func (s *Store) SaveCheckpoint(ctx context.Context, runID id.RunID, stepName string, data []byte) error {
	col := s.mdb.Collection(colCheckpoints)
	t := now()

	filter := bson.M{
		"run_id":    runID.String(),
		"step_name": stepName,
	}

	update := bson.M{
		"$set": bson.M{
			"data":       data,
			"created_at": t,
		},
		"$setOnInsert": bson.M{
			"_id":       id.NewCheckpointID().String(),
			"run_id":    runID.String(),
			"step_name": stepName,
		},
	}

	opts := options.UpdateOne().SetUpsert(true)
	_, err := col.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: save checkpoint: %w", err)
	}
	return nil
}

// GetCheckpoint retrieves checkpoint data for a specific workflow step.
// Returns nil data if no checkpoint exists.
func (s *Store) GetCheckpoint(ctx context.Context, runID id.RunID, stepName string) ([]byte, error) {
	col := s.mdb.Collection(colCheckpoints)

	var m checkpointModel
	err := col.FindOne(ctx, bson.M{
		"run_id":    runID.String(),
		"step_name": stepName,
	}).Decode(&m)
	if err != nil {
		if isNoDocuments(err) {
			return nil, nil // no checkpoint is not an error
		}
		return nil, fmt.Errorf("dispatch/mongo: get checkpoint: %w", err)
	}
	return m.Data, nil
}

// ListCheckpoints returns all checkpoints for a workflow run.
func (s *Store) ListCheckpoints(ctx context.Context, runID id.RunID) ([]*workflow.Checkpoint, error) {
	col := s.mdb.Collection(colCheckpoints)

	findOpts := options.Find().SetSort(bson.D{{Key: "created_at", Value: 1}})
	cursor, err := col.Find(ctx, bson.M{"run_id": runID.String()}, findOpts)
	if err != nil {
		return nil, fmt.Errorf("dispatch/mongo: list checkpoints: %w", err)
	}
	defer cursor.Close(ctx)

	var models []checkpointModel
	if err := cursor.All(ctx, &models); err != nil {
		return nil, fmt.Errorf("dispatch/mongo: list checkpoints decode: %w", err)
	}

	checkpoints := make([]*workflow.Checkpoint, 0, len(models))
	for i := range models {
		cp, convErr := fromCheckpointModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/mongo: list checkpoints convert: %w", convErr)
		}
		checkpoints = append(checkpoints, cp)
	}
	return checkpoints, nil
}
