package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/workflow"
)

// CreateRun persists a new workflow run.
func (s *Store) CreateRun(ctx context.Context, run *workflow.Run) error {
	m := toRunModel(run)
	_, err := s.pgdb.NewInsert(m).Exec(ctx)
	if err != nil {
		if isDuplicateKey(err) {
			return dispatch.ErrJobAlreadyExists
		}
		return fmt.Errorf("dispatch/bun: create run: %w", err)
	}
	return nil
}

// GetRun retrieves a workflow run by ID.
func (s *Store) GetRun(ctx context.Context, runID id.RunID) (*workflow.Run, error) {
	m := new(workflowRunModel)
	err := s.pgdb.NewSelect(m).
		Where("id = ?", runID.String()).
		Limit(1).
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, dispatch.ErrRunNotFound
		}
		return nil, fmt.Errorf("dispatch/bun: get run: %w", err)
	}
	return fromRunModel(m)
}

// UpdateRun persists changes to an existing workflow run.
func (s *Store) UpdateRun(ctx context.Context, run *workflow.Run) error {
	m := toRunModel(run)
	m.UpdatedAt = time.Now().UTC()
	res, err := s.pgdb.NewUpdate(m).WherePK().Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/bun: update run: %w", err)
	}
	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		return dispatch.ErrRunNotFound
	}
	return nil
}

// ListRuns returns workflow runs matching the given options.
func (s *Store) ListRuns(ctx context.Context, opts workflow.ListOpts) ([]*workflow.Run, error) {
	var models []workflowRunModel
	q := s.pgdb.NewSelect(&models)

	if opts.State != "" {
		q = q.Where("state = ?", string(opts.State))
	}

	q = q.OrderExpr("created_at ASC")

	if opts.Limit > 0 {
		q = q.Limit(opts.Limit)
	}
	if opts.Offset > 0 {
		q = q.Offset(opts.Offset)
	}

	err := q.Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("dispatch/bun: list runs: %w", err)
	}

	runs := make([]*workflow.Run, 0, len(models))
	for i := range models {
		r, convErr := fromRunModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/bun: list runs convert: %w", convErr)
		}
		runs = append(runs, r)
	}
	return runs, nil
}

// SaveCheckpoint persists checkpoint data for a workflow step.
// If a checkpoint already exists for the same run/step, it is replaced.
func (s *Store) SaveCheckpoint(ctx context.Context, runID id.RunID, stepName string, data []byte) error {
	m := &checkpointModel{
		ID:        id.NewCheckpointID().String(),
		RunID:     runID.String(),
		StepName:  stepName,
		Data:      data,
		CreatedAt: time.Now().UTC(),
	}
	_, err := s.pgdb.NewInsert(m).
		OnConflict("(run_id, step_name) DO UPDATE").
		Set("data = EXCLUDED.data").
		Set("created_at = EXCLUDED.created_at").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/bun: save checkpoint: %w", err)
	}
	return nil
}

// GetCheckpoint retrieves checkpoint data for a specific workflow step.
// Returns nil data if no checkpoint exists.
func (s *Store) GetCheckpoint(ctx context.Context, runID id.RunID, stepName string) ([]byte, error) {
	m := new(checkpointModel)
	err := s.pgdb.NewSelect(m).
		Where("run_id = ?", runID.String()).
		Where("step_name = ?", stepName).
		Limit(1).
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, nil // no checkpoint is not an error
		}
		return nil, fmt.Errorf("dispatch/bun: get checkpoint: %w", err)
	}
	return m.Data, nil
}

// ListCheckpoints returns all checkpoints for a workflow run.
func (s *Store) ListCheckpoints(ctx context.Context, runID id.RunID) ([]*workflow.Checkpoint, error) {
	var models []checkpointModel
	err := s.pgdb.NewSelect(&models).
		Where("run_id = ?", runID.String()).
		OrderExpr("created_at ASC").
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("dispatch/bun: list checkpoints: %w", err)
	}

	checkpoints := make([]*workflow.Checkpoint, 0, len(models))
	for i := range models {
		cp, convErr := fromCheckpointModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/bun: list checkpoints convert: %w", convErr)
		}
		checkpoints = append(checkpoints, cp)
	}
	return checkpoints, nil
}
