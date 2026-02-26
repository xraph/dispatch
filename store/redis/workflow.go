package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/workflow"
)

// ── JSON model for KV storage ──

type runEntity struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	State       string     `json:"state"`
	Input       []byte     `json:"input,omitempty"`
	Output      []byte     `json:"output,omitempty"`
	Error       string     `json:"error"`
	ScopeAppID  string     `json:"scope_app_id"`
	ScopeOrgID  string     `json:"scope_org_id"`
	StartedAt   time.Time  `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

func toRunEntity(r *workflow.Run) *runEntity {
	return &runEntity{
		ID:          r.ID.String(),
		Name:        r.Name,
		State:       string(r.State),
		Input:       r.Input,
		Output:      r.Output,
		Error:       r.Error,
		ScopeAppID:  r.ScopeAppID,
		ScopeOrgID:  r.ScopeOrgID,
		StartedAt:   r.StartedAt,
		CompletedAt: r.CompletedAt,
		CreatedAt:   r.CreatedAt,
		UpdatedAt:   r.UpdatedAt,
	}
}

func fromRunEntity(e *runEntity) (*workflow.Run, error) {
	rID, err := id.ParseRunID(e.ID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: parse run id: %w", err)
	}

	return &workflow.Run{
		Entity: dispatch.Entity{
			CreatedAt: e.CreatedAt,
			UpdatedAt: e.UpdatedAt,
		},
		ID:          rID,
		Name:        e.Name,
		State:       workflow.RunState(e.State),
		Input:       e.Input,
		Output:      e.Output,
		Error:       e.Error,
		ScopeAppID:  e.ScopeAppID,
		ScopeOrgID:  e.ScopeOrgID,
		StartedAt:   e.StartedAt,
		CompletedAt: e.CompletedAt,
	}, nil
}

type checkpointEntity struct {
	ID        string    `json:"id"`
	RunID     string    `json:"run_id"`
	StepName  string    `json:"step_name"`
	Data      []byte    `json:"data"`
	CreatedAt time.Time `json:"created_at"`
}

// CreateRun persists a new workflow run.
func (s *Store) CreateRun(ctx context.Context, run *workflow.Run) error {
	rID := run.ID.String()
	key := runKey(rID)

	exists, err := s.entityExists(ctx, key)
	if err != nil {
		return fmt.Errorf("dispatch/redis: create run exists: %w", err)
	}
	if exists {
		return dispatch.ErrJobAlreadyExists // reuse duplicate sentinel
	}

	e := toRunEntity(run)
	if err := s.setEntity(ctx, key, e); err != nil {
		return fmt.Errorf("dispatch/redis: create run set: %w", err)
	}

	if err := s.rdb.SAdd(ctx, runIDsKey, rID).Err(); err != nil {
		return fmt.Errorf("dispatch/redis: create run index: %w", err)
	}
	return nil
}

// GetRun retrieves a workflow run by ID.
func (s *Store) GetRun(ctx context.Context, runID id.RunID) (*workflow.Run, error) {
	var e runEntity
	if err := s.getEntity(ctx, runKey(runID.String()), &e); err != nil {
		if isNotFound(err) {
			return nil, dispatch.ErrRunNotFound
		}
		return nil, fmt.Errorf("dispatch/redis: get run: %w", err)
	}
	return fromRunEntity(&e)
}

// UpdateRun persists changes to an existing workflow run.
func (s *Store) UpdateRun(ctx context.Context, run *workflow.Run) error {
	key := runKey(run.ID.String())
	exists, err := s.entityExists(ctx, key)
	if err != nil {
		return fmt.Errorf("dispatch/redis: update run exists: %w", err)
	}
	if !exists {
		return dispatch.ErrRunNotFound
	}

	e := toRunEntity(run)
	e.UpdatedAt = now()
	return s.setEntity(ctx, key, e)
}

// ListRuns returns workflow runs matching the given options.
func (s *Store) ListRuns(ctx context.Context, opts workflow.ListOpts) ([]*workflow.Run, error) {
	ids, err := s.rdb.SMembers(ctx, runIDsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: list runs smembers: %w", err)
	}

	runs := make([]*workflow.Run, 0, len(ids))
	for _, rID := range ids {
		var e runEntity
		if getErr := s.getEntity(ctx, runKey(rID), &e); getErr != nil {
			continue
		}
		if opts.State != "" && workflow.RunState(e.State) != opts.State {
			continue
		}
		r, convErr := fromRunEntity(&e)
		if convErr != nil {
			continue
		}
		runs = append(runs, r)
	}

	return applyPagination(runs, opts.Offset, opts.Limit), nil
}

// SaveCheckpoint persists checkpoint data for a workflow step.
func (s *Store) SaveCheckpoint(ctx context.Context, runID id.RunID, stepName string, data []byte) error {
	rID := runID.String()
	key := checkpointKey(rID, stepName)

	e := &checkpointEntity{
		ID:        id.NewCheckpointID().String(),
		RunID:     rID,
		StepName:  stepName,
		Data:      data,
		CreatedAt: now(),
	}

	if err := s.setEntity(ctx, key, e); err != nil {
		return fmt.Errorf("dispatch/redis: save checkpoint: %w", err)
	}

	if err := s.rdb.SAdd(ctx, checkpointIndexKey(rID), stepName).Err(); err != nil {
		return fmt.Errorf("dispatch/redis: save checkpoint index: %w", err)
	}
	return nil
}

// GetCheckpoint retrieves checkpoint data for a specific workflow step.
func (s *Store) GetCheckpoint(ctx context.Context, runID id.RunID, stepName string) ([]byte, error) {
	key := checkpointKey(runID.String(), stepName)
	var e checkpointEntity
	if err := s.getEntity(ctx, key, &e); err != nil {
		if isNotFound(err) {
			return nil, nil // no checkpoint is not an error
		}
		return nil, fmt.Errorf("dispatch/redis: get checkpoint: %w", err)
	}
	return e.Data, nil
}

// ListCheckpoints returns all checkpoints for a workflow run.
func (s *Store) ListCheckpoints(ctx context.Context, runID id.RunID) ([]*workflow.Checkpoint, error) {
	rID := runID.String()
	steps, err := s.rdb.SMembers(ctx, checkpointIndexKey(rID)).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: list checkpoints: %w", err)
	}

	checkpoints := make([]*workflow.Checkpoint, 0, len(steps))
	for _, step := range steps {
		key := checkpointKey(rID, step)
		var e checkpointEntity
		if getErr := s.getEntity(ctx, key, &e); getErr != nil {
			continue
		}

		cpID, _ := id.ParseCheckpointID(e.ID)  //nolint:errcheck // best-effort
		rIDParsed, _ := id.ParseRunID(e.RunID) //nolint:errcheck // best-effort

		checkpoints = append(checkpoints, &workflow.Checkpoint{
			ID:        cpID,
			RunID:     rIDParsed,
			StepName:  e.StepName,
			Data:      e.Data,
			CreatedAt: e.CreatedAt,
		})
	}
	return checkpoints, nil
}

// ListChildRuns returns all child workflow runs for a parent.
func (s *Store) ListChildRuns(ctx context.Context, parentRunID id.RunID) ([]*workflow.Run, error) {
	// Redis doesn't have relational indexes, so we scan all runs.
	allRuns, err := s.ListRuns(ctx, workflow.ListOpts{})
	if err != nil {
		return nil, err
	}

	var children []*workflow.Run
	for _, r := range allRuns {
		if r.ParentRunID != nil && *r.ParentRunID == parentRunID {
			children = append(children, r)
		}
	}
	return children, nil
}

// DeleteCheckpointsAfter removes all checkpoints created after the
// given step name (by creation order). Used for workflow replay.
func (s *Store) DeleteCheckpointsAfter(ctx context.Context, runID id.RunID, afterStep string) error {
	rID := runID.String()

	// Get the target checkpoint's time.
	var target checkpointEntity
	if err := s.getEntity(ctx, checkpointKey(rID, afterStep), &target); err != nil {
		if isNotFound(err) {
			return nil // step not found; nothing to delete
		}
		return fmt.Errorf("dispatch/redis: get target checkpoint: %w", err)
	}

	// List all step names for this run.
	steps, err := s.rdb.SMembers(ctx, checkpointIndexKey(rID)).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: list checkpoint steps: %w", err)
	}

	for _, step := range steps {
		key := checkpointKey(rID, step)
		var e checkpointEntity
		if getErr := s.getEntity(ctx, key, &e); getErr != nil {
			continue
		}
		if e.CreatedAt.After(target.CreatedAt) {
			if delErr := s.rdb.Del(ctx, key).Err(); delErr != nil {
				return fmt.Errorf("delete checkpoint %s: %w", key, delErr)
			}
			if remErr := s.rdb.SRem(ctx, checkpointIndexKey(rID), step).Err(); remErr != nil {
				return fmt.Errorf("remove checkpoint index %s: %w", step, remErr)
			}
		}
	}
	return nil
}
