package redis

import (
	"context"
	"fmt"
	"sort"
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

	// Version and ParentRunID were dropped by every write before this
	// release, so a run written then reads back as unversioned (latest)
	// and top-level. Both are omitted when zero, like on workflow.Run.
	Version          int    `json:"version,omitempty"`
	ReplayGeneration int64  `json:"replay_generation"`
	ParentRunID      string `json:"parent_run_id,omitempty"`
}

func toRunEntity(r *workflow.Run) *runEntity {
	var parentRunID string
	if r.ParentRunID != nil {
		parentRunID = r.ParentRunID.String()
	}

	return &runEntity{
		ID:               r.ID.String(),
		Name:             r.Name,
		State:            string(r.State),
		Input:            r.Input,
		Output:           r.Output,
		Error:            r.Error,
		ScopeAppID:       r.ScopeAppID,
		ScopeOrgID:       r.ScopeOrgID,
		StartedAt:        r.StartedAt,
		CompletedAt:      r.CompletedAt,
		CreatedAt:        r.CreatedAt,
		UpdatedAt:        r.UpdatedAt,
		Version:          r.Version,
		ReplayGeneration: r.ReplayGeneration,
		ParentRunID:      parentRunID,
	}
}

func fromRunEntity(e *runEntity) (*workflow.Run, error) {
	rID, err := id.ParseRunID(e.ID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: parse run id: %w", err)
	}

	var parentRunID *id.RunID
	if e.ParentRunID != "" {
		parsed, parseErr := id.ParseRunID(e.ParentRunID)
		if parseErr != nil {
			return nil, fmt.Errorf("dispatch/redis: parse parent run id: %w", parseErr)
		}
		parentRunID = &parsed
	}

	return &workflow.Run{
		Entity: dispatch.Entity{
			CreatedAt: e.CreatedAt,
			UpdatedAt: e.UpdatedAt,
		},
		ID:               rID,
		Name:             e.Name,
		State:            workflow.RunState(e.State),
		Input:            e.Input,
		Output:           e.Output,
		Error:            e.Error,
		ScopeAppID:       e.ScopeAppID,
		ScopeOrgID:       e.ScopeOrgID,
		StartedAt:        e.StartedAt,
		CompletedAt:      e.CompletedAt,
		Version:          e.Version,
		ReplayGeneration: e.ReplayGeneration,
		ParentRunID:      parentRunID,
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
	key := s.keys.run(rID)

	exists, err := s.entityExists(ctx, key)
	if err != nil {
		return fmt.Errorf("dispatch/redis: create run exists: %w", err)
	}
	if exists {
		return dispatch.ErrJobAlreadyExists // reuse duplicate sentinel
	}

	// Index before the entity; indexCreated says why the order matters.
	if err := s.indexCreated(ctx, entityRun, run.ID); err != nil {
		return fmt.Errorf("dispatch/redis: create run created index: %w", err)
	}

	e := toRunEntity(run)
	if err := s.setEntity(ctx, key, e); err != nil {
		return fmt.Errorf("dispatch/redis: create run set: %w", err)
	}

	if err := s.rdb.SAdd(ctx, s.keys.runIDs(), rID).Err(); err != nil {
		return fmt.Errorf("dispatch/redis: create run index: %w", err)
	}
	return nil
}

// GetRun retrieves a workflow run by ID.
func (s *Store) GetRun(ctx context.Context, runID id.RunID) (*workflow.Run, error) {
	var e runEntity
	if err := s.getEntity(ctx, s.keys.run(runID.String()), &e); err != nil {
		if isNotFound(err) {
			return nil, dispatch.ErrRunNotFound
		}
		return nil, fmt.Errorf("dispatch/redis: get run: %w", err)
	}
	return fromRunEntity(&e)
}

// UpdateRun persists changes to an existing workflow run.
func (s *Store) UpdateRun(ctx context.Context, run *workflow.Run) error {
	key := s.keys.run(run.ID.String())
	e := toRunEntity(run)
	e.UpdatedAt = now()
	return updateEntity(ctx, s, key, dispatch.ErrRunNotFound, func(current *runEntity) error {
		if current.ReplayGeneration != run.ReplayGeneration {
			return fmt.Errorf("%w: run %s replay generation changed", dispatch.ErrInvalidState, run.ID)
		}
		*current = *e
		return nil
	})
}

// ListRuns returns workflow runs matching the given options.
func (s *Store) ListRuns(ctx context.Context, opts workflow.ListOpts) ([]*workflow.Run, error) {
	ids, err := s.rdb.SMembers(ctx, s.keys.runIDs()).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: list runs smembers: %w", err)
	}

	runs := make([]*workflow.Run, 0, len(ids))
	for _, rID := range ids {
		var e runEntity
		if getErr := s.getEntity(ctx, s.keys.run(rID), &e); getErr != nil {
			if isNotFound(getErr) {
				continue
			}
			return nil, fmt.Errorf("dispatch/redis: list runs read: %w", getErr)
		}
		if opts.State != "" && workflow.RunState(e.State) != opts.State {
			continue
		}
		r, convErr := fromRunEntity(&e)
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/redis: list runs convert: %w", convErr)
		}
		runs = append(runs, r)
	}

	return applyPagination(runs, opts.Offset, opts.Limit), nil
}

// SaveCheckpoint persists checkpoint data for a workflow step.
func (s *Store) SaveCheckpoint(ctx context.Context, runID id.RunID, stepName string, data []byte) error {
	rID := runID.String()
	key := s.keys.checkpoint(rID, stepName)

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

	if err := s.rdb.SAdd(ctx, s.keys.checkpointIndex(rID), stepName).Err(); err != nil {
		return fmt.Errorf("dispatch/redis: save checkpoint index: %w", err)
	}
	return nil
}

// GetCheckpoint retrieves checkpoint data for a specific workflow step.
func (s *Store) GetCheckpoint(ctx context.Context, runID id.RunID, stepName string) ([]byte, error) {
	key := s.keys.checkpoint(runID.String(), stepName)
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
	steps, err := s.rdb.SMembers(ctx, s.keys.checkpointIndex(rID)).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: list checkpoints: %w", err)
	}

	checkpoints := make([]*workflow.Checkpoint, 0, len(steps))
	for _, step := range steps {
		key := s.keys.checkpoint(rID, step)
		var e checkpointEntity
		if getErr := s.getEntity(ctx, key, &e); getErr != nil {
			if isNotFound(getErr) {
				continue
			}
			return nil, fmt.Errorf("dispatch/redis: list checkpoints read: %w", getErr)
		}

		cpID, parseErr := id.ParseCheckpointID(e.ID)
		if parseErr != nil || cpID.IsNil() {
			return nil, fmt.Errorf("dispatch/redis: invalid checkpoint ID %q", e.ID)
		}
		rIDParsed, parseErr := id.ParseRunID(e.RunID)
		if parseErr != nil || rIDParsed != runID || e.StepName != step {
			return nil, fmt.Errorf("dispatch/redis: checkpoint identity mismatch for run %s step %q", runID, step)
		}

		checkpoints = append(checkpoints, &workflow.Checkpoint{
			ID:        cpID,
			RunID:     rIDParsed,
			StepName:  e.StepName,
			Data:      e.Data,
			CreatedAt: e.CreatedAt,
		})
	}
	sort.Slice(checkpoints, func(i, j int) bool {
		return workflow.CompareCheckpoints(checkpoints[i], checkpoints[j]) < 0
	})
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
	// Resolve the whole boundary before deleting anything. A failed read
	// cannot turn into a replay that silently keeps a later checkpoint.
	checkpoints, err := s.ListCheckpoints(ctx, runID)
	if err != nil {
		return err
	}
	var target *workflow.Checkpoint
	for _, cp := range checkpoints {
		if cp.StepName == afterStep {
			target = cp
			break
		}
	}
	if target == nil {
		return nil
	}
	rID := runID.String()
	for _, cp := range checkpoints {
		if workflow.CompareCheckpoints(cp, target) <= 0 {
			continue
		}
		key := s.keys.checkpoint(rID, cp.StepName)
		if err := s.rdb.Del(ctx, key).Err(); err != nil {
			return fmt.Errorf("delete checkpoint %s: %w", key, err)
		}
		if err := s.rdb.SRem(ctx, s.keys.checkpointIndex(rID), cp.StepName).Err(); err != nil {
			return fmt.Errorf("remove checkpoint index %s: %w", cp.StepName, err)
		}
	}
	return nil
}
