package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/workflow"
)

// CreateRun persists a new workflow run.
func (s *Store) CreateRun(ctx context.Context, run *workflow.Run) error {
	rID := run.ID.String()
	key := runKey(rID)

	exists, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: create run exists: %w", err)
	}
	if exists > 0 {
		return dispatch.ErrJobAlreadyExists // reuse duplicate sentinel
	}

	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, key, runToMap(run))
	pipe.SAdd(ctx, runIDsKey, rID)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/redis: create run: %w", err)
	}
	return nil
}

// GetRun retrieves a workflow run by ID.
func (s *Store) GetRun(ctx context.Context, runID id.RunID) (*workflow.Run, error) {
	key := runKey(runID.String())
	vals, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: get run: %w", err)
	}
	if len(vals) == 0 {
		return nil, dispatch.ErrRunNotFound
	}
	return mapToRun(vals)
}

// UpdateRun persists changes to an existing workflow run.
func (s *Store) UpdateRun(ctx context.Context, run *workflow.Run) error {
	key := runKey(run.ID.String())
	exists, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: update run exists: %w", err)
	}
	if exists == 0 {
		return dispatch.ErrRunNotFound
	}

	m := runToMap(run)
	m["updated_at"] = time.Now().UTC().Format(time.RFC3339Nano)
	_, err = s.client.HSet(ctx, key, m).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: update run: %w", err)
	}
	return nil
}

// ListRuns returns workflow runs matching the given options.
func (s *Store) ListRuns(ctx context.Context, opts workflow.ListOpts) ([]*workflow.Run, error) {
	ids, err := s.client.SMembers(ctx, runIDsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: list runs smembers: %w", err)
	}

	var runs []*workflow.Run
	for _, rID := range ids {
		vals, getErr := s.client.HGetAll(ctx, runKey(rID)).Result()
		if getErr != nil || len(vals) == 0 {
			continue
		}
		r, convErr := mapToRun(vals)
		if convErr != nil {
			continue
		}
		if opts.State != "" && r.State != opts.State {
			continue
		}
		runs = append(runs, r)
	}

	if opts.Offset > 0 && opts.Offset < len(runs) {
		runs = runs[opts.Offset:]
	} else if opts.Offset >= len(runs) {
		return nil, nil
	}
	if opts.Limit > 0 && opts.Limit < len(runs) {
		runs = runs[:opts.Limit]
	}
	return runs, nil
}

// SaveCheckpoint persists checkpoint data for a workflow step.
func (s *Store) SaveCheckpoint(ctx context.Context, runID id.RunID, stepName string, data []byte) error {
	rID := runID.String()
	key := checkpointKey(rID, stepName)
	cpID := id.NewCheckpointID().String()

	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, key,
		"id", cpID,
		"run_id", rID,
		"step_name", stepName,
		"data", string(data),
		"created_at", time.Now().UTC().Format(time.RFC3339Nano),
	)
	pipe.SAdd(ctx, checkpointIndexKey(rID), stepName)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/redis: save checkpoint: %w", err)
	}
	return nil
}

// GetCheckpoint retrieves checkpoint data for a specific workflow step.
func (s *Store) GetCheckpoint(ctx context.Context, runID id.RunID, stepName string) ([]byte, error) {
	key := checkpointKey(runID.String(), stepName)
	data, err := s.client.HGet(ctx, key, "data").Result()
	if err != nil {
		if err == goredis.Nil {
			return nil, nil // no checkpoint is not an error
		}
		return nil, fmt.Errorf("dispatch/redis: get checkpoint: %w", err)
	}
	return []byte(data), nil
}

// ListCheckpoints returns all checkpoints for a workflow run.
func (s *Store) ListCheckpoints(ctx context.Context, runID id.RunID) ([]*workflow.Checkpoint, error) {
	rID := runID.String()
	steps, err := s.client.SMembers(ctx, checkpointIndexKey(rID)).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: list checkpoints: %w", err)
	}

	var checkpoints []*workflow.Checkpoint
	for _, step := range steps {
		key := checkpointKey(rID, step)
		vals, getErr := s.client.HGetAll(ctx, key).Result()
		if getErr != nil || len(vals) == 0 {
			continue
		}

		cpID, _ := id.ParseCheckpointID(vals["id"])
		rIDParsed, _ := id.ParseRunID(vals["run_id"])
		createdAt, _ := time.Parse(time.RFC3339Nano, vals["created_at"])

		checkpoints = append(checkpoints, &workflow.Checkpoint{
			ID:        cpID,
			RunID:     rIDParsed,
			StepName:  vals["step_name"],
			Data:      []byte(vals["data"]),
			CreatedAt: createdAt,
		})
	}
	return checkpoints, nil
}

// ── helpers ──

func runToMap(r *workflow.Run) map[string]interface{} {
	m := map[string]interface{}{
		"id":         r.ID.String(),
		"name":       r.Name,
		"state":      string(r.State),
		"input":      string(r.Input),
		"output":     string(r.Output),
		"error":      r.Error,
		"scope_app":  r.ScopeAppID,
		"scope_org":  r.ScopeOrgID,
		"started_at": r.StartedAt.Format(time.RFC3339Nano),
		"created_at": r.Entity.CreatedAt.Format(time.RFC3339Nano),
		"updated_at": r.Entity.UpdatedAt.Format(time.RFC3339Nano),
	}
	if r.CompletedAt != nil {
		m["completed_at"] = r.CompletedAt.Format(time.RFC3339Nano)
	}
	return m
}

func mapToRun(m map[string]string) (*workflow.Run, error) {
	rID, err := id.ParseRunID(m["id"])
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: parse run id: %w", err)
	}

	startedAt, _ := time.Parse(time.RFC3339Nano, m["started_at"])
	createdAt, _ := time.Parse(time.RFC3339Nano, m["created_at"])
	updatedAt, _ := time.Parse(time.RFC3339Nano, m["updated_at"])

	r := &workflow.Run{
		Entity: dispatch.Entity{
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
		},
		ID:         rID,
		Name:       m["name"],
		State:      workflow.RunState(m["state"]),
		Input:      []byte(m["input"]),
		Output:     []byte(m["output"]),
		Error:      m["error"],
		ScopeAppID: m["scope_app"],
		ScopeOrgID: m["scope_org"],
		StartedAt:  startedAt,
	}

	if v := m["completed_at"]; v != "" {
		t, _ := time.Parse(time.RFC3339Nano, v)
		r.CompletedAt = &t
	}
	return r, nil
}
