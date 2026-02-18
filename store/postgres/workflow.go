package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/workflow"
)

// CreateRun persists a new workflow run.
func (s *Store) CreateRun(ctx context.Context, run *workflow.Run) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO dispatch_workflow_runs (
			id, name, state, input, output, error,
			scope_app_id, scope_org_id, started_at, completed_at,
			created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		run.ID.String(), run.Name, string(run.State),
		run.Input, run.Output, run.Error,
		run.ScopeAppID, run.ScopeOrgID, run.StartedAt, run.CompletedAt,
		run.CreatedAt, run.UpdatedAt,
	)
	if err != nil {
		if isDuplicateKey(err) {
			return dispatch.ErrJobAlreadyExists
		}
		return fmt.Errorf("dispatch/postgres: create run: %w", err)
	}
	return nil
}

// GetRun retrieves a workflow run by ID.
func (s *Store) GetRun(ctx context.Context, runID id.RunID) (*workflow.Run, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT
			id, name, state, input, output, error,
			scope_app_id, scope_org_id, started_at, completed_at,
			created_at, updated_at
		FROM dispatch_workflow_runs
		WHERE id = $1`,
		runID.String(),
	)

	r, err := scanRun(row)
	if err != nil {
		if isNoRows(err) {
			return nil, dispatch.ErrRunNotFound
		}
		return nil, fmt.Errorf("dispatch/postgres: get run: %w", err)
	}
	return r, nil
}

// UpdateRun persists changes to an existing workflow run.
func (s *Store) UpdateRun(ctx context.Context, run *workflow.Run) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE dispatch_workflow_runs SET
			name = $2, state = $3, input = $4, output = $5,
			error = $6, scope_app_id = $7, scope_org_id = $8,
			started_at = $9, completed_at = $10, updated_at = NOW()
		WHERE id = $1`,
		run.ID.String(), run.Name, string(run.State),
		run.Input, run.Output, run.Error,
		run.ScopeAppID, run.ScopeOrgID, run.StartedAt, run.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("dispatch/postgres: update run: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return dispatch.ErrRunNotFound
	}
	return nil
}

// ListRuns returns workflow runs matching the given options.
func (s *Store) ListRuns(ctx context.Context, opts workflow.ListOpts) ([]*workflow.Run, error) {
	query := `
		SELECT
			id, name, state, input, output, error,
			scope_app_id, scope_org_id, started_at, completed_at,
			created_at, updated_at
		FROM dispatch_workflow_runs
		WHERE 1=1`
	args := []interface{}{}
	argIdx := 1

	if opts.State != "" {
		query += fmt.Sprintf(" AND state = $%d", argIdx)
		args = append(args, string(opts.State))
		argIdx++
	}

	query += " ORDER BY created_at ASC"

	if opts.Limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIdx)
		args = append(args, opts.Limit)
		argIdx++
	}
	if opts.Offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", argIdx)
		args = append(args, opts.Offset)
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("dispatch/postgres: list runs: %w", err)
	}
	defer rows.Close()

	var runs []*workflow.Run
	for rows.Next() {
		r, scanErr := scanRun(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("dispatch/postgres: scan run row: %w", scanErr)
		}
		runs = append(runs, r)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("dispatch/postgres: iterate run rows: %w", err)
	}
	return runs, nil
}

// SaveCheckpoint persists checkpoint data for a workflow step.
// If a checkpoint already exists for the same run/step, it is replaced.
func (s *Store) SaveCheckpoint(ctx context.Context, runID id.RunID, stepName string, data []byte) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO dispatch_checkpoints (id, run_id, step_name, data, created_at)
		VALUES ($1, $2, $3, $4, NOW())
		ON CONFLICT (run_id, step_name) DO UPDATE SET
			data = EXCLUDED.data,
			created_at = NOW()`,
		id.NewCheckpointID().String(), runID.String(), stepName, data,
	)
	if err != nil {
		return fmt.Errorf("dispatch/postgres: save checkpoint: %w", err)
	}
	return nil
}

// GetCheckpoint retrieves checkpoint data for a specific workflow step.
// Returns nil data if no checkpoint exists.
func (s *Store) GetCheckpoint(ctx context.Context, runID id.RunID, stepName string) ([]byte, error) {
	var data []byte
	err := s.pool.QueryRow(ctx,
		`SELECT data FROM dispatch_checkpoints WHERE run_id = $1 AND step_name = $2`,
		runID.String(), stepName,
	).Scan(&data)
	if err != nil {
		if isNoRows(err) {
			return nil, nil // no checkpoint is not an error
		}
		return nil, fmt.Errorf("dispatch/postgres: get checkpoint: %w", err)
	}
	return data, nil
}

// ListCheckpoints returns all checkpoints for a workflow run.
func (s *Store) ListCheckpoints(ctx context.Context, runID id.RunID) ([]*workflow.Checkpoint, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, run_id, step_name, data, created_at
		FROM dispatch_checkpoints
		WHERE run_id = $1
		ORDER BY created_at ASC`,
		runID.String(),
	)
	if err != nil {
		return nil, fmt.Errorf("dispatch/postgres: list checkpoints: %w", err)
	}
	defer rows.Close()

	var checkpoints []*workflow.Checkpoint
	for rows.Next() {
		var (
			cp     workflow.Checkpoint
			idStr  string
			runStr string
		)
		if scanErr := rows.Scan(&idStr, &runStr, &cp.StepName, &cp.Data, &cp.CreatedAt); scanErr != nil {
			return nil, fmt.Errorf("dispatch/postgres: scan checkpoint: %w", scanErr)
		}

		parsedID, parseErr := id.ParseCheckpointID(idStr)
		if parseErr != nil {
			return nil, fmt.Errorf("dispatch/postgres: parse checkpoint id %q: %w", idStr, parseErr)
		}
		cp.ID = parsedID

		parsedRunID, runParseErr := id.ParseRunID(runStr)
		if runParseErr != nil {
			return nil, fmt.Errorf("dispatch/postgres: parse run id %q: %w", runStr, runParseErr)
		}
		cp.RunID = parsedRunID

		checkpoints = append(checkpoints, &cp)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("dispatch/postgres: iterate checkpoint rows: %w", err)
	}
	return checkpoints, nil
}

// scanRun scans a single workflow run row.
func scanRun(row pgx.Row) (*workflow.Run, error) {
	var (
		r        workflow.Run
		idStr    string
		stateStr string
	)
	err := row.Scan(
		&idStr, &r.Name, &stateStr, &r.Input, &r.Output,
		&r.Error, &r.ScopeAppID, &r.ScopeOrgID,
		&r.StartedAt, &r.CompletedAt, &r.CreatedAt, &r.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	r.State = workflow.RunState(stateStr)

	parsedID, parseErr := id.ParseRunID(idStr)
	if parseErr != nil {
		return nil, fmt.Errorf("dispatch/postgres: parse run id %q: %w", idStr, parseErr)
	}
	r.ID = parsedID

	return &r, nil
}
