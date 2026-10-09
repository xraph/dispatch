package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/xraph/grove/driver"

	"github.com/xraph/dispatch/durable"
)

func lockChildIdentities(ctx context.Context, tx driver.Tx, children []durable.ChildStartSpec) error {
	ordered := append([]durable.ChildStartSpec(nil), children...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Start.WorkflowID < ordered[j].Start.WorkflowID })
	for _, child := range ordered {
		if err := lockSignalWorkflow(ctx, tx, child.Start.Namespace, child.Start.WorkflowID); err != nil {
			return err
		}
	}
	return nil
}

func insertChildExecutions(ctx context.Context, tx driver.Tx, parent durable.Key, children []durable.ChildStartSpec, now time.Time) error {
	for _, child := range children {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM dispatch_child_executions WHERE namespace=$1 AND parent_workflow_id=$2 AND parent_run_id=$3 AND command_id=$4)`, parent.Namespace, parent.WorkflowID, parent.RunID, child.CommandID).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return &durable.ChildStartError{CommandID: child.CommandID, Err: durable.ErrExists}
		}
		digest, err := durable.Fingerprint("start", child.Start)
		if err != nil {
			return err
		}
		created, err := insertStartedExecution(ctx, tx, child.Start, digest, now)
		if err != nil {
			return err
		}
		if !created {
			return &durable.ChildStartError{CommandID: child.CommandID, Err: durable.ErrExists}
		}
		payload, err := json.Marshal(child.Start)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO dispatch_child_executions(namespace,parent_workflow_id,parent_run_id,command_id,child_workflow_id,child_run_id,start_request,parent_queue,parent_close_policy,created_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, parent.Namespace, parent.WorkflowID, parent.RunID, child.CommandID, child.Start.WorkflowID, child.Start.RunID, payload, child.ParentQueue, string(child.ParentClosePolicy), now)
		if err != nil {
			return err
		}
	}
	return nil
}

const childColumns = `c.namespace,c.parent_workflow_id,c.parent_run_id,c.command_id,c.start_request,c.parent_queue,c.parent_close_policy,c.created_at,e.state,e.updated_at,e.workflow_id,e.run_id,c.child_run_id,e.first_run_id`
const childJoin = ` FROM dispatch_child_executions c JOIN LATERAL (SELECT * FROM dispatch_executions x WHERE x.namespace=c.namespace AND x.workflow_id=c.child_workflow_id AND x.first_run_id=c.child_run_id ORDER BY x.run_number DESC LIMIT 1) e ON true`
const childRootSelector = `(SELECT first_run_id FROM dispatch_executions WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3)`

func scanChildExecution(row driver.Row) (durable.ChildExecution, error) {
	var child durable.ChildExecution
	var payload []byte
	var workflowID, runID, rootID, firstID string
	err := row.Scan(&child.Parent.Namespace, &child.Parent.WorkflowID, &child.Parent.RunID, &child.CommandID, &payload, &child.ParentQueue, &child.ParentClosePolicy, &child.CreatedAt, &child.State, &child.UpdatedAt, &workflowID, &runID, &rootID, &firstID)
	if isNoRows(err) {
		return durable.ChildExecution{}, durable.ErrNotFound
	}
	if err != nil {
		return durable.ChildExecution{}, err
	}
	if decodeErr := json.Unmarshal(payload, &child.Start); decodeErr != nil {
		return durable.ChildExecution{}, decodeErr
	}
	if child.Start.Namespace != child.Parent.Namespace || child.Start.WorkflowID != workflowID || child.Start.RunID != rootID || rootID != firstID || child.Validate(child.Parent) != nil {
		return durable.ChildExecution{}, fmt.Errorf("%w: invalid stored child relationship", durable.ErrInvalid)
	}
	child.CurrentKey = durable.Key{Namespace: child.Parent.Namespace, WorkflowID: workflowID, RunID: runID}
	return child, nil
}

// GetChildExecution reads the saved relationship and the child's current state.
func (s *Store) GetChildExecution(ctx context.Context, parent durable.Key, commandID string) (durable.ChildExecution, error) {
	if err := parent.Validate(); err != nil {
		return durable.ChildExecution{}, err
	}
	if err := durable.ValidateTaskID(commandID); err != nil {
		return durable.ChildExecution{}, err
	}
	return scanChildExecution(s.pgdb.QueryRow(ctx, `SELECT `+childColumns+childJoin+` WHERE c.namespace=$1 AND c.parent_workflow_id=$2 AND c.parent_run_id=$3 AND c.command_id=$4`, parent.Namespace, parent.WorkflowID, parent.RunID, commandID))
}

// GetParentExecution reads a child's unique original parent relationship.
func (s *Store) GetParentExecution(ctx context.Context, child durable.Key) (durable.ChildExecution, error) {
	if err := child.Validate(); err != nil {
		return durable.ChildExecution{}, err
	}
	return scanChildExecution(s.pgdb.QueryRow(ctx, `SELECT `+childColumns+childJoin+` WHERE c.namespace=$1 AND c.child_workflow_id=$2 AND c.child_run_id=`+childRootSelector, child.Namespace, child.WorkflowID, child.RunID))
}

// ListChildExecutions pages by command ID, with an exclusive cursor.
func (s *Store) ListChildExecutions(ctx context.Context, parent durable.Key, after string, limit int) ([]durable.ChildExecution, error) {
	if err := parent.Validate(); err != nil {
		return nil, err
	}
	if (after != "" && durable.ValidateTaskID(after) != nil) || limit < 1 || limit > 1000 {
		return nil, durable.ErrInvalid
	}
	if _, err := s.GetExecution(ctx, parent); err != nil {
		return nil, err
	}
	rows, err := s.pgdb.Query(ctx, `SELECT `+childColumns+childJoin+` WHERE c.namespace=$1 AND c.parent_workflow_id=$2 AND c.parent_run_id=$3 AND c.command_id COLLATE "C">$4 COLLATE "C" ORDER BY c.command_id COLLATE "C" LIMIT $5`, parent.Namespace, parent.WorkflowID, parent.RunID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]durable.ChildExecution, 0, limit)
	for rows.Next() {
		child, scanErr := scanChildExecution(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, child)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, rowsErr
	}
	return result, nil
}
