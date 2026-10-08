package postgres

import (
	"context"
	"time"

	"github.com/xraph/grove/driver"

	"github.com/xraph/dispatch/durable"
)

// GetTask reads pending or finished task state using its full execution identity.
func (s *Store) GetTask(ctx context.Context, key durable.Key, taskID string) (durable.Task, error) {
	if err := key.Validate(); err != nil {
		return durable.Task{}, err
	}
	if err := durable.ValidateTaskID(taskID); err != nil {
		return durable.Task{}, err
	}
	task, err := scanTask(s.pgdb.QueryRow(ctx, `SELECT `+taskColumns+` FROM dispatch_execution_tasks t
        WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND task_id=$4`,
		key.Namespace, key.WorkflowID, key.RunID, taskID))
	if isNoRows(err) {
		return durable.Task{}, durable.ErrNotFound
	}
	if err != nil {
		return durable.Task{}, err
	}
	return *task, nil
}

func lockTaskConditions(ctx context.Context, tx driver.Tx, r durable.CommitRequest) (map[string]durable.Task, error) {
	locked := make(map[string]durable.Task, len(r.Conditions))
	// Every transition already holds this execution's row lock, so other
	// transitions cannot take these task locks in a conflicting order.
	for _, condition := range r.Conditions {
		task, err := scanTask(tx.QueryRow(ctx, `SELECT `+taskColumns+` FROM dispatch_execution_tasks t
            WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND task_id=$4 FOR UPDATE`,
			r.Namespace, r.WorkflowID, r.RunID, condition.TaskID))
		if isNoRows(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		locked[condition.TaskID] = *task
	}
	return locked, nil
}

func saveExecutionTaskState(ctx context.Context, tx driver.Tx, task durable.Task) error {
	_, err := tx.Exec(ctx, `UPDATE dispatch_execution_tasks SET
        available_at=$5, owner=$6, lease_until=$7, version=$8, deadline_at=$9, progress=$10, done=$11, lease_kind=$12
        WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND task_id=$4`,
		task.Namespace, task.WorkflowID, task.RunID, task.ID, task.AvailableAt, task.Owner,
		taskNullableTime(task.LeaseUntil), task.Version, taskNullableTime(task.DeadlineAt), executionBytes(task.Progress), task.Done, string(task.LeaseKind))
	return err
}

func taskNullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}
