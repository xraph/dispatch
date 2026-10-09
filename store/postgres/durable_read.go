package postgres

import (
	"context"
	"database/sql"

	"github.com/xraph/dispatch/durable"
)

var _ durable.ReadStore = (*Store)(nil)

func (s *Store) ListExecutions(ctx context.Context, r durable.ExecutionList) ([]durable.Execution, string, error) {
	p, err := r.Position()
	if err != nil {
		return nil, "", err
	}
	rows, err := s.pgdb.Query(ctx, `SELECT `+executionColumns+` FROM dispatch_executions
 WHERE namespace=$1 AND ($2='' OR workflow_id=$2) AND ($3='' OR workflow_type=$3) AND ($4='' OR build_id=$4) AND ($5='' OR state=$5)
 AND ($6='' OR (created_at,workflow_id COLLATE "C",run_id COLLATE "C")<($7,$8 COLLATE "C",$6 COLLATE "C"))
 ORDER BY created_at DESC,workflow_id COLLATE "C" DESC,run_id COLLATE "C" DESC LIMIT $9`, r.Namespace, r.WorkflowID, r.WorkflowType, r.BuildID, string(r.State), p.ID, p.CreatedAt, p.WorkflowID, r.Limit+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	out := []durable.Execution{}
	for rows.Next() {
		e, scanErr := scanExecution(rows)
		if scanErr != nil {
			return nil, "", scanErr
		}
		out = append(out, e)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, "", rowsErr
	}
	next := ""
	if len(out) > r.Limit {
		out = out[:r.Limit]
		next, err = r.Next(out[len(out)-1])
	}
	return out, next, err
}
func (s *Store) ListTasks(ctx context.Context, r durable.TaskList) ([]durable.Task, string, error) {
	p, err := r.Position()
	if err != nil {
		return nil, "", err
	}
	if _, err = s.GetExecution(ctx, r.Key); err != nil {
		return nil, "", err
	}
	rows, err := s.pgdb.Query(ctx, `SELECT `+taskColumns+` FROM dispatch_execution_tasks t WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND ($4='' OR kind=$4) AND task_id COLLATE "C">$5 COLLATE "C" ORDER BY task_id COLLATE "C" LIMIT $6`, r.Namespace, r.WorkflowID, r.RunID, string(r.Kind), p.ID, r.Limit+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	out := []durable.Task{}
	for rows.Next() {
		t, scanErr := scanTask(rows)
		if scanErr != nil {
			return nil, "", scanErr
		}
		out = append(out, *t)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, "", rowsErr
	}
	next := ""
	if len(out) > r.Limit {
		out = out[:r.Limit]
		next, err = r.Next(out[len(out)-1])
	}
	return out, next, err
}
func (s *Store) ReadBuildFacts(ctx context.Context, namespace, build string) (durable.BuildFacts, error) {
	var out durable.BuildFacts
	if durable.ValidateBuildRead(namespace, build) != nil {
		return out, durable.ErrInvalid
	}
	err := s.pgdb.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE state='running'),coalesce(sum((SELECT count(*) FROM dispatch_execution_tasks t WHERE t.namespace=e.namespace AND t.workflow_id=e.workflow_id AND t.run_id=e.run_id AND NOT t.done)),0) FROM dispatch_executions e WHERE namespace=$1 AND build_id=$2`, namespace, build).Scan(&out.Executions, &out.Running, &out.PendingTasks)
	return out, err
}
func (s *Store) ReadDeliveryStatus(ctx context.Context, r durable.ScopedDeliveryStatus) (durable.DeliveryStatus, error) {
	out := durable.DeliveryStatus{Records: []durable.DeliveryRecord{}}
	if err := r.Validate(); err != nil {
		return out, err
	}
	var oldest sql.NullTime
	err := s.pgdb.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE error_category='conflict'),min(accepted_at) FROM dispatch_durable_outbox WHERE installation_id=$1 AND destination=$2 AND namespace=$3 AND ($4='' OR (workflow_id=$4 AND run_id=$5)) AND delivered_at IS NULL`, r.InstallationID, string(r.Destination), r.Namespace, r.WorkflowID, r.RunID).Scan(&out.Pending, &out.Blocked, &oldest)
	if err != nil {
		return out, err
	}
	out.OldestAcceptedAt = oldest.Time
	rows, err := s.pgdb.Query(ctx, `SELECT `+outboxColumns+` FROM dispatch_durable_outbox WHERE installation_id=$1 AND destination=$2 AND namespace=$3 AND ($4='' OR (workflow_id=$4 AND run_id=$5)) AND id>$6 ORDER BY id LIMIT $7`, r.InstallationID, string(r.Destination), r.Namespace, r.WorkflowID, r.RunID, r.After, r.Limit)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		d, scanErr := scanDelivery(rows)
		if scanErr != nil {
			return out, scanErr
		}
		out.Records = append(out.Records, d)
	}
	return out, rows.Err()
}
