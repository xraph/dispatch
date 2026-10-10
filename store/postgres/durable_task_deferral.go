package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/xraph/grove/driver"

	"github.com/xraph/dispatch/durable"
)

var _ durable.WorkflowTaskDeferralStore = (*Store)(nil)

func readTaskDeferral(ctx context.Context, tx driver.Tx, key durable.Key, id string, current bool) (durable.WorkflowTaskDeferral, error) {
	query := `SELECT response FROM dispatch_workflow_task_deferral_receipts WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND request_id=$4`
	if current {
		query = `SELECT r.response FROM dispatch_workflow_task_deferrals d JOIN dispatch_workflow_task_deferral_receipts r USING(namespace,workflow_id,run_id,request_id) WHERE d.namespace=$1 AND d.workflow_id=$2 AND d.run_id=$3 AND d.task_id=$4`
	}
	var data []byte
	if err := tx.QueryRow(ctx, query, key.Namespace, key.WorkflowID, key.RunID, id).Scan(&data); err != nil {
		if isNoRows(err) {
			return durable.WorkflowTaskDeferral{}, durable.ErrNotFound
		}
		return durable.WorkflowTaskDeferral{}, err
	}
	var d durable.WorkflowTaskDeferral
	if err := json.Unmarshal(data, &d); err != nil {
		return d, durable.ErrInvalid
	}
	if d.Key != key || d.PolicyVersion != durable.WorkflowTaskDeferralPolicyVersion || d.DeferralCount < 1 || d.TaskVersion < 1 || d.RecordedAt.IsZero() || !d.RetryAt.After(d.RecordedAt) || (current && d.TaskID != id) || (!current && d.RequestID != id) {
		return d, durable.ErrInvalid
	}
	return d, nil
}
func (s *Store) DeferWorkflowTask(ctx context.Context, r durable.WorkflowTaskDeferralRequest) (result durable.WorkflowTaskDeferralReceipt, resultErr error) {
	defer func() { resultErr = normalizeExecutionError(resultErr) }()
	if err := r.Validate(); err != nil {
		return result, err
	}
	metadata := durable.AuditMetadataFromContext(ctx)
	metadata.RequestID = r.RequestID
	metadata.ReasonCode = "target_" + string(r.TargetState)
	ctx = durable.WithAuditMetadata(ctx, metadata)
	digest, err := durable.Fingerprint("workflow-task.defer.v1", r)
	if err != nil {
		return result, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer s.rollbackExecution(tx)
	if checkErr := lockAuditMutation(ctx, tx, r.Namespace); checkErr != nil {
		return result, checkErr
	}
	execution, err := scanExecution(tx.QueryRow(ctx, `SELECT `+executionColumns+` FROM dispatch_executions WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 FOR UPDATE`, r.Namespace, r.WorkflowID, r.RunID))
	if err != nil {
		return result, err
	}
	receipt, found, err := readExecutionReceipt(ctx, tx, r.Key, r.RequestID, digest)
	if err != nil {
		return result, err
	}
	if found {
		saved, readErr := readTaskDeferral(ctx, tx, r.Key, r.RequestID, false)
		if readErr != nil || saved.Receipt != receipt {
			return result, errors.Join(durable.ErrInvalid, readErr)
		}
		return saved.WorkflowTaskDeferralReceipt, nil
	}
	var installation string
	if err = tx.QueryRow(ctx, `SELECT n.installation_id FROM dispatch_durable_namespaces n JOIN dispatch_retirement_namespaces r USING(namespace) WHERE n.namespace=$1 AND n.require_audit`, r.Namespace).Scan(&installation); err != nil {
		if isNoRows(err) {
			return result, durable.ErrWriterCompatibility
		}
		return result, err
	}
	task, err := scanTask(tx.QueryRow(ctx, `SELECT `+taskColumns+` FROM dispatch_execution_tasks t WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND task_id=$4 FOR UPDATE`, r.Namespace, r.WorkflowID, r.RunID, r.Token.TaskID))
	if isNoRows(err) {
		return result, durable.ErrLeaseLost
	}
	if err != nil {
		return result, err
	}
	now, err := executionTime(ctx, tx)
	if err != nil {
		return result, err
	}
	target := durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: installation, Namespace: r.Namespace}, BuildID: r.TargetBuildID}
	build, err := readBuildAdmission(ctx, tx, target)
	if errors.Is(err, durable.ErrNotFound) {
		build = durable.BuildAdmission{BuildTarget: target, State: string(durable.DeferralUnregistered)}
	} else if err != nil {
		return result, err
	}
	previous, err := readTaskDeferral(ctx, tx, r.Key, r.Token.TaskID, true)
	if err != nil && !errors.Is(err, durable.ErrNotFound) {
		return result, err
	}
	next, response, err := durable.PrepareWorkflowTaskDeferral(execution, *task, r, build, previous, now)
	if err != nil {
		return result, err
	}
	if saveErr := saveExecutionTaskState(ctx, tx, next); saveErr != nil {
		return result, saveErr
	}
	data, err := json.Marshal(response)
	if err != nil {
		return result, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO dispatch_workflow_task_deferral_receipts(namespace,workflow_id,run_id,request_id,response) VALUES($1,$2,$3,$4,$5)`, r.Namespace, r.WorkflowID, r.RunID, r.RequestID, data); err != nil {
		return result, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO dispatch_workflow_task_deferrals(namespace,workflow_id,run_id,task_id,request_id) VALUES($1,$2,$3,$4,$5) ON CONFLICT(namespace,workflow_id,run_id,task_id) DO UPDATE SET request_id=EXCLUDED.request_id`, r.Namespace, r.WorkflowID, r.RunID, r.Token.TaskID, r.RequestID); err != nil {
		return result, err
	}
	if saveErr := saveExecutionReceipt(ctx, tx, r.Key, r.RequestID, digest, digest, "workflow.task_deferred", response.Receipt); saveErr != nil {
		return result, saveErr
	}
	return response.WorkflowTaskDeferralReceipt, tx.Commit()
}
func (s *Store) GetWorkflowTaskDeferral(ctx context.Context, key durable.Key, taskID string) (durable.WorkflowTaskDeferral, error) {
	if key.Validate() != nil || durable.ValidateTaskID(taskID) != nil {
		return durable.WorkflowTaskDeferral{}, durable.ErrInvalid
	}
	var data []byte
	var active bool
	err := s.pgdb.QueryRow(ctx, `SELECT r.response, e.state='running' AND NOT t.done AND t.owner='' AND t.version=(r.response->>'TaskVersion')::BIGINT AND t.available_at=(r.response->>'RetryAt')::TIMESTAMPTZ FROM dispatch_workflow_task_deferrals d JOIN dispatch_workflow_task_deferral_receipts r USING(namespace,workflow_id,run_id,request_id) JOIN dispatch_executions e USING(namespace,workflow_id,run_id) JOIN dispatch_execution_tasks t ON (t.namespace,t.workflow_id,t.run_id,t.task_id)=(d.namespace,d.workflow_id,d.run_id,d.task_id) WHERE d.namespace=$1 AND d.workflow_id=$2 AND d.run_id=$3 AND d.task_id=$4`, key.Namespace, key.WorkflowID, key.RunID, taskID).Scan(&data, &active)
	if isNoRows(err) {
		return durable.WorkflowTaskDeferral{}, durable.ErrNotFound
	}
	if err != nil {
		return durable.WorkflowTaskDeferral{}, err
	}
	var d durable.WorkflowTaskDeferral
	if err = json.Unmarshal(data, &d); err != nil {
		return d, durable.ErrInvalid
	}
	if d.Key != key || d.TaskID != taskID || d.PolicyVersion != durable.WorkflowTaskDeferralPolicyVersion {
		return d, durable.ErrInvalid
	}
	d.Active = active
	return d, nil
}
