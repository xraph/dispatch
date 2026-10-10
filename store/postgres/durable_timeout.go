package postgres

import (
	"context"
	"fmt"

	"github.com/xraph/dispatch/durable"
)

// ClaimTimeoutTask grants expired activity processing across queues. The timeout
// lease is independent of the expired business deadline and fences older grants.
func (s *Store) ClaimTimeoutTask(ctx context.Context, r durable.TimeoutClaimRequest) (result *durable.Task, resultErr error) {
	defer func() { resultErr = normalizeExecutionError(resultErr) }()
	if err := r.Validate(); err != nil {
		return nil, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer s.rollbackExecution(tx)
	if checkErr := lockAuditMutation(ctx, tx, r.Namespace); checkErr != nil {
		return nil, checkErr
	}
	task, err := scanTask(tx.QueryRow(ctx, `WITH candidate AS (
  SELECT t.namespace,t.workflow_id,t.run_id,t.task_id
  FROM dispatch_execution_tasks t JOIN dispatch_executions e USING(namespace,workflow_id,run_id)
  WHERE t.namespace=$1 AND t.kind='activity' AND NOT t.done
   AND e.state='running' AND (LEAST(e.run_deadline_at,e.execution_deadline_at) IS NULL OR LEAST(e.run_deadline_at,e.execution_deadline_at)>clock_timestamp()) AND ($2='' OR e.build_id=$2)
   AND t.deadline_at <= clock_timestamp()
   AND (t.lease_kind='' OR t.lease_until IS NULL OR t.lease_until <= clock_timestamp())
  ORDER BY t.deadline_at,t.workflow_id,t.run_id,t.task_id
  FOR UPDATE OF t SKIP LOCKED LIMIT 1
 ) UPDATE dispatch_execution_tasks t SET owner=$3,epoch=t.epoch+1,version=t.version+1,
  lease_kind='timeout',lease_until=clock_timestamp()+($4 * interval '1 microsecond')
 FROM candidate c WHERE t.namespace=c.namespace AND t.workflow_id=c.workflow_id
  AND t.run_id=c.run_id AND t.task_id=c.task_id RETURNING `+taskColumns,
		r.Namespace, r.BuildID, r.Owner, r.LeaseDuration.Microseconds()))
	if isNoRows(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf(errPrefix+"claim activity timeout: %w", err)
	}
	if checkErr := tx.Commit(); checkErr != nil {
		return nil, checkErr
	}
	return task, nil
}
