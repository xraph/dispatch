package postgres

import (
	"context"

	"github.com/xraph/dispatch/durable"
)

// ResolveExecution selects a projection using one database snapshot.
func (s *Store) ResolveExecution(ctx context.Context, r durable.ExecutionTarget) (durable.Execution, error) {
	if err := r.Validate(); err != nil {
		return durable.Execution{}, err
	}
	switch r.Selection {
	case durable.RunExplicit:
		return s.GetExecution(ctx, r.Key)
	case durable.RunCurrent:
		return scanExecution(s.pgdb.QueryRow(ctx, `SELECT `+executionColumns+` FROM dispatch_executions WHERE namespace=$1 AND workflow_id=$2 AND state='running'`, r.Namespace, r.WorkflowID))
	default:
		// The sentinel row distinguishes unknown historical ordering from absence in
		// the same SQL snapshot. Real executions cannot have an empty RunID.
		execution, err := scanExecution(s.pgdb.QueryRow(ctx, `SELECT `+executionColumns+`
 FROM dispatch_executions WHERE namespace=$1 AND workflow_id=$2 AND run_id=(
 SELECT run_id FROM dispatch_execution_heads WHERE namespace=$1 AND workflow_id=$2)
 UNION ALL SELECT namespace,workflow_id,'','','','',0,0,''::bytea,''::bytea,'epoch'::timestamptz,'epoch'::timestamptz,NULL::timestamptz,NULL::timestamptz,'','','',0,'epoch'::timestamptz,0,NULL::jsonb,1,'epoch'::timestamptz,0
 FROM dispatch_execution_heads WHERE namespace=$1 AND workflow_id=$2 AND run_id IS NULL`, r.Namespace, r.WorkflowID))
		if err == nil && execution.RunID == "" {
			return durable.Execution{}, durable.ErrAmbiguousRun
		}
		return execution, err
	}
}
