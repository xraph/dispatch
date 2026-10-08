package postgres

import (
	"context"
	"database/sql"

	"github.com/xraph/dispatch/durable"
)

// LookupReceipt reads one committed intent without locking or changing its run.
func (s *Store) LookupReceipt(ctx context.Context, r durable.ReceiptRequest) (durable.Receipt, bool, error) {
	if err := r.Validate(); err != nil {
		return durable.Receipt{}, false, err
	}
	var intent sql.NullString
	var revision, first, last sql.NullInt64
	err := s.pgdb.QueryRow(ctx, `SELECT r.intent_digest, r.revision, r.first_sequence, r.last_sequence
        FROM dispatch_executions e LEFT JOIN dispatch_execution_receipts r
          ON r.namespace=e.namespace AND r.workflow_id=e.workflow_id AND r.run_id=e.run_id AND r.request_id=$4
        WHERE e.namespace=$1 AND e.workflow_id=$2 AND e.run_id=$3`, r.Namespace, r.WorkflowID, r.RunID, r.RequestID).
		Scan(&intent, &revision, &first, &last)
	if isNoRows(err) {
		return durable.Receipt{}, false, durable.ErrNotFound
	}
	if err != nil {
		return durable.Receipt{}, false, err
	}
	if !intent.Valid {
		return durable.Receipt{}, false, nil
	}
	if err := durable.CheckReceiptIntent(intent.String, r.IntentDigest); err != nil {
		return durable.Receipt{}, false, err
	}
	return durable.Receipt{Revision: revision.Int64, FirstSequence: first.Int64, LastSequence: last.Int64}, true, nil
}
