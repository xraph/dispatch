package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/xraph/grove/driver"

	"github.com/xraph/dispatch/durable"
)

const childDeliveryColumns = `namespace,source_workflow_id,source_run_id,delivery_id,kind,target_workflow_id,target_run_id,target_build_id,target_queue,message,created_at,available_at,owner,epoch,attempt,lease_until,done,disposition`

func scanChildDelivery(row driver.Row) (durable.ChildDelivery, error) {
	var d durable.ChildDelivery
	var payload []byte
	var until sql.NullTime
	err := row.Scan(&d.Source.Namespace, &d.Source.WorkflowID, &d.Source.RunID, &d.ID, &d.Kind, &d.Target.WorkflowID, &d.Target.RunID, &d.TargetBuildID, &d.TargetQueue, &payload, &d.CreatedAt, &d.AvailableAt, &d.Owner, &d.Epoch, &d.Attempt, &until, &d.Done, &d.Disposition)
	if isNoRows(err) {
		return durable.ChildDelivery{}, durable.ErrNotFound
	}
	if err != nil {
		return durable.ChildDelivery{}, err
	}
	d.Target.Namespace = d.Source.Namespace
	if until.Valid {
		d.LeaseUntil = until.Time
	}
	if err := json.Unmarshal(payload, &d.Message); err != nil {
		return durable.ChildDelivery{}, err
	}
	return d, d.Validate()
}

func insertChildDeliveries(ctx context.Context, tx driver.Tx, deliveries []durable.ChildDelivery) error {
	for _, d := range deliveries {
		if err := d.Validate(); err != nil {
			return err
		}
		payload, err := json.Marshal(d.Message)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO dispatch_child_deliveries(namespace,source_workflow_id,source_run_id,delivery_id,kind,target_workflow_id,target_run_id,target_build_id,target_queue,message,created_at,available_at)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, d.Source.Namespace, d.Source.WorkflowID, d.Source.RunID, d.ID, string(d.Kind), d.Target.WorkflowID, d.Target.RunID, d.TargetBuildID, d.TargetQueue, payload, d.CreatedAt, d.AvailableAt)
		if isDuplicateKey(err) {
			return durable.ErrExists
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func prepareChildDeliveries(ctx context.Context, tx driver.Tx, next durable.Execution, r durable.CommitRequest, now time.Time) ([]durable.ChildDelivery, error) {
	if next.State == durable.StateRunning && !r.CancelPendingTasks && len(r.CancelChildren) == 0 {
		return nil, nil
	}
	b := durable.ChildDeliveryBatch{Next: next, Request: r}
	rows, err := tx.Query(ctx, `SELECT `+childColumns+childJoin+` WHERE c.namespace=$1 AND c.parent_workflow_id=$2 AND c.parent_run_id=$3`, next.Namespace, next.WorkflowID, next.RunID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		child, scanErr := scanChildExecution(rows)
		if scanErr != nil {
			_ = rows.Close()
			return nil, scanErr
		}
		b.Children = append(b.Children, child)
	}
	rowsErr := rows.Err()
	_ = rows.Close()
	if rowsErr != nil {
		return nil, rowsErr
	}
	parent, err := scanChildExecution(tx.QueryRow(ctx, `SELECT `+childColumns+childJoin+` WHERE c.namespace=$1 AND c.child_workflow_id=$2 AND c.child_run_id=$3`, next.Namespace, next.WorkflowID, next.RunID))
	if err != nil && !errors.Is(err, durable.ErrNotFound) {
		return nil, err
	}
	if err == nil {
		b.Parent = &parent
		if err := tx.QueryRow(ctx, `SELECT build_id FROM dispatch_executions WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3`, parent.Parent.Namespace, parent.Parent.WorkflowID, parent.Parent.RunID).Scan(&b.ParentBuildID); err != nil {
			return nil, err
		}
	}
	return durable.PrepareChildDeliveries(b, now)
}

// ClaimChildDelivery leases pending messages independently of source state.
func (s *Store) ClaimChildDelivery(ctx context.Context, r durable.ChildDeliveryClaimRequest) (*durable.ChildDelivery, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer s.rollbackExecution(tx)
	d, err := scanChildDelivery(tx.QueryRow(ctx, `SELECT `+childDeliveryColumns+` FROM dispatch_child_deliveries WHERE namespace=$1 AND ($2='' OR target_build_id=$2) AND NOT done AND available_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp()) ORDER BY available_at,delivery_id COLLATE "C" LIMIT 1 FOR UPDATE SKIP LOCKED`, r.Namespace, r.BuildID))
	if errors.Is(err, durable.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if d.Epoch == math.MaxInt64 || d.Attempt == math.MaxInt64 {
		return nil, durable.ErrInvalid
	}
	now, err := executionTime(ctx, tx)
	if err != nil {
		return nil, err
	}
	d.Owner = r.Owner
	d.Epoch++
	d.Attempt++
	d.LeaseUntil = durable.Timestamp(now.Add(r.LeaseDuration))
	_, err = tx.Exec(ctx, `UPDATE dispatch_child_deliveries SET owner=$5,epoch=$6,attempt=$7,lease_until=$8 WHERE namespace=$1 AND source_workflow_id=$2 AND source_run_id=$3 AND delivery_id=$4`, d.Source.Namespace, d.Source.WorkflowID, d.Source.RunID, d.ID, d.Owner, d.Epoch, d.Attempt, d.LeaseUntil)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &d, nil
}

// GetChildDelivery reads the original routing and its current delivery state.
func (s *Store) GetChildDelivery(ctx context.Context, source durable.Key, id string) (durable.ChildDelivery, error) {
	if err := source.Validate(); err != nil {
		return durable.ChildDelivery{}, err
	}
	if err := durable.ValidateTaskID(id); err != nil {
		return durable.ChildDelivery{}, err
	}
	return scanChildDelivery(s.pgdb.QueryRow(ctx, `SELECT `+childDeliveryColumns+` FROM dispatch_child_deliveries WHERE namespace=$1 AND source_workflow_id=$2 AND source_run_id=$3 AND delivery_id=$4`, source.Namespace, source.WorkflowID, source.RunID, id))
}

// ListChildDeliveries pages by exclusive delivery ID, including completed rows.
func (s *Store) ListChildDeliveries(ctx context.Context, source durable.Key, after string, limit int) ([]durable.ChildDelivery, error) {
	if err := source.Validate(); err != nil {
		return nil, err
	}
	if after != "" && durable.ValidateTaskID(after) != nil || limit < 1 || limit > 1000 {
		return nil, durable.ErrInvalid
	}
	if _, err := s.GetExecution(ctx, source); err != nil {
		return nil, err
	}
	rows, err := s.pgdb.Query(ctx, `SELECT `+childDeliveryColumns+` FROM dispatch_child_deliveries WHERE namespace=$1 AND source_workflow_id=$2 AND source_run_id=$3 AND delivery_id COLLATE "C">$4 COLLATE "C" ORDER BY delivery_id COLLATE "C" LIMIT $5`, source.Namespace, source.WorkflowID, source.RunID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]durable.ChildDelivery, 0, limit)
	for rows.Next() {
		d, scanErr := scanChildDelivery(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, d)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func readChildDeliveryReceipt(ctx context.Context, tx driver.Tx, r durable.ChildDeliveryRequest, digest string) (durable.ChildDeliveryReceipt, bool, error) {
	var receipt durable.ChildDeliveryReceipt
	var stored string
	err := tx.QueryRow(ctx, `SELECT digest,target_workflow_id,target_run_id,revision,first_sequence,last_sequence,disposition FROM dispatch_child_delivery_receipts WHERE namespace=$1 AND source_workflow_id=$2 AND source_run_id=$3 AND delivery_id=$4 AND request_id=$5`, r.Source.Namespace, r.Source.WorkflowID, r.Source.RunID, r.DeliveryID, r.RequestID).Scan(&stored, &receipt.Target.WorkflowID, &receipt.Target.RunID, &receipt.Revision, &receipt.FirstSequence, &receipt.LastSequence, &receipt.Disposition)
	if isNoRows(err) {
		return durable.ChildDeliveryReceipt{}, false, nil
	}
	if err != nil {
		return durable.ChildDeliveryReceipt{}, false, err
	}
	if stored != digest {
		return durable.ChildDeliveryReceipt{}, true, durable.ErrRequestConflict
	}
	receipt.Target.Namespace = r.Source.Namespace
	return receipt, true, nil
}

func finishChildDelivery(ctx context.Context, tx driver.Tx, r durable.ChildDeliveryRequest, digest string, receipt durable.ChildDeliveryReceipt) error {
	_, err := tx.Exec(ctx, `UPDATE dispatch_child_deliveries SET done=TRUE,disposition=$5 WHERE namespace=$1 AND source_workflow_id=$2 AND source_run_id=$3 AND delivery_id=$4`, r.Source.Namespace, r.Source.WorkflowID, r.Source.RunID, r.DeliveryID, receipt.Disposition)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO dispatch_child_delivery_receipts(namespace,source_workflow_id,source_run_id,delivery_id,request_id,digest,target_workflow_id,target_run_id,revision,first_sequence,last_sequence,disposition) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, r.Source.Namespace, r.Source.WorkflowID, r.Source.RunID, r.DeliveryID, r.RequestID, digest, receipt.Target.WorkflowID, receipt.Target.RunID, receipt.Revision, receipt.FirstSequence, receipt.LastSequence, receipt.Disposition)
	return err
}

// ApplyChildDelivery serializes one target without locking the source execution.
func (s *Store) ApplyChildDelivery(ctx context.Context, r durable.ChildDeliveryRequest) (result durable.ChildDeliveryReceipt, resultErr error) {
	defer func() { resultErr = normalizeExecutionError(resultErr) }()
	if err := r.Validate(); err != nil {
		return durable.ChildDeliveryReceipt{}, err
	}
	digest, err := durable.Fingerprint("child-delivery", r)
	if err != nil {
		return durable.ChildDeliveryReceipt{}, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return durable.ChildDeliveryReceipt{}, err
	}
	defer s.rollbackExecution(tx)
	prior, found, err := readChildDeliveryReceipt(ctx, tx, r, digest)
	if err != nil || found {
		return prior, err
	}
	d, err := scanChildDelivery(tx.QueryRow(ctx, `SELECT `+childDeliveryColumns+` FROM dispatch_child_deliveries WHERE namespace=$1 AND source_workflow_id=$2 AND source_run_id=$3 AND delivery_id=$4`, r.Source.Namespace, r.Source.WorkflowID, r.Source.RunID, r.DeliveryID))
	if err != nil {
		return durable.ChildDeliveryReceipt{}, err
	}
	if lockErr := lockSignalWorkflow(ctx, tx, d.Target.Namespace, d.Target.WorkflowID); lockErr != nil {
		return durable.ChildDeliveryReceipt{}, lockErr
	}
	target, err := scanExecution(tx.QueryRow(ctx, `SELECT `+executionColumns+` FROM dispatch_executions WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 FOR UPDATE`, d.Target.Namespace, d.Target.WorkflowID, d.Target.RunID))
	if err != nil {
		return durable.ChildDeliveryReceipt{}, err
	}
	d, err = scanChildDelivery(tx.QueryRow(ctx, `SELECT `+childDeliveryColumns+` FROM dispatch_child_deliveries WHERE namespace=$1 AND source_workflow_id=$2 AND source_run_id=$3 AND delivery_id=$4 FOR UPDATE`, r.Source.Namespace, r.Source.WorkflowID, r.Source.RunID, r.DeliveryID))
	if err != nil {
		return durable.ChildDeliveryReceipt{}, err
	}
	prior, found, err = readChildDeliveryReceipt(ctx, tx, r, digest)
	if err != nil || found {
		return prior, err
	}
	if d.Kind == durable.ChildDeliveryClose && d.Message.Policy == durable.ParentCloseTerminate {
		_, err = tx.Exec(ctx, `SELECT 1 FROM dispatch_execution_tasks WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND NOT done FOR UPDATE`, d.Target.Namespace, d.Target.WorkflowID, d.Target.RunID)
		if err != nil {
			return durable.ChildDeliveryReceipt{}, err
		}
	}
	now, err := executionTime(ctx, tx)
	if err != nil {
		return durable.ChildDeliveryReceipt{}, err
	}
	if leaseErr := d.CheckLease(r, now); leaseErr != nil {
		return durable.ChildDeliveryReceipt{}, leaseErr
	}
	var queue string
	if queueErr := tx.QueryRow(ctx, `SELECT queue FROM dispatch_execution_tasks WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND task_id='workflow:1'`, d.Target.Namespace, d.Target.WorkflowID, d.Target.RunID).Scan(&queue); queueErr != nil {
		return durable.ChildDeliveryReceipt{}, queueErr
	}
	if target.BuildID != d.TargetBuildID || queue != d.TargetQueue {
		return durable.ChildDeliveryReceipt{}, durable.ErrInvalid
	}
	receipt, err := applyChildTarget(ctx, tx, target, d, now)
	if err != nil {
		return durable.ChildDeliveryReceipt{}, err
	}
	if err := finishChildDelivery(ctx, tx, r, digest, receipt); err != nil {
		return durable.ChildDeliveryReceipt{}, err
	}
	if err := tx.Commit(); err != nil {
		return durable.ChildDeliveryReceipt{}, fmt.Errorf(errPrefix+"commit child delivery: %w", err)
	}
	return receipt, nil
}
