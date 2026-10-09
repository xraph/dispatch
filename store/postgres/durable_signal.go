package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/xraph/grove/driver"

	"github.com/xraph/dispatch/durable"
)

// SignalExecution accepts a signal and workflow wakeup in one transaction.
func (s *Store) SignalExecution(ctx context.Context, r durable.SignalRequest) (durable.SignalReceipt, error) {
	r.Input = bytes.Clone(r.Input)
	if err := r.Validate(); err != nil {
		return durable.SignalReceipt{}, err
	}
	digest, err := durable.Fingerprint("signal", r)
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	payload, err := json.Marshal(durable.Signal{Version: 1, ID: r.RequestID, Name: r.Name, Input: r.Input})
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	defer s.rollbackExecution(tx)
	if lockErr := lockSignalWorkflow(ctx, tx, r.Namespace, r.WorkflowID); lockErr != nil {
		return durable.SignalReceipt{}, lockErr
	}
	prior, found, err := readSignalReceipt(ctx, tx, r.Key, r.RequestID, digest)
	if err != nil || found {
		return prior, err
	}
	var current durable.Execution
	if r.RunID == "" {
		current, err = lockOpenSignalRun(ctx, tx, r.Namespace, r.WorkflowID)
	} else {
		current, err = scanExecution(tx.QueryRow(ctx, `SELECT `+executionColumns+` FROM dispatch_executions
 WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 FOR UPDATE`, r.Namespace, r.WorkflowID, r.RunID))
	}
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	receipt, err := appendSignal(ctx, tx, current, r.BuildID, payload)
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	return finishSignal(ctx, tx, r.RequestID, digest, receipt)
}

// SignalWithStart resolves its workflow-scoped receipt before choosing a run.
func (s *Store) SignalWithStart(ctx context.Context, r durable.SignalWithStartRequest) (durable.SignalReceipt, error) {
	r.Input, r.Start.Input = bytes.Clone(r.Input), bytes.Clone(r.Start.Input)
	if err := r.Validate(); err != nil {
		return durable.SignalReceipt{}, err
	}
	digest, err := durable.Fingerprint("signal-with-start", r)
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	payload, err := json.Marshal(durable.Signal{Version: 1, ID: r.Start.RequestID, Name: r.Name, Input: r.Input})
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	defer s.rollbackExecution(tx)
	if lockErr := lockSignalWorkflow(ctx, tx, r.Start.Namespace, r.Start.WorkflowID); lockErr != nil {
		return durable.SignalReceipt{}, lockErr
	}
	prior, found, err := readSignalReceipt(ctx, tx, r.Start.Key, r.Start.RequestID, digest)
	if err != nil || found {
		return prior, err
	}
	for range 16 {
		current, readErr := lockOpenSignalRun(ctx, tx, r.Start.Namespace, r.Start.WorkflowID)
		if readErr == nil {
			receipt, appendErr := appendSignal(ctx, tx, current, r.Start.BuildID, payload)
			if appendErr != nil {
				return durable.SignalReceipt{}, appendErr
			}
			return finishSignal(ctx, tx, r.Start.RequestID, digest, receipt)
		}
		if !errors.Is(readErr, durable.ErrNotFound) {
			return durable.SignalReceipt{}, readErr
		}
		created, createErr := createSignalRun(ctx, tx, r.Start, payload)
		if createErr != nil {
			return durable.SignalReceipt{}, createErr
		}
		if created {
			receipt := durable.SignalReceipt{Key: r.Start.Key, Receipt: durable.Receipt{Revision: 1, FirstSequence: 1, LastSequence: 2}, Started: true}
			return finishSignal(ctx, tx, r.Start.RequestID, digest, receipt)
		}
		// Recheck the open run before rejecting a closed proposed identity.
		// All current execution creators share this workflow identity lock.
		current, readErr = lockOpenSignalRun(ctx, tx, r.Start.Namespace, r.Start.WorkflowID)
		if readErr == nil {
			receipt, appendErr := appendSignal(ctx, tx, current, r.Start.BuildID, payload)
			if appendErr != nil {
				return durable.SignalReceipt{}, appendErr
			}
			return finishSignal(ctx, tx, r.Start.RequestID, digest, receipt)
		}
		if !errors.Is(readErr, durable.ErrNotFound) {
			return durable.SignalReceipt{}, readErr
		}
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM dispatch_executions WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3)`, r.Start.Namespace, r.Start.WorkflowID, r.Start.RunID).Scan(&exists); err != nil {
			return durable.SignalReceipt{}, err
		}
		if exists {
			return durable.SignalReceipt{}, durable.ErrExists
		}
	}
	return durable.SignalReceipt{}, durable.ErrRevisionConflict
}

func lockSignalWorkflow(ctx context.Context, tx driver.Tx, namespace, workflowID string) error {
	// Quoted components prevent collisions between ambiguous concatenations.
	key := fmt.Sprintf("dispatch.signal/%q/%q", namespace, workflowID)
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, key)
	return err
}

func lockOpenSignalRun(ctx context.Context, tx driver.Tx, namespace, workflowID string) (durable.Execution, error) {
	return scanExecution(tx.QueryRow(ctx, `SELECT `+executionColumns+` FROM dispatch_executions
 WHERE namespace=$1 AND workflow_id=$2 AND state='running' FOR UPDATE`, namespace, workflowID))
}

func readSignalReceipt(ctx context.Context, tx driver.Tx, key durable.Key, requestID, digest string) (durable.SignalReceipt, bool, error) {
	result := durable.SignalReceipt{Key: durable.Key{Namespace: key.Namespace, WorkflowID: key.WorkflowID}}
	var stored string
	err := tx.QueryRow(ctx, `SELECT run_id,digest,revision,first_sequence,last_sequence,started
 FROM dispatch_signal_receipts WHERE namespace=$1 AND workflow_id=$2 AND request_id=$3`, key.Namespace, key.WorkflowID, requestID).
		Scan(&result.RunID, &stored, &result.Revision, &result.FirstSequence, &result.LastSequence, &result.Started)
	if isNoRows(err) {
		return durable.SignalReceipt{}, false, nil
	}
	if err != nil {
		return durable.SignalReceipt{}, false, err
	}
	if stored != digest {
		return durable.SignalReceipt{}, true, durable.ErrRequestConflict
	}
	return result, true, nil
}

func finishSignal(ctx context.Context, tx driver.Tx, requestID, digest string, receipt durable.SignalReceipt) (durable.SignalReceipt, error) {
	_, err := tx.Exec(ctx, `INSERT INTO dispatch_signal_receipts
 (namespace,workflow_id,request_id,run_id,digest,revision,first_sequence,last_sequence,started)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, receipt.Namespace, receipt.WorkflowID, requestID, receipt.RunID, digest, receipt.Revision, receipt.FirstSequence, receipt.LastSequence, receipt.Started)
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	if err = tx.Commit(); err != nil {
		return durable.SignalReceipt{}, fmt.Errorf(errPrefix+"commit signal acceptance: %w", err)
	}
	return receipt, nil
}

func appendSignal(ctx context.Context, tx driver.Tx, current durable.Execution, build string, payload []byte) (durable.SignalReceipt, error) {
	return appendWorkflowInput(ctx, tx, current, build, payload, durable.EventSignalReceived, "signal")
}

func appendWorkflowInput(ctx context.Context, tx driver.Tx, current durable.Execution, build string, payload []byte, eventType, wakeKind string) (durable.SignalReceipt, error) {
	if current.State != durable.StateRunning {
		return durable.SignalReceipt{}, durable.ErrClosed
	}
	if current.BuildID != build {
		return durable.SignalReceipt{}, durable.ErrInvalid
	}
	if current.Revision == math.MaxInt64 || current.LastSequence == math.MaxInt64 {
		return durable.SignalReceipt{}, durable.ErrInvalid
	}
	var queue, kind string
	err := tx.QueryRow(ctx, `SELECT queue,kind FROM dispatch_execution_tasks WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND task_id='workflow:1'`, current.Namespace, current.WorkflowID, current.RunID).Scan(&queue, &kind)
	if isNoRows(err) || err == nil && kind != string(durable.TaskWorkflow) {
		return durable.SignalReceipt{}, durable.ErrInvalid
	}
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	now, err := executionTime(ctx, tx)
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	receipt := durable.SignalReceipt{Key: current.Key, Receipt: durable.Receipt{Revision: current.Revision + 1, FirstSequence: current.LastSequence + 1, LastSequence: current.LastSequence + 1}}
	if eventErr := insertExecutionEvent(ctx, tx, current.Key, durable.Event{EventInput: durable.EventInput{Type: eventType, Payload: payload}, Sequence: receipt.LastSequence, Time: now}); eventErr != nil {
		return durable.SignalReceipt{}, eventErr
	}
	_, err = tx.Exec(ctx, `UPDATE dispatch_executions SET revision=$4,last_sequence=$5,updated_at=$6 WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3`, current.Namespace, current.WorkflowID, current.RunID, receipt.Revision, receipt.LastSequence, now)
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	if err := insertExecutionTask(ctx, tx, current.Key, durable.TaskSpec{ID: fmt.Sprintf("workflow:%s:%d", wakeKind, receipt.Revision), Kind: durable.TaskWorkflow, Queue: queue}, now); err != nil {
		return durable.SignalReceipt{}, err
	}
	return receipt, nil
}

func createSignalRun(ctx context.Context, tx driver.Tx, r durable.StartRequest, payload []byte) (bool, error) {
	result, err := tx.Exec(ctx, `INSERT INTO dispatch_executions (`+executionColumns+`)
 VALUES ($1,$2,$3,$4,$5,'running',1,2,$6,$7,clock_timestamp(),clock_timestamp()) ON CONFLICT DO NOTHING`, r.Namespace, r.WorkflowID, r.RunID, r.WorkflowType, r.BuildID, executionBytes(r.Input), []byte{})
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	if err != nil || count == 0 {
		return false, err
	}
	// Timestamp after uniqueness waits, while the newly inserted row remains
	// private to this transaction, so start and message share the same clock.
	now, err := executionTime(ctx, tx)
	if err != nil {
		return false, err
	}
	_, err = tx.Exec(ctx, `UPDATE dispatch_executions SET created_at=$4,updated_at=$4 WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3`, r.Namespace, r.WorkflowID, r.RunID, now)
	if err != nil {
		return false, err
	}
	for i, event := range []durable.EventInput{{Type: "execution.started", Payload: r.Input}, {Type: durable.EventSignalReceived, Payload: payload}} {
		if eventErr := insertExecutionEvent(ctx, tx, r.Key, durable.Event{EventInput: event, Sequence: int64(i + 1), Time: now}); eventErr != nil {
			return false, eventErr
		}
	}
	if err := insertExecutionTask(ctx, tx, r.Key, durable.TaskSpec{ID: "workflow:1", Kind: durable.TaskWorkflow, Queue: r.Queue}, now); err != nil {
		return false, err
	}
	return true, nil
}
