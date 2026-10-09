package postgres

import (
	"context"
	"encoding/json"
	"math"
	"time"

	"github.com/xraph/grove/driver"

	"github.com/xraph/dispatch/durable"
)

func applyChildTarget(ctx context.Context, tx driver.Tx, target durable.Execution, d durable.ChildDelivery, now time.Time) (durable.ChildDeliveryReceipt, error) {
	receipt := durable.ChildDeliveryReceipt{Target: d.Target, Receipt: durable.Receipt{Revision: target.Revision, LastSequence: target.LastSequence}, Disposition: durable.ChildDeliveryIgnoredClosed}
	if target.State == durable.StateRunning && durable.CheckExecutionDeadline(target, now) != nil {
		receipt.Disposition = durable.ChildDeliveryIgnoredExpired
	} else if target.State == durable.StateRunning {
		receipt.Disposition = durable.ChildDeliveryApplied
		var err error
		switch d.Kind {
		case durable.ChildDeliveryResult, durable.ChildDeliveryCancelAck:
			payload, encodeErr := json.Marshal(d.Message)
			if encodeErr != nil {
				return durable.ChildDeliveryReceipt{}, encodeErr
			}
			eventType := durable.EventChildCompleted
			if d.Kind == durable.ChildDeliveryCancelAck {
				eventType = durable.EventChildCancellationAcknowledged
			}
			accepted, appendErr := appendWorkflowInput(ctx, tx, target, d.TargetBuildID, payload, eventType, "child")
			err = appendErr
			receipt.Receipt = accepted.Receipt
		case durable.ChildDeliveryClose:
			if d.Message.Policy == durable.ParentCloseTerminate {
				receipt.Receipt, err = terminateChildTarget(ctx, tx, target, d, now)
			} else {
				receipt.Receipt, err = cancelChildTarget(ctx, tx, target, d)
			}
		case durable.ChildDeliveryCancel:
			receipt.Receipt, err = cancelChildTarget(ctx, tx, target, d)
		}
		if err != nil {
			return durable.ChildDeliveryReceipt{}, err
		}
	}
	if d.Kind == durable.ChildDeliveryCancel && d.Message.CancellationID != "" {
		link, err := scanChildExecution(tx.QueryRow(ctx, `SELECT `+childColumns+childJoin+` WHERE c.namespace=$1 AND c.child_workflow_id=$2 AND c.child_run_id=`+childRootSelector, d.Target.Namespace, d.Target.WorkflowID, d.Target.RunID))
		if err != nil {
			return durable.ChildDeliveryReceipt{}, err
		}
		if link.Parent != d.Source || link.CommandID != d.Message.CommandID {
			return durable.ChildDeliveryReceipt{}, durable.ErrInvalid
		}
		var build string
		if buildErr := tx.QueryRow(ctx, `SELECT build_id FROM dispatch_executions WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3`, link.Parent.Namespace, link.Parent.WorkflowID, link.Parent.RunID).Scan(&build); buildErr != nil {
			return durable.ChildDeliveryReceipt{}, buildErr
		}
		ack, err := d.CancellationAcknowledgment(build, link.ParentQueue, receipt.Disposition, now)
		if err != nil {
			return durable.ChildDeliveryReceipt{}, err
		}
		if err := insertChildDeliveries(ctx, tx, []durable.ChildDelivery{ack}); err != nil {
			return durable.ChildDeliveryReceipt{}, err
		}
	}
	return receipt, nil
}

func cancelChildTarget(ctx context.Context, tx driver.Tx, target durable.Execution, d durable.ChildDelivery) (durable.Receipt, error) {
	r := d.CancellationRequest()
	digest, err := durable.Fingerprint("cancel-execution", r)
	if err != nil {
		return durable.Receipt{}, err
	}
	prior, found, err := readCancellationReceipt(ctx, tx, r.Key, r.RequestID, digest)
	if err != nil || found {
		return prior.Receipt, err
	}
	payload, err := json.Marshal(durable.ExecutionCancellation{Version: 1, RequestID: r.RequestID, Reason: r.Reason})
	if err != nil {
		return durable.Receipt{}, err
	}
	accepted, err := appendWorkflowInput(ctx, tx, target, r.BuildID, payload, durable.EventCancellationRequested, "cancel-request")
	if err != nil {
		return durable.Receipt{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO dispatch_cancellation_receipts(namespace,workflow_id,request_id,run_id,digest,revision,first_sequence,last_sequence) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, r.Namespace, r.WorkflowID, r.RequestID, r.RunID, digest, accepted.Revision, accepted.FirstSequence, accepted.LastSequence)
	if err != nil {
		return durable.Receipt{}, err
	}
	return accepted.Receipt, nil
}

func terminateChildTarget(ctx context.Context, tx driver.Tx, target durable.Execution, d durable.ChildDelivery, now time.Time) (durable.Receipt, error) {
	if target.Revision == math.MaxInt64 || target.LastSequence == math.MaxInt64 {
		return durable.Receipt{}, durable.ErrInvalid
	}
	payload, err := json.Marshal(durable.ExecutionTermination{Version: 1, RequestID: d.CancellationRequest().RequestID, Reason: "parent closed"})
	if err != nil {
		return durable.Receipt{}, err
	}
	event := durable.EventInput{Type: durable.EventWorkflowTerminated, Payload: payload}
	target.Revision++
	target.LastSequence++
	target.State = durable.StateTerminated
	target.Output = nil
	target.UpdatedAt = now
	if eventErr := insertExecutionEvent(ctx, tx, target.Key, durable.Event{EventInput: event, Sequence: target.LastSequence, Time: now}); eventErr != nil {
		return durable.Receipt{}, eventErr
	}
	_, err = tx.Exec(ctx, `UPDATE dispatch_executions SET state=$4,revision=$5,last_sequence=$6,output=$7,updated_at=$8 WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3`, target.Namespace, target.WorkflowID, target.RunID, string(target.State), target.Revision, target.LastSequence, []byte{}, now)
	if err != nil {
		return durable.Receipt{}, err
	}
	_, err = tx.Exec(ctx, `UPDATE dispatch_execution_tasks SET done=TRUE,version=version+1 WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND NOT done`, target.Namespace, target.WorkflowID, target.RunID)
	if err != nil {
		return durable.Receipt{}, err
	}
	generated, err := prepareChildDeliveries(ctx, tx, target, durable.CommitRequest{Key: target.Key, Events: []durable.EventInput{event}}, now)
	if err != nil {
		return durable.Receipt{}, err
	}
	if err := insertChildDeliveries(ctx, tx, generated); err != nil {
		return durable.Receipt{}, err
	}
	return durable.Receipt{Revision: target.Revision, FirstSequence: target.LastSequence, LastSequence: target.LastSequence}, nil
}
