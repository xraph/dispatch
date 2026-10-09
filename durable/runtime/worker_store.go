package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xraph/dispatch/durable"
)

func (w *Worker) snapshot(ctx context.Context, key durable.Key) (durable.Execution, []durable.Event, error) {
	return w.readSnapshot(ctx, key, false)
}

func (w *Worker) readSnapshot(ctx context.Context, key durable.Key, allowClosed bool) (durable.Execution, []durable.Event, error) {
	if ctx.Err() != nil {
		return durable.Execution{}, nil, context.Cause(ctx)
	}
	execution, err := storeCall(ctx, w, func(callCtx context.Context) (durable.Execution, error) { return w.store.GetExecution(callCtx, key) })
	if err != nil {
		return execution, nil, err
	}
	return w.readSnapshotHistory(ctx, key, execution, allowClosed)
}

// readSnapshotHistory consumes the projection already selected by the caller.
// Its identity and sequence bound remain fixed across subsequent history reads.
func (w *Worker) readSnapshotHistory(ctx context.Context, key durable.Key, execution durable.Execution, allowClosed bool) (durable.Execution, []durable.Event, error) {
	if !allowClosed && execution.State != durable.StateRunning {
		return execution, nil, durable.ErrClosed
	}
	if execution.Key != key || execution.BuildID != w.options.BuildID || execution.Namespace != w.options.Namespace {
		return execution, nil, fmt.Errorf("%w: execution does not match worker routing", durable.ErrInvalid)
	}
	if execution.Revision < 1 || execution.LastSequence < 1 || execution.LastSequence > historyLimit(execution) {
		return execution, nil, fmt.Errorf("%w: history exceeds runtime bounds", ErrHistory)
	}
	events := make([]durable.Event, 0, execution.LastSequence)
	for int64(len(events)) < execution.LastSequence {
		if ctx.Err() != nil {
			return execution, nil, context.Cause(ctx)
		}
		limit := int(min(1000, execution.LastSequence-int64(len(events))))
		page, readErr := storeCall(ctx, w, func(callCtx context.Context) ([]durable.Event, error) {
			return w.store.ReadHistory(callCtx, key, int64(len(events)), limit)
		})
		if readErr != nil {
			return execution, nil, readErr
		}
		if len(page) == 0 || len(page) > limit {
			return execution, nil, fmt.Errorf("%w: history page does not match snapshot bounds", ErrHistory)
		}
		events = append(events, page...)
	}
	return execution, events, nil
}

func taskRequest(task durable.Task, revision int64) durable.CommitRequest {
	return durable.CommitRequest{Key: task.Key, Token: task.Token(), ExpectedRevision: revision,
		RequestID: fmt.Sprintf("task:%s:%d:%d", task.ID, task.Epoch, revision)}
}

// Persist retries the exact request after an ambiguous error. Only an explicit
// revision or task observation conflict permits rebuilding against a new snapshot.
func (w *Worker) persist(ctx context.Context, request durable.CommitRequest) error {
	_, err := w.persistReceipt(ctx, request)
	return err
}

func (w *Worker) persistReceipt(ctx context.Context, request durable.CommitRequest) (durable.Receipt, error) {
	return w.persistReceiptWithSend(ctx, request, nil)
}

func (w *Worker) persistReceiptWithSend(ctx context.Context, request durable.CommitRequest, onSend func()) (durable.Receipt, error) {
	var receipt durable.Receipt
	var last error
	for attempt := range 3 {
		if ctx.Err() != nil {
			return durable.Receipt{}, context.Cause(ctx)
		}
		receipt, last = storeCall(ctx, w, func(callCtx context.Context) (durable.Receipt, error) {
			if callCtx.Err() != nil {
				return durable.Receipt{}, context.Cause(callCtx)
			}
			if onSend != nil {
				onSend()
			}
			return w.store.CommitTransition(callCtx, request)
		})
		if last == nil || definitiveCommitError(last) {
			return receipt, last
		}
		if attempt < 2 {
			if waitErr := wait(ctx, time.Duration(attempt+1)*10*time.Millisecond); waitErr != nil {
				return durable.Receipt{}, waitErr
			}
		}
	}
	return durable.Receipt{}, fmt.Errorf("persist task result after retries: %w", last)
}

func definitiveCommitError(err error) bool {
	return errors.Is(err, durable.ErrExecutionDeadline) || errors.Is(err, durable.ErrTaskDeadline) || errors.Is(err, durable.ErrTaskConflict) || errors.Is(err, durable.ErrInvalid) || errors.Is(err, durable.ErrLeaseLost) ||
		errors.Is(err, durable.ErrRevisionConflict) || errors.Is(err, durable.ErrClosed) ||
		errors.Is(err, durable.ErrRequestConflict) || errors.Is(err, durable.ErrExists) || errors.Is(err, durable.ErrNotFound)
}
