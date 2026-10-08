package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xraph/dispatch/durable"
)

func (w *Worker) snapshot(ctx context.Context, key durable.Key) (durable.Execution, []durable.Event, error) {
	execution, err := storeCall(ctx, w, func(callCtx context.Context) (durable.Execution, error) { return w.store.GetExecution(callCtx, key) })
	if err != nil {
		return execution, nil, err
	}
	if execution.State != durable.StateRunning {
		return execution, nil, durable.ErrClosed
	}
	if execution.BuildID != w.options.BuildID || execution.Namespace != w.options.Namespace {
		return execution, nil, fmt.Errorf("%w: execution does not match worker routing", durable.ErrInvalid)
	}
	if execution.LastSequence < 1 || execution.LastSequence > 100000 {
		return execution, nil, fmt.Errorf("%w: history exceeds runtime bounds", ErrHistory)
	}
	events := make([]durable.Event, 0, execution.LastSequence)
	for int64(len(events)) < execution.LastSequence {
		page, readErr := storeCall(ctx, w, func(callCtx context.Context) ([]durable.Event, error) {
			return w.store.ReadHistory(callCtx, key, int64(len(events)), int(min(1000, execution.LastSequence-int64(len(events)))))
		})
		if readErr != nil {
			return execution, nil, readErr
		}
		if len(page) == 0 {
			return execution, nil, fmt.Errorf("%w: history ended before snapshot sequence", ErrHistory)
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
// revision conflict permits rebuilding its content against a newer snapshot.
func (w *Worker) persist(ctx context.Context, request durable.CommitRequest) error {
	var last error
	for attempt := range 3 {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		_, last = storeCall(ctx, w, func(callCtx context.Context) (durable.Receipt, error) {
			return w.store.CommitTransition(callCtx, request)
		})
		if last == nil || errors.Is(last, durable.ErrInvalid) || errors.Is(last, durable.ErrLeaseLost) ||
			errors.Is(last, durable.ErrRevisionConflict) || errors.Is(last, durable.ErrClosed) ||
			errors.Is(last, durable.ErrRequestConflict) || errors.Is(last, durable.ErrExists) || errors.Is(last, durable.ErrNotFound) {
			return last
		}
		if attempt < 2 {
			if waitErr := wait(ctx, time.Duration(attempt+1)*10*time.Millisecond); waitErr != nil {
				return waitErr
			}
		}
	}
	return fmt.Errorf("persist task result after retries: %w", last)
}
