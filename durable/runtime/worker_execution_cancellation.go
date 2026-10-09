package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/dispatch/durable"
)

// RequestCancelExecution durably accepts cancellation on an explicit or current
// run. The receipt proves acceptance only. Authorize callers before exposing this
// trusted API remotely; repeat the whole request after an unknown response.
func (w *Worker) RequestCancelExecution(ctx context.Context, request durable.CancelExecutionRequest) (durable.CancelExecutionReceipt, error) {
	if err := request.Validate(); err != nil {
		return durable.CancelExecutionReceipt{}, err
	}
	if request.Namespace != w.options.Namespace || request.BuildID != w.options.BuildID {
		return durable.CancelExecutionReceipt{}, fmt.Errorf("%w: cancellation does not match worker namespace and build", durable.ErrInvalid)
	}
	var last error
	for attempt := range 3 {
		if ctx.Err() != nil {
			return durable.CancelExecutionReceipt{}, context.Cause(ctx)
		}
		receipt, err := storeCall(ctx, w, func(callCtx context.Context) (durable.CancelExecutionReceipt, error) {
			return w.store.RequestCancelExecution(callCtx, request)
		})
		if err == nil || definitiveCommitError(err) {
			return receipt, err
		}
		last = err
		if attempt < 2 {
			if waitErr := wait(ctx, time.Duration(attempt+1)*10*time.Millisecond); waitErr != nil {
				return durable.CancelExecutionReceipt{}, waitErr
			}
		}
	}
	return durable.CancelExecutionReceipt{}, fmt.Errorf("accept cancellation after retries: %w", last)
}
