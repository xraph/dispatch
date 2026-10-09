package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/dispatch/durable"
)

func (w *Worker) runExecutionTimeout(ctx context.Context) (bool, error) {
	grant, err := storeCall(ctx, w, func(callCtx context.Context) (*durable.ExecutionTimeoutTask, error) {
		return w.store.ClaimExecutionTimeout(callCtx, durable.ExecutionTimeoutClaimRequest{Namespace: w.options.Namespace, Owner: w.options.Owner, LeaseDuration: w.options.LeaseDuration})
	})
	if err != nil || grant == nil {
		return false, err
	}
	metadata := durable.ExecutionTimeout{Version: 1, Kind: grant.Kind, DeadlineAt: grant.DeadlineAt}
	if grant.Validate() != nil || grant.Namespace != w.options.Namespace || grant.Owner != w.options.Owner || grant.Epoch < 1 || grant.Attempt < 1 || grant.LeaseUntil.IsZero() || metadata.Validate() != nil {
		return true, fmt.Errorf("%w: timeout grant does not match worker routing", durable.ErrInvalid)
	}
	request := durable.ExecutionTimeoutRequest{Key: grant.Key, RequestID: fmt.Sprintf("execution-timeout:%d", grant.Epoch), Owner: grant.Owner, Epoch: grant.Epoch}
	var last error
	for attempt := range 3 {
		if ctx.Err() != nil {
			return true, context.Cause(ctx)
		}
		_, last = storeCall(ctx, w, func(callCtx context.Context) (durable.Receipt, error) {
			return w.store.ApplyExecutionTimeout(callCtx, request)
		})
		if last == nil || definitiveCommitError(last) {
			return true, last
		}
		if attempt < 2 {
			if waitErr := wait(ctx, time.Duration(attempt+1)*10*time.Millisecond); waitErr != nil {
				return true, waitErr
			}
		}
	}
	return true, fmt.Errorf("apply execution timeout after retries: %w", last)
}
