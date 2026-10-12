package runtime

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/xraph/dispatch/durable"
)

// SignalExecution persists a signal without requiring a registered handler.
// Authorize callers before exposing this trusted Go API remotely. Retry with
// the entire original request when an acknowledgement is lost.
func (w *Worker) SignalExecution(ctx context.Context, request durable.SignalRequest) (durable.SignalReceipt, error) {
	request.Input = bytes.Clone(request.Input)
	if err := request.Validate(); err != nil {
		return durable.SignalReceipt{}, err
	}
	if request.Namespace != w.options.Namespace || request.BuildID != w.options.BuildID {
		return durable.SignalReceipt{}, fmt.Errorf("%w: signal does not match worker namespace and build", durable.ErrInvalid)
	}
	return w.acceptSignal(ctx, func(callCtx context.Context) (durable.SignalReceipt, error) {
		return w.store.SignalExecution(callCtx, request)
	})
}

// SignalWithStart atomically chooses or creates a run and accepts its signal.
// The proposed queue is used only for a new run. The callback worker's queue
// and handler registrations do not change the existing run's routing.
func (w *Worker) SignalWithStart(ctx context.Context, request durable.SignalWithStartRequest) (durable.SignalReceipt, error) {
	request.Input, request.Start.Input = bytes.Clone(request.Input), bytes.Clone(request.Start.Input)
	if err := request.Validate(); err != nil {
		return durable.SignalReceipt{}, err
	}
	if request.Start.Namespace != w.options.Namespace || request.Start.BuildID != w.options.BuildID || !validID(request.Start.Queue) || !validID(request.Start.WorkflowType) {
		return durable.SignalReceipt{}, fmt.Errorf("%w: signal start does not match worker routing or runtime limits", durable.ErrInvalid)
	}
	return w.acceptSignal(ctx, func(callCtx context.Context) (durable.SignalReceipt, error) {
		return w.store.SignalWithStart(callCtx, request)
	})
}

func (w *Worker) acceptSignal(ctx context.Context, call func(context.Context) (durable.SignalReceipt, error)) (durable.SignalReceipt, error) {
	var last error
	for attempt := range 3 {
		if ctx.Err() != nil {
			return durable.SignalReceipt{}, context.Cause(ctx)
		}
		receipt, err := storeCall(ctx, w, call)
		if err == nil || definitiveCommitError(err) {
			return receipt, err
		}
		last = err
		if attempt < 2 {
			if waitErr := wait(ctx, time.Duration(attempt+1)*10*time.Millisecond); waitErr != nil {
				return durable.SignalReceipt{}, waitErr
			}
		}
	}
	return durable.SignalReceipt{}, fmt.Errorf("accept signal after retries: %w", last)
}

// SignalWithStartOutcome retains recovery provenance through unknown-ACK retries.
// Stores without atomic provenance are unavailable to this remote-facing path.
func (w *Worker) SignalWithStartOutcome(ctx context.Context, request durable.SignalWithStartRequest) (durable.SignalStartOutcome, error) {
	store, ok := w.store.(durable.SignalStartOutcomeStore)
	if !ok {
		return durable.SignalStartOutcome{}, ErrHandlerNotFound
	}
	request.Input, request.Start.Input = bytes.Clone(request.Input), bytes.Clone(request.Start.Input)
	if err := request.Validate(); err != nil {
		return durable.SignalStartOutcome{}, err
	}
	if request.Start.Namespace != w.options.Namespace || request.Start.BuildID != w.options.BuildID || !validID(request.Start.Queue) || !validID(request.Start.WorkflowType) {
		return durable.SignalStartOutcome{}, durable.ErrInvalid
	}
	var outcome durable.SignalStartOutcome
	_, err := w.acceptSignal(ctx, func(callCtx context.Context) (durable.SignalReceipt, error) {
		result, callErr := store.SignalWithStartOutcome(callCtx, request)
		if callErr == nil {
			outcome = result
		}
		return result.Receipt, callErr
	})
	if err != nil {
		return durable.SignalStartOutcome{}, err
	}
	return outcome, nil
}

// SupportsSignalStartOutcome reports atomic acceptance provenance support.
func (w *Worker) SupportsSignalStartOutcome() bool {
	if w == nil {
		return false
	}
	_, ok := w.store.(durable.SignalStartOutcomeStore)
	return ok
}
