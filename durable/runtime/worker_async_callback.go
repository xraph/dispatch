package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xraph/dispatch/durable"
)

func (w *Worker) validateAsyncHandle(handle AsyncActivityHandle) error {
	if err := handle.Validate(); err != nil {
		return err
	}
	if handle.Key.Namespace != w.options.Namespace || handle.BuildID != w.options.BuildID {
		return fmt.Errorf("%w: callback does not match worker routing", durable.ErrInvalid)
	}
	return nil
}

// CompleteAsyncActivity publishes a result or failure using the saved retry
// policy. A compatible worker needs no activity handler. Accepted request IDs
// recover their original receipt after retry, timeout, state advancement or closure.
// This trusted Go API requires caller authorization before remote exposure.
func (w *Worker) CompleteAsyncActivity(ctx context.Context, client AsyncCompletionRequest) (durable.Receipt, error) {
	client.Output = bytes.Clone(client.Output)
	if client.Failure != nil {
		copyFailure := *client.Failure
		client.Failure = &copyFailure
	}
	if err := w.validateAsyncHandle(client.Handle); err != nil {
		return durable.Receipt{}, err
	}
	if !validID(client.RequestID) || (client.Failure != nil && (len(client.Output) != 0 || !validFailure(client.Failure))) {
		return durable.Receipt{}, fmt.Errorf("%w: invalid asynchronous completion request", durable.ErrInvalid)
	}
	intent, err := durable.Fingerprint("runtime.async.complete.v1", client)
	if err != nil {
		return durable.Receipt{}, err
	}
	query := durable.ReceiptRequest{Key: client.Handle.Key, RequestID: "async-result:" + client.RequestID, IntentDigest: intent}
	if receipt, found, lookupErr := w.lookupIntent(ctx, query); lookupErr != nil || found {
		return receipt, lookupErr
	}
	receipt, err := w.completeAsync(ctx, client, query)
	if err == nil {
		return receipt, nil
	}
	// Another caller may have accepted this exact intent between lookup and
	// observation, including when the workflow already closed or retried.
	if accepted, found, lookupErr := w.lookupIntent(ctx, query); lookupErr != nil || found {
		return accepted, lookupErr
	}
	return durable.Receipt{}, err
}

type intentLookup struct {
	receipt durable.Receipt
	found   bool
}

func (w *Worker) lookupIntent(ctx context.Context, query durable.ReceiptRequest) (durable.Receipt, bool, error) {
	result, err := storeCall(ctx, w, func(callCtx context.Context) (intentLookup, error) {
		receipt, found, lookupErr := w.store.LookupReceipt(callCtx, query)
		return intentLookup{receipt: receipt, found: found}, lookupErr
	})
	return result.receipt, result.found, err
}

func (w *Worker) completeAsync(ctx context.Context, client AsyncCompletionRequest, query durable.ReceiptRequest) (durable.Receipt, error) {
	handle := client.Handle
	task, err := storeCall(ctx, w, func(callCtx context.Context) (durable.Task, error) {
		return w.store.GetTask(callCtx, handle.Key, handle.Token.TaskID)
	})
	if err != nil {
		return durable.Receipt{}, err
	}
	if task.Done || task.Token() != handle.Token {
		return durable.Receipt{}, durable.ErrLeaseLost
	}
	hash, err := durable.HashAsyncSecret(handle.Secret)
	if err != nil {
		return durable.Receipt{}, err
	}
	// Reject a copied task projection before parsing history. The store repeats
	// proof validation under its transaction locks at publication.
	if err = durable.CheckReceiptIntent(task.AsyncKeyHash, hash); err != nil {
		return durable.Receipt{}, durable.ErrLeaseLost
	}
	var payload taskPayload
	if decodeErr := decode(task.Payload, &payload); decodeErr != nil {
		return durable.Receipt{}, decodeErr
	}
	if payload.Version != 1 || payload.Command.validate() != nil || payload.Command.Version != 2 || payload.Command.Kind != durable.TaskActivity ||
		task.Kind != durable.TaskActivity || task.ID != fmt.Sprintf("command:%d", payload.Command.Index) || !validID(payload.WorkflowQueue) {
		return durable.Receipt{}, fmt.Errorf("%w: invalid asynchronous task payload", ErrHistory)
	}
	_, history, err := w.effectSnapshot(ctx, task, payload.Command)
	if err != nil {
		return durable.Receipt{}, err
	}
	prior := history.attempts[payload.Command.ID]
	if prior.handoff == nil || prior.failed || prior.value.Epoch != task.Epoch {
		return durable.Receipt{}, w.effectConflict(ctx, task, "callback does not match a deferred attempt")
	}
	outcome := Outcome{Version: 2, CommandID: payload.Command.ID, Attempt: prior.value.Attempt, Output: client.Output, Failure: client.Failure}
	return w.publishActivity(ctx, task, payload, task.Epoch, outcome, &activityCommitIdentity{requestID: query.RequestID, intent: query.IntentDigest, secret: handle.Secret})
}

// HeartbeatAsyncActivity persists external progress without renewing a worker
// lease. Callers coordinate consecutive sequences starting after the handle's
// initial sequence. An exact retry returns the original receipt after closure.
func (w *Worker) HeartbeatAsyncActivity(ctx context.Context, client AsyncHeartbeatRequest) (durable.Receipt, error) {
	if err := w.validateAsyncHandle(client.Handle); err != nil {
		return durable.Receipt{}, err
	}
	if !validID(client.RequestID) {
		return durable.Receipt{}, fmt.Errorf("%w: heartbeat request ID is required", durable.ErrInvalid)
	}
	request := durable.HeartbeatRequest{Key: client.Handle.Key, RequestID: "async-heartbeat:" + client.RequestID, Token: client.Handle.Token,
		AsyncSecret: client.Handle.Secret, Sequence: client.Sequence, Progress: bytes.Clone(client.Details)}
	if err := request.Validate(); err != nil {
		return durable.Receipt{}, err
	}
	// The handle and callback worker can both carry a changed build label. Check
	// the run's immutable pin even when replaying a receipt after closure.
	execution, err := storeCall(ctx, w, func(callCtx context.Context) (durable.Execution, error) {
		return w.store.GetExecution(callCtx, client.Handle.Key)
	})
	if err != nil {
		return durable.Receipt{}, err
	}
	if execution.BuildID != client.Handle.BuildID {
		return durable.Receipt{}, fmt.Errorf("%w: callback build does not match execution", durable.ErrInvalid)
	}
	var last error
	for attempt := range 3 {
		if ctx.Err() != nil {
			return durable.Receipt{}, context.Cause(ctx)
		}
		receipt, err := storeCall(ctx, w, func(callCtx context.Context) (durable.Receipt, error) {
			return w.store.RecordHeartbeat(callCtx, request)
		})
		if err == nil || normalContention(err) || errors.Is(err, durable.ErrInvalid) || errors.Is(err, durable.ErrRequestConflict) || errors.Is(err, durable.ErrNotFound) {
			return receipt, err
		}
		last = err
		if attempt < 2 {
			if err := wait(ctx, time.Duration(attempt+1)*10*time.Millisecond); err != nil {
				return durable.Receipt{}, err
			}
		}
	}
	return durable.Receipt{}, fmt.Errorf("persist asynchronous heartbeat after retries: %w", last)
}
