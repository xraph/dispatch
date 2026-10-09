package runtime

import (
	"bytes"
	"context"
	"fmt"

	"github.com/xraph/dispatch/durable"
)

// QueryExecution reads a consistent history prefix and invokes a query against
// compatible workflow code. It never claims a task or publishes a decision.
// The request needs a run selector and pinned build; the client's queue does
// not restrict reads. Authorize callers before exposing this method remotely.
func (w *Worker) QueryExecution(ctx context.Context, request QueryRequest) (QueryResult, error) {
	if err := request.Validate(); err != nil {
		return QueryResult{}, err
	}
	if request.Namespace != w.options.Namespace || request.BuildID != w.options.BuildID {
		return QueryResult{}, fmt.Errorf("%w: query does not match worker namespace and build", durable.ErrInvalid)
	}
	request.Input = bytes.Clone(request.Input)
	execution, events, err := w.querySnapshot(ctx, request)
	if err != nil {
		return QueryResult{}, err
	}
	handler := w.options.Workflows[execution.WorkflowType]
	if handler == nil {
		return QueryResult{}, fmt.Errorf("%w: workflow %q", ErrHandlerNotFound, execution.WorkflowType)
	}
	request.Key, request.Selection = execution.Key, durable.RunExplicit
	return evaluateQuery(ctx, execution, events, handler, request)
}

func (w *Worker) querySnapshot(ctx context.Context, request QueryRequest) (durable.Execution, []durable.Event, error) {
	if request.Selection == durable.RunExplicit {
		return w.readSnapshot(ctx, request.Key, true)
	}
	execution, err := storeCall(ctx, w, func(callCtx context.Context) (durable.Execution, error) {
		return w.store.ResolveExecution(callCtx, durable.ExecutionTarget{Key: request.Key, Selection: request.Selection})
	})
	if err != nil {
		return durable.Execution{}, nil, err
	}
	key := request.Key
	key.RunID = execution.RunID
	if execution.Validate() != nil || execution.Key != key || (request.Selection == durable.RunCurrent && execution.State != durable.StateRunning) {
		return durable.Execution{}, nil, fmt.Errorf("%w: resolved execution does not match query target", durable.ErrInvalid)
	}
	return w.readSnapshotHistory(ctx, key, execution, true)
}
