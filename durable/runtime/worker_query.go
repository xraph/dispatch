package runtime

import (
	"bytes"
	"context"
	"fmt"

	"github.com/xraph/dispatch/durable"
)

// QueryExecution reads a consistent history prefix and invokes a query against
// compatible workflow code. It never claims a task or publishes a decision.
// The request needs an explicit run and pinned build; the client's queue does
// not restrict reads. Authorize callers before exposing this method remotely.
func (w *Worker) QueryExecution(ctx context.Context, request QueryRequest) (QueryResult, error) {
	if err := request.Validate(); err != nil {
		return QueryResult{}, err
	}
	if request.Namespace != w.options.Namespace || request.BuildID != w.options.BuildID {
		return QueryResult{}, fmt.Errorf("%w: query does not match worker namespace and build", durable.ErrInvalid)
	}
	request.Input = bytes.Clone(request.Input)
	execution, events, err := w.readSnapshot(ctx, request.Key, true)
	if err != nil {
		return QueryResult{}, err
	}
	handler := w.options.Workflows[execution.WorkflowType]
	if handler == nil {
		return QueryResult{}, fmt.Errorf("%w: workflow %q", ErrHandlerNotFound, execution.WorkflowType)
	}
	return evaluateQuery(ctx, execution, events, handler, request)
}
