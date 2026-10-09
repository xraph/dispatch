package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/xraph/dispatch/durable"
)

// Query errors distinguish absent registration, prohibited SDK use and panics.
var (
	ErrQueryNotFound = errors.New("durable runtime: query handler not registered at this snapshot")
	ErrQueryMutation = errors.New("durable runtime: workflow operations are forbidden in a query")
	ErrQueryPanic    = errors.New("durable runtime: query handler panicked")
)

// QueryFunc reads reconstructed workflow state. Return promptly without external
// effects or asynchronous workflow operations. Each call uses a fresh replay.
type QueryFunc func([]byte) ([]byte, error)

// QueryRequest identifies a query on one explicit run and its pinned build.
// Queries are observations and do not use mutation request IDs or receipts.
type QueryRequest struct {
	durable.Key
	BuildID string `json:"build_id"`
	Name    string `json:"name"`
	Input   []byte `json:"input,omitempty"`
}

// QueryResult identifies the persisted snapshot used to reconstruct the answer.
// State describes that saved projection, even when replay observes a signal
// whose consumption has not yet been committed by a workflow worker.
type QueryResult struct {
	durable.Key
	State        durable.State `json:"state"`
	Revision     int64         `json:"revision"`
	LastSequence int64         `json:"last_sequence"`
	Output       []byte        `json:"output,omitempty"`
}

// Validate checks the explicit run, pinned build, query name and input bound.
func (r QueryRequest) Validate() error {
	if err := r.Key.Validate(); err != nil {
		return err
	}
	if !validIdentifier(r.BuildID, 512) || !validID(r.Name) || len(r.Input) > 1<<20 {
		return fmt.Errorf("%w: invalid query build, name or input size", durable.ErrInvalid)
	}
	return nil
}

// SetQueryHandler registers a name on this replay instance. Register handlers
// before an unresolved future can yield. Invalid or duplicate registrations fail
// evaluation; they do not become workflow application failures.
func (w *Workflow) SetQueryHandler(name string, handler QueryFunc) {
	w.checkOperation()
	if !validID(name) || handler == nil || w.queries[name] != nil {
		w.stop(fmt.Errorf("%w: invalid or duplicate query registration", durable.ErrInvalid))
	}
	if w.queries == nil {
		w.queries = make(map[string]QueryFunc)
	}
	w.queries[name] = handler
}

// EvaluateQuery reconstructs a complete immutable history prefix and invokes a
// named query on its private workflow instance. It never returns a publishable
// decision or performs store operations. Arbitrary Go side effects and blocking
// cannot be sandboxed; keep query handlers read-only and prompt.
func EvaluateQuery(execution durable.Execution, events []durable.Event, handler WorkflowFunc, request QueryRequest) (QueryResult, error) {
	return evaluateQuery(context.Background(), execution, events, handler, request)
}

func evaluateQuery(ctx context.Context, execution durable.Execution, events []durable.Event, handler WorkflowFunc, request QueryRequest) (QueryResult, error) {
	if ctx.Err() != nil {
		return QueryResult{}, context.Cause(ctx)
	}
	if err := request.Validate(); err != nil {
		return QueryResult{}, err
	}
	if request.Key != execution.Key || request.BuildID != execution.BuildID || execution.Revision < 1 {
		return QueryResult{}, fmt.Errorf("%w: query does not match execution snapshot", durable.ErrInvalid)
	}
	input := bytes.Clone(request.Input)
	_, w, err := evaluateWorkflow(execution, events, handler)
	if err != nil {
		return QueryResult{}, err
	}
	if ctx.Err() != nil {
		return QueryResult{}, context.Cause(ctx)
	}
	query := w.queries[request.Name]
	if query == nil {
		return QueryResult{}, fmt.Errorf("%w: %s", ErrQueryNotFound, request.Name)
	}
	w.querying = true
	output, err := invokeQuery(w, query, input)
	if ctx.Err() != nil {
		return QueryResult{}, context.Cause(ctx)
	}
	if w.fault != nil {
		return QueryResult{}, w.fault
	}
	if err != nil {
		return QueryResult{}, err
	}
	if len(output) > 1<<20 {
		return QueryResult{}, fmt.Errorf("%w: query output exceeds 1 MiB", durable.ErrInvalid)
	}
	return QueryResult{Key: execution.Key, State: execution.State, Revision: execution.Revision, LastSequence: execution.LastSequence, Output: bytes.Clone(output)}, nil
}

func invokeQuery(w *Workflow, handler QueryFunc, input []byte) (output []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			output = nil
			if w.fault != nil {
				err = w.fault
			} else {
				err = fmt.Errorf("%w: %v", ErrQueryPanic, recovered)
			}
		}
	}()
	return handler(input)
}
