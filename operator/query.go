package operator

import (
	"context"
	"strconv"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/security"
)

type QueryResult struct {
	durable.Key
	State        durable.State `json:"state"`
	Revision     string        `json:"revision"`
	LastSequence string        `json:"last_sequence"`
	Encoding     string        `json:"encoding"`
	Output       []byte        `json:"output"`
}

func (s *Service) Query(ctx context.Context, p security.Principal, in drt.QueryRequest) (QueryResult, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	if in.Validate() != nil {
		return QueryResult{}, durable.ErrInvalid
	}
	// Select once, then authorize and replay that explicit immutable key.
	if in.Selection != durable.RunExplicit {
		if err := p.Validate(); err != nil {
			return QueryResult{}, s.denied(ctx, p, QueryWorkflow, err)
		}
		e, err := s.store.ResolveExecution(ctx, durable.ExecutionTarget{Key: in.Key, Selection: in.Selection})
		if err != nil {
			if checkErr := s.check(ctx, p, QueryWorkflow, durable.Key{Namespace: in.Namespace, WorkflowID: in.WorkflowID}); checkErr != nil {
				return QueryResult{}, checkErr
			}
			return QueryResult{}, safeError(err)
		}
		in.Key = e.Key
		in.Selection = durable.RunExplicit
	}
	e, err := s.commandRun(ctx, p, QueryWorkflow, in.Key, in.BuildID)
	if err != nil {
		return QueryResult{}, err
	}
	w, err := s.worker(e.Namespace, e.BuildID)
	if err != nil {
		return QueryResult{}, err
	}
	if auditErr := s.audit.RecordDurableRead(ctx, p, QueryWorkflow, "allowed", e.Namespace, e.Key); auditErr != nil {
		return QueryResult{}, auditErr
	}
	result, err := w.QueryExecution(ctx, in)
	if err != nil {
		return QueryResult{}, commandError(err)
	}
	return QueryResult{Key: result.Key, State: result.State, Revision: strconv.FormatInt(result.Revision, 10), LastSequence: strconv.FormatInt(result.LastSequence, 10), Encoding: "base64", Output: result.Output}, nil
}
