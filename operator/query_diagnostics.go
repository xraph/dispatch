package operator

import (
	"context"

	"github.com/xraph/dispatch/durable"
)

func (s *Service) observeQueryRejection(ctx context.Context, target durable.QueryRuntimeTarget, request string, operation durable.LifecycleOperation, err error) {
	if s.queryRejection == nil {
		return
	}
	d, ok := durable.QueryRejectionDetails(err)
	if !ok {
		return
	}
	d.Target, d.RequestID, d.Operation = target, request, operation
	// Reapply bounds after adding command context. The callback receives no aliases.
	safe, _ := durable.QueryRejectionDetails(durable.NewQueryRejection(err, d))
	// Trusted synchronous instrumentation can delay a call, but cannot replace its result.
	defer func() {
		if recovered := recover(); recovered != nil {
			return
		}
	}()
	s.queryRejection(ctx, safe)
}
