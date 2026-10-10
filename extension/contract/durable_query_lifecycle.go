package contract

import (
	"context"

	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/security"
)

func queryLifecycleDurableAction(intent string) string {
	switch intent {
	case "durable.queryRuntime":
		return operator.ReadQueryRuntime
	case "durable.queryRuntimeRegister":
		return operator.RegisterQueryRuntime
	case "durable.queryRuntimeVerify":
		return operator.VerifyQueryRuntime
	case "durable.queryRuntimeRemove", "durable.queryRuntimeRemovalCheck":
		return operator.RemoveQueryRuntime
	case "durable.queryRuntimeFinish":
		return operator.FinishQueryRuntime
	case "durable.queryRuntimeAbort":
		return operator.AbortQueryRuntime
	}
	return ""
}

func queryLifecycleDurableKind(intent string) fc.Kind {
	switch intent {
	case "durable.queryRuntime", "durable.queryRuntimeRemovalCheck":
		return fc.KindQuery
	case "durable.queryRuntimeRegister", "durable.queryRuntimeVerify", "durable.queryRuntimeRemove", "durable.queryRuntimeFinish", "durable.queryRuntimeAbort":
		return fc.KindCommand
	}
	return ""
}

func queryLifecycleDurableBindings(deps Deps) []binding {
	return []binding{
		query("durable.queryRuntime", durableHandle(deps, "durable.queryRuntime", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.QueryRuntimeInput) (operator.QueryRuntime, error) {
			return s.QueryRuntime(ctx, p, in)
		})),
		command(deps, "durable.queryRuntimeRegister", durableHandle(deps, "durable.queryRuntimeRegister", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.QueryRuntimeCommand) (operator.QueryRuntimeAcceptance, error) {
			return s.RegisterQueryRuntime(ctx, p, in)
		})),
		command(deps, "durable.queryRuntimeVerify", durableHandle(deps, "durable.queryRuntimeVerify", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.QueryRuntimeCommand) (operator.QueryRuntimeAcceptance, error) {
			return s.VerifyQueryRuntime(ctx, p, in)
		})),
		command(deps, "durable.queryRuntimeRemove", durableHandle(deps, "durable.queryRuntimeRemove", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.QueryRuntimeCommand) (operator.QueryRemovalAcceptance, error) {
			return s.BeginQueryRuntimeRemoval(ctx, p, in)
		})),
		query("durable.queryRuntimeRemovalCheck", durableHandle(deps, "durable.queryRuntimeRemovalCheck", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.QueryReservationInput) (operator.QueryRemovalCheck, error) {
			return s.CheckQueryRuntimeRemoval(ctx, p, in)
		})),
		command(deps, "durable.queryRuntimeFinish", durableHandle(deps, "durable.queryRuntimeFinish", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.QueryRemovalInput) (operator.QueryRuntimeAcceptance, error) {
			return s.FinishQueryRuntimeRemoval(ctx, p, in)
		})),
		command(deps, "durable.queryRuntimeAbort", durableHandle(deps, "durable.queryRuntimeAbort", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.QueryRemovalInput) (operator.QueryRuntimeAcceptance, error) {
			return s.AbortQueryRuntimeRemoval(ctx, p, in)
		})),
	}
}
