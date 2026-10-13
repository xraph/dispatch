package contract

import (
	"context"

	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/security"
)

func queryGuardDurableAction(intent string) string {
	switch intent {
	case "durable.queryRemovalRevalidate":
		return operator.RevalidateQueryRuntime
	case "durable.queryRemovalRevalidation":
		return operator.ReadQueryRevalidation
	case "durable.queryRemovalCommit":
		return operator.CommitQueryRemoval
	case "durable.queryRemovalCommitStatus":
		return operator.ReadQueryCommit
	case "durable.queryRemovalGuardClose":
		return operator.CloseQueryGuard
	case "durable.queryRemovalGuardReceipt":
		return operator.ReadQueryGuard
	}
	return ""
}
func queryGuardDurableKind(intent string) fc.Kind {
	switch intent {
	case "durable.queryRemovalRevalidate":
		return fc.KindCommand
	case "durable.queryRemovalRevalidation":
		return fc.KindQuery
	case "durable.queryRemovalCommit":
		return fc.KindCommand
	case "durable.queryRemovalCommitStatus":
		return fc.KindQuery
	case "durable.queryRemovalGuardClose":
		return fc.KindCommand
	case "durable.queryRemovalGuardReceipt":
		return fc.KindQuery
	}
	return ""
}
func queryGuardDurableBindings(deps Deps) []binding {
	return []binding{
		command(deps, "durable.queryRemovalRevalidate", durableHandle(deps, "durable.queryRemovalRevalidate", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.QueryRevalidationInput) (durable.QueryRemovalRevalidationReceipt, error) {
			return s.RevalidateQueryRuntimeRemoval(ctx, p, in)
		})),
		query("durable.queryRemovalRevalidation", durableHandle(deps, "durable.queryRemovalRevalidation", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.QueryGuardReadInput) (durable.QueryRemovalRevalidationReceipt, error) {
			return s.ReadQueryRemovalRevalidation(ctx, p, in)
		})),
		command(deps, "durable.queryRemovalCommit", durableHandle(deps, "durable.queryRemovalCommit", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.QueryCommitInput) (operator.QueryGuardAcceptance, error) {
			return s.BindQueryRemovalCommit(ctx, p, in)
		})),
		query("durable.queryRemovalCommitStatus", durableHandle(deps, "durable.queryRemovalCommitStatus", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.QueryGuardReadInput) (operator.QueryCommitStatus, error) {
			return s.CheckQueryRemovalCommit(ctx, p, in)
		})),
		command(deps, "durable.queryRemovalGuardClose", durableHandle(deps, "durable.queryRemovalGuardClose", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.QueryGuardCloseInput) (operator.QueryGuardAcceptance, error) {
			return s.CloseQueryRemovalGuard(ctx, p, in)
		})),
		query("durable.queryRemovalGuardReceipt", durableHandle(deps, "durable.queryRemovalGuardReceipt", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.QueryGuardReceiptInput) (operator.QueryGuardAcceptance, error) {
			return s.ReadQueryRemovalGuardReceipt(ctx, p, in)
		})),
	}
}
