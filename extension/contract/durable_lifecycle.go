package contract

import (
	"context"

	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/security"
)

func lifecycleDurableAction(intent string) string {
	switch intent {
	case "durable.compatibility":
		return operator.ReadBuild
	case "durable.build":
		return operator.ReadBuild
	case "durable.retirementEnroll":
		return operator.EnrollRetirement
	case "durable.buildRegister":
		return operator.RegisterBuild
	case "durable.buildRetire":
		return operator.RetireBuild
	case "durable.buildFinalize":
		return operator.FinalizeBuild
	case "durable.buildResume":
		return operator.ResumeBuild
	case "durable.workerStatus":
		return operator.ReadWorker
	case "durable.workerDrain":
		return operator.DrainWorker
	case "durable.workerDrainReceipt":
		return operator.ReadWorker
	}
	return ""
}
func lifecycleDurableKind(intent string) fc.Kind {
	switch intent {
	case "durable.compatibility":
		return fc.KindQuery
	case "durable.build":
		return fc.KindQuery
	case "durable.retirementEnroll":
		return fc.KindCommand
	case "durable.buildRegister":
		return fc.KindCommand
	case "durable.buildRetire":
		return fc.KindCommand
	case "durable.buildFinalize":
		return fc.KindCommand
	case "durable.buildResume":
		return fc.KindCommand
	case "durable.workerStatus":
		return fc.KindQuery
	case "durable.workerDrain":
		return fc.KindCommand
	case "durable.workerDrainReceipt":
		return fc.KindQuery
	}
	return ""
}
func lifecycleDurableBindings(deps Deps) []binding {
	return []binding{
		query("durable.compatibility", durableHandle(deps, "durable.compatibility", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.NamespaceLifecycleInput) (operator.Compatibility, error) {
			return s.Compatibility(ctx, p, in)
		})),
		query("durable.build", durableHandle(deps, "durable.build", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.BuildInput) (operator.BuildLifecycle, error) {
			return s.BuildLifecycle(ctx, p, in)
		})),
		command(deps, "durable.retirementEnroll", durableHandle(deps, "durable.retirementEnroll", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.EnrollmentInput) (operator.LifecycleAcceptance, error) {
			return s.EnrollRetirement(ctx, p, in)
		})),
		command(deps, "durable.buildRegister", durableHandle(deps, "durable.buildRegister", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.RegisterBuildInput) (operator.LifecycleAcceptance, error) {
			return s.RegisterBuild(ctx, p, in)
		})),
		command(deps, "durable.buildRetire", durableHandle(deps, "durable.buildRetire", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.BuildRetirementInput) (operator.LifecycleAcceptance, error) {
			return s.RetireBuild(ctx, p, in)
		})),
		command(deps, "durable.buildFinalize", durableHandle(deps, "durable.buildFinalize", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.BuildRetirementInput) (operator.LifecycleAcceptance, error) {
			return s.FinalizeBuild(ctx, p, in)
		})),
		command(deps, "durable.buildResume", durableHandle(deps, "durable.buildResume", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.BuildRetirementInput) (operator.LifecycleAcceptance, error) {
			return s.ResumeBuild(ctx, p, in)
		})),
		query("durable.workerStatus", durableHandle(deps, "durable.workerStatus", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.WorkerInput) (operator.WorkerObservation, error) {
			return s.WorkerStatus(ctx, p, in)
		})),
		command(deps, "durable.workerDrain", durableHandle(deps, "durable.workerDrain", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.WorkerDrainInput) (operator.WorkerDrainAcceptance, error) {
			return s.RequestWorkerDrain(ctx, p, in)
		})),
		query("durable.workerDrainReceipt", durableHandle(deps, "durable.workerDrainReceipt", func(ctx context.Context, s *operator.Service, p security.Principal, in operator.WorkerDrainInput) (operator.WorkerDrainAcceptance, error) {
			return s.WorkerDrainReceipt(ctx, p, in)
		})),
	}
}
