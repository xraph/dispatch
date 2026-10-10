package engine_test

import (
	"context"

	"github.com/xraph/dispatch/durable"
)

func (s *strictLifecycleStore) InspectCompatibility(ctx context.Context, r durable.NamespaceTarget) (durable.CompatibilityFacts, error) {
	defer s.call("InspectCompatibility")()
	return s.base.InspectCompatibility(ctx, r)
}
func (s *strictLifecycleStore) EnrollRetirement(ctx context.Context, r durable.RetirementEnrollmentRequest) (durable.LifecycleReceipt, error) {
	defer s.call("EnrollRetirement")()
	return s.base.EnrollRetirement(ctx, r)
}
func (s *strictLifecycleStore) LookupLifecycleReceipt(ctx context.Context, r durable.LifecycleReceiptLookup) (durable.LifecycleReceipt, error) {
	defer s.call("LookupLifecycleReceipt")()
	return s.base.LookupLifecycleReceipt(ctx, r)
}

func (s *strictLifecycleStore) RegisterBuild(ctx context.Context, r durable.RegisterBuildRequest) (durable.LifecycleReceipt, error) {
	defer s.call("RegisterBuild")()
	return s.base.RegisterBuild(ctx, r)
}
func (s *strictLifecycleStore) InspectBuildLifecycle(ctx context.Context, r durable.BuildTarget) (durable.BuildLifecycleFacts, error) {
	defer s.call("InspectBuildLifecycle")()
	return s.base.InspectBuildLifecycle(ctx, r)
}
func (s *strictLifecycleStore) BeginBuildRetirement(ctx context.Context, r durable.BuildRetirementRequest) (durable.LifecycleReceipt, error) {
	defer s.call("BeginBuildRetirement")()
	return s.base.BeginBuildRetirement(ctx, r)
}
func (s *strictLifecycleStore) FinalizeBuildRetirement(ctx context.Context, r durable.BuildRetirementRequest) (durable.LifecycleReceipt, error) {
	defer s.call("FinalizeBuildRetirement")()
	return s.base.FinalizeBuildRetirement(ctx, r)
}
func (s *strictLifecycleStore) AbortBuildRetirement(ctx context.Context, r durable.BuildRetirementRequest) (durable.LifecycleReceipt, error) {
	defer s.call("AbortBuildRetirement")()
	return s.base.AbortBuildRetirement(ctx, r)
}
