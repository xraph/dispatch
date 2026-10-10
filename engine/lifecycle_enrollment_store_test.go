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

func (s *strictLifecycleStore) DeferWorkflowTask(ctx context.Context, r durable.WorkflowTaskDeferralRequest) (durable.WorkflowTaskDeferralReceipt, error) {
	defer s.call("DeferWorkflowTask")()
	return s.base.DeferWorkflowTask(ctx, r)
}
func (s *strictLifecycleStore) GetWorkflowTaskDeferral(ctx context.Context, key durable.Key, taskID string) (durable.WorkflowTaskDeferral, error) {
	defer s.call("GetWorkflowTaskDeferral")()
	return s.base.GetWorkflowTaskDeferral(ctx, key, taskID)
}

func (s *strictLifecycleStore) RegisterQueryRuntime(ctx context.Context, r durable.RegisterQueryRuntimeRequest) (durable.LifecycleReceipt, error) {
	defer s.call("RegisterQueryRuntime")()
	return s.base.RegisterQueryRuntime(ctx, r)
}

func (s *strictLifecycleStore) RecordQueryRuntimeVerification(ctx context.Context, r durable.VerifyQueryRuntimeRequest) (durable.LifecycleReceipt, error) {
	defer s.call("RecordQueryRuntimeVerification")()
	return s.base.RecordQueryRuntimeVerification(ctx, r)
}

func (s *strictLifecycleStore) InspectQueryRetention(ctx context.Context, r durable.BuildTarget) (durable.QueryRetentionFacts, error) {
	defer s.call("InspectQueryRetention")()
	return s.base.InspectQueryRetention(ctx, r)
}

func (s *strictLifecycleStore) ListQueryRuntimes(ctx context.Context, r durable.QueryRuntimeList) (durable.QueryRuntimePage, error) {
	defer s.call("ListQueryRuntimes")()
	return s.base.ListQueryRuntimes(ctx, r)
}

func (s *strictLifecycleStore) BeginQueryRuntimeRemoval(ctx context.Context, r durable.BeginQueryRemovalRequest) (durable.LifecycleReceipt, error) {
	defer s.call("BeginQueryRuntimeRemoval")()
	return s.base.BeginQueryRuntimeRemoval(ctx, r)
}

func (s *strictLifecycleStore) CheckQueryRuntimeRemoval(ctx context.Context, r durable.QueryRemovalFence) (durable.QueryRemovalFacts, error) {
	defer s.call("CheckQueryRuntimeRemoval")()
	return s.base.CheckQueryRuntimeRemoval(ctx, r)
}

func (s *strictLifecycleStore) FinishQueryRuntimeRemoval(ctx context.Context, r durable.FinishQueryRemovalRequest) (durable.LifecycleReceipt, error) {
	defer s.call("FinishQueryRuntimeRemoval")()
	return s.base.FinishQueryRuntimeRemoval(ctx, r)
}

func (s *strictLifecycleStore) AbortQueryRuntimeRemoval(ctx context.Context, r durable.AbortQueryRemovalRequest) (durable.LifecycleReceipt, error) {
	defer s.call("AbortQueryRuntimeRemoval")()
	return s.base.AbortQueryRuntimeRemoval(ctx, r)
}
