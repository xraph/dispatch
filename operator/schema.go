package operator

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/xraph/warden"

	"github.com/xraph/warden/id"
	"github.com/xraph/warden/resourcetype"

	"github.com/xraph/dispatch/durable"
)

// RegisterNamespaceSchema declares the known actions without granting them.
// Hosts call this during policy provisioning, with persisted namespace ownership.
func RegisterNamespaceSchema(ctx context.Context, store resourcetype.Store, n durable.NamespaceRecord) error {
	if store == nil || n.Validate() != nil {
		return durable.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	permissions := []resourcetype.PermissionDef{}
	for _, action := range []string{RevalidateQueryRuntime, ReadQueryRevalidation, CommitQueryRemoval, ReadQueryCommit, CloseQueryGuard, ReadQueryGuard, ReadQueryRuntime, RegisterQueryRuntime, VerifyQueryRuntime, RemoveQueryRuntime, FinishQueryRuntime, AbortQueryRuntime, ReadBuild, EnrollRetirement, RegisterBuild, RetireBuild, FinalizeBuild, ResumeBuild, ReadWorker, DrainWorker, Discover, ListExecutions, ReadExecution, ReadHistory, ReadTasks, ReadChain, ReadPayload, QueryWorkflow, ReadAudit, ReadHooks, StartWorkflow, SignalWorkflow, SignalStartWorkflow, CancelWorkflow, CompleteActivity, HeartbeatActivity} {
		permissions = append(permissions, resourcetype.PermissionDef{Name: action, Expression: "operator"})
	}
	expected := &resourcetype.ResourceType{ID: id.NewResourceTypeID(), TenantID: n.TenantID, NamespacePath: n.Namespace, AppID: n.AppID, Name: "dispatch_namespace", Relations: []resourcetype.RelationDef{{Name: "operator", AllowedSubjects: []string{"user", "service", "api_key", "service_acct"}}}, Permissions: permissions}

	existing, err := store.GetResourceTypeByName(ctx, n.TenantID, n.Namespace, expected.Name)
	if err == nil {
		return checkNamespaceSchema(existing, expected)
	}
	if !errors.Is(err, warden.ErrResourceTypeNotFound) {
		return err
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	if err = store.CreateResourceType(ctx, expected); !errors.Is(err, warden.ErrAlreadyExists) {
		return err
	}
	// A concurrent provisioner may have created the type after our read.
	existing, err = store.GetResourceTypeByName(ctx, n.TenantID, n.Namespace, expected.Name)
	if err != nil {
		return err
	}
	return checkNamespaceSchema(existing, expected)
}

func checkNamespaceSchema(existing, expected *resourcetype.ResourceType) error {
	// Database identity and audit timestamps are assigned by the store. Reuse
	// never changes schema ownership, relation subjects, or permission expressions.
	if existing == nil || existing.ID.IsNil() || existing.TenantID != expected.TenantID ||
		existing.NamespacePath != expected.NamespacePath || existing.AppID != expected.AppID ||
		existing.Name != expected.Name || existing.Description != expected.Description ||
		len(existing.Metadata) != 0 ||
		!slices.EqualFunc(existing.Relations, expected.Relations, func(a, b resourcetype.RelationDef) bool {
			return a.Name == b.Name && slices.Equal(a.AllowedSubjects, b.AllowedSubjects)
		}) || !slices.Equal(existing.Permissions, expected.Permissions) {
		return fmt.Errorf("operator: namespace schema differs from the required definition: %w", durable.ErrInvalid)
	}
	return nil
}
