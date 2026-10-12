package operator

import (
	"context"

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
	permissions := []resourcetype.PermissionDef{}
	for _, action := range []string{ReadQueryRuntime, RegisterQueryRuntime, VerifyQueryRuntime, RemoveQueryRuntime, FinishQueryRuntime, AbortQueryRuntime, ReadBuild, EnrollRetirement, RegisterBuild, RetireBuild, FinalizeBuild, ResumeBuild, ReadWorker, DrainWorker, Discover, ListExecutions, ReadExecution, ReadHistory, ReadTasks, ReadChain, ReadPayload, QueryWorkflow, ReadAudit, ReadHooks, StartWorkflow, SignalWorkflow, SignalStartWorkflow, CancelWorkflow, CompleteActivity, HeartbeatActivity} {
		permissions = append(permissions, resourcetype.PermissionDef{Name: action, Expression: "operator"})
	}
	return store.CreateResourceType(ctx, &resourcetype.ResourceType{ID: id.NewResourceTypeID(), TenantID: n.TenantID, NamespacePath: n.Namespace, AppID: n.AppID, Name: "dispatch_namespace", Relations: []resourcetype.RelationDef{{Name: "operator", AllowedSubjects: []string{"user", "service", "api_key", "service_acct"}}}, Permissions: permissions})
}
