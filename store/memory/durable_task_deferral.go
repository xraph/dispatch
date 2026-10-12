package memory

import (
	"context"
	"time"

	"github.com/xraph/dispatch/durable"
)

type taskDeferralKey struct {
	key durable.Key
	id  string
}

var _ durable.WorkflowTaskDeferralStore = (*Store)(nil)

func (m *Store) DeferWorkflowTask(ctx context.Context, r durable.WorkflowTaskDeferralRequest) (durable.WorkflowTaskDeferralReceipt, error) {
	if err := r.Validate(); err != nil {
		return durable.WorkflowTaskDeferralReceipt{}, err
	}
	metadata := durable.AuditMetadataFromContext(ctx)
	metadata.RequestID = r.RequestID
	metadata.ReasonCode = "target_" + string(r.TargetState)
	ctx = durable.WithAuditMetadata(ctx, metadata)
	digest, err := durable.Fingerprint("workflow-task.defer.v1", r)
	if err != nil {
		return durable.WorkflowTaskDeferralReceipt{}, err
	}
	return mutateAudited(ctx, m, r.Namespace, func(c *Store) (durable.WorkflowTaskDeferralReceipt, error) {
		var zero durable.WorkflowTaskDeferralReceipt
		record := c.executions[r.Key]
		if record == nil {
			return zero, durable.ErrNotFound
		}
		if saved, ok := record.receipts[r.RequestID]; ok {
			if _, replayErr := replayReceipt(saved, digest); replayErr != nil {
				return zero, replayErr
			}
			response, found := c.taskDeferralReceipts[taskDeferralKey{r.Key, r.RequestID}]
			if !found || response.Receipt != saved.value || response.PolicyVersion != durable.WorkflowTaskDeferralPolicyVersion {
				return zero, durable.ErrInvalid
			}
			return response.WorkflowTaskDeferralReceipt, nil
		}
		n, ok := c.namespaces[r.Namespace]
		if !ok || !n.RequireAudit || !c.retirementNamespaces[r.Namespace].Enrolled {
			return zero, durable.ErrWriterCompatibility
		}
		task := record.tasks[r.Token.TaskID]
		if task == nil {
			return zero, durable.ErrLeaseLost
		}
		target := durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: n.InstallationID, Namespace: r.Namespace}, BuildID: r.TargetBuildID}
		build, ok := c.buildAdmissions[target]
		if !ok {
			build = durable.BuildAdmission{BuildTarget: target, State: string(durable.DeferralUnregistered)}
		}
		key := taskDeferralKey{r.Key, r.Token.TaskID}
		next, response, prepareErr := durable.PrepareWorkflowTaskDeferral(record.execution, task.Task, r, build, c.taskDeferrals[key], durable.Timestamp(time.Now()))
		if prepareErr != nil {
			return zero, prepareErr
		}
		task.Task = next
		record.receipts[r.RequestID] = durableReceipt{action: "workflow.task_deferred", digest: digest, intent: digest, value: response.Receipt}
		c.taskDeferrals[key] = response
		c.taskDeferralReceipts[taskDeferralKey{r.Key, r.RequestID}] = response
		return response.WorkflowTaskDeferralReceipt, nil
	})
}
func (m *Store) GetWorkflowTaskDeferral(ctx context.Context, key durable.Key, taskID string) (durable.WorkflowTaskDeferral, error) {
	if key.Validate() != nil || durable.ValidateTaskID(taskID) != nil {
		return durable.WorkflowTaskDeferral{}, durable.ErrInvalid
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return durable.WorkflowTaskDeferral{}, err
	}
	d, ok := m.taskDeferrals[taskDeferralKey{key, taskID}]
	if !ok {
		return d, durable.ErrNotFound
	}
	record := m.executions[key]
	if record == nil || record.tasks[taskID] == nil {
		return d, durable.ErrInvalid
	}
	d.Active = d.IsActive(record.execution, record.tasks[taskID].Task)
	return d, nil
}
