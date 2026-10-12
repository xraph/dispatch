package relayhook

import (
	"context"

	"github.com/xraph/relay"
	"github.com/xraph/relay/catalog"
)

// Dispatch lifecycle event types. Each constant maps to one ext lifecycle
// hook and is used as the event.Event.Type when sending via Relay.
const (
	EventJobEnqueued           = "dispatch.job.enqueued"
	EventJobStarted            = "dispatch.job.started"
	EventJobCompleted          = "dispatch.job.completed"
	EventJobFailed             = "dispatch.job.failed"
	EventJobRetrying           = "dispatch.job.retrying"
	EventJobDLQ                = "dispatch.job.dlq"
	EventJobCancelled          = "dispatch.job.cancelled"
	EventWorkflowStarted       = "dispatch.workflow.started"
	EventWorkflowStepCompleted = "dispatch.workflow.step_completed"
	EventWorkflowStepFailed    = "dispatch.workflow.step_failed"
	EventWorkflowCompleted     = "dispatch.workflow.completed"
	EventWorkflowFailed        = "dispatch.workflow.failed"
	EventCronFired             = "dispatch.cron.fired"
)

// Operator action event types. Each constant maps to one ext.ActionKind.
// They sit under dispatch.operator so a subscriber can tell an operator's
// cancel apart from the dispatch.job.cancelled lifecycle event it causes.
const (
	EventOperatorJobCancelled     = "dispatch.operator.job_cancelled"
	EventOperatorJobRetried       = "dispatch.operator.job_retried"
	EventOperatorDLQReplayed      = "dispatch.operator.dlq_replayed"
	EventOperatorDLQDeleted       = "dispatch.operator.dlq_deleted"
	EventOperatorDLQPurged        = "dispatch.operator.dlq_purged"
	EventOperatorCronEnabled      = "dispatch.operator.cron_enabled"
	EventOperatorCronDisabled     = "dispatch.operator.cron_disabled"
	EventOperatorCronDeleted      = "dispatch.operator.cron_deleted"
	EventOperatorCronTriggered    = "dispatch.operator.cron_triggered"
	EventOperatorWorkflowReplayed = "dispatch.operator.workflow_replayed"
)

// AllDefinitions returns webhook definitions for all Dispatch lifecycle
// event types. Pass these to relay.RegisterEventType to populate the catalog.
func AllDefinitions() []catalog.WebhookDefinition {
	return []catalog.WebhookDefinition{
		// ── Job events ──────────────────────────────────
		{
			Name:        EventJobEnqueued,
			Description: "Fired when a job is enqueued for processing.",
			Group:       "jobs",
			Version:     "2025-01-01",
		},
		{
			Name:        EventJobStarted,
			Description: "Fired when a worker begins executing a job.",
			Group:       "jobs",
			Version:     "2025-01-01",
		},
		{
			Name:        EventJobCompleted,
			Description: "Fired when a job finishes successfully.",
			Group:       "jobs",
			Version:     "2025-01-01",
		},
		{
			Name:        EventJobFailed,
			Description: "Fired when a job fails terminally with no more retries.",
			Group:       "jobs",
			Version:     "2025-01-01",
		},
		{
			Name:        EventJobRetrying,
			Description: "Fired when a job fails but is scheduled for retry.",
			Group:       "jobs",
			Version:     "2025-01-01",
		},
		{
			Name:        EventJobDLQ,
			Description: "Fired when a job is moved to the dead letter queue.",
			Group:       "jobs",
			Version:     "2025-01-01",
		},
		{
			Name:        EventJobCancelled,
			Description: "Fired when a job reaches cancelled.",
			Group:       "jobs",
			Version:     "2026-10-07",
		},
		// ── Workflow events ─────────────────────────────
		{
			Name:        EventWorkflowStarted,
			Description: "Fired when a workflow run begins execution.",
			Group:       "workflows",
			Version:     "2025-01-01",
		},
		{
			Name:        EventWorkflowStepCompleted,
			Description: "Fired after a workflow step completes successfully.",
			Group:       "workflows",
			Version:     "2025-01-01",
		},
		{
			Name:        EventWorkflowStepFailed,
			Description: "Fired when a workflow step fails.",
			Group:       "workflows",
			Version:     "2025-01-01",
		},
		{
			Name:        EventWorkflowCompleted,
			Description: "Fired when a workflow run finishes successfully.",
			Group:       "workflows",
			Version:     "2025-01-01",
		},
		{
			Name:        EventWorkflowFailed,
			Description: "Fired when a workflow run fails terminally.",
			Group:       "workflows",
			Version:     "2025-01-01",
		},
		// ── Cron events ────────────────────────────────
		{
			Name:        EventCronFired,
			Description: "Fired when a cron entry fires and enqueues a job.",
			Group:       "cron",
			Version:     "2025-01-01",
		},
		// ── Operator events ────────────────────────────
		{
			Name:        EventOperatorJobCancelled,
			Description: "Fired when an operator cancels a job.",
			Group:       "operator",
			Version:     "2026-10-07",
		},
		{
			Name:        EventOperatorJobRetried,
			Description: "Fired when an operator retries a failed job.",
			Group:       "operator",
			Version:     "2026-10-07",
		},
		{
			Name:        EventOperatorDLQReplayed,
			Description: "Fired when an operator replays a dead letter queue entry.",
			Group:       "operator",
			Version:     "2026-10-07",
		},
		{
			Name:        EventOperatorDLQDeleted,
			Description: "Fired when an operator deletes a dead letter queue entry.",
			Group:       "operator",
			Version:     "2026-10-07",
		},
		{
			Name:        EventOperatorDLQPurged,
			Description: "Fired when an operator purges old dead letter queue entries.",
			Group:       "operator",
			Version:     "2026-10-07",
		},
		{
			Name:        EventOperatorCronEnabled,
			Description: "Fired when an operator enables a cron entry.",
			Group:       "operator",
			Version:     "2026-10-07",
		},
		{
			Name:        EventOperatorCronDisabled,
			Description: "Fired when an operator disables a cron entry.",
			Group:       "operator",
			Version:     "2026-10-07",
		},
		{
			Name:        EventOperatorCronDeleted,
			Description: "Fired when an operator deletes a cron entry.",
			Group:       "operator",
			Version:     "2026-10-07",
		},
		{
			Name:        EventOperatorCronTriggered,
			Description: "Fired when an operator triggers a cron entry by hand.",
			Group:       "operator",
			Version:     "2026-10-07",
		},
		{
			Name:        EventOperatorWorkflowReplayed,
			Description: "Fired when an operator replays a workflow run from a step.",
			Group:       "operator",
			Version:     "2026-10-07",
		},
	}
}

// RegisterAll registers all Dispatch webhook event types in the Relay catalog.
// Call this once during application startup before sending events.
func RegisterAll(ctx context.Context, r *relay.Relay) error {
	for _, def := range AllDefinitions() {
		if _, err := r.RegisterEventType(ctx, def); err != nil {
			return err
		}
	}
	return nil
}
