package audithook

// Audit event actions. Each constant corresponds to one ext lifecycle hook
// and becomes the Action field of the audit event.
const (
	ActionJobEnqueued           = "job.enqueued"
	ActionJobStarted            = "job.started"
	ActionJobCompleted          = "job.completed"
	ActionJobFailed             = "job.failed"
	ActionJobRetrying           = "job.retrying"
	ActionJobDLQ                = "job.dlq"
	ActionWorkflowStarted       = "workflow.started"
	ActionWorkflowStepCompleted = "workflow.step_completed"
	ActionWorkflowStepFailed    = "workflow.step_failed"
	ActionWorkflowCompleted     = "workflow.completed"
	ActionWorkflowFailed        = "workflow.failed"
	ActionCronFired             = "cron.fired"
)

// Audit event categories group related actions.
const (
	CategoryJob      = "dispatch.job"
	CategoryWorkflow = "dispatch.workflow"
	CategoryCron     = "dispatch.cron"
)

// Resource types used as the Resource field in audit events.
const (
	ResourceJob      = "job"
	ResourceWorkflow = "workflow_run"
	ResourceCron     = "cron_entry"
)

// AllActions returns every action this extension can emit.
func AllActions() []string {
	return []string{
		ActionJobEnqueued,
		ActionJobStarted,
		ActionJobCompleted,
		ActionJobFailed,
		ActionJobRetrying,
		ActionJobDLQ,
		ActionWorkflowStarted,
		ActionWorkflowStepCompleted,
		ActionWorkflowStepFailed,
		ActionWorkflowCompleted,
		ActionWorkflowFailed,
		ActionCronFired,
	}
}
