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
	ActionJobCancelled          = "job.cancelled"
	ActionWorkflowStarted       = "workflow.started"
	ActionWorkflowStepCompleted = "workflow.step_completed"
	ActionWorkflowStepFailed    = "workflow.step_failed"
	ActionWorkflowCompleted     = "workflow.completed"
	ActionWorkflowFailed        = "workflow.failed"
	ActionCronFired             = "cron.fired"
)

// Operator audit actions. Each constant corresponds to one ext.ActionKind
// and is recorded under CategoryOperator, so an operator's cancel stays
// distinct from the job.cancelled lifecycle event it causes.
const (
	ActionOperatorJobCancelled     = "operator.job_cancelled"
	ActionOperatorJobRetried       = "operator.job_retried"
	ActionOperatorDLQReplayed      = "operator.dlq_replayed"
	ActionOperatorDLQDeleted       = "operator.dlq_deleted"
	ActionOperatorDLQPurged        = "operator.dlq_purged"
	ActionOperatorCronEnabled      = "operator.cron_enabled"
	ActionOperatorCronDisabled     = "operator.cron_disabled"
	ActionOperatorCronDeleted      = "operator.cron_deleted"
	ActionOperatorCronTriggered    = "operator.cron_triggered"
	ActionOperatorWorkflowReplayed = "operator.workflow_replayed"
)

// Audit event categories group related actions.
const (
	CategoryJob      = "dispatch.job"
	CategoryWorkflow = "dispatch.workflow"
	CategoryCron     = "dispatch.cron"
	CategoryOperator = "dispatch.operator"
)

// Resource types used as the Resource field in audit events.
const (
	ResourceJob      = "job"
	ResourceWorkflow = "workflow_run"
	ResourceCron     = "cron_entry"
	ResourceDLQ      = "dlq_entry"
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
		ActionJobCancelled,
		ActionWorkflowStarted,
		ActionWorkflowStepCompleted,
		ActionWorkflowStepFailed,
		ActionWorkflowCompleted,
		ActionWorkflowFailed,
		ActionCronFired,
		ActionOperatorJobCancelled,
		ActionOperatorJobRetried,
		ActionOperatorDLQReplayed,
		ActionOperatorDLQDeleted,
		ActionOperatorDLQPurged,
		ActionOperatorCronEnabled,
		ActionOperatorCronDisabled,
		ActionOperatorCronDeleted,
		ActionOperatorCronTriggered,
		ActionOperatorWorkflowReplayed,
	}
}
