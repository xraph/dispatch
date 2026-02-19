// Package ext defines the extension system for Dispatch.
//
// Extensions are notified of lifecycle events and can react to them —
// recording metrics, emitting webhooks, writing audit logs, etc.
// Each lifecycle hook is a separate interface so extensions opt in only
// to the events they care about.
//
// # Implementing an Extension
//
//	type MyExtension struct{}
//
//	func (e *MyExtension) Name() string { return "my-extension" }
//
//	// Opt in to specific hooks by implementing their interfaces.
//	func (e *MyExtension) OnJobCompleted(ctx context.Context, j *job.Job, elapsed time.Duration) error {
//	    log.Printf("job %s completed in %s", j.ID, elapsed)
//	    return nil
//	}
//
// # Job Lifecycle Hooks
//
//   - [JobEnqueued] — job was accepted into the queue
//   - [JobStarted] — worker began executing the job
//   - [JobCompleted] — job finished successfully
//   - [JobFailed] — job failed with no retries remaining
//   - [JobRetrying] — job failed but will be retried
//   - [JobDLQ] — job was moved to the dead letter queue
//
// # Workflow Lifecycle Hooks
//
//   - [WorkflowStarted] — workflow run began
//   - [WorkflowStepCompleted] — a step finished successfully
//   - [WorkflowStepFailed] — a step failed
//   - [WorkflowCompleted] — workflow run finished successfully
//   - [WorkflowFailed] — workflow run failed terminally
//
// # Other Hooks
//
//   - [CronFired] — a cron entry was triggered and a job was enqueued
//   - [Shutdown] — the dispatcher is shutting down gracefully
//
// The [Registry] fans out each event to all registered extensions that
// implement the corresponding hook interface.
package ext
