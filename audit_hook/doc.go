// Package audithook is a Dispatch extension that bridges lifecycle events
// to an immutable audit trail backend such as Chronicle.
//
// Every job, workflow, and cron lifecycle hook emits a structured audit event
// through the [Recorder] interface. The extension assigns appropriate severity
// levels (info for normal operations, warning for retries, critical for
// terminal failures) and rich metadata (job name, queue, elapsed time, errors).
//
// Operator actions taken through the engine (cancel, retry, DLQ replay,
// delete and purge, cron enable, disable, delete and trigger, workflow
// replay) are recorded under [CategoryOperator], with actions such as
// [ActionOperatorJobCancelled]. The acting subject is in Metadata["actor"].
//
// # Usage with Chronicle
//
//	audithook.New(audithook.RecorderFunc(func(ctx context.Context, evt *audithook.AuditEvent) error {
//	    return chronicle.Info(ctx, evt.Action, evt.Resource, evt.ResourceID).
//	        Category(evt.Category).
//	        Outcome(evt.Outcome).
//	        Record()
//	}))
//
// # Selective filtering
//
//	audithook.New(recorder,
//	    audithook.WithActions(
//	        audithook.ActionJobFailed,
//	        audithook.ActionJobDLQ,
//	        audithook.ActionWorkflowFailed,
//	    ),
//	)
package audithook
