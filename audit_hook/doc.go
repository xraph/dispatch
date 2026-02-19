// Package audithook is a Dispatch extension that bridges lifecycle events
// to an immutable audit trail backend such as Chronicle.
//
// Every job, workflow, and cron lifecycle hook emits a structured audit event
// through the [Recorder] interface. The extension assigns appropriate severity
// levels (info for normal operations, warning for retries, critical for
// terminal failures) and rich metadata (job name, queue, elapsed time, errors).
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
