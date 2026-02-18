// Package relayhook bridges Dispatch lifecycle events to Relay for webhook
// delivery. When registered as an extension, it emits typed webhook events
// (dispatch.job.completed, dispatch.workflow.failed, etc.) at every
// lifecycle point.
package relayhook
