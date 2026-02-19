// Package relayhook bridges Dispatch lifecycle events to Relay for webhook
// delivery. When registered as an extension, it emits typed webhook events
// (dispatch.job.completed, dispatch.workflow.failed, etc.) at every
// lifecycle point.
//
// Usage:
//
//	r, _ := relay.New(relay.WithStore(store))
//	relayhook.RegisterAll(ctx, r)
//
//	hook := relayhook.New(r)
//	engine.WithExtension(hook)
//
// To restrict which events are emitted:
//
//	hook := relayhook.New(r,
//	    relayhook.WithEvents(
//	        relayhook.EventJobCompleted,
//	        relayhook.EventJobFailed,
//	    ),
//	)
package relayhook
