// Package observability provides the legacy lifecycle counters for
// Dispatch. The MetricsExtension implements lifecycle hooks to record
// unlabelled, system-wide counters for job enqueue, completion, failure,
// retry, DLQ, workflow, and cron events.
//
// The spans and labelled metrics for job attempts, cron, workflows, the
// durable runtime, the outbox and the artifact sweeper live in the
// telemetry package, and engine.Build wires both.
package observability
