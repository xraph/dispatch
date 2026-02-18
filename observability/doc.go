// Package observability provides OpenTelemetry-based metrics and tracing
// extensions for Dispatch. The MetricsExtension implements lifecycle hooks
// to record system-wide counters for job enqueue, completion, failure,
// retry, DLQ, workflow, and cron events.
//
// For per-execution tracing and metrics, see the middleware package:
// middleware.Tracing() and middleware.Metrics().
package observability
