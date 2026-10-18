// Package telemetry holds the OpenTelemetry spans and metric instruments
// Dispatch records for the work it does itself: the job pool and runner,
// cron, checkpoint workflows, the durable runtime, the outbox publisher and
// the artifact sweeper.
//
// It is a leaf package. It imports the standard library, OpenTelemetry and
// go-utils metrics, and nothing else from Dispatch, so every subsystem can
// import it without a cycle. exec deliberately does not import it.
//
// You rarely build a Telemetry yourself. engine.Build builds one from
// engine.WithMetricFactory and engine.WithTracerProvider and hands it to every
// subsystem it constructs, and the Forge extension passes the app's metrics.
// Build one with New only when you assemble a subsystem (a worker.Pool, a
// durable runtime Worker, a delivery Publisher) outside the engine.
//
// A nil *Telemetry is valid everywhere: spans go to the global tracer
// provider and no metrics are recorded.
package telemetry
