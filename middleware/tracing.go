package middleware

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/telemetry"
)

// tracerName is the instrumentation scope name for dispatch tracing.
const tracerName = "github.com/xraph/dispatch"

// Tracing returns middleware that wraps the rest of the chain, and the
// handler, in an OpenTelemetry span named "run job handler". If no
// TracerProvider is configured globally, the default noop tracer is used and
// this middleware becomes a pass-through with zero overhead.
//
// Inside an engine the worker pool already gives every attempt a root
// "process job" span, and this span is its child. Span attributes include:
// dispatch.job.id, dispatch.job.name, dispatch.queue, dispatch.retry_count,
// dispatch.scope.app_id, dispatch.scope.org_id. When the handler fails the
// span gets an Error status with a fixed description and an exception event
// carrying only exception.type. The error's message is never recorded: a
// handler error can quote its payload. A handler that panics gets the status
// class "panic" and exception.type "panic", and the panic continues to the
// Recover middleware.
func Tracing() Middleware {
	tracer := otel.Tracer(tracerName)
	return TracingWithTracer(tracer)
}

// TracingWithTracer returns tracing middleware using the provided tracer.
// This variant allows injecting a specific TracerProvider for testing or
// when multiple providers are in use.
func TracingWithTracer(tracer trace.Tracer) Middleware {
	return func(ctx context.Context, j *job.Job, next Handler) error {
		ctx, span := tracer.Start(ctx, "run job handler",
			trace.WithAttributes(
				attribute.String("dispatch.job.id", j.ID.String()),
				attribute.String("dispatch.job.name", j.Name),
				attribute.String("dispatch.queue", j.Queue),
				attribute.Int("dispatch.retry_count", j.RetryCount),
				attribute.String("dispatch.scope.app_id", j.ScopeAppID),
				attribute.String("dispatch.scope.org_id", j.ScopeOrgID),
			),
			trace.WithSpanKind(trace.SpanKindInternal),
		)
		// Recover sits outside this middleware, so a panicking handler
		// unwinds through here. Mark the span before it ends; the panic
		// carries on to Recover untouched.
		returned := false
		defer func() {
			if !returned {
				telemetry.Fail(span, "panic", "panic")
			}
			span.End()
		}()

		err := next(ctx)
		returned = true
		if err != nil {
			telemetry.Fail(span, "handler error", telemetry.ErrorType(err))
		}

		return err
	}
}
