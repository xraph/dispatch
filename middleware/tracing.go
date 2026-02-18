package middleware

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/dispatch/job"
)

// tracerName is the instrumentation scope name for dispatch tracing.
const tracerName = "github.com/xraph/dispatch"

// Tracing returns middleware that wraps job execution in an OpenTelemetry span.
// If no TracerProvider is configured globally, the default noop tracer is used
// and this middleware becomes a pass-through with zero overhead.
//
// Span attributes include: dispatch.job.id, dispatch.job.name, dispatch.queue,
// dispatch.retry_count, dispatch.scope.app_id, dispatch.scope.org_id.
// On error, the span status is set to codes.Error with the error message.
func Tracing() Middleware {
	tracer := otel.Tracer(tracerName)
	return TracingWithTracer(tracer)
}

// TracingWithTracer returns tracing middleware using the provided tracer.
// This variant allows injecting a specific TracerProvider for testing or
// when multiple providers are in use.
func TracingWithTracer(tracer trace.Tracer) Middleware {
	return func(ctx context.Context, j *job.Job, next Handler) error {
		ctx, span := tracer.Start(ctx, "dispatch.job.execute",
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
		defer span.End()

		err := next(ctx)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "")
		}

		return err
	}
}
