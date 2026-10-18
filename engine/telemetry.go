package engine

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/telemetry"
)

// configuredQueues is every queue name this engine was configured with: the
// queues its workers poll, the ones with limits, and the ones with resource
// defaults. Only these appear as a metric label; any other queue a caller
// names is recorded as telemetry.OtherQueue.
func (eng *Engine) configuredQueues() []string {
	names := append([]string(nil), eng.d.Config().Queues...)
	for _, c := range eng.queueConfigs {
		names = append(names, c.Name)
	}
	for name := range eng.queueResources {
		names = append(names, name)
	}

	return names
}

// startEnqueueSpan starts the producer span for one enqueue. It is a child
// of whatever the caller's ctx carries (an HTTP request, a cron fire), and
// the span context it leaves in ctx is what commitEnqueue remembers for the
// attempt to link back to.
func (eng *Engine) startEnqueueSpan(ctx context.Context, j *job.Job) (context.Context, trace.Span) {
	return eng.tel.Start(ctx, "enqueue job",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("dispatch"),
			semconv.MessagingOperationTypeSend,
			semconv.MessagingOperationName("enqueue"),
			semconv.MessagingDestinationName(j.Queue),
			semconv.MessagingMessageID(j.ID.String()),
			attribute.String("dispatch.job.name", j.Name),
			attribute.Int("dispatch.job.priority", j.Priority),
			attribute.Bool("dispatch.job.delayed", j.RunAt.After(time.Now().UTC())),
		),
	)
}

// failEnqueue marks an enqueue span failed with the error's type only.
func failEnqueue(span trace.Span, err error) {
	telemetry.Fail(span, "enqueue failed", telemetry.ErrorType(err))
}
