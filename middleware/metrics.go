package middleware

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/xraph/dispatch/job"
)

// meterName is the instrumentation scope name for dispatch metrics.
const meterName = "github.com/xraph/dispatch"

// Metrics returns middleware that records per-job execution metrics using
// the global OTel MeterProvider. If no MeterProvider is configured, noop
// instruments are used and this middleware becomes a pass-through.
//
// Instruments:
//   - dispatch.job.duration (Float64Histogram): execution time in seconds,
//     with attributes: job_name, queue, status ("ok" or "error")
//   - dispatch.job.executions (Int64Counter): total executions,
//     with attributes: job_name, queue, status ("ok" or "error")
func Metrics() Middleware {
	meter := otel.Meter(meterName)
	return MetricsWithMeter(meter)
}

// MetricsWithMeter returns metrics middleware using the provided meter.
// This variant allows injecting a specific MeterProvider for testing.
func MetricsWithMeter(meter metric.Meter) Middleware {
	// Create instruments once at middleware construction time.
	// OTel instruments are safe for concurrent use. On error, the API
	// returns noop instruments so the middleware degrades gracefully.
	duration, dErr := meter.Float64Histogram(
		"dispatch.job.duration",
		metric.WithDescription("Duration of job execution in seconds"),
		metric.WithUnit("s"),
	)
	_ = dErr // noop fallback guaranteed by OTel API contract

	executions, eErr := meter.Int64Counter(
		"dispatch.job.executions",
		metric.WithDescription("Total number of job executions"),
		metric.WithUnit("{execution}"),
	)
	_ = eErr // noop fallback guaranteed by OTel API contract

	return func(ctx context.Context, j *job.Job, next Handler) error {
		start := time.Now()
		err := next(ctx)
		elapsed := time.Since(start).Seconds()

		status := "ok"
		if err != nil {
			status = "error"
		}

		attrs := metric.WithAttributes(
			attribute.String("job_name", j.Name),
			attribute.String("queue", j.Queue),
			attribute.String("status", status),
		)

		duration.Record(ctx, elapsed, attrs)
		executions.Add(ctx, 1, attrs)

		return err
	}
}
