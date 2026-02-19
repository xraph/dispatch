package middleware

import (
	"context"
	"time"

	gu "github.com/xraph/go-utils/metrics"

	"github.com/xraph/dispatch/job"
)

// Metrics returns middleware that records per-job execution metrics using
// a default metrics collector. For forge-managed dispatch instances, prefer
// MetricsWithFactory(fapp.Metrics()) so metrics are reported through forge.
//
// Instruments:
//   - dispatch.job.duration (Histogram): execution time in seconds
//   - dispatch.job.executions (Counter): total executions
func Metrics() Middleware {
	return MetricsWithFactory(gu.NewMetricsCollector("dispatch"))
}

// MetricsWithFactory returns metrics middleware using the provided MetricFactory.
// This variant allows injecting a specific factory for testing or forge integration.
func MetricsWithFactory(factory gu.MetricFactory) Middleware {
	duration := factory.Histogram("dispatch.job.duration")
	executions := factory.Counter("dispatch.job.executions")

	return func(ctx context.Context, _ *job.Job, next Handler) error {
		start := time.Now()
		err := next(ctx)
		duration.Observe(time.Since(start).Seconds())
		executions.Inc()
		return err
	}
}
