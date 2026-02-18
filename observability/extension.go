package observability

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// Compile-time interface checks.
var (
	_ ext.Extension         = (*MetricsExtension)(nil)
	_ ext.JobEnqueued       = (*MetricsExtension)(nil)
	_ ext.JobCompleted      = (*MetricsExtension)(nil)
	_ ext.JobFailed         = (*MetricsExtension)(nil)
	_ ext.JobRetrying       = (*MetricsExtension)(nil)
	_ ext.JobDLQ            = (*MetricsExtension)(nil)
	_ ext.WorkflowStarted   = (*MetricsExtension)(nil)
	_ ext.WorkflowCompleted = (*MetricsExtension)(nil)
	_ ext.WorkflowFailed    = (*MetricsExtension)(nil)
	_ ext.CronFired         = (*MetricsExtension)(nil)
)

// extensionMeterName is the instrumentation scope for extension metrics.
const extensionMeterName = "github.com/xraph/dispatch/observability"

// MetricsExtension records system-wide lifecycle metrics via OpenTelemetry.
// Register it as a Dispatch extension to automatically track enqueue rates,
// completion counts, failure rates, retry counts, DLQ entries, workflow
// executions, and cron fires.
type MetricsExtension struct {
	jobEnqueued       metric.Int64Counter
	jobCompleted      metric.Int64Counter
	jobFailed         metric.Int64Counter
	jobRetried        metric.Int64Counter
	jobDLQ            metric.Int64Counter
	workflowStarted   metric.Int64Counter
	workflowCompleted metric.Int64Counter
	workflowFailed    metric.Int64Counter
	cronFired         metric.Int64Counter
}

// NewMetricsExtension creates a MetricsExtension using the global MeterProvider.
func NewMetricsExtension() *MetricsExtension {
	meter := otel.Meter(extensionMeterName)
	return NewMetricsExtensionWithMeter(meter)
}

// NewMetricsExtensionWithMeter creates a MetricsExtension with a specific meter.
// Useful for testing or when multiple MeterProviders are in use.
func NewMetricsExtensionWithMeter(meter metric.Meter) *MetricsExtension {
	mustCounter := func(name, desc string) metric.Int64Counter {
		c, err := meter.Int64Counter(name,
			metric.WithDescription(desc),
			metric.WithUnit("{event}"),
		)
		_ = err // noop fallback guaranteed by OTel API contract
		return c
	}

	return &MetricsExtension{
		jobEnqueued:       mustCounter("dispatch.job.enqueued", "Total jobs enqueued"),
		jobCompleted:      mustCounter("dispatch.job.completed", "Total jobs completed successfully"),
		jobFailed:         mustCounter("dispatch.job.failed", "Total jobs failed terminally"),
		jobRetried:        mustCounter("dispatch.job.retried", "Total job retries scheduled"),
		jobDLQ:            mustCounter("dispatch.job.dlq", "Total jobs moved to dead letter queue"),
		workflowStarted:   mustCounter("dispatch.workflow.started", "Total workflow runs started"),
		workflowCompleted: mustCounter("dispatch.workflow.completed", "Total workflow runs completed"),
		workflowFailed:    mustCounter("dispatch.workflow.failed", "Total workflow runs failed"),
		cronFired:         mustCounter("dispatch.cron.fired", "Total cron entries fired"),
	}
}

// Name implements ext.Extension.
func (m *MetricsExtension) Name() string { return "observability-metrics" }

// ── Job lifecycle hooks ─────────────────────────────

// OnJobEnqueued implements ext.JobEnqueued.
func (m *MetricsExtension) OnJobEnqueued(ctx context.Context, j *job.Job) error {
	m.jobEnqueued.Add(ctx, 1, metric.WithAttributes(
		attribute.String("job_name", j.Name),
		attribute.String("queue", j.Queue),
	))
	return nil
}

// OnJobCompleted implements ext.JobCompleted.
func (m *MetricsExtension) OnJobCompleted(ctx context.Context, j *job.Job, _ time.Duration) error {
	m.jobCompleted.Add(ctx, 1, metric.WithAttributes(
		attribute.String("job_name", j.Name),
		attribute.String("queue", j.Queue),
	))
	return nil
}

// OnJobFailed implements ext.JobFailed.
func (m *MetricsExtension) OnJobFailed(ctx context.Context, j *job.Job, _ error) error {
	m.jobFailed.Add(ctx, 1, metric.WithAttributes(
		attribute.String("job_name", j.Name),
		attribute.String("queue", j.Queue),
	))
	return nil
}

// OnJobRetrying implements ext.JobRetrying.
func (m *MetricsExtension) OnJobRetrying(ctx context.Context, j *job.Job, _ int, _ time.Time) error {
	m.jobRetried.Add(ctx, 1, metric.WithAttributes(
		attribute.String("job_name", j.Name),
		attribute.String("queue", j.Queue),
	))
	return nil
}

// OnJobDLQ implements ext.JobDLQ.
func (m *MetricsExtension) OnJobDLQ(ctx context.Context, j *job.Job, _ error) error {
	m.jobDLQ.Add(ctx, 1, metric.WithAttributes(
		attribute.String("job_name", j.Name),
		attribute.String("queue", j.Queue),
	))
	return nil
}

// ── Workflow lifecycle hooks ────────────────────────

// OnWorkflowStarted implements ext.WorkflowStarted.
func (m *MetricsExtension) OnWorkflowStarted(ctx context.Context, r *workflow.Run) error {
	m.workflowStarted.Add(ctx, 1, metric.WithAttributes(
		attribute.String("workflow_name", r.Name),
	))
	return nil
}

// OnWorkflowCompleted implements ext.WorkflowCompleted.
func (m *MetricsExtension) OnWorkflowCompleted(ctx context.Context, r *workflow.Run, _ time.Duration) error {
	m.workflowCompleted.Add(ctx, 1, metric.WithAttributes(
		attribute.String("workflow_name", r.Name),
	))
	return nil
}

// OnWorkflowFailed implements ext.WorkflowFailed.
func (m *MetricsExtension) OnWorkflowFailed(ctx context.Context, r *workflow.Run, _ error) error {
	m.workflowFailed.Add(ctx, 1, metric.WithAttributes(
		attribute.String("workflow_name", r.Name),
	))
	return nil
}

// ── Cron lifecycle hooks ────────────────────────────

// OnCronFired implements ext.CronFired.
func (m *MetricsExtension) OnCronFired(ctx context.Context, entryName string, _ id.JobID) error {
	m.cronFired.Add(ctx, 1, metric.WithAttributes(
		attribute.String("entry_name", entryName),
	))
	return nil
}
