package observability

import (
	"context"
	"time"

	gu "github.com/xraph/go-utils/metrics"

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

// MetricsExtension records system-wide lifecycle metrics via go-utils MetricFactory.
// Register it as a Dispatch extension to automatically track enqueue rates,
// completion counts, failure rates, retry counts, DLQ entries, workflow
// executions, and cron fires.
type MetricsExtension struct {
	JobEnqueued       gu.Counter
	JobCompleted      gu.Counter
	JobFailed         gu.Counter
	JobRetried        gu.Counter
	JobDLQ            gu.Counter
	WorkflowStarted   gu.Counter
	WorkflowCompleted gu.Counter
	WorkflowFailed    gu.Counter
	CronFired         gu.Counter
}

// NewMetricsExtension creates a MetricsExtension using a default metrics collector.
func NewMetricsExtension() *MetricsExtension {
	return NewMetricsExtensionWithFactory(gu.NewMetricsCollector("dispatch/observability"))
}

// NewMetricsExtensionWithFactory creates a MetricsExtension with the provided MetricFactory.
// Use fapp.Metrics() in forge extensions, or gu.NewMetricsCollector for testing.
func NewMetricsExtensionWithFactory(factory gu.MetricFactory) *MetricsExtension {
	return &MetricsExtension{
		JobEnqueued:       factory.Counter("dispatch.job.enqueued"),
		JobCompleted:      factory.Counter("dispatch.job.completed"),
		JobFailed:         factory.Counter("dispatch.job.failed"),
		JobRetried:        factory.Counter("dispatch.job.retried"),
		JobDLQ:            factory.Counter("dispatch.job.dlq"),
		WorkflowStarted:   factory.Counter("dispatch.workflow.started"),
		WorkflowCompleted: factory.Counter("dispatch.workflow.completed"),
		WorkflowFailed:    factory.Counter("dispatch.workflow.failed"),
		CronFired:         factory.Counter("dispatch.cron.fired"),
	}
}

// Name implements ext.Extension.
func (m *MetricsExtension) Name() string { return "observability-metrics" }

// ── Job lifecycle hooks ─────────────────────────────

// OnJobEnqueued implements ext.JobEnqueued.
func (m *MetricsExtension) OnJobEnqueued(_ context.Context, _ *job.Job) error {
	m.JobEnqueued.Inc()
	return nil
}

// OnJobCompleted implements ext.JobCompleted.
func (m *MetricsExtension) OnJobCompleted(_ context.Context, _ *job.Job, _ time.Duration) error {
	m.JobCompleted.Inc()
	return nil
}

// OnJobFailed implements ext.JobFailed.
func (m *MetricsExtension) OnJobFailed(_ context.Context, _ *job.Job, _ error) error {
	m.JobFailed.Inc()
	return nil
}

// OnJobRetrying implements ext.JobRetrying.
func (m *MetricsExtension) OnJobRetrying(_ context.Context, _ *job.Job, _ int, _ time.Time) error {
	m.JobRetried.Inc()
	return nil
}

// OnJobDLQ implements ext.JobDLQ.
func (m *MetricsExtension) OnJobDLQ(_ context.Context, _ *job.Job, _ error) error {
	m.JobDLQ.Inc()
	return nil
}

// ── Workflow lifecycle hooks ────────────────────────

// OnWorkflowStarted implements ext.WorkflowStarted.
func (m *MetricsExtension) OnWorkflowStarted(_ context.Context, _ *workflow.Run) error {
	m.WorkflowStarted.Inc()
	return nil
}

// OnWorkflowCompleted implements ext.WorkflowCompleted.
func (m *MetricsExtension) OnWorkflowCompleted(_ context.Context, _ *workflow.Run, _ time.Duration) error {
	m.WorkflowCompleted.Inc()
	return nil
}

// OnWorkflowFailed implements ext.WorkflowFailed.
func (m *MetricsExtension) OnWorkflowFailed(_ context.Context, _ *workflow.Run, _ error) error {
	m.WorkflowFailed.Inc()
	return nil
}

// ── Cron lifecycle hooks ────────────────────────────

// OnCronFired implements ext.CronFired.
func (m *MetricsExtension) OnCronFired(_ context.Context, _ string, _ id.JobID) error {
	m.CronFired.Inc()
	return nil
}
