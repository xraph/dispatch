package observability_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/observability"
	"github.com/xraph/dispatch/workflow"
)

func setupTestExtension() (*observability.MetricsExtension, *sdkmetric.ManualReader) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := mp.Meter("test")
	e := observability.NewMetricsExtensionWithMeter(meter)
	return e, reader
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}
	return rm
}

func findCounter(rm metricdata.ResourceMetrics, name string) *metricdata.Sum[int64] {
	for _, sm := range rm.ScopeMetrics {
		for i := range sm.Metrics {
			if sm.Metrics[i].Name == name {
				if sum, ok := sm.Metrics[i].Data.(metricdata.Sum[int64]); ok {
					return &sum
				}
			}
		}
	}
	return nil
}

func assertCounterValue(t *testing.T, rm metricdata.ResourceMetrics, name string, expected int64) {
	t.Helper()
	sum := findCounter(rm, name)
	if sum == nil {
		t.Fatalf("counter %q not found", name)
	}
	if len(sum.DataPoints) == 0 {
		t.Fatalf("counter %q has no data points", name)
	}
	if sum.DataPoints[0].Value != expected {
		t.Errorf("counter %q = %d, want %d", name, sum.DataPoints[0].Value, expected)
	}
}

func assertCounterAttr(t *testing.T, rm metricdata.ResourceMetrics, counterName, attrKey, attrValue string) {
	t.Helper()
	sum := findCounter(rm, counterName)
	if sum == nil {
		t.Fatalf("counter %q not found", counterName)
	}
	if len(sum.DataPoints) == 0 {
		t.Fatalf("counter %q has no data points", counterName)
	}
	attrs := sum.DataPoints[0].Attributes.ToSlice()
	for _, a := range attrs {
		if string(a.Key) == attrKey && a.Value.Type() == attribute.STRING && a.Value.AsString() == attrValue {
			return
		}
	}
	t.Errorf("counter %q: attribute %q=%q not found", counterName, attrKey, attrValue)
}

func newTestJob() *job.Job {
	return &job.Job{
		ID:    id.NewJobID(),
		Name:  "send-email",
		Queue: "default",
	}
}

func newTestRun() *workflow.Run {
	return &workflow.Run{
		ID:   id.NewRunID(),
		Name: "order-flow",
	}
}

func TestMetricsExtension_Name(t *testing.T) {
	e, _ := setupTestExtension()
	if e.Name() != "observability-metrics" {
		t.Errorf("expected name %q, got %q", "observability-metrics", e.Name())
	}
}

func TestMetricsExtension_JobEnqueued(t *testing.T) {
	e, reader := setupTestExtension()
	j := newTestJob()

	if err := e.OnJobEnqueued(context.Background(), j); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rm := collectMetrics(t, reader)
	assertCounterValue(t, rm, "dispatch.job.enqueued", 1)
	assertCounterAttr(t, rm, "dispatch.job.enqueued", "job_name", "send-email")
	assertCounterAttr(t, rm, "dispatch.job.enqueued", "queue", "default")
}

func TestMetricsExtension_JobCompleted(t *testing.T) {
	e, reader := setupTestExtension()
	j := newTestJob()

	if err := e.OnJobCompleted(context.Background(), j, 100*time.Millisecond); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rm := collectMetrics(t, reader)
	assertCounterValue(t, rm, "dispatch.job.completed", 1)
	assertCounterAttr(t, rm, "dispatch.job.completed", "job_name", "send-email")
}

func TestMetricsExtension_JobFailed(t *testing.T) {
	e, reader := setupTestExtension()
	j := newTestJob()

	if err := e.OnJobFailed(context.Background(), j, errors.New("boom")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rm := collectMetrics(t, reader)
	assertCounterValue(t, rm, "dispatch.job.failed", 1)
	assertCounterAttr(t, rm, "dispatch.job.failed", "job_name", "send-email")
}

func TestMetricsExtension_JobRetrying(t *testing.T) {
	e, reader := setupTestExtension()
	j := newTestJob()

	if err := e.OnJobRetrying(context.Background(), j, 1, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rm := collectMetrics(t, reader)
	assertCounterValue(t, rm, "dispatch.job.retried", 1)
	assertCounterAttr(t, rm, "dispatch.job.retried", "job_name", "send-email")
}

func TestMetricsExtension_JobDLQ(t *testing.T) {
	e, reader := setupTestExtension()
	j := newTestJob()

	if err := e.OnJobDLQ(context.Background(), j, errors.New("terminal")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rm := collectMetrics(t, reader)
	assertCounterValue(t, rm, "dispatch.job.dlq", 1)
	assertCounterAttr(t, rm, "dispatch.job.dlq", "queue", "default")
}

func TestMetricsExtension_WorkflowStarted(t *testing.T) {
	e, reader := setupTestExtension()
	r := newTestRun()

	if err := e.OnWorkflowStarted(context.Background(), r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rm := collectMetrics(t, reader)
	assertCounterValue(t, rm, "dispatch.workflow.started", 1)
	assertCounterAttr(t, rm, "dispatch.workflow.started", "workflow_name", "order-flow")
}

func TestMetricsExtension_WorkflowCompleted(t *testing.T) {
	e, reader := setupTestExtension()
	r := newTestRun()

	if err := e.OnWorkflowCompleted(context.Background(), r, 2*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rm := collectMetrics(t, reader)
	assertCounterValue(t, rm, "dispatch.workflow.completed", 1)
	assertCounterAttr(t, rm, "dispatch.workflow.completed", "workflow_name", "order-flow")
}

func TestMetricsExtension_WorkflowFailed(t *testing.T) {
	e, reader := setupTestExtension()
	r := newTestRun()

	if err := e.OnWorkflowFailed(context.Background(), r, errors.New("step failed")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rm := collectMetrics(t, reader)
	assertCounterValue(t, rm, "dispatch.workflow.failed", 1)
	assertCounterAttr(t, rm, "dispatch.workflow.failed", "workflow_name", "order-flow")
}

func TestMetricsExtension_CronFired(t *testing.T) {
	e, reader := setupTestExtension()

	if err := e.OnCronFired(context.Background(), "daily-cleanup", id.NewJobID()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rm := collectMetrics(t, reader)
	assertCounterValue(t, rm, "dispatch.cron.fired", 1)
	assertCounterAttr(t, rm, "dispatch.cron.fired", "entry_name", "daily-cleanup")
}

func TestMetricsExtension_ViaRegistry(t *testing.T) {
	e, reader := setupTestExtension()
	logger := slog.Default()

	// Register via ext.Registry (same as engine does).
	reg := ext.NewRegistry(logger)
	reg.Register(e)

	ctx := context.Background()
	j := newTestJob()
	r := newTestRun()

	// Emit events through the registry.
	reg.EmitJobEnqueued(ctx, j)
	reg.EmitJobCompleted(ctx, j, 50*time.Millisecond)
	reg.EmitJobFailed(ctx, j, errors.New("fail"))
	reg.EmitJobRetrying(ctx, j, 1, time.Now())
	reg.EmitJobDLQ(ctx, j, errors.New("dead"))
	reg.EmitWorkflowStarted(ctx, r)
	reg.EmitWorkflowCompleted(ctx, r, time.Second)
	reg.EmitWorkflowFailed(ctx, r, errors.New("wf fail"))
	reg.EmitCronFired(ctx, "hourly", id.NewJobID())

	// Collect and verify all counters.
	rm := collectMetrics(t, reader)

	tests := []struct {
		name  string
		value int64
	}{
		{"dispatch.job.enqueued", 1},
		{"dispatch.job.completed", 1},
		{"dispatch.job.failed", 1},
		{"dispatch.job.retried", 1},
		{"dispatch.job.dlq", 1},
		{"dispatch.workflow.started", 1},
		{"dispatch.workflow.completed", 1},
		{"dispatch.workflow.failed", 1},
		{"dispatch.cron.fired", 1},
	}

	for _, tt := range tests {
		assertCounterValue(t, rm, tt.name, tt.value)
	}
}
