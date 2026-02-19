package observability_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	gu "github.com/xraph/go-utils/metrics"

	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/observability"
	"github.com/xraph/dispatch/workflow"
)

func newTestExtension() *observability.MetricsExtension {
	return observability.NewMetricsExtensionWithFactory(gu.NewMetricsCollector("test"))
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
	e := newTestExtension()
	if e.Name() != "observability-metrics" {
		t.Errorf("expected name %q, got %q", "observability-metrics", e.Name())
	}
}

func TestMetricsExtension_JobEnqueued(t *testing.T) {
	e := newTestExtension()
	if err := e.OnJobEnqueued(context.Background(), newTestJob()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.JobEnqueued.Value() != 1 {
		t.Errorf("JobEnqueued: want 1, got %v", e.JobEnqueued.Value())
	}
}

func TestMetricsExtension_JobCompleted(t *testing.T) {
	e := newTestExtension()
	if err := e.OnJobCompleted(context.Background(), newTestJob(), 100*time.Millisecond); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.JobCompleted.Value() != 1 {
		t.Errorf("JobCompleted: want 1, got %v", e.JobCompleted.Value())
	}
}

func TestMetricsExtension_JobFailed(t *testing.T) {
	e := newTestExtension()
	if err := e.OnJobFailed(context.Background(), newTestJob(), errors.New("boom")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.JobFailed.Value() != 1 {
		t.Errorf("JobFailed: want 1, got %v", e.JobFailed.Value())
	}
}

func TestMetricsExtension_JobRetrying(t *testing.T) {
	e := newTestExtension()
	if err := e.OnJobRetrying(context.Background(), newTestJob(), 1, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.JobRetried.Value() != 1 {
		t.Errorf("JobRetried: want 1, got %v", e.JobRetried.Value())
	}
}

func TestMetricsExtension_JobDLQ(t *testing.T) {
	e := newTestExtension()
	if err := e.OnJobDLQ(context.Background(), newTestJob(), errors.New("terminal")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.JobDLQ.Value() != 1 {
		t.Errorf("JobDLQ: want 1, got %v", e.JobDLQ.Value())
	}
}

func TestMetricsExtension_WorkflowStarted(t *testing.T) {
	e := newTestExtension()
	if err := e.OnWorkflowStarted(context.Background(), newTestRun()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.WorkflowStarted.Value() != 1 {
		t.Errorf("WorkflowStarted: want 1, got %v", e.WorkflowStarted.Value())
	}
}

func TestMetricsExtension_WorkflowCompleted(t *testing.T) {
	e := newTestExtension()
	if err := e.OnWorkflowCompleted(context.Background(), newTestRun(), 2*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.WorkflowCompleted.Value() != 1 {
		t.Errorf("WorkflowCompleted: want 1, got %v", e.WorkflowCompleted.Value())
	}
}

func TestMetricsExtension_WorkflowFailed(t *testing.T) {
	e := newTestExtension()
	if err := e.OnWorkflowFailed(context.Background(), newTestRun(), errors.New("step failed")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.WorkflowFailed.Value() != 1 {
		t.Errorf("WorkflowFailed: want 1, got %v", e.WorkflowFailed.Value())
	}
}

func TestMetricsExtension_CronFired(t *testing.T) {
	e := newTestExtension()
	if err := e.OnCronFired(context.Background(), "daily-cleanup", id.NewJobID()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.CronFired.Value() != 1 {
		t.Errorf("CronFired: want 1, got %v", e.CronFired.Value())
	}
}

func TestMetricsExtension_ViaRegistry(t *testing.T) {
	e := newTestExtension()
	logger := slog.Default()

	reg := ext.NewRegistry(logger)
	reg.Register(e)

	ctx := context.Background()
	j := newTestJob()
	r := newTestRun()

	reg.EmitJobEnqueued(ctx, j)
	reg.EmitJobCompleted(ctx, j, 50*time.Millisecond)
	reg.EmitJobFailed(ctx, j, errors.New("fail"))
	reg.EmitJobRetrying(ctx, j, 1, time.Now())
	reg.EmitJobDLQ(ctx, j, errors.New("dead"))
	reg.EmitWorkflowStarted(ctx, r)
	reg.EmitWorkflowCompleted(ctx, r, time.Second)
	reg.EmitWorkflowFailed(ctx, r, errors.New("wf fail"))
	reg.EmitCronFired(ctx, "hourly", id.NewJobID())

	checks := []struct {
		name  string
		value float64
	}{
		{"JobEnqueued", e.JobEnqueued.Value()},
		{"JobCompleted", e.JobCompleted.Value()},
		{"JobFailed", e.JobFailed.Value()},
		{"JobRetried", e.JobRetried.Value()},
		{"JobDLQ", e.JobDLQ.Value()},
		{"WorkflowStarted", e.WorkflowStarted.Value()},
		{"WorkflowCompleted", e.WorkflowCompleted.Value()},
		{"WorkflowFailed", e.WorkflowFailed.Value()},
		{"CronFired", e.CronFired.Value()},
	}

	for _, c := range checks {
		if c.value != 1 {
			t.Errorf("%s: want 1, got %v", c.name, c.value)
		}
	}
}
