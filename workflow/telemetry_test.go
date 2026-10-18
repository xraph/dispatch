package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	log "github.com/xraph/go-utils/log"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/dispatch/internal/metricstest"
	"github.com/xraph/dispatch/internal/oteltest"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/telemetry"
	"github.com/xraph/dispatch/workflow"
)

type signupInput struct{ Email string }

func telemetryRunner(t *testing.T) (*workflow.Runner, *workflow.Registry, *memory.Store, *metricstest.Factory) {
	t.Helper()
	s := memory.New()
	reg := workflow.NewRegistry()
	f := metricstest.New()
	runner := workflow.NewRunner(reg, s, s, noopEmitter{}, log.NewNoopLogger()).
		WithTelemetry(telemetry.New(telemetry.WithMetrics(f)))
	t.Cleanup(func() { _ = runner.Shutdown(context.Background()) })
	return runner, reg, s, f
}

func stepKinds(spans []sdktrace.ReadOnlySpan) map[string]int {
	out := map[string]int{}
	for _, s := range oteltest.Named(spans, "run workflow step") {
		kind, _ := oteltest.Attr(s, "dispatch.workflow.step.kind")
		out[kind]++
	}
	return out
}

func TestRunAndStepsAreSpansUnderTheCaller(t *testing.T) {
	sr := oteltest.Record(t)
	runner, reg, _, f := telemetryRunner(t)

	var inStep trace.SpanContext
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("signup", func(wf *workflow.Workflow, in signupInput) error {
		if err := wf.Step("create-account", func(ctx context.Context) error {
			inStep = trace.SpanContextFromContext(ctx)
			return nil
		}); err != nil {
			return err
		}
		_, err := workflow.StepWithResult(wf, "send-welcome", func(context.Context) (string, error) {
			return "", fmt.Errorf("smtp rejected %s", in.Email)
		})
		return err
	}))

	ctx, request := otelTracer().Start(context.Background(), "POST /signup")
	if _, err := workflow.Start(ctx, runner, "signup", signupInput{Email: "alice@example.com"}); err != nil {
		t.Fatal(err)
	}
	request.End()

	spans := sr.Ended()
	runs := oteltest.Named(spans, "run workflow")
	if len(runs) != 1 {
		t.Fatalf("run spans = %d", len(runs))
	}
	run := runs[0]
	if run.Parent().SpanID() != request.SpanContext().SpanID() {
		t.Error("a run the caller waits on should be a child of the caller's span")
	}
	if run.Status().Code != codes.Error || run.Status().Description != "workflow failed" {
		t.Errorf("run status = %+v", run.Status())
	}
	if got, _ := oteltest.Attr(run, "dispatch.workflow.name"); got != "signup" {
		t.Errorf("workflow name = %q", got)
	}

	steps := oteltest.Named(spans, "run workflow step")
	if len(steps) != 2 {
		t.Fatalf("step spans = %d, want 2", len(steps))
	}
	for _, s := range steps {
		if !oteltest.ChildOf(s, run) {
			t.Errorf("step %v is not a child of the run", s.Name())
		}
	}
	if inStep.SpanID() != steps[0].SpanContext().SpanID() {
		t.Error("the step function did not get the step span's context")
	}
	oteltest.AssertNoValue(t, spans, "alice@example.com")

	if n, ok := f.HistogramCount(telemetry.MetricWorkflowRuns, "result", "failed"); !ok || n != 1 {
		t.Errorf("failed runs = %d, %v", n, ok)
	}
	if n, ok := f.HistogramCount(telemetry.MetricWorkflowStep, "kind", "step", "result", "ok"); !ok || n != 1 {
		t.Errorf("ok steps = %d, %v", n, ok)
	}
	if n, ok := f.HistogramCount(telemetry.MetricWorkflowStep, "kind", "step", "result", "error"); !ok || n != 1 {
		t.Errorf("failed steps = %d, %v", n, ok)
	}
}

func TestResumedRunsAreRootsAndReplayedStepsGetNoSpan(t *testing.T) {
	sr := oteltest.Record(t)
	runner, reg, s, _ := telemetryRunner(t)

	failFirst := true
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("import", func(wf *workflow.Workflow, _ struct{}) error {
		if err := wf.Step("download", func(context.Context) error { return nil }); err != nil {
			return err
		}
		return wf.Step("load", func(context.Context) error {
			if failFirst {
				failFirst = false
				return errors.New("crash")
			}
			return nil
		})
	}))
	run, err := workflow.Start(context.Background(), runner, "import", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	// Put the failed run back to running, as a crash mid-run leaves it.
	run.State = workflow.RunStateRunning
	run.Error = ""
	run.CompletedAt = nil
	if err := s.UpdateRun(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	sr.Reset()

	boot := oteltest.SpanContext(t)
	if err := runner.ResumeAll(boot); err != nil {
		t.Fatal(err)
	}

	spans := sr.Ended()
	runs := oteltest.Named(spans, "run workflow")
	if len(runs) != 1 {
		t.Fatalf("resumed runs = %d", len(runs))
	}
	if !oteltest.IsRoot(runs[0]) {
		t.Error("a resumed run should start its own trace")
	}
	if !oteltest.LinksTo(runs[0], trace.SpanContextFromContext(boot)) {
		t.Error("a resumed run should link to whatever resumed it")
	}
	if got, _ := oteltest.Attr(runs[0], "dispatch.workflow.resumed"); got != "true" {
		t.Errorf("resumed = %q", got)
	}
	// "download" replays from its checkpoint; only "load" runs.
	if kinds := stepKinds(spans); kinds["step"] != 1 {
		t.Errorf("step spans after resume = %v, want one", kinds)
	}
}

func TestSpawnedChildIsItsOwnTrace(t *testing.T) {
	sr := oteltest.Record(t)
	runner, reg, _, _ := telemetryRunner(t)

	done := make(chan struct{})
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("child", func(*workflow.Workflow, struct{}) error {
		close(done)
		return nil
	}))
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("parent", func(wf *workflow.Workflow, _ struct{}) error {
		_, err := workflow.SpawnChild(wf, "child", struct{}{})
		return err
	}))

	if _, err := workflow.Start(context.Background(), runner, "parent", struct{}{}); err != nil {
		t.Fatal(err)
	}
	<-done
	runs := sr.WaitFor(t, "run workflow", 2, 5*time.Second)

	var parent, child sdktrace.ReadOnlySpan
	for _, r := range runs {
		if name, _ := oteltest.Attr(r, "dispatch.workflow.name"); name == "child" {
			child = r
		} else {
			parent = r
		}
	}
	spawn := oteltest.Named(sr.Ended(), "run workflow step")
	if len(spawn) != 1 {
		t.Fatalf("spawn step spans = %d", len(spawn))
	}
	if !oteltest.IsRoot(child) {
		t.Error("a spawned child should start its own trace")
	}
	if !oteltest.LinksTo(child, spawn[0].SpanContext()) {
		t.Error("a spawned child should link to the step that spawned it")
	}
	if child.SpanContext().TraceID() == parent.SpanContext().TraceID() {
		t.Error("child shares the parent's trace")
	}
}

func otelTracer() trace.Tracer { return telemetry.New().Tracer() }
