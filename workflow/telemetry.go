package workflow

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/dispatch/telemetry"
)

// Step kinds, the kind label on dispatch_workflow_step_duration_seconds and
// the dispatch.workflow.step.kind span attribute.
const (
	stepKindStep       = "step"
	stepKindParallel   = "parallel"
	stepKindFanOut     = "fan_out"
	stepKindChild      = "child"
	stepKindSpawn      = "spawn"
	stepKindWaitEvent  = "wait_event"
	stepKindWaitAll    = "wait_all"
	stepKindWaitAny    = "wait_any"
	stepKindWaitMatch  = "wait_match"
	stepKindSleep      = "sleep"
	stepKindCompensate = "compensate"
)

// WithTelemetry sets where the runner records its spans and metrics: a
// "run workflow" span per run, a "run workflow step" span per step that
// actually runs (a step replayed from its checkpoint gets none), and the run
// and step duration histograms. It returns r so callers can chain it onto
// NewRunner. engine.Build passes the engine's own.
func (r *Runner) WithTelemetry(t *telemetry.Telemetry) *Runner {
	r.tel = t

	return r
}

// startRun starts the span for one run. opts lets a background run (a
// resume, a replay, a spawned child) start as its own root with a link to
// whatever started it.
func (r *Runner) startRun(ctx context.Context, run *Run, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	opts = append(opts, trace.WithAttributes(
		attribute.String("dispatch.workflow.name", run.Name),
		attribute.String("dispatch.workflow.run_id", run.ID.String()),
		attribute.Int("dispatch.workflow.version", run.Version),
	))

	return r.tel.Start(ctx, "run workflow", opts...)
}

// detachedRun returns ctx without its span, for a run that outlives the
// caller, and the span start options that make the run its own root linked
// back to the caller's span.
func detachedRun(ctx context.Context) (detached context.Context, opts []trace.SpanStartOption) {
	opts = []trace.SpanStartOption{trace.WithNewRoot()}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: sc}))
	}

	return telemetry.Detach(ctx), opts
}

// startStep starts the span and clock for a step that is about to run. The
// returned func ends both; pass it the step's error.
func (w *Workflow) startStep(kind, name string) (stepCtx context.Context, done func(error)) {
	ctx, span := w.tel.Start(w.ctx, "run workflow step", trace.WithAttributes(
		attribute.String("dispatch.workflow.step.kind", kind),
		attribute.String("dispatch.workflow.step.name", name),
	))
	started := time.Now()

	return ctx, func(err error) {
		result := telemetry.ResultOK
		if err != nil {
			result = telemetry.ResultError
			telemetry.Fail(span, "step failed", telemetry.ErrorType(err))
		}
		w.tel.Workflows().Step(kind, result, time.Since(started))
		span.End()
	}
}
