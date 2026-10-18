package runtime

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/telemetry"
)

// Span attribute keys on durable task spans. Workflow and run IDs are
// chosen by whoever starts the workflow, so they stay off spans as well as
// metrics; the kind, queue, namespace and attempt are deployment values.
const (
	attrTaskKind      = attribute.Key("dispatch.durable.task.kind")
	attrTaskAttempt   = attribute.Key("dispatch.durable.task.attempt")
	attrNamespace     = attribute.Key("dispatch.durable.namespace")
	attrBuildID       = attribute.Key("dispatch.durable.build_id")
	attrTaskResult    = attribute.Key("dispatch.durable.task.result")
	attrHistoryEvents = attribute.Key("dispatch.durable.history.events")
	attrActivityName  = attribute.Key("dispatch.durable.activity.name")
	attrDeliveryKind  = attribute.Key("dispatch.durable.child_delivery.kind")
	attrTimeoutKind   = attribute.Key("dispatch.durable.timeout.kind")
)

// recordPoll counts one claim poll for kind.
func (w *Worker) recordPoll(kind durable.TaskKind, claimed bool, err error) {
	result := telemetry.PollEmpty
	switch {
	case err != nil:
		result = telemetry.PollError
	case claimed:
		result = telemetry.PollWork
	}
	w.options.Telemetry.Durable().Poll(string(kind), result)
}

// startTask starts the root span for one claimed task. A claim that came
// back empty never gets here, so an idle worker polling every
// PollInterval starts no traces. The returned func ends the span and
// records the task's duration; pass it the task's final error.
func (w *Worker) startTask(ctx context.Context, kind durable.TaskKind, queue string, attempt int64, attrs ...attribute.KeyValue) (taskCtx context.Context, finish func(error)) {
	attrs = append(attrs,
		semconv.MessagingSystemKey.String("dispatch"),
		semconv.MessagingOperationTypeProcess,
		semconv.MessagingOperationName("process"),
		semconv.MessagingDestinationName(queue),
		attrTaskKind.String(string(kind)),
		attrTaskAttempt.Int64(attempt),
		attrNamespace.String(w.options.Namespace),
		attrBuildID.String(w.options.BuildID),
	)
	ctx, span := w.options.Telemetry.Start(ctx, "process durable task",
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attrs...),
	)
	started := time.Now()

	return ctx, func(err error) {
		result := taskResult(err)
		span.SetAttributes(attrTaskResult.String(result))
		if result == telemetry.DurableError {
			telemetry.Fail(span, "task failed", durableErrorType(err))
		}
		w.options.Telemetry.Durable().Task(string(kind), result, time.Since(started))
		span.End()
	}
}

// taskResult classifies how a claimed task ended. Contention, a lost lease,
// a handler this build lacks and a stopping worker are all ordinary: the
// task stays for another attempt, so none of them is an error.
func taskResult(err error) string {
	switch {
	case err == nil:
		return telemetry.DurableOK
	case errors.Is(err, ErrHandlerNotFound):
		return telemetry.DurableNoHandler
	case errors.Is(err, durable.ErrLeaseLost):
		return telemetry.DurableLeaseLost
	case normalContention(err):
		return telemetry.DurableContention
	case errors.Is(err, context.Canceled), errors.Is(err, ErrWorkerDraining), errors.Is(err, ErrDrainIncomplete):
		return telemetry.DurableCancelled
	default:
		return telemetry.DurableError
	}
}

// durableErrorType names a task failure for its exception event: a durable
// sentinel's name where one matches, else the innermost error's Go type.
func durableErrorType(err error) string {
	for _, s := range []struct {
		err  error
		name string
	}{
		{ErrHistory, "runtime.ErrHistory"},
		{durable.ErrInvalid, "durable.ErrInvalid"},
		{ErrNondeterministic, "runtime.ErrNondeterministic"},
	} {
		if errors.Is(err, s.err) {
			return s.name
		}
	}

	return telemetry.ErrorType(err)
}

// replay evaluates a workflow's history under a "replay workflow history"
// span and records how long it took.
func (w *Worker) replay(ctx context.Context, execution durable.Execution, events []durable.Event, handler WorkflowFunc) (Decision, error) {
	_, span := w.options.Telemetry.Start(ctx, "replay workflow history",
		trace.WithAttributes(attrHistoryEvents.Int(len(events))))
	defer span.End()

	started := time.Now()
	decision, err := Evaluate(execution, events, handler)
	w.options.Telemetry.Durable().Replay(time.Since(started))
	if err != nil {
		telemetry.Fail(span, "replay failed", durableErrorType(err))
	}

	return decision, err
}

// startActivity starts the span around one call into activity code.
func (w *Worker) startActivity(ctx context.Context, name string) (context.Context, trace.Span) {
	return w.options.Telemetry.Start(ctx, "run activity",
		trace.WithAttributes(attrActivityName.String(name)))
}

// endActivity ends an activity span. A failure the activity returned is
// the activity's answer, recorded in history and retried by policy; only
// an error calling it at all marks the span failed.
func endActivity(span trace.Span, outcome Outcome, err error) {
	switch {
	case err != nil:
		telemetry.Fail(span, "activity call failed", durableErrorType(err))
	case outcome.Failure != nil:
		telemetry.Fail(span, "activity failed", "")
	}
	span.End()
}
