package worker

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/telemetry"
)

// Span attribute keys the job attempt spans carry.
const (
	attrJobName       = attribute.Key("dispatch.job.name")
	attrJobAttempt    = attribute.Key("dispatch.job.attempt")
	attrJobMaxRetries = attribute.Key("dispatch.job.max_retries")
	attrLeaseEpoch    = attribute.Key("dispatch.job.lease_epoch")
	attrOutcome       = attribute.Key("dispatch.job.outcome")
	attrExecStatus    = attribute.Key("dispatch.job.exec_status")
	attrExecutor      = attribute.Key("dispatch.job.executor")
	attrDLQReason     = attribute.Key("dispatch.job.dlq_reason")
	attrScopeAppID    = attribute.Key("dispatch.scope.app_id")
	attrScopeOrgID    = attribute.Key("dispatch.scope.org_id")
	attrReclaimed     = attribute.Key("dispatch.lease.reclaimed")
	attrOutputs       = attribute.Key("dispatch.job.outputs")
)

// messagingSystem is the messaging.system value on job spans.
var messagingSystem = semconv.MessagingSystemKey.String("dispatch")

// attempt is what one job attempt reports back to whoever owns its span.
// Runner.Execute fills it in on the way through its terminal paths; the pool
// reads it when the attempt returns.
type attempt struct {
	outcome    string
	errType    string
	execStatus string
	dlqReason  string
	executor   string
}

type attemptKey struct{}

func withAttempt(ctx context.Context, a *attempt) context.Context {
	return context.WithValue(ctx, attemptKey{}, a)
}

func attemptFrom(ctx context.Context) *attempt {
	a, ok := ctx.Value(attemptKey{}).(*attempt)
	if !ok {
		return nil
	}

	return a
}

// setOutcome records how the attempt ended. cause is the failure that led
// there, nil for a success or a benign end.
func setOutcome(ctx context.Context, outcome string, cause error) {
	a := attemptFrom(ctx)
	if a == nil {
		return
	}
	a.outcome = outcome
	if cause == nil {
		return
	}
	a.errType = jobErrorType(cause)
	var execErr *exec.Error
	if errors.As(cause, &execErr) {
		a.execStatus = string(execErr.Status)
	}
}

// failureClass is the span status description for an outcome. Benign ends
// (completed, a lost lease, an operator's cancel) are not failures.
func failureClass(outcome string) string {
	switch outcome {
	case telemetry.OutcomeRetry:
		return "handler error, retrying"
	case telemetry.OutcomeDeadLettered:
		return "handler error, dead-lettered"
	case telemetry.OutcomeRequeued:
		return "launch failed, requeued"
	case telemetry.OutcomeStoreError:
		return "store error"
	default:
		return ""
	}
}

// jobErrorType names a job failure for an exception event without its
// message: a Dispatch sentinel's name where one is in the chain, else the
// innermost error's Go type.
func jobErrorType(err error) string {
	var execErr *exec.Error
	switch {
	case errors.Is(err, job.ErrLeaseLost):
		return "job.ErrLeaseLost"
	case errors.As(err, &execErr):
		return "*exec.Error"
	case errors.Is(err, dispatch.ErrPermanent):
		return "dispatch.ErrPermanent"
	default:
		return telemetry.ErrorType(err)
	}
}

// startAttemptSpan starts the root span for one job attempt. The attempt is
// its own trace: the job crossed the store to get here, and the span that
// enqueued it (if this process did) is a link, not a parent.
func (p *Pool) startAttemptSpan(j *job.Job) (context.Context, trace.Span) {
	attrs := []attribute.KeyValue{
		messagingSystem,
		semconv.MessagingOperationTypeProcess,
		semconv.MessagingOperationName("process"),
		semconv.MessagingDestinationName(j.Queue),
		semconv.MessagingMessageID(j.ID.String()),
		attrJobName.String(j.Name),
		attrJobAttempt.Int(j.RetryCount + 1),
		attrJobMaxRetries.Int(j.MaxRetries),
		attrLeaseEpoch.Int(j.LeaseEpoch),
	}
	if j.ScopeAppID != "" {
		attrs = append(attrs, attrScopeAppID.String(j.ScopeAppID))
	}
	if j.ScopeOrgID != "" {
		attrs = append(attrs, attrScopeOrgID.String(j.ScopeOrgID))
	}

	return p.tel.Start(p.cancelCtx, "process job",
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithLinks(p.tel.EnqueueLinks(j.ID.String())...),
		trace.WithAttributes(attrs...),
	)
}

// endAttemptSpan writes what the attempt reported onto its span.
func endAttemptSpan(span trace.Span, a *attempt) {
	if a.outcome != "" {
		span.SetAttributes(attrOutcome.String(a.outcome))
	}
	if a.execStatus != "" {
		span.SetAttributes(attrExecStatus.String(a.execStatus))
	}
	if a.dlqReason != "" {
		span.SetAttributes(attrDLQReason.String(a.dlqReason))
	}
	if a.executor != "" {
		span.SetAttributes(attrExecutor.String(a.executor))
	}
	if class := failureClass(a.outcome); class != "" {
		telemetry.Fail(span, class, a.errType)
	}
}
