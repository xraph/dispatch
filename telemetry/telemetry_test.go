package telemetry_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/dispatch/internal/metricstest"
	"github.com/xraph/dispatch/internal/oteltest"
	"github.com/xraph/dispatch/telemetry"
)

func TestQueueLabelCollapsesUnconfiguredQueues(t *testing.T) {
	tel := telemetry.New(telemetry.WithQueues("default", "emails", ""))

	for queue, want := range map[string]string{
		"default":           "default",
		"emails":            "emails",
		"tenant-42":         telemetry.OtherQueue,
		"alice@example.com": telemetry.OtherQueue,
		"":                  telemetry.OtherQueue,
	} {
		if got := tel.QueueLabel(queue); got != want {
			t.Errorf("QueueLabel(%q) = %q, want %q", queue, got, want)
		}
	}

	var nilTel *telemetry.Telemetry
	if got := nilTel.QueueLabel("default"); got != telemetry.OtherQueue {
		t.Errorf("nil Telemetry QueueLabel = %q", got)
	}
}

func TestMetricsRecordOnlyDocumentedLabelValues(t *testing.T) {
	f := metricstest.New()
	tel := telemetry.New(telemetry.WithMetrics(f), telemetry.WithQueues("default"))
	jobs := tel.Jobs()

	jobs.Attempt("default", telemetry.OutcomeCompleted, time.Second)
	jobs.Attempt(tel.QueueLabel("customer-queue"), telemetry.OutcomeRetry, time.Second)
	// Neither value is in its label's set, so neither creates a series.
	jobs.Attempt("customer-queue", telemetry.OutcomeCompleted, time.Second)
	jobs.Attempt("default", "boom: alice@example.com", time.Second)

	if n, ok := f.HistogramCount(telemetry.MetricJobAttemptDuration, "queue", "default", "outcome", "completed"); !ok || n != 1 {
		t.Fatalf("completed attempt count = %d, %v", n, ok)
	}
	if n, ok := f.HistogramCount(telemetry.MetricJobAttemptDuration, "queue", "other", "outcome", "retry"); !ok || n != 1 {
		t.Fatalf("collapsed queue count = %d, %v", n, ok)
	}
	if got := len(f.Series(telemetry.MetricJobAttemptDuration)); got != 2 {
		t.Fatalf("attempt series = %d, want 2: %v", got, f.Series(telemetry.MetricJobAttemptDuration))
	}
	for _, labels := range f.Series(telemetry.MetricJobAttemptDuration) {
		if len(labels) != 2 || labels["queue"] == "" || labels["outcome"] == "" {
			t.Errorf("series labels = %v, want exactly queue and outcome", labels)
		}
	}
}

func TestNilTelemetryIsSafe(t *testing.T) {
	var tel *telemetry.Telemetry
	tel.Jobs().Attempt("default", telemetry.OutcomeCompleted, time.Second)
	tel.Cron().Leader(true)
	tel.Workflows().Step("step", "ok", time.Second)
	tel.Durable().Task("workflow", "ok", time.Second)
	tel.Outbox().Delivery("relay", telemetry.OutboxAcknowledged, time.Second)
	tel.Artifacts().BytesReclaimed(10)
	tel.RememberEnqueue("job", trace.SpanContext{})
	if links := tel.EnqueueLinks("job"); links != nil {
		t.Fatalf("nil Telemetry links = %v", links)
	}
	_, span := tel.Start(context.Background(), "noop")
	span.End()

	// A Telemetry without a factory records nothing either.
	if telemetry.New().Jobs() != nil {
		t.Fatal("Jobs() without a factory should be nil")
	}
}

func TestDetachDropsSpanAndBaggage(t *testing.T) {
	ctx := oteltest.SpanContext(t)
	member, err := baggage.NewMember("user", "alice")
	if err != nil {
		t.Fatal(err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatal(err)
	}
	ctx = baggage.ContextWithBaggage(ctx, bag)
	ctx, cancel := context.WithCancel(ctx)

	detached := telemetry.Detach(ctx)
	if trace.SpanContextFromContext(detached).IsValid() {
		t.Fatal("Detach kept the span")
	}
	if baggage.FromContext(detached).Len() != 0 {
		t.Fatal("Detach kept the baggage")
	}
	cancel()
	if detached.Err() == nil {
		t.Fatal("Detach dropped cancellation")
	}
}

func TestEnqueueLinksAreBounded(t *testing.T) {
	tel := telemetry.New()
	sc := func(i int) trace.SpanContext {
		var tid trace.TraceID
		var sid trace.SpanID
		tid[0], sid[0] = byte(i%250+1), byte(i%250+1)
		tid[1], sid[1] = byte(i/250+1), byte(i/250+1)
		return trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled})
	}

	tel.RememberEnqueue("first", sc(0))
	if links := tel.EnqueueLinks("first"); len(links) != 1 || links[0].SpanContext.SpanID() != sc(0).SpanID() {
		t.Fatalf("links = %v", links)
	}
	// A retry reads it again.
	if links := tel.EnqueueLinks("first"); len(links) != 1 {
		t.Fatalf("second read lost the link: %v", links)
	}
	// An invalid span context is never remembered.
	tel.RememberEnqueue("untraced", trace.SpanContext{})
	if links := tel.EnqueueLinks("untraced"); links != nil {
		t.Fatalf("untraced links = %v", links)
	}

	for i := 1; i <= 5000; i++ {
		tel.RememberEnqueue(fmt.Sprintf("job-%d", i), sc(i))
	}
	if links := tel.EnqueueLinks("first"); links != nil {
		t.Fatal("the oldest entry outlived the cache bound")
	}
	if links := tel.EnqueueLinks("job-5000"); len(links) != 1 {
		t.Fatal("the newest entry is missing")
	}
}

func TestFailRecordsClassAndTypeOnly(t *testing.T) {
	sr := oteltest.Record(t)
	_, span := telemetry.New().Start(context.Background(), "work")
	err := fmt.Errorf("deliver to alice@example.com: %w", context.DeadlineExceeded)
	telemetry.Fail(span, "delivery failed", telemetry.ErrorType(err))
	span.End()

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("spans = %d", len(spans))
	}
	if spans[0].Status().Code != codes.Error || spans[0].Status().Description != "delivery failed" {
		t.Fatalf("status = %+v", spans[0].Status())
	}
	if got := spans[0].Events()[0].Attributes[0].Value.AsString(); got != "context.DeadlineExceeded" {
		t.Fatalf("exception.type = %q", got)
	}
	oteltest.AssertNoValue(t, spans, "alice@example.com")
}

type innerError struct{ msg string }

func (e *innerError) Error() string { return e.msg }

func TestErrorTypeNamesInnermostError(t *testing.T) {
	inner := &innerError{msg: "alice@example.com"}
	for err, want := range map[error]string{
		nil:                                             "",
		fmt.Errorf("outer: %w", inner):                  "*telemetry_test.innerError",
		fmt.Errorf("stop: %w", context.Canceled):        "context.Canceled",
		errors.New("dial tcp 10.0.0.1: refused"):        "*errors.errorString",
		fmt.Errorf("a: %w", fmt.Errorf("b: %w", inner)): "*telemetry_test.innerError",
	} {
		if got := telemetry.ErrorType(err); got != want {
			t.Errorf("ErrorType(%v) = %q, want %q", err, got, want)
		}
	}
}
