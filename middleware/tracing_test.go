package middleware_test

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	mw "github.com/xraph/dispatch/middleware"
)

func setupTestTracer() (*tracetest.SpanRecorder, trace.Tracer) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	tracer := tp.Tracer("test")
	return sr, tracer
}

func newTestJob() *job.Job {
	return &job.Job{
		ID:         id.NewJobID(),
		Name:       "send-email",
		Queue:      "default",
		RetryCount: 2,
		ScopeAppID: "app_123",
		ScopeOrgID: "org_456",
	}
}

func TestTracing_CreatesSpan(t *testing.T) {
	sr, tracer := setupTestTracer()
	m := mw.TracingWithTracer(tracer)
	j := newTestJob()

	err := m(context.Background(), j, func(_ context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	if spans[0].Name() != "dispatch.job.execute" {
		t.Errorf("expected span name %q, got %q", "dispatch.job.execute", spans[0].Name())
	}
}

func TestTracing_SpanAttributes(t *testing.T) {
	sr, tracer := setupTestTracer()
	m := mw.TracingWithTracer(tracer)
	j := newTestJob()

	_ = m(context.Background(), j, func(_ context.Context) error {
		return nil
	})

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	attrs := spans[0].Attributes()
	expected := map[string]interface{}{
		"dispatch.job.id":       j.ID.String(),
		"dispatch.job.name":     "send-email",
		"dispatch.queue":        "default",
		"dispatch.retry_count":  int64(2),
		"dispatch.scope.app_id": "app_123",
		"dispatch.scope.org_id": "org_456",
	}

	attrMap := make(map[string]interface{}, len(attrs))
	for _, a := range attrs {
		switch a.Value.Type() {
		case attribute.STRING:
			attrMap[string(a.Key)] = a.Value.AsString()
		case attribute.INT64:
			attrMap[string(a.Key)] = a.Value.AsInt64()
		}
	}

	for key, want := range expected {
		got, ok := attrMap[key]
		if !ok {
			t.Errorf("missing attribute %q", key)
			continue
		}
		if got != want {
			t.Errorf("attribute %q = %v, want %v", key, got, want)
		}
	}
}

func TestTracing_Success_SetsOkStatus(t *testing.T) {
	sr, tracer := setupTestTracer()
	m := mw.TracingWithTracer(tracer)
	j := newTestJob()

	_ = m(context.Background(), j, func(_ context.Context) error {
		return nil
	})

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	if spans[0].Status().Code != codes.Ok {
		t.Errorf("expected status Ok, got %v", spans[0].Status().Code)
	}
}

func TestTracing_Error_SetsErrorStatus(t *testing.T) {
	sr, tracer := setupTestTracer()
	m := mw.TracingWithTracer(tracer)
	j := newTestJob()

	handlerErr := errors.New("handler failed")
	err := m(context.Background(), j, func(_ context.Context) error {
		return handlerErr
	})
	if !errors.Is(err, handlerErr) {
		t.Fatalf("expected handler error, got %v", err)
	}

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	if spans[0].Status().Code != codes.Error {
		t.Errorf("expected status Error, got %v", spans[0].Status().Code)
	}
	if spans[0].Status().Description != "handler failed" {
		t.Errorf("expected status description %q, got %q", "handler failed", spans[0].Status().Description)
	}

	// Verify error event was recorded.
	events := spans[0].Events()
	found := false
	for _, ev := range events {
		if ev.Name == "exception" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'exception' event to be recorded on span")
	}
}

func TestTracing_PropagatesContext(t *testing.T) {
	sr, tracer := setupTestTracer()
	m := mw.TracingWithTracer(tracer)
	j := newTestJob()

	var handlerSpanCtx trace.SpanContext
	_ = m(context.Background(), j, func(ctx context.Context) error {
		handlerSpanCtx = trace.SpanFromContext(ctx).SpanContext()
		return nil
	})

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	// The handler should have received the span context from the middleware.
	if !handlerSpanCtx.IsValid() {
		t.Error("expected valid span context in handler, got invalid")
	}
	if handlerSpanCtx.TraceID() != spans[0].SpanContext().TraceID() {
		t.Error("handler span context trace ID does not match middleware span")
	}
}

func TestTracing_DefaultNoopSafe(t *testing.T) {
	// Calling Tracing() without a global provider should not panic.
	m := mw.Tracing()
	j := newTestJob()

	called := false
	err := m(context.Background(), j, func(_ context.Context) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("handler was not called")
	}
}
