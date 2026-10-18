// Package oteltest installs a span recorder for Dispatch's tests and checks
// what the recorded spans may and may not carry.
package oteltest

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// Recorder is a span recorder that also signals each span's end, so a test
// can wait for a span without sleeping.
type Recorder struct {
	*tracetest.SpanRecorder
	ended chan struct{}
}

// OnEnd implements sdktrace.SpanProcessor.
func (r *Recorder) OnEnd(s sdktrace.ReadOnlySpan) {
	r.SpanRecorder.OnEnd(s)
	select {
	case r.ended <- struct{}{}:
	default:
	}
}

// WaitFor blocks until n spans called name have ended, or fails the test
// after timeout, and returns them.
func (r *Recorder) WaitFor(t *testing.T, name string, n int, timeout time.Duration) []sdktrace.ReadOnlySpan {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if got := Named(r.Ended(), name); len(got) >= n {
			return got
		}
		select {
		case <-r.ended:
		case <-deadline.C:
			t.Fatalf("timed out waiting for %d %q spans; have %d", n, name, len(Named(r.Ended(), name)))
			return nil
		}
	}
}

// NewRecorder returns a Recorder that is not installed anywhere. Register it
// with forge.RegisterSpanProcessor to record a Forge app's spans.
func NewRecorder() *Recorder {
	return &Recorder{SpanRecorder: tracetest.NewSpanRecorder(), ended: make(chan struct{}, 1)}
}

// Record installs a recording tracer provider as the global provider and
// restores the previous one when the test ends. Tests that call it must not
// run in parallel with other tests that read the global provider.
func Record(t *testing.T) *Recorder {
	t.Helper()
	r := NewRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(r))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background()) //nolint:errcheck // test cleanup
	})
	return r
}

// SpanContext returns a context carrying a started span from a throwaway
// provider, standing in for the boot or request span a long-lived loop must
// not keep. End is called for you when the test ends.
func SpanContext(t *testing.T) context.Context {
	t.Helper()
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("oteltest").Start(context.Background(), "start dispatch")
	t.Cleanup(func() {
		span.End()
		_ = tp.Shutdown(context.Background()) //nolint:errcheck // test cleanup
	})
	return ctx
}

// Named returns the ended spans called name.
func Named(spans []sdktrace.ReadOnlySpan, name string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range spans {
		if s.Name() == name {
			out = append(out, s)
		}
	}
	return out
}

// Attr returns the value of key on s as a string, and whether it is set.
func Attr(s sdktrace.ReadOnlySpan, key string) (string, bool) {
	for _, a := range s.Attributes() {
		if string(a.Key) == key {
			return a.Value.String(), true
		}
	}
	return "", false
}

// IsRoot reports whether s has no parent.
func IsRoot(s sdktrace.ReadOnlySpan) bool {
	return !s.Parent().IsValid()
}

// ChildOf reports whether child's parent is parent.
func ChildOf(child, parent sdktrace.ReadOnlySpan) bool {
	return child.Parent().SpanID() == parent.SpanContext().SpanID() &&
		child.Parent().TraceID() == parent.SpanContext().TraceID()
}

// LinksTo reports whether s links to sc.
func LinksTo(s sdktrace.ReadOnlySpan, sc trace.SpanContext) bool {
	for _, l := range s.Links() {
		if l.SpanContext.SpanID() == sc.SpanID() && l.SpanContext.TraceID() == sc.TraceID() {
			return true
		}
	}
	return false
}

// AssertNoValue fails when any span quotes needle in its name, status,
// attributes or event attributes. Feed a personal value (an email address)
// through a payload or an error and check it never reaches a span.
func AssertNoValue(t *testing.T, spans []sdktrace.ReadOnlySpan, needle string) {
	t.Helper()
	for _, s := range spans {
		if strings.Contains(s.Name(), needle) || strings.Contains(s.Status().Description, needle) {
			t.Errorf("span %q quotes %q in its name or status", s.Name(), needle)
		}
		for _, a := range s.Attributes() {
			if strings.Contains(a.Value.String(), needle) {
				t.Errorf("span %q attribute %s quotes %q", s.Name(), a.Key, needle)
			}
		}
		for _, ev := range s.Events() {
			for _, a := range ev.Attributes {
				if strings.Contains(a.Value.String(), needle) {
					t.Errorf("span %q event %q attribute %s quotes %q", s.Name(), ev.Name, a.Key, needle)
				}
			}
		}
	}
}
