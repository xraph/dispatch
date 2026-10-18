package extension_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xraph/forge"
	"go.opentelemetry.io/otel"

	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/extension"
	"github.com/xraph/dispatch/internal/oteltest"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

func scrape(t *testing.T, app forge.App) string {
	t.Helper()
	p, ok := app.Metrics().(interface{ PrometheusHandler() http.Handler })
	if !ok {
		t.Fatalf("app metrics %T have no Prometheus handler", app.Metrics())
	}
	rr := httptest.NewRecorder()
	p.PrometheusHandler().ServeHTTP(rr, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/_/metrics", nil))
	body, err := io.ReadAll(rr.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestExtensionRecordsThroughForge(t *testing.T) {
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})

	app := forge.New(forge.WithAppName("dispatch-telemetry-test"), forge.WithAppLogger(forge.NewNoopLogger()))
	rec := oteltest.NewRecorder()
	if err := forge.RegisterSpanProcessor(app, rec); err != nil {
		t.Fatalf("RegisterSpanProcessor: %v", err)
	}

	ext := extension.New(extension.WithStore(memory.New()), extension.WithConcurrency(1),
		extension.WithQueues([]string{"default"}))
	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}
	engine.Register(ext.Engine(), job.NewDefinition("send-email", func(context.Context, struct{ To string }) error { return nil }))

	// forge starts every extension inside a "start <ext>" span.
	startCtx, boot := forge.StartSpan(t.Context(), "start dispatch")
	if err := ext.Start(startCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	boot.End()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = ext.Stop(ctx)
	})

	if _, err := engine.Enqueue(t.Context(), ext.Engine(), "send-email", struct{ To string }{"alice@example.com"}); err != nil {
		t.Fatal(err)
	}
	process := rec.WaitFor(t, "process job", 1, 5*time.Second)[0]
	if process.InstrumentationScope().Name != "github.com/xraph/dispatch" {
		t.Errorf("scope = %q", process.InstrumentationScope().Name)
	}
	for _, s := range rec.Ended() {
		if s.SpanContext().TraceID() == boot.SpanContext().TraceID() && s.Name() != "start dispatch" {
			t.Errorf("%q landed in the boot trace", s.Name())
		}
	}

	body := scrape(t, app)
	for _, want := range []string{
		`forge_dispatch_jobs_enqueued_total{queue="default"} 1`,
		`forge_dispatch_job_attempt_duration_seconds_count{outcome="completed",queue="default"} 1`,
		`forge_dispatch_job_queue_wait_seconds_count{queue="default"} 1`,
		`forge_dispatch_job_polls_total{result="work"}`,
		`forge_dispatch_jobs_active 0`,
		`forge_dispatch_cron_leader`,
		// The legacy counters keep their names.
		`forge_dispatch_job_enqueued 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape is missing %s", want)
		}
	}
	if strings.Contains(body, "alice") {
		t.Error("a payload value reached the scrape")
	}
}
