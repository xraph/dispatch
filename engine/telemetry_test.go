package engine_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/internal/metricstest"
	"github.com/xraph/dispatch/internal/oteltest"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/telemetry"
)

// beatCheckingStore records whether the worker-row heartbeat, which runs
// for the life of the engine, carries a span.
type beatCheckingStore struct {
	*memory.Store
	mu     sync.Mutex
	beats  int
	traced int
	beat   chan struct{}
}

func (s *beatCheckingStore) HeartbeatWorker(ctx context.Context, workerID id.WorkerID) error {
	s.mu.Lock()
	s.beats++
	if trace.SpanContextFromContext(ctx).IsValid() {
		s.traced++
	}
	if s.beats == 1 {
		close(s.beat)
	}
	s.mu.Unlock()
	return s.Store.HeartbeatWorker(ctx, workerID)
}

func TestEnqueueAndAttemptAreLinkedAndLoopsKeepNoBootSpan(t *testing.T) {
	sr := oteltest.Record(t)
	s := &beatCheckingStore{Store: memory.New(), beat: make(chan struct{})}
	d, err := dispatch.New(dispatch.WithStore(s), dispatch.WithConcurrency(1),
		dispatch.WithQueues([]string{"default"}), dispatch.WithPollInterval(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	f := metricstest.New()
	eng, err := engine.Build(d, engine.WithMetricFactory(f))
	if err != nil {
		t.Fatal(err)
	}
	engine.Register(eng, job.NewDefinition("send-email", func(context.Context, emailPayload) error { return nil }))

	ctx, request := eng.Telemetry().Tracer().Start(context.Background(), "POST /signup")
	j, err := engine.Enqueue(ctx, eng, "send-email", emailPayload{To: "alice@example.com", Subject: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	request.End()
	// A queue nobody configured: its name must not become a label value.
	if _, err = engine.Enqueue(context.Background(), eng, "send-email", emailPayload{To: "bob@example.com"},
		job.WithQueue("customer-chosen")); err != nil {
		t.Fatal(err)
	}

	// Started under a context carrying a span, as forge's Start is.
	if err = eng.Start(oteltest.SpanContext(t)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = eng.Stop(stopCtx)
	})
	select {
	case <-s.beat:
	case <-time.After(5 * time.Second):
		t.Fatal("worker heartbeat never ran")
	}

	sr.WaitFor(t, "process job", 1, 5*time.Second)
	spans := sr.Ended()

	var enqueue []trace.SpanContext
	for _, span := range oteltest.Named(spans, "enqueue job") {
		if id, _ := oteltest.Attr(span, "messaging.message.id"); id == j.ID.String() {
			if span.SpanKind() != trace.SpanKindProducer {
				t.Errorf("enqueue kind = %v", span.SpanKind())
			}
			if span.Parent().SpanID() != request.SpanContext().SpanID() {
				t.Error("enqueue job is not a child of the request span")
			}
			enqueue = append(enqueue, span.SpanContext())
		}
	}
	if len(enqueue) != 1 {
		t.Fatalf("enqueue spans for the job = %d", len(enqueue))
	}

	// The default chain's tracing middleware nests under the attempt.
	handlerSpans := 0
	process := oteltest.Named(spans, "process job")[0]
	if !oteltest.IsRoot(process) || !oteltest.LinksTo(process, enqueue[0]) {
		t.Error("process job should be a root linked to its enqueue span")
	}
	for _, h := range oteltest.Named(spans, "run job handler") {
		if oteltest.ChildOf(h, process) {
			handlerSpans++
		}
	}
	if handlerSpans != 1 {
		t.Errorf("run job handler spans under the attempt = %d", handlerSpans)
	}
	oteltest.AssertNoValue(t, spans, "alice@example.com")

	s.mu.Lock()
	traced := s.traced
	s.mu.Unlock()
	if traced != 0 {
		t.Errorf("%d worker heartbeats carried the boot span", traced)
	}

	// The unconfigured queue collapses into "other".
	if v, ok := f.CounterValue(telemetry.MetricJobsEnqueued, "queue", telemetry.OtherQueue); !ok || v != 1 {
		t.Errorf("enqueued{other} = %v, %v", v, ok)
	}
	if v, ok := f.CounterValue(telemetry.MetricJobsEnqueued, "queue", "default"); !ok || v != 1 {
		t.Errorf("enqueued{default} = %v, %v", v, ok)
	}
	for _, labels := range f.Series(telemetry.MetricJobsEnqueued) {
		if q := labels["queue"]; q != "default" && q != telemetry.OtherQueue {
			t.Errorf("queue label %q leaked into a metric", q)
		}
	}
}
