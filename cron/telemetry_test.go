package cron_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/internal/metricstest"
	"github.com/xraph/dispatch/internal/oteltest"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/telemetry"
)

// spanEnqueuer records the span context each enqueue ran under.
type spanEnqueuer struct {
	mu    sync.Mutex
	spans []trace.SpanContext
}

func (e *spanEnqueuer) Fn() cron.EnqueueFunc {
	return func(ctx context.Context, _ string, _ []byte, _ ...job.Option) (id.JobID, error) {
		e.mu.Lock()
		e.spans = append(e.spans, trace.SpanContextFromContext(ctx))
		e.mu.Unlock()
		return id.NewJobID(), nil
	}
}

func (e *spanEnqueuer) first() trace.SpanContext {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.spans) == 0 {
		return trace.SpanContext{}
	}
	return e.spans[0]
}

// spanCheckingCronStore records whether an idle tick's queries carry a span.
type spanCheckingCronStore struct {
	*memory.Store
	mu     sync.Mutex
	lists  int
	traced int
	listed chan struct{}
}

func (s *spanCheckingCronStore) ListCrons(ctx context.Context) ([]*cron.Entry, error) {
	s.mu.Lock()
	s.lists++
	if trace.SpanContextFromContext(ctx).IsValid() {
		s.traced++
	}
	if s.lists == 2 {
		close(s.listed)
	}
	s.mu.Unlock()
	return s.Store.ListCrons(ctx)
}

func leadingScheduler(t *testing.T, cs cron.Store, mem *memory.Store, enqueue cron.EnqueueFunc, tel *telemetry.Telemetry) *cron.Scheduler {
	t.Helper()
	ctx := context.Background()
	workerID := id.NewWorkerID()
	if err := mem.RegisterWorker(ctx, &cluster.Worker{ID: workerID, Hostname: "test", State: cluster.WorkerActive,
		LastSeen: time.Now().UTC(), CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if ok, err := mem.AcquireLeadership(ctx, workerID, 30*time.Second); err != nil || !ok {
		t.Fatalf("AcquireLeadership: %v %v", ok, err)
	}
	sched := cron.NewScheduler(cs, mem, enqueue, nil, workerID, nil,
		cron.WithTickInterval(10*time.Millisecond),
		cron.WithLeaderTTL(10*time.Second),
		cron.WithCronRefreshInterval(10*time.Millisecond),
		cron.WithTelemetry(tel),
	)
	t.Cleanup(func() { _ = sched.Stop(context.Background()) })
	return sched
}

func TestFireCronEntryIsARootSpanAroundTheEnqueue(t *testing.T) {
	sr := oteltest.Record(t)
	mem := memory.New()
	f := metricstest.New()
	enq := &spanEnqueuer{}
	sched := leadingScheduler(t, mem, mem, enq.Fn(), telemetry.New(telemetry.WithMetrics(f)))
	registerDueEntry(t, mem, "nightly-report", "send-report")

	if err := sched.Start(oteltest.SpanContext(t)); err != nil {
		t.Fatal(err)
	}
	fire := sr.WaitFor(t, "fire cron entry", 1, 5*time.Second)[0]

	if !oteltest.IsRoot(fire) {
		t.Error("fire cron entry has a parent")
	}
	for key, want := range map[string]string{
		"dispatch.cron.name":     "nightly-report",
		"dispatch.cron.job_name": "send-report",
		"dispatch.cron.result":   telemetry.CronFired,
	} {
		if got, _ := oteltest.Attr(fire, key); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
	if got := enq.first(); got.SpanID() != fire.SpanContext().SpanID() {
		t.Error("the cron enqueue did not run under the fire span")
	}

	if v, ok := f.CounterValue(telemetry.MetricCronFires, "result", telemetry.CronFired); !ok || v < 1 {
		t.Errorf("fired = %v, %v", v, ok)
	}
	if len(f.Series(telemetry.MetricCronFireLag)) != 1 {
		t.Error("no cron lag series")
	}
	if v, _ := f.GaugeValue(telemetry.MetricCronLeader); v != 1 {
		t.Errorf("leader gauge = %v, want 1", v)
	}
	if err := sched.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v, _ := f.GaugeValue(telemetry.MetricCronLeader); v != 0 {
		t.Errorf("leader gauge after Stop = %v, want 0", v)
	}
}

func TestIdleCronTicksStartNoSpans(t *testing.T) {
	sr := oteltest.Record(t)
	mem := memory.New()
	cs := &spanCheckingCronStore{Store: mem, listed: make(chan struct{})}
	sched := leadingScheduler(t, cs, mem, (&spanEnqueuer{}).Fn(), telemetry.New())

	if err := sched.Start(oteltest.SpanContext(t)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cs.listed:
	case <-time.After(5 * time.Second):
		t.Fatal("scheduler never listed its entries")
	}

	cs.mu.Lock()
	traced := cs.traced
	cs.mu.Unlock()
	if traced != 0 {
		t.Errorf("%d idle ListCrons calls carried a span", traced)
	}
	if spans := sr.Ended(); len(spans) != 0 {
		t.Errorf("idle scheduler started %d spans", len(spans))
	}
}
