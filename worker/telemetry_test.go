package worker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	log "github.com/xraph/go-utils/log"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/internal/metricstest"
	"github.com/xraph/dispatch/internal/oteltest"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/middleware"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/telemetry"
	"github.com/xraph/dispatch/worker"
)

const waitSpan = 5 * time.Second

// spanCtxExt records the span context every lifecycle hook ran under.
type spanCtxExt struct {
	mu   sync.Mutex
	seen map[string]trace.SpanContext
}

func (e *spanCtxExt) Name() string { return "span-ctx" }

func (e *spanCtxExt) record(ctx context.Context, hook string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.seen == nil {
		e.seen = map[string]trace.SpanContext{}
	}
	e.seen[hook] = trace.SpanContextFromContext(ctx)
}

func (e *spanCtxExt) get(hook string) trace.SpanContext {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.seen[hook]
}

func (e *spanCtxExt) OnJobStarted(ctx context.Context, _ *job.Job) error {
	e.record(ctx, "started")
	return nil
}

func (e *spanCtxExt) OnJobCompleted(ctx context.Context, _ *job.Job, _ time.Duration) error {
	e.record(ctx, "completed")
	return nil
}

func (e *spanCtxExt) OnJobDLQ(ctx context.Context, _ *job.Job, _ error) error {
	e.record(ctx, "dlq")
	return nil
}

type telemetryPool struct {
	pool    *worker.Pool
	store   *memory.Store
	reg     *job.Registry
	tel     *telemetry.Telemetry
	metrics *metricstest.Factory
	hooks   *spanCtxExt
}

func newTelemetryPool(t *testing.T, s job.Store, mem *memory.Store) *telemetryPool {
	t.Helper()
	logger := log.NewNoopLogger()
	reg := job.NewRegistry()
	extensions := ext.NewRegistry(logger)
	hooks := &spanCtxExt{}
	extensions.Register(hooks)
	f := metricstest.New()
	tel := telemetry.New(telemetry.WithMetrics(f), telemetry.WithQueues("default"))

	runner := worker.NewExecutor(reg, extensions, mem, dlq.NewService(mem, mem),
		backoff.NewConstant(time.Hour), logger, middleware.Recover(logger)).WithTelemetry(tel)
	pool := worker.NewPool(s, runner, extensions, logger,
		worker.WithPoolConcurrency(1),
		worker.WithPollInterval(10*time.Millisecond),
		worker.WithPoolQueues([]string{"default"}),
		worker.WithTelemetry(tel),
	)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pool.Stop(ctx)
	})
	return &telemetryPool{pool: pool, store: mem, reg: reg, tel: tel, metrics: f, hooks: hooks}
}

func (tp *telemetryPool) enqueue(t *testing.T, name string, payload any) *job.Job {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	j := &job.Job{ID: id.NewJobID(), Name: name, Queue: "default", Payload: data,
		State: job.StatePending, MaxRetries: 3, RunAt: now}
	j.CreatedAt, j.UpdatedAt = now, now
	if err := tp.store.EnqueueJob(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	return j
}

func childrenOf(spans []sdktrace.ReadOnlySpan, parent sdktrace.ReadOnlySpan, name string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range oteltest.Named(spans, name) {
		if oteltest.ChildOf(s, parent) {
			out = append(out, s)
		}
	}
	return out
}

func TestAttemptSpanOwnsTheTerminalPath(t *testing.T) {
	sr := oteltest.Record(t)
	mem := memory.New()
	tp := newTelemetryPool(t, mem, mem)
	job.RegisterDefinition(tp.reg, job.NewDefinition("greet", func(context.Context, struct{ Name string }) error { return nil }))

	// The process that enqueued the job remembered its span, so the attempt
	// links back to it.
	_, enqueueSpan := sdktrace.NewTracerProvider().Tracer("test").Start(context.Background(), "enqueue job")
	j := tp.enqueue(t, "greet", struct{ Name string }{"Ada"})
	tp.tel.RememberEnqueue(j.ID.String(), enqueueSpan.SpanContext())
	enqueueSpan.End()

	if err := tp.pool.Start(oteltest.SpanContext(t)); err != nil {
		t.Fatal(err)
	}
	process := sr.WaitFor(t, "process job", 1, waitSpan)[0]

	if !oteltest.IsRoot(process) {
		t.Error("process job has a parent; it must start its own trace")
	}
	if process.SpanKind() != trace.SpanKindConsumer {
		t.Errorf("kind = %v, want consumer", process.SpanKind())
	}
	if !oteltest.LinksTo(process, enqueueSpan.SpanContext()) {
		t.Error("process job does not link to the enqueue span")
	}
	for key, want := range map[string]string{
		"messaging.system":           "dispatch",
		"messaging.operation.type":   "process",
		"messaging.destination.name": "default",
		"messaging.message.id":       j.ID.String(),
		"dispatch.job.name":          "greet",
		"dispatch.job.attempt":       "1",
		"dispatch.job.outcome":       telemetry.OutcomeCompleted,
	} {
		if got, _ := oteltest.Attr(process, key); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
	if process.Status().Code != codes.Unset {
		t.Errorf("status = %v, want unset", process.Status())
	}

	spans := sr.Ended()
	complete := childrenOf(spans, process, "complete job")
	if len(complete) != 1 {
		t.Fatalf("complete job children = %d, want 1", len(complete))
	}
	// Hooks run inside the attempt's trace: JobStarted under the process
	// span itself, JobCompleted under the terminal write's span.
	if got := tp.hooks.get("started"); got.SpanID() != process.SpanContext().SpanID() {
		t.Error("JobStarted did not run under the process job span")
	}
	if got := tp.hooks.get("completed"); got.SpanID() != complete[0].SpanContext().SpanID() {
		t.Error("JobCompleted did not run under the complete job span")
	}

	if n, ok := tp.metrics.HistogramCount(telemetry.MetricJobAttemptDuration, "queue", "default", "outcome", "completed"); !ok || n != 1 {
		t.Errorf("attempt metric = %d, %v", n, ok)
	}
	if n, ok := tp.metrics.HistogramCount(telemetry.MetricJobQueueWait, "queue", "default"); !ok || n != 1 {
		t.Errorf("queue wait metric = %d, %v", n, ok)
	}
	if v, ok := tp.metrics.CounterValue(telemetry.MetricJobPolls, "result", telemetry.PollWork); !ok || v < 1 {
		t.Errorf("work polls = %v, %v", v, ok)
	}
	if v, _ := tp.metrics.GaugeValue(telemetry.MetricJobsActive); v != 0 {
		t.Errorf("active jobs after the attempt = %v, want 0", v)
	}
}

func TestAttemptSpanOnDeadLetterKeepsPayloadAndErrorOff(t *testing.T) {
	sr := oteltest.Record(t)
	mem := memory.New()
	tp := newTelemetryPool(t, mem, mem)
	job.RegisterDefinition(tp.reg, job.NewDefinition("charge", func(_ context.Context, p struct{ Email string }) error {
		return fmt.Errorf("%w: card for %s declined", dispatch.ErrPermanent, p.Email)
	}))
	tp.enqueue(t, "charge", struct{ Email string }{"alice@example.com"})

	if err := tp.pool.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	process := sr.WaitFor(t, "process job", 1, waitSpan)[0]

	if got, _ := oteltest.Attr(process, "dispatch.job.outcome"); got != telemetry.OutcomeDeadLettered {
		t.Errorf("outcome = %q", got)
	}
	if got, _ := oteltest.Attr(process, "dispatch.job.dlq_reason"); got != telemetry.DeadLetterPermanent {
		t.Errorf("dlq reason = %q", got)
	}
	if process.Status().Code != codes.Error || process.Status().Description != "handler error, dead-lettered" {
		t.Errorf("status = %+v", process.Status())
	}
	var excType string
	for _, ev := range process.Events() {
		if ev.Name == "exception" {
			for _, a := range ev.Attributes {
				if a.Key == "exception.type" {
					excType = a.Value.AsString()
				}
			}
		}
	}
	if excType != "dispatch.ErrPermanent" {
		t.Errorf("exception.type = %q", excType)
	}

	spans := sr.Ended()
	dead := childrenOf(spans, process, "dead-letter job")
	if len(dead) != 1 {
		t.Fatalf("dead-letter job children = %d, want 1", len(dead))
	}
	if got := tp.hooks.get("dlq"); got.SpanID() != dead[0].SpanContext().SpanID() {
		t.Error("JobDLQ did not run under the dead-letter job span")
	}
	if v, ok := tp.metrics.CounterValue(telemetry.MetricJobsDeadLettered, "queue", "default", "reason", "permanent"); !ok || v != 1 {
		t.Errorf("dead lettered metric = %v, %v", v, ok)
	}
	oteltest.AssertNoValue(t, spans, "alice@example.com")
}

func TestAttemptSpanOnRetry(t *testing.T) {
	sr := oteltest.Record(t)
	mem := memory.New()
	tp := newTelemetryPool(t, mem, mem)
	job.RegisterDefinition(tp.reg, job.NewDefinition("flaky", func(context.Context, struct{}) error {
		return fmt.Errorf("upstream returned 503")
	}))
	tp.enqueue(t, "flaky", struct{}{})

	if err := tp.pool.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	process := sr.WaitFor(t, "process job", 1, waitSpan)[0]

	if got, _ := oteltest.Attr(process, "dispatch.job.outcome"); got != telemetry.OutcomeRetry {
		t.Errorf("outcome = %q", got)
	}
	if process.Status().Description != "handler error, retrying" {
		t.Errorf("status = %+v", process.Status())
	}
	if len(childrenOf(sr.Ended(), process, "schedule job retry")) != 1 {
		t.Error("no schedule job retry child")
	}
	if n, ok := tp.metrics.HistogramCount(telemetry.MetricJobAttemptDuration, "queue", "default", "outcome", "retry"); !ok || n != 1 {
		t.Errorf("retry attempt metric = %d, %v", n, ok)
	}
}

// spanCheckingStore records whether any query the pool's background loops
// make carries a valid span.
type spanCheckingStore struct {
	*memory.Store
	mu      sync.Mutex
	polls   int
	traced  int
	polled  chan struct{}
	limitAt int
}

func (s *spanCheckingStore) DequeueJobs(ctx context.Context, opts job.DequeueOpts) ([]*job.Job, error) {
	s.mu.Lock()
	s.polls++
	if trace.SpanContextFromContext(ctx).IsValid() {
		s.traced++
	}
	if s.polls == s.limitAt {
		close(s.polled)
	}
	s.mu.Unlock()
	return s.Store.DequeueJobs(ctx, opts)
}

func TestIdlePollsCarryNoSpan(t *testing.T) {
	sr := oteltest.Record(t)
	mem := memory.New()
	s := &spanCheckingStore{Store: mem, polled: make(chan struct{}), limitAt: 3}
	tp := newTelemetryPool(t, s, mem)

	// Started under a context that carries a span, as forge's Start does.
	if err := tp.pool.Start(oteltest.SpanContext(t)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.polled:
	case <-time.After(waitSpan):
		t.Fatal("pool did not poll")
	}

	s.mu.Lock()
	traced := s.traced
	s.mu.Unlock()
	if traced != 0 {
		t.Errorf("%d idle store calls carried a span", traced)
	}
	if spans := sr.Ended(); len(spans) != 0 {
		t.Errorf("idle pool started %d spans", len(spans))
	}
	if v, ok := tp.metrics.CounterValue(telemetry.MetricJobPolls, "result", telemetry.PollEmpty); !ok || v < 1 {
		t.Errorf("empty polls = %v, %v", v, ok)
	}
}

func TestReclaimLeavesOneSpanPerPassThatDidWork(t *testing.T) {
	sr := oteltest.Record(t)
	ctx := context.Background()
	mem := memory.New()
	tp := newTelemetryPool(t, mem, mem)
	j := tp.enqueue(t, "reclaim-me", struct{}{})
	if _, err := mem.DequeueJobs(ctx, job.DequeueOpts{Queues: []string{"default"}, Limit: 1,
		WorkerID: id.NewWorkerID(), LeaseUntil: time.Now().UTC().Add(-time.Minute)}); err != nil {
		t.Fatal(err)
	}

	tp.pool.ReclaimOnce(ctx)
	tp.pool.ReclaimOnce(ctx) // nothing left: no span

	spans := oteltest.Named(sr.Ended(), "reclaim job leases")
	if len(spans) != 1 {
		t.Fatalf("reclaim spans = %d, want 1", len(spans))
	}
	if !oteltest.IsRoot(spans[0]) {
		t.Error("reclaim span has a parent")
	}
	if got, _ := oteltest.Attr(spans[0], "dispatch.lease.reclaimed"); got != "1" {
		t.Errorf("reclaimed = %q", got)
	}
	if v, ok := tp.metrics.CounterValue(telemetry.MetricJobsReclaimed); !ok || v != 1 {
		t.Errorf("reclaimed metric = %v, %v", v, ok)
	}
	got, err := mem.GetJob(ctx, j.ID)
	if err != nil || got.State != job.StatePending {
		t.Fatalf("job = %+v, %v", got, err)
	}
}

func TestRequeueCountsItsReason(t *testing.T) {
	mem := memory.New()
	tp := newTelemetryPool(t, mem, mem)
	j := tp.enqueue(t, "limited", struct{}{})

	tp.pool.RequeueRateLimitedForTest(context.Background(), j)
	tp.pool.RequeueUndispatchedForTest(j)

	if v, ok := tp.metrics.CounterValue(telemetry.MetricJobsRequeued, "queue", "default", "reason", telemetry.RequeueRateLimited); !ok || v != 1 {
		t.Errorf("rate limited = %v, %v", v, ok)
	}
	if v, ok := tp.metrics.CounterValue(telemetry.MetricJobsRequeued, "queue", "default", "reason", telemetry.RequeueShutdown); !ok || v != 1 {
		t.Errorf("shutdown = %v, %v", v, ok)
	}
}

// ctxHonouringStore fails writes whose context is done, as the SQL and
// Mongo backends do.
type ctxHonouringStore struct{ *memory.Store }

func (s ctxHonouringStore) UpdateJob(ctx context.Context, j *job.Job) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.Store.UpdateJob(ctx, j)
}

func TestShutdownDuringTheTerminalWriteIsCancelledNotStoreError(t *testing.T) {
	sr := oteltest.Record(t)
	mem := memory.New()
	s := ctxHonouringStore{Store: mem}
	f := metricstest.New()
	tel := telemetry.New(telemetry.WithMetrics(f), telemetry.WithQueues("default"))
	logger := log.NewNoopLogger()
	reg := job.NewRegistry()
	// The handler finishes after its attempt's context is cancelled, as an
	// in-flight job does when Pool.Stop cancels it.
	ctx, cancel := context.WithCancel(context.Background())
	job.RegisterDefinition(reg, job.NewDefinition("slow", func(context.Context, struct{}) error {
		cancel()
		return nil
	}))
	runner := worker.NewExecutor(reg, ext.NewRegistry(logger), s, dlq.NewService(mem, mem),
		backoff.NewConstant(time.Hour), logger).WithTelemetry(tel)
	now := time.Now().UTC()
	j := &job.Job{ID: id.NewJobID(), Name: "slow", Queue: "default", Payload: []byte(`{}`),
		State: job.StateRunning, MaxRetries: 3, RunAt: now}

	attemptCtx, attempt := tel.Tracer().Start(ctx, "process job")
	if err := runner.Execute(attemptCtx, j); err == nil {
		t.Fatal("the terminal write should have failed")
	}
	attempt.End()

	if n, ok := f.HistogramCount(telemetry.MetricJobAttemptDuration, "queue", "default", "outcome", telemetry.OutcomeCancelled); !ok || n != 1 {
		t.Errorf("cancelled attempts = %d, %v", n, ok)
	}
	if _, ok := f.HistogramCount(telemetry.MetricJobAttemptDuration, "queue", "default", "outcome", telemetry.OutcomeStoreError); ok {
		t.Error("a shutdown was recorded as a store error")
	}
	for _, span := range oteltest.Named(sr.Ended(), "complete job") {
		if span.Status().Code != codes.Unset {
			t.Errorf("complete job status = %+v, want unset", span.Status())
		}
	}
}
