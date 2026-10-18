package telemetry

import (
	"context"
	"errors"
	"fmt"
	"sync"

	gu "github.com/xraph/go-utils/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// ScopeName is the instrumentation scope on every span Dispatch starts.
const ScopeName = "github.com/xraph/dispatch"

// OtherQueue is the queue label for any queue outside the configured set.
// Queue names are chosen by whoever enqueues, so only the names you
// configured (WithQueues) become label values. Everything else collapses
// into this one.
const OtherQueue = "other"

// Telemetry carries the tracer provider and metric factory Dispatch records
// through. Build it once with New and share it; it is safe for concurrent
// use. A nil *Telemetry records no metrics and starts spans on the global
// tracer provider.
type Telemetry struct {
	tp      trace.TracerProvider
	factory gu.MetricFactory
	queues  map[string]struct{}
	links   *linkCache

	jobsOnce      sync.Once
	jobs          *JobMetrics
	cronOnce      sync.Once
	cron          *CronMetrics
	workflowOnce  sync.Once
	workflow      *WorkflowMetrics
	durableOnce   sync.Once
	durable       *DurableMetrics
	outboxOnce    sync.Once
	outbox        *OutboxMetrics
	artifactsOnce sync.Once
	artifacts     *ArtifactMetrics
}

// Option configures a Telemetry.
type Option func(*Telemetry)

// WithMetrics records Dispatch's metrics through f. In a Forge app that is
// app.Metrics(). The factory has to key series by name and labels, as
// Forge's does. A nil factory leaves metrics off.
func WithMetrics(f gu.MetricFactory) Option {
	return func(t *Telemetry) { t.factory = f }
}

// WithTracerProvider starts Dispatch's spans on tp. A nil tp, the default,
// means the global provider, looked up on every span so a provider installed
// later (forge.New, a test recorder) still receives them.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(t *Telemetry) { t.tp = tp }
}

// WithQueues names the queues that may appear as a metric label. Pass the
// queues your workers poll and the ones you configured limits for. Any other
// queue name is recorded as OtherQueue.
func WithQueues(names ...string) Option {
	return func(t *Telemetry) {
		for _, n := range names {
			if n != "" && n != OtherQueue {
				t.queues[n] = struct{}{}
			}
		}
	}
}

// New builds a Telemetry.
func New(opts ...Option) *Telemetry {
	t := &Telemetry{queues: map[string]struct{}{}, links: newLinkCache(defaultLinkCapacity)}
	for _, opt := range opts {
		opt(t)
	}

	return t
}

// Tracer returns the tracer Dispatch's spans start on.
func (t *Telemetry) Tracer() trace.Tracer {
	if t == nil || t.tp == nil {
		return otel.Tracer(ScopeName)
	}

	return t.tp.Tracer(ScopeName)
}

// Start starts a span under Dispatch's instrumentation scope. The caller
// ends it.
func (t *Telemetry) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return t.Tracer().Start(ctx, name, opts...) //nolint:spancheck // the caller ends the span
}

// StartChild starts a span only when ctx already carries a local Dispatch
// span, using that span's own tracer provider. Code that has no Telemetry of
// its own (the staging middleware) uses it to nest under the job attempt it
// runs in without a span of its own outside one. The caller ends it.
func StartChild(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	parent := trace.SpanFromContext(ctx)

	return parent.TracerProvider().Tracer(ScopeName).Start(ctx, name, opts...) //nolint:spancheck // the caller ends the span
}

// QueueLabel maps a queue name to its metric label: the name itself when it
// is one of the configured queues, OtherQueue otherwise.
func (t *Telemetry) QueueLabel(queue string) string {
	if t == nil {
		return OtherQueue
	}
	if _, ok := t.queues[queue]; ok {
		return queue
	}

	return OtherQueue
}

func (t *Telemetry) queueValues() []string {
	out := make([]string, 0, len(t.queues)+1)
	for q := range t.queues {
		out = append(out, q)
	}

	return append(out, OtherQueue)
}

// Detach returns ctx without its span or baggage, keeping its values,
// deadline and cancellation. A long-lived loop calls it once on the context
// it was started with. forge v1.12.9 hands an extension's Start a context
// that carries the "start <ext>" boot span, and a request context carries
// the request's server span; without this, every query an idle poll makes
// would land in that long-finished trace.
func Detach(ctx context.Context) context.Context {
	ctx = trace.ContextWithSpanContext(ctx, trace.SpanContext{})

	return baggage.ContextWithoutBaggage(ctx)
}

// Fail marks span as failed. class is a short, bounded description (a
// failure class such as "handler error", never err.Error()), and the
// exception event carries only exception.type: the error's Go type or a
// sentinel's name. Driver and handler messages quote values (payload
// fragments, keys, addresses), and none of that belongs on a span.
func Fail(span trace.Span, class, errType string) {
	span.SetStatus(codes.Error, class)
	if errType != "" {
		span.AddEvent("exception", trace.WithAttributes(semconv.ExceptionType(errType)))
	}
}

// ErrorType names err for an exception event: "context.Canceled" or
// "context.DeadlineExceeded" when the chain holds one, otherwise the Go type
// of the innermost error, such as "*net.OpError". It returns "" for nil.
func ErrorType(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "context.Canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "context.DeadlineExceeded"
	}
	for {
		next := errors.Unwrap(err)
		if next == nil {
			return fmt.Sprintf("%T", err)
		}
		err = next
	}
}

// RememberEnqueue records the span context that enqueued a job, so an
// attempt this process runs can link back to it. Nothing about it is stored
// with the job: the cache is in memory, bounded, and an attempt on another
// worker simply starts without a link.
func (t *Telemetry) RememberEnqueue(jobID string, sc trace.SpanContext) {
	if t == nil || !sc.IsValid() {
		return
	}
	t.links.put(jobID, sc)
}

// EnqueueLinks returns a link to the span that enqueued jobID, when this
// process enqueued it recently enough to remember.
func (t *Telemetry) EnqueueLinks(jobID string) []trace.Link {
	if t == nil {
		return nil
	}
	sc, ok := t.links.get(jobID)
	if !ok {
		return nil
	}

	return []trace.Link{{SpanContext: sc}}
}

// defaultLinkCapacity bounds the enqueue link cache. An entry is a job ID
// and a span context, so this is well under a megabyte.
const defaultLinkCapacity = 4096

// linkCache is a bounded FIFO map from job ID to the span context that
// enqueued it. Entries are kept after a read so retries link too, and the
// oldest entry goes when the cache is full.
type linkCache struct {
	mu    sync.Mutex
	size  int
	order []string
	next  int
	items map[string]trace.SpanContext
}

func newLinkCache(capacity int) *linkCache {
	return &linkCache{size: capacity, order: make([]string, 0, capacity), items: make(map[string]trace.SpanContext, capacity)}
}

func (c *linkCache) put(key string, sc trace.SpanContext) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.items[key]; ok {
		c.items[key] = sc
		return
	}
	if len(c.order) < c.size {
		c.order = append(c.order, key)
	} else {
		delete(c.items, c.order[c.next])
		c.order[c.next] = key
		c.next = (c.next + 1) % c.size
	}
	c.items[key] = sc
}

func (c *linkCache) get(key string) (trace.SpanContext, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	sc, ok := c.items[key]

	return sc, ok
}
