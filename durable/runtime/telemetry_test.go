package runtime_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/internal/metricstest"
	"github.com/xraph/dispatch/internal/oteltest"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/telemetry"
)

func TestClaimedTasksAreRootSpans(t *testing.T) {
	sr := oteltest.Record(t)
	s := memory.New()
	f := metricstest.New()
	options := workerOptions(t)
	options.Telemetry = telemetry.New(telemetry.WithMetrics(f))
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		return w.Activity("charge", "charge", "", []byte("alice@example.com")).Get()
	}
	options.Activities["charge"] = func(context.Context, drt.ActivityInfo, []byte) ([]byte, error) {
		return []byte("paid"), nil
	}
	w := newWorker(t, s, options)
	startWorkerRun(t, w, options)

	// A context carrying a span, as an extension's Start or a request
	// would hand over. Each task must still be its own trace.
	ctx := oteltest.SpanContext(t)
	for _, kind := range []durable.TaskKind{durable.TaskWorkflow, durable.TaskActivity, durable.TaskWorkflow} {
		if worked, err := w.RunOnce(ctx, kind); err != nil || !worked {
			t.Fatalf("run %s: worked=%t err=%v", kind, worked, err)
		}
	}

	spans := sr.Ended()
	tasks := oteltest.Named(spans, "process durable task")
	if len(tasks) != 3 {
		t.Fatalf("task spans = %d, want 3", len(tasks))
	}
	byKind := map[string][]int{}
	for i, task := range tasks {
		if !oteltest.IsRoot(task) {
			t.Errorf("task span %d has a parent", i)
		}
		if task.SpanKind() != trace.SpanKindConsumer {
			t.Errorf("task span %d kind = %v", i, task.SpanKind())
		}
		kind, _ := oteltest.Attr(task, "dispatch.durable.task.kind")
		byKind[kind] = append(byKind[kind], i)
		if got, _ := oteltest.Attr(task, "dispatch.durable.task.result"); got != telemetry.DurableOK {
			t.Errorf("task %s result = %q", kind, got)
		}
		if got, _ := oteltest.Attr(task, "messaging.destination.name"); got != "orders" {
			t.Errorf("task %s queue = %q", kind, got)
		}
	}
	if len(byKind["workflow"]) != 2 || len(byKind["activity"]) != 1 {
		t.Fatalf("task kinds = %v", byKind)
	}

	replays := oteltest.Named(spans, "replay workflow history")
	if len(replays) != 2 {
		t.Fatalf("replay spans = %d, want 2", len(replays))
	}
	if !oteltest.ChildOf(replays[0], tasks[byKind["workflow"][0]]) {
		t.Error("replay is not a child of its workflow task")
	}
	activities := oteltest.Named(spans, "run activity")
	if len(activities) != 1 || !oteltest.ChildOf(activities[0], tasks[byKind["activity"][0]]) {
		t.Fatalf("activity spans = %d or not under the activity task", len(activities))
	}
	if got, _ := oteltest.Attr(activities[0], "dispatch.durable.activity.name"); got != "charge" {
		t.Errorf("activity name = %q", got)
	}
	// Workflow and run IDs are caller-chosen, and inputs are payload.
	oteltest.AssertNoValue(t, spans, "alice@example.com")
	for _, task := range tasks {
		for _, a := range task.Attributes() {
			if a.Value.String() == "order" || a.Value.String() == "run" {
				t.Errorf("task span carries a workflow or run ID in %s", a.Key)
			}
		}
	}

	if n, ok := f.HistogramCount(telemetry.MetricDurableTaskDuration, "kind", "workflow", "result", "ok"); !ok || n != 2 {
		t.Errorf("workflow tasks = %d, %v", n, ok)
	}
	if n, ok := f.HistogramCount(telemetry.MetricDurableTaskDuration, "kind", "activity", "result", "ok"); !ok || n != 1 {
		t.Errorf("activity tasks = %d, %v", n, ok)
	}
	if v, ok := f.CounterValue(telemetry.MetricDurablePolls, "kind", "workflow", "result", telemetry.PollWork); !ok || v != 2 {
		t.Errorf("workflow work polls = %v, %v", v, ok)
	}
	if len(f.Series(telemetry.MetricDurableReplayDuration)) != 1 {
		t.Error("no replay duration series")
	}
}

// spanCheckingDurableStore records whether idle claim polls carry a span.
type spanCheckingDurableStore struct {
	*memory.Store
	mu      sync.Mutex
	claims  int
	traced  int
	claimed chan struct{}
}

func (s *spanCheckingDurableStore) ClaimTask(ctx context.Context, request durable.ClaimRequest) (*durable.Task, error) {
	s.mu.Lock()
	s.claims++
	if trace.SpanContextFromContext(ctx).IsValid() {
		s.traced++
	}
	if s.claims == 10 {
		close(s.claimed)
	}
	s.mu.Unlock()
	return s.Store.ClaimTask(ctx, request)
}

func TestIdleRunPollsCarryNoSpan(t *testing.T) {
	sr := oteltest.Record(t)
	s := &spanCheckingDurableStore{Store: memory.New(), claimed: make(chan struct{})}
	f := metricstest.New()
	options := workerOptions(t)
	options.Telemetry = telemetry.New(telemetry.WithMetrics(f))
	options.Workflows["order"] = func(*drt.Workflow, []byte) ([]byte, error) { return nil, nil }
	w := newWorker(t, s, options)

	ctx, cancel := context.WithCancel(oteltest.SpanContext(t))
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case <-s.claimed:
	case <-time.After(5 * time.Second):
		t.Fatal("worker never polled")
	}
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}

	s.mu.Lock()
	traced := s.traced
	s.mu.Unlock()
	if traced != 0 {
		t.Errorf("%d idle claims carried a span", traced)
	}
	if spans := sr.Ended(); len(spans) != 0 {
		t.Errorf("idle worker started %d spans", len(spans))
	}
	if v, ok := f.CounterValue(telemetry.MetricDurablePolls, "kind", "workflow", "result", telemetry.PollEmpty); !ok || v < 1 {
		t.Errorf("empty workflow polls = %v, %v", v, ok)
	}
}
