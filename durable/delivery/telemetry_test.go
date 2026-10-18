package delivery_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery"
	"github.com/xraph/dispatch/internal/metricstest"
	"github.com/xraph/dispatch/internal/oteltest"
	"github.com/xraph/dispatch/telemetry"
)

func TestEachDeliveryIsARootProducerSpan(t *testing.T) {
	sr := oteltest.Record(t)
	s := fixture(t, 1)
	f := metricstest.New()
	var failures atomic.Int64
	var sinkSpan atomic.Value
	sink := sinkFunc(func(ctx context.Context, d durable.Delivery) (durable.SinkReceipt, error) {
		sinkSpan.Store(trace.SpanContextFromContext(ctx))
		// The first attempt fails with a message quoting a value; the retry
		// is acknowledged.
		if failures.Add(1) == 1 {
			return durable.SinkReceipt{}, errors.New("relay at alice@example.com unavailable")
		}
		return receipt(d), nil
	})
	p, err := delivery.New(s, config("one"), map[durable.Destination]delivery.Sink{durable.DestinationChronicle: sink},
		delivery.WithTelemetry(telemetry.New(telemetry.WithMetrics(f))))
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Start(); err != nil {
		t.Fatal(err)
	}
	spans := sr.WaitFor(t, "deliver outbox event", 2, 5*time.Second)
	stop(t, p)

	results := map[string]int{}
	for _, span := range spans {
		if !oteltest.IsRoot(span) {
			t.Error("delivery span has a parent")
		}
		if span.SpanKind() != trace.SpanKindProducer {
			t.Errorf("kind = %v", span.SpanKind())
		}
		if got, _ := oteltest.Attr(span, "dispatch.outbox.destination"); got != "chronicle" {
			t.Errorf("destination = %q", got)
		}
		result, _ := oteltest.Attr(span, "dispatch.outbox.result")
		results[result]++
		if result == telemetry.OutboxUnavailable && span.Status().Code != codes.Error {
			t.Error("a failed delivery should have an error status")
		}
	}
	if results[telemetry.OutboxUnavailable] != 1 || results[telemetry.OutboxAcknowledged] < 1 {
		t.Errorf("results = %v", results)
	}
	if sc, _ := sinkSpan.Load().(trace.SpanContext); !sc.IsValid() {
		t.Error("the sink did not run under the delivery span")
	}
	oteltest.AssertNoValue(t, sr.Ended(), "alice@example.com")

	if n, ok := f.HistogramCount(telemetry.MetricOutboxDeliveryDuration, "destination", "chronicle", "result", "unavailable"); !ok || n != 1 {
		t.Errorf("unavailable deliveries = %d, %v", n, ok)
	}
	if n, ok := f.HistogramCount(telemetry.MetricOutboxDeliveryLag, "destination", "chronicle"); !ok || n < 1 {
		t.Errorf("delivery lag observations = %d, %v", n, ok)
	}
}

func TestIdlePublisherStartsNoSpans(t *testing.T) {
	sr := oteltest.Record(t)
	s := fixture(t, 0)
	polled := make(chan struct{})
	var closed atomic.Bool
	store := &pollSignalStore{OutboxStore: s, polled: polled, closed: &closed}
	sink := sinkFunc(func(_ context.Context, d durable.Delivery) (durable.SinkReceipt, error) { return receipt(d), nil })
	p, err := delivery.New(store, config("one"), map[durable.Destination]delivery.Sink{durable.DestinationChronicle: sink},
		delivery.WithTelemetry(telemetry.New()))
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-polled:
	case <-time.After(5 * time.Second):
		t.Fatal("publisher never polled")
	}
	stop(t, p)
	if spans := sr.Ended(); len(spans) != 0 {
		t.Errorf("idle publisher started %d spans", len(spans))
	}
}

// pollSignalStore closes polled once the publisher has claimed a few times.
type pollSignalStore struct {
	durable.OutboxStore
	polled chan struct{}
	closed *atomic.Bool
	claims atomic.Int64
}

func (s *pollSignalStore) ClaimDeliveries(ctx context.Context, claim durable.DeliveryClaim) ([]durable.DeliveryRecord, error) {
	if s.claims.Add(1) == 5 && s.closed.CompareAndSwap(false, true) {
		close(s.polled)
	}
	return s.OutboxStore.ClaimDeliveries(ctx, claim)
}
