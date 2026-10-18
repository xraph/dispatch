package ecosystem_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	"github.com/xraph/dispatch/internal/oteltest"
)

func TestRemoteCarriesTraceparentButNoBaggage(t *testing.T) {
	sr := oteltest.Record(t)
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	var mu sync.Mutex
	var headers http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		headers = r.Header.Clone()
		mu.Unlock()
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	remote, err := ecosystem.NewRemote(server.URL, "private-bearer", time.Second, true)
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()

	member, err := baggage.NewMember("user.email", "alice")
	if err != nil {
		t.Fatal(err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatal(err)
	}
	ctx, span := otel.Tracer("test").Start(baggage.ContextWithBaggage(context.Background(), bag), "deliver outbox event")
	b, d := envelope(t, durable.DestinationRelay)
	req, _ := ecosystem.RelayRequest(b, d)
	if _, err = remote.SendReliable(ctx, req); err == nil {
		t.Fatal("a 503 should fail the call")
	}
	span.End()

	mu.Lock()
	defer mu.Unlock()
	if headers.Get("Baggage") != "" {
		t.Errorf("baggage reached the remote host: %q", headers.Get("Baggage"))
	}
	if headers.Get("Traceparent") == "" {
		t.Error("the call carried no traceparent")
	}
	clients := 0
	for _, s := range sr.Ended() {
		if s.Parent().SpanID() == span.SpanContext().SpanID() {
			clients++
		}
	}
	if clients != 1 {
		t.Errorf("client spans under the delivery = %d, want 1", clients)
	}
}
