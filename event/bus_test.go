package event_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/store/memory"
)

func TestBus_PublishSubscribe(t *testing.T) {
	s := memory.New()
	bus := event.NewBus(s)

	ctx := context.Background()

	// Publish an event.
	evt, err := bus.Publish(ctx, "order.created", []byte(`{"id":"ord_1"}`), "app_1", "org_1")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if evt.Name != "order.created" {
		t.Errorf("Name = %q, want %q", evt.Name, "order.created")
	}
	if string(evt.Payload) != `{"id":"ord_1"}` {
		t.Errorf("Payload = %q, want %q", string(evt.Payload), `{"id":"ord_1"}`)
	}
	if evt.ScopeAppID != "app_1" {
		t.Errorf("ScopeAppID = %q, want %q", evt.ScopeAppID, "app_1")
	}

	// Subscribe should find the event.
	got, err := bus.Subscribe(ctx, "order.created", 1*time.Second)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if got == nil {
		t.Fatal("expected event, got nil")
	}
	if got.ID != evt.ID {
		t.Errorf("event ID = %s, want %s", got.ID, evt.ID)
	}
}

func TestBus_SubscribeTimeout(t *testing.T) {
	s := memory.New()
	bus := event.NewBus(s)

	ctx := context.Background()

	// Subscribe with a very short timeout — no events published.
	got, err := bus.Subscribe(ctx, "nonexistent", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil event on timeout, got %+v", got)
	}
}

func TestBus_Ack(t *testing.T) {
	s := memory.New()
	bus := event.NewBus(s)

	ctx := context.Background()

	evt, err := bus.Publish(ctx, "ack-test", nil, "", "")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Ack the event.
	if ackErr := bus.Ack(ctx, evt.ID); ackErr != nil {
		t.Fatalf("Ack: %v", ackErr)
	}

	// After ack, Subscribe should not find the event (it's acked).
	got, err := bus.Subscribe(ctx, "ack-test", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("Subscribe after ack: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil after ack, got %+v", got)
	}
}

func TestBus_Store(t *testing.T) {
	s := memory.New()
	bus := event.NewBus(s)

	if bus.Store() == nil {
		t.Fatal("expected non-nil store")
	}
}
