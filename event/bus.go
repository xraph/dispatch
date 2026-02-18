// Package event provides the event bus for workflow coordination, supporting
// both in-memory and store-backed event publish/subscribe with WaitForEvent.
package event

import (
	"context"
	"time"

	"github.com/xraph/dispatch/id"
)

// Bus provides high-level publish/subscribe operations over an event Store.
// Workflows use the Bus via WaitForEvent; external code publishes events
// through it to trigger workflow continuations.
type Bus struct {
	store Store
}

// NewBus creates an event bus backed by the given store.
func NewBus(store Store) *Bus {
	return &Bus{store: store}
}

// Publish creates and persists a new event, making it available for subscribers.
func (b *Bus) Publish(ctx context.Context, name string, payload []byte, scopeAppID, scopeOrgID string) (*Event, error) {
	evt := &Event{
		ID:         id.NewEventID(),
		Name:       name,
		Payload:    payload,
		ScopeAppID: scopeAppID,
		ScopeOrgID: scopeOrgID,
		CreatedAt:  time.Now().UTC(),
	}
	if err := b.store.PublishEvent(ctx, evt); err != nil {
		return nil, err
	}
	return evt, nil
}

// Subscribe waits for an unacked event matching the given name.
// Blocks until available or timeout. Returns nil on timeout.
func (b *Bus) Subscribe(ctx context.Context, name string, timeout time.Duration) (*Event, error) {
	return b.store.SubscribeEvent(ctx, name, timeout)
}

// Ack acknowledges an event, marking it as consumed.
func (b *Bus) Ack(ctx context.Context, eventID id.EventID) error {
	return b.store.AckEvent(ctx, eventID)
}

// Store returns the underlying event store.
func (b *Bus) Store() Store { return b.store }
