package event

import (
	"context"
	"time"

	"github.com/xraph/dispatch/id"
)

// Store defines the persistence contract for events.
type Store interface {
	// PublishEvent persists a new event and makes it available for subscribers.
	PublishEvent(ctx context.Context, evt *Event) error

	// SubscribeEvent waits for an unacked event matching the given name.
	// Blocks until an event is available or the timeout expires.
	// Returns nil if no event is found within the timeout.
	SubscribeEvent(ctx context.Context, name string, timeout time.Duration) (*Event, error)

	// AckEvent acknowledges an event, marking it as consumed.
	AckEvent(ctx context.Context, eventID id.EventID) error
}
