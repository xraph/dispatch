package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/id"
)

// ── JSON model for KV storage ──

type eventEntity struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Payload    []byte    `json:"payload,omitempty"`
	ScopeAppID string    `json:"scope_app_id"`
	ScopeOrgID string    `json:"scope_org_id"`
	Acked      bool      `json:"acked"`
	CreatedAt  time.Time `json:"created_at"`
}

func toEventEntity(evt *event.Event) *eventEntity {
	return &eventEntity{
		ID:         evt.ID.String(),
		Name:       evt.Name,
		Payload:    evt.Payload,
		ScopeAppID: evt.ScopeAppID,
		ScopeOrgID: evt.ScopeOrgID,
		Acked:      evt.Acked,
		CreatedAt:  evt.CreatedAt,
	}
}

func fromEventEntity(e *eventEntity) (*event.Event, error) {
	eID, err := id.ParseEventID(e.ID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: parse event id: %w", err)
	}

	return &event.Event{
		ID:         eID,
		Name:       e.Name,
		Payload:    e.Payload,
		ScopeAppID: e.ScopeAppID,
		ScopeOrgID: e.ScopeOrgID,
		Acked:      e.Acked,
		CreatedAt:  e.CreatedAt,
	}, nil
}

// PublishEvent persists a new event and adds it to the name's stream.
func (s *Store) PublishEvent(ctx context.Context, evt *event.Event) error {
	eID := evt.ID.String()
	key := eventKey(eID)

	e := toEventEntity(evt)
	if err := s.setEntity(ctx, key, e); err != nil {
		return fmt.Errorf("dispatch/redis: publish event set: %w", err)
	}

	// Add to the named stream so subscribers get notified.
	if err := s.rdb.XAdd(ctx, &goredis.XAddArgs{
		Stream: eventStreamKey(evt.Name),
		Values: map[string]interface{}{
			"event_id": eID,
		},
	}).Err(); err != nil {
		return fmt.Errorf("dispatch/redis: publish event stream: %w", err)
	}
	return nil
}

// SubscribeEvent waits for an unacked event matching the given name.
// Uses stream polling for efficient waiting.
func (s *Store) SubscribeEvent(ctx context.Context, name string, timeout time.Duration) (*event.Event, error) {
	stream := eventStreamKey(name)
	deadline := time.Now().Add(timeout)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if time.Now().After(deadline) {
			return nil, nil
		}

		// Read oldest messages from the stream.
		msgs, err := s.rdb.XRangeN(ctx, stream, "-", "+", 10).Result()
		if err != nil {
			return nil, fmt.Errorf("dispatch/redis: subscribe xrange: %w", err)
		}

		for _, msg := range msgs {
			eID, ok := msg.Values["event_id"].(string)
			if !ok {
				continue
			}

			key := eventKey(eID)
			var e eventEntity
			if getErr := s.getEntity(ctx, key, &e); getErr != nil {
				continue
			}
			if e.Acked {
				continue // already consumed
			}

			evt, convErr := fromEventEntity(&e)
			if convErr != nil {
				continue
			}
			return evt, nil
		}

		// No unacked event found -- wait a bit before retrying.
		remaining := time.Until(deadline)
		blockTime := 50 * time.Millisecond
		if blockTime > remaining {
			blockTime = remaining
		}
		sleepCtx(ctx, blockTime)
	}
}

// AckEvent acknowledges an event, marking it as consumed.
func (s *Store) AckEvent(ctx context.Context, eventID id.EventID) error {
	key := eventKey(eventID.String())

	var e eventEntity
	if err := s.getEntity(ctx, key, &e); err != nil {
		if isNotFound(err) {
			return dispatch.ErrEventNotFound
		}
		return fmt.Errorf("dispatch/redis: ack event get: %w", err)
	}

	e.Acked = true
	return s.setEntity(ctx, key, &e)
}
