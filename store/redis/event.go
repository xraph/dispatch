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

// PublishEvent persists a new event and adds it to the name's stream.
func (s *Store) PublishEvent(ctx context.Context, evt *event.Event) error {
	eID := evt.ID.String()
	key := eventKey(eID)

	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, key,
		"id", eID,
		"name", evt.Name,
		"payload", string(evt.Payload),
		"scope_app", evt.ScopeAppID,
		"scope_org", evt.ScopeOrgID,
		"acked", "0",
		"created_at", evt.CreatedAt.Format(time.RFC3339Nano),
	)
	// Add to the named stream so subscribers get notified.
	pipe.XAdd(ctx, &goredis.XAddArgs{
		Stream: eventStreamKey(evt.Name),
		Values: map[string]interface{}{
			"event_id": eID,
		},
	})
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/redis: publish event: %w", err)
	}
	return nil
}

// SubscribeEvent waits for an unacked event matching the given name.
// Uses XREAD BLOCK for efficient blocking.
func (s *Store) SubscribeEvent(ctx context.Context, name string, timeout time.Duration) (*event.Event, error) {
	stream := eventStreamKey(name)

	// Use XREAD with BLOCK to wait for new messages.
	// We read from the beginning to find unacked events first.
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

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, nil
		}

		// Read oldest messages from the stream.
		msgs, err := s.client.XRangeN(ctx, stream, "-", "+", 10).Result()
		if err != nil {
			return nil, fmt.Errorf("dispatch/redis: subscribe xrange: %w", err)
		}

		for _, msg := range msgs {
			eID, ok := msg.Values["event_id"].(string)
			if !ok {
				continue
			}

			key := eventKey(eID)
			acked, getErr := s.client.HGet(ctx, key, "acked").Result()
			if getErr != nil {
				continue
			}
			if acked == "1" {
				continue // already consumed
			}

			// Found an unacked event.
			vals, hErr := s.client.HGetAll(ctx, key).Result()
			if hErr != nil || len(vals) == 0 {
				continue
			}

			evt, convErr := mapToEvent(vals)
			if convErr != nil {
				continue
			}
			return evt, nil
		}

		// No unacked event found — wait a bit before retrying.
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

	exists, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: ack event exists: %w", err)
	}
	if exists == 0 {
		return dispatch.ErrEventNotFound
	}

	_, err = s.client.HSet(ctx, key, "acked", "1").Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: ack event: %w", err)
	}
	return nil
}

// sleepCtx sleeps for the given duration, or returns early if the context
// is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func mapToEvent(m map[string]string) (*event.Event, error) {
	eID, err := id.ParseEventID(m["id"])
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: parse event id: %w", err)
	}

	createdAt, _ := time.Parse(time.RFC3339Nano, m["created_at"]) //nolint:errcheck // best-effort parse from trusted Redis data

	return &event.Event{
		ID:         eID,
		Name:       m["name"],
		Payload:    []byte(m["payload"]),
		ScopeAppID: m["scope_app"],
		ScopeOrgID: m["scope_org"],
		Acked:      m["acked"] == "1",
		CreatedAt:  createdAt,
	}, nil
}
