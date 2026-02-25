package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/id"
)

// PublishEvent persists a new event and makes it available for subscribers.
func (s *Store) PublishEvent(ctx context.Context, evt *event.Event) error {
	m := toEventModel(evt)
	_, err := s.sdb.NewInsert(m).Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: publish event: %w", err)
	}
	return nil
}

// SubscribeEvent waits for an unacked event matching the given name.
// Uses a polling approach with short intervals.
func (s *Store) SubscribeEvent(ctx context.Context, name string, timeout time.Duration) (*event.Event, error) {
	deadline := time.Now().Add(timeout)

	for {
		// Check context cancellation.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if time.Now().After(deadline) {
			return nil, nil
		}

		// Try to find an unacked event.
		m := new(eventModel)
		err := s.sdb.NewSelect(m).
			Where("name = ?", name).
			Where("acked = ?", false).
			OrderExpr("created_at ASC").
			Limit(1).
			Scan(ctx)
		if err != nil {
			if isNoRows(err) {
				// No event yet -- wait and retry.
				sleepCtx(ctx, 50*time.Millisecond)
				continue
			}
			return nil, fmt.Errorf("dispatch/sqlite: subscribe event: %w", err)
		}

		evt, convErr := fromEventModel(m)
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/sqlite: subscribe event convert: %w", convErr)
		}
		return evt, nil
	}
}

// AckEvent acknowledges an event, marking it as consumed.
func (s *Store) AckEvent(ctx context.Context, eventID id.EventID) error {
	res, err := s.sdb.NewUpdate((*eventModel)(nil)).
		Set("acked = ?", true).
		Where("id = ?", eventID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: ack event: %w", err)
	}
	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		return dispatch.ErrEventNotFound
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
