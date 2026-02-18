package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/id"
)

// PublishEvent persists a new event and notifies subscribers via LISTEN/NOTIFY.
func (s *Store) PublishEvent(ctx context.Context, evt *event.Event) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO dispatch_events (
			id, name, payload, scope_app_id, scope_org_id, acked, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		evt.ID.String(), evt.Name, evt.Payload,
		evt.ScopeAppID, evt.ScopeOrgID, evt.Acked, evt.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("dispatch/postgres: publish event: %w", err)
	}

	// Notify listeners on the event channel.
	_, notifyErr := s.pool.Exec(ctx,
		`SELECT pg_notify('dispatch_events', $1)`,
		evt.Name,
	)
	if notifyErr != nil {
		// Log but don't fail — the event is persisted, subscribers will
		// fall back to polling.
		s.logger.Warn("failed to notify event subscribers",
			"event", evt.Name, "error", notifyErr)
	}

	return nil
}

// SubscribeEvent waits for an unacked event matching the given name.
// Uses a polling approach with short intervals. For production, consider
// upgrading to LISTEN/NOTIFY with a dedicated connection.
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

		// Try to find and claim an unacked event.
		row := s.pool.QueryRow(ctx, `
			SELECT id, name, payload, scope_app_id, scope_org_id, acked, created_at
			FROM dispatch_events
			WHERE name = $1 AND acked = FALSE
			ORDER BY created_at ASC
			LIMIT 1`,
			name,
		)

		evt, err := scanEvent(row)
		if err != nil {
			if isNoRows(err) {
				// No event yet — wait and retry.
				sleepCtx(ctx, 50*time.Millisecond)
				continue
			}
			return nil, fmt.Errorf("dispatch/postgres: subscribe event: %w", err)
		}
		return evt, nil
	}
}

// AckEvent acknowledges an event, marking it as consumed.
func (s *Store) AckEvent(ctx context.Context, eventID id.EventID) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE dispatch_events SET acked = TRUE WHERE id = $1`,
		eventID.String(),
	)
	if err != nil {
		return fmt.Errorf("dispatch/postgres: ack event: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return dispatch.ErrEventNotFound
	}
	return nil
}

// scanEvent scans a single event row.
func scanEvent(row pgx.Row) (*event.Event, error) {
	var (
		evt   event.Event
		idStr string
	)
	err := row.Scan(
		&idStr, &evt.Name, &evt.Payload,
		&evt.ScopeAppID, &evt.ScopeOrgID, &evt.Acked, &evt.CreatedAt,
	)
	if err != nil {
		return nil, err
	}

	parsedID, parseErr := id.ParseEventID(idStr)
	if parseErr != nil {
		return nil, fmt.Errorf("dispatch/postgres: parse event id %q: %w", idStr, parseErr)
	}
	evt.ID = parsedID

	return &evt, nil
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
