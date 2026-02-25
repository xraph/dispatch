package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/id"
)

// PublishEvent persists a new event and makes it available for subscribers.
func (s *Store) PublishEvent(ctx context.Context, evt *event.Event) error {
	m := toEventModel(evt)
	_, err := s.mdb.NewInsert(m).Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: publish event: %w", err)
	}
	return nil
}

// SubscribeEvent waits for an unacked event matching the given name.
// Uses a polling approach with short intervals.
func (s *Store) SubscribeEvent(ctx context.Context, name string, timeout time.Duration) (*event.Event, error) {
	deadline := time.Now().Add(timeout)
	col := s.mdb.Collection(colEvents)

	findOpts := options.FindOne().SetSort(bson.D{{Key: "created_at", Value: 1}})

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
		var m eventModel
		err := col.FindOne(ctx, bson.M{
			"name":  name,
			"acked": false,
		}, findOpts).Decode(&m)
		if err != nil {
			if isNoDocuments(err) {
				// No event yet -- wait and retry.
				sleepCtx(ctx, 50*time.Millisecond)
				continue
			}
			return nil, fmt.Errorf("dispatch/mongo: subscribe event: %w", err)
		}

		evt, convErr := fromEventModel(&m)
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/mongo: subscribe event convert: %w", convErr)
		}
		return evt, nil
	}
}

// AckEvent acknowledges an event, marking it as consumed.
func (s *Store) AckEvent(ctx context.Context, eventID id.EventID) error {
	col := s.mdb.Collection(colEvents)
	res, err := col.UpdateOne(ctx,
		bson.M{"_id": eventID.String()},
		bson.M{"$set": bson.M{"acked": true}},
	)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: ack event: %w", err)
	}
	if res.MatchedCount == 0 {
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
