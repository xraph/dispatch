package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
)

// PushDLQ adds a failed job entry to the dead letter queue.
func (s *Store) PushDLQ(ctx context.Context, entry *dlq.Entry) error {
	m := toDLQModel(entry)
	_, err := s.mdb.NewInsert(m).Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: push dlq: %w", err)
	}
	return nil
}

// ListDLQ returns DLQ entries matching the given options.
func (s *Store) ListDLQ(ctx context.Context, opts dlq.ListOpts) ([]*dlq.Entry, error) {
	col := s.mdb.Collection(colDLQ)
	filter := bson.M{}

	if opts.Queue != "" {
		filter["queue"] = opts.Queue
	}

	findOpts := options.Find().SetSort(bson.D{{Key: "failed_at", Value: 1}})
	if opts.Limit > 0 {
		findOpts.SetLimit(int64(opts.Limit))
	}
	if opts.Offset > 0 {
		findOpts.SetSkip(int64(opts.Offset))
	}

	cursor, err := col.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, fmt.Errorf("dispatch/mongo: list dlq: %w", err)
	}
	defer cursor.Close(ctx)

	var models []dlqEntryModel
	if err := cursor.All(ctx, &models); err != nil {
		return nil, fmt.Errorf("dispatch/mongo: list dlq decode: %w", err)
	}

	entries := make([]*dlq.Entry, 0, len(models))
	for i := range models {
		e, convErr := fromDLQModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/mongo: list dlq convert: %w", convErr)
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// GetDLQ retrieves a DLQ entry by ID.
func (s *Store) GetDLQ(ctx context.Context, entryID id.DLQID) (*dlq.Entry, error) {
	col := s.mdb.Collection(colDLQ)
	var m dlqEntryModel
	err := col.FindOne(ctx, bson.M{"_id": entryID.String()}).Decode(&m)
	if err != nil {
		if isNoDocuments(err) {
			return nil, dispatch.ErrDLQNotFound
		}
		return nil, fmt.Errorf("dispatch/mongo: get dlq: %w", err)
	}
	return fromDLQModel(&m)
}

// ReplayDLQ marks a DLQ entry as replayed.
func (s *Store) ReplayDLQ(ctx context.Context, entryID id.DLQID) error {
	col := s.mdb.Collection(colDLQ)
	t := now()

	res, err := col.UpdateOne(ctx,
		bson.M{"_id": entryID.String()},
		bson.M{"$set": bson.M{"replayed_at": t}},
	)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: replay dlq: %w", err)
	}
	if res.MatchedCount == 0 {
		return dispatch.ErrDLQNotFound
	}
	return nil
}

// PurgeDLQ removes DLQ entries with FailedAt before the given time.
// Returns the number of entries removed.
func (s *Store) PurgeDLQ(ctx context.Context, before time.Time) (int64, error) {
	col := s.mdb.Collection(colDLQ)
	res, err := col.DeleteMany(ctx, bson.M{
		"failed_at": bson.M{"$lt": before},
	})
	if err != nil {
		return 0, fmt.Errorf("dispatch/mongo: purge dlq: %w", err)
	}
	return res.DeletedCount, nil
}

// CountDLQ returns the total number of entries in the dead letter queue.
func (s *Store) CountDLQ(ctx context.Context) (int64, error) {
	col := s.mdb.Collection(colDLQ)
	count, err := col.CountDocuments(ctx, bson.M{})
	if err != nil {
		return 0, fmt.Errorf("dispatch/mongo: count dlq: %w", err)
	}
	return count, nil
}
