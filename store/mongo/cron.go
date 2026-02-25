package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/id"
)

// RegisterCron persists a new cron entry. Returns an error if the name
// already exists.
func (s *Store) RegisterCron(ctx context.Context, entry *cron.Entry) error {
	m := toCronModel(entry)
	_, err := s.mdb.NewInsert(m).Exec(ctx)
	if err != nil {
		if isDuplicateKey(err) {
			return dispatch.ErrDuplicateCron
		}
		return fmt.Errorf("dispatch/mongo: register cron: %w", err)
	}
	return nil
}

// GetCron retrieves a cron entry by ID.
func (s *Store) GetCron(ctx context.Context, entryID id.CronID) (*cron.Entry, error) {
	col := s.mdb.Collection(colCronEntries)
	var m cronEntryModel
	err := col.FindOne(ctx, bson.M{"_id": entryID.String()}).Decode(&m)
	if err != nil {
		if isNoDocuments(err) {
			return nil, dispatch.ErrCronNotFound
		}
		return nil, fmt.Errorf("dispatch/mongo: get cron: %w", err)
	}
	return fromCronModel(&m)
}

// ListCrons returns all cron entries.
func (s *Store) ListCrons(ctx context.Context) ([]*cron.Entry, error) {
	col := s.mdb.Collection(colCronEntries)

	findOpts := options.Find().SetSort(bson.D{{Key: "created_at", Value: 1}})
	cursor, err := col.Find(ctx, bson.M{}, findOpts)
	if err != nil {
		return nil, fmt.Errorf("dispatch/mongo: list crons: %w", err)
	}
	defer cursor.Close(ctx)

	var models []cronEntryModel
	if err := cursor.All(ctx, &models); err != nil {
		return nil, fmt.Errorf("dispatch/mongo: list crons decode: %w", err)
	}

	entries := make([]*cron.Entry, 0, len(models))
	for i := range models {
		e, convErr := fromCronModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/mongo: list crons convert: %w", convErr)
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// AcquireCronLock attempts to acquire a distributed lock for a cron entry.
// Uses FindOneAndUpdate for atomic lock acquisition.
func (s *Store) AcquireCronLock(ctx context.Context, entryID id.CronID, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	t := now()
	until := t.Add(ttl)
	wID := workerID.String()
	eID := entryID.String()
	col := s.mdb.Collection(colCronEntries)

	// Try to acquire: succeed if no lock, lock expired, or we already hold it.
	filter := bson.M{
		"_id": eID,
		"$or": []bson.M{
			{"locked_by": nil},
			{"locked_by": bson.M{"$exists": false}},
			{"locked_until": bson.M{"$lt": t}},
			{"locked_by": wID},
		},
	}

	update := bson.M{
		"$set": bson.M{
			"locked_by":    wID,
			"locked_until": until,
			"updated_at":   t,
		},
	}

	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	var m cronEntryModel
	err := col.FindOneAndUpdate(ctx, filter, update, opts).Decode(&m)
	if err != nil {
		if isNoDocuments(err) {
			// Check if the entry exists at all.
			count, existErr := col.CountDocuments(ctx, bson.M{"_id": eID})
			if existErr != nil {
				return false, fmt.Errorf("dispatch/mongo: check cron exists: %w", existErr)
			}
			if count == 0 {
				return false, dispatch.ErrCronNotFound
			}
			// Entry exists but lock is held by someone else.
			return false, nil
		}
		return false, fmt.Errorf("dispatch/mongo: acquire cron lock: %w", err)
	}

	return true, nil
}

// ReleaseCronLock releases the distributed lock for a cron entry.
func (s *Store) ReleaseCronLock(ctx context.Context, entryID id.CronID, workerID id.WorkerID) error {
	col := s.mdb.Collection(colCronEntries)
	t := now()

	_, err := col.UpdateOne(ctx,
		bson.M{
			"_id":       entryID.String(),
			"locked_by": workerID.String(),
		},
		bson.M{
			"$set": bson.M{
				"updated_at": t,
			},
			"$unset": bson.M{
				"locked_by":    "",
				"locked_until": "",
			},
		},
	)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: release cron lock: %w", err)
	}
	return nil
}

// UpdateCronLastRun records when a cron entry last fired.
func (s *Store) UpdateCronLastRun(ctx context.Context, entryID id.CronID, at time.Time) error {
	col := s.mdb.Collection(colCronEntries)
	t := now()

	res, err := col.UpdateOne(ctx,
		bson.M{"_id": entryID.String()},
		bson.M{"$set": bson.M{
			"last_run_at": at,
			"updated_at":  t,
		}},
	)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: update cron last run: %w", err)
	}
	if res.MatchedCount == 0 {
		return dispatch.ErrCronNotFound
	}
	return nil
}

// UpdateCronEntry updates a cron entry (Enabled, NextRunAt, etc.).
func (s *Store) UpdateCronEntry(ctx context.Context, entry *cron.Entry) error {
	m := toCronModel(entry)
	m.UpdatedAt = now()
	col := s.mdb.Collection(colCronEntries)
	res, err := col.ReplaceOne(ctx, bson.M{"_id": m.ID}, m)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: update cron entry: %w", err)
	}
	if res.MatchedCount == 0 {
		return dispatch.ErrCronNotFound
	}
	return nil
}

// DeleteCron removes a cron entry by ID.
func (s *Store) DeleteCron(ctx context.Context, entryID id.CronID) error {
	col := s.mdb.Collection(colCronEntries)
	res, err := col.DeleteOne(ctx, bson.M{"_id": entryID.String()})
	if err != nil {
		return fmt.Errorf("dispatch/mongo: delete cron: %w", err)
	}
	if res.DeletedCount == 0 {
		return dispatch.ErrCronNotFound
	}
	return nil
}
