package bunstore

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
)

// PushDLQ adds a failed job entry to the dead letter queue.
func (s *Store) PushDLQ(ctx context.Context, entry *dlq.Entry) error {
	m := toDLQModel(entry)
	_, err := s.db.NewInsert().Model(m).Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/bun: push dlq: %w", err)
	}
	return nil
}

// ListDLQ returns DLQ entries matching the given options.
func (s *Store) ListDLQ(ctx context.Context, opts dlq.ListOpts) ([]*dlq.Entry, error) {
	var models []dlqEntryModel
	q := s.db.NewSelect().Model(&models)

	if opts.Queue != "" {
		q = q.Where("queue = ?", opts.Queue)
	}

	q = q.Order("failed_at ASC")

	if opts.Limit > 0 {
		q = q.Limit(opts.Limit)
	}
	if opts.Offset > 0 {
		q = q.Offset(opts.Offset)
	}

	err := q.Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("dispatch/bun: list dlq: %w", err)
	}

	entries := make([]*dlq.Entry, 0, len(models))
	for i := range models {
		e, convErr := fromDLQModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/bun: list dlq convert: %w", convErr)
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// GetDLQ retrieves a DLQ entry by ID.
func (s *Store) GetDLQ(ctx context.Context, entryID id.DLQID) (*dlq.Entry, error) {
	m := new(dlqEntryModel)
	err := s.db.NewSelect().Model(m).
		Where("id = ?", entryID.String()).
		Limit(1).
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, dispatch.ErrDLQNotFound
		}
		return nil, fmt.Errorf("dispatch/bun: get dlq: %w", err)
	}
	return fromDLQModel(m)
}

// ReplayDLQ marks a DLQ entry as replayed.
func (s *Store) ReplayDLQ(ctx context.Context, entryID id.DLQID) error {
	res, err := s.db.NewUpdate().
		TableExpr("dispatch_dlq").
		Set("replayed_at = NOW()").
		Where("id = ?", entryID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/bun: replay dlq: %w", err)
	}
	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		return dispatch.ErrDLQNotFound
	}
	return nil
}

// PurgeDLQ removes DLQ entries with FailedAt before the given time.
// Returns the number of entries removed.
func (s *Store) PurgeDLQ(ctx context.Context, before time.Time) (int64, error) {
	res, err := s.db.NewDelete().
		TableExpr("dispatch_dlq").
		Where("failed_at < ?", before).
		Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("dispatch/bun: purge dlq: %w", err)
	}
	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	return rows, nil
}

// CountDLQ returns the total number of entries in the dead letter queue.
func (s *Store) CountDLQ(ctx context.Context) (int64, error) {
	count, err := s.db.NewSelect().
		TableExpr("dispatch_dlq").
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("dispatch/bun: count dlq: %w", err)
	}
	return int64(count), nil
}
