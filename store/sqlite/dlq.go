package sqlite

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
	_, err := s.sdb.NewInsert(m).Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: push dlq: %w", err)
	}
	return nil
}

// ListDLQ returns DLQ entries matching the given options.
func (s *Store) ListDLQ(ctx context.Context, opts dlq.ListOpts) ([]*dlq.Entry, error) {
	var models []dlqEntryModel
	q := s.sdb.NewSelect(&models)

	if opts.Queue != "" {
		q = q.Where("queue = ?", opts.Queue)
	}

	q = q.OrderExpr("failed_at ASC")

	if opts.Limit > 0 {
		q = q.Limit(opts.Limit)
	}
	if opts.Offset > 0 {
		q = q.Offset(opts.Offset)
	}

	err := q.Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: list dlq: %w", err)
	}

	entries := make([]*dlq.Entry, 0, len(models))
	for i := range models {
		e, convErr := fromDLQModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/sqlite: list dlq convert: %w", convErr)
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// GetDLQ retrieves a DLQ entry by ID.
func (s *Store) GetDLQ(ctx context.Context, entryID id.DLQID) (*dlq.Entry, error) {
	m := new(dlqEntryModel)
	err := s.sdb.NewSelect(m).
		Where("id = ?", entryID.String()).
		Limit(1).
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, dispatch.ErrDLQNotFound
		}
		return nil, fmt.Errorf("dispatch/sqlite: get dlq: %w", err)
	}
	return fromDLQModel(m)
}

// ReplayDLQ marks a DLQ entry as replayed.
func (s *Store) ReplayDLQ(ctx context.Context, entryID id.DLQID) error {
	now := time.Now().UTC()
	res, err := s.sdb.NewUpdate((*dlqEntryModel)(nil)).
		Set("replayed_at = ?", now).
		Where("id = ?", entryID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: replay dlq: %w", err)
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
	res, err := s.sdb.NewDelete((*dlqEntryModel)(nil)).
		Where("failed_at < ?", before).
		Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("dispatch/sqlite: purge dlq: %w", err)
	}
	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	return rows, nil
}

// CountDLQ returns the total number of entries in the dead letter queue.
func (s *Store) CountDLQ(ctx context.Context) (int64, error) {
	count, err := s.sdb.NewSelect((*dlqEntryModel)(nil)).
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("dispatch/sqlite: count dlq: %w", err)
	}
	return count, nil
}
