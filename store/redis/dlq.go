package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
)

// ── JSON model for KV storage ──

type dlqEntity struct {
	ID         string     `json:"id"`
	JobID      string     `json:"job_id"`
	JobName    string     `json:"job_name"`
	Queue      string     `json:"queue"`
	Payload    []byte     `json:"payload"`
	Error      string     `json:"error"`
	RetryCount int        `json:"retry_count"`
	MaxRetries int        `json:"max_retries"`
	ScopeAppID string     `json:"scope_app_id"`
	ScopeOrgID string     `json:"scope_org_id"`
	FailedAt   time.Time  `json:"failed_at"`
	ReplayedAt *time.Time `json:"replayed_at,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
}

func toDLQEntity(e *dlq.Entry) *dlqEntity {
	return &dlqEntity{
		ID:         e.ID.String(),
		JobID:      e.JobID.String(),
		JobName:    e.JobName,
		Queue:      e.Queue,
		Payload:    e.Payload,
		Error:      e.Error,
		RetryCount: e.RetryCount,
		MaxRetries: e.MaxRetries,
		ScopeAppID: e.ScopeAppID,
		ScopeOrgID: e.ScopeOrgID,
		FailedAt:   e.FailedAt,
		ReplayedAt: e.ReplayedAt,
		CreatedAt:  e.CreatedAt,
	}
}

func fromDLQEntity(e *dlqEntity) (*dlq.Entry, error) {
	parsedID, err := id.ParseDLQID(e.ID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: parse dlq id: %w", err)
	}

	parsedJobID, _ := id.ParseJobID(e.JobID) //nolint:errcheck // best-effort

	return &dlq.Entry{
		ID:         parsedID,
		JobID:      parsedJobID,
		JobName:    e.JobName,
		Queue:      e.Queue,
		Payload:    e.Payload,
		Error:      e.Error,
		RetryCount: e.RetryCount,
		MaxRetries: e.MaxRetries,
		ScopeAppID: e.ScopeAppID,
		ScopeOrgID: e.ScopeOrgID,
		FailedAt:   e.FailedAt,
		ReplayedAt: e.ReplayedAt,
		CreatedAt:  e.CreatedAt,
	}, nil
}

// PushDLQ adds a failed job entry to the dead letter queue.
func (s *Store) PushDLQ(ctx context.Context, entry *dlq.Entry) error {
	eID := entry.ID.String()
	key := dlqKey(eID)

	e := toDLQEntity(entry)
	if err := s.setEntity(ctx, key, e); err != nil {
		return fmt.Errorf("dispatch/redis: push dlq set: %w", err)
	}

	if err := s.rdb.SAdd(ctx, dlqIDsKey, eID).Err(); err != nil {
		return fmt.Errorf("dispatch/redis: push dlq index: %w", err)
	}
	return nil
}

// ListDLQ returns DLQ entries matching the given options.
func (s *Store) ListDLQ(ctx context.Context, opts dlq.ListOpts) ([]*dlq.Entry, error) {
	ids, err := s.rdb.SMembers(ctx, dlqIDsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: list dlq: %w", err)
	}

	entries := make([]*dlq.Entry, 0, len(ids))
	for _, eID := range ids {
		var e dlqEntity
		if getErr := s.getEntity(ctx, dlqKey(eID), &e); getErr != nil {
			continue
		}
		if opts.Queue != "" && e.Queue != opts.Queue {
			continue
		}
		entry, convErr := fromDLQEntity(&e)
		if convErr != nil {
			continue
		}
		entries = append(entries, entry)
	}

	return applyPagination(entries, opts.Offset, opts.Limit), nil
}

// GetDLQ retrieves a DLQ entry by ID.
func (s *Store) GetDLQ(ctx context.Context, entryID id.DLQID) (*dlq.Entry, error) {
	var e dlqEntity
	if err := s.getEntity(ctx, dlqKey(entryID.String()), &e); err != nil {
		if isNotFound(err) {
			return nil, dispatch.ErrDLQNotFound
		}
		return nil, fmt.Errorf("dispatch/redis: get dlq: %w", err)
	}
	return fromDLQEntity(&e)
}

// ReplayDLQ marks a DLQ entry as replayed.
func (s *Store) ReplayDLQ(ctx context.Context, entryID id.DLQID) error {
	key := dlqKey(entryID.String())
	var e dlqEntity
	if err := s.getEntity(ctx, key, &e); err != nil {
		if isNotFound(err) {
			return dispatch.ErrDLQNotFound
		}
		return fmt.Errorf("dispatch/redis: replay dlq get: %w", err)
	}

	t := now()
	e.ReplayedAt = &t
	return s.setEntity(ctx, key, &e)
}

// PurgeDLQ removes DLQ entries with FailedAt before the given time.
func (s *Store) PurgeDLQ(ctx context.Context, before time.Time) (int64, error) {
	ids, err := s.rdb.SMembers(ctx, dlqIDsKey).Result()
	if err != nil {
		return 0, fmt.Errorf("dispatch/redis: purge dlq smembers: %w", err)
	}

	var purged int64
	for _, eID := range ids {
		key := dlqKey(eID)
		var e dlqEntity
		if getErr := s.getEntity(ctx, key, &e); getErr != nil {
			continue
		}

		if e.FailedAt.Before(before) {
			pipe := s.rdb.TxPipeline()
			pipe.Del(ctx, key)
			pipe.SRem(ctx, dlqIDsKey, eID)
			if _, pErr := pipe.Exec(ctx); pErr != nil {
				return purged, fmt.Errorf("dispatch/redis: purge dlq del: %w", pErr)
			}
			purged++
		}
	}
	return purged, nil
}

// CountDLQ returns the total number of entries in the dead letter queue.
func (s *Store) CountDLQ(ctx context.Context) (int64, error) {
	count, err := s.rdb.SCard(ctx, dlqIDsKey).Result()
	if err != nil {
		return 0, fmt.Errorf("dispatch/redis: count dlq: %w", err)
	}
	return count, nil
}
