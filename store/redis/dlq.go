package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
)

// PushDLQ adds a failed job entry to the dead letter queue.
func (s *Store) PushDLQ(ctx context.Context, entry *dlq.Entry) error {
	eID := entry.ID.String()
	key := dlqKey(eID)

	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, key, dlqToMap(entry))
	pipe.SAdd(ctx, dlqIDsKey, eID)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/redis: push dlq: %w", err)
	}
	return nil
}

// ListDLQ returns DLQ entries matching the given options.
func (s *Store) ListDLQ(ctx context.Context, opts dlq.ListOpts) ([]*dlq.Entry, error) {
	ids, err := s.client.SMembers(ctx, dlqIDsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: list dlq: %w", err)
	}

	entries := make([]*dlq.Entry, 0, len(ids))
	for _, eID := range ids {
		vals, getErr := s.client.HGetAll(ctx, dlqKey(eID)).Result()
		if getErr != nil || len(vals) == 0 {
			continue
		}
		e, convErr := mapToDLQ(vals)
		if convErr != nil {
			continue
		}
		if opts.Queue != "" && e.Queue != opts.Queue {
			continue
		}
		entries = append(entries, e)
	}

	if opts.Offset > 0 && opts.Offset < len(entries) {
		entries = entries[opts.Offset:]
	} else if opts.Offset >= len(entries) {
		return nil, nil
	}
	if opts.Limit > 0 && opts.Limit < len(entries) {
		entries = entries[:opts.Limit]
	}
	return entries, nil
}

// GetDLQ retrieves a DLQ entry by ID.
func (s *Store) GetDLQ(ctx context.Context, entryID id.DLQID) (*dlq.Entry, error) {
	key := dlqKey(entryID.String())
	vals, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: get dlq: %w", err)
	}
	if len(vals) == 0 {
		return nil, dispatch.ErrDLQNotFound
	}
	return mapToDLQ(vals)
}

// ReplayDLQ marks a DLQ entry as replayed.
func (s *Store) ReplayDLQ(ctx context.Context, entryID id.DLQID) error {
	key := dlqKey(entryID.String())
	exists, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: replay dlq exists: %w", err)
	}
	if exists == 0 {
		return dispatch.ErrDLQNotFound
	}

	_, err = s.client.HSet(ctx, key,
		"replayed_at", time.Now().UTC().Format(time.RFC3339Nano),
	).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: replay dlq: %w", err)
	}
	return nil
}

// PurgeDLQ removes DLQ entries with FailedAt before the given time.
func (s *Store) PurgeDLQ(ctx context.Context, before time.Time) (int64, error) {
	ids, err := s.client.SMembers(ctx, dlqIDsKey).Result()
	if err != nil {
		return 0, fmt.Errorf("dispatch/redis: purge dlq smembers: %w", err)
	}

	var purged int64
	for _, eID := range ids {
		key := dlqKey(eID)
		failedAtStr, getErr := s.client.HGet(ctx, key, "failed_at").Result()
		if getErr != nil {
			if errors.Is(getErr, goredis.Nil) {
				continue
			}
			return purged, fmt.Errorf("dispatch/redis: purge dlq get: %w", getErr)
		}

		failedAt, _ := time.Parse(time.RFC3339Nano, failedAtStr) //nolint:errcheck // best-effort parse from trusted Redis data
		if failedAt.Before(before) {
			pipe := s.client.TxPipeline()
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
	count, err := s.client.SCard(ctx, dlqIDsKey).Result()
	if err != nil {
		return 0, fmt.Errorf("dispatch/redis: count dlq: %w", err)
	}
	return count, nil
}

// ── helpers ──

func dlqToMap(e *dlq.Entry) map[string]interface{} {
	m := map[string]interface{}{
		"id":          e.ID.String(),
		"job_id":      e.JobID.String(),
		"job_name":    e.JobName,
		"queue":       e.Queue,
		"payload":     string(e.Payload),
		"error":       e.Error,
		"retry_count": strconv.Itoa(e.RetryCount),
		"max_retries": strconv.Itoa(e.MaxRetries),
		"scope_app":   e.ScopeAppID,
		"scope_org":   e.ScopeOrgID,
		"failed_at":   e.FailedAt.Format(time.RFC3339Nano),
		"created_at":  e.CreatedAt.Format(time.RFC3339Nano),
	}
	if e.ReplayedAt != nil {
		m["replayed_at"] = e.ReplayedAt.Format(time.RFC3339Nano)
	}
	return m
}

func mapToDLQ(m map[string]string) (*dlq.Entry, error) {
	eID, err := id.ParseDLQID(m["id"])
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: parse dlq id: %w", err)
	}
	jobID, _ := id.ParseJobID(m["job_id"])                        //nolint:errcheck // best-effort parse from trusted Redis data
	retryCount, _ := strconv.Atoi(m["retry_count"])               //nolint:errcheck // best-effort parse from trusted Redis data
	maxRetries, _ := strconv.Atoi(m["max_retries"])               //nolint:errcheck // best-effort parse from trusted Redis data
	failedAt, _ := time.Parse(time.RFC3339Nano, m["failed_at"])   //nolint:errcheck // best-effort parse from trusted Redis data
	createdAt, _ := time.Parse(time.RFC3339Nano, m["created_at"]) //nolint:errcheck // best-effort parse from trusted Redis data

	e := &dlq.Entry{
		ID:         eID,
		JobID:      jobID,
		JobName:    m["job_name"],
		Queue:      m["queue"],
		Payload:    []byte(m["payload"]),
		Error:      m["error"],
		RetryCount: retryCount,
		MaxRetries: maxRetries,
		ScopeAppID: m["scope_app"],
		ScopeOrgID: m["scope_org"],
		FailedAt:   failedAt,
		CreatedAt:  createdAt,
	}

	if v := m["replayed_at"]; v != "" {
		t, _ := time.Parse(time.RFC3339Nano, v) //nolint:errcheck // best-effort parse from trusted Redis data
		e.ReplayedAt = &t
	}
	return e, nil
}
