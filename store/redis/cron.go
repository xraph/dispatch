package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/id"
)

// RegisterCron persists a new cron entry.
func (s *Store) RegisterCron(ctx context.Context, entry *cron.Entry) error {
	eID := entry.ID.String()
	key := cronKey(eID)

	// Check for duplicate name.
	existing, err := s.client.HGet(ctx, cronNamesKey, entry.Name).Result()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return fmt.Errorf("dispatch/redis: register cron check name: %w", err)
	}
	if existing != "" {
		return dispatch.ErrDuplicateCron
	}

	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, key, cronToMap(entry))
	pipe.SAdd(ctx, cronIDsKey, eID)
	pipe.HSet(ctx, cronNamesKey, entry.Name, eID)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/redis: register cron: %w", err)
	}
	return nil
}

// GetCron retrieves a cron entry by ID.
func (s *Store) GetCron(ctx context.Context, entryID id.CronID) (*cron.Entry, error) {
	key := cronKey(entryID.String())
	vals, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: get cron: %w", err)
	}
	if len(vals) == 0 {
		return nil, dispatch.ErrCronNotFound
	}
	return mapToCron(vals)
}

// ListCrons returns all cron entries.
func (s *Store) ListCrons(ctx context.Context) ([]*cron.Entry, error) {
	ids, err := s.client.SMembers(ctx, cronIDsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: list crons: %w", err)
	}

	entries := make([]*cron.Entry, 0, len(ids))
	for _, eID := range ids {
		vals, getErr := s.client.HGetAll(ctx, cronKey(eID)).Result()
		if getErr != nil || len(vals) == 0 {
			continue
		}
		e, convErr := mapToCron(vals)
		if convErr != nil {
			continue
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// AcquireCronLock attempts to acquire a distributed lock for a cron entry.
func (s *Store) AcquireCronLock(ctx context.Context, entryID id.CronID, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	eID := entryID.String()
	key := cronKey(eID)
	wID := workerID.String()
	now := time.Now().UTC()
	until := now.Add(ttl)

	// Check entry exists.
	exists, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("dispatch/redis: acquire cron lock exists: %w", err)
	}
	if exists == 0 {
		return false, dispatch.ErrCronNotFound
	}

	// Check current lock state.
	lockedBy, _ := s.client.HGet(ctx, key, "locked_by").Result()          //nolint:errcheck // best-effort parse from trusted Redis data
	lockedUntilStr, _ := s.client.HGet(ctx, key, "locked_until").Result() //nolint:errcheck // best-effort parse from trusted Redis data

	if lockedBy != "" && lockedBy != wID {
		// Someone else holds the lock — check if expired.
		lockedUntil, _ := time.Parse(time.RFC3339Nano, lockedUntilStr) //nolint:errcheck // best-effort parse from trusted Redis data
		if lockedUntil.After(now) {
			return false, nil // lock still valid
		}
	}

	// Acquire or re-acquire.
	_, err = s.client.HSet(ctx, key,
		"locked_by", wID,
		"locked_until", until.Format(time.RFC3339Nano),
		"updated_at", now.Format(time.RFC3339Nano),
	).Result()
	if err != nil {
		return false, fmt.Errorf("dispatch/redis: acquire cron lock: %w", err)
	}
	return true, nil
}

// ReleaseCronLock releases the distributed lock for a cron entry.
func (s *Store) ReleaseCronLock(ctx context.Context, entryID id.CronID, workerID id.WorkerID) error {
	key := cronKey(entryID.String())
	wID := workerID.String()

	lockedBy, _ := s.client.HGet(ctx, key, "locked_by").Result() //nolint:errcheck // best-effort parse from trusted Redis data
	if lockedBy != wID {
		return nil // not our lock, no-op
	}

	_, err := s.client.HSet(ctx, key,
		"locked_by", "",
		"locked_until", "",
		"updated_at", time.Now().UTC().Format(time.RFC3339Nano),
	).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: release cron lock: %w", err)
	}
	return nil
}

// UpdateCronLastRun records when a cron entry last fired.
func (s *Store) UpdateCronLastRun(ctx context.Context, entryID id.CronID, at time.Time) error {
	key := cronKey(entryID.String())
	exists, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: update last run exists: %w", err)
	}
	if exists == 0 {
		return dispatch.ErrCronNotFound
	}

	_, err = s.client.HSet(ctx, key,
		"last_run_at", at.Format(time.RFC3339Nano),
		"updated_at", time.Now().UTC().Format(time.RFC3339Nano),
	).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: update last run: %w", err)
	}
	return nil
}

// UpdateCronEntry updates a cron entry.
func (s *Store) UpdateCronEntry(ctx context.Context, entry *cron.Entry) error {
	key := cronKey(entry.ID.String())
	exists, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: update cron exists: %w", err)
	}
	if exists == 0 {
		return dispatch.ErrCronNotFound
	}

	m := cronToMap(entry)
	m["updated_at"] = time.Now().UTC().Format(time.RFC3339Nano)
	_, err = s.client.HSet(ctx, key, m).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: update cron: %w", err)
	}
	return nil
}

// DeleteCron removes a cron entry by ID.
func (s *Store) DeleteCron(ctx context.Context, entryID id.CronID) error {
	eID := entryID.String()
	key := cronKey(eID)

	// Get name for name index cleanup.
	name, _ := s.client.HGet(ctx, key, "name").Result() //nolint:errcheck // best-effort parse from trusted Redis data

	exists, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: delete cron exists: %w", err)
	}
	if exists == 0 {
		return dispatch.ErrCronNotFound
	}

	pipe := s.client.TxPipeline()
	pipe.Del(ctx, key)
	pipe.SRem(ctx, cronIDsKey, eID)
	if name != "" {
		pipe.HDel(ctx, cronNamesKey, name)
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/redis: delete cron: %w", err)
	}
	return nil
}

// ── helpers ──

func cronToMap(e *cron.Entry) map[string]interface{} {
	m := map[string]interface{}{
		"id":         e.ID.String(),
		"name":       e.Name,
		"schedule":   e.Schedule,
		"job_name":   e.JobName,
		"queue":      e.Queue,
		"payload":    string(e.Payload),
		"scope_app":  e.ScopeAppID,
		"scope_org":  e.ScopeOrgID,
		"locked_by":  e.LockedBy,
		"enabled":    strconv.FormatBool(e.Enabled),
		"created_at": e.CreatedAt.Format(time.RFC3339Nano),
		"updated_at": e.UpdatedAt.Format(time.RFC3339Nano),
	}
	if e.LastRunAt != nil {
		m["last_run_at"] = e.LastRunAt.Format(time.RFC3339Nano)
	}
	if e.NextRunAt != nil {
		m["next_run_at"] = e.NextRunAt.Format(time.RFC3339Nano)
	}
	if e.LockedUntil != nil {
		m["locked_until"] = e.LockedUntil.Format(time.RFC3339Nano)
	}
	return m
}

func mapToCron(m map[string]string) (*cron.Entry, error) {
	eID, err := id.ParseCronID(m["id"])
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: parse cron id: %w", err)
	}

	createdAt, _ := time.Parse(time.RFC3339Nano, m["created_at"]) //nolint:errcheck // best-effort parse from trusted Redis data
	updatedAt, _ := time.Parse(time.RFC3339Nano, m["updated_at"]) //nolint:errcheck // best-effort parse from trusted Redis data
	enabled, _ := strconv.ParseBool(m["enabled"])                 //nolint:errcheck // best-effort parse from trusted Redis data

	e := &cron.Entry{
		Entity: dispatch.Entity{
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
		},
		ID:         eID,
		Name:       m["name"],
		Schedule:   m["schedule"],
		JobName:    m["job_name"],
		Queue:      m["queue"],
		Payload:    []byte(m["payload"]),
		ScopeAppID: m["scope_app"],
		ScopeOrgID: m["scope_org"],
		LockedBy:   m["locked_by"],
		Enabled:    enabled,
	}

	if v := m["last_run_at"]; v != "" {
		t, _ := time.Parse(time.RFC3339Nano, v) //nolint:errcheck // best-effort parse from trusted Redis data
		e.LastRunAt = &t
	}
	if v := m["next_run_at"]; v != "" {
		t, _ := time.Parse(time.RFC3339Nano, v) //nolint:errcheck // best-effort parse from trusted Redis data
		e.NextRunAt = &t
	}
	if v := m["locked_until"]; v != "" {
		t, _ := time.Parse(time.RFC3339Nano, v) //nolint:errcheck // best-effort parse from trusted Redis data
		e.LockedUntil = &t
	}
	return e, nil
}
