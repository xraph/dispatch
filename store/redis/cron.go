package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/id"
)

// ── JSON model for KV storage ──

type cronEntity struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Schedule    string     `json:"schedule"`
	JobName     string     `json:"job_name"`
	Queue       string     `json:"queue"`
	Payload     []byte     `json:"payload,omitempty"`
	ScopeAppID  string     `json:"scope_app_id"`
	ScopeOrgID  string     `json:"scope_org_id"`
	LastRunAt   *time.Time `json:"last_run_at,omitempty"`
	NextRunAt   *time.Time `json:"next_run_at,omitempty"`
	LockedBy    string     `json:"locked_by"`
	LockedUntil *time.Time `json:"locked_until,omitempty"`
	Enabled     bool       `json:"enabled"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

func toCronEntity(e *cron.Entry) *cronEntity {
	return &cronEntity{
		ID:          e.ID.String(),
		Name:        e.Name,
		Schedule:    e.Schedule,
		JobName:     e.JobName,
		Queue:       e.Queue,
		Payload:     e.Payload,
		ScopeAppID:  e.ScopeAppID,
		ScopeOrgID:  e.ScopeOrgID,
		LastRunAt:   e.LastRunAt,
		NextRunAt:   e.NextRunAt,
		LockedBy:    e.LockedBy,
		LockedUntil: e.LockedUntil,
		Enabled:     e.Enabled,
		CreatedAt:   e.CreatedAt,
		UpdatedAt:   e.UpdatedAt,
	}
}

func fromCronEntity(e *cronEntity) (*cron.Entry, error) {
	eID, err := id.ParseCronID(e.ID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: parse cron id: %w", err)
	}

	return &cron.Entry{
		Entity: dispatch.Entity{
			CreatedAt: e.CreatedAt,
			UpdatedAt: e.UpdatedAt,
		},
		ID:          eID,
		Name:        e.Name,
		Schedule:    e.Schedule,
		JobName:     e.JobName,
		Queue:       e.Queue,
		Payload:     e.Payload,
		ScopeAppID:  e.ScopeAppID,
		ScopeOrgID:  e.ScopeOrgID,
		LastRunAt:   e.LastRunAt,
		NextRunAt:   e.NextRunAt,
		LockedBy:    e.LockedBy,
		LockedUntil: e.LockedUntil,
		Enabled:     e.Enabled,
	}, nil
}

// RegisterCron persists a new cron entry.
func (s *Store) RegisterCron(ctx context.Context, entry *cron.Entry) error {
	eID := entry.ID.String()
	key := cronKey(eID)

	// Check for duplicate name.
	existing, err := s.rdb.HGet(ctx, cronNamesKey, entry.Name).Result()
	if err != nil && !isRedisNil(err) {
		return fmt.Errorf("dispatch/redis: register cron check name: %w", err)
	}
	if existing != "" {
		return dispatch.ErrDuplicateCron
	}

	e := toCronEntity(entry)
	if setErr := s.setEntity(ctx, key, e); setErr != nil {
		return fmt.Errorf("dispatch/redis: register cron set: %w", setErr)
	}

	pipe := s.rdb.TxPipeline()
	pipe.SAdd(ctx, cronIDsKey, eID)
	pipe.HSet(ctx, cronNamesKey, entry.Name, eID)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/redis: register cron indexes: %w", err)
	}
	return nil
}

// GetCron retrieves a cron entry by ID.
func (s *Store) GetCron(ctx context.Context, entryID id.CronID) (*cron.Entry, error) {
	var e cronEntity
	if err := s.getEntity(ctx, cronKey(entryID.String()), &e); err != nil {
		if isNotFound(err) {
			return nil, dispatch.ErrCronNotFound
		}
		return nil, fmt.Errorf("dispatch/redis: get cron: %w", err)
	}
	return fromCronEntity(&e)
}

// ListCrons returns all cron entries.
func (s *Store) ListCrons(ctx context.Context) ([]*cron.Entry, error) {
	ids, err := s.rdb.SMembers(ctx, cronIDsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: list crons: %w", err)
	}

	entries := make([]*cron.Entry, 0, len(ids))
	for _, eID := range ids {
		var e cronEntity
		if getErr := s.getEntity(ctx, cronKey(eID), &e); getErr != nil {
			continue
		}
		entry, convErr := fromCronEntity(&e)
		if convErr != nil {
			continue
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// AcquireCronLock attempts to acquire a distributed lock for a cron entry.
func (s *Store) AcquireCronLock(ctx context.Context, entryID id.CronID, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	eID := entryID.String()
	key := cronKey(eID)
	wID := workerID.String()
	t := now()
	until := t.Add(ttl)

	// Read current entity.
	var e cronEntity
	if err := s.getEntity(ctx, key, &e); err != nil {
		if isNotFound(err) {
			return false, dispatch.ErrCronNotFound
		}
		return false, fmt.Errorf("dispatch/redis: acquire cron lock get: %w", err)
	}

	// Check current lock state.
	if e.LockedBy != "" && e.LockedBy != wID {
		// Someone else holds the lock -- check if expired.
		if e.LockedUntil != nil && e.LockedUntil.After(t) {
			return false, nil // lock still valid
		}
	}

	// Acquire or re-acquire.
	e.LockedBy = wID
	e.LockedUntil = &until
	e.UpdatedAt = t
	if err := s.setEntity(ctx, key, &e); err != nil {
		return false, fmt.Errorf("dispatch/redis: acquire cron lock set: %w", err)
	}
	return true, nil
}

// ReleaseCronLock releases the distributed lock for a cron entry.
func (s *Store) ReleaseCronLock(ctx context.Context, entryID id.CronID, workerID id.WorkerID) error {
	key := cronKey(entryID.String())
	wID := workerID.String()

	var e cronEntity
	if err := s.getEntity(ctx, key, &e); err != nil {
		if isNotFound(err) {
			return nil // entry gone, no-op
		}
		return fmt.Errorf("dispatch/redis: release cron lock get: %w", err)
	}

	if e.LockedBy != wID {
		return nil // not our lock, no-op
	}

	e.LockedBy = ""
	e.LockedUntil = nil
	e.UpdatedAt = now()
	return s.setEntity(ctx, key, &e)
}

// UpdateCronLastRun records when a cron entry last fired.
func (s *Store) UpdateCronLastRun(ctx context.Context, entryID id.CronID, at time.Time) error {
	key := cronKey(entryID.String())
	var e cronEntity
	if err := s.getEntity(ctx, key, &e); err != nil {
		if isNotFound(err) {
			return dispatch.ErrCronNotFound
		}
		return fmt.Errorf("dispatch/redis: update last run get: %w", err)
	}

	e.LastRunAt = &at
	e.UpdatedAt = now()
	return s.setEntity(ctx, key, &e)
}

// UpdateCronEntry updates a cron entry.
func (s *Store) UpdateCronEntry(ctx context.Context, entry *cron.Entry) error {
	key := cronKey(entry.ID.String())
	exists, err := s.entityExists(ctx, key)
	if err != nil {
		return fmt.Errorf("dispatch/redis: update cron exists: %w", err)
	}
	if !exists {
		return dispatch.ErrCronNotFound
	}

	e := toCronEntity(entry)
	e.UpdatedAt = now()
	return s.setEntity(ctx, key, e)
}

// DeleteCron removes a cron entry by ID.
func (s *Store) DeleteCron(ctx context.Context, entryID id.CronID) error {
	eID := entryID.String()
	key := cronKey(eID)

	// Get name for name index cleanup.
	var e cronEntity
	if err := s.getEntity(ctx, key, &e); err != nil {
		if isNotFound(err) {
			return dispatch.ErrCronNotFound
		}
		return fmt.Errorf("dispatch/redis: delete cron get: %w", err)
	}

	pipe := s.rdb.TxPipeline()
	pipe.Del(ctx, key)
	pipe.SRem(ctx, cronIDsKey, eID)
	if e.Name != "" {
		pipe.HDel(ctx, cronNamesKey, e.Name)
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/redis: delete cron: %w", err)
	}
	return nil
}
