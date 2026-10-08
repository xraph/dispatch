package redis

import (
	"context"
	"errors"
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
	key := s.keys.cron(eID)

	// Check for duplicate name.
	existing, err := s.rdb.HGet(ctx, s.keys.cronNames(), entry.Name).Result()
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
	pipe.SAdd(ctx, s.keys.cronIDs(), eID)
	pipe.HSet(ctx, s.keys.cronNames(), entry.Name, eID)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/redis: register cron indexes: %w", err)
	}
	return nil
}

// GetCron retrieves a cron entry by ID.
func (s *Store) GetCron(ctx context.Context, entryID id.CronID) (*cron.Entry, error) {
	var e cronEntity
	if err := s.getEntity(ctx, s.keys.cron(entryID.String()), &e); err != nil {
		if isNotFound(err) {
			return nil, dispatch.ErrCronNotFound
		}
		return nil, fmt.Errorf("dispatch/redis: get cron: %w", err)
	}
	if e.ID != entryID.String() {
		return nil, fmt.Errorf("dispatch/redis: cron identity mismatch for key %s", entryID)
	}
	return fromCronEntity(&e)
}

// ListCrons returns all cron entries.
func (s *Store) ListCrons(ctx context.Context) ([]*cron.Entry, error) {
	ids, err := s.rdb.SMembers(ctx, s.keys.cronIDs()).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: list crons: %w", err)
	}

	entries := make([]*cron.Entry, 0, len(ids))
	for _, eID := range ids {
		entryID, parseErr := id.ParseCronID(eID)
		if parseErr != nil {
			return nil, fmt.Errorf("dispatch/redis: parse cron index ID: %w", parseErr)
		}
		entry, readErr := s.GetCron(ctx, entryID)
		if errors.Is(readErr, dispatch.ErrCronNotFound) {
			continue
		}
		if readErr != nil {
			return nil, readErr
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// The lock, the last and next run, and the enabled flag all live in one
// JSON blob per entry, so every write that changes one of them rewrites
// the whole blob. Each of them goes through updateEntity (cas.go), which
// only writes over the exact value it read. A scheduler write that read
// the entry before an operator disabled it therefore re-reads and keeps
// the disable, instead of putting enabled back; and two workers that both
// read the entry unlocked cannot both take the lock.

// errCronLockHeld is AcquireCronLock's refusal inside updateEntity: the
// lock belongs to another worker and has not expired.
var errCronLockHeld = errors.New("dispatch/redis: cron lock held by another worker")

// AcquireCronLock attempts to acquire a distributed lock for a cron entry.
// Of several workers racing for a free lock, exactly one gets true.
func (s *Store) AcquireCronLock(ctx context.Context, entryID id.CronID, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	wID := workerID.String()

	err := updateEntity(ctx, s, s.keys.cron(entryID.String()), dispatch.ErrCronNotFound,
		func(e *cronEntity) error {
			t := now()
			if e.LockedBy != "" && e.LockedBy != wID && e.LockedUntil != nil && e.LockedUntil.After(t) {
				return errCronLockHeld
			}

			until := t.Add(ttl)
			e.LockedBy = wID
			e.LockedUntil = &until
			e.UpdatedAt = t

			return nil
		})
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, errCronLockHeld):
		return false, nil
	case errors.Is(err, dispatch.ErrCronNotFound):
		return false, err
	default:
		return false, fmt.Errorf("dispatch/redis: acquire cron lock: %w", err)
	}
}

// ReleaseCronLock releases the distributed lock for a cron entry. It is a
// no-op when the entry is gone or the lock is not this worker's.
func (s *Store) ReleaseCronLock(ctx context.Context, entryID id.CronID, workerID id.WorkerID) error {
	wID := workerID.String()

	err := updateEntity(ctx, s, s.keys.cron(entryID.String()), errSkipWrite,
		func(e *cronEntity) error {
			if e.LockedBy != wID {
				return errSkipWrite
			}

			e.LockedBy = ""
			e.LockedUntil = nil
			e.UpdatedAt = now()

			return nil
		})
	if err != nil && !errors.Is(err, errSkipWrite) {
		return fmt.Errorf("dispatch/redis: release cron lock: %w", err)
	}

	return nil
}

// UpdateCronLastRun records when a cron entry last fired, and nothing else.
func (s *Store) UpdateCronLastRun(ctx context.Context, entryID id.CronID, at time.Time) error {
	return updateEntity(ctx, s, s.keys.cron(entryID.String()), dispatch.ErrCronNotFound,
		func(e *cronEntity) error {
			e.LastRunAt = &at
			e.UpdatedAt = now()

			return nil
		})
}

// SetCronEnabled sets enabled, sets next_run_at when nextRunAt is non-nil,
// and stamps updated_at. Every other field keeps whatever the entry holds
// at the moment of the write.
func (s *Store) SetCronEnabled(ctx context.Context, entryID id.CronID, enabled bool, nextRunAt *time.Time) error {
	return updateEntity(ctx, s, s.keys.cron(entryID.String()), dispatch.ErrCronNotFound,
		func(e *cronEntity) error {
			e.Enabled = enabled
			if nextRunAt != nil {
				next := *nextRunAt
				e.NextRunAt = &next
			}
			e.UpdatedAt = now()

			return nil
		})
}

// UpdateCronNextRun sets next_run_at and stamps updated_at, and never
// touches enabled.
func (s *Store) UpdateCronNextRun(ctx context.Context, entryID id.CronID, nextRunAt time.Time) error {
	return updateEntity(ctx, s, s.keys.cron(entryID.String()), dispatch.ErrCronNotFound,
		func(e *cronEntity) error {
			e.NextRunAt = &nextRunAt
			e.UpdatedAt = now()

			return nil
		})
}

// UpdateCronEntry updates a cron entry.
func (s *Store) UpdateCronEntry(ctx context.Context, entry *cron.Entry) error {
	key := s.keys.cron(entry.ID.String())
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
	key := s.keys.cron(eID)

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
	pipe.SRem(ctx, s.keys.cronIDs(), eID)
	if e.Name != "" {
		pipe.HDel(ctx, s.keys.cronNames(), e.Name)
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/redis: delete cron: %w", err)
	}
	return nil
}
