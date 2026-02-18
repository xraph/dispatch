package cron

import (
	"context"
	"time"

	"github.com/xraph/dispatch/id"
)

// Store defines the persistence contract for cron entries.
type Store interface {
	// RegisterCron persists a new cron entry. Returns an error if the name
	// already exists.
	RegisterCron(ctx context.Context, entry *Entry) error

	// GetCron retrieves a cron entry by ID.
	GetCron(ctx context.Context, entryID id.CronID) (*Entry, error)

	// ListCrons returns all cron entries.
	ListCrons(ctx context.Context) ([]*Entry, error)

	// AcquireCronLock attempts to acquire a distributed lock for a cron entry.
	// Returns true if the lock was acquired. The lock expires after ttl.
	AcquireCronLock(ctx context.Context, entryID id.CronID, workerID id.WorkerID, ttl time.Duration) (bool, error)

	// ReleaseCronLock releases the distributed lock for a cron entry.
	ReleaseCronLock(ctx context.Context, entryID id.CronID, workerID id.WorkerID) error

	// UpdateCronLastRun records when a cron entry last fired.
	UpdateCronLastRun(ctx context.Context, entryID id.CronID, at time.Time) error

	// UpdateCronEntry updates a cron entry (Enabled, NextRunAt, etc.).
	UpdateCronEntry(ctx context.Context, entry *Entry) error

	// DeleteCron removes a cron entry by ID.
	DeleteCron(ctx context.Context, entryID id.CronID) error
}
