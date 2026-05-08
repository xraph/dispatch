package dispatch

import "time"

// Config holds configuration for the Dispatcher.
type Config struct {
	// Concurrency is the maximum number of jobs processed concurrently.
	Concurrency int

	// Queues is the list of queues this dispatcher will poll.
	Queues []string

	// PollInterval is how often to poll for new jobs.
	PollInterval time.Duration

	// ShutdownTimeout is the maximum time to wait for graceful shutdown.
	ShutdownTimeout time.Duration

	// HeartbeatInterval is how often running jobs send heartbeats.
	HeartbeatInterval time.Duration

	// StaleJobThreshold is how long before a job without heartbeat is
	// considered stale.
	StaleJobThreshold time.Duration

	// WorkerStoreCallTimeout caps a single worker store roundtrip
	// (DequeueJobs, HeartbeatJob, ReapStaleJobs, UpdateJob). Bounds
	// how long a stalled driver session can hold a pool connection
	// before the worker abandons it. Zero leaves the worker default
	// (5s); negative disables (test-only).
	WorkerStoreCallTimeout time.Duration

	// CronTickInterval controls how often the cron scheduler checks
	// for due entries. Default 1s. Production deployments running
	// against a single mongo / postgres should bump this to 5–10s
	// — every tick costs at least a GetLeader call against the
	// shared driver pool.
	CronTickInterval time.Duration

	// CronLeaderTTL is the TTL for the cron leader election lock.
	// Default 15s. The leader loop renews at half this interval.
	CronLeaderTTL time.Duration

	// CronLockTTL is the per-entry distributed lock TTL fired around
	// each cron entry execution. Default 30s.
	CronLockTTL time.Duration

	// CronStoreCallTimeout mirrors WorkerStoreCallTimeout for the
	// cron scheduler's GetLeader / ListCrons / Acquire/Release lock
	// paths. Default 5s; negative disables (test-only).
	CronStoreCallTimeout time.Duration
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Concurrency:       10,
		Queues:            []string{"default"},
		PollInterval:      1 * time.Second,
		ShutdownTimeout:   30 * time.Second,
		HeartbeatInterval: 10 * time.Second,
		StaleJobThreshold: 30 * time.Second,
		// Tuning fields (zero leaves the subsystem default in place;
		// callers override via options).
		WorkerStoreCallTimeout: 0,
		CronTickInterval:       0,
		CronLeaderTTL:          0,
		CronLockTTL:            0,
		CronStoreCallTimeout:   0,
	}
}
