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
	}
}
