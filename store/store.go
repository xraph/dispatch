// Package store defines the aggregate persistence interface. Each subsystem
// (job, workflow, cron, dlq, event, cluster) defines its own store interface.
// The composite Store composes them all. Backends: Postgres, Bun, SQLite,
// Redis, and Memory.
package store

import (
	"context"

	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// WakeNotifier is an optional store capability: backends that can push
// new-work signals (e.g. Postgres LISTEN/NOTIFY) implement it so worker
// pools on other instances wake immediately instead of waiting out their
// idle poll backoff. Polling remains the correctness mechanism — a missed
// wake only costs poll latency.
type WakeNotifier interface {
	// StartWakeListener subscribes to the backend's wake signal and calls
	// wake for every notification until stop is invoked. Implementations
	// must survive connection loss by re-subscribing internally.
	StartWakeListener(ctx context.Context, wake func()) (stop func(), err error)
}

// Store is the aggregate persistence interface.
// Each subsystem store is a composable interface — same pattern as ControlPlane.
// A single backend (postgres, bun, sqlite, etc.) implements all of them.
type Store interface {
	job.Store
	workflow.Store
	cron.Store
	dlq.Store
	event.Store
	cluster.Store

	// Migrate runs all schema migrations.
	Migrate(ctx context.Context) error

	// Ping checks database connectivity.
	Ping(ctx context.Context) error

	// Close closes the store connection.
	Close() error
}
