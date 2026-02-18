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
