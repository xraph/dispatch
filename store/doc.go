// Package store defines the aggregate persistence interface. Each subsystem
// (job, workflow, cron, dlq, event, cluster) defines its own store interface.
// The composite Store composes them all. Backends: Postgres, Bun, SQLite,
// Redis, and Memory.
package store
