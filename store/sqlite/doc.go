// Package sqlite implements store.Store using the grove ORM with SQLite
// dialect. Suitable for embedded/edge deployments, CLI tools, and standalone
// applications.
//
// The caller owns the *grove.DB lifecycle -- sqlite never closes it. Pass the
// db handle through the constructor:
//
//	import (
//	    "github.com/xraph/grove"
//	    "github.com/xraph/dispatch/store/sqlite"
//	)
//
//	db, _ := grove.Open(ctx, "sqlite", dsn)
//	store := sqlite.New(db)
//	store.Migrate(ctx)
//
// # Write concurrency
//
// SQLite allows one writer at a time for the whole database, and this
// store does more of its work through writes than a reader would expect:
// claiming a job, renewing a lease and reclaiming an expired one are all
// writes, and a busy pool performs them continuously.
//
// Two settings normally smooth that over, and neither is reachable from
// here. Grove's sqlitedriver enables WAL but sets no busy_timeout, so a
// writer that loses the race fails immediately with SQLITE_BUSY rather
// than waiting for the lock, and the driver does not expose the underlying
// *sql.DB, so this package cannot call SetMaxOpenConns to keep more than
// one connection from trying at once. The store compensates in Go by
// retrying SQLITE_BUSY with a jittered backoff (see withBusyRetry), which
// is enough for ordinary contention.
//
// It is a mitigation, not a substitute. If a deployment is write-heavy
// enough to see SQLITE_BUSY surface as an error after the retries are
// exhausted, the fixes are, in order of preference:
//
//   - Open the database with busy_timeout set in the DSN, for example
//     "file:dispatch.db?_pragma=busy_timeout(5000)", so SQLite itself
//     blocks on the lock instead of failing fast. The exact parameter
//     name depends on the driver build.
//   - Constrain the pool to a single connection if the driver in use
//     allows configuring it, which serialises writers before they reach
//     SQLite rather than after.
//   - Move to postgres. SQLite's single-writer model is a property of the
//     engine, and a queue with several busy pools is the workload it
//     suits least.
//
// A single process with one worker pool, which is what embedded and CLI
// deployments usually are, will not meaningfully encounter this.
package sqlite
