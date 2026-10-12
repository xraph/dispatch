// Package sqlite implements store.Store using the Grove ORM with SQLite.
// You can use it for embedded applications, CLI tools and standalone services.
// The caller owns the *grove.DB lifecycle; sqlite never closes it.
//
// Construct the driver before passing the database to the store. Check errors
// from Open and Migrate in your application:
//
//	import (
//	    "github.com/xraph/grove"
//	    "github.com/xraph/grove/driver"
//	    "github.com/xraph/grove/drivers/sqlitedriver"
//	    "github.com/xraph/dispatch/store/sqlite"
//	)
//
//	drv := sqlitedriver.New()
//	err := drv.Open(ctx, "file:dispatch.db", driver.WithPoolSize(10))
//	// Handle err before continuing.
//	db, err := grove.Open(drv)
//	// Handle err before continuing.
//	store := sqlite.New(db)
//	err = store.Migrate(ctx)
//
// # Write concurrency
//
// SQLite permits one writer at a time per database. Job claims, lease renewals
// and workflow reopens all compete for that writer, even within one process.
// Grove's default SQLite profile enables WAL, uses up to ten pooled connections
// and leaves busy_timeout at zero. A competing writer can therefore return
// SQLITE_BUSY immediately.
//
// Store operations that use withBusyRetry retry SQLITE_BUSY within a five-second
// window. Jittered exponential backoff keeps each pause below 48 milliseconds.
// Earlier caller cancellation prevents further retries. When the internal window
// expires, the operation returns its last busy error. Non-busy errors propagate
// without retry. This policy does not cover every database operation and does
// not guarantee that a contended write will succeed.
//
// The window bounds retry issuance and waits, not the duration of a synchronous
// driver call. A call that ignores context can return later. If that call succeeds,
// the store preserves the accepted result even when the deadline has passed.
//
// You can configure connection behavior when opening the driver, though Store
// does not expose the underlying sql.DB. driver.WithPoolSize sets the pool size.
// With Grove's SQLite driver, this optional DSN sets a five-second busy timeout
// on each connection:
//
//	file:dispatch.db?_pragma=busy_timeout(5000)
//
// Dispatch does not add this option by default. A single PRAGMA executed through
// a pooled handle only configures the connection that executes it. Use the DSN
// when you need the setting on every connection. Driver-level waits can extend
// a synchronous call beyond the store's retry window, so choose both settings
// with your request deadlines in mind. Reducing the pool can reduce contention
// within one handle; other handles and processes still compete for the writer.
// For sustained concurrent writes, consider the PostgreSQL store.
package sqlite
