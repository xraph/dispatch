// Package store defines the aggregate persistence interface.
//
// Each subsystem (job, workflow, cron, dlq, event, cluster) defines its own
// store interface. The composite [Store] composes them all. A single backend
// need only implement Store to satisfy every subsystem's persistence contract.
//
// The composite interface:
//
//	type Store interface {
//	    job.Store
//	    workflow.Store
//	    cron.Store
//	    dlq.Store
//	    event.Store
//	    cluster.Store
//
//	    Migrate(ctx context.Context) error
//	    Ping(ctx context.Context) error
//	    Close() error
//	}
//
// # Available Backends
//
//   - store/memory — in-memory store for development and testing
//   - store/postgres — PostgreSQL backend using pgx/v5
//   - store/bun — Bun ORM backend
//   - store/sqlite — SQLite backend
//   - store/redis — Redis backend
//
// # Usage
//
//	import "github.com/xraph/dispatch/store/postgres"
//
//	s, err := postgres.New(ctx, "postgres://user:pass@localhost/dispatch")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer s.Close()
//
//	d, err := dispatch.New(dispatch.WithStore(s))
//
// # Migrations
//
// Call Migrate once at startup to create or update the schema:
//
//	if err := s.Migrate(ctx); err != nil {
//	    log.Fatal(err)
//	}
package store
