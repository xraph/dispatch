package bunstore

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"

	"github.com/uptrace/bun"

	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Ensure Store implements all subsystem interfaces at compile time.
var (
	_ job.Store      = (*Store)(nil)
	_ workflow.Store = (*Store)(nil)
	_ cron.Store     = (*Store)(nil)
	_ dlq.Store      = (*Store)(nil)
	_ event.Store    = (*Store)(nil)
	_ cluster.Store  = (*Store)(nil)
)

// Store is a Bun ORM implementation of store.Store using PostgreSQL dialect.
// The caller owns the *bun.DB lifecycle; Store never closes it.
type Store struct {
	db     *bun.DB
	logger *slog.Logger
}

// Option configures the Store.
type Option func(*Store)

// WithLogger sets the logger for the store.
func WithLogger(logger *slog.Logger) Option {
	return func(s *Store) {
		s.logger = logger
	}
}

// New creates a new Bun store. The caller owns the db lifecycle — the Store
// will not close it on Close().
func New(db *bun.DB, opts ...Option) *Store {
	s := &Store{
		db:     db,
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// DB returns the underlying *bun.DB for advanced usage.
func (s *Store) DB() *bun.DB {
	return s.db
}

// Migrate runs all embedded SQL migration files in order.
func (s *Store) Migrate(ctx context.Context) error {
	// Create migrations tracking table.
	_, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS dispatch_migrations (
			filename TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("dispatch/bun: create migrations table: %w", err)
	}

	// Read embedded migration files.
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("dispatch/bun: read migrations: %w", err)
	}

	// Sort by filename for deterministic order.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		// Check if already applied.
		var applied bool
		err = s.db.QueryRowContext(ctx,
			`SELECT EXISTS(SELECT 1 FROM dispatch_migrations WHERE filename = ?)`,
			entry.Name(),
		).Scan(&applied)
		if err != nil {
			return fmt.Errorf("dispatch/bun: check migration %s: %w", entry.Name(), err)
		}
		if applied {
			continue
		}

		// Read and execute migration.
		data, readErr := fs.ReadFile(migrationsFS, "migrations/"+entry.Name())
		if readErr != nil {
			return fmt.Errorf("dispatch/bun: read migration %s: %w", entry.Name(), readErr)
		}

		_, execErr := s.db.ExecContext(ctx, string(data))
		if execErr != nil {
			return fmt.Errorf("dispatch/bun: execute migration %s: %w", entry.Name(), execErr)
		}

		// Record migration.
		_, recErr := s.db.ExecContext(ctx,
			`INSERT INTO dispatch_migrations (filename) VALUES (?)`,
			entry.Name(),
		)
		if recErr != nil {
			return fmt.Errorf("dispatch/bun: record migration %s: %w", entry.Name(), recErr)
		}

		s.logger.Info("applied migration", "file", entry.Name())
	}

	return nil
}

// Ping checks database connectivity.
func (s *Store) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Close is a no-op because the caller owns the *bun.DB lifecycle.
func (s *Store) Close() error {
	return nil
}
