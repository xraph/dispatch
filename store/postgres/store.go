package postgres

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

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

// Store is a PostgreSQL implementation of store.Store using pgx/v5.
// It uses pgxpool for connection pooling, SKIP LOCKED for atomic dequeue,
// advisory locks for leader election, and LISTEN/NOTIFY for events.
type Store struct {
	pool   *pgxpool.Pool
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

// New creates a new PostgreSQL store from a connection string.
// The connString should be a PostgreSQL connection URL, e.g.:
// "postgres://user:pass@localhost:5432/dispatch?sslmode=disable"
func New(ctx context.Context, connString string, opts ...Option) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("dispatch/postgres: parse config: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("dispatch/postgres: connect: %w", err)
	}

	s := &Store{
		pool:   pool,
		logger: slog.Default(),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}

// NewFromPool creates a new PostgreSQL store from an existing pgxpool.Pool.
func NewFromPool(pool *pgxpool.Pool, opts ...Option) *Store {
	s := &Store{
		pool:   pool,
		logger: slog.Default(),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Migrate runs all embedded SQL migration files in order.
func (s *Store) Migrate(ctx context.Context) error {
	// Create migrations tracking table.
	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dispatch_migrations (
			filename TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("dispatch/postgres: create migrations table: %w", err)
	}

	// Read embedded migration files.
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("dispatch/postgres: read migrations: %w", err)
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
		err = s.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM dispatch_migrations WHERE filename = $1)`,
			entry.Name(),
		).Scan(&applied)
		if err != nil {
			return fmt.Errorf("dispatch/postgres: check migration %s: %w", entry.Name(), err)
		}
		if applied {
			continue
		}

		// Read and execute migration.
		data, readErr := fs.ReadFile(migrationsFS, "migrations/"+entry.Name())
		if readErr != nil {
			return fmt.Errorf("dispatch/postgres: read migration %s: %w", entry.Name(), readErr)
		}

		_, execErr := s.pool.Exec(ctx, string(data))
		if execErr != nil {
			return fmt.Errorf("dispatch/postgres: execute migration %s: %w", entry.Name(), execErr)
		}

		// Record migration.
		_, recErr := s.pool.Exec(ctx,
			`INSERT INTO dispatch_migrations (filename) VALUES ($1)`,
			entry.Name(),
		)
		if recErr != nil {
			return fmt.Errorf("dispatch/postgres: record migration %s: %w", entry.Name(), recErr)
		}

		s.logger.Info("applied migration", "file", entry.Name())
	}

	return nil
}

// Ping checks database connectivity.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close closes the connection pool.
func (s *Store) Close() error {
	s.pool.Close()
	return nil
}

// Pool returns the underlying pgxpool.Pool for advanced usage.
func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}
