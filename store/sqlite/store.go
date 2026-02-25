package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/sqlitedriver"
	_ "github.com/xraph/grove/drivers/sqlitedriver/sqlitemigrate" // register sqlite migration executor
	"github.com/xraph/grove/migrate"

	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// Ensure Store implements all subsystem interfaces at compile time.
var (
	_ job.Store      = (*Store)(nil)
	_ workflow.Store = (*Store)(nil)
	_ cron.Store     = (*Store)(nil)
	_ dlq.Store      = (*Store)(nil)
	_ event.Store    = (*Store)(nil)
	_ cluster.Store  = (*Store)(nil)
)

// Store is a grove ORM implementation of store.Store using SQLite dialect.
// The caller owns the *grove.DB lifecycle; Store never closes it.
type Store struct {
	db     *grove.DB
	sdb    *sqlitedriver.SqliteDB
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

// New creates a new grove store. The caller owns the db lifecycle -- the Store
// will not close it on Close().
func New(db *grove.DB, opts ...Option) *Store {
	s := &Store{
		db:     db,
		sdb:    sqlitedriver.Unwrap(db),
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// DB returns the underlying *grove.DB for advanced usage.
func (s *Store) DB() *grove.DB {
	return s.db
}

// Migrate runs programmatic migrations via the grove orchestrator.
func (s *Store) Migrate(ctx context.Context) error {
	executor, err := migrate.NewExecutorFor(s.sdb)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: create migration executor: %w", err)
	}
	orch := migrate.NewOrchestrator(executor, Migrations)
	if _, err := orch.Migrate(ctx); err != nil {
		return fmt.Errorf("dispatch/sqlite: migration failed: %w", err)
	}
	return nil
}

// Ping checks database connectivity.
func (s *Store) Ping(ctx context.Context) error {
	return s.db.Ping(ctx)
}

// Close is a no-op because the caller owns the *grove.DB lifecycle.
func (s *Store) Close() error {
	return nil
}

// ── helpers ──────────────────────────────────────────────────────

// isNoRows returns true when err indicates no rows were found.
func isNoRows(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

// isDuplicateKey checks if a SQLite error is a unique constraint violation.
func isDuplicateKey(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "UNIQUE constraint failed")
}
