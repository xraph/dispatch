package postgres

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// Migrations is the grove migration group for the dispatch bun store.
// It contains all schema migrations as Go functions, converted from the
// original embedded SQL files.
var Migrations = migrate.NewGroup("dispatch")

func init() {
	Migrations.MustRegister(
		// 001: Create jobs table and indexes.
		&migrate.Migration{
			Name:    "create_jobs_table",
			Version: "20240101120000",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				_, err := exec.Exec(ctx, `
					CREATE TABLE IF NOT EXISTS dispatch_jobs (
						id              TEXT PRIMARY KEY,
						name            TEXT NOT NULL,
						queue           TEXT NOT NULL DEFAULT 'default',
						payload         BYTEA NOT NULL,
						state           TEXT NOT NULL DEFAULT 'pending',
						priority        INTEGER NOT NULL DEFAULT 0,
						max_retries     INTEGER NOT NULL DEFAULT 3,
						retry_count     INTEGER NOT NULL DEFAULT 0,
						last_error      TEXT,
						scope_app_id    TEXT,
						scope_org_id    TEXT,
						worker_id       TEXT,
						run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
						started_at      TIMESTAMPTZ,
						completed_at    TIMESTAMPTZ,
						heartbeat_at    TIMESTAMPTZ,
						created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
						updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
					)`)
				if err != nil {
					return err
				}

				_, err = exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_jobs_dequeue
						ON dispatch_jobs (queue, priority DESC, run_at ASC)
						WHERE state IN ('pending', 'retrying')`)
				if err != nil {
					return err
				}

				_, err = exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_jobs_state
						ON dispatch_jobs (state)`)
				if err != nil {
					return err
				}

				_, err = exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_jobs_scope
						ON dispatch_jobs (scope_app_id, scope_org_id)`)
				if err != nil {
					return err
				}

				_, err = exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_jobs_heartbeat
						ON dispatch_jobs (heartbeat_at)
						WHERE state = 'running'`)
				return err
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				_, err := exec.Exec(ctx, `DROP TABLE IF EXISTS dispatch_jobs CASCADE`)
				return err
			},
		},

		// 002: Create workflow runs and checkpoints tables.
		&migrate.Migration{
			Name:    "create_workflows_tables",
			Version: "20240101120001",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				_, err := exec.Exec(ctx, `
					CREATE TABLE IF NOT EXISTS dispatch_workflow_runs (
						id              TEXT PRIMARY KEY,
						name            TEXT NOT NULL,
						state           TEXT NOT NULL DEFAULT 'running',
						input           BYTEA,
						output          BYTEA,
						error           TEXT,
						scope_app_id    TEXT,
						scope_org_id    TEXT,
						started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
						completed_at    TIMESTAMPTZ,
						created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
						updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
					)`)
				if err != nil {
					return err
				}

				_, err = exec.Exec(ctx, `
					CREATE TABLE IF NOT EXISTS dispatch_checkpoints (
						id              TEXT PRIMARY KEY,
						run_id          TEXT NOT NULL REFERENCES dispatch_workflow_runs(id) ON DELETE CASCADE,
						step_name       TEXT NOT NULL,
						data            BYTEA NOT NULL,
						created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
						UNIQUE(run_id, step_name)
					)`)
				if err != nil {
					return err
				}

				_, err = exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_workflow_runs_state
						ON dispatch_workflow_runs (state)`)
				if err != nil {
					return err
				}

				_, err = exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_checkpoints_run
						ON dispatch_checkpoints (run_id)`)
				return err
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				_, err := exec.Exec(ctx, `DROP TABLE IF EXISTS dispatch_checkpoints CASCADE`)
				if err != nil {
					return err
				}
				_, err = exec.Exec(ctx, `DROP TABLE IF EXISTS dispatch_workflow_runs CASCADE`)
				return err
			},
		},

		// 003: Create cron entries table.
		&migrate.Migration{
			Name:    "create_cron_entries_table",
			Version: "20240101120002",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				_, err := exec.Exec(ctx, `
					CREATE TABLE IF NOT EXISTS dispatch_cron_entries (
						id              TEXT PRIMARY KEY,
						name            TEXT NOT NULL UNIQUE,
						schedule        TEXT NOT NULL,
						job_name        TEXT NOT NULL,
						payload         BYTEA,
						scope_app_id    TEXT,
						scope_org_id    TEXT,
						last_run_at     TIMESTAMPTZ,
						next_run_at     TIMESTAMPTZ,
						locked_by       TEXT,
						locked_until    TIMESTAMPTZ,
						enabled         BOOLEAN NOT NULL DEFAULT TRUE,
						created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
						updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
					)`)
				if err != nil {
					return err
				}

				_, err = exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_cron_next
						ON dispatch_cron_entries (next_run_at)
						WHERE enabled = TRUE`)
				return err
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				_, err := exec.Exec(ctx, `DROP TABLE IF EXISTS dispatch_cron_entries CASCADE`)
				return err
			},
		},

		// 004: Create DLQ table.
		&migrate.Migration{
			Name:    "create_dlq_table",
			Version: "20240101120003",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				_, err := exec.Exec(ctx, `
					CREATE TABLE IF NOT EXISTS dispatch_dlq (
						id              TEXT PRIMARY KEY,
						job_id          TEXT NOT NULL,
						job_name        TEXT NOT NULL,
						queue           TEXT NOT NULL,
						payload         BYTEA NOT NULL,
						error           TEXT NOT NULL,
						retry_count     INTEGER NOT NULL,
						scope_app_id    TEXT,
						scope_org_id    TEXT,
						failed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
						replayed_at     TIMESTAMPTZ,
						created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
					)`)
				if err != nil {
					return err
				}

				_, err = exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_dlq_queue
						ON dispatch_dlq (queue, failed_at DESC)`)
				return err
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				_, err := exec.Exec(ctx, `DROP TABLE IF EXISTS dispatch_dlq CASCADE`)
				return err
			},
		},

		// 005: Create events table.
		&migrate.Migration{
			Name:    "create_events_table",
			Version: "20240101120004",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				_, err := exec.Exec(ctx, `
					CREATE TABLE IF NOT EXISTS dispatch_events (
						id              TEXT PRIMARY KEY,
						name            TEXT NOT NULL,
						payload         BYTEA,
						scope_app_id    TEXT,
						scope_org_id    TEXT,
						acked           BOOLEAN NOT NULL DEFAULT FALSE,
						created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
					)`)
				if err != nil {
					return err
				}

				_, err = exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_events_pending
						ON dispatch_events (name, created_at)
						WHERE acked = FALSE`)
				return err
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				_, err := exec.Exec(ctx, `DROP TABLE IF EXISTS dispatch_events CASCADE`)
				return err
			},
		},

		// 006: Create workers table for cluster support.
		&migrate.Migration{
			Name:    "create_workers_table",
			Version: "20240101120005",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				_, err := exec.Exec(ctx, `
					CREATE TABLE IF NOT EXISTS dispatch_workers (
						id              TEXT PRIMARY KEY,
						hostname        TEXT NOT NULL,
						queues          TEXT[] DEFAULT '{}',
						concurrency     INTEGER NOT NULL DEFAULT 10,
						state           TEXT NOT NULL DEFAULT 'active',
						is_leader       BOOLEAN NOT NULL DEFAULT FALSE,
						leader_until    TIMESTAMPTZ,
						last_seen       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
						metadata        JSONB DEFAULT '{}',
						created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
					)`)
				if err != nil {
					return err
				}

				_, err = exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_workers_state
						ON dispatch_workers (state)`)
				if err != nil {
					return err
				}

				_, err = exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_workers_leader
						ON dispatch_workers (is_leader)
						WHERE is_leader = TRUE`)
				if err != nil {
					return err
				}

				_, err = exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_workers_stale
						ON dispatch_workers (last_seen)
						WHERE state = 'active'`)
				return err
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				_, err := exec.Exec(ctx, `DROP TABLE IF EXISTS dispatch_workers CASCADE`)
				return err
			},
		},

		// 007: Schema additions - timeout, queue override, max_retries.
		&migrate.Migration{
			Name:    "schema_additions",
			Version: "20240101120006",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				// Job timeout stored as nanoseconds (int64). 0 means no timeout.
				_, err := exec.Exec(ctx, `ALTER TABLE dispatch_jobs ADD COLUMN IF NOT EXISTS timeout BIGINT NOT NULL DEFAULT 0`)
				if err != nil {
					return err
				}

				// Cron entry queue override. Empty string means use default queue.
				_, err = exec.Exec(ctx, `ALTER TABLE dispatch_cron_entries ADD COLUMN IF NOT EXISTS queue TEXT NOT NULL DEFAULT ''`)
				if err != nil {
					return err
				}

				// DLQ max_retries preserves the original job's retry budget for replay.
				_, err = exec.Exec(ctx, `ALTER TABLE dispatch_dlq ADD COLUMN IF NOT EXISTS max_retries INT NOT NULL DEFAULT 3`)
				return err
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				_, err := exec.Exec(ctx, `ALTER TABLE dispatch_jobs DROP COLUMN IF EXISTS timeout`)
				if err != nil {
					return err
				}

				_, err = exec.Exec(ctx, `ALTER TABLE dispatch_cron_entries DROP COLUMN IF EXISTS queue`)
				if err != nil {
					return err
				}

				_, err = exec.Exec(ctx, `ALTER TABLE dispatch_dlq DROP COLUMN IF EXISTS max_retries`)
				return err
			},
		},
	)
}
