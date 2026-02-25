package sqlite

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// Migrations is the grove migration group for the dispatch sqlite store.
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
						payload         BLOB NOT NULL,
						state           TEXT NOT NULL DEFAULT 'pending',
						priority        INTEGER NOT NULL DEFAULT 0,
						max_retries     INTEGER NOT NULL DEFAULT 3,
						retry_count     INTEGER NOT NULL DEFAULT 0,
						last_error      TEXT,
						scope_app_id    TEXT,
						scope_org_id    TEXT,
						worker_id       TEXT,
						run_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
						started_at      TEXT,
						completed_at    TEXT,
						heartbeat_at    TEXT,
						timeout         INTEGER NOT NULL DEFAULT 0,
						created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
						updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
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
				_, err := exec.Exec(ctx, `DROP TABLE IF EXISTS dispatch_jobs`)
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
						input           BLOB,
						output          BLOB,
						error           TEXT,
						scope_app_id    TEXT,
						scope_org_id    TEXT,
						started_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
						completed_at    TEXT,
						created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
						updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
					)`)
				if err != nil {
					return err
				}

				_, err = exec.Exec(ctx, `
					CREATE TABLE IF NOT EXISTS dispatch_checkpoints (
						id              TEXT PRIMARY KEY,
						run_id          TEXT NOT NULL REFERENCES dispatch_workflow_runs(id) ON DELETE CASCADE,
						step_name       TEXT NOT NULL,
						data            BLOB NOT NULL,
						created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
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
				_, err := exec.Exec(ctx, `DROP TABLE IF EXISTS dispatch_checkpoints`)
				if err != nil {
					return err
				}
				_, err = exec.Exec(ctx, `DROP TABLE IF EXISTS dispatch_workflow_runs`)
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
						queue           TEXT NOT NULL DEFAULT '',
						payload         BLOB,
						scope_app_id    TEXT,
						scope_org_id    TEXT,
						last_run_at     TEXT,
						next_run_at     TEXT,
						locked_by       TEXT,
						locked_until    TEXT,
						enabled         INTEGER NOT NULL DEFAULT 1,
						created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
						updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
					)`)
				if err != nil {
					return err
				}

				_, err = exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_cron_next
						ON dispatch_cron_entries (next_run_at)
						WHERE enabled = 1`)
				return err
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				_, err := exec.Exec(ctx, `DROP TABLE IF EXISTS dispatch_cron_entries`)
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
						payload         BLOB NOT NULL,
						error           TEXT NOT NULL,
						retry_count     INTEGER NOT NULL,
						max_retries     INTEGER NOT NULL DEFAULT 3,
						scope_app_id    TEXT,
						scope_org_id    TEXT,
						failed_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
						replayed_at     TEXT,
						created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
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
				_, err := exec.Exec(ctx, `DROP TABLE IF EXISTS dispatch_dlq`)
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
						payload         BLOB,
						scope_app_id    TEXT,
						scope_org_id    TEXT,
						acked           INTEGER NOT NULL DEFAULT 0,
						created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
					)`)
				if err != nil {
					return err
				}

				_, err = exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_events_pending
						ON dispatch_events (name, created_at)
						WHERE acked = 0`)
				return err
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				_, err := exec.Exec(ctx, `DROP TABLE IF EXISTS dispatch_events`)
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
						queues          TEXT NOT NULL DEFAULT '[]',
						concurrency     INTEGER NOT NULL DEFAULT 10,
						state           TEXT NOT NULL DEFAULT 'active',
						is_leader       INTEGER NOT NULL DEFAULT 0,
						leader_until    TEXT,
						last_seen       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
						metadata        TEXT NOT NULL DEFAULT '{}',
						created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
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
						WHERE is_leader = 1`)
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
				_, err := exec.Exec(ctx, `DROP TABLE IF EXISTS dispatch_workers`)
				return err
			},
		},
	)
}
