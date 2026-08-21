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

		// 006: Create artifacts and artifact links tables.
		&migrate.Migration{
			Name:    "create_artifacts_tables",
			Version: "20260811120000",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				if _, err := exec.Exec(ctx, `
					CREATE TABLE IF NOT EXISTS dispatch_artifacts (
						id              TEXT PRIMARY KEY,
						backend         TEXT NOT NULL,
						bucket          TEXT NOT NULL,
						key             TEXT NOT NULL,
						size            INTEGER NOT NULL DEFAULT 0,
						content_hash    TEXT,
						content_type    TEXT,
						lifecycle       TEXT NOT NULL,
						scope_app_id    TEXT,
						scope_org_id    TEXT,
						expires_at      TEXT,
						created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
						deleted_at      TEXT
					)`); err != nil {
					return err
				}

				// Partial unique index rather than a table constraint: a
				// purged key must be reusable, so only live rows collide.
				if _, err := exec.Exec(ctx, `
					CREATE UNIQUE INDEX IF NOT EXISTS uq_dispatch_artifacts_key
						ON dispatch_artifacts (backend, bucket, key)
						WHERE deleted_at IS NULL`); err != nil {
					return err
				}

				if _, err := exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_artifacts_sweep
						ON dispatch_artifacts (lifecycle, created_at)
						WHERE deleted_at IS NULL`); err != nil {
					return err
				}

				if _, err := exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_artifacts_purge
						ON dispatch_artifacts (deleted_at)
						WHERE deleted_at IS NOT NULL`); err != nil {
					return err
				}

				if _, err := exec.Exec(ctx, `
					CREATE TABLE IF NOT EXISTS dispatch_artifact_links (
						artifact_id     TEXT NOT NULL REFERENCES dispatch_artifacts(id) ON DELETE CASCADE,
						owner_kind      TEXT NOT NULL,
						owner_id        TEXT NOT NULL,
						name            TEXT NOT NULL,
						attempt         INTEGER NOT NULL DEFAULT 0,
						role            TEXT NOT NULL,
						created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
						PRIMARY KEY (artifact_id, owner_kind, owner_id, name, attempt)
					)`); err != nil {
					return err
				}

				if _, err := exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_artifact_links_owner
						ON dispatch_artifact_links (owner_kind, owner_id)`); err != nil {
					return err
				}

				_, err := exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_artifact_links_artifact
						ON dispatch_artifact_links (artifact_id)`)

				return err
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				if _, err := exec.Exec(ctx, `DROP TABLE IF EXISTS dispatch_artifact_links`); err != nil {
					return err
				}

				_, err := exec.Exec(ctx, `DROP TABLE IF EXISTS dispatch_artifacts`)

				return err
			},
		},

		// Lease columns. See the postgres migration of the same name for
		// why the lease lives on the row.
		//
		// Every ADD COLUMN is guarded, for the reason spelled out on the
		// resource migration below: SQLite has no ADD COLUMN IF NOT
		// EXISTS and grove runs Up outside any transaction, so a failure
		// at the third of the four would leave two columns added and no
		// row in grove_migrations, and every retry from every pod would
		// then die on "duplicate column name" forever, with no recovery
		// short of hand-written DDL against a production database. The
		// guard is the whole difference between a failed migration and a
		// wedged one.
		&migrate.Migration{
			Name:    "add_job_lease_columns",
			Version: "20260812120000",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				for _, c := range []struct{ name, ddl string }{
					{"lease_epoch", `INTEGER NOT NULL DEFAULT 0`},
					// TEXT, not a numeric timestamp: SQLite has no
					// timestamp type, every other time column here is
					// text, and the reclaim predicate compares
					// lease_expires_at against the driver's own rendering
					// of a time.Time. See ReclaimExpiredLeases for why that
					// rendering, not ISO-8601, is what has to be matched.
					{"lease_expires_at", `TEXT`},
					{"lease_ttl", `INTEGER NOT NULL DEFAULT 0`},
					{"evict_count", `INTEGER NOT NULL DEFAULT 0`},
				} {
					if err := addColumnIfMissing(ctx, exec,
						"dispatch_jobs", c.name, c.ddl); err != nil {
						return err
					}
				}

				// There is deliberately no backfill of lease_expires_at for
				// rows already running here, matching the postgres migration
				// of the same name. A one-shot backfill only sees rows that
				// exist the moment it runs, so it misses every job an old pod
				// claims later in a rolling upgrade, and every job claimed
				// through job.Store without lease options, which never grants
				// a lease at all. It also seeds an expiry in the past, which
				// evicts jobs old pods are still actively running.
				// ReclaimExpiredLeases adopts those rows on every sweep
				// instead, gated on silence. See job.UnleasedReclaimGrace.

				_, err := exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_jobs_lease
						ON dispatch_jobs (lease_expires_at) WHERE state = 'running'`)

				return err
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				if _, err := exec.Exec(ctx, `DROP INDEX IF EXISTS idx_dispatch_jobs_lease`); err != nil {
					return err
				}

				// Guarded for the same reason Up is: a Down that fails
				// halfway must be re-runnable.
				for _, col := range []string{
					"lease_epoch", "lease_expires_at", "lease_ttl", "evict_count",
				} {
					if err := dropColumnIfPresent(ctx, exec, "dispatch_jobs", col); err != nil {
						return err
					}
				}

				return nil
			},
		},

		// Resource model columns for resource-aware scheduling. See the
		// postgres migration of the same name for why the four canonical
		// dimensions get real scalar columns alongside the JSON copy.
		//
		// Every column defaults to zero or empty, so rows written before
		// this migration remain dequeueable by every worker during a
		// rolling deploy.
		&migrate.Migration{
			Name:    "job_resource_columns",
			Version: "20260812130000",
			// Every ADD COLUMN is guarded, and the guard is not
			// belt-and-braces — it is the only thing standing between a
			// partial failure and a permanently wedged database.
			//
			// SQLite has no ADD COLUMN IF NOT EXISTS and grove runs Up
			// bare, outside any transaction. So a failure at the sixth of
			// ten statements — a disk-full, a SIGKILL mid-deploy, a
			// cancelled context — leaves five columns added and no row in
			// grove_migrations. The retry then dies on "duplicate column
			// name: req_cpu_milli" and dies the same way forever: every
			// pod that starts reports the same error, and there is no
			// recovery short of an operator hand-writing DDL against a
			// production database. Postgres avoided this with IF NOT
			// EXISTS throughout; this is SQLite's equivalent.
			//
			// A transaction would have been the other answer. It is not
			// taken because the sqlite migrate executor routes through
			// the pooled *sql.DB rather than a pinned connection, so a
			// BEGIN issued via Exec is not guaranteed to be the same
			// session as the statements that follow it. Guarding each
			// statement needs no assumption about connection affinity and
			// makes the whole Up re-runnable from any point, which is
			// strictly stronger than atomic-or-nothing.
			Up: func(ctx context.Context, exec migrate.Executor) error {
				columns := []struct{ name, ddl string }{
					{"req_cpu_milli", `INTEGER NOT NULL DEFAULT 0`},
					{"req_memory_bytes", `INTEGER NOT NULL DEFAULT 0`},
					{"req_disk_bytes", `INTEGER NOT NULL DEFAULT 0`},
					{"req_gpu_milli", `INTEGER NOT NULL DEFAULT 0`},
					{"req_custom_keys", `TEXT NOT NULL DEFAULT ''`},
					// No JSONB in SQLite: plain TEXT columns hold the
					// full-fidelity JSON copy that fromJobModel reads
					// Resources/ResourceLimits back from.
					{"resource_requests", `TEXT`},
					{"resource_limits", `TEXT`},
					{"resource_class", `TEXT NOT NULL DEFAULT ''`},
					{"input_bytes", `INTEGER NOT NULL DEFAULT 0`},
					{"primary_input_hash", `TEXT`},
				}

				for _, c := range columns {
					if err := addColumnIfMissing(ctx, exec,
						"dispatch_jobs", c.name, c.ddl); err != nil {
						return err
					}
				}

				// SQLite has no INCLUDE clause for a covering index, so
				// the scalar columns the dequeue predicate reads go
				// directly in the key list instead.
				if _, err := exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_jobs_dequeue_res
						ON dispatch_jobs (queue, priority DESC, run_at ASC,
						                  req_cpu_milli, req_memory_bytes,
						                  req_disk_bytes, req_gpu_milli)
						WHERE state IN ('pending', 'retrying')`); err != nil {
					return err
				}

				// idx_dispatch_jobs_dequeue (the initial migration) is a
				// strict key PREFIX of the index just created, over the
				// same partial predicate, so every query it could serve
				// the new one serves too. Keeping both costs a second
				// B-tree insert on every enqueue and a second delete on
				// every claim, forever, for a plan SQLite would never
				// choose. Postgres drops its equivalent for the same
				// reason.
				//
				// Dropped after the replacement exists, so there is no
				// instant at which the dequeue statement has no index.
				_, err := exec.Exec(ctx, `DROP INDEX IF EXISTS idx_dispatch_jobs_dequeue`)

				return err
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				// Restore the prefix index before dropping its superset,
				// same ordering rule in reverse.
				if _, err := exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_jobs_dequeue
						ON dispatch_jobs (queue, priority DESC, run_at ASC)
						WHERE state IN ('pending', 'retrying')`); err != nil {
					return err
				}

				if _, err := exec.Exec(ctx,
					`DROP INDEX IF EXISTS idx_dispatch_jobs_dequeue_res`); err != nil {
					return err
				}

				// Guarded for the same reason Up is: a Down that fails
				// halfway must be re-runnable.
				for _, col := range []string{
					"req_cpu_milli", "req_memory_bytes", "req_disk_bytes",
					"req_gpu_milli", "req_custom_keys", "resource_requests",
					"resource_limits", "resource_class", "input_bytes",
					"primary_input_hash",
				} {
					if err := dropColumnIfPresent(ctx, exec, "dispatch_jobs", col); err != nil {
						return err
					}
				}

				return nil
			},
		},
		// Replay rebuilds a job from the DLQ row and enqueues it directly,
		// so anything the row does not carry the replayed job silently takes
		// a default for. Losing lease_ttl is the worst of them: a six-hour
		// job replayed on the pool default lease lapses mid-run and is
		// reclaimed and restarted forever without finishing. See dlq.Entry.
		&migrate.Migration{
			Name:    "dlq_job_execution_columns",
			Version: "20260817120000",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				// Every ADD COLUMN is guarded, for the reason spelled out on
				// the lease and resource migrations above: SQLite has no ADD
				// COLUMN IF NOT EXISTS and grove runs Up outside any
				// transaction, so a failure partway through would leave some
				// columns added and no row in grove_migrations, and every
				// retry from every pod would then die on "duplicate column
				// name" forever.
				for _, c := range []struct{ name, ddl string }{
					{"priority", `INTEGER NOT NULL DEFAULT 0`},
					{"timeout", `INTEGER NOT NULL DEFAULT 0`},
					{"lease_ttl", `INTEGER NOT NULL DEFAULT 0`},
					{"artifact_bindings", `BLOB`},
					// Nullable TEXT, matching dispatch_jobs: resource.
					// EncodeSetString writes NULL for a zero Set rather than
					// an empty object, so a job that declared nothing reads
					// back as a nil Set rather than an empty one.
					{"resources", `TEXT`},
					{"resource_limits", `TEXT`},
					{"resource_class", `TEXT NOT NULL DEFAULT ''`},
					{"input_bytes", `INTEGER NOT NULL DEFAULT 0`},
					{"primary_input_hash", `TEXT`},
				} {
					if err := addColumnIfMissing(ctx, exec,
						"dispatch_dlq", c.name, c.ddl); err != nil {
						return err
					}
				}

				return nil
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				// Guarded for the same reason Up is: a Down that fails
				// halfway must be re-runnable.
				for _, col := range []string{
					"priority", "timeout", "lease_ttl", "artifact_bindings",
					"resources", "resource_limits", "resource_class",
					"input_bytes", "primary_input_hash",
				} {
					if err := dropColumnIfPresent(ctx, exec, "dispatch_dlq", col); err != nil {
						return err
					}
				}

				return nil
			},
		},
	)
}

// columnExists reports whether table already has the named column.
//
// pragma_table_info is the table-valued form of PRAGMA table_info, which
// means it can be queried with bind parameters like any other relation
// rather than string-formatted into a PRAGMA statement.
func columnExists(ctx context.Context, exec migrate.Executor, table, column string) (bool, error) {
	rows, err := exec.Query(ctx,
		`SELECT 1 FROM pragma_table_info(?) WHERE name = ?`, table, column)
	if err != nil {
		return false, err
	}

	found := rows.Next()

	if closeErr := rows.Close(); closeErr != nil {
		return false, closeErr
	}

	return found, rows.Err()
}

// addColumnIfMissing is SQLite's stand-in for ADD COLUMN IF NOT EXISTS.
func addColumnIfMissing(ctx context.Context, exec migrate.Executor, table, column, ddl string) error {
	present, err := columnExists(ctx, exec, table, column)
	if err != nil || present {
		return err
	}

	_, err = exec.Exec(ctx, `ALTER TABLE `+table+` ADD COLUMN `+column+` `+ddl)

	return err
}

// dropColumnIfPresent is SQLite's stand-in for DROP COLUMN IF EXISTS.
func dropColumnIfPresent(ctx context.Context, exec migrate.Executor, table, column string) error {
	present, err := columnExists(ctx, exec, table, column)
	if err != nil || !present {
		return err
	}

	_, err = exec.Exec(ctx, `ALTER TABLE `+table+` DROP COLUMN `+column)

	return err
}
