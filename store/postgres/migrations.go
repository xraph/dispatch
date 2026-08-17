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

		// 007: Create artifacts and artifact links tables.
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
						size            BIGINT NOT NULL DEFAULT 0,
						content_hash    TEXT,
						content_type    TEXT,
						lifecycle       TEXT NOT NULL,
						scope_app_id    TEXT,
						scope_org_id    TEXT,
						expires_at      TIMESTAMPTZ,
						created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
						deleted_at      TIMESTAMPTZ
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
					CREATE INDEX IF NOT EXISTS idx_dispatch_artifacts_hash
						ON dispatch_artifacts (content_hash)
						WHERE content_hash IS NOT NULL`); err != nil {
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
						created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
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

		// 008: Lease columns. Execution becomes a lease: lease_ttl on the
		// row is what lets one reclaim query serve a 30-second job and a
		// six-hour one, and lease_epoch fences a worker that was reclaimed
		// while it was merely paused.
		//
		// This migration runs against a LIVE fleet — extension.Start calls
		// Migrate by default, so the first upgraded pod executes it while
		// every old pod is still enqueueing, claiming and completing on
		// dispatch_jobs. See migration 009 below, which hit the identical
		// hazards; the same three mechanisms are used here, and 008 runs
		// first so it has to be at least as safe.
		&migrate.Migration{
			Name:    "add_job_lease_columns",
			Version: "20260812120000",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				// Batched into one ALTER and bounded by lock_timeout: a
				// pending ACCESS EXCLUSIVE request blocks the whole lock
				// queue behind it, so an ALTER that queues behind one
				// in-flight SELECT ... FOR UPDATE SKIP LOCKED stalls every
				// enqueue and completion in the fleet. All four defaults
				// are constants, so on PostgreSQL 11+ this is a catalog
				// update with no table rewrite; acquiring the lock is the
				// only part that can wait, and that is what the timeout
				// bounds.
				if err := withLockTimeout(ctx, exec, `
					ALTER TABLE dispatch_jobs
						ADD COLUMN IF NOT EXISTS lease_epoch      INTEGER NOT NULL DEFAULT 0,
						ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ,
						ADD COLUMN IF NOT EXISTS lease_ttl        BIGINT NOT NULL DEFAULT 0,
						ADD COLUMN IF NOT EXISTS evict_count      INTEGER NOT NULL DEFAULT 0`); err != nil {
					return err
				}

				// There is deliberately no backfill of lease_expires_at for
				// rows already running here. An earlier version of this
				// migration seeded them from COALESCE(heartbeat_at,
				// started_at, NOW()) so the first sweep would collect them,
				// and that was too weak in one direction and too strong in
				// the other. Too weak: it only sees rows that exist the
				// moment it runs, so during a rolling upgrade every job an
				// old pod claims afterwards is stranded exactly as before,
				// and so is every job claimed later through job.Store
				// without lease options, which is a supported call that
				// never grants a lease. Too strong: seeding an expiry in the
				// past evicts jobs old pods are still actively running,
				// because an old binary's heartbeats do not push an expiry
				// it does not know about.
				//
				// ReclaimExpiredLeases adopts those rows instead, on every
				// sweep rather than once, and gated on silence so a worker
				// that is still heartbeating is never touched. See
				// job.UnleasedReclaimGrace.

				// CONCURRENTLY: a plain CREATE INDEX holds a SHARE lock
				// for the whole build, blocking every INSERT, UPDATE and
				// DELETE on dispatch_jobs until it finishes. Grove does
				// not wrap Up in a transaction — migrate.Orchestrator
				// calls m.Up directly and the pg executor runs autocommit
				// on a pinned connection — so CONCURRENTLY, which cannot
				// run inside one, is available here. Migration 009 relies
				// on the same property.
				//
				// Its cost is that a failed build leaves an INVALID index
				// that the planner ignores and IF NOT EXISTS would then
				// skip forever, so an invalid leftover is dropped first.
				// That is what makes a retry converge instead of
				// reporting success over an unusable index.
				//
				// Built after the backfill so the write lands in the index
				// as it is created rather than as a maintenance cost on
				// top of it.
				if err := dropIfInvalid(ctx, exec, "idx_dispatch_jobs_lease"); err != nil {
					return err
				}

				_, err := exec.Exec(ctx, `
					CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dispatch_jobs_lease
						ON dispatch_jobs (lease_expires_at)
						WHERE state = 'running'`)

				return err
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				_, err := exec.Exec(ctx,
					`DROP INDEX CONCURRENTLY IF EXISTS idx_dispatch_jobs_lease`)
				if err != nil {
					return err
				}

				return withLockTimeout(ctx, exec, `
					ALTER TABLE dispatch_jobs
						DROP COLUMN IF EXISTS lease_epoch,
						DROP COLUMN IF EXISTS lease_expires_at,
						DROP COLUMN IF EXISTS lease_ttl,
						DROP COLUMN IF EXISTS evict_count`)
			},
		},

		// 009: Resource model columns for resource-aware scheduling.
		//
		// Every column defaults to zero or empty, so rows written before
		// this migration remain dequeueable by every worker during a
		// rolling deploy.
		//
		// This migration runs against a LIVE fleet. extension.Start calls
		// Migrate by default, so the first upgraded pod executes it while
		// every old pod is still enqueueing, claiming and completing on
		// dispatch_jobs — the hottest table in the schema. Both halves are
		// written for that: the DDL takes its exclusive lock under a
		// timeout rather than queueing behind a long-running claim, and
		// the index is built without blocking writes at all.
		&migrate.Migration{
			Name:    "job_resource_columns",
			Version: "20260812130000",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				// One ALTER, not ten. Each statement takes its own
				// ACCESS EXCLUSIVE lock on dispatch_jobs, so ten of them
				// are ten separate chances to queue behind an in-flight
				// SELECT ... FOR UPDATE SKIP LOCKED — and every enqueue
				// and completion in the fleet queues behind THAT, because
				// a waiting ACCESS EXCLUSIVE request blocks the lock
				// queue ahead of it. Migration 008 already batches its
				// four this way.
				//
				// All ten defaults are constants, so on PostgreSQL 11+
				// this is a catalog update and does not rewrite the
				// table; the lock is held for microseconds once acquired.
				// Acquiring it is the part that can wait, which is what
				// the lock_timeout below bounds.
				if err := withLockTimeout(ctx, exec, `
					ALTER TABLE dispatch_jobs
						ADD COLUMN IF NOT EXISTS req_cpu_milli     BIGINT NOT NULL DEFAULT 0,
						ADD COLUMN IF NOT EXISTS req_memory_bytes  BIGINT NOT NULL DEFAULT 0,
						ADD COLUMN IF NOT EXISTS req_disk_bytes    BIGINT NOT NULL DEFAULT 0,
						ADD COLUMN IF NOT EXISTS req_gpu_milli     BIGINT NOT NULL DEFAULT 0,
						ADD COLUMN IF NOT EXISTS req_custom_keys   TEXT NOT NULL DEFAULT '',
						ADD COLUMN IF NOT EXISTS resource_requests JSONB,
						ADD COLUMN IF NOT EXISTS resource_limits   JSONB,
						ADD COLUMN IF NOT EXISTS resource_class    TEXT NOT NULL DEFAULT '',
						ADD COLUMN IF NOT EXISTS input_bytes       BIGINT NOT NULL DEFAULT 0,
						ADD COLUMN IF NOT EXISTS primary_input_hash TEXT`); err != nil {
					return err
				}

				// Covering index: the dequeue predicate reads all four
				// scalars for every candidate row, so including them
				// keeps the scan index-only.
				//
				// CONCURRENTLY because a plain CREATE INDEX takes a SHARE
				// lock for the whole build, which blocks every INSERT,
				// UPDATE and DELETE on dispatch_jobs — the entire fleet's
				// enqueues, claims and completions, for as long as the
				// build takes on a production-sized queue. Grove does not
				// wrap Up in a transaction (migrate.Orchestrator.Migrate
				// calls m.Up directly, and the pg executor runs
				// autocommit on a pinned connection), so CONCURRENTLY,
				// which cannot run inside one, is available here.
				//
				// The cost of CONCURRENTLY is that a failed build leaves
				// an INVALID index behind, which the planner ignores and
				// IF NOT EXISTS would then silently skip forever. So an
				// invalid leftover is dropped first, which makes a retry
				// after a failed migration converge instead of wedging.
				if err := dropIfInvalid(ctx, exec, "idx_dispatch_jobs_dequeue_res"); err != nil {
					return err
				}

				if _, err := exec.Exec(ctx, `
					CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dispatch_jobs_dequeue_res
						ON dispatch_jobs (queue, priority DESC, run_at ASC)
						INCLUDE (req_cpu_milli, req_memory_bytes,
						         req_disk_bytes, req_gpu_milli)
						WHERE state IN ('pending', 'retrying')`); err != nil {
					return err
				}

				// idx_dispatch_jobs_dequeue (migration 001) has the
				// IDENTICAL key list and the IDENTICAL partial predicate;
				// the index just created differs only by an INCLUDE
				// payload, which makes it a strict superset. Keeping both
				// costs a second B-tree insert on every enqueue and a
				// second delete on every claim, forever, on the hottest
				// table in the schema — for a plan the planner would
				// never choose.
				//
				// Dropped AFTER the replacement is valid, so there is no
				// instant at which the dequeue statement has no index.
				_, err := exec.Exec(ctx,
					`DROP INDEX CONCURRENTLY IF EXISTS idx_dispatch_jobs_dequeue`)

				return err
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				// Restore 001's index before dropping its superset, same
				// ordering rule in reverse: never leave the dequeue
				// statement unindexed.
				if err := dropIfInvalid(ctx, exec, "idx_dispatch_jobs_dequeue"); err != nil {
					return err
				}

				if _, err := exec.Exec(ctx, `
					CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dispatch_jobs_dequeue
						ON dispatch_jobs (queue, priority DESC, run_at ASC)
						WHERE state IN ('pending', 'retrying')`); err != nil {
					return err
				}

				if _, err := exec.Exec(ctx,
					`DROP INDEX CONCURRENTLY IF EXISTS idx_dispatch_jobs_dequeue_res`); err != nil {
					return err
				}

				return withLockTimeout(ctx, exec, `
					ALTER TABLE dispatch_jobs
						DROP COLUMN IF EXISTS req_cpu_milli,
						DROP COLUMN IF EXISTS req_memory_bytes,
						DROP COLUMN IF EXISTS req_disk_bytes,
						DROP COLUMN IF EXISTS req_gpu_milli,
						DROP COLUMN IF EXISTS req_custom_keys,
						DROP COLUMN IF EXISTS resource_requests,
						DROP COLUMN IF EXISTS resource_limits,
						DROP COLUMN IF EXISTS resource_class,
						DROP COLUMN IF EXISTS input_bytes,
						DROP COLUMN IF EXISTS primary_input_hash`)
			},
		},
	)
}

// ddlLockTimeout bounds how long a DDL statement waits for its ACCESS
// EXCLUSIVE lock on a table the fleet is actively writing.
//
// Without it the ALTER waits indefinitely behind whatever claim happens
// to hold a row lock, AND — because a pending ACCESS EXCLUSIVE request
// blocks everything queued behind it — every enqueue and completion in
// the fleet waits behind the ALTER. A migration that cannot get the lock
// promptly must fail and be retried, not convert one slow query into a
// fleet-wide stall.
//
// Three seconds is long enough to win an uncontended queue comfortably
// and short enough that a failed attempt is a blip rather than an
// outage. Migrate is idempotent here, so a retry is free.
const ddlLockTimeout = "3s"

// withLockTimeout runs one DDL statement under ddlLockTimeout.
//
// SET rather than SET LOCAL because grove runs Up outside a transaction,
// where SET LOCAL is a no-op with a warning. The pg executor pins one
// connection for the whole migration run (see pgmigrate.Executor), so
// the setting would otherwise leak into every migration after this one —
// hence the reset, which runs even when the statement failed.
func withLockTimeout(ctx context.Context, exec migrate.Executor, stmt string) error {
	if _, err := exec.Exec(ctx, `SET lock_timeout = '`+ddlLockTimeout+`'`); err != nil {
		return err
	}

	_, execErr := exec.Exec(ctx, stmt)

	if _, err := exec.Exec(ctx, `SET lock_timeout = DEFAULT`); err != nil && execErr == nil {
		return err
	}

	return execErr
}

// dropIfInvalid removes an index left INVALID by a CREATE INDEX
// CONCURRENTLY that failed partway.
//
// Such an index exists in the catalog but is ignored by the planner, so
// CREATE INDEX CONCURRENTLY IF NOT EXISTS sees it, does nothing, and the
// table permanently has no usable index while every subsequent migration
// run reports success. Dropping it first is what makes a retry after a
// failed migration converge.
func dropIfInvalid(ctx context.Context, exec migrate.Executor, name string) error {
	rows, err := exec.Query(ctx, `
		SELECT 1
		FROM pg_class c
		JOIN pg_index i ON i.indexrelid = c.oid
		WHERE c.relname = $1 AND NOT i.indisvalid`, name)
	if err != nil {
		return err
	}

	invalid := rows.Next()

	if closeErr := rows.Close(); closeErr != nil {
		return closeErr
	}

	if rowsErr := rows.Err(); rowsErr != nil {
		return rowsErr
	}

	if !invalid {
		return nil
	}

	_, err = exec.Exec(ctx, `DROP INDEX CONCURRENTLY IF EXISTS `+name)

	return err
}
