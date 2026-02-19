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
);

CREATE INDEX IF NOT EXISTS idx_dispatch_cron_next
    ON dispatch_cron_entries (next_run_at)
    WHERE enabled = TRUE;
