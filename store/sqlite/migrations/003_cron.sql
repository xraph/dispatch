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
);

CREATE INDEX IF NOT EXISTS idx_dispatch_cron_next
    ON dispatch_cron_entries (next_run_at)
    WHERE enabled = 1;
