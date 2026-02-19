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
);

CREATE INDEX IF NOT EXISTS idx_dispatch_dlq_queue
    ON dispatch_dlq (queue, failed_at DESC);
