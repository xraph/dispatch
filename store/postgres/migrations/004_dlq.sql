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
);

CREATE INDEX IF NOT EXISTS idx_dispatch_dlq_queue
    ON dispatch_dlq (queue, failed_at DESC);
