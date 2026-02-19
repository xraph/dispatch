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
);

CREATE INDEX IF NOT EXISTS idx_dispatch_jobs_dequeue
    ON dispatch_jobs (queue, priority DESC, run_at ASC)
    WHERE state IN ('pending', 'retrying');

CREATE INDEX IF NOT EXISTS idx_dispatch_jobs_state
    ON dispatch_jobs (state);

CREATE INDEX IF NOT EXISTS idx_dispatch_jobs_scope
    ON dispatch_jobs (scope_app_id, scope_org_id);

CREATE INDEX IF NOT EXISTS idx_dispatch_jobs_heartbeat
    ON dispatch_jobs (heartbeat_at)
    WHERE state = 'running';
