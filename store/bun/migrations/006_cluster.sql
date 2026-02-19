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
);

CREATE INDEX IF NOT EXISTS idx_dispatch_workers_state
    ON dispatch_workers (state);

CREATE INDEX IF NOT EXISTS idx_dispatch_workers_leader
    ON dispatch_workers (is_leader)
    WHERE is_leader = TRUE;

CREATE INDEX IF NOT EXISTS idx_dispatch_workers_stale
    ON dispatch_workers (last_seen)
    WHERE state = 'active';
