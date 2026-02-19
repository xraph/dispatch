CREATE TABLE IF NOT EXISTS dispatch_workers (
    id              TEXT PRIMARY KEY,
    hostname        TEXT NOT NULL,
    queues          TEXT DEFAULT '[]',
    concurrency     INTEGER NOT NULL DEFAULT 10,
    state           TEXT NOT NULL DEFAULT 'active',
    is_leader       INTEGER NOT NULL DEFAULT 0,
    leader_until    TEXT,
    last_seen       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    metadata        TEXT DEFAULT '{}',
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_dispatch_workers_state
    ON dispatch_workers (state);

CREATE INDEX IF NOT EXISTS idx_dispatch_workers_leader
    ON dispatch_workers (is_leader)
    WHERE is_leader = 1;

CREATE INDEX IF NOT EXISTS idx_dispatch_workers_stale
    ON dispatch_workers (last_seen)
    WHERE state = 'active';
