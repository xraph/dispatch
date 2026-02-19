CREATE TABLE IF NOT EXISTS dispatch_events (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    payload         BLOB,
    scope_app_id    TEXT,
    scope_org_id    TEXT,
    acked           INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_dispatch_events_pending
    ON dispatch_events (name, created_at)
    WHERE acked = 0;
