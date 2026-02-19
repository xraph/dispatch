CREATE TABLE IF NOT EXISTS dispatch_events (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    payload         BYTEA,
    scope_app_id    TEXT,
    scope_org_id    TEXT,
    acked           BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dispatch_events_pending
    ON dispatch_events (name, created_at)
    WHERE acked = FALSE;
