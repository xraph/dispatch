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
);

CREATE TABLE IF NOT EXISTS dispatch_checkpoints (
    id              TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL REFERENCES dispatch_workflow_runs(id) ON DELETE CASCADE,
    step_name       TEXT NOT NULL,
    data            BYTEA NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(run_id, step_name)
);

CREATE INDEX IF NOT EXISTS idx_dispatch_workflow_runs_state
    ON dispatch_workflow_runs (state);

CREATE INDEX IF NOT EXISTS idx_dispatch_checkpoints_run
    ON dispatch_checkpoints (run_id);
