CREATE TABLE IF NOT EXISTS events (
    id                TEXT PRIMARY KEY NOT NULL,
    installation_id   TEXT NOT NULL,
    invocation_id     TEXT NOT NULL,
    occurred_at       TEXT NOT NULL,
    received_at       TEXT NOT NULL,
    command           TEXT NOT NULL,
    lucy_version      TEXT NOT NULL,
    lucy_commit       TEXT NOT NULL,
    channel           TEXT NOT NULL,
    os                TEXT NOT NULL,
    arch              TEXT NOT NULL,
    duration_ms       INTEGER,
    outcome           TEXT NOT NULL,
    error_code        TEXT,
    schema_version    INTEGER NOT NULL CHECK (schema_version > 0),
    collection_policy TEXT NOT NULL,
    payload_bytes     INTEGER NOT NULL CHECK (payload_bytes >= 0),
    ingest_digest     TEXT NOT NULL,
    CHECK (duration_ms IS NULL OR duration_ms >= 0)
);

CREATE INDEX IF NOT EXISTS idx_events_received_at
    ON events (received_at);
CREATE INDEX IF NOT EXISTS idx_events_installation_received
    ON events (installation_id, received_at);

CREATE TABLE IF NOT EXISTS context_snapshots (
    event_id     TEXT NOT NULL,
    ordinal      INTEGER NOT NULL CHECK (ordinal >= 0),
    context_type TEXT NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('present', 'absent', 'failed')),
    data         TEXT,
    PRIMARY KEY (event_id, ordinal),
    FOREIGN KEY (event_id) REFERENCES events (id) ON DELETE CASCADE,
    CHECK (status <> 'absent' OR data IS NULL)
);

CREATE TABLE IF NOT EXISTS reports (
    id                TEXT PRIMARY KEY NOT NULL,
    installation_id   TEXT NOT NULL,
    invocation_id     TEXT,
    kind              TEXT NOT NULL CHECK (kind IN ('failure', 'panic', 'agent_feedback')),
    agent             TEXT,
    model             TEXT,
    created_at        TEXT NOT NULL,
    received_at       TEXT NOT NULL,
    schema_version    INTEGER NOT NULL CHECK (schema_version > 0),
    collection_policy TEXT NOT NULL,
    r2_key            TEXT NOT NULL UNIQUE,
    body_bytes        INTEGER NOT NULL CHECK (body_bytes >= 0),
    body_sha256       TEXT NOT NULL,
    payload_bytes     INTEGER NOT NULL CHECK (payload_bytes >= 0),
    state             TEXT NOT NULL CHECK (state IN ('pending', 'stored', 'deleting')),
    stored_at         TEXT,
    delete_claimed_at TEXT,
    delete_claim_id   INTEGER,
    CHECK (state <> 'deleting'
           OR (delete_claimed_at IS NOT NULL AND delete_claim_id IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS idx_reports_received_at
    ON reports (received_at);
CREATE INDEX IF NOT EXISTS idx_reports_state_received
    ON reports (state, received_at);
CREATE INDEX IF NOT EXISTS idx_reports_delete_claim_id
    ON reports (delete_claim_id);
CREATE INDEX IF NOT EXISTS idx_reports_kind_received
    ON reports (kind, received_at);
CREATE INDEX IF NOT EXISTS idx_reports_installation_received
    ON reports (installation_id, received_at);

CREATE TABLE IF NOT EXISTS report_tombstones (
    id         TEXT PRIMARY KEY NOT NULL,
    deleted_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_report_tombstones_deleted_at
    ON report_tombstones (deleted_at);

CREATE TABLE IF NOT EXISTS intake_budget (
    bucket  TEXT PRIMARY KEY NOT NULL,
    events  INTEGER NOT NULL CHECK (events >= 0),
    reports INTEGER NOT NULL CHECK (reports >= 0)
);
