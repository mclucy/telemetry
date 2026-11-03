//! D1 batches execute as transactions.

/// The maximum Cloudflare queue delivery window.
pub const MAX_QUEUE_DELIVERY_DAYS: i64 = 14;

pub const INSERT_EVENT: &str = "\
INSERT INTO events (
    id, installation_id, invocation_id, occurred_at, received_at, command,
    lucy_version, lucy_commit, channel, os, arch, duration_ms, outcome,
    error_code, schema_version, collection_policy, payload_bytes, ingest_digest
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO NOTHING";

/// A conflicting duplicate event cannot change its accepted contexts.
pub const INSERT_CONTEXT: &str = "\
INSERT INTO context_snapshots (event_id, ordinal, context_type, status, data)
SELECT ?, ?, ?, ?, ?
WHERE EXISTS (
    SELECT 1 FROM events WHERE id = ? AND ingest_digest = ?
)
ON CONFLICT(event_id, ordinal) DO NOTHING";

pub const SELECT_EVENT_DIGEST: &str = "\
SELECT ingest_digest FROM events WHERE id = ?";

pub const DELETE_EXPIRED_CONTEXTS: &str = "\
DELETE FROM context_snapshots
WHERE event_id IN (
    SELECT id FROM events
    WHERE received_at < ?1
    ORDER BY received_at ASC, id ASC
    LIMIT ?2
)";

pub const DELETE_EXPIRED_EVENTS: &str = "\
DELETE FROM events
WHERE id IN (
    SELECT id FROM events
    WHERE received_at < ?1
    ORDER BY received_at ASC, id ASC
    LIMIT ?2
)";

/// Counts at most `?2` expired events. A result equal to the cap means
/// `at least` in the backlog log.
pub const COUNT_EXPIRED_EVENTS: &str = "\
SELECT COUNT(*) AS expired FROM (
    SELECT 1 FROM events WHERE received_at < ?1 LIMIT ?2
)";

/// A tombstone prevents a retry from recreating a deleted report.
pub const INSERT_REPORT_PENDING: &str = "\
INSERT INTO reports (
    id, installation_id, invocation_id, kind, agent, model, created_at,
    received_at, schema_version, collection_policy, r2_key, body_bytes,
    body_sha256, payload_bytes, state
)
SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending'
WHERE NOT EXISTS (SELECT 1 FROM report_tombstones WHERE id = ?)
ON CONFLICT(id) DO NOTHING";

/// Promotion is atomic with respect to cleanup claims.
pub const INSERT_REPORT_STORED: &str = "\
INSERT INTO reports (
    id, installation_id, invocation_id, kind, agent, model, created_at,
    received_at, schema_version, collection_policy, r2_key, body_bytes,
    body_sha256, payload_bytes, state, stored_at
)
SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'stored', ?
WHERE NOT EXISTS (SELECT 1 FROM report_tombstones WHERE id = ?)
ON CONFLICT(id) DO UPDATE SET
    state = 'stored',
    stored_at = excluded.stored_at
WHERE reports.state = 'pending'
  AND reports.body_sha256 = excluded.body_sha256
  AND reports.r2_key = excluded.r2_key";

pub const SELECT_REPORT_STATE: &str = "\
SELECT state, received_at, body_sha256, r2_key
FROM reports
WHERE id = ?";

pub const SELECT_REPORT_TOMBSTONE: &str = "\
SELECT id FROM report_tombstones WHERE id = ?";

/// D1 serializes claims, so each active claim receives a distinct ID.
pub const CLAIM_EXPIRED_REPORTS: &str = "\
UPDATE reports
SET state = 'deleting',
    delete_claimed_at = ?1,
    delete_claim_id = (SELECT COALESCE(MAX(delete_claim_id), 0) + 1 FROM reports)
WHERE id IN (
    SELECT id FROM reports
    WHERE (state = 'stored' AND received_at < ?2)
       OR (state = 'pending' AND received_at < ?3)
       OR (state = 'deleting' AND delete_claimed_at < ?4)
    ORDER BY received_at ASC, id ASC
    LIMIT ?5
)
AND state IN ('stored', 'pending', 'deleting')
RETURNING r2_key, delete_claim_id";

/// Run with DELETE_CLAIMED_REPORTS in one D1 batch.
pub const TOMBSTONE_CLAIMED_REPORTS: &str = "\
INSERT INTO report_tombstones (id, deleted_at)
SELECT id, ?2 FROM reports
WHERE state = 'deleting' AND delete_claim_id = ?1
ON CONFLICT(id) DO NOTHING";

pub const DELETE_CLAIMED_REPORTS: &str = "\
DELETE FROM reports
WHERE state = 'deleting' AND delete_claim_id = ?1";

pub const DELETE_OLD_REPORT_TOMBSTONES: &str = "\
DELETE FROM report_tombstones
WHERE id IN (
    SELECT id FROM report_tombstones
    WHERE deleted_at < ?1
    ORDER BY deleted_at ASC, id ASC
    LIMIT ?2
)";

/// Counts expired rows per state for the backlog log. The cap marks
/// `at least`.
pub const COUNT_EXPIRED_REPORTS: &str = "\
SELECT
    (SELECT COUNT(*) FROM (
        SELECT 1 FROM reports
        WHERE state = 'stored' AND received_at < ?1
        LIMIT ?4
    )) AS expired_stored,
    (SELECT COUNT(*) FROM (
        SELECT 1 FROM reports
        WHERE state = 'pending' AND received_at < ?2
        LIMIT ?4
    )) AS expired_pending,
    (SELECT COUNT(*) FROM (
        SELECT 1 FROM reports
        WHERE state = 'deleting' AND delete_claimed_at < ?3
        LIMIT ?4
    )) AS stale_claims";

/// Reserves slots in one atomic UPSERT. Zero changes means the cap was hit.
pub const RESERVE_INTAKE: &str = "\
INSERT INTO intake_budget (bucket, events, reports)
SELECT ?1, ?2, ?3 WHERE ?2 <= ?4 AND ?3 <= ?5
ON CONFLICT(bucket) DO UPDATE SET
    events = intake_budget.events + excluded.events,
    reports = intake_budget.reports + excluded.reports
WHERE intake_budget.events + excluded.events <= ?4
  AND intake_budget.reports + excluded.reports <= ?5";

pub const DELETE_OLD_INTAKE_BUCKETS: &str = "\
DELETE FROM intake_budget
WHERE bucket IN (
    SELECT bucket FROM intake_budget
    WHERE bucket < ?1
    ORDER BY bucket ASC
    LIMIT ?2
)";
