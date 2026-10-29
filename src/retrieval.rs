//! The admin hostname must also be protected by Cloudflare Access.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use worker::{console_error, Env, Method, Request, Response};

use crate::config::Config;
use crate::models::canonical_uuid;
use crate::sql;
use crate::util;

const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 200;
const MAX_CURSOR_BYTES: usize = 96;

const SELECT_EVENT_FOR_RETRIEVAL: &str = "\
SELECT id, installation_id, invocation_id, occurred_at, received_at, command,
       lucy_version, lucy_commit, channel, os, arch, duration_ms, outcome,
       error_code, schema_version, collection_policy, payload_bytes
FROM events
WHERE id = ?1 AND received_at >= ?2";

const SELECT_REPORT_FOR_RETRIEVAL: &str = "\
SELECT id, installation_id, invocation_id, kind, agent, model, created_at,
       received_at, schema_version, collection_policy, r2_key, body_bytes,
       body_sha256, payload_bytes, state, stored_at
FROM reports
WHERE id = ?1 AND state != 'deleting' AND received_at >= ?2";

const SELECT_REPORT_BODY_FOR_RETRIEVAL: &str = "\
SELECT r2_key FROM reports
WHERE id = ?1 AND state != 'deleting' AND received_at >= ?2";

#[derive(Debug, Serialize, Deserialize)]
struct EventRow {
    id: String,
    installation_id: String,
    invocation_id: String,
    occurred_at: String,
    received_at: String,
    command: String,
    lucy_version: String,
    lucy_commit: String,
    channel: String,
    os: String,
    arch: String,
    duration_ms: Option<i64>,
    outcome: String,
    error_code: Option<String>,
    schema_version: i64,
    collection_policy: String,
    payload_bytes: i64,
}

#[derive(Debug, Deserialize)]
struct ContextRow {
    ordinal: i64,
    context_type: String,
    status: String,
    data: Option<String>,
}

#[derive(Debug, Serialize)]
struct ContextView {
    ordinal: i64,
    #[serde(rename = "type")]
    context_type: String,
    status: String,
    data: Option<Value>,
}

#[derive(Debug, Serialize)]
struct EventList {
    events: Vec<EventRow>,
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
struct EventDetail {
    event: EventRow,
    contexts: Vec<ContextView>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ReportRow {
    id: String,
    installation_id: String,
    invocation_id: Option<String>,
    kind: String,
    agent: Option<String>,
    model: Option<String>,
    created_at: String,
    received_at: String,
    schema_version: i64,
    collection_policy: String,
    r2_key: String,
    body_bytes: i64,
    body_sha256: String,
    payload_bytes: i64,
    state: String,
    stored_at: Option<String>,
}

#[derive(Debug, Serialize)]
struct ReportList {
    reports: Vec<ReportRow>,
    next_cursor: Option<String>,
}

// The ID breaks ties between records received in the same millisecond.
#[derive(Debug)]
struct Cursor {
    received_at: String,
    id: String,
}

impl Cursor {
    fn encode(&self) -> String {
        format!("{}|{}", self.received_at, self.id)
    }
}

#[derive(Debug, Deserialize)]
struct BodyRow {
    r2_key: String,
}

#[derive(Debug, Deserialize)]
struct StatsRow {
    event_count: Option<f64>,
    event_payload_bytes: Option<f64>,
    context_count: Option<f64>,
    report_count: Option<f64>,
    report_payload_bytes: Option<f64>,
    report_body_bytes: Option<f64>,
    pending_report_count: Option<f64>,
    first_event_at: Option<String>,
    last_event_at: Option<String>,
    first_report_at: Option<String>,
    last_report_at: Option<String>,
}

#[derive(Debug, Serialize)]
struct QueueStatus {
    status: &'static str,
    note: &'static str,
}

#[derive(Debug, Serialize)]
struct StatsView {
    event_count: u64,
    context_count: u64,
    report_count: u64,
    pending_report_count: u64,
    accepted_event_payload_bytes: u64,
    accepted_report_payload_bytes: u64,
    accepted_total_logical_bytes: u64,
    report_body_bytes: u64,
    period_start: Option<String>,
    period_end: Option<String>,
    queue: QueueStatus,
}

pub async fn handle(
    request: &Request,
    env: &Env,
    method: Method,
    path: &str,
) -> worker::Result<Response> {
    let config = match Config::load(env) {
        Ok(config) => config,
        Err(_) => return util::error_response(500, "not_configured"),
    };
    let Some(admin_host) = &config.admin_host else {
        return util::error_response(404, "not_found");
    };
    {
        let host = request
            .headers()
            .get("host")
            .ok()
            .flatten()
            .unwrap_or_default();
        let host = host
            .split(':')
            .next()
            .unwrap_or("")
            .trim_end_matches('.')
            .to_ascii_lowercase();
        if !host.eq_ignore_ascii_case(admin_host) {
            return util::error_response(404, "not_found");
        }
    }
    if !util::is_authorized(request, env) {
        return util::unauthorized();
    }
    if method != Method::Get {
        return util::method_not_allowed("GET");
    }

    if path == "/v1/events" {
        return list_events(request, env, &config).await;
    }
    if path == "/v1/reports" {
        return list_reports(request, env, &config).await;
    }
    if path == "/v1/stats" {
        return stats(env, &config).await;
    }

    if let Some(rest) = path.strip_prefix("/v1/events/") {
        if rest.is_empty() || rest.contains('/') {
            return util::error_response(404, "not_found");
        }
        return event_detail(env, &config, rest).await;
    }

    if let Some(rest) = path.strip_prefix("/v1/reports/") {
        if let Some(id) = rest.strip_suffix("/body") {
            if id.is_empty() || id.contains('/') {
                return util::error_response(404, "not_found");
            }
            return report_body(env, &config, id).await;
        }
        if rest.is_empty() || rest.contains('/') {
            return util::error_response(404, "not_found");
        }
        return report_detail(env, &config, rest).await;
    }

    util::error_response(404, "not_found")
}

fn parse_limit(params: &HashMap<String, String>) -> std::result::Result<usize, &'static str> {
    match params.get("limit") {
        None => Ok(DEFAULT_LIMIT),
        Some(raw) => {
            let value = raw.parse::<usize>().map_err(|_| "invalid_limit")?;
            if value == 0 || value > MAX_LIMIT {
                return Err("invalid_limit");
            }
            Ok(value)
        }
    }
}

fn parse_cursor(
    params: &HashMap<String, String>,
) -> std::result::Result<Option<Cursor>, &'static str> {
    match params.get("cursor") {
        None => Ok(None),
        Some(raw) => parse_cursor_value(raw).map(Some),
    }
}

fn parse_cursor_value(raw: &str) -> std::result::Result<Cursor, &'static str> {
    if raw.is_empty() || raw.len() > MAX_CURSOR_BYTES {
        return Err("invalid_cursor");
    }
    let (received_at, id) = raw.split_once('|').ok_or("invalid_cursor")?;
    let received_at = util::canonical_filter_time(received_at).ok_or("invalid_cursor")?;
    let id = canonical_uuid(id, "cursor").map_err(|_| "invalid_cursor")?;
    Ok(Cursor { received_at, id })
}

async fn list_events(request: &Request, env: &Env, config: &Config) -> worker::Result<Response> {
    let params = util::query_map(request)?;
    let limit = match parse_limit(&params) {
        Ok(limit) => limit,
        Err(code) => return util::error_response(400, code),
    };
    let cursor = match parse_cursor(&params) {
        Ok(cursor) => cursor,
        Err(code) => return util::error_response(400, code),
    };
    let installation_id = match params.get("installation_id") {
        None => None,
        Some(raw) => match canonical_uuid(raw, "installation_id") {
            Ok(id) => Some(id),
            Err(_) => return util::error_response(400, "invalid_installation_id"),
        },
    };
    let cursor_time = cursor.as_ref().map(|cursor| cursor.received_at.clone());
    let cursor_id = cursor.as_ref().map(|cursor| cursor.id.clone());
    let cutoff = util::cutoff_rfc3339(config.event_retention_days);

    let database = env.d1("DB")?;
    let statement = database
        .prepare(
            "SELECT id, installation_id, invocation_id, occurred_at, received_at, command,
                    lucy_version, lucy_commit, channel, os, arch, duration_ms, outcome,
                    error_code, schema_version, collection_policy, payload_bytes
             FROM events
             WHERE (?1 IS NULL OR installation_id = ?1)
               AND received_at >= ?2
               AND (?3 IS NULL
                    OR received_at < ?3
                    OR (received_at = ?3 AND id < ?4))
             ORDER BY received_at DESC, id DESC
             LIMIT ?5",
        )
        .bind(&[
            util::js_opt_str(&installation_id),
            util::js_str(&cutoff),
            util::js_opt_str(&cursor_time),
            util::js_opt_str(&cursor_id),
            util::js_num((limit + 1) as i64),
        ])?;
    let result = statement.all().await?;
    let mut events: Vec<EventRow> = result.results()?;
    let next_cursor = if events.len() > limit {
        events.truncate(limit);
        events.last().map(|event| {
            Cursor {
                received_at: event.received_at.clone(),
                id: event.id.clone(),
            }
            .encode()
        })
    } else {
        None
    };

    util::json_response(
        200,
        &EventList {
            events,
            next_cursor,
        },
    )
}

async fn event_detail(env: &Env, config: &Config, raw_id: &str) -> worker::Result<Response> {
    let id = match canonical_uuid(raw_id, "id") {
        Ok(id) => id,
        Err(_) => return util::error_response(400, "invalid_id"),
    };
    let cutoff = util::cutoff_rfc3339(config.event_retention_days);
    let database = env.d1("DB")?;
    let event = database
        .prepare(SELECT_EVENT_FOR_RETRIEVAL)
        .bind(&[util::js_str(&id), util::js_str(&cutoff)])?
        .first::<EventRow>(None)
        .await?;
    let Some(event) = event else {
        return util::error_response(404, "not_found");
    };

    let context_rows: Vec<ContextRow> = database
        .prepare(sql::SELECT_CONTEXTS)
        .bind(&[util::js_str(&id)])?
        .all()
        .await?
        .results()?;

    let contexts = context_rows
        .into_iter()
        .map(|row| ContextView {
            ordinal: row.ordinal,
            context_type: row.context_type,
            status: row.status,
            data: row
                .data
                .and_then(|text| serde_json::from_str::<Value>(&text).ok()),
        })
        .collect();

    util::json_response(200, &EventDetail { event, contexts })
}

async fn list_reports(request: &Request, env: &Env, config: &Config) -> worker::Result<Response> {
    let params = util::query_map(request)?;
    let limit = match parse_limit(&params) {
        Ok(limit) => limit,
        Err(code) => return util::error_response(400, code),
    };
    let cursor = match parse_cursor(&params) {
        Ok(cursor) => cursor,
        Err(code) => return util::error_response(400, code),
    };
    let kind = match params.get("kind") {
        None => None,
        Some(value) if matches!(value.as_str(), "failure" | "panic" | "agent_feedback") => {
            Some(value.clone())
        }
        Some(_) => return util::error_response(400, "invalid_kind"),
    };
    let cursor_time = cursor.as_ref().map(|cursor| cursor.received_at.clone());
    let cursor_id = cursor.as_ref().map(|cursor| cursor.id.clone());
    let cutoff = util::cutoff_rfc3339(config.report_retention_days);

    let database = env.d1("DB")?;
    let statement = database
        .prepare(
            "SELECT id, installation_id, invocation_id, kind, agent, model, created_at,
                    received_at, schema_version, collection_policy, r2_key, body_bytes,
                    body_sha256, payload_bytes, state, stored_at
             FROM reports
             WHERE (?1 IS NULL OR kind = ?1)
               AND state != 'deleting'
               AND received_at >= ?2
               AND (?3 IS NULL
                    OR received_at < ?3
                    OR (received_at = ?3 AND id < ?4))
             ORDER BY received_at DESC, id DESC
             LIMIT ?5",
        )
        .bind(&[
            util::js_opt_str(&kind),
            util::js_str(&cutoff),
            util::js_opt_str(&cursor_time),
            util::js_opt_str(&cursor_id),
            util::js_num((limit + 1) as i64),
        ])?;
    let result = statement.all().await?;
    let mut reports: Vec<ReportRow> = result.results()?;
    let next_cursor = if reports.len() > limit {
        reports.truncate(limit);
        reports.last().map(|report| {
            Cursor {
                received_at: report.received_at.clone(),
                id: report.id.clone(),
            }
            .encode()
        })
    } else {
        None
    };

    util::json_response(
        200,
        &ReportList {
            reports,
            next_cursor,
        },
    )
}

async fn report_detail(env: &Env, config: &Config, raw_id: &str) -> worker::Result<Response> {
    let id = match canonical_uuid(raw_id, "id") {
        Ok(id) => id,
        Err(_) => return util::error_response(400, "invalid_id"),
    };
    let cutoff = util::cutoff_rfc3339(config.report_retention_days);
    let database = env.d1("DB")?;
    let report = database
        .prepare(SELECT_REPORT_FOR_RETRIEVAL)
        .bind(&[util::js_str(&id), util::js_str(&cutoff)])?
        .first::<ReportRow>(None)
        .await?;
    match report {
        Some(report) => util::json_response(200, &report),
        None => util::error_response(404, "not_found"),
    }
}

async fn report_body(env: &Env, config: &Config, raw_id: &str) -> worker::Result<Response> {
    let id = match canonical_uuid(raw_id, "id") {
        Ok(id) => id,
        Err(_) => return util::error_response(400, "invalid_id"),
    };
    let cutoff = util::cutoff_rfc3339(config.report_retention_days);
    let database = env.d1("DB")?;
    let row = database
        .prepare(SELECT_REPORT_BODY_FOR_RETRIEVAL)
        .bind(&[util::js_str(&id), util::js_str(&cutoff)])?
        .first::<BodyRow>(None)
        .await?;
    let Some(row) = row else {
        return util::error_response(404, "not_found");
    };

    let bucket = env.bucket("REPORTS")?;
    let object = match bucket.get(row.r2_key).execute().await {
        Ok(object) => object,
        Err(_) => {
            console_error!("retrieval_r2_get_failed");
            return util::error_response(502, "storage_unavailable");
        }
    };
    let Some(object) = object else {
        return util::error_response(410, "body_expired");
    };
    let Some(body) = object.body() else {
        return util::error_response(502, "storage_unavailable");
    };
    let text = match body.bytes().await.and_then(|bytes| {
        crate::encoding::decompress(&bytes, config.max_report_body_bytes)
            .map_err(|_| worker::Error::RustError("invalid report body".into()))
    }) {
        Ok(text) => text,
        Err(_) => {
            console_error!("retrieval_r2_body_failed");
            return util::error_response(502, "storage_unavailable");
        }
    };

    util::text_response(text, "text/plain; charset=utf-8")
}

async fn stats(env: &Env, config: &Config) -> worker::Result<Response> {
    let event_cutoff = util::cutoff_rfc3339(config.event_retention_days);
    let report_cutoff = util::cutoff_rfc3339(config.report_retention_days);
    let database = env.d1("DB")?;
    let row = database
        .prepare(
            "SELECT
                (SELECT COUNT(*) FROM events WHERE received_at >= ?1) AS event_count,
                (SELECT COALESCE(SUM(payload_bytes), 0) FROM events WHERE received_at >= ?1) AS event_payload_bytes,
                (SELECT COUNT(*) FROM context_snapshots AS contexts
                   JOIN events AS retained ON retained.id = contexts.event_id
                  WHERE retained.received_at >= ?1) AS context_count,
                (SELECT COUNT(*) FROM reports WHERE state = 'stored' AND received_at >= ?2) AS report_count,
                (SELECT COALESCE(SUM(payload_bytes), 0) FROM reports WHERE state = 'stored' AND received_at >= ?2) AS report_payload_bytes,
                (SELECT COALESCE(SUM(body_bytes), 0) FROM reports WHERE state = 'stored' AND received_at >= ?2) AS report_body_bytes,
                (SELECT COUNT(*) FROM reports WHERE state = 'pending' AND received_at >= ?2) AS pending_report_count,
                (SELECT MIN(received_at) FROM events WHERE received_at >= ?1) AS first_event_at,
                (SELECT MAX(received_at) FROM events WHERE received_at >= ?1) AS last_event_at,
                (SELECT MIN(received_at) FROM reports WHERE state != 'deleting' AND received_at >= ?2) AS first_report_at,
                (SELECT MAX(received_at) FROM reports WHERE state != 'deleting' AND received_at >= ?2) AS last_report_at",
        )
        .bind(&[util::js_str(&event_cutoff), util::js_str(&report_cutoff)])?
        .first::<StatsRow>(None)
        .await?
        .unwrap_or(StatsRow {
            event_count: None,
            event_payload_bytes: None,
            context_count: None,
            report_count: None,
            report_payload_bytes: None,
            report_body_bytes: None,
            pending_report_count: None,
            first_event_at: None,
            last_event_at: None,
            first_report_at: None,
            last_report_at: None,
        });

    let event_count = count(row.event_count);
    let report_count = count(row.report_count);
    let event_payload_bytes = count(row.event_payload_bytes);
    let report_payload_bytes = count(row.report_payload_bytes);

    let period_start = minimum_time(
        row.first_event_at.as_deref(),
        row.first_report_at.as_deref(),
    );
    let period_end = maximum_time(row.last_event_at.as_deref(), row.last_report_at.as_deref());

    util::json_response(
        200,
        &StatsView {
            event_count,
            context_count: count(row.context_count),
            report_count,
            pending_report_count: count(row.pending_report_count),
            accepted_event_payload_bytes: event_payload_bytes,
            accepted_report_payload_bytes: report_payload_bytes,
            accepted_total_logical_bytes: event_payload_bytes.saturating_add(report_payload_bytes),
            report_body_bytes: count(row.report_body_bytes),
            period_start,
            period_end,
            queue: QueueStatus {
                status: "not_observed_by_worker",
                note: "Queue depth is not exposed to the Worker. Use `cf queues metrics get <queue-id>` or the Cloudflare dashboard.",
            },
        },
    )
}

fn count(value: Option<f64>) -> u64 {
    value.unwrap_or(0.0).max(0.0) as u64
}

fn minimum_time<'a>(left: Option<&'a str>, right: Option<&'a str>) -> Option<String> {
    match (left, right) {
        (Some(a), Some(b)) => Some(a.min(b).to_string()),
        (Some(a), None) => Some(a.to_string()),
        (None, Some(b)) => Some(b.to_string()),
        (None, None) => None,
    }
}

fn maximum_time<'a>(left: Option<&'a str>, right: Option<&'a str>) -> Option<String> {
    match (left, right) {
        (Some(a), Some(b)) => Some(a.max(b).to_string()),
        (Some(a), None) => Some(a.to_string()),
        (None, Some(b)) => Some(b.to_string()),
        (None, None) => None,
    }
}
