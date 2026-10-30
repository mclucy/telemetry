//! Admission reserves capacity before R2 or queue writes; failed requests do not refund it.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, TimeDelta, Utc};
use serde::Serialize;
use worker::{
    console_error, wasm_bindgen::JsValue, Env, QueueContentType, RawMessageBuilder, Request,
    Response, SendMessage,
};

use crate::config::Config;
use crate::models::{validate_event, validate_report, QueueMessage, ReportRecord, ValidatedReport};
use crate::sql;
use crate::util;

const MAX_QUEUE_BATCH_BYTES: usize = 200 * 1024;

#[derive(Serialize)]
struct RejectedRecord {
    id: String,
    reason: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    field: Option<&'static str>,
}

#[derive(Serialize)]
struct IngestResponse {
    accepted: Vec<String>,
    rejected: Vec<RejectedRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retryable: Option<bool>,
}

struct Prepared {
    id: String,
    message: QueueMessage,
}

struct CandidateReport {
    response_id: String,
    report: ValidatedReport,
    record: ReportRecord,
}

enum Candidate {
    Event {
        response_id: String,
        message: QueueMessage,
    },
    Report(CandidateReport),
}

impl Candidate {
    fn response_id(&self) -> &str {
        match self {
            Candidate::Event { response_id, .. } => response_id,
            Candidate::Report(report) => &report.response_id,
        }
    }

    fn is_event(&self) -> bool {
        matches!(self, Candidate::Event { .. })
    }
}

enum StoreError {
    Conflict,
    Storage,
}

enum StoreOutcome {
    Queued,
    AlreadyStored,
}

enum ReserveError {
    Limited,
    Storage,
}

#[derive(serde::Deserialize)]
struct ExistingReport {
    state: String,
    received_at: String,
    body_sha256: String,
    r2_key: String,
}

#[derive(serde::Deserialize)]
struct TombstoneRow {
    #[serde(rename = "id")]
    _id: String,
}

pub async fn handle(request: &mut Request, env: &Env) -> worker::Result<Response> {
    let config = match Config::load(env) {
        Ok(config) => config,
        Err(_) => {
            console_error!("ingest_config_invalid");
            return util::error_response(500, "not_configured");
        }
    };

    let content_type = request
        .headers()
        .get("content-type")
        .ok()
        .flatten()
        .unwrap_or_default();
    let media_type = content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    if media_type != "application/json" {
        return util::error_response(415, "unsupported_media_type");
    }

    let body = match util::read_body_limited(request, config.max_request_bytes).await {
        Ok(body) => body,
        Err(error) => return util::body_error_response(error, config.max_request_bytes),
    };
    let request: crate::models::IngestRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return util::error_response(400, "invalid_json"),
    };

    if request.records.is_empty() {
        return util::error_response(400, "empty_batch");
    }
    if request.records.len() > config.max_records {
        return util::error_response(400, "too_many_records");
    }

    let received_at = util::now_rfc3339();
    let mut seen = HashSet::with_capacity(request.records.len());
    let mut candidates = Vec::new();
    let mut rejected = Vec::new();
    let mut transient_failure = false;

    for raw in request.records {
        let response_id = display_id(&raw.id);
        let duplicate_key = raw.id.trim().to_ascii_lowercase();
        if !seen.insert(duplicate_key) {
            rejected.push(RejectedRecord {
                id: response_id,
                reason: "duplicate_id",
                field: Some("id"),
            });
            continue;
        }

        let record_size = match serde_json::to_vec(&raw) {
            Ok(encoded) => encoded.len(),
            Err(_) => {
                rejected.push(RejectedRecord {
                    id: response_id,
                    reason: "invalid_payload",
                    field: Some("payload"),
                });
                continue;
            }
        };
        if record_size > config.max_record_bytes {
            rejected.push(RejectedRecord {
                id: response_id,
                reason: "record_too_large",
                field: None,
            });
            continue;
        }

        let payload_bytes = serde_json::to_vec(&raw.payload)
            .map(|encoded| encoded.len() as i64)
            .unwrap_or(0);

        match raw.record_type.as_str() {
            "event" => {
                match validate_event(&raw.id, &raw.payload, &config, &received_at, payload_bytes) {
                    Ok(event) => {
                        let message = QueueMessage::Event {
                            v: QueueMessage::VERSION,
                            event,
                        };
                        if !queue_message_fits(&message, &config) {
                            rejected.push(RejectedRecord {
                                id: response_id,
                                reason: "record_too_large",
                                field: None,
                            });
                            continue;
                        }
                        candidates.push(Candidate::Event {
                            response_id,
                            message,
                        });
                    }
                    Err(error) => rejected.push(RejectedRecord {
                        id: response_id,
                        reason: error.reason,
                        field: error.field,
                    }),
                }
            }
            "report" => {
                let report = match validate_report(&raw.id, &raw.payload, &config) {
                    Ok(report) => report,
                    Err(error) => {
                        rejected.push(RejectedRecord {
                            id: response_id,
                            reason: error.reason,
                            field: error.field,
                        });
                        continue;
                    }
                };
                let record = prepare_report_record(&report, &received_at, payload_bytes);
                let message = QueueMessage::Report {
                    v: QueueMessage::VERSION,
                    report: record.clone(),
                };
                if !queue_message_fits(&message, &config) {
                    rejected.push(RejectedRecord {
                        id: response_id,
                        reason: "record_too_large",
                        field: None,
                    });
                    continue;
                }
                candidates.push(Candidate::Report(CandidateReport {
                    response_id,
                    report,
                    record,
                }));
            }
            _ => rejected.push(RejectedRecord {
                id: response_id,
                reason: "unsupported_type",
                field: Some("type"),
            }),
        }
    }

    let event_count = candidates
        .iter()
        .filter(|candidate| candidate.is_event())
        .count() as i64;
    let report_count = candidates.len() as i64 - event_count;

    let mut prepared = Vec::new();
    let mut already_stored = Vec::new();

    let admitted = if candidates.is_empty() {
        true
    } else {
        match reserve_intake(env, &config, event_count, report_count).await {
            Ok(()) => true,
            Err(ReserveError::Limited) => {
                console_error!("ingest_intake_limited events={event_count} reports={report_count}");
                for candidate in &candidates {
                    rejected.push(RejectedRecord {
                        id: candidate.response_id().to_string(),
                        reason: "intake_limited",
                        field: None,
                    });
                }
                transient_failure = true;
                false
            }
            Err(ReserveError::Storage) => {
                console_error!("ingest_intake_unavailable");
                for candidate in &candidates {
                    rejected.push(RejectedRecord {
                        id: candidate.response_id().to_string(),
                        reason: "storage_unavailable",
                        field: None,
                    });
                }
                transient_failure = true;
                false
            }
        }
    };

    if admitted {
        for candidate in candidates {
            match candidate {
                Candidate::Event { message, .. } => {
                    let id = message_id(&message);
                    prepared.push(Prepared { id, message });
                }
                Candidate::Report(candidate) => {
                    match store_report(env, &config, &candidate.report, &candidate.record).await {
                        Ok(StoreOutcome::Queued) => {
                            let id = candidate.record.report_id.clone();
                            prepared.push(Prepared {
                                id,
                                message: QueueMessage::Report {
                                    v: QueueMessage::VERSION,
                                    report: candidate.record,
                                },
                            });
                        }
                        Ok(StoreOutcome::AlreadyStored) => {
                            already_stored.push(candidate.record.report_id)
                        }
                        Err(StoreError::Conflict) => rejected.push(RejectedRecord {
                            id: candidate.response_id,
                            reason: "report_conflict",
                            field: Some("payload.body"),
                        }),
                        Err(StoreError::Storage) => {
                            transient_failure = true;
                            rejected.push(RejectedRecord {
                                id: candidate.response_id,
                                reason: "storage_unavailable",
                                field: None,
                            });
                        }
                    }
                }
            }
        }
    }

    let mut accepted = already_stored;
    if !prepared.is_empty() {
        match send_queue(env, &prepared).await {
            Ok(()) => accepted.extend(prepared.iter().map(|item| item.id.clone())),
            Err(()) => {
                console_error!("ingest_queue_send_failed count={}", prepared.len());
                transient_failure = true;
                for item in &prepared {
                    rejected.push(RejectedRecord {
                        id: item.id.clone(),
                        reason: "queue_unavailable",
                        field: None,
                    });
                }
            }
        }
    }

    if transient_failure {
        accepted.clear();
    }
    let status = if transient_failure {
        503
    } else if !accepted.is_empty() || rejected.iter().any(|item| item.reason == "report_conflict") {
        200
    } else {
        400
    };

    util::json_response(
        status,
        &IngestResponse {
            accepted,
            rejected,
            retryable: (status == 503).then_some(true),
        },
    )
}

fn display_id(raw: &str) -> String {
    raw.chars().take(128).collect()
}

fn message_id(message: &QueueMessage) -> String {
    match message {
        QueueMessage::Event { event, .. } => event.id.clone(),
        QueueMessage::Report { report, .. } => report.report_id.clone(),
    }
}

fn queue_message_fits(message: &QueueMessage, config: &Config) -> bool {
    match serde_json::to_vec(message) {
        Ok(encoded) => {
            encoded.len() <= config.max_record_bytes
                && encoded.len() <= crate::config::MAX_QUEUE_MESSAGE_BYTES
        }
        Err(_) => false,
    }
}

fn prepare_report_record(
    report: &ValidatedReport,
    received_at: &str,
    payload_bytes: i64,
) -> ReportRecord {
    let body_bytes = report.body.as_bytes().len() as i64;
    let body_sha256 = util::sha256_hex(report.body.as_bytes());
    let r2_key = format!("reports/{}/{}.txt.gz", report.report_id, body_sha256);
    ReportRecord {
        report_id: report.report_id.clone(),
        installation_id: report.installation_id.clone(),
        invocation_id: report.invocation_id.clone(),
        kind: report.kind.clone(),
        agent: report.agent.clone(),
        model: report.model.clone(),
        created_at: report.created_at.clone(),
        received_at: received_at.to_string(),
        schema_version: report.schema_version,
        collection_policy: report.collection_policy.clone(),
        r2_key,
        body_bytes,
        body_sha256,
        payload_bytes,
    }
}

/// Half the hourly cleanup capacity leaves room to drain a delayed run.
fn intake_capacity(config: &Config) -> (i64, i64) {
    let capacity =
        (config.cleanup_batch_size as i64).saturating_mul(config.cleanup_max_batches as i64);
    let per_type = (capacity / 2).max(1);
    (per_type, per_type)
}

fn hour_bucket() -> String {
    Utc::now().format("%Y-%m-%dT%H:00:00Z").to_string()
}

async fn reserve_intake(
    env: &Env,
    config: &Config,
    events: i64,
    reports: i64,
) -> std::result::Result<(), ReserveError> {
    if events == 0 && reports == 0 {
        return Ok(());
    }
    let (event_cap, report_cap) = intake_capacity(config);
    if events > event_cap || reports > report_cap {
        return Err(ReserveError::Limited);
    }

    let database = env.d1("DB").map_err(|_| ReserveError::Storage)?;
    let result = database
        .prepare(sql::RESERVE_INTAKE)
        .bind(&[
            util::js_str(&hour_bucket()),
            util::js_num(events),
            util::js_num(reports),
            util::js_num(event_cap),
            util::js_num(report_cap),
        ])
        .map_err(|_| ReserveError::Storage)?
        .run()
        .await
        .map_err(|_| ReserveError::Storage)?;
    if !result.success() {
        return Err(ReserveError::Storage);
    }
    let changes = result
        .meta()
        .ok()
        .flatten()
        .and_then(|meta| meta.changes)
        .unwrap_or(0);
    if changes == 0 {
        Err(ReserveError::Limited)
    } else {
        Ok(())
    }
}

fn report_age_exceeds(retention_days: i64, received_at: &str) -> bool {
    DateTime::parse_from_rfc3339(received_at)
        .map(|parsed| Utc::now() - parsed.with_timezone(&Utc) > TimeDelta::days(retention_days))
        .unwrap_or(false)
}

async fn store_report(
    env: &Env,
    config: &Config,
    report: &ValidatedReport,
    record: &ReportRecord,
) -> std::result::Result<StoreOutcome, StoreError> {
    let database = env.d1("DB").map_err(|_| StoreError::Storage)?;

    let existing = fetch_existing(&database, &record.report_id).await?;
    if let Some(existing) = existing {
        return match existing.state.as_str() {
            "stored" => {
                if existing.body_sha256 == record.body_sha256 && existing.r2_key == record.r2_key {
                    Ok(StoreOutcome::AlreadyStored)
                } else {
                    Err(StoreError::Conflict)
                }
            }
            "pending" => {
                if existing.body_sha256 != record.body_sha256
                    || existing.r2_key != record.r2_key
                    || report_age_exceeds(config.report_retention_days, &existing.received_at)
                {
                    Err(StoreError::Conflict)
                } else {
                    write_pending_report(env, config, &database, report, record).await
                }
            }
            "deleting" => Err(StoreError::Conflict),
            _ => Err(StoreError::Storage),
        };
    }
    if fetch_tombstone(&database, &record.report_id)
        .await?
        .is_some()
    {
        return Err(StoreError::Conflict);
    }

    write_pending_report(env, config, &database, report, record).await
}

async fn write_pending_report(
    env: &Env,
    config: &Config,
    database: &worker::D1Database,
    report: &ValidatedReport,
    record: &ReportRecord,
) -> std::result::Result<StoreOutcome, StoreError> {
    let bucket = env.bucket("REPORTS").map_err(|_| StoreError::Storage)?;

    let mut custom_metadata = HashMap::new();
    custom_metadata.insert("sha256".to_string(), record.body_sha256.clone());
    custom_metadata.insert("report_id".to_string(), record.report_id.clone());
    custom_metadata.insert("kind".to_string(), record.kind.clone());
    custom_metadata.insert("body_bytes".to_string(), record.body_bytes.to_string());

    let http_metadata = worker::HttpMetadata {
        content_type: Some("text/plain; charset=utf-8".to_string()),
        content_language: None,
        content_disposition: Some("attachment".to_string()),
        content_encoding: Some("gzip".to_string()),
        cache_control: Some("no-store".to_string()),
        cache_expiry: None,
    };

    let body_bytes =
        crate::encoding::compress(report.body.as_bytes()).map_err(|_| StoreError::Storage)?;
    let put = bucket
        .put(record.r2_key.clone(), body_bytes)
        .http_metadata(http_metadata)
        .custom_metadata(custom_metadata)
        .execute()
        .await
        .map_err(|_| StoreError::Storage)?;
    if put.is_none() {
        return Err(StoreError::Storage);
    }

    let statement = database
        .prepare(sql::INSERT_REPORT_PENDING)
        .bind(&[
            util::js_str(&record.report_id),
            util::js_str(&record.installation_id),
            util::js_opt_str(&record.invocation_id),
            util::js_str(&record.kind),
            util::js_opt_str(&record.agent),
            util::js_opt_str(&record.model),
            util::js_str(&record.created_at),
            util::js_str(&record.received_at),
            util::js_num(record.schema_version),
            util::js_str(&record.collection_policy),
            util::js_str(&record.r2_key),
            util::js_num(record.body_bytes),
            util::js_str(&record.body_sha256),
            util::js_num(record.payload_bytes),
            util::js_str(&record.report_id),
        ])
        .map_err(|_| StoreError::Storage)?;

    let result = statement.run().await.map_err(|_| StoreError::Storage)?;
    if !result.success() {
        return Err(StoreError::Storage);
    }
    let changes = result
        .meta()
        .ok()
        .flatten()
        .and_then(|meta| meta.changes)
        .unwrap_or(0);

    if changes > 0 {
        return Ok(StoreOutcome::Queued);
    }

    // The guarded insert did not apply. Re-read; never invent success for a
    // row that does not exist.
    let tombstoned = fetch_tombstone(database, &record.report_id)
        .await?
        .is_some();
    if tombstoned {
        delete_uploaded_body(env, &record.r2_key).await;
        return Err(StoreError::Conflict);
    }
    match fetch_existing(database, &record.report_id).await? {
        Some(existing) => match existing.state.as_str() {
            "stored"
                if existing.body_sha256 == record.body_sha256
                    && existing.r2_key == record.r2_key =>
            {
                Ok(StoreOutcome::AlreadyStored)
            }
            "pending"
                if existing.body_sha256 == record.body_sha256
                    && existing.r2_key == record.r2_key
                    && !report_age_exceeds(config.report_retention_days, &existing.received_at) =>
            {
                Ok(StoreOutcome::Queued)
            }
            "deleting" => {
                delete_uploaded_body(env, &record.r2_key).await;
                Err(StoreError::Conflict)
            }
            _ => {
                delete_uploaded_body(env, &record.r2_key).await;
                Err(StoreError::Conflict)
            }
        },
        None => Err(StoreError::Storage),
    }
}

async fn fetch_existing(
    database: &worker::D1Database,
    report_id: &str,
) -> std::result::Result<Option<ExistingReport>, StoreError> {
    database
        .prepare(sql::SELECT_REPORT_STATE)
        .bind(&[util::js_str(report_id)])
        .map_err(|_| StoreError::Storage)?
        .first::<ExistingReport>(None)
        .await
        .map_err(|_| StoreError::Storage)
}

async fn fetch_tombstone(
    database: &worker::D1Database,
    report_id: &str,
) -> std::result::Result<Option<TombstoneRow>, StoreError> {
    database
        .prepare(sql::SELECT_REPORT_TOMBSTONE)
        .bind(&[util::js_str(report_id)])
        .map_err(|_| StoreError::Storage)?
        .first::<TombstoneRow>(None)
        .await
        .map_err(|_| StoreError::Storage)
}

/// The R2 lifecycle removes bodies if this best-effort deletion fails.
async fn delete_uploaded_body(env: &Env, r2_key: &str) {
    if let Ok(bucket) = env.bucket("REPORTS") {
        let _ = bucket.delete(r2_key).await;
    }
}

async fn send_queue(env: &Env, prepared: &[Prepared]) -> std::result::Result<(), ()> {
    let queue = env.queue("RECORDS").map_err(|_| ())?;
    let mut chunk: Vec<SendMessage<JsValue>> = Vec::new();
    let mut chunk_bytes = 0usize;

    for item in prepared {
        let encoded = serde_json::to_string(&item.message).map_err(|_| ())?;
        let message_bytes = encoded.len().saturating_add(64);

        if !chunk.is_empty()
            && (chunk.len() >= 100
                || chunk_bytes.saturating_add(message_bytes) > MAX_QUEUE_BATCH_BYTES)
        {
            queue
                .send_raw_batch(std::mem::take(&mut chunk))
                .await
                .map_err(|_| ())?;
            chunk_bytes = 0;
        }

        // Queue content is JSON. serde_wasm_bindgen maps nested JSON objects to
        // JS Maps, which JSON serialization writes as {}, so the body is parsed
        // from its JSON text instead of serializing the Rust message directly.
        let body = worker::js_sys::JSON::parse(&encoded).map_err(|_| ())?;
        chunk.push(RawMessageBuilder::new(body).build_with_content_type(QueueContentType::Json));
        chunk_bytes = chunk_bytes.saturating_add(message_bytes);
    }

    if !chunk.is_empty() {
        queue.send_raw_batch(chunk).await.map_err(|_| ())?;
    }
    Ok(())
}
