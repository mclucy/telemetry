//! Queue delivery is at least once.

use chrono::{DateTime, TimeDelta, Utc};
use worker::{console_error, console_log, Env, MessageBatch, MessageExt};

use crate::config::Config;
use crate::models::{EventRecord, QueueMessage, ReportRecord};
use crate::sql;
use crate::util;

pub async fn handle(batch: MessageBatch<serde_json::Value>, env: &Env) -> worker::Result<()> {
    let config = Config::load(env).unwrap_or_else(|_| {
        console_error!("queue_config_invalid_using_defaults");
        Config::default()
    });

    let messages = match batch.messages() {
        Ok(messages) => messages,
        Err(_) => {
            console_error!("queue_batch_decode_failed");
            return Err(worker::Error::RustError(
                "queue_batch_decode_failed".to_string(),
            ));
        }
    };

    for message in messages {
        let message_id = message.id();
        let parsed = serde_json::from_value::<QueueMessage>(message.body().clone());
        let result = match parsed {
            Ok(QueueMessage::Event { v, event }) => {
                if v == QueueMessage::VERSION {
                    store_event(event, &config, env).await
                } else {
                    Err("unsupported_message_version")
                }
            }
            Ok(QueueMessage::Report { v, report }) => {
                if v == QueueMessage::VERSION {
                    store_report(report, &config, env).await
                } else {
                    Err("unsupported_message_version")
                }
            }
            Err(_) => Err("invalid_message"),
        };

        match result {
            Ok(()) => message.ack(),
            Err(reason) => {
                console_error!("queue_retry message={message_id} reason={reason}");
                message.retry();
            }
        }
    }

    Ok(())
}

fn age_exceeds(retention_days: i64, received_at: &str) -> bool {
    DateTime::parse_from_rfc3339(received_at)
        .map(|parsed| Utc::now() - parsed.with_timezone(&Utc) > TimeDelta::days(retention_days))
        .unwrap_or(false)
}

fn report_recovery_days(config: &Config) -> i64 {
    config
        .report_retention_days
        .min(sql::MAX_QUEUE_DELIVERY_DAYS)
        .max(1)
}

async fn store_event(
    mut event: EventRecord,
    config: &Config,
    env: &Env,
) -> std::result::Result<(), &'static str> {
    if age_exceeds(config.event_retention_days, &event.received_at) {
        console_log!("queue_event_expired");
        return Ok(());
    }

    let database = env.d1("DB").map_err(|_| "d1_unavailable")?;
    let received_at = std::mem::take(&mut event.received_at);
    let encoded = serde_json::to_vec(&event).map_err(|_| "event_digest_failed")?;
    event.received_at = received_at;
    let ingest_digest = util::sha256_hex(&encoded);
    let mut statements = Vec::with_capacity(1 + event.contexts.len());

    statements.push(
        database
            .prepare(sql::INSERT_EVENT)
            .bind(&[
                util::js_str(&event.id),
                util::js_str(&event.installation_id),
                util::js_str(&event.invocation_id),
                util::js_str(&event.occurred_at),
                util::js_str(&event.received_at),
                util::js_str(&event.command),
                util::js_str(&event.lucy_version),
                util::js_str(&event.lucy_commit),
                util::js_str(&event.channel),
                util::js_str(&event.os),
                util::js_str(&event.arch),
                util::js_opt_num(event.duration_ms),
                util::js_str(&event.outcome),
                util::js_opt_str(&event.error_code),
                util::js_num(event.schema_version),
                util::js_str(&event.collection_policy),
                util::js_num(event.payload_bytes),
                util::js_str(&ingest_digest),
            ])
            .map_err(|_| "d1_bind_failed")?,
    );

    for (ordinal, context) in event.contexts.iter().enumerate() {
        let data = match &context.data {
            Some(value) => Some(serde_json::to_string(value).map_err(|_| "invalid_context")?),
            None => None,
        };
        statements.push(
            database
                .prepare(sql::INSERT_CONTEXT)
                .bind(&[
                    util::js_str(&event.id),
                    util::js_num(ordinal as i64),
                    util::js_str(&context.context_type),
                    util::js_str(&context.status),
                    util::js_opt_str(&data),
                    util::js_str(&event.id),
                    util::js_str(&ingest_digest),
                ])
                .map_err(|_| "d1_bind_failed")?,
        );
    }

    let results = database
        .batch(statements)
        .await
        .map_err(|_| "d1_batch_failed")?;
    if results.iter().any(|result| !result.success()) {
        return Err("d1_batch_rejected");
    }

    let inserted = results
        .first()
        .and_then(|result| result.meta().ok().flatten())
        .and_then(|meta| meta.changes)
        .unwrap_or(0);
    if inserted == 0 {
        let existing: Option<DigestRow> = database
            .prepare(sql::SELECT_EVENT_DIGEST)
            .bind(&[util::js_str(&event.id)])
            .map_err(|_| "d1_bind_failed")?
            .first(None)
            .await
            .map_err(|_| "d1_query_failed")?;
        match existing {
            Some(row) if row.ingest_digest != ingest_digest => {
                console_log!("queue_event_conflict");
            }
            _ => {}
        }
    }

    Ok(())
}

#[derive(serde::Deserialize)]
struct DigestRow {
    ingest_digest: String,
}

#[derive(serde::Deserialize)]
struct ReportRow {
    state: String,
    body_sha256: String,
    r2_key: String,
}

#[derive(serde::Deserialize)]
struct TombstoneRow {
    #[serde(rename = "id")]
    _id: String,
}

async fn store_report(
    report: ReportRecord,
    config: &Config,
    env: &Env,
) -> std::result::Result<(), &'static str> {
    if age_exceeds(report_recovery_days(config), &report.received_at) {
        console_log!("queue_report_expired");
        return Ok(());
    }

    let bucket = env.bucket("REPORTS").map_err(|_| "r2_unavailable")?;
    let database = env.d1("DB").map_err(|_| "d1_unavailable")?;

    let object = match bucket.head(&report.r2_key).await {
        Ok(object) => object,
        Err(_) => return Err("r2_unavailable"),
    };
    let Some(object) = object else {
        return handle_missing_body(&database, &report).await;
    };

    if object.size() > config.max_report_body_bytes as u64 + 1024 {
        return Err("r2_size_mismatch");
    }
    let metadata = object
        .custom_metadata()
        .map_err(|_| "r2_metadata_unavailable")?;
    if metadata.get("sha256").map(String::as_str) != Some(report.body_sha256.as_str()) {
        return Err("r2_digest_mismatch");
    }
    if metadata.get("body_bytes").map(String::as_str)
        != Some(report.body_bytes.to_string().as_str())
    {
        return Err("r2_digest_mismatch");
    }

    let stored_at = util::now_rfc3339();
    let insert = database
        .prepare(sql::INSERT_REPORT_STORED)
        .bind(&[
            util::js_str(&report.report_id),
            util::js_str(&report.installation_id),
            util::js_opt_str(&report.invocation_id),
            util::js_str(&report.kind),
            util::js_opt_str(&report.agent),
            util::js_opt_str(&report.model),
            util::js_str(&report.created_at),
            util::js_str(&report.received_at),
            util::js_num(report.schema_version),
            util::js_str(&report.collection_policy),
            util::js_str(&report.r2_key),
            util::js_num(report.body_bytes),
            util::js_str(&report.body_sha256),
            util::js_num(report.payload_bytes),
            util::js_str(&stored_at),
            util::js_str(&report.report_id),
        ])
        .map_err(|_| "d1_bind_failed")?;

    let result = insert.run().await.map_err(|_| "d1_write_failed")?;
    if !result.success() {
        return Err("d1_write_rejected");
    }
    let changed = result
        .meta()
        .ok()
        .flatten()
        .and_then(|meta| meta.changes)
        .unwrap_or(0);
    if changed > 0 {
        return Ok(());
    }

    diagnose_report_state(&database, &report).await
}

async fn handle_missing_body(
    database: &worker::D1Database,
    report: &ReportRecord,
) -> std::result::Result<(), &'static str> {
    if tombstone_exists(database, &report.report_id).await? {
        console_log!("queue_report_expired");
        return Ok(());
    }
    match fetch_report_row(database, &report.report_id).await? {
        Some(row) if row.r2_key == report.r2_key && row.body_sha256 == report.body_sha256 => {
            match row.state.as_str() {
                "stored" => Ok(()),
                "deleting" => Ok(()),
                "pending" => Err("r2_object_missing"),
                _ => Err("d1_metadata_invalid"),
            }
        }
        Some(_) => Err("report_conflict"),
        None => Err("d1_metadata_missing"),
    }
}

async fn diagnose_report_state(
    database: &worker::D1Database,
    report: &ReportRecord,
) -> std::result::Result<(), &'static str> {
    if tombstone_exists(database, &report.report_id).await? {
        console_log!("queue_report_expired");
        return Ok(());
    }
    match fetch_report_row(database, &report.report_id).await? {
        Some(row) if row.r2_key == report.r2_key && row.body_sha256 == report.body_sha256 => {
            match row.state.as_str() {
                "stored" | "deleting" => Ok(()),
                "pending" => Err("d1_promotion_rejected"),
                _ => Err("d1_metadata_invalid"),
            }
        }
        Some(_) => Err("report_conflict"),
        None => Err("d1_metadata_missing"),
    }
}

async fn fetch_report_row(
    database: &worker::D1Database,
    report_id: &str,
) -> std::result::Result<Option<ReportRow>, &'static str> {
    database
        .prepare(sql::SELECT_REPORT_STATE)
        .bind(&[util::js_str(report_id)])
        .map_err(|_| "d1_bind_failed")?
        .first::<ReportRow>(None)
        .await
        .map_err(|_| "d1_query_failed")
}

async fn tombstone_exists(
    database: &worker::D1Database,
    report_id: &str,
) -> std::result::Result<bool, &'static str> {
    let row: Option<TombstoneRow> = database
        .prepare(sql::SELECT_REPORT_TOMBSTONE)
        .bind(&[util::js_str(report_id)])
        .map_err(|_| "d1_bind_failed")?
        .first(None)
        .await
        .map_err(|_| "d1_query_failed")?;
    Ok(row.is_some())
}
