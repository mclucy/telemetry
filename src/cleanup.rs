//! Report claims coordinate deletion across D1 and R2.

use chrono::{SecondsFormat, TimeDelta, Utc};
use worker::{console_error, console_log, Bucket, D1Database, Env};

use crate::config::Config;
use crate::sql;
use crate::util;

/// A claim older than this may be reclaimed. One invocation cannot run this
/// long.
const DELETE_CLAIM_LEASE_MINUTES: i64 = 30;

/// Outlives the maximum queue delivery window after a report is deleted.
const REPORT_TOMBSTONE_RETENTION_DAYS: i64 = 15;

/// Only the current and previous hour are used. Two days covers delayed
/// retries.
const INTAKE_LEDGER_RETENTION_DAYS: i64 = 2;

#[derive(serde::Deserialize)]
struct ClaimedReport {
    r2_key: String,
    delete_claim_id: i64,
}

#[derive(serde::Deserialize)]
struct CountRow {
    expired: Option<f64>,
}

#[derive(serde::Deserialize)]
struct ReportBacklogRow {
    expired_stored: Option<f64>,
    expired_pending: Option<f64>,
    stale_claims: Option<f64>,
}

pub async fn handle(env: &Env) -> worker::Result<()> {
    let config = Config::load(env).unwrap_or_else(|_| {
        console_error!("cleanup_config_invalid_using_defaults");
        Config::default()
    });

    let database = match env.d1("DB") {
        Ok(database) => database,
        Err(_) => {
            console_error!("cleanup_d1_binding_missing");
            return Ok(());
        }
    };
    let bucket = match env.bucket("REPORTS") {
        Ok(bucket) => bucket,
        Err(_) => {
            console_error!("cleanup_r2_binding_missing");
            return Ok(());
        }
    };

    match cleanup_events(&database, &config).await {
        Ok(deleted) if deleted > 0 => console_log!("cleanup_events deleted={deleted}"),
        Ok(_) => {}
        Err(reason) => console_error!("cleanup_events_failed reason={reason}"),
    }
    if let Err(reason) = log_event_backlog(&database, &config).await {
        console_error!("cleanup_backlog_failed kind=events reason={reason}");
    }

    match cleanup_reports(&database, &bucket, &config).await {
        Ok(deleted) if deleted > 0 => console_log!("cleanup_reports deleted={deleted}"),
        Ok(_) => {}
        Err(reason) => console_error!("cleanup_reports_failed reason={reason}"),
    }
    if let Err(reason) = log_report_backlog(&database, &config).await {
        console_error!("cleanup_backlog_failed kind=reports reason={reason}");
    }

    match cleanup_tombstones(&database, &config).await {
        Ok(deleted) if deleted > 0 => console_log!("cleanup_tombstones deleted={deleted}"),
        Ok(_) => {}
        Err(reason) => console_error!("cleanup_tombstones_failed reason={reason}"),
    }

    match cleanup_intake_ledger(&database, &config).await {
        Ok(deleted) if deleted > 0 => console_log!("cleanup_intake_ledger deleted={deleted}"),
        Ok(_) => {}
        Err(reason) => console_error!("cleanup_intake_ledger_failed reason={reason}"),
    }

    Ok(())
}

fn cutoff_minutes(minutes: i64) -> String {
    (Utc::now() - TimeDelta::minutes(minutes)).to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// A pending row shares its body lifetime. Cap the grace by body retention and
/// by the maximum delivery window; after that window, no message can verify
/// the row.
fn pending_report_cutoff_hours(config: &Config) -> i64 {
    let body_retention_hours = config.report_retention_days.saturating_mul(24);
    let delivery_hours = sql::MAX_QUEUE_DELIVERY_DAYS.saturating_mul(24);
    config
        .pending_report_grace_hours
        .min(body_retention_hours)
        .min(delivery_hours)
        .max(1)
}

fn cleanup_capacity(config: &Config) -> usize {
    config
        .cleanup_batch_size
        .saturating_mul(config.cleanup_max_batches)
}

async fn cleanup_events(
    database: &D1Database,
    config: &Config,
) -> std::result::Result<usize, &'static str> {
    let cutoff = util::cutoff_rfc3339(config.event_retention_days);
    let mut total = 0usize;

    for _ in 0..config.cleanup_max_batches {
        let contexts = database
            .prepare(sql::DELETE_EXPIRED_CONTEXTS)
            .bind(&[
                util::js_str(&cutoff),
                util::js_num(config.cleanup_batch_size as i64),
            ])
            .map_err(|_| "d1_bind_failed")?;
        let events = database
            .prepare(sql::DELETE_EXPIRED_EVENTS)
            .bind(&[
                util::js_str(&cutoff),
                util::js_num(config.cleanup_batch_size as i64),
            ])
            .map_err(|_| "d1_bind_failed")?;

        let results = database
            .batch(vec![contexts, events])
            .await
            .map_err(|_| "d1_batch_failed")?;
        if results.iter().any(|result| !result.success()) {
            return Err("d1_batch_rejected");
        }

        let deleted = results
            .get(1)
            .and_then(|result| result.meta().ok().flatten())
            .and_then(|meta| meta.changes)
            .unwrap_or(0);
        total = total.saturating_add(deleted);
        if deleted < config.cleanup_batch_size {
            break;
        }
    }

    Ok(total)
}

async fn log_event_backlog(
    database: &D1Database,
    config: &Config,
) -> std::result::Result<(), &'static str> {
    let cutoff = util::cutoff_rfc3339(config.event_retention_days);
    let cap = cleanup_capacity(config).saturating_add(1);
    let row: Option<CountRow> = database
        .prepare(sql::COUNT_EXPIRED_EVENTS)
        .bind(&[util::js_str(&cutoff), util::js_num(cap as i64)])
        .map_err(|_| "d1_bind_failed")?
        .first(None)
        .await
        .map_err(|_| "d1_query_failed")?;
    let expired = row.and_then(|row| row.expired).unwrap_or(0.0).max(0.0) as usize;
    if expired > 0 {
        let at_least = expired >= cap;
        console_log!(
            "cleanup_backlog_events expired={expired} at_least={at_least} capacity_per_run={}",
            cap.saturating_sub(1)
        );
    }
    Ok(())
}

async fn log_report_backlog(
    database: &D1Database,
    config: &Config,
) -> std::result::Result<(), &'static str> {
    let stored_cutoff = util::cutoff_rfc3339(config.report_retention_days);
    let pending_cutoff = util::cutoff_rfc3339_hours(pending_report_cutoff_hours(config));
    let stale_cutoff = cutoff_minutes(DELETE_CLAIM_LEASE_MINUTES);
    let cap = cleanup_capacity(config).saturating_add(1);
    let row: Option<ReportBacklogRow> = database
        .prepare(sql::COUNT_EXPIRED_REPORTS)
        .bind(&[
            util::js_str(&stored_cutoff),
            util::js_str(&pending_cutoff),
            util::js_str(&stale_cutoff),
            util::js_num(cap as i64),
        ])
        .map_err(|_| "d1_bind_failed")?
        .first(None)
        .await
        .map_err(|_| "d1_query_failed")?;
    let Some(row) = row else {
        return Ok(());
    };
    let stored = row.expired_stored.unwrap_or(0.0).max(0.0) as usize;
    let pending = row.expired_pending.unwrap_or(0.0).max(0.0) as usize;
    let stale = row.stale_claims.unwrap_or(0.0).max(0.0) as usize;
    if stored + pending + stale > 0 {
        let at_least = stored >= cap || pending >= cap || stale >= cap;
        console_log!(
            "cleanup_backlog_reports stored_expired={stored} pending_expired={pending} stale_claims={stale} at_least={at_least} capacity_per_run={}",
            cap.saturating_sub(1)
        );
    }
    Ok(())
}

async fn cleanup_reports(
    database: &D1Database,
    bucket: &Bucket,
    config: &Config,
) -> std::result::Result<usize, &'static str> {
    let stored_cutoff = util::cutoff_rfc3339(config.report_retention_days);
    let pending_cutoff = util::cutoff_rfc3339_hours(pending_report_cutoff_hours(config));
    let stale_cutoff = cutoff_minutes(DELETE_CLAIM_LEASE_MINUTES);
    let mut total = 0usize;

    for _ in 0..config.cleanup_max_batches {
        let claim_at = util::now_rfc3339();
        let claim_result = database
            .prepare(sql::CLAIM_EXPIRED_REPORTS)
            .bind(&[
                util::js_str(&claim_at),
                util::js_str(&stored_cutoff),
                util::js_str(&pending_cutoff),
                util::js_str(&stale_cutoff),
                util::js_num(config.cleanup_batch_size as i64),
            ])
            .map_err(|_| "d1_bind_failed")?
            .all()
            .await
            .map_err(|_| "d1_claim_failed")?;
        if !claim_result.success() {
            return Err("d1_claim_rejected");
        }
        let rows: Vec<ClaimedReport> = claim_result
            .results()
            .map_err(|_| "d1_claim_decode_failed")?;
        if rows.is_empty() {
            break;
        }
        let claimed = rows.len();

        let keys: Vec<&str> = rows
            .iter()
            .map(|row| row.r2_key.as_str())
            .filter(|key| !key.is_empty())
            .collect();
        if !keys.is_empty() {
            bucket
                .delete_multiple(keys)
                .await
                .map_err(|_| "r2_delete_failed")?;
        }

        let deleted_at = util::now_rfc3339();
        let mut claim_ids: Vec<i64> = rows.iter().map(|row| row.delete_claim_id).collect();
        claim_ids.sort_unstable();
        claim_ids.dedup();

        for claim_id in claim_ids {
            let tombstone = database
                .prepare(sql::TOMBSTONE_CLAIMED_REPORTS)
                .bind(&[util::js_num(claim_id), util::js_str(&deleted_at)])
                .map_err(|_| "d1_bind_failed")?;
            let delete = database
                .prepare(sql::DELETE_CLAIMED_REPORTS)
                .bind(&[util::js_num(claim_id)])
                .map_err(|_| "d1_bind_failed")?;

            let results = database
                .batch(vec![tombstone, delete])
                .await
                .map_err(|_| "d1_batch_failed")?;
            if results.iter().any(|result| !result.success()) {
                return Err("d1_batch_rejected");
            }
            let deleted = results
                .get(1)
                .and_then(|result| result.meta().ok().flatten())
                .and_then(|meta| meta.changes)
                .unwrap_or(0);
            total = total.saturating_add(deleted);
        }

        if claimed < config.cleanup_batch_size {
            break;
        }
    }

    Ok(total)
}

async fn cleanup_tombstones(
    database: &D1Database,
    config: &Config,
) -> std::result::Result<usize, &'static str> {
    let cutoff = util::cutoff_rfc3339(REPORT_TOMBSTONE_RETENTION_DAYS);
    let mut total = 0usize;

    for _ in 0..config.cleanup_max_batches {
        let result = database
            .prepare(sql::DELETE_OLD_REPORT_TOMBSTONES)
            .bind(&[
                util::js_str(&cutoff),
                util::js_num(config.cleanup_batch_size as i64),
            ])
            .map_err(|_| "d1_bind_failed")?
            .run()
            .await
            .map_err(|_| "d1_delete_failed")?;
        if !result.success() {
            return Err("d1_delete_rejected");
        }
        let deleted = result
            .meta()
            .ok()
            .flatten()
            .and_then(|meta| meta.changes)
            .unwrap_or(0);
        total = total.saturating_add(deleted);
        if deleted < config.cleanup_batch_size {
            break;
        }
    }

    Ok(total)
}

async fn cleanup_intake_ledger(
    database: &D1Database,
    config: &Config,
) -> std::result::Result<usize, &'static str> {
    let cutoff = util::cutoff_rfc3339(INTAKE_LEDGER_RETENTION_DAYS);
    let result = database
        .prepare(sql::DELETE_OLD_INTAKE_BUCKETS)
        .bind(&[
            util::js_str(&cutoff),
            util::js_num(config.cleanup_batch_size as i64),
        ])
        .map_err(|_| "d1_bind_failed")?
        .run()
        .await
        .map_err(|_| "d1_delete_failed")?;
    if !result.success() {
        return Err("d1_delete_rejected");
    }
    Ok(result
        .meta()
        .ok()
        .flatten()
        .and_then(|meta| meta.changes)
        .unwrap_or(0))
}
