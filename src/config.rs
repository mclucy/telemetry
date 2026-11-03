use serde::{Deserialize, Serialize};
use worker::{Env, Error, Result};

pub const DEFAULT_MAX_REQUEST_BYTES: usize = 256 * 1024;
pub const DEFAULT_MAX_RECORDS: usize = 50;
pub const DEFAULT_MAX_RECORD_BYTES: usize = 96 * 1024;
pub const DEFAULT_MAX_REPORT_BODY_BYTES: usize = 64 * 1024;
pub const DEFAULT_MAX_CONTEXT_BYTES: usize = 8 * 1024;
pub const DEFAULT_MAX_CONTEXTS: usize = 16;
pub const DEFAULT_EVENT_RETENTION_DAYS: i64 = 30;
pub const DEFAULT_REPORT_RETENTION_DAYS: i64 = 14;
pub const MAX_EVENT_RETENTION_DAYS: i64 = 30;
pub const MAX_REPORT_RETENTION_DAYS: i64 = 14;
pub const DEFAULT_PENDING_REPORT_GRACE_HOURS: i64 = DEFAULT_REPORT_RETENTION_DAYS * 24;
pub const MAX_PENDING_REPORT_GRACE_HOURS: i64 = MAX_REPORT_RETENTION_DAYS * 24;
pub const DEFAULT_CLEANUP_BATCH_SIZE: usize = 100;
pub const DEFAULT_CLEANUP_MAX_BATCHES: usize = 10;
pub const DEFAULT_MAX_CLOCK_SKEW_SECONDS: i64 = 7 * 24 * 60 * 60;

// Cloudflare Queues limits a message to 128 KiB.
pub const MAX_QUEUE_MESSAGE_BYTES: usize = 120 * 1024;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub environment: String,
    pub max_request_bytes: usize,
    pub max_records: usize,
    pub max_record_bytes: usize,
    pub max_report_body_bytes: usize,
    pub max_context_bytes: usize,
    pub max_contexts: usize,
    pub event_retention_days: i64,
    pub report_retention_days: i64,
    pub pending_report_grace_hours: i64,
    pub cleanup_batch_size: usize,
    pub cleanup_max_batches: usize,
    pub max_clock_skew_seconds: i64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            environment: "unknown".to_string(),
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            max_records: DEFAULT_MAX_RECORDS,
            max_record_bytes: DEFAULT_MAX_RECORD_BYTES,
            max_report_body_bytes: DEFAULT_MAX_REPORT_BODY_BYTES,
            max_context_bytes: DEFAULT_MAX_CONTEXT_BYTES,
            max_contexts: DEFAULT_MAX_CONTEXTS,
            event_retention_days: DEFAULT_EVENT_RETENTION_DAYS,
            report_retention_days: DEFAULT_REPORT_RETENTION_DAYS,
            pending_report_grace_hours: DEFAULT_PENDING_REPORT_GRACE_HOURS,
            cleanup_batch_size: DEFAULT_CLEANUP_BATCH_SIZE,
            cleanup_max_batches: DEFAULT_CLEANUP_MAX_BATCHES,
            max_clock_skew_seconds: DEFAULT_MAX_CLOCK_SKEW_SECONDS,
        }
    }
}

impl Config {
    /// Reject malformed configuration instead of widening collection limits.
    pub fn load(env: &Env) -> Result<Self> {
        let raw = match env.var("CONFIG_JSON") {
            Ok(value) => value.to_string(),
            Err(_) => return Ok(Self::default()),
        };
        if raw.trim().is_empty() {
            return Ok(Self::default());
        }

        let config: Config = serde_json::from_str(&raw)
            .map_err(|_| Error::RustError("invalid CONFIG_JSON".to_string()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        let fail = |message: &str| -> Result<()> { Err(Error::RustError(message.to_string())) };

        if self.environment.is_empty() || self.environment.len() > 64 {
            return fail("environment must be 1..=64 bytes");
        }
        if !(1024..=1024 * 1024).contains(&self.max_request_bytes) {
            return fail("max_request_bytes must be 1024..=1048576");
        }
        if !(1..=100).contains(&self.max_records) {
            return fail("max_records must be 1..=100");
        }
        if !(1024..=MAX_QUEUE_MESSAGE_BYTES).contains(&self.max_record_bytes) {
            return fail("max_record_bytes must be 1024..=122880");
        }
        if self.max_report_body_bytes == 0 || self.max_report_body_bytes > self.max_record_bytes {
            return fail("max_report_body_bytes must be 1..=max_record_bytes");
        }
        if self.max_context_bytes > 32 * 1024 {
            return fail("max_context_bytes must be <=32768");
        }
        if !(1..=32).contains(&self.max_contexts) {
            return fail("max_contexts must be 1..=32");
        }
        if !(1..=MAX_EVENT_RETENTION_DAYS).contains(&self.event_retention_days) {
            return fail("event_retention_days must be 1..=30");
        }
        if !(1..=MAX_REPORT_RETENTION_DAYS).contains(&self.report_retention_days) {
            return fail("report_retention_days must be 1..=14");
        }
        if !(1..=MAX_PENDING_REPORT_GRACE_HOURS).contains(&self.pending_report_grace_hours) {
            return fail("pending_report_grace_hours must be 1..=336");
        }
        if self.pending_report_grace_hours != self.report_retention_days * 24 {
            return fail("pending_report_grace_hours must equal report_retention_days * 24");
        }
        if !(1..=500).contains(&self.cleanup_batch_size) {
            return fail("cleanup_batch_size must be 1..=500");
        }
        if !(1..=100).contains(&self.cleanup_max_batches) {
            return fail("cleanup_max_batches must be 1..=100");
        }
        if !(0..=30 * 24 * 60 * 60).contains(&self.max_clock_skew_seconds) {
            return fail("max_clock_skew_seconds must be 0..=2592000");
        }

        Ok(())
    }
}
