use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::config::Config;

const MAX_DURATION_MS: i64 = 7 * 24 * 60 * 60 * 1000;
const MAX_SCHEMA_VERSION: i64 = 1000;

#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    pub records: Vec<RawRecord>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RawRecord {
    pub id: String,
    #[serde(rename = "type")]
    pub record_type: String,
    pub payload: Value,
}

#[derive(Debug, Deserialize)]
pub struct EventPayload {
    pub invocation_id: String,
    pub installation_id: String,
    pub occurred_at: String,
    pub command: String,
    pub lucy_version: String,
    pub lucy_commit: String,
    pub channel: String,
    pub os: String,
    pub arch: String,
    #[serde(default)]
    pub duration_ms: Option<i64>,
    pub outcome: String,
    #[serde(default)]
    pub error_code: Option<String>,
    pub schema_version: i64,
    pub collection_policy: String,
    #[serde(default)]
    pub contexts: Vec<ContextInput>,
}

#[derive(Debug, Deserialize)]
pub struct ContextInput {
    #[serde(rename = "type")]
    pub context_type: String,
    pub status: String,
    #[serde(default)]
    pub data: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct ReportPayload {
    pub report_id: String,
    pub installation_id: String,
    #[serde(default)]
    pub invocation_id: Option<String>,
    pub kind: String,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    pub created_at: String,
    pub schema_version: i64,
    pub collection_policy: String,
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextRecord {
    #[serde(rename = "type")]
    pub context_type: String,
    pub status: String,
    pub data: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRecord {
    pub id: String,
    pub installation_id: String,
    pub invocation_id: String,
    pub occurred_at: String,
    pub received_at: String,
    pub command: String,
    pub lucy_version: String,
    pub lucy_commit: String,
    pub channel: String,
    pub os: String,
    pub arch: String,
    pub duration_ms: Option<i64>,
    pub outcome: String,
    pub error_code: Option<String>,
    pub schema_version: i64,
    pub collection_policy: String,
    pub payload_bytes: i64,
    pub contexts: Vec<ContextRecord>,
}

#[derive(Debug, Clone)]
pub struct ValidatedReport {
    pub report_id: String,
    pub installation_id: String,
    pub invocation_id: Option<String>,
    pub kind: String,
    pub agent: Option<String>,
    pub model: Option<String>,
    pub created_at: String,
    pub schema_version: i64,
    pub collection_policy: String,
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportRecord {
    pub report_id: String,
    pub installation_id: String,
    pub invocation_id: Option<String>,
    pub kind: String,
    pub agent: Option<String>,
    pub model: Option<String>,
    pub created_at: String,
    pub received_at: String,
    pub schema_version: i64,
    pub collection_policy: String,
    pub r2_key: String,
    pub body_bytes: i64,
    pub body_sha256: String,
    pub payload_bytes: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QueueMessage {
    Event { v: u8, event: EventRecord },
    Report { v: u8, report: ReportRecord },
}

impl QueueMessage {
    pub const VERSION: u8 = 1;
}

#[derive(Debug, Clone, Copy)]
pub struct ValidationError {
    pub reason: &'static str,
    pub field: Option<&'static str>,
}

impl ValidationError {
    fn new(reason: &'static str, field: &'static str) -> Self {
        Self {
            reason,
            field: Some(field),
        }
    }
}

fn required_string(
    value: &str,
    field: &'static str,
    max: usize,
) -> Result<String, ValidationError> {
    if value.is_empty() || value.len() > max {
        return Err(ValidationError::new("invalid_field", field));
    }
    Ok(value.to_string())
}

fn bounded_string(value: &str, field: &'static str, max: usize) -> Result<String, ValidationError> {
    if value.len() > max {
        return Err(ValidationError::new("invalid_field", field));
    }
    Ok(value.to_string())
}

fn optional_string(
    value: &Option<String>,
    field: &'static str,
    max: usize,
) -> Result<Option<String>, ValidationError> {
    match value {
        None => Ok(None),
        Some(text) if text.len() <= max => Ok(Some(text.clone())),
        Some(_) => Err(ValidationError::new("invalid_field", field)),
    }
}

pub fn canonical_uuid(value: &str, field: &'static str) -> Result<String, ValidationError> {
    let parsed =
        Uuid::parse_str(value.trim()).map_err(|_| ValidationError::new("invalid_id", field))?;
    if parsed.is_nil() {
        return Err(ValidationError::new("invalid_id", field));
    }
    Ok(parsed.to_string())
}

fn validate_time(
    value: &str,
    field: &'static str,
    max_clock_skew_seconds: i64,
) -> Result<String, ValidationError> {
    let parsed = DateTime::parse_from_rfc3339(value.trim())
        .map_err(|_| ValidationError::new("invalid_time", field))?;
    let utc = parsed.with_timezone(&Utc);
    if utc > Utc::now() + TimeDelta::seconds(max_clock_skew_seconds) {
        return Err(ValidationError::new("time_in_future", field));
    }
    Ok(utc.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn validate_schema_version(value: i64) -> Result<i64, ValidationError> {
    if !(1..=MAX_SCHEMA_VERSION).contains(&value) {
        return Err(ValidationError::new(
            "unsupported_schema_version",
            "schema_version",
        ));
    }
    Ok(value)
}

fn validate_context(input: ContextInput, cfg: &Config) -> Result<ContextRecord, ValidationError> {
    let context_type = required_string(&input.context_type, "contexts.type", 64)?;
    if context_type != "workspace" && context_type != "java" && context_type != "agent" {
        return Err(ValidationError::new("invalid_context", "contexts.type"));
    }
    let data = match input.status.as_str() {
        "present" => {
            let value = input
                .data
                .filter(|value| !value.is_null())
                .ok_or_else(|| ValidationError::new("invalid_context", "contexts.data"))?;
            Some(value)
        }
        "absent" => {
            if input.data.as_ref().is_some_and(|value| !value.is_null()) {
                return Err(ValidationError::new("invalid_context", "contexts.data"));
            }
            None
        }
        "failed" if input.data.as_ref().is_none_or(Value::is_null) => None,
        _ => return Err(ValidationError::new("invalid_context", "contexts.status")),
    };

    if let Some(value) = &data {
        let encoded = serde_json::to_vec(value)
            .map_err(|_| ValidationError::new("invalid_context", "contexts.data"))?;
        if encoded.len() > cfg.max_context_bytes {
            return Err(ValidationError::new("context_too_large", "contexts.data"));
        }
        validate_context_data(&context_type, value)?;
    }

    Ok(ContextRecord {
        context_type,
        status: input.status,
        data,
    })
}

fn validate_context_data(kind: &str, value: &Value) -> Result<(), ValidationError> {
    let fields = value
        .as_object()
        .ok_or_else(|| ValidationError::new("invalid_context", "contexts.data"))?;
    for (name, value) in fields {
        let valid = match (kind, name.as_str()) {
            ("workspace", "product") | ("java", "distribution") => {
                value.as_str().is_some_and(|s| identifier(s, 64, false))
            }
            ("workspace", "product_version" | "minecraft_version") | ("java", "version") => {
                value.as_str().is_some_and(|s| {
                    s.len() <= 64
                        && s.bytes().any(|b| b.is_ascii_digit())
                        && s.bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b".+-_".contains(&b))
                })
            }
            ("workspace", "packages") => value.as_u64().is_some_and(|n| n <= 1_000_000),
            ("java", "major") => value.as_u64().is_some_and(|n| n > 0 && n <= 255),
            ("agent", "detected") => value.is_boolean(),
            _ => false,
        };
        if !valid {
            return Err(ValidationError::new("invalid_context", "contexts.data"));
        }
    }
    Ok(())
}

fn identifier(value: &str, max: usize, spaces: bool) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-+:".contains(&b) || (spaces && b == b' '))
}

pub fn validate_event(
    raw_id: &str,
    payload: &Value,
    cfg: &Config,
    received_at: &str,
    payload_bytes: i64,
) -> Result<EventRecord, ValidationError> {
    let payload: EventPayload = serde_json::from_value(payload.clone())
        .map_err(|_| ValidationError::new("invalid_payload", "payload"))?;

    let id = canonical_uuid(raw_id, "id")?;
    let installation_id = canonical_uuid(&payload.installation_id, "payload.installation_id")?;
    let invocation_id = canonical_uuid(&payload.invocation_id, "payload.invocation_id")?;
    let occurred_at = validate_time(
        &payload.occurred_at,
        "payload.occurred_at",
        cfg.max_clock_skew_seconds,
    )?;

    if payload.contexts.len() > cfg.max_contexts {
        return Err(ValidationError::new(
            "too_many_contexts",
            "payload.contexts",
        ));
    }
    let mut contexts = Vec::with_capacity(payload.contexts.len());
    for context in payload.contexts {
        contexts.push(validate_context(context, cfg)?);
    }

    if let Some(duration) = payload.duration_ms {
        if !(0..=MAX_DURATION_MS).contains(&duration) {
            return Err(ValidationError::new("invalid_field", "payload.duration_ms"));
        }
    }
    if !identifier(&payload.command, 128, true)
        || !identifier(&payload.channel, 32, false)
        || !identifier(&payload.os, 64, false)
        || !identifier(&payload.arch, 32, false)
        || (!payload.lucy_version.is_empty() && !identifier(&payload.lucy_version, 64, false))
        || (!payload.lucy_commit.is_empty() && !identifier(&payload.lucy_commit, 64, false))
        || payload
            .error_code
            .as_deref()
            .is_some_and(|v| !v.is_empty() && !identifier(v, 64, false))
        || !matches!(payload.outcome.as_str(), "success" | "failure" | "canceled")
        || !matches!(payload.collection_policy.as_str(), "default" | "opt_in")
    {
        return Err(ValidationError::new("invalid_field", "payload"));
    }

    Ok(EventRecord {
        id,
        installation_id,
        invocation_id,
        occurred_at,
        received_at: received_at.to_string(),
        command: required_string(&payload.command, "payload.command", 128)?,
        lucy_version: bounded_string(&payload.lucy_version, "payload.lucy_version", 64)?,
        lucy_commit: bounded_string(&payload.lucy_commit, "payload.lucy_commit", 64)?,
        channel: required_string(&payload.channel, "payload.channel", 32)?,
        os: required_string(&payload.os, "payload.os", 64)?,
        arch: required_string(&payload.arch, "payload.arch", 32)?,
        duration_ms: payload.duration_ms,
        outcome: required_string(&payload.outcome, "payload.outcome", 32)?,
        error_code: optional_string(&payload.error_code, "payload.error_code", 64)?,
        schema_version: validate_schema_version(payload.schema_version)?,
        collection_policy: required_string(
            &payload.collection_policy,
            "payload.collection_policy",
            64,
        )?,
        payload_bytes,
        contexts,
    })
}

pub fn validate_report(
    raw_id: &str,
    payload: &Value,
    cfg: &Config,
) -> Result<ValidatedReport, ValidationError> {
    let payload: ReportPayload = serde_json::from_value(payload.clone())
        .map_err(|_| ValidationError::new("invalid_payload", "payload"))?;

    let report_id = canonical_uuid(raw_id, "id")?;
    let payload_report_id = canonical_uuid(&payload.report_id, "payload.report_id")?;
    if report_id != payload_report_id {
        return Err(ValidationError::new("id_mismatch", "payload.report_id"));
    }

    let installation_id = canonical_uuid(&payload.installation_id, "payload.installation_id")?;
    let invocation_id = match &payload.invocation_id {
        Some(value) => Some(canonical_uuid(value, "payload.invocation_id")?),
        None => None,
    };
    let kind = match payload.kind.as_str() {
        "failure" | "panic" | "agent_feedback" => payload.kind.clone(),
        _ => return Err(ValidationError::new("invalid_field", "payload.kind")),
    };
    let created_at = validate_time(
        &payload.created_at,
        "payload.created_at",
        cfg.max_clock_skew_seconds,
    )?;

    let body_bytes = payload.body.as_bytes().len();
    if body_bytes == 0 || body_bytes > cfg.max_report_body_bytes {
        return Err(ValidationError::new("invalid_body_size", "payload.body"));
    }
    if payload.body.contains('\0') {
        return Err(ValidationError::new("invalid_body", "payload.body"));
    }
    if !matches!(payload.collection_policy.as_str(), "default" | "opt_in") {
        return Err(ValidationError::new(
            "invalid_field",
            "payload.collection_policy",
        ));
    }
    for value in [&payload.agent, &payload.model].into_iter().flatten() {
        if value.len() > 128
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-/:@".contains(&b))
        {
            return Err(ValidationError::new(
                "invalid_field",
                "payload.agent_or_model",
            ));
        }
    }
    let mut body = crate::privacy::filter_report(&payload.body);
    if body.len() > cfg.max_report_body_bytes {
        let mut end = cfg.max_report_body_bytes;
        while !body.is_char_boundary(end) {
            end -= 1;
        }
        body.truncate(end);
    }

    Ok(ValidatedReport {
        report_id,
        installation_id,
        invocation_id,
        kind,
        agent: optional_string(&payload.agent, "payload.agent", 128)?,
        model: optional_string(&payload.model, "payload.model", 128)?,
        created_at,
        schema_version: validate_schema_version(payload.schema_version)?,
        collection_policy: required_string(
            &payload.collection_policy,
            "payload.collection_policy",
            64,
        )?,
        body,
    })
}
