use std::collections::HashSet;

use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::config::Config;

const MAX_DURATION_MS: i64 = 7 * 24 * 60 * 60 * 1000;
const MAX_DURATION_MS_U64: u64 = MAX_DURATION_MS as u64;
const MAX_SCHEMA_VERSION: i64 = 1000;
/// Bound for counted work (packages, files), independent of elapsed time.
const MAX_ITEMS: u64 = 1_000_000;
const MAX_ARGUMENTS: usize = 32;
const MAX_ARGUMENT_NAME: usize = 64;
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
const MAX_PHASES: usize = 24;

/// Selectors and lifecycle stages whose spelling is meaningful. A selector may
/// carry one `@`-qualified version (`paper@232`); no other free text is allowed.
const SELECTORS: [&str; 28] = [
    "vanilla",
    "paper",
    "fabric",
    "folia",
    "purpur",
    "forge",
    "neoforge",
    "quilt",
    "spigot",
    "bukkit",
    "velocity",
    "waterfall",
    "modrinth",
    "curseforge",
    "hangar",
    "github",
    "release",
    "beta",
    "alpha",
    "stable",
    "auto",
    "latest",
    "none",
    "always",
    "never",
    "off",
    "on",
    "snapshot",
];

const ABSENT_REASONS: [&str; 4] = ["not_observed", "not_reached", "not_applicable", "no_result"];

const FAILED_REASON: &str = "observation_failed";

#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    pub records: Vec<RawRecord>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawRecord {
    pub id: String,
    #[serde(rename = "type")]
    pub record_type: String,
    pub payload: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventPayload {
    pub invocation_id: String,
    pub installation_id: String,
    pub occurred_at: String,
    /// Trusted command path; low-cardinality operation name for aggregates.
    pub operation: String,
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
    #[serde(default)]
    pub contexts: Vec<ContextInput>,
}

#[derive(Debug, Deserialize)]
pub struct ContextInput {
    #[serde(rename = "type")]
    pub context_type: String,
    pub status: String,
    #[serde(default)]
    pub version: Option<i64>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub data: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextRecord {
    #[serde(rename = "type")]
    pub context_type: String,
    pub status: String,
    /// Nullable so records queued before this schema stay byte-identical on
    /// re-serialization, which keeps their event digest stable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub data: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRecord {
    pub id: String,
    pub installation_id: String,
    pub invocation_id: String,
    pub occurred_at: String,
    pub received_at: String,
    pub operation: String,
    pub lucy_version: String,
    pub lucy_commit: String,
    pub channel: String,
    pub os: String,
    pub arch: String,
    pub duration_ms: Option<i64>,
    pub outcome: String,
    pub error_code: Option<String>,
    pub schema_version: i64,
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

fn context_data_error() -> ValidationError {
    ValidationError::new("invalid_context", "contexts.data")
}

/// A payload may carry only the retained observation contexts. Retired
/// contexts are rejected rather than silently ignored.
fn is_payload_context_type(kind: &str) -> bool {
    matches!(
        kind,
        "workspace" | "java" | "agent" | "invocation" | "execution"
    )
}

fn validate_context_version(version: Option<i64>) -> Result<Option<i64>, ValidationError> {
    match version {
        None => Ok(None),
        Some(1) => Ok(Some(1)),
        Some(_) => Err(ValidationError::new("invalid_context", "contexts.version")),
    }
}

fn validate_context_reason(
    status: &str,
    reason: Option<&str>,
) -> Result<Option<String>, ValidationError> {
    let Some(reason) = reason else {
        return Ok(None);
    };
    let valid = match status {
        "absent" => ABSENT_REASONS.contains(&reason),
        "failed" => reason == FAILED_REASON,
        _ => false,
    };
    if valid {
        Ok(Some(reason.to_string()))
    } else {
        Err(ValidationError::new("invalid_context", "contexts.reason"))
    }
}

fn validate_context(input: ContextInput, cfg: &Config) -> Result<ContextRecord, ValidationError> {
    let context_type = required_string(&input.context_type, "contexts.type", 64)?;
    if !is_payload_context_type(&context_type) {
        return Err(ValidationError::new("invalid_context", "contexts.type"));
    }
    let version = validate_context_version(input.version)?;
    let reason = validate_context_reason(&input.status, input.reason.as_deref())?;
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
        let encoded = serde_json::to_vec(value).map_err(|_| context_data_error())?;
        if encoded.len() > cfg.max_context_bytes {
            return Err(ValidationError::new("context_too_large", "contexts.data"));
        }
        validate_context_data(&context_type, value)?;
    }

    Ok(ContextRecord {
        context_type,
        status: input.status,
        version,
        reason,
        data,
    })
}

fn validate_context_data(kind: &str, value: &Value) -> Result<(), ValidationError> {
    match kind {
        "workspace" | "java" | "agent" => validate_observation_context_data(kind, value),
        "invocation" => validate_invocation_data(value),
        "execution" => validate_execution_data(value),
        _ => Err(context_data_error()),
    }
}

/// Observation contexts are closed objects: unknown fields, null fields, and
/// arbitrary strings are rejected rather than stored.
fn strict_context_object(value: &Value) -> Result<(), ValidationError> {
    let object = value.as_object().ok_or_else(context_data_error)?;
    if object.values().any(Value::is_null) {
        return Err(context_data_error());
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InvocationData {
    intent: String,
    #[serde(default)]
    arguments: Option<Vec<InvocationArgument>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InvocationArgument {
    name: String,
    kind: String,
    #[serde(default)]
    value: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionData {
    #[serde(default)]
    phases: Option<Vec<ExecutionPhase>>,
    #[serde(default)]
    active_phase: Option<String>,
    #[serde(default)]
    cancel_reason: Option<String>,
    #[serde(default)]
    failure_kind: Option<String>,
    #[serde(default)]
    input_wait_ms: Option<u64>,
    #[serde(default)]
    files: Option<u64>,
    #[serde(default)]
    http_ms: Option<u64>,
    #[serde(default)]
    lock_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionPhase {
    name: String,
    elapsed_ms: u64,
    state: String,
}

fn validate_invocation_data(value: &Value) -> Result<(), ValidationError> {
    strict_context_object(value)?;
    let data = InvocationData::deserialize(value).map_err(|_| context_data_error())?;
    if !matches!(data.intent.as_str(), "command" | "invalid_usage") {
        return Err(context_data_error());
    }
    let Some(arguments) = &data.arguments else {
        return Ok(());
    };
    if arguments.len() > MAX_ARGUMENTS {
        return Err(context_data_error());
    }
    let mut names = HashSet::with_capacity(arguments.len());
    for argument in arguments {
        if !valid_argument_name(&argument.name)
            || !names.insert(argument.name.as_str())
            || !safe_argument_value(&argument.kind, argument.value.as_ref())
        {
            return Err(context_data_error());
        }
    }
    Ok(())
}

/// Names are Cobra flag names or one-based `arg-N` positional slots. Both are
/// bounded lowercase identifiers, so a raw command line cannot be smuggled in.
fn valid_argument_name(value: &str) -> bool {
    if value.is_empty() || value.len() > MAX_ARGUMENT_NAME {
        return false;
    }
    if let Some(slot) = value.strip_prefix("arg-") {
        return !slot.is_empty()
            && !slot.starts_with('0')
            && slot.bytes().all(|byte| byte.is_ascii_digit());
    }
    let mut bytes = value.bytes();
    let first = bytes.next().unwrap();
    (first.is_ascii_lowercase() || first.is_ascii_digit())
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

/// Only parsed, typed values cross the wire. Text and list values are omitted;
/// their names still describe the command shape.
fn safe_argument_value(kind: &str, value: Option<&Value>) -> bool {
    match kind {
        "boolean" => value.is_some_and(Value::is_boolean),
        "number" => value.is_some_and(safe_number),
        "selector" => value.and_then(Value::as_str).is_some_and(safe_selector),
        "version" => value.and_then(Value::as_str).is_some_and(safe_version),
        "text" | "list" => value.is_none(),
        _ => false,
    }
}

fn safe_number(value: &Value) -> bool {
    if let Some(number) = value.as_i64() {
        return number.unsigned_abs() <= MAX_SAFE_INTEGER;
    }
    if let Some(number) = value.as_u64() {
        return number <= MAX_SAFE_INTEGER;
    }
    value
        .as_f64()
        .is_some_and(|number| number.is_finite() && number.abs() <= MAX_SAFE_INTEGER as f64)
}

fn safe_selector(value: &str) -> bool {
    match value.split_once('@') {
        Some((selector, version)) => SELECTORS.contains(&selector) && safe_version(version),
        None => SELECTORS.contains(&value),
    }
}

/// Version segments are 1-4 digits, at most four segments; Minecraft snapshot
/// versions use the `YYwWWx` form.
fn safe_version(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() == 6
        && bytes[0].is_ascii_digit()
        && bytes[1].is_ascii_digit()
        && bytes[2] == b'w'
        && bytes[3].is_ascii_digit()
        && bytes[4].is_ascii_digit()
        && bytes[5].is_ascii_lowercase()
    {
        return true;
    }
    let mut segments = 0;
    for segment in value.split('.') {
        segments += 1;
        if segments > 4
            || segment.is_empty()
            || segment.len() > 4
            || !segment.bytes().all(|byte| byte.is_ascii_digit())
        {
            return false;
        }
    }
    segments > 0
}

fn valid_phase_name(value: &str) -> bool {
    if value.is_empty() || value.len() > 64 {
        return false;
    }
    let mut bytes = value.bytes();
    let first = bytes.next().unwrap();
    (first.is_ascii_lowercase() || first.is_ascii_digit())
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-'
        })
}

fn validate_execution_data(value: &Value) -> Result<(), ValidationError> {
    strict_context_object(value)?;
    let data = ExecutionData::deserialize(value).map_err(|_| context_data_error())?;
    if let Some(phases) = &data.phases {
        if phases.len() > MAX_PHASES {
            return Err(context_data_error());
        }
        for phase in phases {
            if !valid_phase_name(&phase.name)
                || phase.elapsed_ms > MAX_DURATION_MS_U64
                || !matches!(phase.state.as_str(), "complete" | "active")
            {
                return Err(context_data_error());
            }
        }
    }
    if let Some(active_phase) = &data.active_phase
        && !valid_phase_name(active_phase)
    {
        return Err(context_data_error());
    }
    if let Some(cancel_reason) = &data.cancel_reason
        && !matches!(
            cancel_reason.as_str(),
            "prompt_aborted" | "interrupted" | "deadline" | "context_canceled"
        )
    {
        return Err(context_data_error());
    }
    if let Some(failure_kind) = &data.failure_kind
        && !matches!(
            failure_kind.as_str(),
            "usage" | "panic" | "handled" | "command"
        )
    {
        return Err(context_data_error());
    }
    for elapsed in [data.http_ms, data.lock_ms, data.input_wait_ms]
        .into_iter()
        .flatten()
    {
        if elapsed > MAX_DURATION_MS_U64 {
            return Err(context_data_error());
        }
    }
    if data.files.is_some_and(|files| files > MAX_ITEMS) {
        return Err(context_data_error());
    }
    Ok(())
}

fn validate_observation_context_data(kind: &str, value: &Value) -> Result<(), ValidationError> {
    let fields = value.as_object().ok_or_else(context_data_error)?;
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
            ("workspace", "packages") => value.as_u64().is_some_and(|n| n <= MAX_ITEMS),
            ("java", "major") => value.as_u64().is_some_and(|n| n > 0 && n <= 255),
            ("agent", "detected") => value.is_boolean(),
            _ => false,
        };
        if !valid {
            return Err(context_data_error());
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

    if let Some(duration) = payload.duration_ms
        && !(0..=MAX_DURATION_MS).contains(&duration)
    {
        return Err(ValidationError::new("invalid_field", "payload.duration_ms"));
    }
    if !identifier(&payload.operation, 128, true)
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
    {
        return Err(ValidationError::new("invalid_field", "payload"));
    }

    Ok(EventRecord {
        id,
        installation_id,
        invocation_id,
        occurred_at,
        received_at: received_at.to_string(),
        operation: required_string(&payload.operation, "payload.operation", 128)?,
        lucy_version: bounded_string(&payload.lucy_version, "payload.lucy_version", 64)?,
        lucy_commit: bounded_string(&payload.lucy_commit, "payload.lucy_commit", 64)?,
        channel: required_string(&payload.channel, "payload.channel", 32)?,
        os: required_string(&payload.os, "payload.os", 64)?,
        arch: required_string(&payload.arch, "payload.arch", 32)?,
        duration_ms: payload.duration_ms,
        outcome: required_string(&payload.outcome, "payload.outcome", 32)?,
        error_code: optional_string(&payload.error_code, "payload.error_code", 64)?,
        schema_version: validate_schema_version(payload.schema_version)?,
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

    let body_bytes = payload.body.len();
    if body_bytes == 0 || body_bytes > cfg.max_report_body_bytes {
        return Err(ValidationError::new("invalid_body_size", "payload.body"));
    }
    if payload.body.contains('\0') {
        return Err(ValidationError::new("invalid_body", "payload.body"));
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
        body,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const ID: &str = "11111111-1111-4111-8111-111111111111";
    const RECEIVED_AT: &str = "2026-01-01T00:00:00.000Z";

    fn event_payload(contexts: Value) -> Value {
        json!({
            "invocation_id": "22222222-2222-4222-8222-222222222222",
            "installation_id": "33333333-3333-4333-8333-333333333333",
            "occurred_at": Utc::now().to_rfc3339(),
            "operation": "lucy install",
            "lucy_version": "1.2.3",
            "lucy_commit": "abcdef0",
            "channel": "stable",
            "os": "macos",
            "arch": "arm64",
            "duration_ms": 10,
            "outcome": "success",
            "schema_version": 1,
            "contexts": contexts,
        })
    }

    fn invocation(data: Value) -> Value {
        json!([{
            "type": "invocation",
            "status": "present",
            "version": 1,
            "data": data,
        }])
    }

    fn execution(data: Value) -> Value {
        json!([{
            "type": "execution",
            "status": "present",
            "version": 1,
            "data": data,
        }])
    }

    fn validate(contexts: Value) -> Result<EventRecord, ValidationError> {
        validate_event(
            ID,
            &event_payload(contexts),
            &Config::default(),
            RECEIVED_AT,
            1,
        )
    }

    #[test]
    fn accepts_typed_observation_contexts() {
        let event = validate(json!([
            {
                "type": "invocation",
                "status": "present",
                "version": 1,
                "data": {
                    "intent": "command",
                    "arguments": [
                        {"name": "json", "kind": "boolean", "value": true},
                        {"name": "package", "kind": "text"},
                        {"name": "packages", "kind": "list"},
                        {"name": "retries", "kind": "number", "value": 3},
                        {"name": "core", "kind": "selector", "value": "paper@232"},
                        {"name": "version", "kind": "version", "value": "1.20.4"},
                        {"name": "arg-1", "kind": "text"}
                    ]
                }
            },
            {
                "type": "execution",
                "status": "present",
                "version": 1,
                "data": {
                    "phases": [
                        {"name": "setup", "elapsed_ms": 5, "state": "complete"},
                        {"name": "install", "elapsed_ms": 10, "state": "active"}
                    ],
                    "active_phase": "install",
                    "input_wait_ms": 250,
                    "files": 9,
                    "http_ms": 3,
                    "lock_ms": 1
                }
            },
            {
                "type": "workspace",
                "status": "present",
                "version": 1,
                "data": {"product": "paper", "product_version": "1.20.4", "packages": 3}
            },
            {
                "type": "java",
                "status": "present",
                "version": 1,
                "data": {"distribution": "temurin", "version": "21.0.1", "major": 21}
            },
            {
                "type": "agent",
                "status": "present",
                "version": 1,
                "data": {"detected": false}
            }
        ]))
        .unwrap();
        assert_eq!(event.contexts.len(), 5);
        assert!(
            event
                .contexts
                .iter()
                .all(|context| context.version == Some(1))
        );
    }

    #[test]
    fn accepts_bounded_future_names_and_selector_versions() {
        let event = validate(json!([
            {
                "type": "invocation",
                "status": "present",
                "data": {
                    "intent": "command",
                    "arguments": [
                        {"name": "future-flag", "kind": "text"},
                        {"name": "arg-12", "kind": "text"},
                        {"name": "core", "kind": "selector", "value": "snapshot"},
                        {"name": "loader", "kind": "selector", "value": "fabric@0.16.9"},
                        {"name": "minecraft", "kind": "version", "value": "25w45a"}
                    ]
                }
            },
            {
                "type": "execution",
                "status": "present",
                "data": {
                    "phases": [
                        {"name": "future_phase", "elapsed_ms": 604800000, "state": "complete"},
                        {"name": "install-phase-2", "elapsed_ms": 0, "state": "active"}
                    ],
                    "active_phase": "install-phase-2",
                    "input_wait_ms": 604800000,
                    "files": 1000000
                }
            }
        ]))
        .unwrap();
        assert_eq!(event.contexts.len(), 2);
    }

    #[test]
    fn accepts_absent_and_failed_reasons() {
        let event = validate(json!([
            {"type": "execution", "status": "absent", "reason": "not_reached"},
            {"type": "invocation", "status": "failed", "reason": "observation_failed"}
        ]))
        .unwrap();
        assert_eq!(event.contexts[0].reason.as_deref(), Some("not_reached"));
        assert_eq!(event.contexts[0].data, None);
        assert_eq!(
            event.contexts[1].reason.as_deref(),
            Some("observation_failed")
        );
    }

    #[test]
    fn rejects_unknown_or_arbitrary_values() {
        for contexts in [
            invocation(json!({"intent": "command", "output": "text"})),
            invocation(json!({"intent": "command", "offline": false})),
            invocation(json!({"intent": "command", "argv": ["x"]})),
            invocation(json!({"intent": "bash"})),
            invocation(
                json!({"intent": "command", "arguments": [{"name": "arg/v", "kind": "text"}]}),
            ),
            invocation(
                json!({"intent": "command", "arguments": [{"name": "flag", "kind": "yaml"}]}),
            ),
            invocation(
                json!({"intent": "command", "arguments": [{"name": "flag", "kind": "text", "value": "secret"}]}),
            ),
            invocation(
                json!({"intent": "command", "arguments": [{"name": "flag", "kind": "list", "value": ["a"]}]}),
            ),
            invocation(
                json!({"intent": "command", "arguments": [{"name": "flag", "kind": "boolean", "value": "yes"}]}),
            ),
            invocation(
                json!({"intent": "command", "arguments": [{"name": "flag", "kind": "number", "value": "1"}]}),
            ),
            invocation(
                json!({"intent": "command", "arguments": [{"name": "flag", "kind": "selector", "value": "custom"}]}),
            ),
            invocation(
                json!({"intent": "command", "arguments": [{"name": "flag", "kind": "selector", "value": "paper@latest"}]}),
            ),
            invocation(
                json!({"intent": "command", "arguments": [{"name": "flag", "kind": "selector", "value": "paper@1.2.3.4.5"}]}),
            ),
            invocation(
                json!({"intent": "command", "arguments": [{"name": "flag", "kind": "version", "value": "../../etc"}]}),
            ),
            execution(
                json!({"phases": [{"name": "bad name", "elapsed_ms": 1, "state": "complete"}]}),
            ),
            execution(json!({"active_phase": "Bad"})),
            execution(json!({"cancel_reason": "timeout"})),
            execution(json!({"failure_kind": "unexpected"})),
            execution(json!({"slow": false})),
        ] {
            assert!(
                validate(contexts.clone()).is_err(),
                "case must be rejected: {contexts}"
            );
        }
    }

    #[test]
    fn rejects_retired_contexts() {
        for kind in [
            "mystery",
            "prompt",
            "scale",
            "warnings",
            "collection",
            "transport",
        ] {
            assert!(
                validate(json!([{"type": kind, "status": "absent"}])).is_err(),
                "{kind} must be rejected"
            );
        }
    }

    #[test]
    fn rejects_out_of_range_observations() {
        let long_argument = "a".repeat(MAX_ARGUMENT_NAME + 1);
        let long_phase = "a".repeat(65);
        for contexts in [
            invocation(
                json!({"intent": "command", "arguments": [{"name": long_argument, "kind": "text"}]}),
            ),
            invocation(
                json!({"intent": "command", "arguments": [{"name": "flag", "kind": "number", "value": 9007199254740992i64}]}),
            ),
            invocation(
                json!({"intent": "command", "arguments": [{"name": "flag", "kind": "number", "value": -9007199254740992i64}]}),
            ),
            execution(
                json!({"phases": [{"name": long_phase, "elapsed_ms": 1, "state": "complete"}]}),
            ),
            execution(
                json!({"phases": [{"name": "setup", "elapsed_ms": 604800001, "state": "complete"}]}),
            ),
            execution(json!({"input_wait_ms": 604800001})),
            execution(json!({"files": 1000001})),
            execution(json!({"http_ms": 604800001})),
            execution(json!({"lock_ms": 604800001})),
            json!([{"type": "workspace", "status": "present", "data": {"packages": 1000001}}]),
            json!([{"type": "java", "status": "present", "data": {"major": 256}}]),
        ] {
            assert!(validate(contexts).is_err());
        }
    }

    #[test]
    fn rejects_duplicate_and_malformed_argument_names() {
        for arguments in [
            json!([
                {"name": "json", "kind": "boolean", "value": true},
                {"name": "json", "kind": "text"}
            ]),
            json!([
                {"name": "arg-1", "kind": "text"},
                {"name": "arg-1", "kind": "text"}
            ]),
        ] {
            assert!(
                validate(invocation(
                    json!({"intent": "command", "arguments": arguments})
                ))
                .is_err()
            );
        }

        for arguments in [
            json!([{"name": "arg-0", "kind": "text"}]),
            json!([{"name": "arg-01", "kind": "text"}]),
            json!([{"name": "arg-", "kind": "text"}]),
            json!([{"name": "-flag", "kind": "text"}]),
            json!([{"name": "Flag", "kind": "text"}]),
            json!([{"name": "flag_name", "kind": "text"}]),
        ] {
            assert!(
                validate(invocation(
                    json!({"intent": "command", "arguments": arguments})
                ))
                .is_err()
            );
        }
    }

    #[test]
    fn accepts_at_most_32_arguments() {
        let arguments: Vec<Value> = (0..MAX_ARGUMENTS)
            .map(|index| json!({"name": format!("flag-{index}"), "kind": "text"}))
            .collect();
        assert!(
            validate(invocation(
                json!({"intent": "command", "arguments": arguments})
            ))
            .is_ok()
        );

        let arguments: Vec<Value> = (0..MAX_ARGUMENTS + 1)
            .map(|index| json!({"name": format!("flag-{index}"), "kind": "text"}))
            .collect();
        assert!(
            validate(invocation(
                json!({"intent": "command", "arguments": arguments})
            ))
            .is_err()
        );
    }

    #[test]
    fn rejects_more_than_24_phases() {
        let phases: Vec<Value> = (0..MAX_PHASES + 1)
            .map(|_| json!({"name": "setup", "elapsed_ms": 1, "state": "complete"}))
            .collect();
        assert!(validate(execution(json!({"phases": phases}))).is_err());
    }

    #[test]
    fn rejects_envelope_violations() {
        for contexts in [
            json!([{"type": "invocation", "status": "present", "version": 2, "data": {"intent": "command"}}]),
            json!([{"type": "invocation", "status": "present", "reason": "no_result", "data": {"intent": "command"}}]),
            json!([{"type": "invocation", "status": "absent", "reason": "observation_failed"}]),
            json!([{"type": "invocation", "status": "failed", "reason": "no_result"}]),
            json!([{"type": "invocation", "status": "absent", "data": {"intent": "command"}}]),
            json!([{"type": "transport", "status": "present", "data": {"queue_ms": 1}}]),
            invocation(json!({"intent": "command", "extra": null})),
            execution(json!({"input_wait_ms": null})),
        ] {
            assert!(validate(contexts).is_err());
        }
    }

    #[test]
    fn event_requires_operation_and_rejects_removed_fields() {
        let mut payload = event_payload(json!([]));
        payload["collection_policy"] = json!("default");
        assert!(validate_event(ID, &payload, &Config::default(), RECEIVED_AT, 1).is_err());

        let mut payload = event_payload(json!([]));
        let operation = payload["operation"].clone();
        let _ = payload.as_object_mut().unwrap().remove("operation");
        payload["command"] = operation;
        assert!(validate_event(ID, &payload, &Config::default(), RECEIVED_AT, 1).is_err());
    }

    #[test]
    fn report_drops_collection_policy_and_scrubs_body() {
        let report_payload = || {
            json!({
                "report_id": ID,
                "installation_id": "33333333-3333-4333-8333-333333333333",
                "kind": "failure",
                "created_at": Utc::now().to_rfc3339(),
                "schema_version": 1,
                "body": "panic: token=hunter2 at /Users/me/app.log",
            })
        };

        let report = validate_report(ID, &report_payload(), &Config::default()).unwrap();
        assert!(!report.body.contains("hunter2"));
        assert!(report.body.contains("[redacted]"));
        assert!(report.body.contains("[redacted-path]"));

        let mut payload = report_payload();
        payload["collection_policy"] = json!("default");
        assert!(validate_report(ID, &payload, &Config::default()).is_err());
    }

    #[test]
    fn observation_contexts_keep_their_shape() {
        let event = validate(json!([
            {"type": "workspace", "status": "present", "data": {"product": "paper", "product_version": "1.20.4", "packages": 7}},
            {"type": "java", "status": "absent"},
            {"type": "agent", "status": "failed"}
        ]))
        .unwrap();
        assert_eq!(event.contexts.len(), 3);
        assert!(
            event
                .contexts
                .iter()
                .all(|context| context.version.is_none() && context.reason.is_none())
        );
    }

    #[test]
    fn queued_legacy_context_serialization_is_unchanged() {
        let queued = json!({"type": "workspace", "status": "absent", "data": null});
        let record: ContextRecord = serde_json::from_value(queued.clone()).unwrap();
        assert_eq!(serde_json::to_value(&record).unwrap(), queued);

        let present = json!({
            "type": "workspace",
            "status": "present",
            "data": {"product": "paper", "packages": 2}
        });
        let record: ContextRecord = serde_json::from_value(present.clone()).unwrap();
        assert_eq!(serde_json::to_value(&record).unwrap(), present);
    }

    #[test]
    fn stored_event_operation_replaces_command() {
        let event = validate(json!([])).unwrap();
        assert_eq!(event.operation, "lucy install");
    }
}
