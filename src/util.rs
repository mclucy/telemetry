use std::collections::HashMap;

use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use futures_util::StreamExt;
use serde::Serialize;
use sha2::{Digest, Sha256};
use worker::{console_error, wasm_bindgen::JsValue, Env, Error, Request, Response, Result};

pub enum BodyReadError {
    TooLarge,
    BadRequest,
    UnsupportedEncoding,
    Read,
}

pub fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub fn cutoff_rfc3339(days: i64) -> String {
    (Utc::now() - TimeDelta::days(days)).to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub fn cutoff_rfc3339_hours(hours: i64) -> String {
    (Utc::now() - TimeDelta::hours(hours)).to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub fn canonical_filter_time(value: &str) -> Option<String> {
    DateTime::parse_from_rfc3339(value.trim())
        .ok()
        .map(|parsed| parsed.with_timezone(&Utc))
        .map(|utc| utc.to_rfc3339_opts(SecondsFormat::Millis, true))
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

/// Constant-time comparison of secrets. Both sides are hashed first so the
/// comparison does not leak the configured token's length.
pub fn constant_time_eq(left: &str, right: &str) -> bool {
    let left = Sha256::digest(left.as_bytes());
    let right = Sha256::digest(right.as_bytes());
    let mut difference = 0u8;
    for (a, b) in left.iter().zip(right.iter()) {
        difference |= a ^ b;
    }
    difference == 0
}

pub fn extract_bearer(request: &Request) -> Option<String> {
    let header = request.headers().get("authorization").ok()??;
    let (scheme, token) = header.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    if token.is_empty() || token.contains(char::is_whitespace) {
        return None;
    }
    Some(token.to_string())
}

pub fn is_authorized(request: &Request, env: &Env) -> bool {
    let Some(provided) = extract_bearer(request) else {
        return false;
    };
    let expected = match env.secret("RETRIEVAL_TOKEN") {
        Ok(secret) => secret.to_string(),
        Err(_) => {
            console_error!("retrieval_token_missing");
            return false;
        }
    };
    !expected.is_empty() && constant_time_eq(&provided, &expected)
}

pub fn query_map(request: &Request) -> Result<HashMap<String, String>> {
    let url = request.url()?;
    Ok(url
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect())
}

pub async fn read_body_limited(
    request: &mut Request,
    max: usize,
) -> std::result::Result<Vec<u8>, BodyReadError> {
    if let Ok(Some(encoding)) = request.headers().get("content-encoding") {
        let encoding = encoding.trim().to_ascii_lowercase();
        if !encoding.is_empty() && encoding != "identity" {
            return Err(BodyReadError::UnsupportedEncoding);
        }
    }

    if let Ok(Some(raw_length)) = request.headers().get("content-length") {
        if let Ok(length) = raw_length.trim().parse::<u64>() {
            if length > max as u64 {
                return Err(BodyReadError::TooLarge);
            }
        }
    }

    let mut stream = request.stream().map_err(|_| BodyReadError::BadRequest)?;
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| BodyReadError::Read)?;
        let next_len = body
            .len()
            .checked_add(chunk.len())
            .ok_or(BodyReadError::TooLarge)?;
        if next_len > max {
            return Err(BodyReadError::TooLarge);
        }
        body.extend_from_slice(&chunk);
    }

    if body.is_empty() {
        return Err(BodyReadError::BadRequest);
    }
    Ok(body)
}

pub fn body_error_response(error: BodyReadError, max: usize) -> Result<Response> {
    match error {
        BodyReadError::TooLarge => payload_too_large(max),
        BodyReadError::UnsupportedEncoding => error_response(415, "unsupported_content_encoding"),
        BodyReadError::BadRequest | BodyReadError::Read => error_response(400, "invalid_body"),
    }
}

fn no_store(mut response: Response) -> Result<Response> {
    let headers = response.headers_mut();
    headers.set("cache-control", "no-store")?;
    headers.set("x-content-type-options", "nosniff")?;
    Ok(response)
}

pub fn json_response<T: Serialize>(status: u16, value: &T) -> Result<Response> {
    no_store(Response::from_json(value)?.with_status(status))
}

pub fn text_response(text: String, content_type: &str) -> Result<Response> {
    let mut response = Response::ok(text)?;
    let headers = response.headers_mut();
    headers.set("content-type", content_type)?;
    headers.set("content-disposition", "inline")?;
    headers.set("content-security-policy", "default-src 'none'")?;
    no_store(response)
}

pub fn error_response(status: u16, code: &str) -> Result<Response> {
    json_response(
        status,
        &serde_json::json!({
            "error": code,
        }),
    )
}

pub fn payload_too_large(max: usize) -> Result<Response> {
    json_response(
        413,
        &serde_json::json!({
            "error": "payload_too_large",
            "max_bytes": max,
        }),
    )
}

pub fn unauthorized() -> Result<Response> {
    let mut response = error_response(401, "unauthorized")?;
    response
        .headers_mut()
        .set("www-authenticate", "Bearer realm=\"lucy-telemetry\"")?;
    Ok(response)
}

pub fn method_not_allowed(allow: &str) -> Result<Response> {
    let mut response = error_response(405, "method_not_allowed")?;
    response.headers_mut().set("allow", allow)?;
    Ok(response)
}

pub fn internal_error() -> Result<Response> {
    error_response(500, "internal")
}

pub fn js_str(value: &str) -> JsValue {
    JsValue::from_str(value)
}

pub fn js_opt_str(value: &Option<String>) -> JsValue {
    match value {
        Some(value) => JsValue::from_str(value),
        None => JsValue::NULL,
    }
}

pub fn js_num(value: i64) -> JsValue {
    JsValue::from_f64(value as f64)
}

pub fn js_opt_num(value: Option<i64>) -> JsValue {
    match value {
        Some(value) => JsValue::from_f64(value as f64),
        None => JsValue::NULL,
    }
}

pub fn error_kind(error: &Error) -> &'static str {
    match error {
        Error::RustError(_) => "rust",
        Error::JsError(_) => "js",
        _ => "other",
    }
}
