use regex::Regex;
use std::sync::LazyLock;

static PRIVATE_KEY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?(?:-----END [A-Z0-9 ]*PRIVATE KEY-----|\z)",
    )
    .unwrap()
});
static CREDENTIAL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
    r#"(?i)\b([a-z0-9_]*(?:api[_-]?key|access[_-]?key|private[_-]?key|secret|token|password|passwd|pwd|authorization|cookie|session[_-]?id)[a-z0-9_]*)\b["']?\s*[:=]\s*("(?:\\.|[^"\\])*"?|'(?:\\.|[^'\\])*'?|[^\s,;]+)"#,
).unwrap()
});
static BEARER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bbearer\s+[A-Za-z0-9._~+/\-=]+").unwrap());
static COMMON_TOKEN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
    r"\b(?:sk-[A-Za-z0-9_-]{16,}|gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|xox[baprs]-[A-Za-z0-9-]{10,})\b",
).unwrap()
});
static IPV4: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b(?:\d{1,3}\.){3}\d{1,3}\b").unwrap());
static IPV6: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:[0-9a-f]{1,4}:){2,}[0-9a-f:]{0,35}\b").unwrap());
static UNIX_PATH: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(^|[\s"'(\[])(/(?:[A-Za-z0-9._-]+/)+[A-Za-z0-9._-]+)"#).unwrap()
});
static WINDOWS_PATH: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?i)\b[A-Z]:\\[^\s"'<>]+"#).unwrap());
static UNC_PATH: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"\\\\[A-Za-z0-9._-]+\\[^\s"'<>]+"#).unwrap());
static HOME_PATH: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"~/[^\s"'<>]+"#).unwrap());

pub fn filter_report(body: &str) -> String {
    let body = PRIVATE_KEY.replace_all(body, "[redacted-private-key]");
    let body = BEARER.replace_all(&body, "Bearer [redacted]");
    let body = CREDENTIAL.replace_all(&body, "$1=[redacted]");
    let body = COMMON_TOKEN.replace_all(&body, "[redacted-token]");
    let body = IPV4.replace_all(&body, "[redacted-ip]");
    let body = IPV6.replace_all(&body, "[redacted-ip]");
    let body = WINDOWS_PATH.replace_all(&body, "[redacted-path]");
    let body = UNC_PATH.replace_all(&body, "[redacted-path]");
    let body = HOME_PATH.replace_all(&body, "[redacted-path]");
    UNIX_PATH
        .replace_all(&body, "${1}[redacted-path]")
        .into_owned()
}
