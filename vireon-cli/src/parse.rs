//! Spec / policy / TLS-mode parsing helpers.
//!
//! Each parser converts a user-supplied `&str` into a typed value or a
//! [`CliError::BadArg`]. Used by the command dispatch layer and the
//! `mux` command's `--stream` / `--send` splat.

use std::path::PathBuf;

use bytes::Bytes;
use vireon_sdk::{DeliveryPolicy, TlsVerify};

use crate::error::CliError;

/// Parse the `--tls-verify` flag into a [`TlsVerify`] policy.
pub fn parse_tls_verify(s: &str) -> Result<TlsVerify, CliError> {
    if let Some(ca) = s.strip_prefix("strict:") {
        let p = PathBuf::from(ca);
        if !p.exists() {
            return Err(CliError::BadArg(format!(
                "CA bundle not found: {}",
                p.display()
            )));
        }
        return Ok(TlsVerify::Strict { ca: p });
    }
    if let Some(der) = s.strip_prefix("pinned:") {
        let p = PathBuf::from(der);
        if !p.exists() {
            return Err(CliError::BadArg(format!(
                "pinned cert not found: {}",
                p.display()
            )));
        }
        let bytes = std::fs::read(&p).map_err(|e| {
            CliError::BadArg(format!("read {}: {e}", p.display()))
        })?;
        return Ok(TlsVerify::Pinned { cert_der: bytes });
    }
    match s {
        "tofu" => Ok(TlsVerify::Tofu),
        "danger_accept_invalid" => Ok(TlsVerify::DangerAcceptInvalid),
        other => Err(CliError::BadArg(format!(
            "unknown tls_verify mode: {other} (expected tofu, danger_accept_invalid, strict:<path>, pinned:<path>)"
        ))),
    }
}

/// Parse a per-stream delivery policy name.
pub fn parse_policy(s: &str) -> Result<DeliveryPolicy, CliError> {
    match s {
        "reliable_ordered" | "ordered" => Ok(DeliveryPolicy::ReliableOrdered),
        "reliable_unordered" | "unordered" => Ok(DeliveryPolicy::ReliableUnordered),
        "realtime_drop_old" | "realtime" => Ok(DeliveryPolicy::RealtimeDropOld),
        "latest_only" | "latest" => Ok(DeliveryPolicy::LatestOnly),
        other => Err(CliError::BadArg(format!(
            "unknown policy: {other} (expected reliable_ordered, reliable_unordered, realtime_drop_old, latest_only)"
        ))),
    }
}

/// Parse `--stream LABEL=TOPIC:POLICY`.
///
/// Splits on the first `=` to get (label, "topic:policy"), then on the
/// LAST `:` of the remainder so topic segments may themselves contain `:`.
pub fn parse_stream_spec(
    s: &str,
) -> Result<(String, String, DeliveryPolicy), CliError> {
    let eq = s
        .find('=')
        .ok_or_else(|| CliError::BadArg(format!("invalid --stream '{s}': expected LABEL=TOPIC:POLICY")))?;
    let label = s[..eq].to_string();
    if label.is_empty() {
        return Err(CliError::BadArg(format!(
            "invalid --stream '{s}': empty label"
        )));
    }
    let rest = &s[eq + 1..];
    let colon = rest.rfind(':').ok_or_else(|| {
        CliError::BadArg(format!(
            "invalid --stream '{s}': expected LABEL=TOPIC:POLICY (missing ':policy')"
        ))
    })?;
    let topic = rest[..colon].to_string();
    let policy_str = &rest[colon + 1..];
    if topic.is_empty() {
        return Err(CliError::BadArg(format!(
            "invalid --stream '{s}': empty topic"
        )));
    }
    let policy = parse_policy(policy_str)?;
    Ok((label, topic, policy))
}

/// Parse `--send LABEL=PAYLOAD`. Splits on the FIRST `=` only — the payload
/// may itself contain `=`, `:`, or any other byte.
pub fn parse_send_spec(s: &str) -> Result<(String, Bytes), CliError> {
    let eq = s
        .find('=')
        .ok_or_else(|| CliError::BadArg(format!("invalid --send '{s}': expected LABEL=PAYLOAD")))?;
    let label = s[..eq].to_string();
    if label.is_empty() {
        return Err(CliError::BadArg(format!(
            "invalid --send '{s}': empty label"
        )));
    }
    let payload = Bytes::copy_from_slice(&s.as_bytes()[eq + 1..]);
    Ok((label, payload))
}

/// Inverse of [`parse_policy`] — used for human-readable output labels.
pub fn policy_name(p: DeliveryPolicy) -> &'static str {
    match p {
        DeliveryPolicy::ReliableOrdered => "reliable_ordered",
        DeliveryPolicy::ReliableUnordered => "reliable_unordered",
        DeliveryPolicy::RealtimeDropOld => "realtime_drop_old",
        DeliveryPolicy::LatestOnly => "latest_only",
    }
}
